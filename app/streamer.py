"""Range-aware Telegram byte streamer.

Streams file bytes directly from the file's datacenter using raw
`upload.GetFile` over a dedicated media session. Supports HTTP Range (seeking),
bounded parallel reads and prefetch, plus FloodWait + file-reference recovery
for smooth playback on a small instance.
"""
import asyncio
import re
from typing import AsyncIterator

from pyrogram import Client, raw
from pyrogram.file_id import FileId
from pyrogram.session import Auth, Session

from . import config

CHUNK_SIZE = 1024 * 1024  # 1 MiB


class FileNotFoundError_(Exception):
    pass


class Streamer:
    def __init__(self, client: Client):
        self.client = client
        self._lock = asyncio.Lock()
        self._cache: dict = {}

    def invalidate(self, chat_id: int, message_id: int) -> None:
        self._cache.pop((int(chat_id), int(message_id)), None)

    async def file_properties(self, chat_id: int, message_id: int) -> FileId:
        key = (int(chat_id), int(message_id))
        if key in self._cache:
            return self._cache[key]

        message = await self.client.get_messages(key[0], key[1])
        if not message or getattr(message, "empty", False):
            raise FileNotFoundError_("message not found")
        media = message.video or message.document or message.audio
        if not media:
            raise FileNotFoundError_("no media")

        fid = FileId.decode(media.file_id)
        fid.file_name = getattr(media, "file_name", "") or ""
        fid.file_size = getattr(media, "file_size", 0) or 0
        fid.mime_type = getattr(media, "mime_type", "") or ""
        self._cache[key] = fid
        return fid

    async def _refresh_properties(self, chat_id: int, message_id: int) -> FileId | None:
        """Re-fetch a file's properties (and thus its file_reference)."""
        self.invalidate(chat_id, message_id)
        try:
            return await self.file_properties(chat_id, message_id)
        except Exception:
            return None

    async def _media_session(self, fid: FileId) -> Session:
        dc = fid.dc_id
        if dc in self.client.media_sessions:
            return self.client.media_sessions[dc]

        async with self._lock:
            if dc in self.client.media_sessions:
                return self.client.media_sessions[dc]

            test_mode = await self.client.storage.test_mode()
            current_dc = await self.client.storage.dc_id()

            if dc != current_dc:
                auth_key = await Auth(self.client, dc, test_mode).create()
            else:
                auth_key = await self.client.storage.auth_key()

            session = Session(self.client, dc, auth_key, test_mode, is_media=True)
            session.no_updates = True
            session.timeout = 30
            session.sleep_threshold = 60
            await session.start()

            if dc != current_dc:
                for _ in range(6):
                    try:
                        exported = await self.client.invoke(
                            raw.functions.auth.ExportAuthorization(dc_id=dc)
                        )
                        await session.send(
                            raw.functions.auth.ImportAuthorization(
                                id=exported.id, bytes=exported.bytes
                            )
                        )
                        break
                    except Exception:
                        await asyncio.sleep(0.5)

            self.client.media_sessions[dc] = session
            return session

    @staticmethod
    async def _location(fid: FileId):
        return raw.types.InputDocumentFileLocation(
            id=fid.media_id,
            access_hash=fid.access_hash,
            file_reference=fid.file_reference,
            thumb_size=fid.thumbnail_size,
        )

    async def stream(
        self,
        fid: FileId,
        start: int,
        end: int,
        parallelism: int = None,
        prefetch: int = None,
        chat_id: int = None,
        message_id: int = None,
    ) -> AsyncIterator[bytes]:
        """Yield the inclusive byte range [start, end] as chunks."""
        parallelism = parallelism or config.STREAM_PARALLELISM
        prefetch = prefetch or config.STREAM_PREFETCH
        chunk = CHUNK_SIZE

        offset = start - (start % chunk)
        first_cut = start - offset
        last_cut = (end % chunk) + 1
        part_count = (end // chunk) - (offset // chunk) + 1

        session = await self._media_session(fid)
        # Mutable location so file-reference refreshes take effect mid-stream.
        loc_box: list = [await self._location(fid)]

        async def refresh() -> bool:
            if not chat_id or not message_id:
                return False
            fresh = await self._refresh_properties(chat_id, message_id)
            if fresh:
                loc_box[0] = await self._location(fresh)
                return True
            return False

        sem = asyncio.Semaphore(max(1, parallelism))

        async def fetch(i: int):
            off = offset + i * chunk
            async with sem:
                for attempt in range(8):
                    try:
                        r = await asyncio.wait_for(
                            session.send(
                                raw.functions.upload.GetFile(
                                    location=loc_box[0], offset=off, limit=chunk
                                )
                            ),
                            timeout=30.0,
                        )
                        data = getattr(r, "bytes", None)
                        return data if data is not None else b""
                    except asyncio.TimeoutError:
                        await asyncio.sleep(min(1.0 * (2 ** attempt), 10.0))
                    except Exception as e:
                        err = str(e)
                        if "FILE_REFERENCE" in err or "file_reference" in err.lower():
                            await refresh()
                            # retry immediately with the fresh reference
                            continue
                        m = re.search(r"wait of (\d+) second", err, re.IGNORECASE)
                        if m:
                            await asyncio.sleep(float(m.group(1)) + 1.0)
                        else:
                            await asyncio.sleep(min(0.5 * (2 ** attempt), 10.0))
                return None

        tasks: dict = {}
        next_to_yield = 0
        next_to_schedule = 0

        for i in range(min(part_count, max(1, prefetch))):
            tasks[i] = asyncio.create_task(fetch(i))
            next_to_schedule = i + 1

        try:
            while next_to_yield < part_count:
                task = tasks.pop(next_to_yield)
                data = await task
                if next_to_schedule < part_count:
                    tasks[next_to_schedule] = asyncio.create_task(fetch(next_to_schedule))
                    next_to_schedule += 1
                if data is None:
                    # Unrecoverable chunk — stop to avoid a short body.
                    break
                buf = data
                if next_to_yield == 0:
                    buf = buf[first_cut:]
                if next_to_yield == part_count - 1:
                    buf = buf[:last_cut]
                if buf:
                    yield buf
                next_to_yield += 1
        finally:
            for t in tasks.values():
                t.cancel()
