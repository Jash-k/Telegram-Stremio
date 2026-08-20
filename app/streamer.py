"""Range-aware Telegram byte streamer.

Ports the robust streaming core of the upstream Telegram-Stremio `ByteStreamer`
(weebzone/Telegram-Stremio) into this lean project:

  * producer/consumer with an ordered, bounded asyncio.Queue
  * `asyncio.wait(FIRST_COMPLETED)` so `parallelism` chunks are always in flight
  * per-chunk retries with FloodWait jitter + FILE_REFERENCE refresh
  * client-disconnect detection (aborts early, saves bandwidth)
  * producer-stall timeout (90 s) so a hung stream never spins forever
  * media-session prewarm for the common datacenters (kills first-byte latency)

Single-userbot (no multi-client session pool or telemetry — kept lean for
Koyeb free tier).
"""
import asyncio
import random
import re
from typing import AsyncIterator, Optional

from pyrogram import Client, raw
from pyrogram.errors import AuthBytesInvalid
from pyrogram.file_id import FileId
from pyrogram.session import Auth, Session

from . import config
from .logger import LOGGER

CHUNK_SIZE = 1024 * 1024  # 1 MiB
STALL_TIMEOUT = 90.0      # abort if no chunk for this long
_DISCONNECT_CHECK_EVERY = 8


class FileNotFoundError_(Exception):
    pass


class ClientNotConnected(Exception):
    pass


class Streamer:
    def __init__(self, client: Client):
        self.client = client
        self._session_lock = asyncio.Lock()
        self._cache: dict = {}
        asyncio.create_task(self._prewarm_sessions())

    # ------------------------------------------------------------------
    # File properties (cached FileId lookup)
    # ------------------------------------------------------------------
    def invalidate(self, chat_id: int, message_id: int) -> None:
        self._cache.pop((int(chat_id), int(message_id)), None)

    async def file_properties(self, chat_id: int, message_id: int) -> FileId:
        if self.client is None or not self.client.is_connected:
            raise ClientNotConnected("userbot client is not connected")

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
        self.invalidate(chat_id, message_id)
        try:
            return await self.file_properties(chat_id, message_id)
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Media sessions
    # ------------------------------------------------------------------
    async def _prewarm_sessions(self) -> None:
        """Open media sessions for the common DCs up front (lower first-byte latency)."""
        if self.client is None or not self.client.is_connected:
            return
        try:
            test_mode = await self.client.storage.test_mode()
            current_dc = await self.client.storage.dc_id()
        except Exception:
            return
        for dc in (1, 2, 4, 5):
            if dc in self.client.media_sessions or dc == current_dc:
                continue
            try:
                auth_key = await Auth(self.client, dc, test_mode).create()
                session = Session(self.client, dc, auth_key, test_mode, is_media=True)
                session.no_updates = True
                session.timeout = 30
                session.sleep_threshold = 60
                await session.start()
                imported = False
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
                        imported = True
                        break
                    except AuthBytesInvalid:
                        await asyncio.sleep(0.5)
                    except OSError:
                        await asyncio.sleep(1)
                    except Exception:
                        break
                if imported:
                    self.client.media_sessions[dc] = session
                else:
                    await session.stop()
            except Exception:
                continue

    async def _get_media_session(self, fid: FileId) -> Session:
        dc = fid.dc_id
        if dc in self.client.media_sessions:
            return self.client.media_sessions[dc]

        async with self._session_lock:
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
                    except AuthBytesInvalid:
                        await asyncio.sleep(0.5)
                    except OSError:
                        await asyncio.sleep(1)

            self.client.media_sessions[dc] = session
            return session

    @staticmethod
    async def _get_location(fid: FileId):
        return raw.types.InputDocumentFileLocation(
            id=fid.media_id,
            access_hash=fid.access_hash,
            file_reference=fid.file_reference,
            thumb_size=fid.thumbnail_size,
        )

    # ------------------------------------------------------------------
    # Streaming (producer/consumer, range-aware)
    # ------------------------------------------------------------------
    async def stream(
        self,
        fid: FileId,
        start: int,
        end: int,
        parallelism: int = None,
        prefetch: int = None,
        chat_id: int = None,
        message_id: int = None,
        request=None,
    ) -> AsyncIterator[bytes]:
        parallelism = parallelism or config.STREAM_PARALLELISM
        prefetch = prefetch or config.STREAM_PREFETCH
        chunk = CHUNK_SIZE

        offset = start - (start % chunk)
        first_cut = start - offset
        last_cut = (end % chunk) + 1
        part_count = (end // chunk) - (offset // chunk) + 1

        session = await self._get_media_session(fid)
        loc_box: list = [await self._get_location(fid)]
        stop_event = asyncio.Event()
        queue: asyncio.Queue = asyncio.Queue(maxsize=max(1, prefetch))

        async def refresh() -> bool:
            if not chat_id or not message_id:
                return False
            fresh = await self._refresh_properties(chat_id, message_id)
            if fresh:
                loc_box[0] = await self._get_location(fresh)
                return True
            return False

        async def fetch_chunk(seq: int, off: int):
            """Fetch one chunk; returns (seq, bytes) or (seq, None) on failure."""
            tries = 0
            flood_tries = 0
            while tries < 3 and flood_tries < 5 and not stop_event.is_set():
                try:
                    r = await asyncio.wait_for(
                        session.send(
                            raw.functions.upload.GetFile(
                                location=loc_box[0], offset=off, limit=chunk
                            )
                        ),
                        timeout=15.0,
                    )
                    data = getattr(r, "bytes", None) if r else None
                    if data == b"":
                        return seq, None
                    return seq, data
                except asyncio.TimeoutError:
                    tries += 1
                    await asyncio.sleep(min(0.5 * (2 ** (tries - 1)), 10.0))
                except Exception as e:
                    err = str(e)
                    if "FILE_REFERENCE" in err or "file_reference" in err.lower():
                        await refresh()
                    m = re.search(r"wait of (\d+) second", err, re.IGNORECASE)
                    if m:
                        wait = float(m.group(1)) + random.uniform(0.5, 2.0)
                        flood_tries += 1
                        await asyncio.sleep(wait)
                    else:
                        tries += 1
                        await asyncio.sleep(min(0.5 * (2 ** (tries - 1)), 10.0))
            return seq, None

        async def producer():
            scheduled: dict = {}
            try:
                if part_count <= 0:
                    await queue.put((None, None))
                    return

                next_to_schedule = 0
                results_buffer: dict = {}
                next_to_put = 0
                max_parallel = max(1, parallelism)

                for _ in range(min(part_count, max_parallel)):
                    seq = next_to_schedule
                    scheduled[seq] = asyncio.create_task(
                        fetch_chunk(seq, offset + seq * chunk)
                    )
                    next_to_schedule += 1

                while next_to_put < part_count:
                    if stop_event.is_set():
                        break

                    if not scheduled:
                        seq = next_to_schedule
                        scheduled[seq] = asyncio.create_task(
                            fetch_chunk(seq, offset + seq * chunk)
                        )
                        next_to_schedule += 1

                    done, _ = await asyncio.wait(
                        scheduled.values(), return_when=asyncio.FIRST_COMPLETED
                    )

                    for completed in done:
                        seq = next(k for k, t in scheduled.items() if t is completed)
                        seq_idx, chunk_bytes = completed.result()
                        scheduled.pop(seq, None)

                        if chunk_bytes is None:
                            await queue.put((None, None))
                            return

                        results_buffer[seq_idx] = chunk_bytes

                        if next_to_schedule < part_count:
                            seq = next_to_schedule
                            scheduled[seq] = asyncio.create_task(
                                fetch_chunk(seq, offset + seq * chunk)
                            )
                            next_to_schedule += 1

                    while next_to_put in results_buffer:
                        chunk_bytes = results_buffer.pop(next_to_put)
                        await queue.put((offset + next_to_put * chunk, chunk_bytes))
                        next_to_put += 1

                await queue.put((None, None))
            except asyncio.CancelledError:
                try:
                    await queue.put((None, None))
                except Exception:
                    pass
                raise
            except Exception as exc:
                LOGGER.exception("Stream producer error: %s", exc)
                try:
                    await queue.put((None, None))
                except Exception:
                    pass
            finally:
                stop_event.set()
                for t in scheduled.values():
                    if not t.done():
                        t.cancel()
                if scheduled:
                    await asyncio.gather(*scheduled.values(), return_exceptions=True)

        async def consumer():
            producer_task = asyncio.create_task(producer())
            part_idx = 1
            try:
                while True:
                    if part_idx % _DISCONNECT_CHECK_EVERY == 0:
                        try:
                            if request and await request.is_disconnected():
                                stop_event.set()
                                break
                        except Exception:
                            pass

                    try:
                        item = await asyncio.wait_for(queue.get(), timeout=STALL_TIMEOUT)
                    except asyncio.TimeoutError:
                        LOGGER.error("Stream producer stalled (%ss) — aborting", STALL_TIMEOUT)
                        stop_event.set()
                        break

                    if item is None:
                        break
                    off, data = item
                    if off is None and data is None:
                        break

                    if part_count == 1:
                        out = data[first_cut:last_cut]
                    elif part_idx == 1:
                        out = data[first_cut:]
                    elif part_idx == part_count:
                        out = data[:last_cut]
                    else:
                        out = data

                    if out:
                        yield out
                    part_idx += 1
            finally:
                stop_event.set()
                if not producer_task.done():
                    producer_task.cancel()
                    try:
                        await asyncio.wait_for(producer_task, timeout=2.0)
                    except (Exception, asyncio.CancelledError):
                        pass

        return consumer()
