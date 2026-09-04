"""Range-aware Telegram byte streamer.

Robust streaming core (ported from upstream `ByteStreamer`) plus adaptive
flood handling for a single user session:

  * producer/consumer with an ordered, bounded asyncio.Queue
  * `asyncio.wait(FIRST_COMPLETED)` so `parallelism` chunks are in flight
  * media-session setup is TIMEOUT-BOUND (a throttled DC can never hang a stream)
  * FLOOD_WAIT is "slow down", not "stop" — retried patiently instead of killing the stream
  * adaptive parallelism: any flood temporarily drops concurrency to 1, then
    ramps back up after a quiet period (a user session floods easily at >1)
  * FILE_REFERENCE refresh + client-disconnect detection + stall timeout

Telegram user sessions have a low `GetFile` concurrency limit. This module
keeps throughput as high as Telegram will allow *without* tripping its flood
protection — and when it does flood, it degrades gracefully instead of dying.
"""
import asyncio
import random
import re
import time
from typing import AsyncIterator, Optional

from pyrogram import Client, raw
from pyrogram.errors import AuthBytesInvalid
from pyrogram.file_id import FileId
from pyrogram.session import Auth, Session

from . import config
from . import telemetry
from .logger import LOGGER

CHUNK_SIZE = 1024 * 1024     # 1 MiB
STALL_TIMEOUT = 90.0         # abort if no chunk for this long
MEDIA_SESSION_TIMEOUT = 20.0  # session setup must not hang
# Stay at low parallelism for this long after a flood (was a flat 30s that
# cratered speed after a single tiny flood). Configurable.
_FLOOD_COOLDOWN = float(config.FLOOD_COOLDOWN_SECONDS or 12)
_MAX_FLOOD_RETRIES = 30       # patient ceiling on consecutive floods (~minutes)

# Adaptive flood state (module-wide for health display; the actual throttling
# decision is per-Streamer-instance so a user-session flood never throttles a bot).
_last_flood = 0.0


def flood_state() -> dict:
    """Expose adaptive-flood state (for the health dashboard)."""
    return {
        "last_flood_ago": round(time.time() - _last_flood, 1) if _last_flood else None,
        "throttled": time.time() - _last_flood < _FLOOD_COOLDOWN,
    }


class FileNotFoundError_(Exception):
    pass


class ClientNotConnected(Exception):
    pass


def _session_dead_error(err: str) -> bool:
    """True for errors that mean the media TCP connection died (rebuild it)."""
    low = (err or "").lower()
    markers = (
        "connection", "not connected", "closed", "broken pipe", "reset",
        "eof", "transport", "session revoked", "unauthorized", "406",
        "auth key", "rpc call failed due to network",
    )
    return any(m in low for m in markers)


async def _safe_stop(session) -> None:
    try:
        await session.stop()
    except Exception:
        pass


class Streamer:
    def __init__(self, client: Client):
        self.client = client
        self._session_lock = asyncio.Lock()
        self._media_pool: dict = {}   # dc -> {"sessions": [Session...], "rr": int, "lock": Lock}
        self._cache: dict = {}
        self._last_flood = 0.0  # per-client: a user flood must not throttle a bot
        # Pool size: bots tolerate more than a user session.
        is_bot = bool(getattr(client, "bot_token", None))
        self._pool_size = config.BOT_STREAM_MEDIA_SESSIONS if is_bot else config.STREAM_MEDIA_SESSIONS
        asyncio.create_task(self._prewarm_sessions())

    def _pool(self, dc: int) -> dict:
        p = self._media_pool.get(dc)
        if p is None:
            p = {"sessions": [], "rr": 0, "lock": asyncio.Lock()}
            self._media_pool[dc] = p
        return p

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
    # Media sessions (timeout-bound)
    # ------------------------------------------------------------------
    async def _open_session(self, dc: int) -> Session:
        """Create + start a media session to a DC (no timeout inside — caller bounds it)."""
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
        return session

    async def _prewarm_sessions(self) -> None:
        """Open ONE media session to each common file DC up front, so the FIRST
        stream's chunks never pay a session-handshake (auth-export) cost — that
        is the difference between instant and delayed playback start. The pool
        then grows lazily to its full size under load. Timeout-bounded."""
        if self.client is None or not self.client.is_connected:
            return
        try:
            current_dc = await self.client.storage.dc_id()
        except Exception:
            current_dc = None
        for dc in (1, 2, 4, 5):
            if dc == current_dc:
                continue  # same-DC files use the main session (no new auth needed)
            try:
                await asyncio.wait_for(self._ensure_dc_session(dc), timeout=MEDIA_SESSION_TIMEOUT)
            except Exception:
                continue

    async def _ensure_dc_session(self, dc: int) -> Session:
        """Return a media session for dc, adding one to the pool if there's room."""
        p = self._pool(dc)
        async with p["lock"]:
            live = [s for s in p["sessions"] if getattr(s, "connected", True)]
            p["sessions"] = live
            if live and len(live) >= self._pool_size:
                # pick round-robin
                s = live[p["rr"] % len(live)]
                p["rr"] += 1
                return s
            if live:
                s = live[p["rr"] % len(live)]
                p["rr"] += 1
                # Have at least one; try to grow the pool opportunistically.
                if len(live) < self._pool_size:
                    try:
                        extra = await asyncio.wait_for(self._open_session(dc), timeout=MEDIA_SESSION_TIMEOUT)
                        p["sessions"].append(extra)
                        return extra
                    except Exception:
                        return s
                return s
            # No session yet — create one.
            try:
                session = await asyncio.wait_for(self._open_session(dc), timeout=MEDIA_SESSION_TIMEOUT)
            except asyncio.TimeoutError:
                raise ClientNotConnected(f"media session to DC {dc} timed out (account may be rate-limited)")
            except Exception as exc:
                raise ClientNotConnected(f"media session to DC {dc} failed: {type(exc).__name__}")
            p["sessions"].append(session)
            return session

    async def _get_media_session(self, fid: FileId) -> Session:
        dc = fid.dc_id
        # Fast path: reuse an established pooled connection (no lock).
        p = self._media_pool.get(dc)
        if p and p["sessions"]:
            live = p["sessions"]
            if len(live) > 1:
                s = live[p["rr"] % len(live)]
                p["rr"] += 1
                return s
            return live[0]
        return await self._ensure_dc_session(dc)

    def _drop_dead_session(self, dc: int, sess: Session) -> None:
        """Remove a dead media session so the pool rebuilds it on next use."""
        p = self._media_pool.get(dc)
        if not p:
            return
        p["sessions"] = [s for s in p["sessions"] if s is not sess]
        try:
            if sess is not None and getattr(sess, "connected", False):
                asyncio.create_task(_safe_stop(sess))
        except Exception:
            pass

    @staticmethod
    async def _get_location(fid: FileId):
        return raw.types.InputDocumentFileLocation(
            id=fid.media_id,
            access_hash=fid.access_hash,
            file_reference=fid.file_reference,
            thumb_size=fid.thumbnail_size,
        )

    # ------------------------------------------------------------------
    # Streaming (producer/consumer, range-aware, flood-adaptive)
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

        telemetry.bump("stream_requests")
        telemetry.bump("stream_started")
        stream_id = f"{id(self)}-{int(time.time() * 1000)}"
        entry = telemetry.register_stream(stream_id, {"title": getattr(fid, "file_name", "") or ""})

        async def refresh() -> bool:
            if not chat_id or not message_id:
                return False
            fresh = await self._refresh_properties(chat_id, message_id)
            if fresh:
                loc_box[0] = await self._get_location(fresh)
                return True
            return False

        async def fetch_chunk(seq: int, off: int):
            """Fetch one chunk; returns (seq, bytes) or (seq, None) if unrecoverable."""
            global _last_flood
            tries = 0
            flood_tries = 0
            sess = None
            while tries < 4 and flood_tries < _MAX_FLOOD_RETRIES and not stop_event.is_set():
                # Grab a pooled media session each attempt so chunks spread across
                # multiple TCP connections (higher per-DC throughput) and a dead
                # session is automatically replaced with a fresh one.
                try:
                    sess = await self._get_media_session(fid)
                    r = await asyncio.wait_for(
                        sess.send(
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
                    await asyncio.sleep(min(0.4 * (2 ** (tries - 1)), 6.0))
                except Exception as e:
                    err = str(e)
                    if "FILE_REFERENCE" in err or "file_reference" in err.lower():
                        await refresh()
                    m = re.search(r"wait of (\d+) second", err, re.IGNORECASE)
                    if m:
                        # FLOOD_WAIT = "slow down", not "stop". Record it (per
                        # client AND module-wide for the dashboard) and retry.
                        self._last_flood = time.time()
                        _last_flood = time.time()
                        telemetry.bump("flood_waits")
                        wait = float(m.group(1)) + random.uniform(0.5, 2.0)
                        flood_tries += 1
                        await asyncio.sleep(wait)
                    elif _session_dead_error(err):
                        # Connection/session died mid-stream — drop it so the next
                        # attempt opens a replacement from the pool, and retry.
                        self._drop_dead_session(fid.dc_id, sess)
                        tries += 1
                        await asyncio.sleep(min(0.4 * (2 ** (tries - 1)), 6.0))
                    else:
                        tries += 1
                        await asyncio.sleep(min(0.4 * (2 ** (tries - 1)), 6.0))
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
                # Adaptive: drop to 1 while a recent flood cools down, then ramp
                # back up (per-client, so bots and the user session are independent).
                max_parallel = max(1, parallelism)
                if time.time() - self._last_flood < _FLOOD_COOLDOWN:
                    max_parallel = 1

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
                    try:
                        if request is not None and await request.is_disconnected():
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
                        telemetry.note_chunk(entry, len(out))
                        yield out
                    part_idx += 1
            finally:
                telemetry.finish_stream(stream_id, "finished")
                stop_event.set()
                if not producer_task.done():
                    producer_task.cancel()
                    try:
                        await asyncio.wait_for(producer_task, timeout=2.0)
                    except (Exception, asyncio.CancelledError):
                        pass

        return consumer()
