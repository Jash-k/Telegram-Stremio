"""Single Telegram userbot client.

The session string grants access to the channels being indexed, so it powers
both the indexer AND the stream proxy (no bot token needed).

Also exposes session status for the health dashboard and supports the
background watchdog (reconnect with backoff).
"""
import asyncio
import time

from pyrogram import Client

from . import config
from .logger import LOGGER

client: Client = None
_start_lock = asyncio.Lock()

# Session state (read by the health dashboard + watchdog).
_state = {
    "connected": False,
    "dc_id": None,
    "username": None,
    "first_name": None,
    "last_error": None,
    "last_error_at": None,
    "last_connected_at": None,
    "reconnect_attempts": 0,
}


def build() -> Client:
    return Client(
        name="globaldb_userbot",
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        session_string=config.SESSION_STRING,
        sleep_threshold=20,
        workers=4,
        max_concurrent_transmissions=3,
        no_updates=False,
        in_memory=True,
    )


def _mark_connected(c) -> None:
    _state["connected"] = True
    _state["last_connected_at"] = time.time()
    _state["reconnect_attempts"] = 0
    _state["last_error"] = None
    _state["last_error_at"] = None
    try:
        _state["dc_id"] = getattr(c, "dc_id", None)
    except Exception:
        pass
    try:
        me = getattr(c, "me", None)
        _state["username"] = getattr(me, "username", None)
        _state["first_name"] = getattr(me, "first_name", None)
    except Exception:
        pass


def _mark_disconnected(exc: Exception = None) -> None:
    _state["connected"] = False
    if exc is not None:
        _state["last_error"] = f"{type(exc).__name__}: {exc}"
        _state["last_error_at"] = time.time()


async def start() -> Client:
    """Start the userbot (used at app startup). Raises on failure.

    The connection is gated on the session lease so a redeploy overlap can't
    race the previous pod and trigger AUTH_KEY_DUPLICATED.
    """
    global client
    from . import session_lease

    if not await session_lease.try_acquire():
        _mark_disconnected(RuntimeError("Session lease held by another instance"))
        raise RuntimeError("Session lease held by another instance")

    if client is None:
        client = build()
    if not client.is_connected:
        try:
            await client.start()
        except Exception as exc:
            await session_lease.release()
            _mark_disconnected(exc)
            raise
        await client.get_me()
        _mark_connected(client)
        session_lease.start_heartbeat()
        LOGGER.info("Userbot online as %s (@%s)", _state["first_name"] or "?", _state["username"] or "?")
    return client


async def ensure_started() -> Client | None:
    """Connect (or reconnect) the userbot if it isn't connected.

    Returns the connected client, or None if a connection cannot be
    established (e.g. session conflict, or the lease is held elsewhere).
    Never raises.
    """
    global client
    from . import session_lease

    if client is None:
        client = build()
    if client.is_connected:
        return client
    async with _start_lock:
        if client.is_connected:
            return client

        # Only connect if we own (or can claim) the lease.
        if not session_lease.is_held() and not await session_lease.try_acquire():
            _mark_disconnected(RuntimeError("Session lease held by another instance"))
            return None

        _state["reconnect_attempts"] += 1
        try:
            await client.start()
            await client.get_me()
            _mark_connected(client)
            session_lease.start_heartbeat()
            LOGGER.info("Userbot (re)connected as %s (@%s)", _state["first_name"] or "?", _state["username"] or "?")
            return client
        except Exception as exc:
            await session_lease.release()
            _mark_disconnected(exc)
            LOGGER.error("Userbot could not connect: %s", exc)
            return None


def is_connected() -> bool:
    return client is not None and client.is_connected


def session_status() -> dict:
    """Snapshot of session state for the health dashboard."""
    status = dict(_state)
    status["connected"] = is_connected()
    return status


async def stop() -> None:
    global client
    from . import session_lease

    if client is not None and client.is_connected:
        await client.stop()
    await session_lease.stop_heartbeat()
    await session_lease.release()
    client = None
    _state["connected"] = False
