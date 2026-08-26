"""Optional Telegram BOT client for streaming specific channels.

A bot token has a higher GetFile rate limit than a user session, so routing
heavy (e.g. 4K) files through the bot avoids flooding the user account. The
bot only serves the channels listed in BOT_CHANNELS — everything else streams
through the user session.
"""
import asyncio
import time

from pyrogram import Client

from . import config
from .logger import LOGGER
from .session_lease import SessionLease

client: Client = None
_start_lock = asyncio.Lock()
_lease = SessionLease("bot_session", "bot")

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


def is_enabled() -> bool:
    return bool(config.BOT_TOKEN)


def _resolve(ids) -> set:
    """Normalize channel ids to full -100 form (matches the stream token)."""
    out = set()
    for c in ids:
        c = str(c).strip()
        if not c:
            continue
        try:
            n = int(c)
        except ValueError:
            continue
        out.add(n if n < 0 else int(f"-100{n}"))
    return out


def channel_set() -> set:
    return _resolve(config.BOT_CHANNELS)


def serves_chat(chat_id: int) -> bool:
    """Whether this bot should stream the given chat."""
    return is_enabled() and int(chat_id) in channel_set()


def build() -> Client:
    return Client(
        name="globaldb_bot",
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        bot_token=config.BOT_TOKEN,
        sleep_threshold=20,
        workers=4,
        max_concurrent_transmissions=3,
        no_updates=True,
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
    """Start the bot (used at app startup). Raises on failure."""
    global client
    if not is_enabled():
        raise RuntimeError("BOT_TOKEN not configured")

    if not await _lease.try_acquire():
        _mark_disconnected(RuntimeError("Bot lease held by another instance"))
        raise RuntimeError("Bot lease held by another instance")

    if client is None:
        client = build()
    if not client.is_connected:
        try:
            await client.start()
        except Exception as exc:
            await _lease.release()
            _mark_disconnected(exc)
            raise
        await client.get_me()
        _mark_connected(client)
        _lease.start_heartbeat()
        LOGGER.info("Bot online as @%s", _state["username"] or "?")
    return client


async def ensure_started() -> Client | None:
    """Connect (or reconnect) the bot if it isn't connected. Never raises."""
    global client
    if not is_enabled():
        return None
    if client is None:
        client = build()
    if client.is_connected:
        return client
    async with _start_lock:
        if client.is_connected:
            return client
        if not _lease.is_held() and not await _lease.try_acquire():
            _mark_disconnected(RuntimeError("Bot lease held by another instance"))
            return None
        _state["reconnect_attempts"] += 1
        try:
            await client.start()
            await client.get_me()
            _mark_connected(client)
            _lease.start_heartbeat()
            LOGGER.info("Bot (re)connected as @%s", _state["username"] or "?")
            return client
        except Exception as exc:
            await _lease.release()
            _mark_disconnected(exc)
            LOGGER.error("Bot could not connect: %s", exc)
            return None


def is_connected() -> bool:
    return client is not None and client.is_connected


def session_status() -> dict:
    """Snapshot of bot state for the health dashboard."""
    status = dict(_state)
    status["connected"] = is_connected()
    status["channels"] = sorted(channel_set(), key=abs)
    return status


async def stop() -> None:
    global client
    if client is not None and client.is_connected:
        await client.stop()
    await _lease.stop_heartbeat()
    await _lease.release()
    client = None
    _state["connected"] = False
