"""Optional Telegram BOT clients for streaming specific channels.

Bots have a higher GetFile rate limit than user sessions, so routing heavy
(e.g. 4K) files through bots avoids flooding the user account. Multiple bots
load-balance across their independent rate limits.

Each bot serves only the channels in BOT_CHANNELS; everything else streams
through the user session.
"""
import asyncio
import itertools
import time

from pyrogram import Client

from . import config
from .logger import LOGGER
from .session_lease import SessionLease

_lock = asyncio.Lock()

# One entry per configured bot token.
_bots: list = []  # list of dict: {index, token, client, lease, state}
_rr = itertools.count()


def is_enabled() -> bool:
    return bool(config.BOT_TOKENS)


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
    """Whether the bot pool should stream the given chat."""
    return is_enabled() and int(chat_id) in channel_set()


def _build(token: str, index: int) -> Client:
    return Client(
        name=f"globaldb_bot_{index}",
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        bot_token=token,
        sleep_threshold=20,
        workers=4,
        max_concurrent_transmissions=3,
        no_updates=True,
        in_memory=True,
    )


def _fresh_state() -> dict:
    return {
        "connected": False,
        "dc_id": None,
        "username": None,
        "first_name": None,
        "last_error": None,
        "last_error_at": None,
        "last_connected_at": None,
        "reconnect_attempts": 0,
    }


async def _ensure_bots() -> list:
    """Build the bot list (lazily, once) from config.BOT_TOKENS."""
    global _bots
    if _bots:
        return _bots
    _bots = [
        {
            "index": i,
            "token": tok,
            "client": None,
            "lease": SessionLease(f"bot_session_{i}", f"bot{i}"),
            "state": _fresh_state(),
        }
        for i, tok in enumerate(config.BOT_TOKENS)
    ]
    return _bots


def _mark_connected(bot: dict, c: Client) -> None:
    st = bot["state"]
    st["connected"] = True
    st["last_connected_at"] = time.time()
    st["reconnect_attempts"] = 0
    st["last_error"] = None
    st["last_error_at"] = None
    try:
        st["dc_id"] = getattr(c, "dc_id", None)
    except Exception:
        pass
    try:
        me = getattr(c, "me", None)
        st["username"] = getattr(me, "username", None)
        st["first_name"] = getattr(me, "first_name", None)
    except Exception:
        pass


def _mark_disconnected(bot: dict, exc: Exception = None) -> None:
    st = bot["state"]
    st["connected"] = False
    if exc is not None:
        st["last_error"] = f"{type(exc).__name__}: {exc}"
        st["last_error_at"] = time.time()


async def _start_one(bot: dict) -> Client | None:
    """Connect one bot. Returns the client or None. Never raises."""
    if not await bot["lease"].try_acquire():
        _mark_disconnected(bot, RuntimeError("Bot lease held by another instance"))
        return None

    if bot["client"] is None:
        bot["client"] = _build(bot["token"], bot["index"])
    c = bot["client"]
    if c.is_connected:
        return c

    bot["state"]["reconnect_attempts"] += 1
    try:
        await c.start()
        await c.get_me()
        _mark_connected(bot, c)
        bot["lease"].start_heartbeat()
        LOGGER.info("Bot #%d online as @%s", bot["index"], bot["state"]["username"] or "?")
        return c
    except Exception as exc:
        await bot["lease"].release()
        _mark_disconnected(bot, exc)
        LOGGER.error("Bot #%d could not connect: %s", bot["index"], exc)
        return None


async def start() -> list:
    """Start all configured bots at app startup. Returns connected clients."""
    if not is_enabled():
        return []
    bots = await _ensure_bots()
    connected = []
    for bot in bots:
        try:
            c = await _start_one(bot)
            if c is not None:
                connected.append(c)
        except Exception as exc:
            _mark_disconnected(bot, exc)
            LOGGER.error("Bot #%d failed to start: %s", bot["index"], exc)
    return connected


async def connected_bots() -> list:
    """Return the connected bot clients (reconnect lazily where possible)."""
    if not is_enabled():
        return []
    bots = await _ensure_bots()
    out = []
    for bot in bots:
        c = bot["client"]
        if c is not None and c.is_connected:
            out.append(c)
        else:
            c = await _start_one(bot)
            if c is not None:
                out.append(c)
    return out


async def pick_bot(chat_id: int):
    """Pick a connected bot to stream `chat_id`, round-robin. None if all down."""
    bots = await connected_bots()
    if not bots:
        return None
    return bots[next(_rr) % len(bots)]


async def ensure_started():
    """Connect all bots (used by watchdog). Returns any connected client or None."""
    bots = await connected_bots()
    return bots[0] if bots else None


def is_connected() -> bool:
    return any(
        bot["client"] is not None and bot["client"].is_connected
        for bot in _bots
    )


def session_status() -> dict:
    """Snapshot of the bot pool for the health dashboard."""
    if not is_enabled():
        return None
    bots = _bots or [
        {"index": i, "state": _fresh_state(), "client": None}
        for i in range(len(config.BOT_TOKENS))
    ]
    return {
        "count": len(config.BOT_TOKENS),
        "connected": sum(1 for b in bots if b["client"] is not None and b["client"].is_connected),
        "channels": sorted(channel_set(), key=abs),
        "bots": [
            {
                "index": b["index"],
                "username": b["state"].get("username"),
                "first_name": b["state"].get("first_name"),
                "connected": b["client"] is not None and b["client"].is_connected,
                "last_error": b["state"].get("last_error"),
                "reconnect_attempts": b["state"].get("reconnect_attempts", 0),
            }
            for b in bots
        ],
    }


async def stop() -> None:
    for bot in _bots:
        c = bot["client"]
        if c is not None and c.is_connected:
            await c.stop()
        await bot["lease"].stop_heartbeat()
        await bot["lease"].release()
    _bots.clear()
