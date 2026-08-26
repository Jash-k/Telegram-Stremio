"""Distributed session lease.

Prevents AUTH_KEY_DUPLICATED during Koyeb redeploys. All instances share the
same MongoDB (`dbFyvio`), so it's the natural single source of truth.

How it works:
  * Before the userbot is allowed to connect, the instance must atomically
    claim the lease (`state.session_owner`).
  * While connected, a heartbeat renews the lease every few seconds.
  * On graceful shutdown the lease is released immediately, so the next pod
    takes over cleanly.
  * If a pod dies without releasing, the lease auto-expires (~45 s) and the
    survivor can take over.

A pod that does NOT hold the lease never calls client.start(), so it cannot
race the real owner and trigger the duplicate-auth error.
"""
import asyncio
import time
import uuid

from app import db
from app.logger import LOGGER

_LEASE_ID = "userbot_session"
_OWNER = uuid.uuid4().hex
_TTL = 45.0          # lease lifetime if the owner dies without releasing
_HEARTBEAT = 10.0    # renew interval while connected

_held = False
_heartbeat_task = None


def _now() -> float:
    return time.time()


def is_held() -> bool:
    return _held


async def try_acquire() -> bool:
    """Atomically claim the lease. Returns True only if THIS process owns it."""
    global _held
    if db is None:
        return True  # no DB configured -> can't coordinate, allow local run

    now = _now()
    try:
        doc = await db.col("state").find_one_and_update(
            {
                "_id": _LEASE_ID,
                "$or": [
                    {"owner": {"$exists": False}},
                    {"owner": _OWNER},
                    {"expires_at": {"$lte": now}},
                ],
            },
            {
                "$set": {
                    "owner": _OWNER,
                    "expires_at": now + _TTL,
                    "host": _hostname(),
                    "updated_at": now,
                },
                "$setOnInsert": {"_id": _LEASE_ID},
            },
            upsert=True,
            return_document=True,
        )
    except Exception:
        # DuplicateKeyError: the lease exists and is held by a live foreign
        # owner — we don't own it.
        return False
    if doc and doc.get("owner") == _OWNER:
        _held = True
        LOGGER.info("[SESSION LEASE] acquired by %s", _hostname())
        return True
    return False


async def heartbeat() -> None:
    if not _held:
        return
    await db.col("state").update_one(
        {"_id": _LEASE_ID, "owner": _OWNER},
        {"$set": {"expires_at": _now() + _TTL, "updated_at": _now()}},
    )


async def release() -> None:
    global _held
    if not _held:
        return
    try:
        await db.col("state").update_one(
            {"_id": _LEASE_ID, "owner": _OWNER},
            {"$set": {"owner": None, "expires_at": 0, "updated_at": _now()}},
        )
    except Exception as exc:
        LOGGER.warning("[SESSION LEASE] release failed: %s", exc)
    _held = False


async def _heartbeat_loop() -> None:
    while _held:
        await asyncio.sleep(_HEARTBEAT)
        if not _held:
            break
        await heartbeat()


def start_heartbeat() -> None:
    global _heartbeat_task
    if _heartbeat_task is None or _heartbeat_task.done():
        _heartbeat_task = asyncio.create_task(_heartbeat_loop())


async def stop_heartbeat() -> None:
    global _heartbeat_task
    if _heartbeat_task is not None and not _heartbeat_task.done():
        _heartbeat_task.cancel()
    _heartbeat_task = None


def _hostname() -> str:
    import socket

    try:
        return socket.gethostname()
    except Exception:
        return "unknown"
