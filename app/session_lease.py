"""Distributed session leases.

Prevents AUTH_KEY_DUPLICATED during Koyeb redeploys. All instances share the
same MongoDB (`dbFyvio`), so it's the natural single source of truth.

Each Telegram client (user session AND bot token) gets its own lease, because
they are independent auth keys and must not race across redeploys.

How it works (per lease):
  * Before a client is allowed to connect, the instance must atomically claim
    its lease (`state.{lease_id}`).
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

_TTL = 45.0          # lease lifetime if the owner dies without releasing
_HEARTBEAT = 10.0    # renew interval while connected


def _now() -> float:
    return time.time()


def _hostname() -> str:
    import socket

    try:
        return socket.gethostname()
    except Exception:
        return "unknown"


class SessionLease:
    """One named lease for one Telegram client (user session or bot token)."""

    def __init__(self, lease_id: str, label: str = ""):
        self._lease_id = lease_id
        self._label = label or lease_id
        self._owner = uuid.uuid4().hex
        self._held = False
        self._heartbeat_task = None

    def is_held(self) -> bool:
        return self._held

    async def try_acquire(self) -> bool:
        """Atomically claim the lease. True only if THIS process owns it."""
        if db is None:
            return True  # no DB configured -> can't coordinate, allow local run

        now = _now()
        try:
            doc = await db.col("state").find_one_and_update(
                {
                    "_id": self._lease_id,
                    "$or": [
                        {"owner": {"$exists": False}},
                        {"owner": self._owner},
                        {"expires_at": {"$lte": now}},
                    ],
                },
                {
                    "$set": {
                        "owner": self._owner,
                        "expires_at": now + _TTL,
                        "host": _hostname(),
                        "updated_at": now,
                    },
                    "$setOnInsert": {"_id": self._lease_id},
                },
                upsert=True,
                return_document=True,
            )
        except Exception:
            # DuplicateKeyError: lease exists and is held by a live foreign
            # owner — we don't own it.
            return False
        if doc and doc.get("owner") == self._owner:
            self._held = True
            LOGGER.info("[LEASE %s] acquired by %s", self._label, _hostname())
            return True
        return False

    async def heartbeat(self) -> None:
        if not self._held:
            return
        await db.col("state").update_one(
            {"_id": self._lease_id, "owner": self._owner},
            {"$set": {"expires_at": _now() + _TTL, "updated_at": _now()}},
        )

    async def release(self) -> None:
        if not self._held:
            return
        try:
            await db.col("state").update_one(
                {"_id": self._lease_id, "owner": self._owner},
                {"$set": {"owner": None, "expires_at": 0, "updated_at": _now()}},
            )
        except Exception as exc:
            LOGGER.warning("[LEASE %s] release failed: %s", self._label, exc)
        self._held = False

    async def _heartbeat_loop(self) -> None:
        while self._held:
            await asyncio.sleep(_HEARTBEAT)
            if not self._held:
                break
            await self.heartbeat()

    def start_heartbeat(self) -> None:
        if self._heartbeat_task is None or self._heartbeat_task.done():
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def stop_heartbeat(self) -> None:
        if self._heartbeat_task is not None and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
        self._heartbeat_task = None
