"""Small persistent log of admin/automation runs (cleanup, self-heal, sweeps).

One document, one $push per finished run — results that used to vanish into
the server log after the panel tab was closed. Never raises: the writers are
all best-effort and the reader degrades to an empty list.
"""
import time

from . import db
from .logger import LOGGER

COL = "ops_log"
DOC_ID = "recent"
MAX_ENTRIES = 15


def _friendly(v):
    """Keep only scalar, log-safe values in an entry."""
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return round(v, 2)
    if isinstance(v, str):
        return v[:160]
    return None


async def log_op(op_type: str, status: str, **counts) -> None:
    """Record one finished run. Fire-and-forget safe."""
    try:
        entry = {"type": op_type, "status": status, "at": time.time()}
        for k, v in counts.items():
            fv = _friendly(v)
            if fv is not None:
                entry[k] = fv
        await db.col(COL).update_one(
            {"_id": DOC_ID},
            {
                "$push": {"entries": {"$each": [entry], "$slice": -MAX_ENTRIES}},
                "$set": {"updated_at": entry["at"]},
            },
            upsert=True,
        )
    except Exception as exc:  # logging must never break the operation it logs
        LOGGER.debug("[OPSLOG] %s write failed: %s", op_type, exc)


async def recent_ops(limit: int = 10) -> list:
    """Most-recent-first entries; [] when the DB is down."""
    try:
        doc = await db.col(COL).find_one({"_id": DOC_ID}, {"entries": 1}) or {}
        entries = list(doc.get("entries") or [])
        entries.reverse()
        return entries[: max(1, min(limit, MAX_ENTRIES))]
    except Exception:
        return []
