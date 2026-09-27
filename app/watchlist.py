"""Watchlist: user pins a title (/watch add X, or the panel), and the indexer
DMs the userbot's self-chat the first time a file for that title indexes.

Design rules that matter here:
  * The matching runs inside the index hot path, so it reads an in-memory
    snapshot (refreshed via a short TTL) — zero DB queries per file.
  * Everything is best-effort: a dead DB or a dead session changes nothing
    about indexing success.
  * A hit removes the entry (one-shot watch) so a pack of 40 files for one
    title sends one DM, not forty.
"""
import re
import time

from . import db
from .cache import TTLCache
from .logger import LOGGER

COL = "watchlist"

_cache = TTLCache()
_SNAP_KEY = "watchlist_snapshot"
_SNAP_TTL = 300  # seconds — new watches appear within 5 min of being added


def normalize_title(title: str) -> str:
    t = (title or "").lower()
    t = re.sub(r"\(\s*\d{4}\s*\)", " ", t)          # drop (2024)
    t = re.sub(r"\[[^\]]*\]", " ", t)               # drop [stuff]
    t = re.sub(r"[^a-z0-9]+", " ", t)               # keep letters/digits only
    return re.sub(r"\s+", " ", t).strip()


def _doc_to_watch(doc: dict) -> dict:
    return {
        "_id": doc.get("_id"),
        "title": doc.get("title") or doc.get("_id"),
        "created_at": doc.get("created_at"),
    }


async def _snapshot() -> dict:
    """{norm_title: watch_doc} — 5-minute cached; empty when DB down."""
    cached = _cache.get(_SNAP_KEY)
    if cached is not None:
        return cached
    snap = {}
    try:
        async for doc in db.col(COL).find({}, {"title": 1, "created_at": 1}):
            key = normalize_title(doc.get("title") or doc.get("_id") or "")
            if key:
                snap[key] = doc
            if len(snap) >= 500:
                break
    except Exception as exc:
        LOGGER.debug("[WATCH] snapshot load failed: %s", exc)
    _cache.set(_SNAP_KEY, snap, _SNAP_TTL)
    return snap


def invalidate() -> None:
    _cache.clear()


async def add(title: str) -> dict:
    title = (title or "").strip()[:160]
    if not title:
        return {"ok": False, "detail": "Usage: /watch add <title>"}
    key = normalize_title(title)
    if not key:
        return {"ok": False, "detail": "Title has no searchable characters."}
    doc = {"_id": key, "title": title, "created_at": time.time()}
    try:
        await db.col(COL).update_one({"_id": key}, {"$set": doc}, upsert=True)
        invalidate()
        return {"ok": True, "detail": f"Watching “{title}”. You'll get a DM when it lands."}
    except Exception as exc:
        return {"ok": False, "detail": f"Could not save watch (DB?): {exc}"}


async def remove(title: str) -> dict:
    key = normalize_title(title)
    if not key:
        return {"ok": False, "detail": "Usage: /watch del <title>"}
    try:
        res = await db.col(COL).delete_one({"_id": key})
        invalidate()
        if res.deleted_count:
            return {"ok": True, "detail": f"Removed watch “{title}”."}
        return {"ok": False, "detail": f"No watch for “{title}”."}
    except Exception as exc:
        return {"ok": False, "detail": f"Could not remove watch (DB?): {exc}"}


async def list_watch() -> list:
    try:
        docs = await db.col(COL).find({}, {"title": 1, "created_at": 1}).to_list(200)
    except Exception:
        return []
    out = [_doc_to_watch(d) for d in docs]
    out.sort(key=lambda d: d.get("created_at") or 0, reverse=True)
    return out


async def check_landed(title: str, filename: str, size_str: str) -> bool:
    """Called by the indexer after a SUCCESSFUL index. Returns True on a hit."""
    try:
        snap = await _snapshot()
        if not snap:
            return False
        # Match against both the resolved title and the raw filename so a
        # watch like " Leo " hits whether the meta title or a weird release
        # name carries it.
        for candidate in (title or "", filename or ""):
            norm = normalize_title(candidate)
            if not norm:
                continue
            doc = snap.get(norm)
            if doc is None:
                # containment pass: short watches hit longer titles
                for key, d in snap.items():
                    if len(key) >= 4 and key in norm:
                        doc = d
                        break
            if doc is not None:
                await db.col(COL).delete_one({"_id": doc.get("_id")})
                invalidate()
                line = (
                    f"✅ {doc.get('title') or norm} just landed\n"
                    f"📁 {(filename or '')[:200]}\n💾 {size_str or '—'}"
                )
                try:
                    from .client import client

                    if client is not None and client.is_connected:
                        await client.send_message("me", line)
                except Exception as exc:
                    LOGGER.debug("[WATCH] DM failed: %s", exc)
                return True
        return False
    except Exception as exc:
        LOGGER.debug("[WATCH] check failed (ignored): %s", exc)
        return False
