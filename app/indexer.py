"""Resumable GlobalDB indexer.

Historic + incremental scans per channel, checkpointed in the `state`
collection so a scale-to-zero wake or crash resumes cleanly. A lightweight
lease in `state` prevents overlapping runs and lets another replica stop it.
"""
import asyncio
import re
import time
import uuid

import PTN
from pyrogram import enums
from pyrogram.errors import FloodWait

from . import config, db
from .logger import LOGGER
from .metadata import format_tmdb_image, tmdb_details, tmdb_search, year_number
from .parser import (
    clean_filename,
    determine_catalog,
    episode_bounds,
    extract_fallback_title_and_year,
    first_int,
    global_file_key,
    languages_from_filename,
    parse_combined_episodes,
)

_VIDEO_EXTS = (".mkv", ".mp4", ".avi", ".ts", ".m4v", ".mov", ".wmv", ".webm", ".flv")

_OWNER = uuid.uuid4().hex
_LEASE_SECONDS = 180
_JOB_ID = "global_indexer_job"

_running = False
_stop_requested = False
_task = None
_status = {
    "running": False,
    "stop_requested": False,
    "processed": 0,
    "current_chat": None,
    "current_filter": None,
    "last_error": None,
}


def readable_size(size_in_bytes) -> str:
    size_in_bytes = int(size_in_bytes or 0)
    units = ["B", "KB", "MB", "GB", "TB"]
    idx = 0
    while size_in_bytes >= 1024 and idx < len(units) - 1:
        size_in_bytes /= 1024
        idx += 1
    return f"{size_in_bytes:.2f}{units[idx]}" if idx else f"{size_in_bytes:.0f}B"


# ---------------------------------------------------------------------------
# Channel id resolution
# ---------------------------------------------------------------------------


def resolve_channel_ids(raw_ids) -> list[int]:
    resolved, seen = [], set()
    for c in raw_ids:
        c = str(c).strip()
        if not c:
            continue
        try:
            n = int(c)
        except ValueError:
            continue
        canonical = n if n < 0 else int(f"-100{n}")
        if canonical not in seen:
            seen.add(canonical)
            resolved.append(canonical)
    return resolved


_CHANNELS_DOC = "channels_config"


async def configured_channels() -> list[int]:
    """Channels the indexer should track.

    Merges env CHANNELS + persisted DB config + existing sync state + indexed files
    so no channel is ever missed or dropped.
    """
    ids = set()
    if config.CHANNELS:
        ids.update(resolve_channel_ids(config.CHANNELS))
    doc = await db.col("state").find_one({"_id": _CHANNELS_DOC})
    if doc and doc.get("channels"):
        ids.update(resolve_channel_ids(doc["channels"]))
    async for s in db.col("state").find({"_id": {"$regex": "^sync_"}}):
        try:
            ids.add(int(str(s["_id"]).split("_")[1]))
        except (ValueError, IndexError):
            continue
    async for f in db.col("files").aggregate([{"$group": {"_id": "$chat_id"}}]):
        try:
            if f["_id"]:
                raw_n = int(f["_id"])
                canon_n = raw_n if raw_n < 0 else int(f"-100{raw_n}")
                ids.add(canon_n)
        except (ValueError, IndexError):
            continue
    return sorted(ids, key=abs)


async def get_channel_config() -> list[int]:
    """Raw configured list (panel management view), or auto-derived."""
    doc = await db.col("state").find_one({"_id": _CHANNELS_DOC})
    if doc and doc.get("channels"):
        return resolve_channel_ids(doc["channels"])
    return await configured_channels()


async def add_channel(chat_id: int) -> bool:
    """Persist a channel id into the config list."""
    chat_id = int(chat_id)
    doc = await db.col("state").find_one({"_id": _CHANNELS_DOC}) or {}
    current = list(doc.get("channels", []))
    # normalize alongside existing entries
    all_ids = set(resolve_channel_ids([*current, chat_id]))
    await db.col("state").update_one(
        {"_id": _CHANNELS_DOC},
        {"$set": {"channels": sorted(all_ids, key=abs)}},
        upsert=True,
    )
    return True


async def remove_channel(chat_id: int) -> bool:
    """Remove a channel id from the config list (and its sync state)."""
    chat_id = int(chat_id)
    doc = await db.col("state").find_one({"_id": _CHANNELS_DOC}) or {}
    current = set(resolve_channel_ids(doc.get("channels", [])))
    current.discard(chat_id)
    if doc.get("channels"):
        await db.col("state").update_one(
            {"_id": _CHANNELS_DOC},
            {"$set": {"channels": sorted(current, key=abs)}},
        )
    # Clear its sync checkpoints so a re-add starts fresh.
    await db.col("state").delete_many({"_id": {"$regex": f"^sync_{chat_id}_"}})
    return True


# Backwards-compatible alias used elsewhere.
async def _configured_channels() -> list[int]:
    return await configured_channels()


# ---------------------------------------------------------------------------
# Video filename extraction
# ---------------------------------------------------------------------------


def video_filename(message):
    media = getattr(message, "video", None) or getattr(message, "document", None) or getattr(message, "animation", None)
    if not media:
        return None

    doc_name = getattr(media, "file_name", None) or ""
    caption = (getattr(message, "caption", None) or getattr(message, "text", None) or "").strip()

    # Prioritize real video file name ending with video extension
    if doc_name and any(doc_name.lower().endswith(ext) for ext in _VIDEO_EXTS):
        name = doc_name
    elif caption and any(ext in caption.lower() for ext in _VIDEO_EXTS):
        name = caption
    elif doc_name:
        name = doc_name
    elif caption:
        name = caption
    elif getattr(message, "video", None):
        name = "video.mkv"
    else:
        name = None

    if name:
        return clean_filename(name)
    return None


# ---------------------------------------------------------------------------
# Lease
# ---------------------------------------------------------------------------


async def _acquire_lease() -> bool:
    now = time.time()
    try:
        job = await db.col("state").find_one_and_update(
            {
                "_id": _JOB_ID,
                "$or": [
                    {"running": {"$ne": True}},
                    {"lease_until": {"$lte": now}},
                    {"lease_until": {"$exists": False}},
                    {"owner": _OWNER},
                ],
            },
            {
                "$set": {
                    "running": True,
                    "status": "running",
                    "owner": _OWNER,
                    "lease_until": now + _LEASE_SECONDS,
                    "started_at": now,
                    "finished_at": None,
                    "stop_requested": False,
                    "processed": 0,
                    "last_error": None,
                }
            },
            upsert=True,
            return_document=True,
        )
    except Exception:
        return False
    return bool(job and job.get("owner") == _OWNER)


async def _heartbeat(force: bool = False) -> bool:
    """Renew lease; returns True if a stop was requested (local or remote)."""
    global _stop_requested
    if _stop_requested:
        return True
    now = time.time()
    if not force and now - _heartbeat._last < 15:
        return False
    job = await db.col("state").find_one_and_update(
        {"_id": _JOB_ID, "running": True, "owner": _OWNER},
        {
            "$set": {
                "lease_until": now + _LEASE_SECONDS,
                "processed": _status["processed"],
                "current_chat": _status["current_chat"],
                "current_filter": _status["current_filter"],
            }
        },
        return_document=True,
    )
    _heartbeat._last = now
    if not job:
        _status["last_error"] = "Indexer lease lost."
        _stop_requested = True
        return True
    if job.get("stop_requested"):
        _stop_requested = True
        return True
    return False


_heartbeat._last = 0.0


async def _release(final_status: str, processed: int) -> None:
    await db.col("state").update_one(
        {"_id": _JOB_ID, "owner": _OWNER},
        {
            "$set": {
                "running": False,
                "status": final_status,
                "stop_requested": False,
                "processed": processed,
                "last_error": _status.get("last_error"),
                "finished_at": time.time(),
                "lease_until": time.time(),
            }
        },
    )


# ---------------------------------------------------------------------------
# Scheduling / status
# ---------------------------------------------------------------------------


def schedule_index(force_historic: bool = False, target_chat_id=None) -> bool:
    global _running, _stop_requested, _task
    if _running or (_task is not None and not _task.done()):
        return False
    _running = True
    _stop_requested = False
    _status.update({"running": True, "stop_requested": False, "processed": 0, "current_chat": None, "current_filter": None, "last_error": None})
    _task = asyncio.create_task(_run(force_historic, target_chat_id))
    return True


async def request_stop() -> bool:
    global _stop_requested
    if _running:
        _stop_requested = True
        _status["stop_requested"] = True
    result = await db.col("state").update_one(
        {"_id": _JOB_ID, "running": True}, {"$set": {"stop_requested": True}}
    )
    return _running or bool(result.modified_count)


def status() -> dict:
    return dict(_status)


# ---------------------------------------------------------------------------
# Indexing core
# ---------------------------------------------------------------------------


async def log_unindexed(file_key, filename, size, chat_id, message_id, reason, title="", year=""):
    doc = {
        "_id": file_key,
        "filename": filename,
        "size": size,
        "size_str": readable_size(size),
        "chat_id": int(chat_id),
        "message_id": int(message_id),
        "reason": reason,
        "parsed_title": title or "",
        "parsed_year": year or "",
        "updated_at": time.time(),
    }
    await db.col("unindexed").update_one({"_id": file_key}, {"$set": doc}, upsert=True)


async def remove_file_reference(chat_id, message_id) -> int:
    file_key = global_file_key(chat_id, message_id)
    existing = await db.col("files").find_one(
        {"$or": [{"_id": file_key}, {"chat_id": {"$in": [int(chat_id), str(chat_id)]}, "message_id": int(message_id)}]},
        {"meta_id": 1},
    )
    deleted = await db.col("files").delete_one(
        {"$or": [{"_id": file_key}, {"chat_id": {"$in": [int(chat_id), str(chat_id)]}, "message_id": int(message_id)}]}
    )
    await db.col("unindexed").delete_one({"_id": file_key})
    meta_id = (existing or {}).get("meta_id")
    if meta_id and not await db.col("files").find_one({"meta_id": meta_id}, {"_id": 1}):
        await db.col("meta").delete_one({"_id": meta_id})
    return deleted.deleted_count


async def _process_message(chat_id: int, message) -> str | None:
    try:
        media = getattr(message, "video", None) or getattr(message, "document", None) or getattr(message, "animation", None)
        if not media:
            return None
        
        file_key = global_file_key(chat_id, message.id)
        size = getattr(media, "file_size", 0) or 0
        filename = video_filename(message)
        if not filename:
            raw_name = getattr(media, "file_name", None) or getattr(message, "caption", None) or "unnamed_video"
            await log_unindexed(file_key, raw_name, size, chat_id, message.id, "Non-Video / Unsupported Media Format")
            return None

        try:
            parsed = PTN.parse(filename)
        except Exception:
            parsed = {}

        title = parsed.get("title")
        year = parsed.get("year")

        # If PTN failed or parsed a generic language/edition keyword (e.g. DC, Tamil, Director's Cut)
        generic_words = {"tamil", "telugu", "hindi", "malayalam", "kannada", "english", "multi", "director's cut", "directors cut", "extended", "remastered", "unrated"}
        if not title or str(title).strip().lower() in generic_words or len(str(title).strip()) < 2:
            fb_title, fb_year = extract_fallback_title_and_year(filename)
            if fb_title:
                title = fb_title
                if not year and fb_year:
                    year = fb_year

        if not title:
            await log_unindexed(file_key, filename, size, chat_id, message.id, "No Title Found", title, year)
            return None

        combined = parse_combined_episodes(filename)
        season = first_int(parsed.get("season"))
        ep_start, ep_end = episode_bounds(parsed.get("episode"))
        media_type = "series" if (season is not None or ep_start is not None or combined) else "movie"
        tmdb_type = "tv" if media_type == "series" else "movie"

        res = await tmdb_search(title, tmdb_type, year)
        if not res and year is not None:
            res = await tmdb_search(title, tmdb_type, None)
        if not res:
            await log_unindexed(file_key, filename, size, chat_id, message.id, "TMDb Match Failed", title, year)
            return None

        tmdb_id = res["id"]
        details = await tmdb_details(tmdb_type, tmdb_id)
        if not details:
            await log_unindexed(file_key, filename, size, chat_id, message.id, "TMDb Details Failed", title, year)
            return None

        catalog = determine_catalog(details, media_type, filename)
        doc_id = f"tmdb:{tmdb_id}"
        external = details.get("external_ids") or {}
        imdb_id = external.get("imdb_id")
        year_number_ = year_number(details, media_type)
        aliases = [doc_id] + ([imdb_id] if imdb_id else [])

        update_data = {
            "tmdb_id": int(tmdb_id),
            "imdb_id": imdb_id,
            "aliases": aliases,
            "title": details.get("title") or details.get("name") or "",
            "year": year_number_,
            "poster": format_tmdb_image(details.get("poster_path")),
            "background": format_tmdb_image(details.get("backdrop_path"), "original"),
            "description": details.get("overview") or "",
            "media_type": media_type,
            "catalog": catalog,
            "genres": [g.get("name") for g in (details.get("genres") or [])],
            "rating": details.get("vote_average", 0.0),
            "updated_at": time.time(),
        }

        languages = languages_from_filename(filename)
        if details.get("original_language") == "ta" and "Tamil" not in languages:
            languages.append("Tamil")

        await db.col("meta").update_one(
            {"_id": doc_id},
            {"$set": update_data, "$addToSet": {"languages": {"$each": languages}}},
            upsert=True,
        )

        old_file = await db.col("files").find_one({"_id": file_key}, {"meta_id": 1})
        file_data = {
            "_id": file_key,
            "meta_id": doc_id,
            "filename": filename,
            "size": size,
            "size_str": readable_size(size),
            "quality": parsed.get("resolution") or "HD",
            # Pre-computed technical metadata (no PTN re-parse needed at stream time).
            "codec": parsed.get("codec") or "",
            "audio": parsed.get("audio") or "",
            "resolution": parsed.get("resolution") or "",
            "chat_id": int(chat_id),
            "message_id": int(message.id),
            "season": first_int(combined["season"]) if combined else season,
            "episode_start": first_int(combined["start"]) if combined else ep_start,
            "episode_end": first_int(combined["end"]) if combined else ep_end,
            "indexed_at": time.time(),
        }
        await db.col("files").update_one({"_id": file_key}, {"$set": file_data}, upsert=True)
        await db.col("unindexed").delete_one({"_id": file_key})

        old_meta_id = (old_file or {}).get("meta_id")
        if old_meta_id and old_meta_id != doc_id:
            if not await db.col("files").find_one({"meta_id": old_meta_id}, {"_id": 1}):
                await db.col("meta").delete_one({"_id": old_meta_id})
        return doc_id
    except Exception as exc:
        LOGGER.error(f"[INDEXER] Exception processing message {getattr(message, 'id', '?')} in {chat_id}: {exc}")
        return None


async def _unprocessed(chat_id: int, messages) -> list:
    if not messages:
        return []
    by_id = {global_file_key(chat_id, m.id): m for m in messages}
    keys = list(by_id)
    existing = set()
    async for row in db.col("files").find({"_id": {"$in": keys}}, {"_id": 1}):
        existing.add(row["_id"])
    async for row in db.col("unindexed").find({"_id": {"$in": keys}}, {"_id": 1}):
        existing.add(row["_id"])
    return [m for k, m in by_id.items() if k not in existing]


async def _scan_channel(client, chat_id: int, force_historic: bool, total: dict) -> None:
    for msg_filter in (enums.MessagesFilter.VIDEO, enums.MessagesFilter.DOCUMENT):
        if _stop_requested:
            return
        _status["current_filter"] = msg_filter.name
        sync_key = f"sync_{chat_id}_{msg_filter.name}"
        sync = await db.col("state").find_one({"_id": sync_key}) or {}

        historic_done = False if force_historic else sync.get("historic_done", False)
        last_id = sync.get("last_id", 0)
        offset_id = sync.get("historic_offset_id", 0)

        if not historic_done:
            LOGGER.info(f"[INDEXER] {chat_id} {msg_filter.name}: historic scan from {offset_id}")
            highest_seen = last_id
            try:
                batch = []
                async for msg in client.search_messages(chat_id, filter=msg_filter):
                    if _stop_requested:
                        break
                    if offset_id > 0 and msg.id >= offset_id:
                        continue
                    highest_seen = max(highest_seen, msg.id)
                    batch.append(msg)
                    if len(batch) < 100:
                        continue
                    for candidate in (batch if force_historic else await _unprocessed(chat_id, batch)):
                        if _stop_requested:
                            break
                        try:
                            await _process_message(chat_id, candidate)
                        except Exception as p_err:
                            LOGGER.error(f"[INDEXER] error processing msg {getattr(candidate, 'id', '?')} in {chat_id}: {p_err}")
                        total["processed"] += 1
                    _status["processed"] = total["processed"]
                    await db.col("state").update_one(
                        {"_id": sync_key},
                        {"$set": {"historic_offset_id": batch[-1].id, "last_id": highest_seen}},
                        upsert=True,
                    )
                    batch = []
                if batch and not _stop_requested:
                    for candidate in (batch if force_historic else await _unprocessed(chat_id, batch)):
                        try:
                            await _process_message(chat_id, candidate)
                        except Exception as p_err:
                            LOGGER.error(f"[INDEXER] error processing msg {getattr(candidate, 'id', '?')} in {chat_id}: {p_err}")
                        total["processed"] += 1
                    _status["processed"] = total["processed"]
                if not _stop_requested:
                    await db.col("state").update_one(
                        {"_id": sync_key},
                        {"$set": {"historic_done": True, "historic_offset_id": 0, "last_id": highest_seen}},
                        upsert=True,
                    )
                    LOGGER.info(f"[INDEXER] {chat_id} {msg_filter.name}: historic complete")
            except FloodWait as fw:
                _status["last_error"] = f"FloodWait {chat_id} (resumable)"
                await asyncio.sleep(getattr(fw, "value", 5))
            except Exception as exc:
                _status["last_error"] = f"{type(exc).__name__}: {exc}"
                LOGGER.error(f"[INDEXER] historic error {chat_id}: {exc}")
        else:
            LOGGER.info(f"[INDEXER] {chat_id} {msg_filter.name}: incremental after {last_id}")
            highest_seen = last_id
            try:
                async for msg in client.search_messages(chat_id, filter=msg_filter):
                    if _stop_requested:
                        break
                    if msg.id <= last_id:
                        break
                    highest_seen = max(highest_seen, msg.id)
                    try:
                        await _process_message(chat_id, msg)
                    except Exception as p_err:
                        LOGGER.error(f"[INDEXER] error processing incremental msg {getattr(msg, 'id', '?')} in {chat_id}: {p_err}")
                    total["processed"] += 1
                    _status["processed"] = total["processed"]
                if highest_seen > last_id:
                    await db.col("state").update_one(
                        {"_id": sync_key}, {"$set": {"last_id": highest_seen}}, upsert=True
                    )
            except FloodWait as fw:
                _status["last_error"] = f"FloodWait {chat_id} (resumable)"
                await asyncio.sleep(getattr(fw, "value", 5))
            except Exception as exc:
                _status["last_error"] = f"{type(exc).__name__}: {exc}"
                LOGGER.error(f"[INDEXER] incremental error {chat_id}: {exc}")


async def _run(force_historic: bool, target_chat_id=None) -> None:
    global _running, _stop_requested, _task
    from .client import client

    total = {"processed": 0}
    final_status = "completed"
    try:
        if client is None:
            _status["last_error"] = "Userbot not configured"
            final_status = "failed"
            return
        if not await _acquire_lease():
            _status["last_error"] = "Another indexer owns the lease"
            final_status = "failed"
            return

        await _heartbeat(force=True)
        if target_chat_id:
            targets = [int(target_chat_id)]
        else:
            targets = await _configured_channels()
        LOGGER.info(f"[INDEXER] Started for {len(targets)} channel(s)")

        for chat_id in targets:
            if _stop_requested:
                break
            _status["current_chat"] = chat_id
            try:
                await _scan_channel(client, chat_id, force_historic, total)
            except Exception as exc:
                _status["last_error"] = f"channel {chat_id}: {type(exc).__name__}: {exc}"
                LOGGER.error(f"[INDEXER] channel error {chat_id}: {exc}")

        if _stop_requested:
            final_status = "stopped"
        elif _status.get("last_error"):
            final_status = "failed"
        LOGGER.info(f"[INDEXER] Finished ({final_status}), processed {total['processed']}")
    except Exception as exc:
        _status["last_error"] = f"{type(exc).__name__}: {exc}"
        final_status = "failed"
        LOGGER.error(f"[INDEXER] fatal: {exc}")
    finally:
        await _release(final_status, total["processed"])
        _running = False
        _stop_requested = False
        _task = None
        _status.update({"running": False, "status": final_status, "stop_requested": False, "current_chat": None, "current_filter": None})


# ---------------------------------------------------------------------------
# Background Periodic Sync Loop
# ---------------------------------------------------------------------------
_bg_sync_task: asyncio.Task | None = None
_bg_sync_stop_event = asyncio.Event()


async def _background_sync_loop(interval_seconds: int = 300) -> None:
    """Periodically syncs all tracked channels in the background every 5 minutes."""
    LOGGER.info(f"[INDEXER] Global background sync worker started (interval: {interval_seconds}s)")
    # Initial pause for client bootstrap
    try:
        await asyncio.sleep(20)
    except asyncio.CancelledError:
        return

    while not _bg_sync_stop_event.is_set():
        try:
            if not _running:
                chans = await configured_channels()
                if chans:
                    LOGGER.info(f"[INDEXER] Global background periodic sync cycle running for {len(chans)} channel(s)...")
                    schedule_index(force_historic=False)
        except Exception as exc:
            LOGGER.error(f"[INDEXER] Global background sync worker error: {exc}")

        try:
            await asyncio.wait_for(_bg_sync_stop_event.wait(), timeout=interval_seconds)
        except asyncio.TimeoutError:
            pass
        except asyncio.CancelledError:
            break


def start_background_watcher(interval_seconds: int = 300) -> None:
    """Start the Global channel background watcher/sync loop."""
    global _bg_sync_task, _bg_sync_stop_event
    if _bg_sync_task is not None and not _bg_sync_task.done():
        return
    _bg_sync_stop_event.clear()
    _bg_sync_task = asyncio.create_task(_background_sync_loop(interval_seconds))


def stop_background_watcher() -> None:
    """Stop the Global channel background watcher/sync loop."""
    global _bg_sync_task, _bg_sync_stop_event
    _bg_sync_stop_event.set()
    if _bg_sync_task and not _bg_sync_task.done():
        _bg_sync_task.cancel()
