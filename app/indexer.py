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
from .metadata import (
    format_tmdb_image,
    media_fields_from_details,
    tmdb_details,
    tmdb_search,
    tmdb_search_multi,
    year_number,
)
from .parser import (
    analyze_episodes,
    clean_filename,
    clean_movie_title,
    determine_catalog,
    episode_bounds,
    extract_fallback_title_and_year,
    first_int,
    global_file_key,
    languages_from_filename,
    parse_combined_episodes,
    series_title_candidates,
    series_title_match,
    source_from_filename,
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
    # v17 session counters (in-memory, zero DB cost) for the Ops Board.
    "indexed_total": 0,
    "last_indexed_ts": None,
}


async def last_sweep() -> dict:
    """Most recent finished index run, persisted by _release in `state`."""
    try:
        if not db.is_connected():
            return {}
        doc = await db.col("state").find_one(
            {"_id": _JOB_ID},
            {"status": 1, "processed": 1, "finished_at": 1, "last_error": 1},
        )
        return doc or {}
    except Exception:
        return {}


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


# ---------------------------------------------------------------------------
# Cached configured-channel set (hot path for live update filters)
# ---------------------------------------------------------------------------
#
# Live Telegram handlers must answer "is this one of my media channels?" for
# EVERY incoming update. Resolving that via configured_channels() (which runs a
# full-collection aggregation over all files) on each message is far too
# expensive — a busy leech supergroup emits hundreds of progress-edits a
# minute, and each one used to trigger a DB scan. Cache the canonical id set.
_CHANNEL_CACHE_TTL = 60.0
_channel_cache: dict = {"ids": None, "expires": 0.0}


async def configured_channel_ids(force_refresh: bool = False) -> set[int]:
    """Canonical (-100…) media-channel ids, cached for a few seconds."""
    now = time.time()
    if (
        not force_refresh
        and _channel_cache["ids"] is not None
        and now < _channel_cache["expires"]
    ):
        return _channel_cache["ids"]
    ids = set(await configured_channels())
    _channel_cache["ids"] = ids
    _channel_cache["expires"] = now + _CHANNEL_CACHE_TTL
    return ids


def invalidate_channel_cache() -> None:
    """Drop the cached channel set (call after add/remove)."""
    _channel_cache["ids"] = None
    _channel_cache["expires"] = 0.0


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
    invalidate_channel_cache()
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
    invalidate_channel_cache()
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
    media = getattr(message, "video", None) or getattr(message, "document", None) or getattr(message, "animation", None)
    if not media:
        return None
    size = int(getattr(media, "file_size", 0) or 0)
    filename = video_filename(message)
    if not filename:
        file_key = global_file_key(chat_id, message.id)
        raw_name = getattr(media, "file_name", None) or getattr(message, "caption", None) or "unnamed_video"
        await log_unindexed(file_key, raw_name, size, chat_id, message.id, "Non-Video / Unsupported Media Format")
        return None
    return await index_filename(chat_id, int(message.id), filename, size)


async def index_filename(chat_id: int, message_id: int, filename: str, size: int) -> str | None:
    """Index ONE video into GlobalDB from its stored filename/size.

    Shared by the live hook, the history scan, the startup repair pass and
    manual retries — the Telegram message itself is never re-fetched.
    """
    try:
        file_key = global_file_key(chat_id, message_id)
        size = int(size or 0)

        try:
            parsed = PTN.parse(filename)
        except Exception:
            parsed = {}

        raw_ptn_title = parsed.get("title")
        year = parsed.get("year")

        # PTN keeps source-site tokens (www, 1TamilMV, TamilBlasters…) and the
        # uploader/ripper handle (meme, ing, Pizza…) in the title. Strip them so
        # TMDb search sees only the real movie name.
        title = clean_movie_title(raw_ptn_title) if raw_ptn_title else ""

        # If PTN failed, produced only a generic keyword (Tamil, Director's Cut),
        # a bare 4-digit year, or nothing left after cleaning — use the fallback
        # parser on the raw filename.
        generic_words = {"tamil", "telugu", "hindi", "malayalam", "kannada", "english", "multi", "director's cut", "directors cut", "extended", "remastered", "unrated"}
        t_check = str(title).strip().lower()
        if (not title or t_check in generic_words or len(t_check) < 2
                or re.fullmatch(r"(19|20)\d{2}", t_check)):
            fb_title, fb_year = extract_fallback_title_and_year(filename)
            if fb_title:
                title = fb_title
                if not year and fb_year:
                    year = fb_year

        # TV structure is decided by our own parser, NOT PTN: PTN misses
        # "Ep 09", mis-reads reality-show seasons, and keeps "Ep NN" inside
        # the title — which is what made Bigg Boss entries mix together.
        ep_info = analyze_episodes(filename)
        season = first_int(parsed.get("season"))
        ep_start, ep_end = episode_bounds(parsed.get("episode"))
        if ep_info:
            season = ep_info["season"] or season
            if ep_info["start"] is not None:
                ep_start, ep_end = ep_info["start"], ep_info["end"]
            else:
                ep_start = ep_end = None  # keyword-only whole-season pack
        is_series = ep_info is not None or season is not None or ep_start is not None
        # The Stremio videos list only shows files carrying a season number;
        # most Indian TV uploads are season-1 packs/episodes without one.
        if is_series and season is None:
            season = 1
        # Series search titles keep the language word ("Bigg Boss Tamil" is a
        # DIFFERENT show than Hindi "Bigg Boss").
        series_cands = series_title_candidates(filename) if is_series else []

        if not title and not series_cands:
            await log_unindexed(file_key, filename, size, chat_id, message_id, "No Title Found", title, year)
            return None

        media_type = "series" if is_series else "movie"
        tmdb_type = "tv" if media_type == "series" else "movie"

        res = None
        if media_type == "series":
            # Scan the result list and accept only a show whose name is fully
            # contained in our clean candidate — TMDb's top hit for a noisy
            # query is often the Hindi "Bigg Boss" (or an unrelated fuzzy
            # match), which is exactly how Tamil episodes ended up mixed.
            for cand in series_cands:
                try:
                    results = await tmdb_search_multi(cand, "tv", limit=8)
                except Exception:
                    results = []
                for r in results:
                    if series_title_match(cand, r.get("name") or r.get("title") or ""):
                        res = r
                        break
                if res:
                    break
        if not res and title:
            res = await tmdb_search(title, tmdb_type, year)
            if not res and year is not None:
                res = await tmdb_search(title, tmdb_type, None)
        if not res:
            await log_unindexed(file_key, filename, size, chat_id, message_id, "TMDb Match Failed", title, year)
            return None

        tmdb_id = res["id"]
        details = await tmdb_details(tmdb_type, tmdb_id)
        if not details:
            await log_unindexed(file_key, filename, size, chat_id, message_id, "TMDb Details Failed", title, year)
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
        # v17: stills / season posters / trailer key ride along in the SAME
        # TMDb call — the Stremio meta response gains thumbnails + trailers
        # without a single extra API hit at stream time.
        update_data.update(media_fields_from_details(details, media_type))

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
            # Release source: 'predvd' (theatrical/cam) vs 'digital' vs 'unknown'.
            # Drives cleanup that removes PreDVD once an official print arrives.
            "source": source_from_filename(filename),
            "chat_id": int(chat_id),
            "message_id": int(message_id),
            "season": season,
            "episode_start": ep_start,
            "episode_end": ep_end,
            "indexed_at": time.time(),
        }
        await db.col("files").update_one({"_id": file_key}, {"$set": file_data}, upsert=True)
        await db.col("unindexed").delete_one({"_id": file_key})

        old_meta_id = (old_file or {}).get("meta_id")
        if old_meta_id and old_meta_id != doc_id:
            if not await db.col("files").find_one({"meta_id": old_meta_id}, {"_id": 1}):
                await db.col("meta").delete_one({"_id": old_meta_id})

        # v17: Ops Board counters + watchlist notification. Both strictly
        # best-effort — nothing here may turn a successful index into a failure.
        _status["indexed_total"] = _status.get("indexed_total", 0) + 1
        _status["last_indexed_ts"] = time.time()
        try:
            from .watchlist import check_landed
            await check_landed(update_data.get("title") or "", filename, file_data["size_str"])
        except Exception as exc:
            LOGGER.debug("[INDEXER] watchlist check skipped: %s", exc)
        return doc_id
    except Exception as exc:
        LOGGER.error(f"[INDEXER] Exception processing message {message_id} in {chat_id}: {exc}")
        return None


async def _cleanup_touched(meta_ids: set) -> int:
    """Run the best-of-3 / PreDVD-removal cleanup over titles touched this scan.

    Capped so a massive historic run doesn't spend forever; titles beyond the
    cap are picked up by a later run (or an explicit bulk cleanup)."""
    if not meta_ids:
        return 0
    from .cleanup import clean_meta_files

    removed = 0
    for meta_id in list(meta_ids)[:200]:
        if _stop_requested:
            break
        try:
            removed += await clean_meta_files(meta_id)
        except Exception as exc:
            LOGGER.error(f"[INDEXER] cleanup error for {meta_id}: {exc}")
    return removed


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
        touched_meta: set = set()

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
                            _mid = await _process_message(chat_id, candidate)
                            if _mid:
                                touched_meta.add(_mid)
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
                            _mid = await _process_message(chat_id, candidate)
                            if _mid:
                                touched_meta.add(_mid)
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
                if not _stop_requested:
                    await _cleanup_touched(touched_meta)
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
                        _mid = await _process_message(chat_id, msg)
                        if _mid:
                            touched_meta.add(_mid)
                    except Exception as p_err:
                        LOGGER.error(f"[INDEXER] error processing incremental msg {getattr(msg, 'id', '?')} in {chat_id}: {p_err}")
                    total["processed"] += 1
                    _status["processed"] = total["processed"]
                if highest_seen > last_id:
                    await db.col("state").update_one(
                        {"_id": sync_key}, {"$set": {"last_id": highest_seen}}, upsert=True
                    )
                if not _stop_requested:
                    await _cleanup_touched(touched_meta)
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
        try:
            from .opslog import log_op
            await log_op("sweep", final_status,
                         processed=total["processed"],
                         error=str(_status.get("last_error") or "")[:120])
        except Exception:
            pass
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
    """Periodically syncs tracked channels in the background (backup to live handlers)."""
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


def start_background_watcher(interval_seconds: int = None) -> None:
    """Start the Global channel background watcher/sync loop."""
    global _bg_sync_task, _bg_sync_stop_event
    if _bg_sync_task is not None and not _bg_sync_task.done():
        return
    interval = interval_seconds or (config.BACKGROUND_SYNC_MINUTES * 60)
    _bg_sync_stop_event.clear()
    _bg_sync_task = asyncio.create_task(_background_sync_loop(interval))


def stop_background_watcher() -> None:
    """Stop the Global channel background watcher/sync loop."""
    global _bg_sync_task, _bg_sync_stop_event
    _bg_sync_stop_event.set()
    if _bg_sync_task and not _bg_sync_task.done():
        _bg_sync_task.cancel()


# ---------------------------------------------------------------------------
# Startup self-heal for series metadata (reparses stored filenames with the
# current parser — no Telegram traffic needed).
# ---------------------------------------------------------------------------


def _norm_title(t: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(t or "").casefold())


async def repair_series_index(max_updates: int = 6000, max_relinks: int = 300,
                              unindexed_retry_limit: int = 300,
                              backfill_meta: bool = False,
                              backfill_limit: int = 100) -> dict:
    """Re-derive season/episode data from stored filenames using the current parser.

    Four phases, all idempotent (only documents that actually change are
    touched, so the next boot is a fast no-op):
      1. Backfill/correct ``season``/``episode_start``/``episode_end`` on files
         saved with missing or broken fields by the old parser — this fixes
         combined EP packs leaking into every episode's stream list.
      2. Full re-index of series files linked to a MOVIE meta or to a show
         with a different title (the "Bigg Boss Tamil" → Hindi "Bigg Boss" mixup).
      3. Retry of the unindexed queue with the new parser.
      4. (v17, opt-in) ``backfill_meta``: pull poster-still / season-poster /
         trailer fields onto titles indexed before those fields existed —
         capped at ``backfill_limit`` titles per run, one TMDb call each.

    Never raises: a dead DB simply ends the pass.
    """
    stats = {"scanned": 0, "fields_fixed": 0, "relinked": 0,
             "unindexed_retried": 0, "unindexed_fixed": 0, "media_backfilled": 0}
    if not db.is_connected():
        return {**stats, "skipped": "db unavailable"}
    try:
        meta_cache: dict = {}
        relink_budget = max_relinks
        async for fdoc in db.col("files").find(
            {},
            {"filename": 1, "season": 1, "episode_start": 1, "episode_end": 1,
             "meta_id": 1, "chat_id": 1, "message_id": 1, "size": 1},
        ).batch_size(200):
            if stats["scanned"] >= max_updates:
                break
            stats["scanned"] += 1
            filename = str(fdoc.get("filename") or "")
            if not filename:
                continue
            info = analyze_episodes(filename)
            if not info:
                continue
            cur_season = first_int(fdoc.get("season"))
            cur_start = first_int(fdoc.get("episode_start"))
            cur_end = first_int(fdoc.get("episode_end"))
            want_season = info["season"] or cur_season or 1
            want_start = info["start"] if info["start"] is not None else cur_start
            want_end = info["end"] if info["end"] is not None else cur_end
            if want_end is not None and want_start is not None and want_end < want_start:
                want_start, want_end = want_end, want_start

            # Phase 2: wrong meta (movie-typed, or a different show title)?
            if relink_budget > 0:
                meta_id = fdoc.get("meta_id")
                meta = meta_cache.get(meta_id, "∅")
                if meta == "∅" and meta_id:
                    meta = await db.col("meta").find_one(
                        {"_id": meta_id}, {"media_type": 1, "title": 1}) or {}
                    meta_cache[meta_id] = meta
                need_relink = False
                if meta:
                    mtype = meta.get("media_type")
                    if mtype == "movie" and info["start"] is not None:
                        need_relink = True
                    elif mtype == "series":
                        mt = _norm_title(meta.get("title"))
                        cands = {_norm_title(c) for c in series_title_candidates(filename)}
                        if mt and cands and mt not in cands:
                            need_relink = True
                if need_relink:
                    relink_budget -= 1
                    try:
                        if await index_filename(int(fdoc["chat_id"]), int(fdoc["message_id"]),
                                                filename, int(fdoc.get("size") or 0)):
                            stats["relinked"] += 1
                            # The meta may now be corrected — drop the cached copy
                            # so sibling files of the same show don't each relink.
                            meta_cache.pop(meta_id, None)
                            continue  # re-index wrote fresh fields already
                    except Exception as exc:
                        LOGGER.debug("[REPAIR] relink failed for %s: %s", fdoc.get("_id"), exc)

            # Phase 1: field backfill.
            if (cur_season, cur_start, cur_end) != (want_season, want_start, want_end):
                await db.col("files").update_one({"_id": fdoc["_id"]}, {"$set": {
                    "season": want_season, "episode_start": want_start, "episode_end": want_end}})
                stats["fields_fixed"] += 1
                if stats["fields_fixed"] % 250 == 0:
                    await asyncio.sleep(0.2)  # let interactive traffic win

        # Phase 3: retry the unindexed queue with the new parser.
        left = unindexed_retry_limit
        async for u in db.col("unindexed").find(
            {}, {"filename": 1, "chat_id": 1, "message_id": 1, "size": 1}
        ).batch_size(50):
            if left <= 0:
                break
            fn = str(u.get("filename") or "")
            if not fn or fn in ("unnamed_video",) or u.get("reason") == "Non-Video / Unsupported Media Format":
                continue
            left -= 1
            stats["unindexed_retried"] += 1
            try:
                if await index_filename(int(u["chat_id"]), int(u["message_id"]),
                                        fn, int(u.get("size") or 0)):
                    stats["unindexed_fixed"] += 1
            except Exception as exc:
                LOGGER.debug("[REPAIR] unindexed retry failed for %s: %s", u.get("_id"), exc)
            await asyncio.sleep(0.1)  # TMDb rate-limit courtesy

        # ---- Phase 4 (v17): media backfill (stills / season posters / trailer).
        if backfill_meta:
            touched = 0
            limit = max(1, min(int(backfill_limit or 100), 250))
            try:
                cursor = db.col("meta").find(
                    {"trailer_yt": {"$exists": False}},
                    {"tmdb_id": 1, "media_type": 1},
                ).limit(limit)
                async for mdoc in cursor:
                    try:
                        tmdb_id = mdoc.get("tmdb_id")
                        if not tmdb_id:
                            continue
                        mt = mdoc.get("media_type") or "movie"
                        details = await tmdb_details(mt, tmdb_id)
                        if not details:
                            continue
                        fields = media_fields_from_details(details, mt)
                        fields["trailer_yt"] = fields.get("trailer_yt") or ""
                        await db.col("meta").update_one(
                            {"_id": mdoc["_id"]}, {"$set": fields})
                        touched += 1
                        if touched % 25 == 0:
                            await asyncio.sleep(0.5)  # let interactive traffic win
                    except Exception as exc:
                        LOGGER.debug("[REPAIR] media backfill skip %s: %s",
                                     mdoc.get("_id"), exc)
                    await asyncio.sleep(0.15)  # TMDb courtesy
                if touched:
                    stats["media_backfilled"] = touched
                    # Stremio meta responses are cached — refresh so new
                    # thumbnails/trailers appear immediately, not at TTL.
                    try:
                        from .cache import meta_cache as _stremio_meta_cache
                        _stremio_meta_cache.clear()
                    except Exception:
                        pass
            except Exception as exc:
                LOGGER.debug("[REPAIR] media backfill aborted: %s", exc)

        if any(v for k, v in stats.items() if k != "scanned"):
            LOGGER.info("[REPAIR] series index self-heal: %s", stats)
        return stats
    except Exception as exc:  # noqa: BLE001 — self-heal must never break boot
        LOGGER.warning("[REPAIR] series index self-heal aborted: %s", exc)
        return {**stats, "error": str(exc)[:200]}


# ---------------------------------------------------------------------------
# v17: single entry point for the Self-Heal pass (panel button + /heal share it)
# ---------------------------------------------------------------------------

_repair_busy = False


def schedule_repair(backfill: bool = False) -> dict:
    """Start a self-heal run in the background. Never blocks the caller.

    Returns {ok, message} on accept, {ok: False, detail} when one is already
    running. On completion the result lands in ops_log so the panel (and
    /stats) can show what it actually did even if the tab was closed.
    """
    global _repair_busy
    if _repair_busy:
        return {"ok": False, "detail": "A repair pass is already running — check back in a minute."}
    _repair_busy = True

    async def _run():
        try:
            res = await repair_series_index(
                max_updates=30000, max_relinks=3000, unindexed_retry_limit=3000,
                backfill_meta=bool(backfill))
            LOGGER.info("[REPAIR] background pass finished: %s", res)
            try:
                from .opslog import log_op
                bad = res.get("error") or res.get("skipped")
                await log_op(
                    "selfheal", "failed" if bad else "ok",
                    scanned=int(res.get("scanned") or 0),
                    fixed=int(res.get("fields_fixed") or 0),
                    relinked=int(res.get("relinked") or 0),
                    retried=int(res.get("unindexed_retried") or 0),
                    recovered=int(res.get("unindexed_fixed") or 0),
                    backfilled=int(res.get("media_backfilled") or 0),
                    detail=str(bad or ""),
                )
            except Exception:
                pass
        except Exception as exc:  # never surface a crash to the panel
            LOGGER.warning("[REPAIR] background pass failed: %s", exc)
        finally:
            _repair_busy = False

    try:
        asyncio.create_task(_run())
    except RuntimeError:  # no running loop (tests) — release the guard
        _repair_busy = False
        return {"ok": False, "detail": "Cannot schedule right now."}
    return {"ok": True,
            "message": "Self-heal started in background (fields, show relinks, unindexed retries"
                       + (", poster/trailer backfill ≤100 titles" if backfill else "")
                       + "). Refresh the queue in a minute."}
