import asyncio
import re
import time
import uuid
from datetime import datetime, timedelta, timezone

import PTN
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError
from pyrogram import enums
from pyrogram.errors import FloodWait

from Backend.logger import LOGGER
from Backend.helper.settings_manager import SettingsManager
from Backend.helper.pyro import get_readable_file_size
from Backend.pyrofork.bot import Userbot
from Backend.helper.split_files import parse_combined_episodes
from Backend.helper.metadata import _tmdb_details, safe_tmdb_search, format_tmdb_image
from Backend.helper.global_db_service import (
    episode_bounds,
    first_int,
    global_file_key,
    remove_global_file_reference,
    remove_global_file_references,
)
from Backend.helper.global_search import _video_filename

_INDEXER_RUNNING = False
_INDEXER_STOP_REQUESTED = False
_INDEXER_TASK = None
_INDEXER_OWNER = uuid.uuid4().hex
_INDEXER_HEARTBEAT_AT = 0.0
_INDEXER_LEASE_SECONDS = 120
_INDEXER_STATUS = {
    "running": False,
    "stop_requested": False,
    "processed": 0,
    "current_chat": None,
    "current_filter": None,
    "last_error": None,
}


def _lease_expiry(now: datetime) -> datetime:
    return now + timedelta(seconds=_INDEXER_LEASE_SECONDS)


async def _acquire_indexer_lease(db, target_chat_id, force_historic: bool) -> bool:
    if db.global_db is None:
        return False
    now = datetime.now(timezone.utc)
    try:
        job = await db.global_db["state"].find_one_and_update(
            {
                "_id": "global_indexer_job",
                "$or": [
                    {"running": {"$ne": True}},
                    {"lease_until": {"$lte": now}},
                    {"lease_until": {"$exists": False}},
                    {"owner": _INDEXER_OWNER},
                ],
            },
            {"$set": {
                "running": True,
                "status": "running",
                "owner": _INDEXER_OWNER,
                "lease_until": _lease_expiry(now),
                "heartbeat_at": now,
                "started_at": now,
                "finished_at": None,
                "stop_requested": False,
                "processed": 0,
                "current_chat": None,
                "current_filter": None,
                "last_error": None,
                "target_chat_id": target_chat_id,
                "force_historic": bool(force_historic),
            }},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
    except DuplicateKeyError:
        return False
    return bool(job and job.get("owner") == _INDEXER_OWNER)


async def _heartbeat_and_should_stop(db, force: bool = False) -> bool:
    """Renew this process' durable lease and observe cross-replica stop requests."""
    global _INDEXER_HEARTBEAT_AT, _INDEXER_STOP_REQUESTED
    if _INDEXER_STOP_REQUESTED:
        return True
    now_mono = time.monotonic()
    if not force and now_mono - _INDEXER_HEARTBEAT_AT < 15:
        return False

    now = datetime.now(timezone.utc)
    job = await db.global_db["state"].find_one_and_update(
        {"_id": "global_indexer_job", "running": True, "owner": _INDEXER_OWNER},
        {"$set": {
            "lease_until": _lease_expiry(now),
            "heartbeat_at": now,
            "processed": _INDEXER_STATUS["processed"],
            "current_chat": _INDEXER_STATUS["current_chat"],
            "current_filter": _INDEXER_STATUS["current_filter"],
        }},
        return_document=ReturnDocument.AFTER,
    )
    _INDEXER_HEARTBEAT_AT = now_mono
    if not job:
        _INDEXER_STATUS["last_error"] = "Indexer lease lost to another process."
        _INDEXER_STOP_REQUESTED = True
        return True
    if job.get("stop_requested"):
        _INDEXER_STOP_REQUESTED = True
        _INDEXER_STATUS["stop_requested"] = True
        return True
    return False


async def _lease_aware_sleep(db, seconds: int) -> None:
    remaining = max(0, int(seconds or 0))
    while remaining and not _INDEXER_STOP_REQUESTED:
        chunk = min(10, remaining)
        await asyncio.sleep(chunk)
        remaining -= chunk
        if await _heartbeat_and_should_stop(db, force=True):
            break


async def schedule_global_indexer(
    db, target_chat_id: int = None, force_historic: bool = False
) -> bool:
    """Claim a cross-replica lease, then schedule one process-local task."""
    global _INDEXER_RUNNING, _INDEXER_STOP_REQUESTED, _INDEXER_TASK
    global _INDEXER_HEARTBEAT_AT
    if _INDEXER_RUNNING or (_INDEXER_TASK is not None and not _INDEXER_TASK.done()):
        return False
    if not await _acquire_indexer_lease(db, target_chat_id, force_historic):
        return False

    _INDEXER_RUNNING = True
    _INDEXER_STOP_REQUESTED = False
    _INDEXER_HEARTBEAT_AT = 0.0
    _INDEXER_STATUS.update(
        {
            "running": True,
            "stop_requested": False,
            "processed": 0,
            "current_chat": None,
            "current_filter": None,
            "last_error": None,
        }
    )
    _INDEXER_TASK = asyncio.create_task(
        run_global_indexer(
            db,
            target_chat_id,
            force_historic,
            _already_claimed=True,
            _lease_claimed=True,
        )
    )
    return True


async def request_global_indexer_stop(db) -> bool:
    global _INDEXER_STOP_REQUESTED
    local_running = _INDEXER_RUNNING
    if local_running:
        _INDEXER_STOP_REQUESTED = True
        _INDEXER_STATUS["stop_requested"] = True
    if db.global_db is None:
        return local_running
    result = await db.global_db["state"].update_one(
        {"_id": "global_indexer_job", "running": True},
        {"$set": {"stop_requested": True, "stop_requested_at": datetime.now(timezone.utc)}},
    )
    return local_running or bool(result.matched_count)


async def global_indexer_status(db) -> dict:
    status = dict(_INDEXER_STATUS)
    if db.global_db is None:
        return status
    job = await db.global_db["state"].find_one(
        {"_id": "global_indexer_job"}, {"_id": 0, "owner": 0}
    )
    if job and job.get("running") and job.get("lease_until"):
        lease_until = job["lease_until"]
        if lease_until.tzinfo is None:
            lease_until = lease_until.replace(tzinfo=timezone.utc)
        if lease_until <= datetime.now(timezone.utc):
            expired = await db.global_db["state"].update_one(
                {
                    "_id": "global_indexer_job",
                    "running": True,
                    "lease_until": job["lease_until"],
                },
                {"$set": {
                    "running": False,
                    "status": "expired",
                    "last_error": "Indexer lease expired before clean shutdown.",
                    "finished_at": datetime.now(timezone.utc),
                }},
            )
            if expired.modified_count:
                job.update(
                    {
                        "running": False,
                        "status": "expired",
                        "last_error": "Indexer lease expired before clean shutdown.",
                    }
                )
            else:
                job = await db.global_db["state"].find_one(
                    {"_id": "global_indexer_job"}, {"_id": 0, "owner": 0}
                )
    if job:
        status.update(job)
    return status


async def get_or_create_global_catalogs(db):
    if db.global_db is None:
        return
    catalogs = [
        {"_id": "tamil_movies", "type": "movie", "name": "Tamil Movies", "order": 1},
        {"_id": "tamil_series", "type": "series", "name": "Tamil Series", "order": 2},
        {"_id": "video_songs", "type": "movie", "name": "Video Songs", "order": 3},
        {"_id": "dubbed_movies", "type": "movie", "name": "Dubbed Movies", "order": 4},
        {"_id": "dubbed_series", "type": "series", "name": "Dubbed Series", "order": 5},
        {"_id": "anime_movies", "type": "movie", "name": "Anime Movies", "order": 6},
        {"_id": "anime_series", "type": "series", "name": "Anime Series", "order": 7},
        {"_id": "other_movies", "type": "movie", "name": "Other Movies", "order": 8},
        {"_id": "other_series", "type": "series", "name": "Other Series", "order": 9},
    ]
    for cat in catalogs:
        await db.global_db["catalogs"].update_one({"_id": cat["_id"]}, {"$set": cat}, upsert=True)

def determine_catalog(parsed: dict, details, media_type: str, filename: str) -> str:
    original_lang = getattr(details, "original_language", "")
    genres = [g.name for g in (getattr(details, "genres", None) or [])]
    is_anime = "Animation" in genres or original_lang == "ja" or "anime" in filename.lower()
    is_tamil = original_lang == "ta"
    is_dubbed = not is_tamil and re.search(r'\b(tam|tamil|multi)\b', filename.lower())
    if is_anime: return "anime_movies" if media_type == "movie" else "anime_series"
    if is_tamil: return "tamil_movies" if media_type == "movie" else "tamil_series"
    if is_dubbed: return "dubbed_movies" if media_type == "movie" else "dubbed_series"
    return "other_movies" if media_type == "movie" else "other_series"

async def log_unindexed(db, file_id, filename, size, chat_id, message_id, reason, title=None, year=None):
    # An edited caption may turn a previously indexed file into an invalid one.
    # Remove the old mapping first so catalogs cannot keep serving stale metadata.
    await remove_global_file_reference(db.global_db, chat_id, message_id)
    doc = {
        "_id": file_id,
        "filename": filename,
        "size": size,
        "size_str": get_readable_file_size(size),
        "chat_id": int(chat_id),
        "message_id": int(message_id),
        "reason": reason,
        "parsed_title": title or "",
        "parsed_year": year or "",
        "updated_at": time.time(),
    }
    await db.global_db["unindexed"].update_one({"_id": file_id}, {"$set": doc}, upsert=True)

async def _process_message(db, message, chat_id):
    filename = _video_filename(message)
    if not filename: return
    
    media = getattr(message, "video", None) or getattr(message, "document", None)
    size = getattr(media, "file_size", 0) or 0
    file_id = global_file_key(chat_id, message.id)
    
    try:
        parsed = PTN.parse(filename)
    except:
        await log_unindexed(db, file_id, filename, size, chat_id, message.id, "PTN Parsing Failed")
        return
        
    title = parsed.get("title")
    year = parsed.get("year")
    
    if not title: 
        await log_unindexed(db, file_id, filename, size, chat_id, message.id, "No Title Found", title, year)
        return
        
    combined = parse_combined_episodes(filename)
    parsed_season = first_int(parsed.get("season"))
    parsed_episode_start, parsed_episode_end = episode_bounds(parsed.get("episode"))
    media_type = (
        "series"
        if parsed_season is not None or parsed_episode_start is not None or combined
        else "movie"
    )
    tmdb_type = "tv" if media_type == "series" else "movie"
    
    tmdb_res = await safe_tmdb_search(title, tmdb_type, year)
    if not tmdb_res and year is not None:
        tmdb_res = await safe_tmdb_search(title, tmdb_type, None)
        
    if not tmdb_res:
        await log_unindexed(db, file_id, filename, size, chat_id, message.id, "TMDb Match Failed", title, year)
        return
        
    tmdb_id = tmdb_res.id
    details = await _tmdb_details(tmdb_type, tmdb_id)
    if not details: 
        await log_unindexed(db, file_id, filename, size, chat_id, message.id, "TMDb Details Failed", title, year)
        return
        
    catalog = determine_catalog(parsed, details, media_type, filename)
    doc_id = f"tmdb:{tmdb_id}"
    external_ids = getattr(details, "external_ids", None)
    if isinstance(external_ids, dict):
        actual_imdb_id = external_ids.get("imdb_id")
    else:
        actual_imdb_id = getattr(external_ids, "imdb_id", None) if external_ids else None
    year_val = getattr(details, "release_date", None) or getattr(details, "first_air_date", "")
    year_number = getattr(year_val, "year", None)
    if year_number is None:
        year_match = re.search(r"\b(19\d{2}|20\d{2})\b", str(year_val or ""))
        year_number = int(year_match.group(1)) if year_match else None
    aliases = [doc_id]
    if actual_imdb_id:
        aliases.append(actual_imdb_id)

    update_data = {
        "tmdb_id": tmdb_id,
        "imdb_id": actual_imdb_id,
        "aliases": aliases,
        "title": getattr(details, "title", None) or getattr(details, "name", ""),
        "year": year_number,
        "poster": format_tmdb_image(details.poster_path),
        "background": format_tmdb_image(details.backdrop_path, "original"),
        "description": details.overview,
        "media_type": media_type,
        "catalog": catalog,
        "genres": [g.name for g in (getattr(details, "genres", None) or [])],
        "rating": getattr(details, "vote_average", 0.0),
        "updated_at": time.time(),
    }
    
    lang_map = {"tam": "Tamil", "tamil": "Tamil", "tel": "Telugu", "telugu": "Telugu", "hin": "Hindi", "hindi": "Hindi", "mal": "Malayalam", "malayalam": "Malayalam", "kan": "Kannada", "kannada": "Kannada", "eng": "English", "english": "English", "multi": "Multi"}
    languages = []
    fname_lower = filename.lower()
    for k, v in lang_map.items():
        if re.search(rf'\b{k}\b', fname_lower):
            if v not in languages:
                languages.append(v)
    if getattr(details, "original_language", "") == "ta" and "Tamil" not in languages:
        languages.append("Tamil")
        
    await db.global_db["meta"].update_one(
        {"_id": doc_id}, 
        {
            "$set": update_data,
            "$addToSet": {"languages": {"$each": languages}}
        }, 
        upsert=True
    )
    
    old_file = await db.global_db["files"].find_one({"_id": file_id}, {"meta_id": 1})
    file_data = {
        "_id": file_id,
        "meta_id": doc_id,
        "filename": filename,
        "size": size,
        "size_str": get_readable_file_size(size),
        "quality": parsed.get("resolution") or "HD",
        "chat_id": int(chat_id),
        "message_id": int(message.id),
        "season": first_int(combined["season"]) if combined else parsed_season,
        "episode_start": first_int(combined["start"]) if combined else parsed_episode_start,
        "episode_end": first_int(combined["end"]) if combined else parsed_episode_end,
        "indexed_at": time.time(),
    }
    await db.global_db["files"].update_one({"_id": file_id}, {"$set": file_data}, upsert=True)
    await db.global_db["unindexed"].delete_one({"_id": file_id})

    old_meta_id = old_file.get("meta_id") if old_file else None
    if old_meta_id and old_meta_id != doc_id:
        remaining = await db.global_db["files"].find_one({"meta_id": old_meta_id}, {"_id": 1})
        if not remaining:
            await db.global_db["meta"].delete_one({"_id": old_meta_id})
    return doc_id


async def clean_meta_files(db, meta_id: str):
    if str(meta_id).startswith("song:"):
        return

    cursor = db.global_db["files"].find({"meta_id": meta_id})
    files = [f async for f in cursor]
    if not files: return

    from collections import defaultdict
    import PTN
    import re

    # Group by season and episode so we only compare identical episodes
    groups = defaultdict(list)
    for f in files:
        def make_hashable(val):
            if isinstance(val, list):
                return tuple(val)
            return val
            
        key = (
            make_hashable(f.get("season")), 
            make_hashable(f.get("episode_start")), 
            make_hashable(f.get("episode_end"))
        )
        groups[key].append(f)

    to_delete = []

    def get_res_val(res_str):
        res_str = str(res_str or "").lower()
        if "2160" in res_str or "4k" in res_str: return 2160
        if "1440" in res_str or "2k" in res_str: return 1440
        if "1080" in res_str: return 1080
        if "720" in res_str: return 720
        if "480" in res_str: return 480
        if "360" in res_str: return 360
        return 0

    for key, group_files in groups.items():
        enriched = []
        has_high_res = False
        for f in group_files:
            try:
                parsed = PTN.parse(f["filename"])
            except:
                parsed = {}
            
            res_val = get_res_val(parsed.get("resolution", f.get("quality", "")))
            if res_val >= 720:
                has_high_res = True
            
            fname_lower = f["filename"].lower()
            lang_score = 0
            if re.search(r'\b(tam|tamil)\b', fname_lower):
                lang_score = 100
            elif re.search(r'\bmulti\b', fname_lower):
                lang_score = 50
            
            v_codec = str(parsed.get("codec", "")).lower()
            codec_score = 0
            if "265" in v_codec or "hevc" in v_codec: codec_score = 20
            elif "264" in v_codec or "avc" in v_codec: codec_score = 10
            
            a_codec = str(parsed.get("audio", "")).lower()
            audio_score = 0
            if "dts" in a_codec or "dd" in a_codec or "ac3" in a_codec or "eac3" in a_codec or "dolby" in a_codec:
                audio_score = 15
            elif "aac" in a_codec:
                audio_score = 10

            total_score = lang_score + codec_score + audio_score
            enriched.append({
                "doc": f,
                "res_val": res_val,
                "lang_score": lang_score,
                "score": total_score,
                "size": f.get("size", 0)
            })

        # Rule 1: Drop 360p/480p if a 720p+ file exists for this episode
        if has_high_res:
            enriched = [e for e in enriched if e["res_val"] >= 720]

        # Rule 2: If a Tamil specific file exists, strongly prioritize it over random unknown languages
        if any(e["lang_score"] >= 100 for e in enriched):
            enriched = [e for e in enriched if e["lang_score"] >= 50]

        # Rule 3: Group by resolution and keep max 3 per resolution
        res_groups = defaultdict(list)
        for e in enriched:
            res_groups[e["res_val"]].append(e)

        keep_docs = []
        for res_val, res_files in res_groups.items():
            # Sort by score DESC, then size DESC
            res_files.sort(key=lambda x: (x["score"], x["size"]), reverse=True)
            # Keep top 3
            kept = res_files[:3]
            keep_docs.extend([k["doc"]["_id"] for k in kept])

        for f in group_files:
            if f["_id"] not in keep_docs:
                to_delete.append(f["_id"])

    if to_delete:
        await db.global_db["files"].delete_many({"_id": {"$in": to_delete}})

async def _unprocessed_messages(global_db, chat_id: int, messages: list) -> list:
    """Filter one Telegram page without loading an entire channel index into RAM."""
    if not messages:
        return []
    by_id = {global_file_key(chat_id, message.id): message for message in messages}
    keys = list(by_id)
    existing_files = await global_db["files"].find(
        {"_id": {"$in": keys}}, {"_id": 1}
    ).to_list(None)
    existing_unindexed = await global_db["unindexed"].find(
        {"_id": {"$in": keys}}, {"_id": 1}
    ).to_list(None)
    existing = {row["_id"] for row in existing_files + existing_unindexed}
    return [message for key, message in by_id.items() if key not in existing]


async def _reconcile_stored_references(db, chat_id: int) -> int:
    """Boundedly remove records whose Telegram messages disappeared offline."""
    removed = 0
    seen_ids = set()
    pending_ids = []

    async def flush() -> bool:
        nonlocal removed, pending_ids
        if not pending_ids:
            return True
        if await _heartbeat_and_should_stop(db, force=True):
            return False

        requested = list(pending_ids)
        messages = await Userbot.get_messages(chat_id, requested)
        if not isinstance(messages, list):
            messages = [messages]
        available = {
            int(message.id)
            for message in messages
            if message
            and not getattr(message, "empty", False)
            and (getattr(message, "video", None) or getattr(message, "document", None))
        }
        stale = [(chat_id, message_id) for message_id in requested if message_id not in available]
        if stale:
            removed += await remove_global_file_references(db.global_db, stale)
        pending_ids = []
        return True

    for collection_name in ("files", "unindexed"):
        cursor = db.global_db[collection_name].find(
            {"chat_id": {"$in": [int(chat_id), str(int(chat_id))]}},
            {"message_id": 1},
        ).batch_size(100)
        async for row in cursor:
            message_id = first_int(row.get("message_id"))
            if message_id is None or message_id in seen_ids:
                continue
            seen_ids.add(message_id)
            pending_ids.append(message_id)
            if len(pending_ids) >= 100 and not await flush():
                return removed
    await flush()
    return removed


async def run_global_indexer(
    db,
    target_chat_id: int = None,
    force_historic: bool = False,
    _already_claimed: bool = False,
    _lease_claimed: bool = False,
):
    global _INDEXER_RUNNING, _INDEXER_STOP_REQUESTED, _INDEXER_TASK
    if _INDEXER_RUNNING and not _already_claimed:
        LOGGER.info("[GLOBAL INDEXER] Already running.")
        return
    if not _already_claimed:
        _INDEXER_RUNNING = True
        _INDEXER_STOP_REQUESTED = False
        _INDEXER_STATUS.update(
            {
                "running": True,
                "stop_requested": False,
                "processed": 0,
                "current_chat": None,
                "current_filter": None,
                "last_error": None,
            }
        )

    last_log_time = time.time()
    total_processed = 0
    try:
        if getattr(db, "global_db", None) is None:
            _INDEXER_STATUS["last_error"] = "No GlobalDB configured."
            LOGGER.info("[GLOBAL INDEXER] No GlobalDB configured. Skipping.")
            return
        if not _lease_claimed:
            _lease_claimed = await _acquire_indexer_lease(
                db, target_chat_id, force_historic
            )
            if not _lease_claimed:
                _INDEXER_STATUS["last_error"] = "Another indexer owns the active lease."
                LOGGER.info("[GLOBAL INDEXER] Another replica is already running.")
                return
        if not Userbot:
            _INDEXER_STATUS["last_error"] = "No Userbot configured."
            LOGGER.info("[GLOBAL INDEXER] No Userbot configured. Skipping.")
            return

        await _heartbeat_and_should_stop(db, force=True)
        await get_or_create_global_catalogs(db)
        LOGGER.info("[GLOBAL INDEXER] Started.")
        if target_chat_id:
            target_ids = [target_chat_id]
        else:
            settings = SettingsManager.current()
            from Backend.helper.global_search import _resolve_channel_ids
            target_ids = _resolve_channel_ids(settings.global_search_channels)
        
        for chat_id in target_ids:
            if await _heartbeat_and_should_stop(db, force=True):
                break
            _INDEXER_STATUS["current_chat"] = chat_id
            try:
                for msg_filter in (enums.MessagesFilter.VIDEO, enums.MessagesFilter.DOCUMENT):
                    if await _heartbeat_and_should_stop(db, force=True):
                        break
                    _INDEXER_STATUS["current_filter"] = msg_filter.name
                    sync_key = f"sync_{chat_id}_{msg_filter.name}"
                    sync_state = await db.global_db["state"].find_one({"_id": sync_key}) or {}
                    
                    historic_done = False if force_historic else sync_state.get("historic_done", False)
                    last_id = sync_state.get("last_id", 0)
                    offset_id = sync_state.get("historic_offset_id", 0)
                    
                    count = 0
                    
                    if not historic_done:
                        LOGGER.info(f"[INDEXER] {chat_id} ({msg_filter.name}) - Historic scan from offset {offset_id} (last seen {last_id})")
                        highest_seen = last_id
                        try:
                            message_batch = []
                            async for msg in Userbot.search_messages(chat_id, filter=msg_filter):
                                if await _heartbeat_and_should_stop(db):
                                    break
                                if offset_id > 0 and msg.id >= offset_id:
                                    continue
                                highest_seen = max(highest_seen, msg.id)
                                message_batch.append(msg)
                                if len(message_batch) < 100:
                                    continue

                                stop_during_batch = False
                                for candidate in (
                                    message_batch
                                    if force_historic
                                    else await _unprocessed_messages(
                                        db.global_db, chat_id, message_batch
                                    )
                                ):
                                    if await _heartbeat_and_should_stop(db):
                                        stop_during_batch = True
                                        break
                                    await _process_message(db, candidate, chat_id)
                                    count += 1
                                    total_processed += 1
                                _INDEXER_STATUS["processed"] = total_processed
                                if stop_during_batch:
                                    break
                                if time.time() - last_log_time >= 120:
                                    LOGGER.info(
                                        "[GLOBAL INDEXER] Still running... "
                                        f"Indexed {total_processed} items so far."
                                    )
                                    last_log_time = time.time()
                                await db.global_db["state"].update_one(
                                    {"_id": sync_key},
                                    {"$set": {
                                        "historic_offset_id": message_batch[-1].id,
                                        "last_id": highest_seen,
                                    }},
                                    upsert=True,
                                )
                                message_batch = []

                            if not _INDEXER_STOP_REQUESTED and message_batch:
                                stop_during_batch = False
                                for candidate in (
                                    message_batch
                                    if force_historic
                                    else await _unprocessed_messages(
                                        db.global_db, chat_id, message_batch
                                    )
                                ):
                                    if await _heartbeat_and_should_stop(db):
                                        stop_during_batch = True
                                        break
                                    await _process_message(db, candidate, chat_id)
                                    count += 1
                                    total_processed += 1
                                _INDEXER_STATUS["processed"] = total_processed
                                if stop_during_batch:
                                    break

                            if not _INDEXER_STOP_REQUESTED:
                                await db.global_db["state"].update_one(
                                    {"_id": sync_key}, 
                                    {"$set": {"historic_done": True, "historic_offset_id": 0, "last_id": highest_seen}}, 
                                    upsert=True
                                )
                                LOGGER.info(f"[INDEXER] {chat_id} ({msg_filter.name}) - Historic scan complete!")
                        except FloodWait as fw:
                            _INDEXER_STATUS["last_error"] = (
                                f"FloodWait while scanning {chat_id}; run can be resumed."
                            )
                            await _lease_aware_sleep(db, getattr(fw, "value", 5))
                        except Exception as e:
                            _INDEXER_STATUS["last_error"] = (
                                f"Historic scan {chat_id}: {type(e).__name__}: {e}"
                            )
                            LOGGER.error(f"[INDEXER] Error in historic scan {chat_id}: {e}")
                    else:
                        LOGGER.info(f"[INDEXER] {chat_id} ({msg_filter.name}) - Syncing new files (Newer than {last_id})")
                        highest_seen = last_id
                        try:
                            async for msg in Userbot.search_messages(chat_id, filter=msg_filter):
                                if await _heartbeat_and_should_stop(db):
                                    break
                                if msg.id <= last_id:
                                    break
                                if msg.id > highest_seen: highest_seen = msg.id
                                    
                                await _process_message(db, msg, chat_id)
                                count += 1
                                total_processed += 1
                                _INDEXER_STATUS["processed"] = total_processed
                                if time.time() - last_log_time >= 120:
                                    LOGGER.info(f"[GLOBAL INDEXER] Still running... Indexed {total_processed} items so far.")
                                    last_log_time = time.time()
                                
                            if highest_seen > last_id:
                                await db.global_db["state"].update_one(
                                    {"_id": sync_key}, 
                                    {"$set": {"last_id": highest_seen}}, 
                                    upsert=True
                                )
                        except FloodWait as fw:
                            _INDEXER_STATUS["last_error"] = (
                                f"FloodWait while syncing {chat_id}; run can be resumed."
                            )
                            await _lease_aware_sleep(db, getattr(fw, "value", 5))
                        except Exception as e:
                            _INDEXER_STATUS["last_error"] = (
                                f"Incremental sync {chat_id}: {type(e).__name__}: {e}"
                            )
                            LOGGER.error(f"[INDEXER] Error in new sync {chat_id}: {e}")

                if force_historic and not _INDEXER_STOP_REQUESTED:
                    _INDEXER_STATUS["current_filter"] = "RECONCILE"
                    stale_count = await _reconcile_stored_references(db, chat_id)
                    LOGGER.info(
                        "[GLOBAL INDEXER] Reconciled %s stale record(s) for %s.",
                        stale_count,
                        chat_id,
                    )
                LOGGER.info(f"[GLOBAL INDEXER] Finished {chat_id}. Processed {count} items.")
            except Exception as e:
                _INDEXER_STATUS["last_error"] = (
                    f"Channel {chat_id}: {type(e).__name__}: {e}"
                )
                LOGGER.error(f"[GLOBAL INDEXER] Error handling {chat_id}: {e}")
                
    except Exception as e:
        _INDEXER_STATUS["last_error"] = f"{type(e).__name__}: {e}"
        LOGGER.error(f"[GLOBAL INDEXER] Fatal Error: {e}")
    finally:
        stopped = _INDEXER_STOP_REQUESTED
        final_status = (
            "failed"
            if _INDEXER_STATUS.get("last_error")
            else "stopped"
            if stopped
            else "completed"
        )
        finished_at = datetime.now(timezone.utc)
        if _lease_claimed and getattr(db, "global_db", None) is not None:
            try:
                await db.global_db["state"].update_one(
                    {
                        "_id": "global_indexer_job",
                        "owner": _INDEXER_OWNER,
                    },
                    {"$set": {
                        "running": False,
                        "status": final_status,
                        "stop_requested": False,
                        "processed": total_processed,
                        "current_chat": None,
                        "current_filter": None,
                        "last_error": _INDEXER_STATUS.get("last_error"),
                        "finished_at": finished_at,
                        "lease_until": finished_at,
                    }},
                )
            except Exception as exc:
                LOGGER.error("[GLOBAL INDEXER] Failed to release durable lease: %s", exc)
        _INDEXER_RUNNING = False
        _INDEXER_STOP_REQUESTED = False
        _INDEXER_TASK = None
        _INDEXER_STATUS.update(
            {
                "running": False,
                "status": final_status,
                "stop_requested": False,
                "current_chat": None,
                "current_filter": None,
                "finished_at": finished_at,
            }
        )
        LOGGER.info("[GLOBAL INDEXER] Engine offline (%s).", final_status)
