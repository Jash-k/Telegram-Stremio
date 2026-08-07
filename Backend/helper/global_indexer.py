import asyncio
import re
import time
from typing import Dict, List, Optional

import PTN
from pyrogram import enums
from pyrogram.errors import (
    FloodWait, ChatAdminRequired, ChannelPrivate, PeerIdInvalid,
    UserNotParticipant, AuthKeyUnregistered, SessionRevoked, RPCError,
)

from Backend.logger import LOGGER
from Backend.helper.settings_manager import SettingsManager
from Backend.helper.pyro import get_readable_file_size
from Backend.pyrofork.bot import Userbot
from Backend.helper.split_files import parse_combined_episodes
from Backend.helper.metadata import _tmdb_details, safe_tmdb_search, format_tmdb_image
from Backend.helper.global_search import _video_filename

_INDEXER_RUNNING = False

async def get_or_create_global_catalogs(db):
    if db.global_db is None:
        return
    catalogs = [
        {"_id": "tamil_movies", "type": "movie", "name": "Tamil Movies"},
        {"_id": "tamil_series", "type": "series", "name": "Tamil Series"},
        {"_id": "video_songs", "type": "movie", "name": "Video Songs"},
        {"_id": "dubbed_movies", "type": "movie", "name": "Dubbed Movies"},
        {"_id": "dubbed_series", "type": "series", "name": "Dubbed Series"},
        {"_id": "anime_movies", "type": "movie", "name": "Anime Movies"},
        {"_id": "anime_series", "type": "series", "name": "Anime Series"},
        {"_id": "other_movies", "type": "movie", "name": "Other Movies"},
        {"_id": "other_series", "type": "series", "name": "Other Series"},
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
    doc = {
        "_id": file_id, "filename": filename, "size": size, "size_str": get_readable_file_size(size),
        "chat_id": chat_id, "message_id": message_id, "reason": reason, "parsed_title": title or "", "parsed_year": year or ""
    }
    await db.global_db["unindexed"].update_one({"_id": file_id}, {"$set": doc}, upsert=True)

async def _process_message(db, message, chat_id):
    filename = _video_filename(message)
    if not filename: return
    
    media = getattr(message, "video", None) or getattr(message, "document", None)
    size = getattr(media, "file_size", 0) or 0
    file_id = f"{chat_id}_{message.id}"
    
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
        
    media_type = "series" if parsed.get("season") or parse_combined_episodes(filename) else "movie"
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
    year_val = getattr(details, "release_date", None) or getattr(details, "first_air_date", "")
    year_str = str(year_val) if year_val else ""

    update_data = {
        "tmdb_id": tmdb_id, "imdb_id": doc_id,
        "title": getattr(details, "title", None) or getattr(details, "name", ""),
        "year": year_str, "poster": format_tmdb_image(details.poster_path),
        "background": format_tmdb_image(details.backdrop_path, "original"),
        "description": details.overview, "media_type": media_type,
        "catalog": catalog, "genres": [g.name for g in (getattr(details, "genres", None) or [])],
        "rating": getattr(details, "vote_average", 0.0),
        "updated_at": __import__("time").time()
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
    
    combined = parse_combined_episodes(filename)
    file_data = {
        "_id": file_id, "meta_id": doc_id, "filename": filename,
        "size": size, "size_str": get_readable_file_size(size), "quality": parsed.get("resolution", "HD"),
        "chat_id": chat_id, "message_id": message.id,
        "season": combined["season"] if combined else parsed.get("season"),
        "episode_start": combined["start"] if combined else parsed.get("episode"),
        "episode_end": combined["end"] if combined else parsed.get("episode")
    }
    await db.global_db["files"].update_one({"_id": file_id}, {"$set": file_data}, upsert=True)
    await db.global_db["unindexed"].delete_one({"_id": file_id})
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

async def run_global_indexer(db, target_chat_id: int = None, force_historic: bool = False):
    if getattr(db, "global_db", None) is not None:
        await get_or_create_global_catalogs(db)

    global _INDEXER_RUNNING
    if _INDEXER_RUNNING:
        LOGGER.info("[GLOBAL INDEXER] Already running.")
        return
        
    if getattr(db, "global_db", None) is None:
        LOGGER.info("[GLOBAL INDEXER] No Global DB configured. Skipping.")
        return
        
    if not Userbot:
        LOGGER.info("[GLOBAL INDEXER] No Userbot configured. Skipping.")
        return
        
    _INDEXER_RUNNING = True
    LOGGER.info("[GLOBAL INDEXER] Started.")
    import time
    last_log_time = time.time()
    total_processed = 0
    try:
        updated_meta_ids = set()
        
        if target_chat_id:
            target_ids = [target_chat_id]
        else:
            settings = SettingsManager.current()
            from Backend.helper.global_search import _resolve_channel_ids
            target_ids = _resolve_channel_ids(settings.global_search_channels)
        
        for chat_id in target_ids:
            try:
                for msg_filter in (enums.MessagesFilter.VIDEO, enums.MessagesFilter.DOCUMENT):
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
                            fetched_count = 0
                            
                            # Grab existing message IDs from our DB so we don't reprocess them!
                            # This completely prevents duplication and allows "resuming" without wiping!
                            existing_files = await db.global_db["files"].find({"chat_id": {"$in": [chat_id, str(chat_id)]}}, {"message_id": 1}).to_list(None)
                            existing_unidx = await db.global_db["unindexed"].find({"chat_id": {"$in": [chat_id, str(chat_id)]}}, {"message_id": 1}).to_list(None)
                            processed_ids = {doc["message_id"] for doc in existing_files} | {doc["message_id"] for doc in existing_unidx}
                            
                            async for msg in Userbot.search_messages(chat_id, filter=msg_filter):
                                if not _INDEXER_RUNNING: break
                                
                                if offset_id > 0 and msg.id >= offset_id:
                                    continue
                                    
                                if msg.id > highest_seen: 
                                    highest_seen = msg.id
                                    
                                # If we ALREADY indexed this file previously, skip it completely!
                                if msg.id in processed_ids:
                                    continue
                                    
                                mid = await _process_message(db, msg, chat_id)
                                if mid:
                                    updated_meta_ids.add(mid)
                                count += 1
                                total_processed += 1
                                if time.time() - last_log_time >= 120:
                                    LOGGER.info(f"[GLOBAL INDEXER] Still running... Indexed {total_processed} items so far.")
                                    last_log_time = time.time()
                                fetched_count += 1
                                
                                if fetched_count % 50 == 0:
                                    await db.global_db["state"].update_one(
                                        {"_id": sync_key}, 
                                        {"$set": {"historic_offset_id": msg.id, "last_id": highest_seen}}, 
                                        upsert=True
                                    )
                                    
                            if _INDEXER_RUNNING:
                                await db.global_db["state"].update_one(
                                    {"_id": sync_key}, 
                                    {"$set": {"historic_done": True, "historic_offset_id": 0, "last_id": highest_seen}}, 
                                    upsert=True
                                )
                                LOGGER.info(f"[INDEXER] {chat_id} ({msg_filter.name}) - Historic scan complete!")
                        except FloodWait as fw:
                            await asyncio.sleep(getattr(fw, "value", 5))
                        except Exception as e:
                            LOGGER.error(f"[INDEXER] Error in historic scan {chat_id}: {e}")
                    else:
                        LOGGER.info(f"[INDEXER] {chat_id} ({msg_filter.name}) - Syncing new files (Newer than {last_id})")
                        highest_seen = last_id
                        try:
                            async for msg in Userbot.search_messages(chat_id, filter=msg_filter):
                                if not _INDEXER_RUNNING: break
                                if msg.id <= last_id: break
                                if msg.id > highest_seen: highest_seen = msg.id
                                    
                                mid = await _process_message(db, msg, chat_id)
                                if mid:
                                    updated_meta_ids.add(mid)
                                count += 1
                                total_processed += 1
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
                            await asyncio.sleep(getattr(fw, "value", 5))
                        except Exception as e:
                            LOGGER.error(f"[INDEXER] Error in new sync {chat_id}: {e}")
                            
                LOGGER.info(f"[GLOBAL INDEXER] Finished {chat_id}. Processed {count} items.")
            except Exception as e:
                LOGGER.error(f"[GLOBAL INDEXER] Error handling {chat_id}: {e}")
                
    except Exception as e:
        LOGGER.error(f"[GLOBAL INDEXER] Fatal Error: {e}")
    finally:
        _INDEXER_RUNNING = False
        LOGGER.info("[GLOBAL INDEXER] Engine offline.")
