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
from Backend.helper.encrypt import encode_string
from Backend.helper.pyro import get_readable_file_size
from Backend.pyrofork.bot import Userbot
from Backend.helper.split_files import parse_combined_episodes
from Backend.helper.metadata import _tmdb_details, safe_tmdb_search, format_tmdb_image

_INDEXER_RUNNING = False

async def get_or_create_global_catalogs(db):
    if db.global_db is None:
        return
        
    catalogs = [
        {"_id": "tamil_movies", "type": "movie", "name": "Tamil Movies"},
        {"_id": "dubbed_movies", "type": "movie", "name": "Dubbed Movies"},
        {"_id": "tamil_series", "type": "series", "name": "Tamil Series"},
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
    
    if is_anime:
        return "anime_movies" if media_type == "movie" else "anime_series"
    if is_tamil:
        return "tamil_movies" if media_type == "movie" else "tamil_series"
    if is_dubbed:
        return "dubbed_movies" if media_type == "movie" else "dubbed_series"
        
    return "other_movies" if media_type == "movie" else "other_series"

async def log_unindexed(db, file_id, filename, size, chat_id, message_id, reason, title=None, year=None):
    doc = {
        "_id": file_id,
        "filename": filename,
        "size": size,
        "size_str": get_readable_file_size(size),
        "chat_id": chat_id,
        "message_id": message_id,
        "reason": reason,
        "parsed_title": title or "",
        "parsed_year": year or ""
    }
    await db.global_db["unindexed"].update_one({"_id": file_id}, {"$set": doc}, upsert=True)

async def run_global_indexer(db):
    global _INDEXER_RUNNING
    if _INDEXER_RUNNING:
        LOGGER.info("[GLOBAL INDEXER] Already running.")
        return
        
    if db.global_db is None:
        LOGGER.info("[GLOBAL INDEXER] No Global DB configured. Skipping.")
        return
        
    if not Userbot:
        LOGGER.info("[GLOBAL INDEXER] No Userbot configured. Skipping.")
        return
        
    _INDEXER_RUNNING = True
    LOGGER.info("[GLOBAL INDEXER] Started.")
    try:
        await get_or_create_global_catalogs(db)
        
        settings = SettingsManager.current()
        from Backend.helper.global_search import _resolve_channel_ids, _video_filename
        target_ids = _resolve_channel_ids(settings.global_search_channels)
        
        for chat_id in target_ids:
            try:
                LOGGER.info(f"[GLOBAL INDEXER] Scanning chat {chat_id}...")
                count = 0
                for msg_filter in (enums.MessagesFilter.VIDEO, enums.MessagesFilter.DOCUMENT):
                    sync_key = f"sync_{chat_id}_{msg_filter.name}"
                    sync_state = await db.global_db["state"].find_one({"_id": sync_key}) or {}
                    last_id = sync_state.get("last_id", 0)
                    max_id_seen = last_id
                    
                    async for message in Userbot.search_messages(chat_id, filter=msg_filter):
                        if message.id <= last_id:
                            break # Reached previously scanned messages
                            
                        max_id_seen = max(max_id_seen, message.id)
                        
                        filename = _video_filename(message)
                        if not filename: continue
                        
                        media = getattr(message, "video", None) or getattr(message, "document", None)
                        size = getattr(media, "file_size", 0) or 0
                        file_id = f"{chat_id}_{message.id}"
                        
                        try:
                            parsed = PTN.parse(filename)
                        except:
                            await log_unindexed(db, file_id, filename, size, chat_id, message.id, "PTN Parsing Failed")
                            continue
                            
                        title = parsed.get("title")
                        year = parsed.get("year")
                        
                        if not title: 
                            await log_unindexed(db, file_id, filename, size, chat_id, message.id, "No Title Found", title, year)
                            continue
                        
                        # 1. Search TMDB
                        media_type = "series" if parsed.get("season") or parse_combined_episodes(filename) else "movie"
                        tmdb_type = "tv" if media_type == "series" else "movie" 
                        
                        tmdb_res = await safe_tmdb_search(title, tmdb_type, year)
                        if not tmdb_res:
                            tmdb_res = await safe_tmdb_search(title, tmdb_type, None)
                            
                        if not tmdb_res:
                            await log_unindexed(db, file_id, filename, size, chat_id, message.id, "TMDb Match Failed", title, year)
                            continue
                        
                        # 2. Get Details
                        tmdb_id = tmdb_res.id
                        details = await _tmdb_details(tmdb_type, tmdb_id)
                        if not details: 
                            await log_unindexed(db, file_id, filename, size, chat_id, message.id, "TMDb Details Failed", title, year)
                            continue
                        
                        # 3. Categorize
                        catalog = determine_catalog(parsed, details, media_type, filename)
                        
                        # 4. Save to DB
                        doc_id = f"tmdb:{tmdb_id}"
                        
                        year_val = getattr(details, "release_date", None) or getattr(details, "first_air_date", "")
                        year_str = str(year_val) if year_val else ""

                        update_data = {
                            "tmdb_id": tmdb_id,
                            "imdb_id": doc_id,
                            "title": getattr(details, "title", None) or getattr(details, "name", ""),
                            "year": year_str,
                            "poster": format_tmdb_image(details.poster_path),
                            "background": format_tmdb_image(details.backdrop_path, "original"),
                            "description": details.overview,
                            "media_type": media_type,
                            "catalog": catalog,
                            "genres": [g.name for g in (getattr(details, "genres", None) or [])]
                        }
                        
                        await db.global_db["meta"].update_one(
                            {"_id": doc_id},
                            {"$set": update_data},
                            upsert=True
                        )
                        
                        combined = parse_combined_episodes(filename)
                        
                        file_data = {
                            "_id": file_id,
                            "meta_id": doc_id,
                            "filename": filename,
                            "size": size,
                            "size_str": get_readable_file_size(size),
                            "quality": parsed.get("resolution", "HD"),
                            "chat_id": chat_id,
                            "message_id": message.id,
                            "season": combined["season"] if combined else parsed.get("season"),
                            "episode_start": combined["start"] if combined else parsed.get("episode"),
                            "episode_end": combined["end"] if combined else parsed.get("episode")
                        }
                        
                        await db.global_db["files"].update_one(
                            {"_id": file_id},
                            {"$set": file_data},
                            upsert=True
                        )
                        # Remove from unindexed if it was previously there
                        await db.global_db["unindexed"].delete_one({"_id": file_id})
                        
                        count += 1
                        
                    # Save checkpoint
                    if max_id_seen > last_id:
                        await db.global_db["state"].update_one({"_id": sync_key}, {"$set": {"last_id": max_id_seen}}, upsert=True)
                        
                LOGGER.info(f"[GLOBAL INDEXER] Indexed {count} new files from {chat_id}.")
            except Exception as e:
                LOGGER.error(f"[GLOBAL INDEXER] Error scanning {chat_id}: {e}")
                
    except Exception as e:
        LOGGER.error(f"[GLOBAL INDEXER] Fatal Error: {e}")
    finally:
        _INDEXER_RUNNING = False
        LOGGER.info("[GLOBAL INDEXER] Finished.")
