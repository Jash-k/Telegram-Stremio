"""Automated Tamil PreDVD Leech-Group Automator & Quality Lifecycle Manager.

Monitors 1TamilMV movies.json feed for NEW Tamil PreDVD releases and:
1. Ignores pre-existing releases via a Baseline Snapshot.
2. Only leeches NEW PreDVD releases (Size >= min_size_mb, max_files_per_movie).
3. Sends `<command_prefix> <magnet>` to configured Telegram Leech Group.
4. Automatically purges old PreDVD streams from MongoDB when official WEB-DL/HD arrives (without leeching WEB-DL).
5. Stores all state in dedicated MongoDB collections (predvd_snapshot, predvd_history, predvd_settings).
"""
import asyncio
import re
import time
import urllib.parse
from typing import Optional, List, Dict, Any
import httpx
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from app import client as client_mod, config, db
from app.logger import LOGGER

def normalize_feed_url(url: str) -> str:
    """Normalizes GitHub web/blob/raw URLs into raw content URLs."""
    u = str(url or "").strip()
    if not u:
        return ""
    # Converts https://github.com/user/repo/blob/branch/path -> https://raw.githubusercontent.com/user/repo/branch/path
    # Converts https://github.com/user/repo/raw/branch/path -> https://raw.githubusercontent.com/user/repo/branch/path
    match = re.match(r"^https?://github\.com/([^/]+)/([^/]+)/(?:blob|raw)/(.+)$", u)
    if match:
        user, repo, path = match.groups()
        return f"https://raw.githubusercontent.com/{user}/{repo}/{path}"
    return u

def get_default_settings() -> Dict[str, Any]:
    return {
        "enabled": getattr(config, "PREDVD_ENABLED", True),
        "group_id": getattr(config, "PREDVD_GROUP_ID", ""),
        "command_prefix": getattr(config, "PREDVD_COMMAND_PREFIX", "/qbleech"),
        "feed_url": getattr(config, "PREDVD_FEED_URL", ""),
        "min_size_mb": getattr(config, "PREDVD_MIN_SIZE_MB", 800),
        "max_files_per_movie": getattr(config, "PREDVD_MAX_FILES_PER_MOVIE", 1),
        "preferred_quality": "720p AVC",
        "auto_purge_on_webdl": getattr(config, "PREDVD_AUTO_PURGE_ON_WEBDL", True),
        "poll_interval_minutes": getattr(config, "PREDVD_POLL_INTERVAL_MINUTES", 15),
    }

PREDVD_REGEX = re.compile(r'\b(predvd|pre[-\s]?dvd|camrip|cam[-\s]?rip|\bcam\b|hdcam|dvdscr|dvd[-\s]?scr|\bscr\b|hdtc|hd[-\s]?tc|hdts|hq[-\s]?ts|telesync|\bts\b|theatrical|theater[-\s]?print|cinema[-\s]?print)\b', re.IGNORECASE)
WEBDL_REGEX = re.compile(r'\b(web[-\s]?dl|webdl|bluray|blu[-\s]?ray|bd[-\s]?rip|br[-\s]?rip|hd[-\s]?rip|hdrip|hq\s*hdrip|true\s*web|dvd[-\s]?rip|dvdrip|\buhd\b|2160p|web[-\s]?hd)\b', re.IGNORECASE)
TAMIL_REGEX = re.compile(r'\b(tamil|tam|multi)\b', re.IGNORECASE)

_running = False
_task: Optional[asyncio.Task] = None
_last_check_time: Optional[float] = None
_last_check_status: str = "Idle"


def parse_size_to_mb(size_str: str) -> float:
    if not size_str:
        return 0.0
    match = re.search(r'([\d.]+)\s*(GB|MB|GiB|MiB)', str(size_str), re.IGNORECASE)
    if not match:
        return 0.0
    val = float(match.group(1))
    unit = match.group(2).upper()
    if 'GB' in unit or 'GIB' in unit:
        return val * 1024.0
    return val


def is_tamil_release(movie: Dict[str, Any]) -> bool:
    raw = f"{movie.get('rawText', '')} {movie.get('rawTitle', '')} {movie.get('name', '')} {movie.get('titleGuess', '')} {movie.get('pageUrl', '')}"
    languages = movie.get("languages") or []
    lang_str = " ".join(languages) if isinstance(languages, list) else str(languages)
    for q in (movie.get("qualities") or []):
        raw += f" {q.get('quality', '')} {q.get('url', '')}"
    return bool(TAMIL_REGEX.search(raw) or TAMIL_REGEX.search(lang_str))


def is_webdl_release(movie: Dict[str, Any]) -> bool:
    """Returns True if the release is an official digital/HD print (WEB-DL, HDRip, HQ HDRip, BluRay, UHD)."""
    raw = f"{movie.get('rawText', '')} {movie.get('rawTitle', '')} {movie.get('name', '')} {movie.get('titleGuess', '')} {movie.get('pageUrl', '')}"
    for q in (movie.get("qualities") or []):
        raw += f" {q.get('quality', '')} {q.get('url', '')}"
    return bool(WEBDL_REGEX.search(raw) and not PREDVD_REGEX.search(raw))


def is_predvd_release(movie: Dict[str, Any]) -> bool:
    """Returns True only if the release is strictly a theatrical/PreDVD/CAM rip (never official digital/HDRip/WEB-DL)."""
    if is_webdl_release(movie):
        return False
    raw = f"{movie.get('rawText', '')} {movie.get('rawTitle', '')} {movie.get('name', '')} {movie.get('titleGuess', '')} {movie.get('pageUrl', '')}"
    for q in (movie.get("qualities") or []):
        raw += f" {q.get('quality', '')} {q.get('url', '')}"
    return bool(PREDVD_REGEX.search(raw))


async def get_settings() -> Dict[str, Any]:
    defaults = get_default_settings()
    doc = await db.col("predvd_settings").find_one({"_id": "config"})
    if not doc:
        return defaults
    cfg = dict(defaults)
    cfg.update({k: v for k, v in doc.items() if k != "_id"})
    return cfg


async def save_settings(new_cfg: Dict[str, Any]) -> Dict[str, Any]:
    defaults = get_default_settings()
    cleaned = {}
    for k in defaults:
        if k in new_cfg:
            if k in ("enabled", "auto_purge_on_webdl"):
                cleaned[k] = bool(new_cfg[k])
            elif k in ("min_size_mb", "max_files_per_movie", "poll_interval_minutes"):
                cleaned[k] = int(new_cfg[k])
            else:
                cleaned[k] = str(new_cfg[k]).strip()
    await db.col("predvd_settings").update_one(
        {"_id": "config"},
        {"$set": cleaned},
        upsert=True
    )
    LOGGER.info("[PREDVD] Settings updated: %s", cleaned)
    return await get_settings()


async def fetch_feed(feed_url: Optional[str] = None) -> List[Dict[str, Any]]:
    settings = await get_settings() if not feed_url else {}
    raw_target = feed_url or settings.get("feed_url") or getattr(config, "PREDVD_FEED_URL", "")
    target_url = normalize_feed_url(raw_target)
    if not target_url:
        LOGGER.warning("[PREDVD] Feed URL is not configured. Please set PREDVD_FEED_URL in settings or config.env")
        return []
    try:
        async with httpx.AsyncClient(timeout=25.0, follow_redirects=True) as client:
            resp = await client.get(target_url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    return data
                LOGGER.warning("[PREDVD] Feed returned non-list JSON: %s", type(data))
                return []
            LOGGER.warning("[PREDVD] Feed fetch failed HTTP %s from %s", resp.status_code, target_url)
    except Exception as exc:
        LOGGER.error("[PREDVD] Feed fetch exception (%s): %s", target_url, exc)
    return []


async def ensure_baseline_snapshot(feed_items: List[Dict[str, Any]]) -> int:
    """Creates a baseline snapshot of existing movies so they are not leeched."""
    count = await db.col("predvd_snapshot").count_documents({})
    if count > 0:
        return count

    LOGGER.info("[PREDVD] Initializing Baseline Snapshot of %d existing feed movies...", len(feed_items))
    seen_keys = set()
    docs = []
    now = time.time()
    for m in feed_items:
        key = m.get("imdbId") or m.get("name") or m.get("titleGuess") or m.get("rawTitle")
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        docs.append({
            "_id": key,
            "title": m.get("name") or m.get("titleGuess") or key,
            "year": m.get("year") or m.get("yearGuess"),
            "imdb_id": m.get("imdbId"),
            "is_predvd": is_predvd_release(m),
            "is_webdl": is_webdl_release(m),
            "created_at": now,
            "is_baseline": True,
        })

    if docs:
        operations = [
            UpdateOne({"_id": doc["_id"]}, {"$setOnInsert": doc}, upsert=True)
            for doc in docs
        ]
        try:
            await db.col("predvd_snapshot").bulk_write(operations, ordered=False)
            LOGGER.info("[PREDVD] Baseline Snapshot established with %d unique movies. Pre-existing movies ignored.", len(docs))
        except BulkWriteError as bwe:
            LOGGER.warning("[PREDVD] BulkWriteError handled gracefully: %s", bwe.details.get("writeErrors", []))
    return len(docs)


async def reset_baseline_snapshot() -> int:
    """Manually re-captures current feed as baseline snapshot."""
    settings = await get_settings()
    feed_items = await fetch_feed(settings.get("feed_url"))
    await db.col("predvd_snapshot").delete_many({})
    return await ensure_baseline_snapshot(feed_items)


def select_qualities_for_leech(movie: Dict[str, Any], min_size_mb: int = 800, max_files: int = 1) -> List[Dict[str, Any]]:
    qualities = movie.get("qualities") or []
    candidates = []

    for q in qualities:
        url = q.get("url") or ""
        if not url.startswith("magnet:"):
            continue
        size_str = q.get("size") or ""
        size_mb = parse_size_to_mb(size_str)
        
        # Rule: Size >= min_size_mb
        if min_size_mb > 0 and size_mb < min_size_mb:
            continue

        q_label = (q.get("quality") or "").lower()
        seeders = int(q.get("seeders") or 0)
        
        # High-Priority Rule: 720p AVC (Maximum quality of 720p)
        score = 0
        if "720p" in q_label and ("avc" in q_label or "x264" in q_label or "h264" in q_label):
            score += 500  # Highest priority: 720p AVC
        elif "720p" in q_label:
            score += 300  # Secondary 720p (e.g. 720p HEVC)
        elif "1080p" in q_label and ("avc" in q_label or "x264" in q_label):
            score += 100
        elif "1080p" in q_label:
            score += 80
        else:
            score += 40

        # Prefer higher bitrate / larger size within the target tier + seeders
        score += min(int(size_mb / 40), 60)
        score += min(seeders, 30)

        candidates.append({
            "quality": q.get("quality") or "720p AVC",
            "size_str": size_str,
            "size_mb": size_mb,
            "seeders": seeders,
            "magnet_url": url,
            "score": score
        })

    # Sort descending by score and pick only the top requested files (default 1)
    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates[:max_files]


def normalize_chat_id(group_id: str):
    gid = str(group_id).strip()
    if gid.startswith("@"):
        return gid
    if gid.startswith("-100"):
        return int(gid)
    if gid.lstrip("-").isdigit():
        clean_num = gid.lstrip("-")
        if len(clean_num) >= 9 and not clean_num.startswith("100"):
            return int(f"-100{clean_num}")
        return int(gid if gid.startswith("-") else f"-100{gid}")
    return gid


async def send_leech_command(group_id: str, command_prefix: str, magnet_url: str, title: str, quality: str) -> tuple[bool, str]:
    """Sends `<command_prefix> <magnet>` to the target Leech Group."""
    if not group_id:
        err_msg = "Leech Group ID is not configured. Please set PREDVD_GROUP_ID in settings or config.env."
        LOGGER.warning("[PREDVD] %s", err_msg)
        return False, err_msg

    if not client_mod.is_connected():
        try:
            await client_mod.ensure_started()
        except Exception as exc:
            err_msg = f"Userbot is disconnected ({exc})"
            LOGGER.error("[PREDVD] %s", err_msg)
            return False, err_msg

    if not client_mod.is_connected() or not client_mod.client:
        err_msg = "Userbot is disconnected. Please check SESSION_STRING in settings."
        LOGGER.warning("[PREDVD] %s", err_msg)
        return False, err_msg

    try:
        chat_target = normalize_chat_id(group_id)
        cmd_text = f"{command_prefix.strip()} {magnet_url.strip()}"
        
        msg = await client_mod.client.send_message(chat_id=chat_target, text=cmd_text)
        LOGGER.info("[PREDVD LEECH SENT] '%s' (%s) -> Group %s (Message ID: %s)", title, quality, group_id, msg.id)
        return True, f"Sent to {group_id} (Msg #{msg.id})"
    except Exception as exc:
        err_msg = f"Telegram error: {exc}"
        LOGGER.error("[PREDVD] Failed to send leech command for '%s' to %s: %s", title, group_id, exc)
        return False, err_msg


async def purge_old_predvd_from_stremio(title: str, imdb_id: Optional[str] = None) -> int:
    """Purges old PreDVD file entries from MongoDB files collection when official WEB-DL arrives."""
    try:
        query_parts = []
        if imdb_id:
            meta_doc = await db.col("meta").find_one({"imdb_id": imdb_id}, {"_id": 1})
            if meta_doc:
                query_parts.append({"meta_id": meta_doc["_id"]})
        
        # Regex match on filename for PreDVD/CAM tags
        predvd_file_regex = re.compile(r'predvd|cam|dvdscr|hdtc|telesync', re.IGNORECASE)
        query = {
            "$and": [
                {"$or": [
                    {"filename": predvd_file_regex},
                    {"quality": {"$in": ["PreDVD", "CAM", "DVDScr", "HDTC"]}}
                ]}
            ]
        }
        if query_parts:
            query["$and"].append({"$or": query_parts})

        found_files = await db.col("files").find(query, {"_id": 1, "filename": 1, "meta_id": 1}).to_list(None)
        if not found_files:
            return 0

        file_ids = [f["_id"] for f in found_files]
        result = await db.col("files").delete_many({"_id": {"$in": file_ids}})
        deleted_count = result.deleted_count

        # Clean meta references
        meta_ids = list({f.get("meta_id") for f in found_files if f.get("meta_id")})
        for mid in meta_ids:
            # Check if meta still has files
            remaining = await db.col("files").count_documents({"meta_id": mid})
            if remaining == 0:
                await db.col("meta").delete_one({"_id": mid})

        LOGGER.info("[PREDVD AUTO-PURGE] Removed %d old PreDVD streams for '%s' (Official WEB-DL/HD arrived)", deleted_count, title)
        
        # Record purge event in history
        await db.col("predvd_history").insert_one({
            "action": "purged_predvd",
            "title": title,
            "imdb_id": imdb_id,
            "files_removed": deleted_count,
            "timestamp": time.time()
        })

        return deleted_count
    except Exception as exc:
        LOGGER.error("[PREDVD] Error purging old PreDVD for '%s': %s", title, exc)
        return 0


async def process_feed_iteration() -> Dict[str, Any]:
    """Runs a single feed check cycle."""
    global _last_check_time, _last_check_status
    _last_check_time = time.time()
    _last_check_status = "Running"

    settings = await get_settings()
    if not settings.get("enabled", True):
        _last_check_status = "Disabled in settings"
        return {"ok": True, "status": "disabled"}

    feed_items = await fetch_feed()
    if not feed_items:
        _last_check_status = "Feed fetch returned 0 items"
        return {"ok": False, "error": "empty_feed"}

    # Ensure baseline snapshot is established
    await ensure_baseline_snapshot(feed_items)

    group_id = settings.get("group_id") or getattr(config, "PREDVD_GROUP_ID", "")
    command_prefix = settings.get("command_prefix") or getattr(config, "PREDVD_COMMAND_PREFIX", "/qbleech")
    min_size_mb = int(settings.get("min_size_mb", 800))
    max_files = int(settings.get("max_files_per_movie", 1))
    auto_purge = settings.get("auto_purge_on_webdl", True)

    if not group_id:
        _last_check_status = "Leech group not configured (set PREDVD_GROUP_ID)"
        LOGGER.warning("[PREDVD] Leech Group ID is not configured.")
        return {"ok": False, "error": "group_not_configured"}

    leeched_count = 0
    purged_count = 0

    for m in feed_items:
        key = m.get("imdbId") or m.get("name") or m.get("titleGuess") or m.get("rawTitle")
        if not key:
            continue

        title = m.get("name") or m.get("titleGuess") or m.get("rawTitle") or key
        imdb_id = m.get("imdbId")
        is_tamil = is_tamil_release(m)
        is_predvd = is_predvd_release(m)
        is_webdl = is_webdl_release(m)

        # Check existing snapshot record
        snapshot_doc = await db.col("predvd_snapshot").find_one({"_id": key})

        # 1. AUTO-PURGE CHECK: Movie was previously tracked as PreDVD, but now has Official WEB-DL / HD print!
        if is_webdl and snapshot_doc and snapshot_doc.get("is_predvd"):
            if auto_purge and not snapshot_doc.get("purged_predvd"):
                removed = await purge_old_predvd_from_stremio(title, imdb_id)
                await db.col("predvd_snapshot").update_one(
                    {"_id": key},
                    {"$set": {"is_predvd": False, "is_webdl": True, "purged_predvd": True, "purged_at": time.time()}}
                )
                if removed > 0:
                    purged_count += 1
            continue

        # 2. SKIP IF NOT TAMIL OR NOT PREDVD
        if not is_tamil or not is_predvd:
            continue

        # 3. BASELINE SNAPSHOT CHECK: If movie was part of baseline, ignore it!
        if snapshot_doc and snapshot_doc.get("is_baseline"):
            continue

        # 4. ALREADY LEECHED CHECK
        if snapshot_doc and snapshot_doc.get("leeched"):
            continue

        # 5. NEW PREDVD RELEASE DETECTED! Select top qualities (Size >= 1GB, max 2 files)
        selected_qualities = select_qualities_for_leech(m, min_size_mb, max_files)
        if not selected_qualities:
            continue

        LOGGER.info("[PREDVD NEW RELEASE] '%s' (%s) - Found %d eligible qualities (>= %d MB)", title, m.get("year", ""), len(selected_qualities), min_size_mb)

        sent_qualities = []
        for q in selected_qualities:
            success, info = await send_leech_command(
                group_id=group_id,
                command_prefix=command_prefix,
                magnet_url=q["magnet_url"],
                title=title,
                quality=q["quality"]
            )
            if success:
                q["info"] = info
                sent_qualities.append(q)
                # Polite spacing between commands to prevent Telegram FloodWait
                await asyncio.sleep(8.0)

        if sent_qualities:
            leeched_count += 1
            now = time.time()
            # Record in snapshot
            await db.col("predvd_snapshot").update_one(
                {"_id": key},
                {"$set": {
                    "title": title,
                    "year": m.get("year") or m.get("yearGuess"),
                    "imdb_id": imdb_id,
                    "is_predvd": True,
                    "leeched": True,
                    "leeched_at": now,
                    "sent_qualities": sent_qualities
                }},
                upsert=True
            )
            # Record in history log
            await db.col("predvd_history").insert_one({
                "action": "leech_sent",
                "key": key,
                "title": title,
                "imdb_id": imdb_id,
                "year": m.get("year") or m.get("yearGuess"),
                "qualities": sent_qualities,
                "timestamp": now
            })

    _last_check_status = f"Idle (Last cycle: {leeched_count} leeched, {purged_count} PreDVDs purged)"
    return {"ok": True, "leeched": leeched_count, "purged": purged_count}


async def _automator_loop():
    LOGGER.info("[PREDVD] PreDVD Automator background task started.")
    # Initial pause for DB & Userbot startup
    await asyncio.sleep(15)
    while _running:
        try:
            settings = await get_settings()
            poll_mins = int(settings.get("poll_interval_minutes", 15))
            await process_feed_iteration()
            await asyncio.sleep(max(5, poll_mins) * 60)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            LOGGER.error("[PREDVD] Loop exception: %s", exc)
            await asyncio.sleep(60)


def start():
    global _running, _task
    if _running:
        return
    _running = True
    _task = asyncio.create_task(_automator_loop())


def stop():
    global _running, _task
    _running = False
    if _task:
        _task.cancel()
        _task = None


def get_status() -> Dict[str, Any]:
    return {
        "running": _running,
        "last_check_time": _last_check_time,
        "last_check_status": _last_check_status,
    }
