"""Automated Tamil PreDVD Leech-Group Automator & Quality Lifecycle Manager.

Monitors 1TamilMV movies.json feed for NEW Tamil PreDVD releases and:
1. Ignores pre-existing releases via a Baseline Snapshot.
2. Only leeches NEW PreDVD releases (Size >= 1.0 GB, max 2 files per movie).
3. Sends `/qbleech@AmitPremium_leechbot <magnet>` to group `-1002695497393`.
4. Automatically purges old PreDVD streams from MongoDB when official WEB-DL/HD arrives (without leeching WEB-DL).
5. Stores all state in dedicated MongoDB collections (predvd_snapshot, predvd_history, predvd_settings).
"""
import asyncio
import re
import time
from typing import Optional, List, Dict, Any
import httpx

from app import client as client_mod, db
from app.logger import LOGGER

FEED_URL = "https://raw.githubusercontent.com/cold-logic5/TMV_Stremio_Addon/refs/heads/main/data/movies.json"

DEFAULT_SETTINGS = {
    "enabled": True,
    "group_id": "-1002695497393",
    "command_prefix": "/qbleech@AmitPremium_leechbot",
    "min_size_mb": 1000,
    "max_files_per_movie": 2,
    "auto_purge_on_webdl": True,
    "poll_interval_minutes": 15,
}

PREDVD_REGEX = re.compile(r'\b(predvd|cam|dvdscr|hdtc|telesync|ts\b|theatrical|early|hq clean)\b', re.IGNORECASE)
WEBDL_REGEX = re.compile(r'\b(web-dl|webdl|bluray|hdrip|true web|bd-rip|brrip|dvdrip)\b', re.IGNORECASE)
TAMIL_REGEX = re.compile(r'\b(tamil|multi)\b', re.IGNORECASE)

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
    raw = movie.get("rawText") or movie.get("rawTitle") or movie.get("name") or ""
    languages = movie.get("languages") or []
    lang_str = " ".join(languages) if isinstance(languages, list) else str(languages)
    return bool(TAMIL_REGEX.search(raw) or TAMIL_REGEX.search(lang_str))


def is_predvd_release(movie: Dict[str, Any]) -> bool:
    raw = movie.get("rawText") or movie.get("rawTitle") or ""
    if PREDVD_REGEX.search(raw):
        return True
    for q in (movie.get("qualities") or []):
        if PREDVD_REGEX.search(q.get("quality", "")):
            return True
    return False


def is_webdl_release(movie: Dict[str, Any]) -> bool:
    raw = movie.get("rawText") or movie.get("rawTitle") or ""
    if WEBDL_REGEX.search(raw) and not PREDVD_REGEX.search(raw):
        return True
    for q in (movie.get("qualities") or []):
        if WEBDL_REGEX.search(q.get("quality", "")) and not PREDVD_REGEX.search(q.get("quality", "")):
            return True
    return False


async def get_settings() -> Dict[str, Any]:
    doc = await db.col("predvd_settings").find_one({"_id": "config"})
    if not doc:
        return dict(DEFAULT_SETTINGS)
    cfg = dict(DEFAULT_SETTINGS)
    cfg.update({k: v for k, v in doc.items() if k != "_id"})
    return cfg


async def save_settings(new_cfg: Dict[str, Any]) -> Dict[str, Any]:
    cleaned = {}
    for k in DEFAULT_SETTINGS:
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


async def fetch_feed() -> List[Dict[str, Any]]:
    try:
        async with httpx.AsyncClient(timeout=25.0) as client:
            resp = await client.get(FEED_URL)
            if resp.status_code == 200:
                return resp.json()
            LOGGER.warning("[PREDVD] Feed fetch failed HTTP %s", resp.status_code)
    except Exception as exc:
        LOGGER.error("[PREDVD] Feed fetch exception: %s", exc)
    return []


async def ensure_baseline_snapshot(feed_items: List[Dict[str, Any]]) -> int:
    """Creates a baseline snapshot of existing movies so they are not leeched."""
    count = await db.col("predvd_snapshot").count_documents({})
    if count > 0:
        return count

    LOGGER.info("[PREDVD] Initializing Baseline Snapshot of %d existing feed movies...", len(feed_items))
    docs = []
    now = time.time()
    for m in feed_items:
        key = m.get("imdbId") or m.get("name") or m.get("titleGuess") or m.get("rawTitle")
        if not key:
            continue
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
        await db.col("predvd_snapshot").insert_many(docs, ordered=False)
        LOGGER.info("[PREDVD] Baseline Snapshot established with %d movies. Pre-existing movies ignored.", len(docs))
    return len(docs)


async def reset_baseline_snapshot() -> int:
    """Manually re-captures current feed as baseline snapshot."""
    feed_items = await fetch_feed()
    await db.col("predvd_snapshot").delete_many({})
    return await ensure_baseline_snapshot(feed_items)


def select_qualities_for_leech(movie: Dict[str, Any], min_size_mb: int, max_files: int) -> List[Dict[str, Any]]:
    qualities = movie.get("qualities") or []
    candidates = []

    for q in qualities:
        url = q.get("url") or ""
        if not url.startswith("magnet:"):
            continue
        size_str = q.get("size") or ""
        size_mb = parse_size_to_mb(size_str)
        
        # Rule: Size >= min_size_mb (1000 MB)
        if size_mb < min_size_mb:
            continue

        q_label = q.get("quality") or ""
        seeders = int(q.get("seeders") or 0)
        
        # Score by resolution priority (1080p > 720p > others) + seeders
        score = 0
        if "1080p" in q_label.lower():
            score += 100
        elif "720p" in q_label.lower():
            score += 80
        elif "4k" in q_label.lower() or "2160p" in q_label.lower():
            score += 60
        else:
            score += 40

        score += min(seeders, 50)

        candidates.append({
            "quality": q_label,
            "size_str": size_str,
            "size_mb": size_mb,
            "seeders": seeders,
            "magnet_url": url,
            "score": score
        })

    # Sort descending by score and pick at most max_files (e.g. 2)
    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates[:max_files]


async def send_leech_command(group_id: str, command_prefix: str, magnet_url: str, title: str, quality: str) -> bool:
    """Sends `/qbleech@AmitPremium_leechbot <magnet>` to the target Leech Group."""
    if not client_mod.is_connected():
        LOGGER.warning("[PREDVD] Cannot send leech command: Userbot is disconnected.")
        return False

    try:
        chat_target = int(group_id) if group_id.startswith("-100") or group_id.lstrip('-').isdigit() else group_id
        cmd_text = f"{command_prefix.strip()} {magnet_url.strip()}"
        
        msg = await client_mod.client.send_message(chat_id=chat_target, text=cmd_text)
        LOGGER.info("[PREDVD LEECH SENT] '%s' (%s) -> Group %s (Message ID: %s)", title, quality, group_id, msg.id)
        return True
    except Exception as exc:
        LOGGER.error("[PREDVD] Failed to send leech command for '%s': %s", title, exc)
        return False


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

    group_id = settings.get("group_id", "-1002695497393")
    command_prefix = settings.get("command_prefix", "/qbleech@AmitPremium_leechbot")
    min_size_mb = int(settings.get("min_size_mb", 1000))
    max_files = int(settings.get("max_files_per_movie", 2))
    auto_purge = settings.get("auto_purge_on_webdl", True)

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
            success = await send_leech_command(
                group_id=group_id,
                command_prefix=command_prefix,
                magnet_url=q["magnet_url"],
                title=title,
                quality=q["quality"]
            )
            if success:
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
