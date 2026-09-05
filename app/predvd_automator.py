"""Tamil PreDVD Auto-Leecher — SEND-ONLY automator.

Single responsibility: monitor the 1TamilMV feed for NEW Tamil PreDVD releases
and send the configured leech command (e.g. `/qbleech <magnet>`) to the leech
supergroup. The leech bot then uploads the finished file into a DUMP CHANNEL
that the global indexer watches, so:

    feed ──(this automator, sends command)──► leech GROUP (noisy; ignored)
                                                   │ leech bot dumps finished file
                                                   ▼
                                           dump CHANNEL (quiet) ──► global indexer ──► GlobalDB

Everything else (indexing, best-of-3, removing PreDVD once the official
digital release arrives, deleting old messages) is owned by the global
indexer / cleanup — NOT here. This module deliberately contains no
delete/purge logic against the media DB.

Priority + queue:
  * The global indexer always has priority. While it is running, this
    automator does NOT send; leech requests are persisted to a Mongo queue
    (restart-safe) and drained FIFO as soon as the indexer is idle. Nothing is
    ever dropped.
"""
import asyncio
import re
import time
import urllib.parse  # noqa: F401 (kept for compatibility with older imports)
import uuid
from typing import Optional, List, Dict, Any

import httpx

from app import client as client_mod, config, db
from app.logger import LOGGER

_QUEUE_COL = "predvd_leech_queue"
_QUEUE_BATCH = 20


# ---------------------------------------------------------------------------
# Feed URL normalisation
# ---------------------------------------------------------------------------

def normalize_feed_url(url: str) -> str:
    u = str(url or "").strip()
    if not u:
        return ""
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
        "poll_interval_minutes": getattr(config, "PREDVD_POLL_INTERVAL_MINUTES", 15),
        # Extra leech targets for MANUAL leeching only (the Digital tab & the
        # manual PreDVD buttons). The primary group_id/command_prefix above is
        # always the AUTO-leech target and stays first in this list. Each entry:
        # {"label": str, "group_id": str, "command_prefix": str, "enabled": bool}
        "manual_groups": [],
    }


async def get_leech_targets() -> List[Dict[str, Any]]:
    """All leech destinations for the manual-leech group picker.

    Index 0 is always the primary (auto-leech) group. Extra MANUAL-only groups
    come from settings["manual_groups"]. Disabled extra groups are omitted.
    """
    settings = await get_settings()
    targets: List[Dict[str, Any]] = [{
        "label": "Primary (auto-leech)",
        "group_id": settings.get("group_id") or "",
        "command_prefix": settings.get("command_prefix") or "/qbleech",
        "auto": True,
        "enabled": True,
    }]
    for i, g in enumerate(settings.get("manual_groups") or []):
        gid = str(g.get("group_id") or "").strip()
        if not gid:
            continue
        if g.get("enabled", True) is False:
            continue
        targets.append({
            "label": str(g.get("label") or f"Group {i + 1}").strip() or f"Group {i + 1}",
            "group_id": gid,
            "command_prefix": str(g.get("command_prefix") or settings.get("command_prefix") or "/qbleech").strip(),
            "auto": False,
            "enabled": True,
        })
    return targets


# ---------------------------------------------------------------------------
# Release classification
# ---------------------------------------------------------------------------

PREDVD_REGEX = re.compile(r'\b(predvd|pre[-\s]?dvd|camrip|cam[-\s]?rip|\bcam\b|hdcam|dvdscr|dvd[-\s]?scr|\bscr\b|hdtc|hd[-\s]?tc|hdts|hq[-\s]?ts|telesync|\bts\b|theatrical|theater[-\s]?print|cinema[-\s]?print)\b', re.IGNORECASE)
WEBDL_REGEX = re.compile(r'\b(web[-\s]?dl|webdl|bluray|blu[-\s]?ray|bd[-\s]?rip|br[-\s]?rip|hd[-\s]?rip|hdrip|hq\s*hdrip|true\s*web|dvd[-\s]?rip|dvdrip|\buhd\b|2160p|web[-\s]?hd)\b', re.IGNORECASE)
TAMIL_REGEX = re.compile(r'\b(tamil|tam|multi)\b', re.IGNORECASE)

_running = False
_iteration_lock = asyncio.Lock()
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


def _raw_text(movie: Dict[str, Any]) -> str:
    raw = f"{movie.get('rawText', '')} {movie.get('rawTitle', '')} {movie.get('name', '')} {movie.get('titleGuess', '')} {movie.get('pageUrl', '')}"
    languages = movie.get("languages") or []
    lang_str = " ".join(languages) if isinstance(languages, list) else str(languages)
    for q in (movie.get("qualities") or []):
        raw += f" {q.get('quality', '')} {q.get('url', '')}"
    return raw + " " + lang_str


def is_tamil_release(movie: Dict[str, Any]) -> bool:
    return bool(TAMIL_REGEX.search(_raw_text(movie)))


def is_webdl_release(movie: Dict[str, Any]) -> bool:
    """True for an official digital/HD print (WEB-DL, HDRip, BluRay, UHD)."""
    raw = _raw_text(movie)
    return bool(WEBDL_REGEX.search(raw) and not PREDVD_REGEX.search(raw))


def is_predvd_release(movie: Dict[str, Any]) -> bool:
    """True only for a theatrical/PreDVD/CAM rip (never a digital/HDRip/WEB-DL)."""
    if is_webdl_release(movie):
        return False
    return bool(PREDVD_REGEX.search(_raw_text(movie)))


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

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
    # Normalize extra manual leech groups first (list, not a scalar).
    if "manual_groups" in new_cfg:
        groups = []
        seen = set()
        for g in new_cfg.get("manual_groups") or []:
            if not isinstance(g, dict):
                continue
            gid = str(g.get("group_id") or "").strip()
            pfx = str(g.get("command_prefix") or "").strip()
            if not gid or gid in seen:
                continue
            seen.add(gid)
            groups.append({
                "label": str(g.get("label") or "").strip()[:60],
                "group_id": gid,
                "command_prefix": pfx or "/qbleech",
                "enabled": bool(g.get("enabled", True)),
            })
        new_cfg = {**new_cfg, "manual_groups": groups}
    cleaned = {}
    for k in defaults:
        if k in new_cfg:
            if k == "enabled":
                cleaned[k] = bool(new_cfg[k])
            elif k == "manual_groups":
                cleaned[k] = new_cfg[k]
            elif k in ("min_size_mb", "max_files_per_movie", "poll_interval_minutes"):
                cleaned[k] = int(new_cfg[k])
            else:
                cleaned[k] = str(new_cfg[k]).strip()
    await db.col("predvd_settings").update_one({"_id": "config"}, {"$set": cleaned}, upsert=True)
    LOGGER.info("[PREDVD] Settings updated: %s", cleaned)
    return await get_settings()


# ---------------------------------------------------------------------------
# Feed fetch
# ---------------------------------------------------------------------------

async def fetch_feed(feed_url: Optional[str] = None) -> List[Dict[str, Any]]:
    settings = await get_settings() if not feed_url else {}
    raw_target = feed_url or settings.get("feed_url") or getattr(config, "PREDVD_FEED_URL", "")
    target_url = normalize_feed_url(raw_target)
    if not target_url:
        LOGGER.warning("[PREDVD] Feed URL is not configured. Set PREDVD_FEED_URL in settings or config.env")
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


# ---------------------------------------------------------------------------
# Baseline snapshot (existing releases are not auto-leeched)
# ---------------------------------------------------------------------------

def _movie_key(m: Dict[str, Any]):
    return m.get("imdbId") or m.get("name") or m.get("titleGuess") or m.get("rawTitle")


# ---------------------------------------------------------------------------
# Stable dedup ledger (survives imdbId flip-flops & title punctuation)
# ---------------------------------------------------------------------------
_SENT_COL = "predvd_sent"


def _norm_title(title: str) -> str:
    """Lowercase, letters/digits only — punctuation/apostrophe/whitespace safe.

    Apostrophes are removed (joining letters: "I'm" -> "im"); other punctuation
    becomes a separator, so title variants across scrapes collapse to one key.
    """
    t = str(title or "").lower().replace("'", "").replace("’", "")
    return re.sub(r"[^a-z0-9]+", " ", t).strip()


def _title_year_key(title: str, year) -> Optional[str]:
    norm = _norm_title(title)
    if not norm:
        return None
    try:
        y = int(year)
    except (TypeError, ValueError):
        y = None
    return f"{norm}|{y}" if y else norm


def _magnet_btih(magnet: str) -> Optional[str]:
    """Extract the BTIH info-hash from a magnet link (stable magnet identity)."""
    try:
        qs = str(magnet).split("?", 1)[1]
        params = dict(p.split("=", 1) for p in qs.split("&") if "=" in p)
        xt = params.get("xt", "")
        m = re.search(r"btih:([a-zA-Z0-9]+)", xt)
        return m.group(1).lower() if m else None
    except Exception:
        return None


async def already_leeched(title: str, year, magnet_url: str, auto: bool) -> bool:
    """True if this title+year (or this exact magnet) was already sent.

    Auto-leeches are blocked on either the stable title+year key or the magnet
    btih. Manual clicks are blocked only on an identical magnet (so you can still
    intentionally re-send a movie), but the same magnet never double-sends.
    """
    try:
        ty = _title_year_key(title, year)
        btih = _magnet_btih(magnet_url)
        ors = []
        if btih:
            ors.append({"_id": f"btih:{btih}"})
        if auto:
            if ty:
                ors.append({"_id": f"ty:{ty}"})
        if not ors:
            return False
        return await db.col(_SENT_COL).find_one({"$or": ors}, {"_id": 1}) is not None
    except Exception as exc:
        LOGGER.warning("[PREDVD] dedup check failed (allowing send): %s", exc)
        return False


async def record_sent(title: str, year, magnet_url: str, auto: bool, info: str = "") -> None:
    """Persist that a leech command was sent (idempotent on _id)."""
    now = time.time()
    docs = []
    ty = _title_year_key(title, year)
    if auto and ty:
        docs.append({"_id": f"ty:{ty}", "kind": "title_year", "title": title,
                     "year": year, "magnet": magnet_url, "auto": auto, "info": info, "ts": now})
    btih = _magnet_btih(magnet_url)
    if btih:
        docs.append({"_id": f"btih:{btih}", "kind": "btih", "title": title,
                     "year": year, "magnet": magnet_url, "auto": auto, "info": info, "ts": now})
    for d in docs:
        try:
            await db.col(_SENT_COL).update_one({"_id": d["_id"]}, {"$setOnInsert": d}, upsert=True)
        except Exception as exc:
            LOGGER.debug("[PREDVD] ledger write failed: %s", exc)


async def ensure_baseline_snapshot(feed_items: List[Dict[str, Any]]) -> int:
    count = await db.col("predvd_snapshot").count_documents({})
    if count > 0:
        return count

    LOGGER.info("[PREDVD] Initializing baseline snapshot of %d feed movies…", len(feed_items))
    seen = set()
    docs = []
    now = time.time()
    for m in feed_items:
        key = _movie_key(m)
        if not key or key in seen:
            continue
        seen.add(key)
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
        from pymongo import UpdateOne
        from pymongo.errors import BulkWriteError
        operations = [UpdateOne({"_id": d["_id"]}, {"$setOnInsert": d}, upsert=True) for d in docs]
        try:
            await db.col("predvd_snapshot").bulk_write(operations, ordered=False)
            LOGGER.info("[PREDVD] Baseline established with %d movies; pre-existing releases ignored.", len(docs))
        except BulkWriteError as bwe:
            LOGGER.warning("[PREDVD] Baseline bulk write issue: %s", bwe.details.get("writeErrors", []))
    return len(docs)


async def reset_baseline_snapshot() -> int:
    settings = await get_settings()
    feed_items = await fetch_feed(settings.get("feed_url"))
    await db.col("predvd_snapshot").delete_many({})
    return await ensure_baseline_snapshot(feed_items)


# ---------------------------------------------------------------------------
# Quality selection
# ---------------------------------------------------------------------------

def select_qualities_for_leech(movie: Dict[str, Any], min_size_mb: int = 800, max_files: int = 1) -> List[Dict[str, Any]]:
    qualities = movie.get("qualities") or []
    candidates = []
    for q in qualities:
        url = q.get("url") or ""
        if not url.startswith("magnet:"):
            continue
        size_str = q.get("size") or ""
        size_mb = parse_size_to_mb(size_str)
        if min_size_mb > 0 and size_mb < min_size_mb:
            continue

        q_label = (q.get("quality") or "").lower()
        seeders = int(q.get("seeders") or 0)

        score = 0
        if "720p" in q_label and ("avc" in q_label or "x264" in q_label or "h264" in q_label):
            score += 500
        elif "720p" in q_label:
            score += 300
        elif "1080p" in q_label and ("avc" in q_label or "x264" in q_label):
            score += 100
        elif "1080p" in q_label:
            score += 80
        else:
            score += 40
        score += min(int(size_mb / 40), 60)
        score += min(seeders, 30)

        candidates.append({
            "quality": q.get("quality") or "720p AVC",
            "size_str": size_str,
            "size_mb": size_mb,
            "seeders": seeders,
            "magnet_url": url,
            "score": score,
        })

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


# ---------------------------------------------------------------------------
# Sending
# ---------------------------------------------------------------------------

async def send_leech_command(group_id: str, command_prefix: str, magnet_url: str, title: str, quality: str) -> tuple[bool, str]:
    """Send `<command_prefix> <magnet>` to the leech group. Returns (ok, info)."""
    if not group_id:
        msg = "Leech Group ID is not configured. Set PREDVD_GROUP_ID in settings."
        LOGGER.warning("[PREDVD] %s", msg)
        return False, msg

    if not client_mod.is_connected():
        try:
            await client_mod.ensure_started()
        except Exception as exc:
            return False, f"Userbot is disconnected ({exc})"

    if not client_mod.is_connected() or not client_mod.client:
        return False, "Userbot is disconnected. Please check SESSION_STRING in settings."

    try:
        chat_target = normalize_chat_id(group_id)
        cmd_text = f"{command_prefix.strip()} {magnet_url.strip()}"
        msg = await client_mod.client.send_message(chat_id=chat_target, text=cmd_text)
        LOGGER.info("[PREDVD LEECH SENT] '%s' (%s) -> group %s (msg %s)", title, quality, group_id, msg.id)
        return True, f"Sent to {group_id} (Msg #{msg.id})"
    except Exception as exc:
        LOGGER.error("[PREDVD] Failed to send leech for '%s' to %s: %s", title, group_id, exc)
        return False, f"Telegram error: {exc}"


# ---------------------------------------------------------------------------
# Indexer-priority gate + restart-safe queue
# ---------------------------------------------------------------------------

def indexer_is_busy() -> bool:
    """True while the global indexer is running (it always gets priority)."""
    try:
        from . import indexer
        return bool(indexer.status().get("running"))
    except Exception:
        return False


async def _enqueue_queued_leech(key, title, quality, magnet_url, group_id, command_prefix,
                                year=None, auto=True) -> None:
    """Persist a leech request so it survives restarts and sends when idle."""
    doc = {
        "_id": uuid.uuid4().hex,
        "key": key or None,
        "title": title,
        "quality": quality,
        "magnet_url": magnet_url,
        "group_id": group_id,
        "command_prefix": command_prefix,
        "year": year,
        "auto": bool(auto),
        "status": "queued",
        "created_at": time.time(),
    }
    # Dedup by magnet so repeated ticks don't double-queue.
    await db.col(_QUEUE_COL).update_one(
        {"magnet_url": magnet_url},
        {"$setOnInsert": doc},
        upsert=True,
    )


async def queue_depth() -> int:
    try:
        return await db.col(_QUEUE_COL).count_documents({})
    except Exception:
        return 0


async def _record_leech_history(key, title, quality, magnet_url, info) -> None:
    now = time.time()
    if key:
        await db.col("predvd_snapshot").update_one(
            {"_id": key},
            {"$set": {"leeched": True, "queued": False, "leeched_at": now, "title": title}},
            upsert=True,
        )
    await db.col("predvd_history").insert_one({
        "action": "leech_sent",
        "key": key,
        "title": title,
        "quality": quality,
        "magnet_url": magnet_url,
        "timestamp": now,
        "info": info,
    })


async def drain_queued_leeches() -> int:
    """Send queued leech commands FIFO, but only while the indexer is idle."""
    if indexer_is_busy():
        return 0
    try:
        docs = await db.col(_QUEUE_COL).find({}).sort("created_at", 1).limit(_QUEUE_BATCH).to_list(_QUEUE_BATCH)
    except Exception as exc:
        LOGGER.error("[PREDVD] queue read failed: %s", exc)
        return 0

    sent = 0
    for d in docs:
        if indexer_is_busy():
            break  # indexer started mid-drain; leave the rest for the next tick
        # Guard against a queued item whose duplicate already went out (e.g. it
        # was sent by another path while queued). Drop it rather than re-send.
        is_auto = d.get("auto", True)
        if await already_leeched(d["title"], d.get("year"), d["magnet_url"], is_auto):
            LOGGER.info("[PREDVD] queued '%s' already leeched — dropping duplicate", d["title"])
            await db.col(_QUEUE_COL).delete_one({"_id": d["_id"]})
            continue
        ok, info = await send_leech_command(d["group_id"], d["command_prefix"], d["magnet_url"], d["title"], d["quality"])
        if not ok:
            LOGGER.warning("[PREDVD] queued send failed (%s); will retry next tick", info)
            break
        sent += 1
        await record_sent(d["title"], d.get("year"), d["magnet_url"], is_auto, info)
        await db.col(_QUEUE_COL).delete_one({"_id": d["_id"]})
        await _record_leech_history(d.get("key"), d["title"], d["quality"], d["magnet_url"], info)
        await asyncio.sleep(2.0)
    if sent:
        LOGGER.info("[PREDVD] drained %d queued leech command(s)", sent)
    return sent


async def request_leech(magnet_url: str, title: str, quality: str, key=None,
                        group_id: Optional[str] = None, command_prefix: Optional[str] = None,
                        year=None, auto: bool = True) -> Dict[str, Any]:
    """Gate-aware leech used by BOTH the automator and manual panel leeches.

    * Indexer busy  -> persist to queue, return queued=True.
    * Indexer idle  -> send now.

    Manual leeches may override `group_id`/`command_prefix` to target one of the
    extra leech groups; the automator leaves both None and uses the primary.
    `auto=True` (automator) is blocked on a previously-seen title+year or magnet;
    `auto=False` (manual panel click) is blocked only on an identical magnet.
    """
    magnet_url = (magnet_url or "").strip()
    title = (title or "Manual Leech").strip()
    quality = (quality or "PreDVD").strip()
    if not magnet_url.startswith("magnet:"):
        return {"ok": False, "queued": False, "error": "Invalid magnet URL (must start with magnet:)"}

    # Stable duplicate guard (survives imdbId flip-flops / title punctuation).
    if await already_leeched(title, year, magnet_url, auto):
        msg = ("Already leeched this release — skipping duplicate." if auto
               else "This exact magnet was already sent — skipping duplicate.")
        LOGGER.info("[PREDVD] skip duplicate leech for '%s' (auto=%s)", title, auto)
        return {"ok": True, "queued": False, "duplicate": True, "message": msg}

    settings = await get_settings()
    group_id = (group_id or "").strip()
    command_prefix = (command_prefix or "").strip()
    if group_id:
        # Manual target chosen — make sure its command prefix is valid.
        command_prefix = command_prefix or settings.get("command_prefix") or "/qbleech"
    else:
        group_id = settings.get("group_id") or getattr(config, "PREDVD_GROUP_ID", "")
        command_prefix = settings.get("command_prefix") or getattr(config, "PREDVD_COMMAND_PREFIX", "/qbleech")
    if not group_id:
        return {"ok": False, "queued": False, "error": "Leech group not configured (set PREDVD_GROUP_ID)."}

    if indexer_is_busy():
        await _enqueue_queued_leech(key, title, quality, magnet_url, group_id, command_prefix,
                                    year=year, auto=auto)
        if key:
            await db.col("predvd_snapshot").update_one(
                {"_id": key},
                {"$set": {"queued": True, "leeched": False, "title": title, "queued_at": time.time()}},
                upsert=True,
            )
        LOGGER.info("[PREDVD] indexer busy — queued leech for '%s' [%s]", title, quality)
        return {"ok": True, "queued": True,
                "message": f"Queued — indexer is running. Will send to {group_id} automatically when it finishes."}

    ok, info = await send_leech_command(group_id, command_prefix, magnet_url, title, quality)
    if not ok:
        return {"ok": False, "queued": False, "error": info}
    await record_sent(title, year, magnet_url, auto, info)
    await _record_leech_history(key, title, quality, magnet_url, info)
    return {"ok": True, "queued": False, "message": f"Leech command sent for {title}! ({info})"}


# ---------------------------------------------------------------------------
# Feed iteration (SEND ONLY)
# ---------------------------------------------------------------------------

async def process_feed_iteration() -> Dict[str, Any]:
    """Public entry: serialized so a manual sync and an automator tick never run
    a feed iteration concurrently (which could double-send)."""
    if _iteration_lock.locked():
        return {"ok": True, "skipped": "already_running"}
    async with _iteration_lock:
        return await _process_feed_iteration_impl()


async def _process_feed_iteration_impl() -> Dict[str, Any]:
    global _last_check_time, _last_check_status
    _last_check_time = time.time()
    _last_check_status = "Running"

    # Always send anything queued first (only when the indexer is idle).
    await drain_queued_leeches()

    settings = await get_settings()
    if not settings.get("enabled", True):
        _last_check_status = "Disabled in settings"
        return {"ok": True, "status": "disabled"}

    feed_items = await fetch_feed()
    if not feed_items:
        _last_check_status = "Feed fetch returned 0 items"
        return {"ok": False, "error": "empty_feed"}

    await ensure_baseline_snapshot(feed_items)

    min_size_mb = int(settings.get("min_size_mb", 800))
    max_files = int(settings.get("max_files_per_movie", 1))

    leeched = 0
    queued = 0

    for m in feed_items:
        key = _movie_key(m)
        if not key:
            continue
        title = m.get("name") or m.get("titleGuess") or m.get("rawTitle") or key

        # Only AUTO-leech Tamil PreDVDs here. Digital/WEB-DL are manual-only
        # (use the "Digital Releases" tab in the panel).
        if not is_tamil_release(m) or not is_predvd_release(m):
            continue

        snapshot_doc = await db.col("predvd_snapshot").find_one({"_id": key})

        if snapshot_doc and snapshot_doc.get("is_baseline"):
            continue
        if snapshot_doc and (snapshot_doc.get("leeched") or snapshot_doc.get("queued")):
            continue

        selected = select_qualities_for_leech(m, min_size_mb, max_files)
        if not selected:
            continue

        LOGGER.info("[PREDVD NEW] '%s' (%s) — %d eligible quality(ies) (>= %d MB)",
                    title, m.get("year", ""), len(selected), min_size_mb)

        for q in selected:
            res = await request_leech(q["magnet_url"], title, q["quality"], key=key,
                                      year=m.get("year"), auto=True)
            if res.get("queued"):
                queued += 1
                LOGGER.info("[PREDVD] '%s' queued (indexer busy)", title)
                break  # next tick / drain handles it
            if res.get("duplicate"):
                # Ledger says it was already sent (e.g. an imdbId flip-flop).
                # Reconcile the snapshot so it's skipped going forward.
                await db.col("predvd_snapshot").update_one(
                    {"_id": key},
                    {"$set": {"leeched": True, "queued": False, "title": title,
                              "leeched_at": time.time()}},
                    upsert=True,
                )
                LOGGER.info("[PREDVD] '%s' already leeched — reconciled snapshot", title)
                break
            if not res.get("ok"):
                LOGGER.warning("[PREDVD] send failed for '%s': %s", title, res.get("error"))
                break  # leave for next tick (snapshot not marked)
            leeched += 1
            await asyncio.sleep(8.0)  # polite spacing between sends

    pending = await queue_depth()
    _last_check_status = f"Idle (last cycle: {leeched} sent, {queued} queued, {pending} pending)"
    return {"ok": True, "leeched": leeched, "queued": queued, "pending": pending}


# ---------------------------------------------------------------------------
# Background loop
# ---------------------------------------------------------------------------

async def _automator_loop():
    LOGGER.info("[PREDVD] PreDVD automator (send-only) started.")
    await asyncio.sleep(15)  # let DB + userbot boot
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
        "indexer_busy": indexer_is_busy(),
        "queue_pending": 0,  # filled in by the admin status endpoint
    }
