"""GlobalDB cleanup: enforce duplicate-quality policy per title (port of
clean_meta_files from the original global_indexer).

Optimized for bulk runs:
  * one query loads all files (no N+1 round-trips)
  * no PTN.parse — resolution is read from the stored `quality` field (already
    computed from PTN at index time) and codec/audio come from fast regex, so
    scoring is ~100x faster than re-parsing every filename
  * deletes are batched
"""
import asyncio
import re
from collections import defaultdict

from app import cleanup_bot, db
from app.logger import LOGGER
from app.parser import source_from_filename

_running = False

_DELETE_BATCH = 5000

# Precompiled scoring regexes (substring checks mirror the original's PTN
# field checks, but run directly on the filename).
_CODEC_HEVC = re.compile(r"x265|h\.?265|hevc")
_CODEC_AVC = re.compile(r"x264|h\.?264|avc")
_AUDIO_HQ = re.compile(r"dts|eac3|ac3|dolby|ddp|\bdd\+|dd5")
_AUDIO_AAC = re.compile(r"\baac\b")
_LANG_TAMIL = re.compile(r"\b(tam|tamil)\b")
_LANG_MULTI = re.compile(r"\bmulti\b")


def is_running() -> bool:
    return _running


def _res_val(res_str) -> int:
    res_str = str(res_str or "").lower()
    if "2160" in res_str or "4k" in res_str:
        return 2160
    if "1440" in res_str or "2k" in res_str:
        return 1440
    if "1080" in res_str:
        return 1080
    if "720" in res_str:
        return 720
    if "480" in res_str:
        return 480
    if "360" in res_str:
        return 360
    return 0


def _group_key(f: dict) -> tuple:
    def make_hashable(val):
        return tuple(val) if isinstance(val, list) else val

    return (
        f.get("meta_id"),
        make_hashable(f.get("season")),
        make_hashable(f.get("episode_start")),
        make_hashable(f.get("episode_end")),
    )


def _deletion_record(f: dict) -> dict | None:
    """Return a {'chat_id','message_id'} for Telegram deletion ONLY if this file
    lives in the configured PreDVD dump channel. Otherwise None.
    """
    dump = cleanup_bot.dump_channel()
    if dump is None:
        return None
    try:
        chat_id = int(f.get("chat_id"))
        message_id = int(f.get("message_id"))
    except (TypeError, ValueError):
        return None
    if chat_id != int(dump) or not message_id:
        return None
    return {"chat_id": chat_id, "message_id": message_id}


def _score_group(files: list) -> tuple[list, list]:
    """Score one (meta, season, episode) group.

    Returns (file_ids_to_delete, telegram_delete_records).

    Policies:
      * SOURCE-AWARE: once a genuine digital print (WEB-DL/BluRay/HDRip/UHD)
        exists for this exact title+season+episode, the theatrical/cam copies
        (predvd/cam/hdtc/hdts/dvdscr) are removed — even when mis-labeled
        "1080p". Predvd files are NEVER removed without a digital replacement.
      * Resolution/language best-of-3 (the original policy) applies to the
        remaining non-predvd files: drop <720p when HD exists, prefer Tamil,
        keep max 3 per resolution.
    """
    if not files:
        return [], []

    enriched = []
    has_high_res = False
    has_digital = False
    for f in files:
        low = str(f.get("filename", "")).lower()

        # Resolution comes from the stored `quality` field — it was already
        # computed from PTN at index time, so re-parsing is redundant.
        res_val = _res_val(f.get("quality", ""))
        if res_val >= 720:
            has_high_res = True

        source = f.get("source") or source_from_filename(low)
        if source == "digital":
            has_digital = True

        lang_score = 100 if _LANG_TAMIL.search(low) else (50 if _LANG_MULTI.search(low) else 0)
        codec_score = 20 if _CODEC_HEVC.search(low) else (10 if _CODEC_AVC.search(low) else 0)
        audio_score = 15 if _AUDIO_HQ.search(low) else (10 if _AUDIO_AAC.search(low) else 0)

        enriched.append({
            "id": f["_id"],
            "res_val": res_val,
            "source": source,
            "lang_score": lang_score,
            "score": lang_score + codec_score + audio_score,
            "size": f.get("size", 0),
            "f": f,
        })

    remove: set = set()
    tg_records: list = []

    predvd_e = [e for e in enriched if e["source"] == "predvd"]
    pool = [e for e in enriched if e["source"] != "predvd"]

    # Rule 1: theatrical/cam copies are removed ONLY once a digital print for
    # the same title+episode exists (and their dump-channel messages deleted).
    if has_digital:
        for e in predvd_e:
            remove.add(e["id"])
            rec = _deletion_record(e["f"])
            if rec:
                tg_records.append(rec)

    # Rule 2: best-of-resolution / language on the non-predvd survivors.
    scored = pool
    if has_high_res:
        scored = [e for e in scored if e["res_val"] >= 720]
    if any(e["lang_score"] >= 100 for e in scored):
        scored = [e for e in scored if e["lang_score"] >= 50]

    res_groups = defaultdict(list)
    for e in scored:
        res_groups[e["res_val"]].append(e)

    keep = set()
    for _resolution, res_files in res_groups.items():
        res_files.sort(key=lambda x: (x["score"], x["size"]), reverse=True)
        keep.update(k["id"] for k in res_files[:3])

    for e in pool:
        if e["id"] not in keep:
            remove.add(e["id"])

    return list(remove), tg_records


async def _apply_deletions(to_delete: list, tg_records: list) -> None:
    """Delete dump-channel Telegram messages (via cleanup bot), then DB rows."""
    if tg_records and cleanup_bot.is_enabled():
        sem = asyncio.Semaphore(5)

        async def _one(rec):
            async with sem:
                await cleanup_bot.delete_dump_message(rec["chat_id"], rec["message_id"])

        await asyncio.gather(*[_one(r) for r in tg_records], return_exceptions=True)
    if to_delete:
        for i in range(0, len(to_delete), _DELETE_BATCH):
            await db.col("files").delete_many({"_id": {"$in": to_delete[i:i + _DELETE_BATCH]}})


async def clean_meta_files(meta_id: str) -> int:
    """Live-path cleanup for a single title (called after each new/edit file).

    Returns the number of DB rows removed. Also deletes the corresponding
    PreDVD messages from the dump channel (via the cleanup bot).
    """
    if str(meta_id).startswith("song:"):
        return 0
    files = await db.col("files").find(
        {"meta_id": meta_id},
        {"_id": 1, "filename": 1, "season": 1, "episode_start": 1, "episode_end": 1,
         "quality": 1, "size": 1, "source": 1, "chat_id": 1, "message_id": 1},
    ).to_list(None)
    if not files:
        return 0

    groups = defaultdict(list)
    for f in files:
        groups[(_group_key(f)[1:])].append(f)  # group by (season, ep_start, ep_end)

    to_delete, tg_records = [], []
    for gfiles in groups.values():
        ids, recs = _score_group(gfiles)
        to_delete.extend(ids)
        tg_records.extend(recs)

    if to_delete:
        await _apply_deletions(to_delete, tg_records)
        LOGGER.info("[CLEANUP] title %s: removed %d file(s) (%d dump message(s))",
                    meta_id, len(to_delete), len(tg_records))
    return len(to_delete)


async def run_cleanup_all() -> int:
    """Bulk cleanup across the whole DB. Returns number of files removed."""
    global _running
    _running = True
    try:
        # 1. ONE query instead of one-per-meta (removes ~9,680 round-trips).
        LOGGER.info("[CLEANUP] loading all files…")
        files = await db.col("files").find(
            {},
            {"_id": 1, "meta_id": 1, "filename": 1, "season": 1,
             "episode_start": 1, "episode_end": 1, "quality": 1, "size": 1,
             "source": 1, "chat_id": 1, "message_id": 1},
        ).to_list(None)
        LOGGER.info("[CLEANUP] %d files loaded, grouping…", len(files))

        # 2. Group in memory (cheap), skipping video-song metas.
        groups = defaultdict(list)
        for f in files:
            meta_id = f.get("meta_id")
            if not meta_id or str(meta_id).startswith("song:"):
                continue
            groups[_group_key(f)].append(f)
        del files
        LOGGER.info("[CLEANUP] %d groups to score", len(groups))

        # 3. Score (fast — regex only, no PTN). Runs in a thread so the event
        #    loop stays responsive even during the largest runs.
        loop = asyncio.get_running_loop()
        to_delete, tg_records = await loop.run_in_executor(None, _score_all, groups)
        LOGGER.info("[CLEANUP] %d files to delete (%d dump messages)", len(to_delete), len(tg_records))

        # 4. Delete dump-channel PreDVD messages, then batch DB deletes.
        if tg_records and cleanup_bot.is_enabled():
            LOGGER.info("[CLEANUP] deleting %d old PreDVD message(s) from dump channel…", len(tg_records))
            await _apply_deletions([], tg_records)
        deleted = 0
        for i in range(0, len(to_delete), _DELETE_BATCH):
            chunk = to_delete[i:i + _DELETE_BATCH]
            result = await db.col("files").delete_many({"_id": {"$in": chunk}})
            deleted += result.deleted_count
            LOGGER.info("[CLEANUP] deleted %d/%d", deleted, len(to_delete))

        LOGGER.info("[CLEANUP] finished, removed %d files", deleted)
        return deleted
    except Exception as exc:
        LOGGER.error("[CLEANUP] error: %s", exc)
        return 0
    finally:
        _running = False


def _score_all(groups: dict) -> tuple[list, list]:
    """Score every group and flatten (delete_ids, telegram_records). Sync, in a thread."""
    ids, recs = [], []
    for gfiles in groups.values():
        g_ids, g_recs = _score_group(gfiles)
        ids.extend(g_ids)
        recs.extend(g_recs)
    return ids, recs
