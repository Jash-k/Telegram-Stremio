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

from app import db
from app.logger import LOGGER

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


def _score_group(files: list) -> list:
    """Score one (meta, season, episode) group; return file _ids to delete.

    Pure CPU function. Mirrors the original policy: drop <720p when HD exists,
    prefer Tamil over unknown, keep max 3 per resolution.
    """
    if not files:
        return []

    enriched = []
    has_high_res = False
    for f in files:
        low = f["filename"].lower()

        # Resolution comes from the stored `quality` field — it was already
        # computed from PTN at index time, so re-parsing is redundant.
        res_val = _res_val(f.get("quality", ""))
        if res_val >= 720:
            has_high_res = True

        lang_score = 100 if _LANG_TAMIL.search(low) else (50 if _LANG_MULTI.search(low) else 0)
        codec_score = 20 if _CODEC_HEVC.search(low) else (10 if _CODEC_AVC.search(low) else 0)
        audio_score = 15 if _AUDIO_HQ.search(low) else (10 if _AUDIO_AAC.search(low) else 0)

        enriched.append({
            "id": f["_id"],
            "res_val": res_val,
            "lang_score": lang_score,
            "score": lang_score + codec_score + audio_score,
            "size": f.get("size", 0),
        })

    if has_high_res:
        enriched = [e for e in enriched if e["res_val"] >= 720]
    if any(e["lang_score"] >= 100 for e in enriched):
        enriched = [e for e in enriched if e["lang_score"] >= 50]

    res_groups = defaultdict(list)
    for e in enriched:
        res_groups[e["res_val"]].append(e)

    keep = set()
    for _resolution, res_files in res_groups.items():
        res_files.sort(key=lambda x: (x["score"], x["size"]), reverse=True)
        keep.update(k["id"] for k in res_files[:3])

    return [f["_id"] for f in files if f["_id"] not in keep]


async def clean_meta_files(meta_id: str) -> int:
    """Live-path cleanup for a single title (called after each new/edit file)."""
    if str(meta_id).startswith("song:"):
        return 0
    files = await db.col("files").find(
        {"meta_id": meta_id},
        {"_id": 1, "filename": 1, "season": 1, "episode_start": 1, "episode_end": 1, "quality": 1, "size": 1},
    ).to_list(None)
    if not files:
        return 0

    groups = defaultdict(list)
    for f in files:
        groups[(_group_key(f)[1:])].append(f)  # group by (season, ep_start, ep_end)

    to_delete = []
    for gfiles in groups.values():
        to_delete.extend(_score_group(gfiles))

    if to_delete:
        await db.col("files").delete_many({"_id": {"$in": to_delete}})
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
             "episode_start": 1, "episode_end": 1, "quality": 1, "size": 1},
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
        to_delete = await loop.run_in_executor(None, _score_all, groups)
        LOGGER.info("[CLEANUP] %d files to delete", len(to_delete))

        # 4. Batched deletes (avoids one giant $in query).
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


def _score_all(groups: dict) -> list:
    """Score every group and flatten the delete list (sync, called in a thread)."""
    return [fid for gfiles in groups.values() for fid in _score_group(gfiles)]
