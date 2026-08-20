"""GlobalDB cleanup: enforce duplicate-quality policy per title (port of
clean_meta_files from the original global_indexer)."""
import asyncio
import re
from collections import defaultdict

import PTN

from app import db
from app.logger import LOGGER

_running = False


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


async def clean_meta_files(meta_id: str) -> int:
    if str(meta_id).startswith("song:"):
        return 0
    files = await db.col("files").find({"meta_id": meta_id}).to_list(None)
    if not files:
        return 0

    groups = defaultdict(list)
    for f in files:
        key = (f.get("season"), f.get("episode_start"), f.get("episode_end"))
        groups[key].append(f)

    to_delete = []
    for _key, group_files in groups.items():
        enriched = []
        has_high_res = False
        for f in group_files:
            try:
                parsed = PTN.parse(f["filename"])
            except Exception:
                parsed = {}
            res_val = _res_val(parsed.get("resolution", f.get("quality", "")))
            if res_val >= 720:
                has_high_res = True
            low = f["filename"].lower()
            lang_score = 100 if re.search(r"\b(tam|tamil)\b", low) else (50 if re.search(r"\bmulti\b", low) else 0)
            codec = str(parsed.get("codec", "")).lower()
            codec_score = 20 if ("265" in codec or "hevc" in codec) else (10 if ("264" in codec or "avc" in codec) else 0)
            audio = str(parsed.get("audio", "")).lower()
            audio_score = 15 if re.search(r"dts|dd|ac3|eac3|dolby", audio) else (10 if "aac" in audio else 0)
            enriched.append({"doc": f, "res_val": res_val, "lang_score": lang_score,
                             "score": lang_score + codec_score + audio_score, "size": f.get("size", 0)})

        if has_high_res:
            enriched = [e for e in enriched if e["res_val"] >= 720]
        if any(e["lang_score"] >= 100 for e in enriched):
            enriched = [e for e in enriched if e["lang_score"] >= 50]

        res_groups = defaultdict(list)
        for e in enriched:
            res_groups[e["res_val"]].append(e)
        keep = []
        for res_val, res_files in res_groups.items():
            res_files.sort(key=lambda x: (x["score"], x["size"]), reverse=True)
            keep.extend(k["doc"]["_id"] for k in res_files[:3])
        keep = set(keep)
        for f in group_files:
            if f["_id"] not in keep:
                to_delete.append(f["_id"])

    if to_delete:
        await db.col("files").delete_many({"_id": {"$in": to_delete}})
    return len(to_delete)


async def run_cleanup_all() -> int:
    global _running
    _running = True
    deleted = 0
    try:
        meta_ids = await db.col("files").distinct("meta_id")
        for i, mid in enumerate(meta_ids):
            deleted += await clean_meta_files(mid)
            if i and i % 200 == 0:
                LOGGER.info("[CLEANUP] %s/%s groups done (%s removed)", i, len(meta_ids), deleted)
        LOGGER.info("[CLEANUP] finished, removed %s files", deleted)
    except Exception as exc:
        LOGGER.error("[CLEANUP] error: %s", exc)
    finally:
        _running = False
    return deleted
