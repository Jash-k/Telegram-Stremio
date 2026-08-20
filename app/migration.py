"""Resumable GlobalDB migration (faithful port of global_migration.py).

Normalizes legacy file coordinates, backfills `languages`, and rebuilds meta
(tmdb/imdb/aliases/year/rating) in checkpointed batches, so an interrupted run
resumes where it left off.
"""
import asyncio
import time
import uuid

from pymongo import UpdateOne

from app import db
from app.logger import LOGGER
from app.metadata import tmdb_details
from app.parser import (
    first_int,
    languages_from_filename,
    normalize_global_file_fields,
)

_JOB_ID = "global_migration_job"
_OWNER = uuid.uuid4().hex
_LEASE_SECONDS = 90
_FILE_BATCH = 500
_META_BATCH = 24
_task = None
_auto_task = None


def _now() -> float:
    return time.time()


async def _acquire() -> dict | None:
    now = _now()
    prev = await db.col("state").find_one({"_id": _JOB_ID}) or {}
    resume = prev.get("status") in {"failed", "expired", "stopped"} or (
        prev.get("running") and float(prev.get("lease_until") or 0) <= now
    )
    phase = prev.get("phase") if resume else "files"
    if phase not in {"files", "meta"}:
        phase = "files"
    base = {"files_processed", "files_modified", "meta_processed", "meta_modified", "metadata_fetch_failures"}
    update = {
        "$set": {
            "running": True, "status": "running", "owner": _OWNER,
            "lease_until": now + _LEASE_SECONDS, "started_at": now,
            "finished_at": None, "stop_requested": False, "last_error": None, "phase": phase,
        }
    }
    for k in base:
        update["$set"][k] = int(prev.get(k, 0) or 0) if resume else 0
    if resume:
        update["$set"]["file_checkpoint"] = prev.get("file_checkpoint")
        update["$set"]["meta_checkpoint"] = prev.get("meta_checkpoint")
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
            update,
            upsert=True,
            return_document=True,
        )
    except Exception:
        return None
    return job if job.get("owner") == _OWNER else None


async def _heartbeat(**progress) -> bool:
    now = _now()
    job = await db.col("state").find_one_and_update(
        {"_id": _JOB_ID, "owner": _OWNER, "running": True},
        {"$set": {"lease_until": now + _LEASE_SECONDS, **progress}},
        return_document=True,
    )
    return bool(job and not job.get("stop_requested"))


async def start_migration() -> dict:
    global _task
    if _task is not None and not _task.done():
        return {"ok": False, "message": "Migration is already running."}
    job = await _acquire()
    if not job:
        return {"ok": False, "message": "Migration is already running on another replica."}
    _task = asyncio.create_task(_run())
    resumed = bool(job.get("file_checkpoint") or job.get("meta_checkpoint"))
    return {"ok": True, "message": "Migration resumed from its saved checkpoint." if resumed else "Fast resumable migration started."}


async def request_stop() -> bool:
    result = await db.col("state").update_one(
        {"_id": _JOB_ID, "running": True}, {"$set": {"stop_requested": True}}
    )
    return bool(result.modified_count)


async def migration_status() -> dict:
    job = await db.col("state").find_one({"_id": _JOB_ID}, {"owner": 0})
    if not job:
        return {"running": False, "status": "idle"}
    if job.get("running") and float(job.get("lease_until") or 0) <= _now():
        await db.col("state").update_one(
            {"_id": _JOB_ID, "running": True},
            {"$set": {"running": False, "status": "expired",
                      "last_error": "Migration worker stopped before clean shutdown.", "finished_at": _now()}},
        )
        job["running"] = False
        job["status"] = "expired"
    return job


def _finish(status: str, last_error=None, **progress) -> None:
    async def _go():
        await db.col("state").update_one(
            {"_id": _JOB_ID, "owner": _OWNER},
            {"$set": {"running": False, "status": status, "stop_requested": False,
                      "last_error": last_error, "finished_at": _now(), "lease_until": _now(), **progress}},
        )
    asyncio.create_task(_go())


async def _migrate_files(job: dict) -> tuple[bool, dict]:
    checkpoint = job.get("file_checkpoint")
    processed = int(job.get("files_processed", 0) or 0)
    modified = int(job.get("files_modified", 0) or 0)
    total = await db.col("files").count_documents({})
    await db.col("meta").update_many({"languages": None}, {"$set": {"languages": []}})
    await _heartbeat(phase="files", total_files=total)

    while True:
        query = {"_id": {"$gt": checkpoint}} if checkpoint is not None else {}
        rows = await db.col("files").find(
            query, {"_id": 1, "meta_id": 1, "filename": 1, "chat_id": 1, "message_id": 1,
                    "season": 1, "episode_start": 1, "episode_end": 1, "indexed_at": 1}
        ).sort("_id", 1).limit(_FILE_BATCH).to_list(_FILE_BATCH)
        if not rows:
            break

        ops = []
        langs_by_meta = {}
        ts = time.time()
        for row in rows:
            ops.append(UpdateOne({"_id": row["_id"]}, {"$set": normalize_global_file_fields(row, indexed_at=ts)}))
            meta_id = row.get("meta_id")
            langs = languages_from_filename(row.get("filename", ""))
            if meta_id and langs:
                langs_by_meta.setdefault(meta_id, set()).update(langs)

        if ops:
            result = await db.col("files").bulk_write(ops, ordered=False)
            modified += int(result.modified_count or 0)
        if langs_by_meta:
            await db.col("meta").bulk_write(
                [UpdateOne({"_id": mid}, {"$addToSet": {"languages": {"$each": sorted(l)}}})
                 for mid, l in langs_by_meta.items()],
                ordered=False,
            )

        processed += len(rows)
        checkpoint = rows[-1]["_id"]
        keep = await _heartbeat(phase="files", total_files=total, files_processed=processed,
                                files_modified=modified, file_checkpoint=checkpoint)
        LOGGER.info("[MIGRATE] files %s/%s (%s modified)", processed, total, modified)
        if not keep:
            return False, {"files_processed": processed, "files_modified": modified, "file_checkpoint": checkpoint}

    progress = {"files_processed": processed, "files_modified": modified, "file_checkpoint": checkpoint, "phase": "meta"}
    await _heartbeat(**progress)
    return True, progress


async def _build_meta_update(meta: dict) -> tuple[UpdateOne, bool]:
    meta_id = str(meta["_id"])
    tmdb_id = first_int(meta.get("tmdb_id"))
    media_type = meta.get("media_type")
    details = None
    fetch_failed = False
    if tmdb_id and media_type:
        details = await tmdb_details("tv" if media_type == "series" else "movie", tmdb_id)
        fetch_failed = details is None

    is_song = meta_id.startswith("song:")
    imdb_id = None
    if details:
        ext = details.get("external_ids") or {}
        imdb_id = ext.get("imdb_id")
    public_imdb = f"song:{imdb_id}" if (is_song and imdb_id) else imdb_id
    existing = str(meta.get("imdb_id") or "")
    if not public_imdb and existing.startswith(("tt", "song:tt")):
        public_imdb = existing

    fields = {
        "tmdb_id": tmdb_id,
        "imdb_id": public_imdb,
        "aliases": [v for v in (meta_id, public_imdb) if v],
        "updated_at": time.time(),
    }
    tamil = False
    if details:
        date = details.get("release_date") or details.get("first_air_date", "")
        year = None
        if isinstance(date, str) and len(date) >= 4:
            try:
                year = int(date[:4])
            except ValueError:
                year = None
        fields["year"] = year
        fields["rating"] = details.get("vote_average", 0.0)
        tamil = details.get("original_language") == "ta"

    update = {"$set": fields}
    if tamil:
        update["$addToSet"] = {"languages": "Tamil"}
    return UpdateOne({"_id": meta_id}, update), fetch_failed


async def _migrate_meta(job: dict) -> tuple[bool, dict]:
    checkpoint = job.get("meta_checkpoint")
    processed = int(job.get("meta_processed", 0) or 0)
    modified = int(job.get("meta_modified", 0) or 0)
    failures = int(job.get("metadata_fetch_failures", 0) or 0)
    total = await db.col("meta").count_documents({})
    await _heartbeat(phase="meta", total_meta=total)

    while True:
        query = {"_id": {"$gt": checkpoint}} if checkpoint is not None else {}
        rows = await db.col("meta").find(query).sort("_id", 1).limit(_META_BATCH).to_list(_META_BATCH)
        if not rows:
            break
        built = await asyncio.gather(*(_build_meta_update(m) for m in rows))
        ops = [o for o, _ in built]
        failures += sum(1 for _, f in built if f)
        if ops:
            result = await db.col("meta").bulk_write(ops, ordered=False)
            modified += int(result.modified_count or 0)
        processed += len(rows)
        checkpoint = rows[-1]["_id"]
        keep = await _heartbeat(phase="meta", total_meta=total, meta_processed=processed,
                                meta_modified=modified, metadata_fetch_failures=failures,
                                meta_checkpoint=checkpoint)
        LOGGER.info("[MIGRATE] meta %s/%s (%s modified, %s failures)", processed, total, modified, failures)
        if not keep:
            return False, {"meta_processed": processed, "meta_modified": modified,
                           "metadata_fetch_failures": failures, "meta_checkpoint": checkpoint}

    return True, {"meta_processed": processed, "meta_modified": modified,
                  "metadata_fetch_failures": failures, "meta_checkpoint": checkpoint}


async def _run() -> None:
    global _task
    try:
        job = await db.col("state").find_one({"_id": _JOB_ID, "owner": _OWNER})
        if not job:
            return
        if job.get("phase") == "files":
            ok, fp = await _migrate_files(job)
            if not ok:
                _finish("stopped", **fp)
                return
            job.update(fp)
        ok, mp = await _migrate_meta(job)
        if not ok:
            _finish("stopped", **mp)
            return
        await db.col("state").update_one(
            {"_id": "schema"}, {"$set": {"version": 3, "data_migrated_at": _now()}}, upsert=True
        )
        final = "completed_with_errors" if mp.get("metadata_fetch_failures") else "completed"
        _finish(final, **mp)
        LOGGER.info("[MIGRATE] finished: %s", final)
    except asyncio.CancelledError:
        _finish("stopped", "Migration task cancelled.")
        raise
    except Exception as exc:
        LOGGER.exception("[MIGRATE] fatal: %s", exc)
        _finish("failed", f"{type(exc).__name__}: {exc}")
    finally:
        _task = None
