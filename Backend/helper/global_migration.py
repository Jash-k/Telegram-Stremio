import asyncio
import time
import uuid
from datetime import datetime, timedelta, timezone

from pymongo import ReturnDocument, UpdateOne
from pymongo.errors import DuplicateKeyError

from Backend.helper.global_db_service import (
    first_int,
    languages_from_filename,
    normalize_global_file_fields,
)
from Backend.helper.metadata import _tmdb_details
from Backend.logger import LOGGER

_JOB_ID = "global_migration_job"
_OWNER = uuid.uuid4().hex
_LEASE_SECONDS = 90
_FILE_BATCH_SIZE = 500
_META_BATCH_SIZE = 24
_MIGRATION_TASK = None
_AUTO_RESUME_TASK = None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _lease_until(now: datetime) -> datetime:
    return now + timedelta(seconds=_LEASE_SECONDS)


def _lease_is_expired(value) -> bool:
    if not isinstance(value, datetime):
        return True
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value <= _now()


async def _acquire_lease(global_db) -> dict | None:
    now = _now()
    previous = await global_db["state"].find_one({"_id": _JOB_ID}) or {}
    resume = previous.get("status") in {"failed", "expired", "stopped"} or (
        previous.get("running") and _lease_is_expired(previous.get("lease_until"))
    )
    phase = previous.get("phase") if resume else "files"
    if phase not in {"files", "meta"}:
        phase = "files"

    update = {
        "$set": {
            "running": True,
            "status": "running",
            "owner": _OWNER,
            "lease_until": _lease_until(now),
            "heartbeat_at": now,
            "started_at": now,
            "finished_at": None,
            "stop_requested": False,
            "last_error": None,
            "phase": phase,
            "files_processed": int(previous.get("files_processed", 0) or 0)
            if resume
            else 0,
            "files_modified": int(previous.get("files_modified", 0) or 0)
            if resume
            else 0,
            "meta_processed": int(previous.get("meta_processed", 0) or 0)
            if resume
            else 0,
            "meta_modified": int(previous.get("meta_modified", 0) or 0)
            if resume
            else 0,
            "metadata_fetch_failures": int(
                previous.get("metadata_fetch_failures", 0) or 0
            )
            if resume
            else 0,
        }
    }
    if not resume:
        update["$unset"] = {"file_checkpoint": "", "meta_checkpoint": ""}

    try:
        return await global_db["state"].find_one_and_update(
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
            return_document=ReturnDocument.AFTER,
        )
    except DuplicateKeyError:
        return None


async def _heartbeat(global_db, **progress) -> bool:
    now = _now()
    job = await global_db["state"].find_one_and_update(
        {"_id": _JOB_ID, "owner": _OWNER, "running": True},
        {
            "$set": {
                "heartbeat_at": now,
                "lease_until": _lease_until(now),
                **progress,
            }
        },
        return_document=ReturnDocument.AFTER,
    )
    return bool(job and not job.get("stop_requested"))


def schedule_interrupted_migration_resume(db) -> None:
    """Resume a worker lost during a process restart after its old lease expires."""
    global _AUTO_RESUME_TASK
    if _AUTO_RESUME_TASK is not None and not _AUTO_RESUME_TASK.done():
        return

    async def resume_when_available():
        global _AUTO_RESUME_TASK
        try:
            if db.global_db is None:
                return
            job = await db.global_db["state"].find_one({"_id": _JOB_ID})
            if not job or job.get("status") not in {"running", "expired"}:
                return
            lease_until = job.get("lease_until")
            if job.get("running") and isinstance(lease_until, datetime):
                if lease_until.tzinfo is None:
                    lease_until = lease_until.replace(tzinfo=timezone.utc)
                delay = max(0.0, (lease_until - _now()).total_seconds()) + 1.0
                await asyncio.sleep(delay)
            result = await start_global_migration(db)
            if result.get("ok"):
                LOGGER.info("[GLOBAL MIGRATE] Automatically resumed interrupted migration.")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.error("[GLOBAL MIGRATE] Automatic resume failed: %s", exc)
        finally:
            _AUTO_RESUME_TASK = None

    _AUTO_RESUME_TASK = asyncio.create_task(resume_when_available())


async def start_global_migration(db) -> dict:
    global _MIGRATION_TASK
    if db.global_db is None:
        return {"ok": False, "message": "No GlobalDB configured."}
    if _MIGRATION_TASK is not None and not _MIGRATION_TASK.done():
        return {"ok": False, "message": "Migration is already running."}

    job = await _acquire_lease(db.global_db)
    if not job or job.get("owner") != _OWNER:
        return {
            "ok": False,
            "message": "Migration is already running on another replica.",
        }

    _MIGRATION_TASK = asyncio.create_task(_run_migration(db))
    resumed = bool(job.get("file_checkpoint") or job.get("meta_checkpoint"))
    return {
        "ok": True,
        "message": "Migration resumed from its saved checkpoint."
        if resumed
        else "Fast resumable migration started.",
    }


async def request_global_migration_stop(db) -> bool:
    if db.global_db is None:
        return False
    result = await db.global_db["state"].update_one(
        {"_id": _JOB_ID, "running": True},
        {"$set": {"stop_requested": True, "stop_requested_at": _now()}},
    )
    return bool(result.matched_count)


async def global_migration_status(db) -> dict:
    if db.global_db is None:
        return {"running": False, "status": "disabled"}
    job = await db.global_db["state"].find_one(
        {"_id": _JOB_ID}, {"_id": 0, "owner": 0}
    )
    if not job:
        return {"running": False, "status": "idle"}

    if job.get("running") and _lease_is_expired(job.get("lease_until")):
        expired = await db.global_db["state"].update_one(
            {
                "_id": _JOB_ID,
                "running": True,
                "lease_until": job.get("lease_until"),
            },
            {
                "$set": {
                    "running": False,
                    "status": "expired",
                    "last_error": "Migration worker stopped before clean shutdown.",
                    "finished_at": _now(),
                }
            },
        )
        if expired.modified_count:
            job.update(
                {
                    "running": False,
                    "status": "expired",
                    "last_error": "Migration worker stopped before clean shutdown.",
                }
            )
        else:
            job = await db.global_db["state"].find_one(
                {"_id": _JOB_ID}, {"_id": 0, "owner": 0}
            )
    return job


async def _migrate_file_batches(global_db, job: dict) -> tuple[bool, dict]:
    checkpoint = job.get("file_checkpoint")
    processed = int(job.get("files_processed", 0) or 0)
    modified = int(job.get("files_modified", 0) or 0)
    total = await global_db["files"].count_documents({})
    # `$addToSet` fails when a legacy language field is explicitly null.
    await global_db["meta"].update_many(
        {"languages": None}, {"$set": {"languages": []}}
    )
    await _heartbeat(global_db, phase="files", total_files=total)

    while True:
        query = {"_id": {"$gt": checkpoint}} if checkpoint is not None else {}
        rows = await global_db["files"].find(
            query,
            {
                "_id": 1,
                "meta_id": 1,
                "filename": 1,
                "chat_id": 1,
                "message_id": 1,
                "season": 1,
                "episode_start": 1,
                "episode_end": 1,
                "indexed_at": 1,
            },
        ).sort("_id", 1).limit(_FILE_BATCH_SIZE).to_list(_FILE_BATCH_SIZE)
        if not rows:
            break

        timestamp = time.time()
        file_operations = []
        languages_by_meta = {}
        for row in rows:
            normalized = normalize_global_file_fields(row, indexed_at=timestamp)
            file_operations.append(
                UpdateOne({"_id": row["_id"]}, {"$set": normalized})
            )
            meta_id = row.get("meta_id")
            languages = languages_from_filename(row.get("filename", ""))
            if meta_id and languages:
                languages_by_meta.setdefault(meta_id, set()).update(languages)

        if file_operations:
            result = await global_db["files"].bulk_write(
                file_operations, ordered=False
            )
            modified += int(result.modified_count or 0)
        if languages_by_meta:
            await global_db["meta"].bulk_write(
                [
                    UpdateOne(
                        {"_id": meta_id},
                        {"$addToSet": {"languages": {"$each": sorted(languages)}}},
                    )
                    for meta_id, languages in languages_by_meta.items()
                ],
                ordered=False,
            )

        processed += len(rows)
        checkpoint = rows[-1]["_id"]
        keep_running = await _heartbeat(
            global_db,
            phase="files",
            total_files=total,
            files_processed=processed,
            files_modified=modified,
            file_checkpoint=checkpoint,
        )
        LOGGER.info(
            "[GLOBAL MIGRATE] Files %s/%s examined (%s modified).",
            processed,
            total,
            modified,
        )
        if not keep_running:
            return False, {
                "files_processed": processed,
                "files_modified": modified,
                "file_checkpoint": checkpoint,
            }

    progress = {
        "files_processed": processed,
        "files_modified": modified,
        "file_checkpoint": checkpoint,
        "phase": "meta",
    }
    await _heartbeat(global_db, **progress)
    return True, progress


async def _build_meta_update(meta: dict) -> tuple[UpdateOne, bool]:
    meta_id = str(meta["_id"])
    tmdb_id = first_int(meta.get("tmdb_id"))
    media_type = meta.get("media_type")
    details = None
    fetch_failed = False
    if tmdb_id and media_type:
        details = await _tmdb_details(
            "tv" if media_type == "series" else "movie", tmdb_id
        )
        fetch_failed = details is None

    is_song = meta_id.startswith("song:")
    actual_imdb_id = None
    if details:
        external_ids = getattr(details, "external_ids", None)
        actual_imdb_id = (
            external_ids.get("imdb_id")
            if isinstance(external_ids, dict)
            else getattr(external_ids, "imdb_id", None)
            if external_ids
            else None
        )
    public_imdb_id = (
        f"song:{actual_imdb_id}" if is_song and actual_imdb_id else actual_imdb_id
    )
    existing_imdb_id = str(meta.get("imdb_id") or "")
    if not public_imdb_id and existing_imdb_id.startswith(("tt", "song:tt")):
        public_imdb_id = existing_imdb_id

    update_fields = {
        "tmdb_id": tmdb_id,
        "imdb_id": public_imdb_id,
        "aliases": [value for value in (meta_id, public_imdb_id) if value],
        "updated_at": time.time(),
    }
    tamil_original = False
    if details:
        year_value = getattr(details, "release_date", None) or getattr(
            details, "first_air_date", ""
        )
        year_number = getattr(year_value, "year", None)
        if year_number is None:
            import re

            year_match = re.search(r"\b(19\d{2}|20\d{2})\b", str(year_value or ""))
            year_number = int(year_match.group(1)) if year_match else None
        update_fields.update(
            {
                "year": year_number,
                "rating": getattr(details, "vote_average", 0.0),
            }
        )
        tamil_original = getattr(details, "original_language", "") == "ta"

    update = {"$set": update_fields}
    if tamil_original:
        update["$addToSet"] = {"languages": "Tamil"}
    return UpdateOne({"_id": meta_id}, update), fetch_failed


async def _migrate_meta_batches(global_db, job: dict) -> tuple[bool, dict]:
    checkpoint = job.get("meta_checkpoint")
    processed = int(job.get("meta_processed", 0) or 0)
    modified = int(job.get("meta_modified", 0) or 0)
    failures = int(job.get("metadata_fetch_failures", 0) or 0)
    total = await global_db["meta"].count_documents({})
    await _heartbeat(global_db, phase="meta", total_meta=total)

    while True:
        query = {"_id": {"$gt": checkpoint}} if checkpoint is not None else {}
        rows = await global_db["meta"].find(query).sort("_id", 1).limit(
            _META_BATCH_SIZE
        ).to_list(_META_BATCH_SIZE)
        if not rows:
            break

        built = await asyncio.gather(*(_build_meta_update(meta) for meta in rows))
        operations = [operation for operation, _ in built]
        failures += sum(1 for _, failed in built if failed)
        if operations:
            result = await global_db["meta"].bulk_write(operations, ordered=False)
            modified += int(result.modified_count or 0)

        processed += len(rows)
        checkpoint = rows[-1]["_id"]
        keep_running = await _heartbeat(
            global_db,
            phase="meta",
            total_meta=total,
            meta_processed=processed,
            meta_modified=modified,
            metadata_fetch_failures=failures,
            meta_checkpoint=checkpoint,
        )
        LOGGER.info(
            "[GLOBAL MIGRATE] Metadata %s/%s examined (%s modified, %s fetch failures).",
            processed,
            total,
            modified,
            failures,
        )
        if not keep_running:
            return False, {
                "meta_processed": processed,
                "meta_modified": modified,
                "metadata_fetch_failures": failures,
                "meta_checkpoint": checkpoint,
            }

    return True, {
        "meta_processed": processed,
        "meta_modified": modified,
        "metadata_fetch_failures": failures,
        "meta_checkpoint": checkpoint,
    }


async def _finish(global_db, status: str, last_error=None, **progress) -> None:
    now = _now()
    await global_db["state"].update_one(
        {"_id": _JOB_ID, "owner": _OWNER},
        {
            "$set": {
                "running": False,
                "status": status,
                "stop_requested": False,
                "last_error": last_error,
                "finished_at": now,
                "lease_until": now,
                **progress,
            }
        },
    )


async def _run_migration(db) -> None:
    global _MIGRATION_TASK
    global_db = db.global_db
    try:
        job = await global_db["state"].find_one({"_id": _JOB_ID, "owner": _OWNER})
        if not job:
            return

        if job.get("phase") == "files":
            completed, file_progress = await _migrate_file_batches(global_db, job)
            if not completed:
                await _finish(global_db, "stopped", **file_progress)
                return
            job.update(file_progress)

        completed, meta_progress = await _migrate_meta_batches(global_db, job)
        if not completed:
            await _finish(global_db, "stopped", **meta_progress)
            return

        await global_db["state"].update_one(
            {"_id": "schema"},
            {"$set": {"version": 3, "data_migrated_at": _now()}},
            upsert=True,
        )
        final_status = (
            "completed_with_errors"
            if meta_progress.get("metadata_fetch_failures")
            else "completed"
        )
        await _finish(global_db, final_status, **meta_progress)
        LOGGER.info("[GLOBAL MIGRATE] Finished with status %s.", final_status)
    except asyncio.CancelledError:
        try:
            await _finish(global_db, "stopped", "Migration task cancelled.")
        finally:
            raise
    except Exception as exc:
        LOGGER.exception("[GLOBAL MIGRATE] Fatal migration error: %s", exc)
        try:
            await _finish(global_db, "failed", f"{type(exc).__name__}: {exc}")
        except Exception:
            LOGGER.exception("[GLOBAL MIGRATE] Could not persist failure status.")
    finally:
        _MIGRATION_TASK = None
