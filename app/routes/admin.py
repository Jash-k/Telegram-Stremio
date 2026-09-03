"""GlobalDB management API — mirrors the original project's
`/api/admin/global/*` surface so the global_manage.html panel works unchanged.
"""
import re
import time

import PTN
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse

import os
import socket

from app import db
from app import telemetry
from app.cleanup import is_running as cleanup_running, run_cleanup_all
from app.indexer import (
    add_channel,
    configured_channels,
    get_channel_config,
    remove_channel,
    remove_file_reference,
    request_stop,
    schedule_index,
    status,
)
from app.logger import LOGGER
from app.metadata import (
    format_tmdb_image,
    tmdb_details,
    tmdb_find_by_imdb,
    tmdb_search_multi,
    year_number,
)
from app.migration import migration_status, request_stop as migration_stop, start_migration
from app.parser import (
    determine_catalog,
    episode_bounds,
    first_int,
    languages_from_filename,
    parse_combined_episodes,
)
from app.security import require_auth

router = APIRouter(prefix="/api/admin/global", tags=["admin"])

PAGE_SIZE = 30


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

@router.get("/stats")
async def global_stats(_: bool = Depends(require_auth)):
    files_count = await db.col("files").count_documents({})
    counts = {}
    async for c in db.col("meta").aggregate([{"$group": {"_id": "$catalog", "count": {"$sum": 1}}}]):
        counts[c["_id"]] = c["count"]
    cats = []
    async for c in db.col("catalogs").find().sort("order", 1):
        c["count"] = counts.get(c["_id"], 0)
        cats.append(c)
    recent = await db.col("files").find().sort("_id", -1).limit(50).to_list(None)
    return {"files_count": files_count, "catalogs": cats, "recent_files": recent}


@router.get("/files/catalog/{catalog_id}")
async def get_catalog_files(catalog_id: str, page: int = 1, _: bool = Depends(require_auth)):
    skip = (page - 1) * PAGE_SIZE
    query = {"catalog": catalog_id}
    total = await db.col("meta").count_documents(query)
    items = await db.col("meta").find(query).sort([("updated_at", -1), ("_id", -1)]).skip(skip).limit(PAGE_SIZE).to_list(None)
    for item in items:
        item["file_count"] = await db.col("files").count_documents({"meta_id": item["_id"]})
    return {"items": items, "total_pages": (total + PAGE_SIZE - 1) // PAGE_SIZE or 1, "total_items": total}


# ---------------------------------------------------------------------------
# Meta / catalog / file deletion
# ---------------------------------------------------------------------------

@router.delete("/meta/{meta_id}")
async def delete_meta(meta_id: str, _: bool = Depends(require_auth)):
    await db.col("meta").delete_one({"_id": meta_id})
    await db.col("files").delete_many({"meta_id": meta_id})
    return {"status": "success"}


@router.delete("/catalogs/{cat_id}")
async def delete_catalog(cat_id: str, _: bool = Depends(require_auth)):
    await db.col("catalogs").delete_one({"_id": cat_id})
    return {"status": "success"}


@router.delete("/files/{file_id}")
async def delete_file(file_id: str, _: bool = Depends(require_auth)):
    fdoc = await db.col("files").find_one({"_id": file_id}, {"chat_id": 1, "message_id": 1})
    if fdoc:
        await remove_file_reference(int(fdoc["chat_id"]), int(fdoc["message_id"]))
    else:
        await db.col("unindexed").delete_one({"_id": file_id})
    return {"status": "success"}


@router.delete("/files")
async def delete_all_files(_: bool = Depends(require_auth)):
    await db.col("files").delete_many({})
    await db.col("meta").delete_many({})
    return {"status": "success"}


@router.get("/meta/{meta_id}/files")
async def get_meta_files(meta_id: str, _: bool = Depends(require_auth)):
    items = await db.col("files").find({"meta_id": meta_id}).sort([("season", 1), ("episode_start", 1)]).to_list(None)
    return {"items": items}


# ---------------------------------------------------------------------------
# Indexer
# ---------------------------------------------------------------------------

@router.post("/index/start")
async def index_start(request: Request, _: bool = Depends(require_auth)):
    try:
        payload = await request.json() or {}
    except Exception:
        payload = {}
    target = payload.get("chat_id")
    if target is not None:
        target = int(target)
    force = bool(payload.get("force_historic", False))
    started = schedule_index(force_historic=force, target_chat_id=target)
    if not started:
        return JSONResponse({"status": "error", "message": "Indexer already running"}, status_code=409)
    return {"status": "success"}


@router.post("/index/stop")
async def index_stop(_: bool = Depends(require_auth)):
    stopped = await request_stop()
    return {"status": "success" if stopped else "error", "message": "Stop requested" if stopped else "Not running"}


@router.get("/index/status")
async def index_status(_: bool = Depends(require_auth)):
    return status()


@router.post("/index/message")
async def index_single_message(payload: dict, _: bool = Depends(require_auth)):
    from app.client import client
    from app.indexer import _process_message
    
    if not client:
        return {"status": "error", "message": "Userbot client is not running."}
        
    chat_id = payload.get("chat_id")
    message_id = payload.get("message_id")
    
    if not chat_id or not message_id:
        return {"status": "error", "message": "chat_id and message_id are required."}
        
    try:
        chat_id = int(chat_id)
        message_id = int(message_id)
        msg = await client.get_messages(chat_id, message_id)
        if not msg:
            return {"status": "error", "message": f"Message {message_id} not found in chat {chat_id}"}
            
        meta_id = await _process_message(chat_id, msg)
        if meta_id:
            return {"status": "success", "message": f"Indexed successfully into {meta_id}!", "meta_id": meta_id}
        else:
            unidx = await db.col("unindexed").find_one({"_id": f"{chat_id}_{message_id}"})
            reason = unidx.get("reason", "Unknown reason") if unidx else "No video media detected"
            return {"status": "unindexed", "message": f"Could not index into catalog: {reason}", "reason": reason}
    except Exception as exc:
        LOGGER.error(f"[ADMIN] index_single_message error: {exc}")
        return {"status": "error", "message": str(exc)}


# ---------------------------------------------------------------------------
# Unindexed queue
# ---------------------------------------------------------------------------

@router.get("/unindexed")
async def get_unindexed(page: int = 1, search: str = "", _: bool = Depends(require_auth)):
    skip = (page - 1) * PAGE_SIZE
    query = {"filename": {"$regex": re.escape(search), "$options": "i"}} if search else {}
    total = await db.col("unindexed").count_documents(query)
    items = await db.col("unindexed").find(query).sort("_id", -1).skip(skip).limit(PAGE_SIZE).to_list(None)
    return {"items": items, "total_pages": (total + PAGE_SIZE - 1) // PAGE_SIZE or 1, "total_items": total}


@router.delete("/unindexed/{file_id}")
async def delete_unindexed(file_id: str, _: bool = Depends(require_auth)):
    await db.col("unindexed").delete_one({"_id": file_id})
    return {"status": "success"}


@router.delete("/unindexed")
async def purge_unindexed(_: bool = Depends(require_auth)):
    await db.col("unindexed").delete_many({})
    return {"status": "success"}


# ---------------------------------------------------------------------------
# Wipe
# ---------------------------------------------------------------------------

@router.post("/wipe")
async def wipe(_: bool = Depends(require_auth)):
    if status().get("running"):
        return JSONResponse({"status": "error", "message": "Stop the indexer before wiping."}, status_code=409)
    if (await migration_status()).get("running"):
        return JSONResponse({"status": "error", "message": "Stop the migration before wiping."}, status_code=409)
    await db.col("files").delete_many({})
    await db.col("meta").delete_many({})
    await db.col("catalogs").delete_many({})
    await db.col("unindexed").delete_many({})
    await db.col("state").delete_many({"_id": {"$ne": "schema"}})
    from app.cache import invalidate_all

    invalidate_all()
    return {"status": "success"}


# ---------------------------------------------------------------------------
# Mapping (single + batch)
# ---------------------------------------------------------------------------

async def _map_file(file_doc, tmdb_id, media_type, is_video_song, details) -> None:
    filename = file_doc.get("filename", "")
    try:
        parsed = PTN.parse(filename)
    except Exception:
        parsed = {}

    doc_id = f"song:tmdb:{tmdb_id}" if is_video_song else f"tmdb:{tmdb_id}"
    external = details.get("external_ids") or {}
    imdb_id = external.get("imdb_id")
    public_imdb = f"song:{imdb_id}" if (is_video_song and imdb_id) else imdb_id
    title_suffix = " (Video Songs)" if is_video_song else ""

    catalog = determine_catalog(details, media_type, filename)
    if is_video_song:
        catalog = "video_songs"

    update_data = {
        "tmdb_id": int(tmdb_id),
        "imdb_id": public_imdb,
        "aliases": [v for v in (doc_id, public_imdb) if v],
        "title": (details.get("title") or details.get("name") or "") + title_suffix,
        "year": year_number(details, media_type),
        "poster": format_tmdb_image(details.get("poster_path")),
        "background": format_tmdb_image(details.get("backdrop_path"), "original"),
        "description": details.get("overview") or "",
        "media_type": media_type,
        "catalog": catalog,
        "genres": [g.get("name") for g in (details.get("genres") or [])],
        "rating": details.get("vote_average", 0.0),
        "updated_at": time.time(),
    }
    languages = languages_from_filename(filename)
    if details.get("original_language") == "ta" and "Tamil" not in languages:
        languages.append("Tamil")
    await db.col("meta").update_one(
        {"_id": doc_id},
        {"$set": update_data, "$addToSet": {"languages": {"$each": languages}}},
        upsert=True,
    )

    combined = parse_combined_episodes(filename)
    ep_start, ep_end = episode_bounds(parsed.get("episode"))
    old_meta_id = file_doc.get("meta_id")
    await db.col("files").update_one(
        {"_id": file_doc["_id"]},
        {"$set": {
            "meta_id": doc_id,
            "filename": filename,
            "size": file_doc.get("size", 0),
            "size_str": file_doc.get("size_str", ""),
            "quality": parsed.get("resolution", "HD"),
            "chat_id": int(file_doc["chat_id"]),
            "message_id": int(file_doc["message_id"]),
            "season": first_int(combined["season"]) if combined else first_int(parsed.get("season")),
            "episode_start": first_int(combined["start"]) if combined else ep_start,
            "episode_end": first_int(combined["end"]) if combined else ep_end,
            "indexed_at": time.time(),
        }},
        upsert=True,
    )
    if old_meta_id and old_meta_id != doc_id:
        if not await db.col("files").find_one({"meta_id": old_meta_id}, {"_id": 1}):
            await db.col("meta").delete_one({"_id": old_meta_id})


async def _resolve_tmdb(input_id: str, media_type: str):
    input_id = str(input_id or "").strip()
    if not input_id:
        return None
    if input_id.startswith("tt") or "imdb.com/title/tt" in input_id:
        m = re.search(r"(tt\d+)", input_id)
        if m:
            try:
                found = await tmdb_find_by_imdb(m.group(1))
                return found["id"] if found else None
            except Exception:
                return None
    if "themoviedb.org" in input_id:
        m = re.search(r"/(?:movie|tv)/(\d+)", input_id)
        if m:
            return int(m.group(1))
    try:
        return int(input_id.replace("tmdb:", "").replace("song:tmdb:", ""))
    except ValueError:
        return None


@router.get("/tmdb/search")
async def tmdb_search_endpoint(query: str = "", media_type: str = "movie", year: str = "", _: bool = Depends(require_auth)):
    """Live TMDb title search for the admin 'Link TMDB' picker.

    Returns lightweight candidates (id, title, year, poster, overview) so the
    panel can show a pick-list instead of requiring a raw TMDb ID/URL.
    """
    query = (query or "").strip()
    if not query:
        return {"results": []}
    tmdb_type = "tv" if media_type == "series" else "movie"
    year_int = None
    if year:
        try:
            year_int = int(year)
        except ValueError:
            year_int = None
    results = await tmdb_search_multi(query, tmdb_type, year_int)
    out = []
    for r in results:
        out.append({
            "tmdb_id": r.get("id"),
            "title": r.get("title") or r.get("name") or "",
            "year": ((r.get("release_date") or r.get("first_air_date") or "")[:4]) or None,
            "poster": format_tmdb_image(r.get("poster_path"), "w154"),
            "overview": r.get("overview") or "",
        })
    return {"results": out}


@router.post("/files/batch_map")
async def batch_map(payload: dict, _: bool = Depends(require_auth)):
    file_ids = payload.get("file_ids", [])
    input_id = payload.get("tmdb_id", "")
    media_type = payload.get("media_type")
    is_video_song = payload.get("is_video_song", False)
    if not file_ids or not input_id or not media_type:
        return {"status": "error", "message": "Missing info"}

    tmdb_id = await _resolve_tmdb(input_id, media_type)
    if not tmdb_id:
        return {"status": "error", "message": "Could not resolve TMDb ID from input"}

    tmdb_type = "tv" if media_type == "series" else "movie"
    details = await tmdb_details(tmdb_type, tmdb_id)
    if not details:
        return {"status": "error", "message": "Invalid TMDb ID"}

    success = 0
    for file_id in file_ids:
        fdoc = await db.col("unindexed").find_one({"_id": file_id})
        if not fdoc:
            fdoc = await db.col("files").find_one({"_id": file_id})
        if not fdoc:
            continue
        await _map_file(fdoc, tmdb_id, media_type, is_video_song, details)
        await db.col("unindexed").delete_one({"_id": file_id})
        success += 1
    return {"status": "success", "count": success}


@router.post("/files/{file_id}/map")
async def single_map(file_id: str, payload: dict, _: bool = Depends(require_auth)):
    tmdb_id = payload.get("tmdb_id")
    media_type = payload.get("media_type", "movie")
    is_video_song = payload.get("is_video_song", False)
    if not tmdb_id:
        return {"status": "error", "message": "Missing tmdb_id"}

    resolved = await _resolve_tmdb(str(tmdb_id), media_type)
    if not resolved:
        return {"status": "error", "message": "Could not resolve TMDb ID"}
    details = await tmdb_details("tv" if media_type == "series" else "movie", resolved)
    if not details:
        return {"status": "error", "message": "Invalid TMDb ID"}

    fdoc = await db.col("unindexed").find_one({"_id": file_id}) or await db.col("files").find_one({"_id": file_id})
    if not fdoc:
        return {"status": "error", "message": "File not found"}
    await _map_file(fdoc, resolved, media_type, is_video_song, details)
    await db.col("unindexed").delete_one({"_id": file_id})
    return {"status": "success"}


# ---------------------------------------------------------------------------
# Channels
# ---------------------------------------------------------------------------

async def _chat_title(chat_id: int) -> str:
    try:
        from app.client import client

        if client is not None and client.is_connected:
            chat = await client.get_chat(chat_id)
            return chat.title or str(chat_id)
    except Exception:
        pass
    return str(chat_id)


@router.get("/channels")
async def channels(_: bool = Depends(require_auth)):
    idx, unidx = {}, {}
    async for c in db.col("files").aggregate([{"$group": {"_id": "$chat_id", "count": {"$sum": 1}}}]):
        try:
            idx[int(c["_id"])] = c["count"]
        except (TypeError, ValueError):
            pass
    async for c in db.col("unindexed").aggregate([{"$group": {"_id": "$chat_id", "count": {"$sum": 1}}}]):
        try:
            unidx[int(c["_id"])] = c["count"]
        except (TypeError, ValueError):
            pass
    scanned = set()
    async for s in db.col("state").find({"_id": {"$regex": "^sync_"}}):
        try:
            scanned.add(int(str(s["_id"]).split("_")[1]))
        except (ValueError, IndexError):
            pass

    result = []
    configured = set(await get_channel_config())
    for cid in sorted(set(idx) | set(unidx) | scanned | configured, key=abs):
        is_scanned = cid in scanned or cid in idx or cid in unidx
        i = idx.get(cid, 0)
        u = unidx.get(cid, 0)
        result.append({
            "chat_id": cid,
            "indexed": i if is_scanned else "--",
            "unindexed": u if is_scanned else "--",
            "total": (i + u) if is_scanned else "--",
            "is_scanned": is_scanned,
            "configured": cid in configured,
            "name": await _chat_title(cid),
        })
    return {"channels": result}


async def _resolve_chat(input_id: str) -> int | None:
    """Resolve a channel identifier (id, -100 id, or @username) to a chat id."""
    input_id = str(input_id or "").strip().lstrip("@")
    if not input_id:
        return None
    # Numeric id (positive -> -100 prefix; negative -> kept as-is).
    try:
        n = int(input_id)
        return n if n < 0 else int(f"-100{n}")
    except ValueError:
        pass
    # Username
    try:
        from app.client import client

        if client is not None and client.is_connected:
            chat = await client.get_chat(input_id)
            return int(chat.id)
    except Exception:
        pass
    return None


@router.get("/channels/config")
async def channel_config(_: bool = Depends(require_auth)):
    configured = await get_channel_config()
    return {"channels": [{"chat_id": c, "name": await _chat_title(c)} for c in configured]}


@router.post("/channels/add")
async def channel_add(payload: dict, _: bool = Depends(require_auth)):
    chat_id = await _resolve_chat(payload.get("chat_id", ""))
    if not chat_id:
        return {"status": "error", "message": "Could not resolve channel (use id, -100 id, or @username)."}
    await add_channel(chat_id)
    return {"status": "success", "chat_id": chat_id, "name": await _chat_title(chat_id)}


@router.post("/channels/remove")
async def channel_remove(payload: dict, _: bool = Depends(require_auth)):
    chat_id = await _resolve_chat(payload.get("chat_id", ""))
    if not chat_id:
        return {"status": "error", "message": "Could not resolve channel id."}
    await remove_channel(chat_id)
    purge = bool(payload.get("purge", False))
    if purge:
        # Also delete this channel's indexed files + orphaned meta.
        file_keys = [f["_id"] async for f in db.col("files").find({"chat_id": {"$in": [chat_id, str(chat_id)]}}, {"_id": 1, "meta_id": 1})]
        meta_ids = {f["meta_id"] for f in file_keys if f.get("meta_id")}
        await db.col("files").delete_many({"chat_id": {"$in": [chat_id, str(chat_id)]}})
        await db.col("unindexed").delete_many({"chat_id": {"$in": [chat_id, str(chat_id)]}})
        for mid in meta_ids:
            if not await db.col("files").find_one({"meta_id": mid}, {"_id": 1}):
                await db.col("meta").delete_one({"_id": mid})
    return {"status": "success"}


@router.get("/channels/{chat_id}/files")
async def channel_files(chat_id: int, filter: str = "indexed", page: int = 1, search: str = "", _: bool = Depends(require_auth)):
    skip = (page - 1) * PAGE_SIZE
    query = {"chat_id": {"$in": [chat_id, str(chat_id)]}}
    if search:
        query["filename"] = {"$regex": re.escape(search), "$options": "i"}

    if filter == "indexed":
        total = await db.col("files").count_documents(query)
        docs = await db.col("files").find(query).sort("_id", -1).skip(skip).limit(PAGE_SIZE).to_list(None)
        meta_ids = list({d.get("meta_id") for d in docs if d.get("meta_id")})
        metas = {}
        async for m in db.col("meta").find({"_id": {"$in": meta_ids}}):
            metas[m["_id"]] = m
        items = []
        for d in docs:
            m = metas.get(d.get("meta_id"), {})
            items.append({
                "_id": d["_id"], "filename": d["filename"], "size_str": d.get("size_str", ""),
                "status": "indexed", "tmdb_id": m.get("tmdb_id", ""),
                "meta_title": m.get("title", "Unknown"), "meta_year": m.get("year", ""),
            })
    else:
        total = await db.col("unindexed").count_documents(query)
        docs = await db.col("unindexed").find(query).sort("_id", -1).skip(skip).limit(PAGE_SIZE).to_list(None)
        items = [{"_id": d["_id"], "filename": d["filename"], "size_str": d.get("size_str", ""),
                  "status": "unindexed", "reason": d.get("reason", "Unknown")} for d in docs]

    return {"items": items, "total_pages": (total + PAGE_SIZE - 1) // PAGE_SIZE or 1, "total_items": total}


# ---------------------------------------------------------------------------
# Migration + cleanup + tasks
# ---------------------------------------------------------------------------

@router.post("/migrate")
async def migrate(_: bool = Depends(require_auth)):
    if status().get("running"):
        return {"status": "error", "message": "Stop the indexer before migration."}
    result = await start_migration()
    return {"status": "success" if result.get("ok") else "error",
            "message": result.get("message", "Migration could not start.")}


@router.post("/migrate/stop")
async def migrate_stop(_: bool = Depends(require_auth)):
    stopped = await migration_stop()
    return {"status": "success" if stopped else "error",
            "message": "Migration stop requested." if stopped else "Migration is not running."}


@router.post("/cleanup")
async def cleanup(_: bool = Depends(require_auth)):
    if cleanup_running():
        return {"status": "error", "message": "Cleanup is already running."}
    if status().get("running"):
        return {"status": "error", "message": "Stop the indexer before cleanup."}
    import asyncio

    async def _go():
        await run_cleanup_all()

    asyncio.create_task(_go())
    return {"status": "success", "message": "Cleanup started in the background!"}


@router.get("/tasks/status")
async def tasks_status(_: bool = Depends(require_auth)):
    migration = await migration_status()
    return {
        "cleanup_running": cleanup_running(),
        "migrate_running": bool(migration.get("running")),
        "migration": migration,
    }


# ---------------------------------------------------------------------------
# Health / telemetry (one call to see everything)
# ---------------------------------------------------------------------------

@router.get("/health")
async def health(_: bool = Depends(require_auth)):
    """Aggregate session + stream + error + instance info."""
    from app.client import session_status

    session = session_status()
    try:
        from app import bot_client

        bot_session = bot_client.session_status() if bot_client.is_enabled() else None
    except Exception:
        bot_session = None

    # DB counts (cheap-ish; the panel already calls these).
    files = await db.col("files").count_documents({})
    meta = await db.col("meta").count_documents({})
    unindexed = await db.col("unindexed").count_documents({})

    tele = telemetry.snapshot()

    # Adaptive flood state (is the account currently throttled?).
    try:
        from app.streamer import flood_state

        tele["flood"] = flood_state()
    except Exception:
        tele["flood"] = {"throttled": False}

    # Instance identity — makes "I have N pods" immediately obvious.
    try:
        instance = {
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
        }
    except Exception:
        instance = {"hostname": "?", "pid": os.getpid()}

    overall = "ok"
    if not session["connected"]:
        overall = "degraded"
    if session.get("last_error") and "AUTH_KEY_DUPLICATED" in str(session.get("last_error")):
        overall = "critical"

    return {
        "overall": overall,
        "session": session,
        "bot_session": bot_session,
        "indexer": status(),
        "cleanup_running": cleanup_running(),
        "catalog_counts": {"files": files, "meta": meta, "unindexed": unindexed},
        "telemetry": tele,
        "instance": instance,
    }


# ---------------------------------------------------------------------------
# PreDVD Leech Automator & Lifecycle Endpoints
# ---------------------------------------------------------------------------

@router.get("/predvd/status", dependencies=[Depends(require_auth)])
async def get_predvd_status():
    from app import predvd_automator
    settings = await predvd_automator.get_settings()
    status_info = predvd_automator.get_status()
    snapshot_count = await db.col("predvd_snapshot").count_documents({})
    history_count = await db.col("predvd_history").count_documents({})
    return {
        "status": status_info,
        "settings": settings,
        "snapshot_count": snapshot_count,
        "history_count": history_count,
    }


@router.get("/predvd/settings", dependencies=[Depends(require_auth)])
async def get_predvd_settings():
    from app import predvd_automator
    return await predvd_automator.get_settings()


@router.post("/predvd/settings", dependencies=[Depends(require_auth)])
async def update_predvd_settings(request: Request):
    from app import predvd_automator
    body = await request.json()
    saved = await predvd_automator.save_settings(body)
    return {"ok": True, "settings": saved}


@router.get("/predvd/feed", dependencies=[Depends(require_auth)])
async def get_predvd_feed():
    from app import predvd_automator
    settings = await predvd_automator.get_settings()
    feed_items = await predvd_automator.fetch_feed(settings.get("feed_url"))
    tamil_predvds = []
    for m in feed_items:
        if predvd_automator.is_tamil_release(m) and predvd_automator.is_predvd_release(m):
            key = m.get("imdbId") or m.get("name") or m.get("titleGuess") or m.get("rawTitle")
            snapshot_doc = await db.col("predvd_snapshot").find_one({"_id": key}) if key else None
            tamil_predvds.append({
                "title": m.get("name") or m.get("titleGuess") or m.get("rawTitle") or "Untitled",
                "year": m.get("year") or m.get("yearGuess") or 2026,
                "imdb_id": m.get("imdbId") or "",
                "poster": m.get("poster") or m.get("thumbnail") or "",
                "languages": m.get("languages") or ["Tamil"],
                "qualities": m.get("qualities") or [],
                "page_url": m.get("pageUrl") or "",
                "is_baseline": bool(snapshot_doc and snapshot_doc.get("is_baseline")),
                "leeched": bool(snapshot_doc and snapshot_doc.get("leeched")),
                "raw_text": m.get("rawText") or m.get("rawTitle") or "",
            })
    return {"ok": True, "count": len(tamil_predvds), "total_scraped": len(feed_items), "items": tamil_predvds}


@router.get("/predvd/history", dependencies=[Depends(require_auth)])
async def get_predvd_history(limit: int = 50):
    cursor = db.col("predvd_history").find({}).sort("timestamp", -1).limit(limit)
    rows = await cursor.to_list(None)
    for r in rows:
        r["_id"] = str(r["_id"])
    return {"ok": True, "history": rows}


@router.post("/predvd/sync", dependencies=[Depends(require_auth)])
async def trigger_predvd_sync():
    from app import predvd_automator
    result = await predvd_automator.process_feed_iteration()
    return {"ok": True, "result": result}


@router.post("/predvd/reset-baseline", dependencies=[Depends(require_auth)])
async def trigger_reset_baseline():
    from app import predvd_automator
    count = await predvd_automator.reset_baseline_snapshot()
    return {"ok": True, "baseline_count": count, "message": f"Baseline snapshot reset with {count} movies. Existing PreDVDs will be ignored."}


@router.post("/predvd/manual-leech", dependencies=[Depends(require_auth)])
async def trigger_manual_leech(request: Request):
    from app import predvd_automator
    body = await request.json()
    magnet_url = (body.get("magnet_url") or "").strip()
    title = (body.get("title") or "Manual Leech").strip()
    quality = (body.get("quality") or "PreDVD").strip()

    if not magnet_url.startswith("magnet:"):
        raise HTTPException(status_code=400, detail="Invalid magnet URL (must start with magnet:)")

    settings = await predvd_automator.get_settings()
    group_id = settings.get("group_id")
    command_prefix = settings.get("command_prefix")

    success, msg_info = await predvd_automator.send_leech_command(
        group_id=group_id,
        command_prefix=command_prefix,
        magnet_url=magnet_url,
        title=title,
        quality=quality
    )
    if not success:
        raise HTTPException(status_code=400, detail=msg_info)

    await db.col("predvd_history").insert_one({
        "action": "manual_leech_sent",
        "title": title,
        "quality": quality,
        "magnet_url": magnet_url,
        "timestamp": time.time(),
        "info": msg_info
    })

    return {"ok": True, "message": f"Leech command sent for {title}! ({msg_info})"}

