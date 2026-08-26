"""Stremio addon endpoints: manifest, catalog, meta, stream."""
import re
from urllib.parse import quote, unquote

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from app import config, db
from app import parser as P
from app import token as tok
from app.cache import (
    CATALOG_TTL,
    FILE_IDS_TTL,
    MANIFEST_TTL,
    META_TTL,
    catalog_cache,
    file_ids_cache,
    manifest_cache,
    meta_cache,
)

router = APIRouter(prefix="/stremio", tags=["stremio"])

PAGE_SIZE = 15

GENRES = [
    "Action", "Adventure", "Animation", "Biography", "Comedy",
    "Crime", "Documentary", "Drama", "Family", "Fantasy",
    "History", "Horror", "Music", "Mystery", "Romance",
    "Sci-Fi", "Sport", "Thriller", "War", "Western",
]

LANGUAGES = ["Tamil", "Telugu", "Hindi", "Malayalam", "Kannada", "English", "Multi"]

ADDON_NAME = "Global Stremio"
ADDON_DESC = "Streams movies and series from your indexed Telegram channels."


def _resolution_priority(name: str) -> int:
    m = {
        "2160p": 2160, "4k": 2160, "uhd": 2160,
        "1080p": 1080, "fhd": 1080,
        "720p": 720, "hd": 720,
        "480p": 480, "sd": 480,
        "360p": 360,
    }
    low = (name or "").lower()
    for key, val in m.items():
        if key in low:
            return val
    return 1


def _language_priority(title: str) -> int:
    low = (title or "").lower()
    if re.search(r"\b(tamil|tam)\b", low):
        return 3
    if re.search(r"\bmulti\b", low):
        return 2
    return 1


def _parse_size(size_str: str) -> int:
    if not size_str:
        return 0
    m = re.match(r"([\d.]+)\s*([A-Za-z]+)", str(size_str).strip())
    if not m:
        return 0
    value, unit = float(m.group(1)), m.group(2).upper()
    mult = {"B": 1, "KB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3, "TB": 1024 ** 4}
    return int(value * mult.get(unit, 1))


def _format_stream_title(filename: str, quality: str, size: str) -> str:
    return f"📁 {filename}\n💾 {size}"


async def _meta_ids_with_files() -> frozenset:
    """Meta ids that currently have >=1 file (cached). Used to hide orphan titles."""
    ids = file_ids_cache.get("ids")
    if ids is None:
        ids = frozenset(await db.col("files").distinct("meta_id"))
        file_ids_cache.set("ids", ids, FILE_IDS_TTL)
    return ids


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------


@router.get("/{token}/manifest.json")
async def manifest(token: str):
    from app.security import require_token

    require_token(token)

    cached = manifest_cache.get(token)
    if cached is not None:
        return cached

    catalogs = []
    ids_with_files = await _meta_ids_with_files()
    counts = {}
    if ids_with_files:
        async for c in db.col("meta").aggregate([
            {"$match": {"_id": {"$in": list(ids_with_files)}}},
            {"$group": {"_id": "$catalog", "count": {"$sum": 1}}},
        ]):
            counts[c["_id"]] = c["count"]
    g_cats = await db.col("catalogs").find().sort("order", 1).to_list(None)
    for gc in g_cats:
        if counts.get(gc["_id"], 0) > 0:
            catalogs.append({
                "type": gc.get("type", "movie"),
                "id": f"global_{gc['_id']}",
                "name": gc.get("name", "Global Catalog"),
                "extra": [
                    {"name": "search", "isRequired": False},
                    {"name": "genre", "isRequired": False, "options": GENRES},
                    {"name": "language", "isRequired": False, "options": LANGUAGES},
                    {"name": "sort", "isRequired": False,
                     "options": ["Latest Added", "Year: Newest", "Year: Oldest", "Highest Rated", "Title: A-Z"]},
                    {"name": "skip", "isRequired": False},
                ],
            })

    manifest_data = {
        "id": f"global.stremio.{token[:8]}",
        "version": "1.0.0",
        "name": ADDON_NAME,
        "description": ADDON_DESC,
        "types": ["movie", "series"],
        "resources": ["catalog", "meta", "stream"],
        "catalogs": catalogs,
        "idPrefixes": ["tt", "tg", "tmdb", "tmdb:", "song:"],
        "behaviorHints": {"configurable": True, "configurationRequired": False},
    }
    manifest_cache.set(token, manifest_data, MANIFEST_TTL)
    return manifest_data


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------


@router.get("/{token}/catalog/{media_type}/{id}.json")
@router.get("/{token}/catalog/{media_type}/{id}/{extra:path}.json")
async def catalog(token: str, media_type: str, id: str, extra: str = None):
    from app.security import require_token

    require_token(token)
    if media_type not in ("movie", "series"):
        raise HTTPException(status_code=404, detail="Invalid type")

    cache_key = (token, media_type, id, extra or "")
    cached = catalog_cache.get(cache_key)
    if cached is not None:
        return cached

    genre_filter = search_query = language_filter = None
    sort_filter = "Latest Added"
    stremio_skip = 0
    if extra:
        for param in extra.replace("&", "/").split("/"):
            if param.startswith("genre="):
                genre_filter = unquote(param.removeprefix("genre="))
            elif param.startswith("search="):
                search_query = unquote(param.removeprefix("search="))
            elif param.startswith("language="):
                language_filter = unquote(param.removeprefix("language="))
            elif param.startswith("sort="):
                sort_filter = unquote(param.removeprefix("sort="))
            elif param.startswith("skip="):
                try:
                    stremio_skip = int(param.removeprefix("skip="))
                except ValueError:
                    stremio_skip = 0

    page = (stremio_skip // PAGE_SIZE) + 1

    if not id.startswith("global_"):
        return {"metas": []}
    real_id = id.replace("global_", "", 1)

    query = {"catalog": real_id}
    # Only list titles that actually have playable files (hides orphan metas).
    ids_with_files = await _meta_ids_with_files()
    if ids_with_files:
        query["_id"] = {"$in": list(ids_with_files)}
    if search_query:
        query["title"] = {"$regex": re.escape(search_query), "$options": "i"}
    if genre_filter:
        query["genres"] = genre_filter
    if language_filter:
        query["languages"] = language_filter

    sort_logic = [("updated_at", -1), ("_id", -1)]
    if sort_filter == "Year: Newest":
        sort_logic = [("year", -1), ("_id", -1)]
    elif sort_filter == "Year: Oldest":
        sort_logic = [("year", 1), ("_id", 1)]
    elif sort_filter == "Highest Rated":
        sort_logic = [("rating", -1), ("_id", -1)]
    elif sort_filter == "Title: A-Z":
        sort_logic = [("title", 1), ("_id", 1)]

    skip = (page - 1) * PAGE_SIZE
    cursor = db.col("meta").find(query).sort(sort_logic).skip(skip).limit(PAGE_SIZE)
    items = [doc async for doc in cursor]

    metas = [{
        "id": item.get("imdb_id") or item["_id"],
        "type": item.get("media_type", "movie"),
        "name": item.get("title", ""),
        "poster": item.get("poster", ""),
        "background": item.get("background", ""),
        "year": item.get("year"),
        "releaseInfo": str(item.get("year", "")),
        "description": item.get("description", ""),
        "genres": item.get("genres", []),
    } for item in items]
    result = {"metas": metas}
    catalog_cache.set(cache_key, result, CATALOG_TTL)
    return result


# ---------------------------------------------------------------------------
# Meta
# ---------------------------------------------------------------------------


@router.get("/{token}/meta/{media_type}/{id}.json")
async def meta(token: str, media_type: str, id: str):
    from app.security import require_token

    require_token(token)
    id = unquote(id)

    cache_key = (token, media_type, id)
    cached = meta_cache.get(cache_key)
    if cached is not None:
        return cached

    g_meta = await P.resolve_global_meta(db.col("meta"), id)
    if not g_meta:
        return {"meta": {}}

    global_type = g_meta.get("media_type", media_type)
    meta_obj = {
        "id": id,
        "type": global_type,
        "name": g_meta.get("title", ""),
        "description": g_meta.get("description", ""),
        "year": str(g_meta.get("year", "")),
        "releaseInfo": str(g_meta.get("year", "")),
        "imdbRating": str(g_meta.get("rating") or ""),
        "imdb_id": g_meta.get("imdb_id") or "",
        "moviedb_id": g_meta.get("tmdb_id") or "",
        "poster": g_meta.get("poster", ""),
        "background": g_meta.get("background", ""),
        "genres": g_meta.get("genres", []),
    }

    if global_type == "series":
        from app.metadata import tmdb_details

        files = await db.col("files").find(
            {"meta_id": g_meta["_id"]},
            {"season": 1, "episode_start": 1, "episode_end": 1},
        ).to_list(None)
        episode_keys, pack_seasons = set(), set()
        for fdoc in files:
            season = P.first_int(fdoc.get("season"))
            start = P.first_int(fdoc.get("episode_start"))
            end = P.first_int(fdoc.get("episode_end"))
            if season is None:
                continue
            if start is None:
                pack_seasons.add(season)
                continue
            end = start if end is None else end
            if 0 <= end - start <= 500:
                episode_keys.update((season, e) for e in range(start, end + 1))

        if pack_seasons and g_meta.get("tmdb_id"):
            try:
                details = await tmdb_details("tv", g_meta["tmdb_id"])
                for s in (details.get("seasons") or []):
                    sn, count = s.get("season_number"), s.get("episode_count", 0) or 0
                    if sn in pack_seasons:
                        episode_keys.update((int(sn), e) for e in range(1, int(count) + 1))
            except Exception:
                pass

        meta_obj["videos"] = [
            {"id": f"{id}:{season}:{episode}", "title": f"Episode {episode}",
             "season": season, "episode": episode}
            for season, episode in sorted(episode_keys)
        ]
    result = {"meta": meta_obj}
    meta_cache.set(cache_key, result, META_TTL)
    return result


# ---------------------------------------------------------------------------
# Stream
# ---------------------------------------------------------------------------


@router.get("/{token}/stream/{media_type}/{id}.json")
async def stream(token: str, media_type: str, id: str):
    from app.security import require_token

    require_token(token)
    id = unquote(id)

    try:
        if id.startswith("song:tmdb:"):
            parts = id.split(":")
            imdb_id = f"song:tmdb:{parts[2]}"
            season = int(parts[3]) if len(parts) > 3 else None
            episode = int(parts[4]) if len(parts) > 4 else None
        elif id.startswith("tmdb:"):
            parts = id.split(":")
            imdb_id = f"tmdb:{parts[1]}"
            season = int(parts[2]) if len(parts) > 2 else None
            episode = int(parts[3]) if len(parts) > 3 else None
        elif id.startswith("song:"):
            # song:tt1234567 — full id is the meta key, no season/episode.
            imdb_id = id
            season = episode = None
        else:
            parts = id.split(":")
            imdb_id = parts[0]
            season = int(parts[1]) if len(parts) > 1 else None
            episode = int(parts[2]) if len(parts) > 2 else None
    except (ValueError, IndexError):
        raise HTTPException(status_code=400, detail="Invalid id")

    g_meta = await P.resolve_global_meta(db.col("meta"), imdb_id)
    if not g_meta:
        return {"streams": []}

    query = P.build_global_file_query(g_meta["_id"], season, episode)
    streams = []
    async for fdoc in db.col("files").find(query):
        sid = tok.encode_payload({"chat_id": int(fdoc["chat_id"]), "msg_id": int(fdoc["message_id"])})
        filename = fdoc.get("filename", "")
        quality = fdoc.get("quality", "HD")
        size = fdoc.get("size_str", "")
        # Pre-computed technical metadata (fast path; no PTN at request time).
        codec = fdoc.get("codec") or ""
        audio = fdoc.get("audio") or ""
        name = f"🌐 GLOBAL {quality}"
        combined = P.parse_combined_episodes(filename)
        if combined:
            label = "Full" if combined.get("start") is None else f"E{combined['start']:02d}-E{combined['end']:02d}"
            if label.lower() not in name.lower():
                name = f"{name} {label}"
        title_parts = [f"📁 {filename}", f"💾 {size}"]
        if codec:
            title_parts.append(f"🎥 {codec}")
        if audio:
            title_parts.append(f"🔊 {audio}")
        title_parts.append("📡 Telegram")
        title = "\n".join(title_parts)
        streams.append({
            "name": name,
            "title": title,
            "url": f"{config.BASE_URL}/dl/{token}/{sid}/{quote(filename)}",
            "size_bytes": _parse_size(size),
            "_res": _resolution_priority(name),
            "_lang": _language_priority(filename),
        })

    streams.sort(key=lambda s: (s["_lang"], s["_res"], s["size_bytes"]), reverse=True)
    for s in streams:
        s.pop("_res", None)
        s.pop("_lang", None)

    # de-duplicate identical names with a counter
    counts: dict = {}
    for s in streams:
        counts[s["name"]] = counts.get(s["name"], 0) + 1
    seen: dict = {}
    for s in streams:
        if counts[s["name"]] > 1:
            seen[s["name"]] = seen.get(s["name"], 0) + 1
            s["name"] = f"{s['name']} ({seen[s['name']]})"
    return {"streams": streams}
