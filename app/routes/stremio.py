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

# A YouTube video id, as returned by TMDb. Kept strict on purpose: a trailer
# stream Stremio cannot parse makes it reject the ENTIRE meta response, so a
# malformed key must never reach the wire.
_YT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")

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


# ---------------------------------------------------------------------------
# Stremio wire-format helpers
#
# stremio-core deserializes a meta response into a strict Rust struct. If ANY
# field fails to parse, the whole response is discarded and the details page
# renders "There is no information available" — one bad trailer key or one
# `type: "tv"` silently kills an otherwise perfect title. Everything below
# exists to guarantee we only ever emit values that struct accepts.
# ---------------------------------------------------------------------------


def _resolve_type(stored, requested) -> str:
    """The single Stremio content type to answer with.

    A stored `media_type` we recognize is authoritative ("tv"/"show" normalize
    to "series"). Otherwise trust the type Stremio asked for — that is the type
    the catalog advertised, so echoing anything else makes the details page a
    type mismatch.
    """
    t = P.stremio_type(stored, None)
    if t:
        return t
    return P.stremio_type(requested, "movie")


def _year_value(doc: dict):
    """A year as int, or None. Never the string 'None'."""
    y = doc.get("year")
    if isinstance(y, bool) or y is None:
        return None
    try:
        y = int(str(y).strip()[:4])
    except (TypeError, ValueError):
        return None
    return y if 1870 <= y <= 2100 else None


def _release_info(doc: dict, stype: str) -> str:
    """`releaseInfo` per the addon spec: '2011' for movies, '2011-' for a
    still-running series. Empty string when the year is unknown."""
    y = _year_value(doc)
    if not y:
        return ""
    if stype == "series" and not doc.get("year_end"):
        return f"{y}-"
    end = _year_value({"year": doc.get("year_end")}) if doc.get("year_end") else None
    return f"{y}-{end}" if end and end > y else str(y)


def _display_name(doc: dict) -> str:
    """`name` is REQUIRED by the spec — an empty one renders a blank page."""
    for key in ("title", "title_english", "original_title"):
        v = str(doc.get(key) or "").strip()
        if v:
            return v
    tmdb_id = doc.get("tmdb_id")
    return f"Untitled (tmdb:{tmdb_id})" if tmdb_id else "Untitled"


def _safe_moviedb_id(doc: dict) -> str:
    """`moviedb_id` must be a string; null/empty breaks some Stremio clients."""
    v = doc.get("tmdb_id")
    if v is None:
        return ""
    v = str(v).strip()
    return "" if v.lower() in ("", "none", "null") else v


def _rating_str(doc: dict) -> str:
    """`imdbRating` is a 0.0–10.0 string. TMDb's raw float would print as
    '8.708' — round it, and never emit '0.0' or 'None' for an unrated title."""
    try:
        r = float(doc.get("rating"))
    except (TypeError, ValueError):
        return ""
    if r <= 0:
        return ""
    return f"{round(min(r, 10.0), 1)}"


def _clean_url(value) -> str:
    """Only absolute http(s) URLs survive: stremio-core parses poster /
    background / logo with `Url::parse`, and a relative or malformed string
    makes it drop the entire meta item."""
    v = str(value or "").strip()
    return v if v.startswith(("http://", "https://")) else ""


def _genre_list(doc: dict) -> list:
    g = doc.get("genres")
    if isinstance(g, str):
        g = [x.strip() for x in re.split(r"[,|/]", g) if x.strip()]
    if not isinstance(g, (list, tuple)):
        return []
    return [str(x).strip() for x in g if str(x or "").strip()][:20]


def _trailer_streams(doc: dict) -> list:
    """Trailer as a REAL Stremio Stream object.

    The Stream source is flattened into the object, so a YouTube trailer must
    be `{"ytId": "<11-char id>"}` — `{"source": "<id>"}` matches no stream
    source variant at all and makes stremio-core reject the whole meta
    response (this is what turned every trailer-having title into
    "no information available").
    """
    key = str(doc.get("trailer_yt") or "").strip()
    if not key:
        return []
    if key.startswith(("http://", "https://")):
        m = re.search(r"(?:v=|youtu\.be/|shorts/|embed/)([A-Za-z0-9_-]{11})", key)
        key = m.group(1) if m else ""
    if not _YT_ID_RE.match(key):
        return []
    return [{"ytId": key, "name": "Trailer", "description": "▶ Official Trailer"}]


async def _meta_ids_with_files() -> frozenset:
    """Meta ids that currently have >=1 file (cached). Used to hide orphan titles."""
    ids = file_ids_cache.get("ids")
    if ids is None:
        ids = frozenset(await db.col("files").distinct("meta_id"))
        file_ids_cache.set("ids", ids, FILE_IDS_TTL)
    return ids


async def _shared_imdb_ids() -> frozenset:
    """IMDb ids claimed by MORE THAN ONE meta doc (cached).

    A catalog entry's id is what Stremio later asks `/meta/{type}/{id}.json`
    for, so it has to address exactly one title. When an original and its
    remake (or a movie and its series adaptation) share one IMDb id, an id of
    `tt…` is ambiguous and the click opens whichever document Mongo happens to
    return first. Those titles fall back to their unique canonical `tmdb:…` id.
    """
    key = "dup_imdb"
    dupes = file_ids_cache.get(key)
    if dupes is None:
        dupes = set()
        try:
            async for row in db.col("meta").aggregate([
                {"$match": {"imdb_id": {"$type": "string", "$ne": ""}}},
                {"$group": {"_id": "$imdb_id", "n": {"$sum": 1}}},
                {"$match": {"n": {"$gt": 1}}},
            ]):
                dupes.add(row["_id"])
        except Exception:
            dupes = set()
        dupes = frozenset(dupes)
        file_ids_cache.set(key, dupes, FILE_IDS_TTL)
    return dupes


def _public_id(doc: dict, shared_imdb: frozenset) -> str:
    """The Stremio id for a meta doc: its IMDb id when that is unambiguous,
    otherwise the canonical (always unique) `_id`."""
    imdb = str(doc.get("imdb_id") or "").strip()
    if imdb and imdb not in shared_imdb:
        return imdb
    return str(doc["_id"])


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

    metas = []
    shared_imdb = await _shared_imdb_ids()
    for item in items:
        stype = _resolve_type(item.get("media_type"), media_type)
        # Every catalog entry MUST carry id/type/name; stremio-core skips an
        # entry it cannot parse, but a bogus `type` also poisons the meta
        # request that follows the click.
        metas.append({
            "id": _public_id(item, shared_imdb),
            "type": stype,
            "name": _display_name(item),
            "poster": _clean_url(item.get("poster")),
            "background": _clean_url(item.get("background")),
            "releaseInfo": _release_info(item, stype),
            "description": str(item.get("description") or ""),
            "genres": _genre_list(item),
            "imdbRating": _rating_str(item),
        })
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

    # Pass the requested type in: several titles can share one IMDb id
    # (original + remake, movie + its series adaptation), and picking the
    # wrong one used to open a different title — or a `series` doc from a
    # `movie` catalog, which Stremio then refuses to render.
    g_meta = await P.resolve_global_meta(db.col("meta"), id, want_type=media_type)
    if not g_meta:
        # Deliberately NOT cached: a transient miss must not pin a title to
        # "no information available" for the next META_TTL seconds.
        return {"meta": {}}

    # Stremio only accepts movie/series/channel. "tv" (what the upstream
    # project and older index runs stored) means LIVE TV to Stremio, so a VOD
    # series tagged "tv" renders as an empty channel page. A stored type we do
    # recognize wins; an absent/garbage one falls back to the type Stremio
    # asked for (which is what the catalog told it), and only then to "movie".
    global_type = _resolve_type(g_meta.get("media_type"), media_type)

    meta_obj = {
        "id": id,
        "type": global_type,
        "name": _display_name(g_meta),
        "description": str(g_meta.get("description") or ""),
        "releaseInfo": _release_info(g_meta, global_type),
        "imdbRating": _rating_str(g_meta),
        "imdb_id": str(g_meta.get("imdb_id") or ""),
        "poster": _clean_url(g_meta.get("poster")),
        "background": _clean_url(g_meta.get("background")),
        "genres": _genre_list(g_meta),
    }
    moviedb_id = _safe_moviedb_id(g_meta)
    if moviedb_id:
        meta_obj["moviedb_id"] = moviedb_id
    year = _year_value(g_meta)
    if year:
        meta_obj["year"] = str(year)

    # v17: trailer + still imagery stored on the meta doc at index time —
    # zero extra TMDb calls at request time, and total absence is fine.
    trailers = _trailer_streams(g_meta)
    if trailers:
        meta_obj["trailerStreams"] = trailers
    if not meta_obj["background"]:
        meta_obj["background"] = _clean_url(g_meta.get("still_path"))

    if global_type == "series":
        from app.metadata import tmdb_details

        files = await db.col("files").find(
            {"meta_id": g_meta["_id"]},
            {"season": 1, "episode_start": 1, "episode_end": 1, "filename": 1},
        ).to_list(None)
        episode_keys, pack_seasons = set(), set()
        for fdoc in files:
            season = P.first_int(fdoc.get("season"))
            start = P.first_int(fdoc.get("episode_start"))
            end = P.first_int(fdoc.get("episode_end"))
            if start is None:
                _info = P.analyze_episodes(str(fdoc.get("filename") or ""))
                if _info and _info.get("start") is not None:
                    start = _info["start"]
                    end = _info["end"] or start
                    if season is None:
                        season = _info["season"] or 1
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

        # Episode rows get season posters (or the title still) as thumbnails.
        _sp = g_meta.get("season_posters") or {}
        _fallback_thumb = _clean_url(g_meta.get("still_path"))
        vids = []
        for season, episode in sorted(episode_keys):
            # stremio-core deserializes season/episode into u32 — a negative
            # or non-integer value makes it drop the whole meta response.
            try:
                season_i, episode_i = int(season), int(episode)
            except (TypeError, ValueError):
                continue
            if season_i < 0 or episode_i < 0:
                continue
            v = {"id": f"{id}:{season_i}:{episode_i}",
                 "title": f"Episode {episode_i}",
                 "season": season_i, "episode": episode_i}
            thumb = _clean_url(_sp.get(str(season_i)) or _sp.get(season_i)) or _fallback_thumb
            if thumb:
                v["thumbnail"] = thumb
            vids.append(v)

        if not vids:
            # Whole-season packs whose TMDb episode expansion failed (no
            # TMDB_API key, rate limit, offline) used to produce an EMPTY
            # videos list: a series page with nothing to click. Fall back to
            # one row per pack season — the stream endpoint already matches a
            # season-pack file to any episode of that season.
            for season in sorted(pack_seasons):
                try:
                    season_i = int(season)
                except (TypeError, ValueError):
                    continue
                if season_i < 0:
                    continue
                v = {"id": f"{id}:{season_i}:1",
                     "title": f"Season {season_i} (full pack)",
                     "season": season_i, "episode": 1}
                thumb = _clean_url(_sp.get(str(season_i)) or _sp.get(season_i)) or _fallback_thumb
                if thumb:
                    v["thumbnail"] = thumb
                vids.append(v)

        meta_obj["videos"] = vids
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

    g_meta = await P.resolve_global_meta(db.col("meta"), imdb_id, want_type=media_type)
    if not g_meta:
        return {"streams": []}

    query = P.build_global_file_query(g_meta["_id"], season, episode)
    streams = []
    async for fdoc in db.col("files").find(query):
        sid = tok.encode_payload({"chat_id": int(fdoc["chat_id"]), "msg_id": int(fdoc["message_id"])})
        raw_filename = fdoc.get("filename", "")
        # Clean multi-line caption data from filename for stream title and URL path
        clean_name = raw_filename.split("\n")[0].strip() or "video.mkv"
        clean_name = re.sub(r"[\r\n\t]+", " ", clean_name).strip()
        quality = fdoc.get("quality", "HD")
        size = fdoc.get("size_str", "")
        # Pre-computed technical metadata (fast path; no PTN at request time).
        codec = fdoc.get("codec") or ""
        audio = fdoc.get("audio") or ""
        # Episode range: stored DB fields first; re-parse the filename for
        # legacy docs indexed before the parser fix. This is what stops an
        # E01-04 pack from leaking into EVERY episode's list (ep 9 showed it).
        f_start = P.first_int(fdoc.get("episode_start"))
        f_end = P.first_int(fdoc.get("episode_end"))
        if f_start is None:
            _info = P.analyze_episodes(clean_name)
            if _info and _info.get("start") is not None:
                f_start = _info["start"]
                f_end = _info["end"] or f_start
        if episode is not None and f_start is not None:
            _hi = f_end if f_end is not None else f_start
            if not (f_start <= episode <= _hi):
                continue  # strict: this file does not contain the requested episode
        # Info-chip name (replaces "🌐 GLOBAL"): one audio truth + quality +
        # codec + one structure truth. The filename itself is NEVER touched —
        # it stays verbatim in the 📁 title line below.
        _prof = P.audio_profile_from_filename(clean_name)
        _chips: list[str] = []
        _lang_chip = P.language_chip(_prof)
        if _lang_chip:
            _chips.append(_lang_chip)
        _chips.append(str(quality or "HD"))
        if codec:
            _chips.append(str(codec))
        _fseason = P.first_int(fdoc.get("season"))
        if f_start is not None:
            _hi = f_end if f_end is not None else f_start
            if _hi > f_start:
                _chips.append(f"🧩 COMBINED E{f_start:02d}-E{_hi:02d}")
            elif season is not None or _fseason is not None:
                _chips.append(f"E{f_start:02d}")
        elif P.parse_combined_episodes(clean_name) and (season is not None or _fseason is not None):
            _sn = _fseason if _fseason is not None else season
            _chips.append(f"📦 FULL SEASON S{_sn:02d}" if _sn is not None else "📦 FULL SEASON")
        if _prof.get("subs_en"):
            _chips.append("💬 SUBS(EN)")
        name = " · ".join(_chips)
        title_parts = [f"📁 {clean_name}", f"💾 {size}"]
        if codec:
            title_parts.append(f"🎥 {codec}")
        if audio:
            title_parts.append(f"🔊 {audio}")
        title_parts.append("📡 Telegram")
        title = "\n".join(title_parts)
        streams.append({
            "name": name,
            "title": title,
            # safe='' : a Telegram filename containing "/" used to split into
            # extra URL path segments and 404 the /dl/{token}/{sid}/{name} route.
            "url": f"{config.BASE_URL}/dl/{token}/{sid}/{quote(clean_name, safe='')}",
            "size_bytes": _parse_size(size),
            # Spec location for the byte size is behaviorHints.videoSize (it is
            # what gets handed to subtitle addons); a top-level "videoSize" is
            # simply ignored by Stremio.
            "behaviorHints": {
                k: v for k, v in (
                    ("videoSize", int(fdoc.get("size") or 0) or None),
                    ("filename", clean_name or None),
                ) if v
            },
            "_res": _resolution_priority(name),
            "_lang": _language_priority(clean_name),
        })

    streams.sort(key=lambda s: (s["_lang"], s["_res"], s["size_bytes"]), reverse=True)
    for s in streams:
        s.pop("_res", None)
        s.pop("_lang", None)
        if not s.get("behaviorHints"):
            s.pop("behaviorHints", None)

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
