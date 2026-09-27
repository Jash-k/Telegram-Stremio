"""TMDb metadata lookups via a single lightweight httpx client.

Returns plain dicts (decoded JSON) — much lighter than the upstream project's
multi-provider metadata stack, and sufficient for GlobalDB indexing.
"""
from typing import Optional

import httpx

from . import config

TMDB_BASE = "https://api.themoviedb.org/3"
IMAGE_BASE = "https://image.tmdb.org/t/p"

_client: Optional[httpx.AsyncClient] = None


def _http() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=httpx.Timeout(20.0))
    return _client


async def _get(path: str, params: dict) -> Optional[dict]:
    params = dict(params or {})
    params["api_key"] = config.TMDB_API
    try:
        r = await _http().get(f"{TMDB_BASE}{path}", params=params)
    except Exception:
        return None
    if r.status_code == 200:
        return r.json()
    return None


async def tmdb_search(title: str, media_type: str, year=None) -> Optional[dict]:
    path = "/search/movie" if media_type == "movie" else "/search/tv"
    params = {"query": title, "include_adult": "true", "language": "en-US"}
    if media_type == "movie" and year:
        params["year"] = int(year)
        params["primary_release_year"] = int(year)
    data = await _get(path, params)
    results = (data.get("results") or []) if data else []
    
    # Fallback 1: try without primary_release_year if year was specified
    if not results and year:
        params.pop("primary_release_year", None)
        data = await _get(path, params)
        results = (data.get("results") or []) if data else []

    # Fallback 2: try without strict year if still empty
    if not results and year:
        params.pop("year", None)
        params.pop("primary_release_year", None)
        data = await _get(path, params)
        results = (data.get("results") or []) if data else []

    if not results:
        return None

    # Exact title match priority
    norm_title = str(title).strip().lower()
    for item in results:
        t = (item.get("title") or item.get("name") or "").strip().lower()
        if t == norm_title:
            return item

    return results[0]


async def tmdb_search_multi(title: str, media_type: str, year=None, limit: int = 10) -> list[dict]:
    """Return up to `limit` raw TMDb search results (for admin picker UIs)."""
    if not title:
        return []
    path = "/search/movie" if media_type == "movie" else "/search/tv"
    params = {"query": title, "include_adult": "true", "language": "en-US"}
    if media_type == "movie" and year:
        params["year"] = int(year)
    data = await _get(path, params)
    if not data:
        return []
    return (data.get("results") or [])[:limit]


async def tmdb_details(media_type: str, tmdb_id) -> Optional[dict]:
    path = f"/{'movie' if media_type == 'movie' else 'tv'}/{tmdb_id}"
    # v17: fetch the trailer list with the details themselves — one call now
    # buys Stremio trailerStreams forever (no extra API call at stream time).
    return await _get(path, {"append_to_response": "external_ids,videos", "language": "en-US"})


def pick_trailer_key(details: dict) -> str:
    """Best YouTube key from a TMDb details payload (official Trailer first)."""
    results = ((details or {}).get("videos") or {}).get("results") or []
    best_score, best_key = None, ""
    for v in results:
        if v.get("site") != "YouTube" or not v.get("key"):
            continue
        vtype = v.get("type")
        if vtype not in ("Trailer", "Teaser", "Clip"):
            continue
        try:
            pop = float(v.get("popularity") or 0)
        except (TypeError, ValueError):
            pop = 0.0
        score = (1 if v.get("official") else 0,
                 1 if vtype == "Trailer" else 0,
                 pop)
        if best_score is None or score > best_score:
            best_score, best_key = score, str(v["key"])
    return best_key


def media_fields_from_details(details: dict, media_type: str) -> dict:
    """Index-time extras stored on the meta doc (v17): episode stills,
    season posters, trailer key. All optional — absence must never break a
    Stremio response."""
    out: dict = {}
    try:
        still = details.get("still_path") or details.get("backdrop_path")
        if still:
            out["still_path"] = format_tmdb_image(still, "w780")
        out["trailer_yt"] = pick_trailer_key(details)
        # meta docs store "series"; TMDb calls use "tv" — accept both.
        if media_type in ("tv", "series"):
            posters = {}
            for s in details.get("seasons") or []:
                sn = s.get("season_number")
                if sn is not None and s.get("poster_path"):
                    posters[str(int(sn))] = format_tmdb_image(s.get("poster_path"))
            if posters:
                out["season_posters"] = posters
    except Exception:
        pass
    return out


async def tmdb_find_by_imdb(imdb_id: str) -> Optional[dict]:
    """Resolve an IMDb id to a TMDb result via the /find endpoint."""
    data = await _get("/find/" + imdb_id, {"external_source": "imdb_id", "language": "en-US"})
    if not data:
        return None
    for key in ("movie_results", "tv_results"):
        results = data.get(key) or []
        if results:
            return results[0]
    return None


def format_tmdb_image(path: str, size: str = "w500") -> str:
    return f"{IMAGE_BASE}/{size}{path}" if path else ""


def year_number(details: dict, media_type: str) -> Optional[int]:
    date = details.get("release_date") if media_type == "movie" else details.get("first_air_date")
    if isinstance(date, str) and len(date) >= 4:
        try:
            return int(date[:4])
        except ValueError:
            return None
    return None
