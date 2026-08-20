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
    data = await _get(path, params)
    if not data:
        return None
    results = data.get("results") or []
    return results[0] if results else None


async def tmdb_details(media_type: str, tmdb_id) -> Optional[dict]:
    path = f"/{'movie' if media_type == 'movie' else 'tv'}/{tmdb_id}"
    return await _get(path, {"append_to_response": "external_ids", "language": "en-US"})


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
