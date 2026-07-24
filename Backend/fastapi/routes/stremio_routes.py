import asyncio
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import quote, unquote

import PTN
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.templating import Jinja2Templates
from pyrogram.enums import ChatMemberStatus
from pyrogram.errors import UserNotParticipant

from Backend import __version__, db
from Backend.config import Telegram
from Backend.fastapi.security.tokens import verify_token
from Backend.fastapi.themes import DEFAULT_THEME, get_theme
from Backend.helper.fanart import fanart_artwork
from Backend.helper.global_search import global_search, is_global_search_enabled
from Backend.helper.imdb import get_detail, get_season
from Backend.helper.metadata import resolve_cover_url, COMBINED_SEASON, COMBINED_EPISODE_BASE
from Backend.helper.split_files import parse_combined_episodes, combined_name_key
from Backend.helper.settings_manager import SettingsManager
from Backend.helper.subtitles import get_subtitles_for, stremio_subtitle_entries
from Backend.logger import LOGGER
from Backend.pyrofork.bot import StreamBot, get_streambot_url

router = APIRouter(prefix="/stremio", tags=["Stremio Addon"])
templates = Jinja2Templates(directory="Backend/fastapi/templates")

#----- Addon configuration
ADDON_NAME = "Telegram"
ADDON_VERSION = __version__
PAGE_SIZE = 15


#----- Wrap a direct stream URL with the configured proxy (plain prepend or MediaFlow)
def build_proxy_url(original_url: str) -> str | None:
    settings = SettingsManager.current()
    base = settings.http_proxy_url
    if not base:
        return None
    if settings.mediaflow_proxy:
        url = f"{base.rstrip('/')}/proxy/stream?d={quote(original_url, safe='')}"
        if settings.mediaflow_password:
            url += f"&api_password={quote(settings.mediaflow_password, safe='')}"
        return url
    return f"{base}{original_url}"

_membership_cache: dict = {}
_MEMBERSHIP_TTL = 60
_MEMBERSHIP_CACHE_MAX = 5000


#----- Drop cached membership results for one user or all users
def invalidate_membership_cache(user_id: int | None = None) -> None:
    if user_id is None:
        _membership_cache.clear()
        return
    for key in [k for k in _membership_cache if k[1] == user_id]:
        _membership_cache.pop(key, None)


#----- Effective (mode, allowed_tokens) for a title, honouring per-item overrides
def _effective_visibility(catalog: dict, item: dict) -> tuple:
    if item.get("visibility") in ("public", "tokens", "owner"):
        return item["visibility"], (item.get("allowed_tokens") or [])
    return (catalog.get("visibility") or "public"), (catalog.get("allowed_tokens") or [])


#----- Whether a token may see content with the given visibility
def _token_can_view(mode: str, allowed_tokens: list, token_data: dict) -> bool:
    user_id = token_data.get("user_id")
    try:
        if user_id is not None and int(user_id) == int(Telegram.OWNER_ID):
            return True
    except (TypeError, ValueError):
        pass
    if mode == "owner":
        return False
    if mode == "tokens":
        return token_data.get("token") in (allowed_tokens or [])
    if SettingsManager.current().subscription:
        return not token_data.get("subscription_expired")
    return True


#----- Mongo filter that hides owner-only / restricted titles from a token
def _visibility_query(token_data: dict) -> dict:
    user_id = token_data.get("user_id")
    try:
        if user_id is not None and int(user_id) == int(Telegram.OWNER_ID):
            return {}
    except (TypeError, ValueError):
        pass
    return {"$or": [
        {"visibility": {"$exists": False}},
        {"visibility": "public"},
        {"visibility": "tokens", "allowed_tokens": token_data.get("token")},
    ]}


#----- Hide titles locked to a single catalog from default listings / search
def _not_exclusive_clause(allow_searchable: bool = False) -> dict:
    ors = [{"exclusive_catalog_id": {"$exists": False}}, {"exclusive_catalog_id": None}]
    if allow_searchable:
        ors.append({"exclusive_searchable": True})
    return {"$or": ors}


#----- Combine non-empty Mongo filters under a single $and
def _merge_filters(*filters) -> dict:
    parts = [f for f in filters if f]
    if not parts:
        return {}
    return parts[0] if len(parts) == 1 else {"$and": parts}


#----- Whether a title (by imdb id) may be seen by this token, honouring its own visibility
async def _title_allowed(imdb_id: str, token_data: dict) -> bool:
    doc = await db.get_media_details(imdb_id=imdb_id)
    if not doc:
        return True
    return _token_can_view(doc.get("visibility") or "public", doc.get("allowed_tokens") or [], token_data)


#----- Available catalog genres
GENRES = [
    "Action", "Adventure", "Animation", "Biography", "Comedy",
    "Crime", "Documentary", "Drama", "Family", "Fantasy",
    "History", "Horror", "Music", "Mystery", "Romance",
    "Sci-Fi", "Sport", "Thriller", "War", "Western"
]


#----- Turn a stored image reference into an absolute URL for Stremio clients.
def _abs_media_url(value: str) -> str:
    value = resolve_cover_url(value)
    idx = value.find("/thumb/")
    return f"{SettingsManager.current().base_url}{value[idx:]}" if idx != -1 else value


BETTERPOSTER_DEFAULT = "https://btttr.cc/poster/imdb/poster-default/{imdb_id}.jpg"
RPDB_FREE = "https://api.ratingposterdb.com/t0-free-rpdb/imdb/poster-default/{imdb_id}.jpg"


def _poster_url(imdb_id: str, fallback: str) -> str:
    settings = SettingsManager.current()
    if imdb_id:
        if settings.better_poster_enabled:
            template = settings.better_poster or BETTERPOSTER_DEFAULT
            return template.replace("{imdb_id}", str(imdb_id))
        if settings.rpdb_enabled:
            key = settings.rpdb_api_key
            template = (
                f"https://api.ratingposterdb.com/{key}/imdb/poster-default/{{imdb_id}}.jpg"
                if key else RPDB_FREE
            )
            return template.replace("{imdb_id}", str(imdb_id))
    return _abs_media_url(fallback)


async def _apply_fanart(meta: dict, item: dict) -> None:
    if not SettingsManager.current().fanart_enabled:
        return
    try:
        art = await fanart_artwork(item.get("imdb_id"), item.get("tmdb_id"), item.get("media_type"))
    except Exception as e:
        LOGGER.warning(f"[FANART] artwork lookup failed for {item.get('imdb_id')}: {e}")
        return
    if art.get("poster"):
        meta["poster"] = art["poster"]
    if art.get("logo"):
        meta["logo"] = art["logo"]
    if art.get("background"):
        meta["background"] = art["background"]


def convert_to_stremio_meta(item: dict) -> dict:
    media_type = "series" if item.get("media_type") == "tv" else "movie"

    meta = {
        "id": item.get('imdb_id'),
        "type": media_type,
        "name": item.get("title"),
        "poster": _poster_url(item.get("imdb_id"), item.get("poster")),
        "logo": item.get("logo") or "",
        "year": item.get("release_year"),
        "releaseInfo": str(item.get("release_year", "")),
        "imdb_id": item.get("imdb_id", ""),
        "moviedb_id": item.get("tmdb_id", ""),
        "background": _abs_media_url(item.get("backdrop")),
        "genres": item.get("genres") or [],
        "imdbRating": str(item.get("rating") or ""),
        "description": item.get("description") or "",
        "cast": item.get("cast") or [],
        "runtime": item.get("runtime") or "",
    }
    return meta


def format_released_date(media):
    year = media.get("release_year")
    if year:
        try:
            return datetime(int(year), 1, 1).isoformat() + "Z"
        except Exception:
            return None
    return None


def format_stream_details(filename: str, quality: str, size: str, is_split: bool = False) -> tuple[str, str]:
    size_emoji = "📦" if is_split else "💾"
    try:
        parsed = PTN.parse(filename)
    except Exception:
        return (f"Telegram {quality}", f"📁 {filename}\n{size_emoji} {size}")

    codec_parts = []
    if parsed.get("codec"):
        codec_parts.append(f"🎥 {parsed.get('codec')}")
    if parsed.get("bitDepth"):
        codec_parts.append(f"🌈 {parsed.get('bitDepth')}bit")
    if parsed.get("audio"):
        codec_parts.append(f"🔊 {parsed.get('audio')}")
    if parsed.get("encoder"):
        codec_parts.append(f"👤 {parsed.get('encoder')}")

    codec_info = " ".join(codec_parts) if codec_parts else ""

    resolution = parsed.get("resolution", quality)
    quality_type = parsed.get("quality", "")
    stream_name = f"Telegram {resolution} {quality_type}".strip()

    stream_title_parts = [
        f"📁 {filename}",
        f"{size_emoji} {size}",
    ]
    if codec_info:
        stream_title_parts.append(codec_info)

    stream_title = "\n".join(stream_title_parts)
    return (stream_name, stream_title)


def parse_size_to_bytes(size_str: str) -> int:
    if not size_str:
        return 0
    match = re.match(r"([\d.]+)\s*([A-Za-z]+)", size_str.strip())
    if not match:
        return 0
    value, unit = float(match.group(1)), match.group(2).upper()
    multipliers = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}
    return int(value * multipliers.get(unit, 1))


def get_resolution_priority(stream_name: str) -> int:
    resolution_map = {
        "2160p": 2160, "4k": 2160, "uhd": 2160,
        "1080p": 1080, "fhd": 1080,
        "720p": 720, "hd": 720,
        "480p": 480, "sd": 480,
        "360p": 360,
    }
    for res_key, res_value in resolution_map.items():
        if res_key in stream_name.lower():
            return res_value
    return 1


def get_language_priority(stream_title: str) -> int:
    title_lower = stream_title.lower()
    if re.search(r'\b(tamil|tam)\b', title_lower):
        return 3
    if re.search(r'\bmulti\b', title_lower):
        return 2
    return 1


@router.get("/{token}/manifest.json")
async def get_manifest(token: str, token_data: dict = Depends(verify_token)):
    user_name = "Unknown"
    is_premium = False

    try:
        user_id = token_data.get("user_id")
        if user_id:
            user = await db.get_user(int(user_id))
            if user:
                user_name = user.get("first_name") or user.get("username") or f"User {user_id}"
                is_premium = user.get("subscription_status") == "active"
    except Exception:
        pass

    subscription_badge = " 🌟" if is_premium else ""
    full_name = f"{ADDON_NAME}{subscription_badge} ({user_name})"

    manifest = {
        "id": "org.telegram.stremio",
        "version": ADDON_VERSION,
        "name": full_name,
        "description": "Stream movies and TV shows directly from your Telegram bot via Stremio.",
        "types": ["movie", "series"],
        "idPrefixes": ["tt", "tmdb"],
        "resources": ["catalog", "stream", "meta", "subtitles"],
        "catalogs": [],
        "behaviorHints": {
            "configurable": True,
            "configurationRequired": False
        }
    }

    vis_query = _visibility_query(token_data)
    custom_catalogs = await db.get_custom_catalogs(query=vis_query)

    for cat in custom_catalogs:
        cat_id = f"tg_custom_{cat['_id']}"
        cat_types = cat.get("media_types") or ["movie", "series"]
        if "tv" in cat_types and "series" not in cat_types:
            cat_types.append("series")
            cat_types.remove("tv")

        for mt in cat_types:
            if mt not in ("movie", "series"):
                continue
            manifest["catalogs"].append({
                "type": mt,
                "id": cat_id,
                "name": cat.get("name", "Custom Catalog"),
                "extra": [
                    {"name": "search", "isRequired": False},
                    {"name": "skip", "isRequired": False}
                ]
            })

    if SettingsManager.current().show_default_catalogs:
        manifest["catalogs"].extend([
            {
                "type": "movie",
                "id": "tg_movies",
                "name": f"{ADDON_NAME} Movies",
                "extra": [
                    {"name": "search", "isRequired": False},
                    {"name": "genre", "isRequired": False, "options": GENRES},
                    {"name": "skip", "isRequired": False}
                ]
            },
            {
                "type": "series",
                "id": "tg_series",
                "name": f"{ADDON_NAME} Series",
                "extra": [
                    {"name": "search", "isRequired": False},
                    {"name": "genre", "isRequired": False, "options": GENRES},
                    {"name": "skip", "isRequired": False}
                ]
            }
        ])

    return manifest


@router.get("/{token}/catalog/{media_type}/{catalog_id}.json")
@router.get("/{token}/catalog/{media_type}/{catalog_id}/{extra}.json")
async def get_catalog(
    token: str,
    media_type: str,
    catalog_id: str,
    extra: str = None,
    token_data: dict = Depends(verify_token)
):
    skip = 0
    search_query = ""
    genre_filter = ""

    if extra:
        try:
            extra = unquote(extra)
            parts = extra.split("&")
            for part in parts:
                if "=" in part:
                    k, v = part.split("=", 1)
                    if k == "skip":
                        skip = int(v)
                    elif k == "search":
                        search_query = v
                    elif k == "genre":
                        genre_filter = v
        except Exception:
            pass

    vis_query = _visibility_query(token_data)

    if catalog_id.startswith("tg_custom_"):
        real_id = catalog_id.replace("tg_custom_", "")
        catalog = await db.get_custom_catalog(real_id)
        if not catalog:
            return {"metas": []}

        cat_vis = catalog.get("visibility") or "public"
        if not _token_can_view(cat_vis, catalog.get("allowed_tokens") or [], token_data):
            return {"metas": []}

        item_refs = catalog.get("items", [])
        if not item_refs:
            return {"metas": []}

        id_map = {
            (ref["imdb_id"], ref.get("tmdb_id"), ref.get("media_type")): ref
            for ref in item_refs
        }
        db_type = "tv" if media_type == "series" else "movie"

        base_query = _merge_filters(
            {"media_type": db_type},
            vis_query,
            {"$or": [
                {"imdb_id": {"$in": [r["imdb_id"] for r in item_refs if r.get("imdb_id")]}},
                {"tmdb_id": {"$in": [r["tmdb_id"] for r in item_refs if r.get("tmdb_id")]}}
            ]}
        )

        if search_query:
            base_query["title"] = {"$regex": search_query, "$options": "i"}

        items = await db.get_media_list(query=base_query, limit=0)

        filtered = []
        for doc in items:
            key = (doc.get("imdb_id"), doc.get("tmdb_id"), doc.get("media_type"))
            ref = id_map.get(key)
            if not ref:
                continue
            eff_vis, eff_tokens = _effective_visibility(catalog, ref)
            if _token_can_view(eff_vis, eff_tokens, token_data):
                filtered.append(doc)

        order_map = {
            (ref["imdb_id"], ref.get("tmdb_id"), ref.get("media_type")): i
            for i, ref in enumerate(item_refs)
        }
        filtered.sort(key=lambda d: order_map.get(
            (d.get("imdb_id"), d.get("tmdb_id"), d.get("media_type")),
            999999
        ))

        page_items = filtered[skip: skip + PAGE_SIZE]

    elif catalog_id in ("tg_movies", "tg_series") and SettingsManager.current().show_default_catalogs:
        db_type = "tv" if media_type == "series" else "movie"
        allow_searchable = bool(search_query)

        query_filters = [
            {"media_type": db_type},
            vis_query,
            _not_exclusive_clause(allow_searchable)
        ]
        if search_query:
            query_filters.append({"title": {"$regex": search_query, "$options": "i"}})
        if genre_filter:
            query_filters.append({"genres": genre_filter})

        final_query = _merge_filters(*query_filters)
        page_items = await db.get_media_list(
            query=final_query,
            skip=skip,
            limit=PAGE_SIZE,
            sort=[("_id", -1)]
        )
    else:
        page_items = []

    metas = []
    for item in page_items:
        meta = convert_to_stremio_meta(item)
        await _apply_fanart(meta, item)
        metas.append(meta)

    return {"metas": metas}


@router.get("/{token}/meta/{media_type}/{id}.json")
async def get_meta(token: str, media_type: str, id: str, token_data: dict = Depends(verify_token)):
    if SettingsManager.current().hide_catalog:
        raise HTTPException(status_code=404, detail="Catalog disabled")

    # If it's a TMDB ID, local DB lookup might fail unless saved strictly. We bypass parsing for DB lookup.
    imdb_id = id

    media = await db.get_media_details(imdb_id=imdb_id)
    if not media:
        return {"meta": {}}

    if not _token_can_view(media.get("visibility") or "public", media.get("allowed_tokens") or [], token_data):
        return {"meta": {}}

    meta_obj = {
        "id": id,
        "type": "series" if media.get("media_type") == "tv" else "movie",
        "name": media.get("title", ""),
        "description": media.get("description", ""),
        "year": str(media.get("release_year", "")),
        "imdbRating": str(media.get("rating", "")),
        "genres": media.get("genres", []),
        "director": [],
        "cast": media.get("cast", []),
        "poster": _poster_url(media.get("imdb_id"), media.get("poster")),
        "background": _abs_media_url(media.get("backdrop")),
        "logo": media.get("logo") or "",
        "runtime": media.get("runtime") or "",
    }

    await _apply_fanart(meta_obj, media)

    released_date = format_released_date(media)
    if released_date:
        meta_obj["released"] = released_date

    if media_type == "series" and "seasons" in media:
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        videos = []
        for season in sorted(media.get("seasons", []), key=lambda s: s.get("season_number")):
            for episode in sorted(season.get("episodes", []), key=lambda e: e.get("episode_number")):
                episode_id = f"{id}:{season['season_number']}:{episode['episode_number']}"
                video = {
                    "id": episode_id,
                    "title": episode.get("episode_title") or f"Episode {episode['episode_number']}",
                    "season": season["season_number"],
                    "episode": episode["episode_number"],
                    "overview": episode.get("episode_overview", ""),
                    "released": episode.get("episode_released") or yesterday
                }
                if episode.get("episode_backdrop"):
                    video["thumbnail"] = _abs_media_url(episode["episode_backdrop"])
                videos.append(video)
        meta_obj["videos"] = videos

    return {"meta": meta_obj}


@router.get("/{token}/subtitles/{media_type}/{id}.json")
async def get_subtitles(token: str, media_type: str, id: str, extra: Optional[str] = None, token_data: dict = Depends(verify_token)):
    try:
        if id.startswith("tmdb:"):
            parts = id.split(":")
            imdb_id = f"tmdb:{parts[1]}"
            season = int(parts[2]) if len(parts) > 2 else None
            episode = int(parts[3]) if len(parts) > 3 else None
        else:
            parts = id.split(":")
            imdb_id = parts[0]
            season = int(parts[1]) if len(parts) > 1 else None
            episode = int(parts[2]) if len(parts) > 2 else None
    except (ValueError, IndexError):
        return {"subtitles": []}

    db_media_type = "tv" if media_type == "series" else "movie"
    subs = await get_subtitles_for(imdb_id, db_media_type, season, episode)
    if not subs:
        return {"subtitles": []}
    return {"subtitles": stremio_subtitle_entries(subs, token, SettingsManager.current().base_url)}


async def _global_streams_for(token: str, imdb_id: str, media_type: str, season_num: Optional[int], episode_num: Optional[int]) -> list:
    imdb_media_type = "tvSeries" if media_type == "series" else "movie"

    # Natively Handle TMDB IDs directly using our internal metadata cache instead of Cinemeta!
    if imdb_id.startswith("tmdb:"):
        tmdb_id_num = imdb_id.split(":")[1]
        from Backend.helper.metadata import _tmdb_details, _tmdb_title_year
        details = await _tmdb_details(media_type, tmdb_id_num)
        if not details:
            return []
        expected_title, year = _tmdb_title_year(details, media_type)
    else:
        detail = await get_detail(imdb_id=imdb_id, media_type=imdb_media_type)
        if not detail or not detail.get("title"):
            return []
        expected_title = detail["title"]
        year = (detail.get("releaseDetailed") or {}).get("year") or None

    LOGGER.info(f"imdb_id={imdb_id} media_type={media_type} imdb_media_type={imdb_media_type} -> {expected_title}")

    if season_num is not None and episode_num is not None:
        try:
            await get_season(imdb_id=imdb_id, season_id=season_num, episode_id=episode_num)
        except Exception:
            pass

    try:
        global_results = await global_search(
            expected_title,
            SettingsManager.current().auth_channels,
            year=year,
            season=season_num,
            episode=episode_num,
        )
    except Exception as e:
        LOGGER.error(f"[GLOBAL SEARCH] search failed for '{expected_title}': {e}")
        return []

    streams = []
    for r in global_results:
        _, stream_title = format_stream_details(r["title"], r["quality"], r["size"], is_split=False)
        stream_name = f"🌐 GLOBAL {r['quality']}"
        
        combined = parse_combined_episodes(r["title"])
        if combined:
            label = "Full" if combined.get("start") is None else f"E{combined['start']:02d}-E{combined['end']:02d}"
            if label.lower() not in stream_name.lower():
                stream_name = f"{stream_name} {label}"

        stream_title = f"{stream_title}\n📡 {r['source_chat']}"
        url = f"{SettingsManager.current().base_url}/dl/{token}/{r['token']}/{quote(r['title'])}"
        size_bytes = parse_size_to_bytes(r.get("size", ""))
        streams.append({"name": stream_name, "title": stream_title, "url": url, "size_bytes": size_bytes})
    return streams


async def _is_subscription_member(user_id: int) -> bool:
    group_id = SettingsManager.current().subscription_group_id
    if not group_id:
        return True

    cache_key = (group_id, user_id)
    cached = _membership_cache.get(cache_key)
    now_ts = time.monotonic()
    if cached and (now_ts - cached[0]) < _MEMBERSHIP_TTL:
        return cached[1]

    try:
        member = await StreamBot.get_chat_member(group_id, user_id)
        result = member.status not in (ChatMemberStatus.LEFT, ChatMemberStatus.BANNED)
    except UserNotParticipant:
        result = False
    except Exception as e:
        LOGGER.warning(f"[SUBSCRIPTION] Membership check failed for user {user_id}: {e}")
        return True

    if len(_membership_cache) >= _MEMBERSHIP_CACHE_MAX:
        for k in [k for k, v in _membership_cache.items() if (now_ts - v[0]) >= _MEMBERSHIP_TTL]:
            _membership_cache.pop(k, None)
        if len(_membership_cache) >= _MEMBERSHIP_CACHE_MAX:
            _membership_cache.clear()

    _membership_cache[cache_key] = (now_ts, result)
    return result


@router.get("/{token}/stream/{media_type}/{id}.json")
async def get_streams(
    token: str,
    media_type: str,
    id: str,
    token_data: dict = Depends(verify_token)
):

    if token_data.get("subscription_expired"):
        return {
            "streams": [
                {
                    "name": "🚫 Plan Expired",
                    "title": "Your plan is expired.\nRenew it from the bot to continue watching.",
                    "url": get_streambot_url()
                }
            ]
        }

    if (SettingsManager.current().subscription
            and not token_data.get("is_admin")
            and not token_data.get("subscription_exempt")
            and not token_data.get("expires_at")):
        user_id = token_data.get("user_id")
        if user_id and not await _is_subscription_member(int(user_id)):
            return {
                "streams": [
                    {
                        "name": "📢 Join Required",
                        "title": "First join the channel to stream it.\nThen wait for 2 min for verification",
                        "url": get_streambot_url()
                    }
                ]
            }

    if token_data.get("limit_exceeded"):
        limit_type = token_data["limit_exceeded"]

        title = (
            "🚫 Daily Limit Reached – Upgrade Required"
            if limit_type == "daily"
            else "🚫 Monthly Limit Reached – Upgrade Required"
        )

        return {
            "streams": [
                {
                    "name": "Limit Reached",
                    "title": title,
                    "url": f"tg://user?id={Telegram.OWNER_ID}"
                }
            ]
        }

    try:
        if id.startswith("tmdb:"):
            parts = id.split(":")
            imdb_id = f"tmdb:{parts[1]}"
            season_num = int(parts[2]) if len(parts) > 2 else None
            episode_num = int(parts[3]) if len(parts) > 3 else None
        else:
            parts = id.split(":")
            imdb_id = parts[0]
            season_num = int(parts[1]) if len(parts) > 1 else None
            episode_num = int(parts[2]) if len(parts) > 2 else None
    except (ValueError, IndexError):
        raise HTTPException(status_code=400, detail="Invalid Stremio ID format")

    if not await _title_allowed(imdb_id, token_data):
        return {"streams": []}

    media_details = await db.get_media_details(
        imdb_id=imdb_id,
        season_number=season_num,
        episode_number=episode_num
    )

    streams = []

    is_combined = season_num == COMBINED_SEASON and episode_num is not None and episode_num >= COMBINED_EPISODE_BASE

    if media_details and "telegram" in media_details:
        for quality in media_details.get("telegram", []):
            if quality.get("id"):
                filename = quality.get("name", "")
                quality_str = quality.get("quality", "HD")
                size = quality.get("size", "")
                size_bytes = parse_size_to_bytes(size)

                combined = parse_combined_episodes(filename) if is_combined else None
                episode_start = combined.get("start") or 0 if combined else 0
                name_key = combined_name_key(filename) if combined else ""

                stream_name, stream_title = format_stream_details(
                    filename, quality_str, size, is_split=bool(quality.get("group_key"))
                )

                if combined:
                    label = "Full" if combined.get("start") is None else f"E{combined['start']:02d}-E{combined['end']:02d}"
                    if label.lower() not in stream_name.lower():
                        stream_name = f"{stream_name} {label}"

                original_url = f"{SettingsManager.current().base_url}/dl/{token}/{quality.get('id')}/video.mkv"
                proxy_url = build_proxy_url(original_url)

                if SettingsManager.current().show_proxy_and_non_proxy_both and proxy_url:
                    streams.append({"name": f"{stream_name} (Proxy)", "title": stream_title, "url": proxy_url, "size_bytes": size_bytes, "episode_start": episode_start, "name_key": name_key})
                    streams.append({"name": f"{stream_name} (Direct)", "title": stream_title, "url": original_url, "size_bytes": size_bytes, "episode_start": episode_start, "name_key": name_key})
                elif proxy_url:
                    streams.append({"name": stream_name, "title": stream_title, "url": proxy_url, "size_bytes": size_bytes, "episode_start": episode_start, "name_key": name_key})
                else:
                    streams.append({"name": stream_name, "title": stream_title, "url": original_url, "size_bytes": size_bytes, "episode_start": episode_start, "name_key": name_key})
    elif is_global_search_enabled():
        try:
            streams.extend(
                await _global_streams_for(token, imdb_id, media_type, season_num, episode_num)
            )
        except Exception as e:
            LOGGER.error(f"[GLOBAL SEARCH] stream search failed for {imdb_id}: {e}")

    if not streams:
        return {"streams": []}

    if is_combined:
        streams.sort(key=lambda s: s.get("episode_start", 0))
        streams.sort(key=lambda s: s.get("name_key", ""))
        streams.sort(key=lambda s: get_resolution_priority(s.get("name", "")), reverse=True)
        streams.sort(key=lambda s: get_language_priority(s.get("title", "")), reverse=True)
    else:
        streams.sort(
            key=lambda s: (
                get_language_priority(s.get("title", "")),
                get_resolution_priority(s.get("name", "")),
                s.get("size_bytes", 0)
            ),
            reverse=True
        )
    name_count: dict = {}
    for s in streams:
        name_count[s["name"]] = name_count.get(s["name"], 0) + 1

    seen: dict = {}
    for s in streams:
        if name_count[s["name"]] > 1:
            seen[s["name"]] = seen.get(s["name"], 0) + 1
            s["name"] = f"{s['name']} ({seen[s['name']]})"
    return {"streams": streams}

@router.get("/{token}/configure")
async def configure_addon(token: str, request: Request):
    manifest_url = f"{SettingsManager.current().base_url}/stremio/{token}/manifest.json"
    web_install_url = f"https://web.stremio.com/#/?addon_manifest={quote(manifest_url, safe='')}"

    token_doc = await db.get_api_token(token)
    user_name = "Unknown"
    expiry_str = "N/A"
    status_color = "#ef4444"
    status_text = "Unknown"

    if token_doc:
        uid = token_doc.get("user_id")
        if uid:
            try:
                user = await db.get_user(int(uid))
                if user:
                    user_name = user.get("first_name") or user.get("username") or f"User {uid}"
                    expiry = user.get("subscription_expiry")
                    if expiry:
                        expiry_str = expiry.strftime("%d %b %Y").lstrip("0")
                    if user.get("subscription_status") == "active":
                        status_color, status_text = "#22c55e", "Active"
                    else:
                        status_color, status_text = "#ef4444", "Expired"
            except Exception:
                pass

    return templates.TemplateResponse("stremio_configure.html", {
        "request": request,
        "theme": get_theme(request.session.get("theme", DEFAULT_THEME)),
        "manifest_url": manifest_url,
        "web_install_url": web_install_url,
        "user_name": user_name,
        "expiry_str": expiry_str,
        "status_text": status_text,
        "status_color": status_color,
    })