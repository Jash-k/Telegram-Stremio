"""Filename parsing + GlobalDB query/field helpers.

Mirrors the schema used by the existing `dbFyvio` GlobalDB so existing data
keeps working unchanged.
"""
import re
from typing import Optional

# ---------------------------------------------------------------------------
# File key / scalar normalization
# ---------------------------------------------------------------------------


def global_file_key(chat_id, message_id) -> str:
    return f"{int(chat_id)}_{int(message_id)}"


def first_int(value) -> Optional[int]:
    """Normalize PTN scalar/list values into a single integer."""
    if isinstance(value, (list, tuple, set)):
        for item in value:
            n = first_int(item)
            if n is not None:
                return n
        return None
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def episode_bounds(value) -> tuple[Optional[int], Optional[int]]:
    """Return inclusive (low, high) episode bounds from a scalar or PTN list."""
    if isinstance(value, (list, tuple, set)):
        numbers = [n for item in value if (n := first_int(item)) is not None]
        return (min(numbers), max(numbers)) if numbers else (None, None)
    number = first_int(value)
    return number, number


def normalize_global_file_fields(file_doc: dict, indexed_at=None) -> dict:
    """Normalize legacy scalar/list file coordinates (used by migration)."""
    start_low, start_high = episode_bounds(file_doc.get("episode_start"))
    end_low, end_high = episode_bounds(file_doc.get("episode_end"))
    bounds = [v for v in (start_low, start_high, end_low, end_high) if v is not None]
    return {
        "chat_id": first_int(file_doc.get("chat_id")),
        "message_id": first_int(file_doc.get("message_id")),
        "season": first_int(file_doc.get("season")),
        "episode_start": min(bounds) if bounds else None,
        "episode_end": max(bounds) if bounds else None,
        "indexed_at": file_doc.get("indexed_at") or indexed_at,
    }


# ---------------------------------------------------------------------------
# Combined-episode detection (S01 E01-E05 style packs)
# ---------------------------------------------------------------------------

_COMBINED_EPISODES_RE = re.compile(
    r"E(?:P|PISODE)?[\s._\-\(\[\{]*0*(\d{1,4})[\s._\-\(\)\]\}]*(?:-|–|~|\+|&|,|to)+"
    r"[\s._\-\(\)\]\}]*(?:E(?:P|PISODE)?[\s._\-\(\)\]\}]*)?0*(\d{1,4})[\s._\-\}\]\)]*(?=\D|$)",
    re.IGNORECASE,
)
_COMBINED_SEASON_RE = re.compile(r"S(?:EASON)?[\s._-]*0*(\d{1,3})", re.IGNORECASE)
_COMBINED_KEYWORD_RE = re.compile(r"(?:\b|#)(?:combined|complete|batch)\b", re.IGNORECASE)


def _combined_season(name: str) -> Optional[int]:
    m = _COMBINED_SEASON_RE.search(name)
    return int(m.group(1)) if m else None


def parse_combined_episodes(filename: str) -> Optional[dict]:
    if not filename:
        return None
    m = _COMBINED_EPISODES_RE.search(filename)
    if m:
        start, end = int(m.group(1)), int(m.group(2))
        if 1 <= start < end <= 99:
            return {"season": _combined_season(filename) or 1, "start": start, "end": end}
    if _COMBINED_KEYWORD_RE.search(filename):
        season = _combined_season(filename)
        if season is not None:
            return {"season": season, "start": None, "end": None}
    return None


# ---------------------------------------------------------------------------
# Filename cleaning
# ---------------------------------------------------------------------------

_EMOJI_RE = re.compile(
    "["
    "\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF"
    "\U0001F700-\U0001FAFF\U00002702-\U000027B0\U000024C2-\U0001F251"
    "\u2600-\u26FF\u2700-\u27BF\uFE00-\uFE0F\U0001F1E0-\U0001F1FF"
    "]+",
    re.UNICODE,
)
_TAG_RE = re.compile(r"@[A-Za-z0-9_.]+")


def clean_filename(name: str) -> str:
    if not name:
        return ""
    # Strip markdown [text](url) -> text
    name = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", name)
    name = re.sub(r"https?://[^\s\)]+", "", name)
    name = _EMOJI_RE.sub(" ", name)
    name = _TAG_RE.sub(" ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


# ---------------------------------------------------------------------------
# Language detection (used by catalog filters)
# ---------------------------------------------------------------------------

_LANG_MAP = {
    "tam": "Tamil", "tamil": "Tamil",
    "tel": "Telugu", "telugu": "Telugu",
    "hin": "Hindi", "hindi": "Hindi",
    "mal": "Malayalam", "malayalam": "Malayalam",
    "kan": "Kannada", "kannada": "Kannada",
    "eng": "English", "english": "English",
    "multi": "Multi",
}


def languages_from_filename(filename: str) -> list[str]:
    value = str(filename or "").lower()
    return sorted(
        {label for token, label in _LANG_MAP.items() if re.search(rf"\b{token}\b", value)}
    )


# ---------------------------------------------------------------------------
# Catalog assignment
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Release-source classification (PreDVD theatrical vs official digital)
# ---------------------------------------------------------------------------
#
# Used by cleanup to remove a title's theatrical/cam rips once a genuine
# digital print exists — even when the predvd is mis-labeled "1080p".
# Kept here next to the other filename helpers so the indexer and cleanup
# share one definition.

_PREDVD_SOURCE_RE = re.compile(
    r"\b(pre[-\s]?dvd|predvd|camrip|cam-rip|hdcam|\bcam\b|dvdscr|dvd-scr|\bscr\b|"
    r"hdtc|hd-tc|hdts|hd-ts|hq-ts|telesync|\bts\b|theatrical|theater-?print|cinema-?print)\b",
    re.IGNORECASE,
)
_DIGITAL_SOURCE_RE = re.compile(
    r"\b(web[-\s]?dl|webdl|web[-\s]?hd|webrip|web-rip|bluray|blu-ray|bdrip|brrip|"
    r"hdrip|hd-rip|dvdrip|dvd-rip|hddvd|\buhd\b|2160p|remux)\b",
    re.IGNORECASE,
)


def source_from_filename(filename: str) -> str:
    """Classify a file's release source: 'predvd', 'digital' or 'unknown'.

    A predvd/cam tag always wins (so a file that mentions both is treated as
    theatrical and removed once a clean digital copy exists).
    """
    low = str(filename or "").lower()
    if _PREDVD_SOURCE_RE.search(low):
        return "predvd"
    if _DIGITAL_SOURCE_RE.search(low):
        return "digital"
    return "unknown"


def is_predvd_filename(filename: str) -> bool:
    return source_from_filename(filename) == "predvd"


def is_digital_filename(filename: str) -> bool:
    return source_from_filename(filename) == "digital"


def determine_catalog(details: dict, media_type: str, filename: str) -> str:
    original_lang = details.get("original_language", "") or ""
    genres = [g.get("name", "") for g in (details.get("genres") or [])]
    is_anime = "Animation" in genres or original_lang == "ja" or "anime" in filename.lower()
    is_tamil = original_lang == "ta"
    is_dubbed = not is_tamil and re.search(r"\b(tam|tamil|multi)\b", filename.lower())
    if is_anime:
        return "anime_movies" if media_type == "movie" else "anime_series"
    if is_tamil:
        return "tamil_movies" if media_type == "movie" else "tamil_series"
    if is_dubbed:
        return "dubbed_movies" if media_type == "movie" else "dubbed_series"
    return "other_movies" if media_type == "movie" else "other_series"


# ---------------------------------------------------------------------------
# Fallback title & year extraction (handles short/acronym titles like DC, LEO)
# ---------------------------------------------------------------------------

_GENERIC_TITLES = {"tamil", "telugu", "hindi", "malayalam", "kannada", "english", "multi", "director's cut", "directors cut", "extended", "remastered", "unrated"}

def extract_fallback_title_and_year(filename: str) -> tuple[Optional[str], Optional[int]]:
    clean = clean_filename(filename)
    clean = re.sub(r"\.(?:mkv|mp4|avi|mov|ts|m4v|flv|webm)$", "", clean, flags=re.I)
    clean = re.sub(r"^(?:www\.)?[a-zA-Z0-9_\-\.]+\.(?:meme|pm|cz|wf|vip|yt|to|org|com|net|in|is|info|pizza|report)[\s._\-]*", "", clean, flags=re.I)
    clean = re.sub(r"^[\[\(\{][^\]\)\}]+[\]\)\}][\s._\-]*", "", clean)
    
    # Check for "Title (YYYY)" or "Title.YYYY" or "Title - YYYY" pattern
    year_match = re.search(r"[\s._\-\(\[]+(19\d\d|20\d\d)[\s._\-\)\]]*", clean)
    if year_match:
        year = int(year_match.group(1))
        title_part = clean[:year_match.start()].strip(" ._-()[]{}")
        if title_part:
            return title_part, year
            
    # Pattern 2: Title before quality/codec tags (1080p, 720p, WEBRip, HDRip, etc.)
    split_match = re.search(r"[\s._\-\(\[]+(?:1080p|720p|2160p|4k|web-?rip|web-?dl|hdrip|bluray|dvd|x264|x265|hevc|h264|tamil|telugu|hindi|malayalam|kannada)[\s._\-\)\]]*", clean, re.I)
    if split_match:
        title_part = clean[:split_match.start()].strip(" ._-()[]{}")
        if title_part:
            return title_part, None
            
    return None, None

# ---------------------------------------------------------------------------
# GlobalDB queries
# ---------------------------------------------------------------------------


def build_global_file_query(meta_id: str, season=None, episode=None) -> dict:
    query: dict = {"meta_id": meta_id}
    season = first_int(season)
    episode = first_int(episode)
    if season is not None:
        query["season"] = season
    if season is not None and episode is not None:
        query["$or"] = [
            {"episode_start": {"$lte": episode}, "episode_end": {"$gte": episode}},
            {"episode_start": None, "episode_end": None},
        ]
    return query


async def resolve_global_meta(coll, content_id) -> Optional[dict]:
    """Resolve a meta doc from its canonical id, TMDb id, IMDb id, or alias."""
    content_id = str(content_id or "").strip()
    if not content_id:
        return None
    meta = await coll.find_one({"_id": content_id})
    if meta:
        return meta
    meta = await coll.find_one({"$or": [{"imdb_id": content_id}, {"aliases": content_id}]})
    return meta
