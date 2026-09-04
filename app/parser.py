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

# Source-site tokens that appear inside release filenames and are never part of
# the real title. Matched case-insensitively as whole tokens.
_SITE_NAME_TOKENS = {
    "1tamilmv", "tamilmv", "tamilblasters", "tamilrockers", "tamilraja",
    "tamilarasan", "tamilyogi", "isaimini", "moviesda", "madrasrockers",
    "jiorockers", "1tamilrockers", "tamilgun", "torrent", "www",
}

# Tokens that mark the START of the "technical" tail (everything from the first
# such token onward is not part of the title).
_TAIL_TECH_RE = re.compile(
    r"^(?:\d{3,4}p|\d{1,2}k|uhd|web[-\s]?dl|webdl|web[-\s]?rip|webrip|hdrip|hd[-\s]?rip|"
    r"bluray|blu[-\s]?ray|brrip|bdrip|dvdrip|dvd[-\s]?rip|predvd|pre[-\s]?dvd|camrip|"
    r"hdcam|hdtc|hd[-\s]?tc|hdts|hd[-\s]?ts|hdtv|hevc|x264|x265|h\.?264|h\.?265|avc|aac|"
    r"ac3|dts|eac3|ddp?|dd|esub|esubs|subs|proper|repack|hdr|"
    r"tam(?:il)?|tel(?:ugu)?|hin(?:di)?|mal(?:ayalam)?|kan(?:nada)?|eng(?:lish)?|multi|"
    r"season|episode|s\d{1,2}e\d{1,3})$",
    re.IGNORECASE,
)

_SE_RE = re.compile(r"^s\d{1,3}(?:e\d{1,3})?$", re.IGNORECASE)


def _normalize_separators(text: str) -> str:
    """Turn dots/underscores/hyphens used as separators into spaces."""
    text = re.sub(r"[._]+", " ", str(text or ""))
    text = re.sub(r"\s+", " ", text)
    return text.strip(" .-_()[]{}")


def _looks_like_title_word(tok: str) -> bool:
    """A token that is clearly part of the movie title (not an uploader tag).

    Capitalized words (The Odyssey, Toxic, Spider) or longer lowercase words
    count; short all-lowercase tags (meme, ing) and lone initials do not.
    """
    if not tok or _SE_RE.match(tok):
        return False
    if tok[0].isupper():
        return True
    if len(tok) >= 4 and tok.isalpha():
        return True
    return False


def strip_site_and_uploader(title: str) -> str:
    """Remove source-site prefix and the ripper/uploader handle after it.

    Handles both dot- and underscore-separated forms, e.g.:
      'www 1TamilMV meme Arulvaan'        -> 'Arulvaan'
      'www 1TamilMV Pizza Photographer'   -> 'Photographer'
      'www 1TamilMV reisen The Odyssey'   -> 'The Odyssey'
      'www 1TamilMV Leo'                  -> 'Leo'  (no ripper handle; kept)
    """
    norm = _normalize_separators(title)
    if not norm:
        return ""
    toks = norm.split()

    while toks and toks[0].lower() in ("www", "http", "https"):
        toks.pop(0)

    site_idx = -1
    for i, t in enumerate(toks):
        if t.lower() in _SITE_NAME_TOKENS:
            site_idx = i
            break

    if site_idx >= 0:
        after = toks[site_idx + 1:]
        ripper = after[0] if after else ""
        rest = after[1:] if after else []
        if ripper and not _SE_RE.match(ripper):
            has_title_after = any(_looks_like_title_word(t) for t in rest)
            if has_title_after:
                toks = rest          # drop site + ripper handle
            else:
                toks = after         # only one token after site; keep it (title)
        else:
            toks = after
    else:
        toks = [t for t in toks if t.lower() not in _SITE_NAME_TOKENS]

    result = " ".join(toks).strip()
    # Unwrap a TLD/handle glued to the title with a hyphen: "lol-Vikram" -> "Vikram".
    result = re.sub(
        r"^(?:www[.\s-]*)?(?:com|net|org|lol|xyz|vip|cc|me|io|to|in|co|link|click)\s*-\s*",
        "",
        result,
        flags=re.I,
    )
    return result.strip()


def _cut_technical_tail(title: str) -> str:
    """Cut the title at the first technical tag (resolution/codec/language/etc)."""
    kept = []
    for t in title.split():
        bare = t.strip("()-[]{},+")
        if _TAIL_TECH_RE.match(bare):
            break
        kept.append(t)
    return " ".join(kept).strip(" .-_()[]{}")


def clean_movie_title(raw: str) -> str:
    """Best-effort clean movie title from a raw filename or a PTN 'title'.

    Idempotent and safe on PTN's already-parsed title: normalizes separators,
    strips source-site + uploader prefix, and drops the technical tail.
    """
    norm = _normalize_separators(raw)
    norm = strip_site_and_uploader(norm)
    ym = re.search(r"\b(19\d{2}|20\d{2})\b", norm)
    if ym:
        norm = norm[:ym.start()].strip(" .-_()[]{}")
    norm = _cut_technical_tail(norm)
    return norm.strip()


def extract_fallback_title_and_year(filename: str) -> tuple[Optional[str], Optional[int]]:
    clean = clean_filename(filename)
    clean = re.sub(r"\.(?:mkv|mp4|avi|mov|ts|m4v|flv|webm)$", "", clean, flags=re.I)
    clean = re.sub(r"^[\[\(\{][^\]\)\}]+[\]\)\}][\s._\-]*", " ", clean)
    # Normalize separators FIRST so site tokens (dot or underscore separated) are
    # matched; strip_site_and_uploader() removes "www <site> <ripper>" as tokens.
    clean = _normalize_separators(clean)

    year = None
    title_part = clean
    year_match = re.search(r"[\s._\-\(\[]+(19\d\d|20\d\d)[\s._\-\)\]]*", clean)
    if year_match:
        year = int(year_match.group(1))
        title_part = clean[:year_match.start()]

    title_part = strip_site_and_uploader(title_part)
    title_part = _cut_technical_tail(title_part)
    title_part = title_part.strip(" ._-()[]{}")
    if title_part:
        return title_part, year

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
