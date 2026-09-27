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
# Season / episode / combined-pack detection (S01E09, Ep 09, EP (01 - 04), …)
#
# Reality-TV / Indian-TVB naming is wildly inconsistent, so we parse it
# ourselves instead of trusting PTN (which, e.g., misses "Ep 09", returns the
# wrong season for "Bigg Boss Tamil 10 Ep 05 - 08", and keeps "Ep 09" glued to
# the title so TMDb matches the wrong show — the cause of the mixed-up
# Bigg Boss entries).
# ---------------------------------------------------------------------------

# "E", "EP", "Eps", "Episode", "Episodes" (case-insensitive). Word-start guard
# blocks mid-word hits (the E in "COMPLETE1080"), the digit guards block
# resolutions (Episode 720p ≠ episode 720).
_EP_TOKEN = r"E(?:P(?:ISOD(?:E|ES))?|PS)"

# S10E09 with optional range (S10E09-E12 or S10E09-12)
_SERIES_EP_RE = re.compile(
    r"\bS(?:EASON)?[\s._-]*0*(\d{1,3})[\s._-]*E(?:P)?[\s._-]*0*(\d{1,3})[\s._-]*?"
    r"(?:(?:-|–|~|to)+[\s._-]*(?:E(?:P)?[\s._-]*)?0*(\d{1,3})(?![\d]))?",
    re.IGNORECASE,
)
# EP 01 - 04 / Ep.09-12 / Episodes 1 to 4 / Eps.(01-04) / EP01-EP04
_EP_RANGE_RE = re.compile(
    rf"(?<![A-Za-z]){_EP_TOKEN}(?![A-Za-z])[\s._\-]*\(?\s*(?!0*\d{{1,4}}[pk](?:\b|[_.-]))0*(\d{{1,3}})\s*\)?[\s._\-]*"
    rf"(?:-|–|~|\+|&|to)+[\s._\-]*(?:{_EP_TOKEN}(?![A-Za-z])[\s._\-]*)?\(?\s*0*(\d{{1,3}})\s*\)?(?![\d])",
    re.IGNORECASE,
)
# EP.01.02.03.04 — dot-separated enumerated packs
_EP_ENUM_RE = re.compile(
    rf"(?<![A-Za-z]){_EP_TOKEN}\.((?:0*\d{{1,3}}\.){{1,11}}0*\d{{1,3}})(?!\d)",
    re.IGNORECASE,
)
# Ep 1, 2, 3 and 4 (3+ numbers; two-number ranges go to the range regex)
_EP_LIST_RE = re.compile(
    rf"(?<![A-Za-z]){_EP_TOKEN}(?![A-Za-z])[\s._\-]+0*(\d{{1,3}})((?:\s*(?:,|&|\+|and|to)\s*0*\d{{1,3}}){{2,}})(?![\d])",
    re.IGNORECASE,
)
# A lone "Ep 09" / "E09" / "Episode 9"
_EP_SINGLE_RE = re.compile(
    rf"(?<![A-Za-z]){_EP_TOKEN}(?![A-Za-z])[\s._\-]*\(?\s*(?!0*\d{{1,4}}[pk](?:\b|[_.-]))0*(\d{{1,3}})(?![\d])",
    re.IGNORECASE,
)
# Plain season tokens
_SEASON_RE = re.compile(r"\bS(?:EASON)?[\s._-]*0*(\d{1,3})\b", re.IGNORECASE)
# "… Tamil 10 Ep 09" — a bare number directly before the Ep token is the
# season (reality-show convention: "Bigg Boss Tamil 10", "Indian Idol 15"…).
_SEASON_BEFORE_EP_RE = re.compile(
    rf"(?:^|[\s._\-])(\d{{1,2}})[\s._\-]+(?:{_EP_TOKEN}(?![A-Za-z])[\s._\-]*0*\d{{1,3}})",
    re.IGNORECASE,
)
# "combined|complete|batch" keyword — a whole-season pack when no range exists
_COMBINED_KEYWORD_RE = re.compile(
    r"(?:\b|#)(?:combined|complete|full season|whole season|all episodes|batch)\b",
    re.IGNORECASE,
)
# dd-mm-yyyy / dd.mm.yy style shoot/air dates that must not look like episodes
_DATE_NOISE_RE = re.compile(r"\(?\b(?:\d{1,2}[.\-/]\d{1,2}[.\-/]\d{4}|\d{4}[.\-/]\d{1,2}[.\-/]\d{1,2})\b\)?")

_MAX_EPISODE = 400  # daily-soap seasons pass 99 (Bigg Boss ~120/season)


def _ep_ok(n: int) -> bool:
    return 1 <= n <= _MAX_EPISODE


def analyze_episodes(filename: str) -> Optional[dict]:
    """Extract (season, episode range) from a TV filename.

    Returns None when nothing episode-like is present, else::

        {"season": int|None, "start": int|None, "end": int|None}

    ``start is None`` with a season means a *whole-season pack* (keyword-only).
    ``start == end`` means a single episode; ``start < end`` a combined pack.
    """
    if not filename:
        return None
    # Multi-line captions: analyze the first line only (rest is promo spam).
    name = _DATE_NOISE_RE.sub(" ", str(filename).split("\n")[0])

    def _season() -> Optional[int]:
        m = _SEASON_RE.search(name)
        if m:
            sn = int(m.group(1))
            if sn <= 100:
                return sn
        m = _SEASON_BEFORE_EP_RE.search(name)
        if m:
            sn = int(m.group(1))
            if sn <= 100:
                return sn
        return None

    m = _EP_ENUM_RE.search(name)
    if m:
        nums = [int(x) for x in m.group(1).split(".") if x.isdigit()]
        nums = [n for n in nums if _ep_ok(n)]
        if len(nums) >= 2:
            return {"season": _season(), "start": min(nums), "end": max(nums)}
    m = _SERIES_EP_RE.search(name)
    if m:
        sn, a = int(m.group(1)), int(m.group(2))
        b = int(m.group(3)) if m.group(3) else a
        if _ep_ok(a) and _ep_ok(b) and a <= b:
            return {"season": sn, "start": a, "end": b}
    m = _EP_RANGE_RE.search(name)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        if _ep_ok(a) and _ep_ok(b) and a <= b:
            return {"season": _season(), "start": a, "end": b}
    m = _EP_LIST_RE.search(name)
    if m:
        nums = [int(m.group(1))]
        nums += [int(x) for x in re.findall(r"0*(\d{1,3})", m.group(2))]
        nums = [n for n in nums if _ep_ok(n)]
        if len(nums) >= 3:
            return {"season": _season(), "start": min(nums), "end": max(nums)}
    m = _EP_SINGLE_RE.search(name)
    if m:
        a = int(m.group(1))
        if _ep_ok(a):
            return {"season": _season(), "start": a, "end": a}
    # Keyword-only pack ("Season 2 Complete", "combined batch") — season needed.
    if _COMBINED_KEYWORD_RE.search(name):
        sn = _season()
        if sn is not None:
            return {"season": sn, "start": None, "end": None}
    return None


def parse_combined_episodes(filename: str) -> Optional[dict]:
    """Back-compat wrapper around analyze_episodes (only range/pack hits)."""
    info = analyze_episodes(filename)
    if not info:
        return None
    start, end = info.get("start"), info.get("end")
    if start is None:  # keyword-only season pack
        if _COMBINED_KEYWORD_RE.search(str(filename).split("\n")[0]):
            return {"season": info["season"], "start": None, "end": None}
        return None
    if end is not None and end > start:  # genuine combined range
        return {"season": info["season"] or 1, "start": start, "end": end}
    return None


def is_series_filename(filename: str) -> bool:
    """True when a filename carries any S/E structure (incl. single episodes)."""
    return analyze_episodes(filename) is not None


# --- Series title extraction for TMDb matching ------------------------------
# Strips every S/E token, bracketed tags and site tokens, then cuts the
# technical tail — but NEVER at language words, because for Indian TV the
# language is part of the show name ("Bigg Boss Tamil" ≠ Hindi "Bigg Boss").
_TITLE_SE_STRIP_RES = (
    _SERIES_EP_RE,
    _EP_RANGE_RE,
    _EP_ENUM_RE,
    _EP_LIST_RE,
    _EP_SINGLE_RE,
    re.compile(r"\bS(?:EASON)?[\s._-]*0*\d{1,3}\b", re.IGNORECASE),
    re.compile(r"\bSEASONS?\s*\d{1,3}\b", re.IGNORECASE),
)
_TITLE_NOISE_TOKENS = {
    "mkv", "mp4", "avi", "mov", "m4v", "flv", "webm", "wmv", "ts",
    "www", "http", "https", "pack", "parts", "part", "parts",
    "complete", "completed", "combined", "batch", "unofficial", "collection",
}
_SERIES_TAIL_RE = re.compile(
    r"^(?:\d{3,4}p|\d{1,2}k|uhd|4k|8k|web[-\s]?dl|webdl|web[-\s]?rip|webrip|hdrip|hd[-\s]?rip|"
    r"bluray|blu[-\s]?ray|brrip|bdrip|dvdrip|dvd[-\s]?rip|predvd|pre[-\s]?dvd|camrip|hdcam|telecine|"
    r"hdtc|hd[-\s]?tc|hdts|hd[-\s]?ts|hdtv|hevc|x264|x265|h\.?264|h\.?265|avc|aac|ac3|dts(?:-hd)?|eac3|"
    r"dd5\.1|ddp5\.1|5\.1|2\.0|7\.1|dd|ddp|esub|esubs|subs|proper|repack|hdr|sdr|10-bit|8-bit|"
    r"amzn|hotstar|disney|netflix|hulu|seq|eps?|episod(?:e|es)?|seasons?)$",
    re.IGNORECASE,
)


def _title_variants(title: str) -> list[str]:
    """'Bigg Boss Tamil 10' -> ['Bigg Boss Tamil 10', 'Bigg Boss Tamil']."""
    out = [title]
    cur = title
    while True:
        nxt = re.sub(r"[\s._-]+\d{1,2}$", "", cur).strip()
        if not nxt or nxt == cur or len(nxt) < 3:
            break
        out.append(nxt)
        cur = nxt
    return out


# Daily-show and packer noise that must never leak into a TMDb query:
# "EP01 DAY 00", "19TH DAY", "2.5GB", "1GB" — leaving these in made Bigg Boss
# files search "BIGG BOSS Tamil DAY 18" and match a different show.
_DAY_NOISE_RE = re.compile(r"\b(?:DAY\b[\s._-]*\(?\s*\d{1,4}|\d{1,4}(?:ST|ND|RD|TH)?[\s._-]*DAY)\b\s*\)?", re.IGNORECASE)
_SIZE_NOISE_RE = re.compile(r"\b\d+(?:[.,]\d+)?\s?(?:GB|MB)\b", re.IGNORECASE)


def series_title_candidates(filename: str) -> list[str]:
    """Likely TMDb series-search titles for a filename, best-first."""
    name = clean_filename(str(filename or "").split("\n")[0])
    name = _SIZE_NOISE_RE.sub(" ", name)
    name = _DAY_NOISE_RE.sub(" ", name)
    name = _normalize_separators(name)
    name = _DATE_NOISE_RE.sub(" ", name)
    name = re.sub(r"\(?\s*(?:19|20)\d{2}\s*\)?", " ", name)   # (2024) / 2024
    name = re.sub(r"\[[^\]]*\d[^\]]*\]", " ", name)            # [1080p] [Group]
    for rx in _TITLE_SE_STRIP_RES:
        name = rx.sub(" ", name)
    toks: list[str] = []
    for t in name.split():
        bare = t.strip("()-[]{},+.")
        if not bare:
            continue
        if bare.lower() in _TITLE_NOISE_TOKENS or bare.lower() in _SITE_NAME_TOKENS:
            continue
        if _SERIES_TAIL_RE.match(bare):
            break  # technical tail starts here — the rest is never the title
        toks.append(bare)
    out: list[str] = []
    for title in _title_variants(re.sub(r"\s+", " ", " ".join(toks)).strip(" ._-()[]{}")):
        if len(title) >= 3 and title not in out:
            out.append(title)
    return out

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
# Audio profile (language chips for stream tiles — filename-derived, honest)
# ---------------------------------------------------------------------------

_AUDIO_LANG_LABELS = [
    ("tamil", "TAMIL"), ("telugu", "TELUGU"), ("malayalam", "MALAYALAM"),
    ("kannada", "KANNADA"), ("hindi", "HINDI"), ("english", "ENGLISH"),
]
_MULTI_AUDIO_RE = re.compile(
    r"\b(?:dual[-\s]?audio|multi[-\s]?(?:audio|lang|language)|triple[-\s]?audio|\d+[-\s]?audio)\b",
    re.IGNORECASE,
)
_TAMIL_DUB_RE = re.compile(r"\btamil[\s._-]*dubb(?:ed|ing)?\b|\btamil[\s._-]*dub\b", re.IGNORECASE)
_EN_SUBS_RE = re.compile(r"\b(?:esubs?|english[\s._-]?subs?|eng[\s._-]?subs?)\b", re.IGNORECASE)

_LANG_MARKS = {"TAMIL": "🟢", "TELUGU": "🟠", "MALAYALAM": "🔹",
               "KANNADA": "🟣", "HINDI": "🔵", "ENGLISH": "⚪"}


def audio_profile_from_filename(filename: str) -> dict:
    """Language/audio facts proven by a filename (never guesses).

    {"langs": [..], "multi": bool, "dub": bool, "subs_en": bool}
    """
    name = str(filename or "")
    # "English Subs"/"ESub" proves SUBTITLES, not an audio track — mask those
    # phrases before counting audio languages (subs_en still reads the raw name).
    audio_text = re.sub(_EN_SUBS_RE, " ", name)
    audio_text = re.sub(r"\b(?:tamil|telugu|hindi|malayalam|kannada)[\s._-]*(?:sub)?titles?\b", " ", audio_text, flags=re.IGNORECASE)
    found = []
    for word, label in _AUDIO_LANG_LABELS:
        if re.search(rf"(?<![a-zA-Z]){word}(?![a-zA-Z])", audio_text, re.IGNORECASE) \
                and label not in found:
            found.append(label)
    multi = len(found) >= 2 or bool(_MULTI_AUDIO_RE.search(name))
    return {"langs": found, "multi": multi,
            "dub": bool(_TAMIL_DUB_RE.search(name)),
            "subs_en": bool(_EN_SUBS_RE.search(name))}


def language_chip(prof: dict) -> str:
    """One honest audio chip for the stream-tile name ('' when unknown)."""
    if not prof:
        return ""
    if prof.get("dub") and len(prof.get("langs") or []) <= 1:
        return "🟣 TAMIL DUB"
    langs = prof.get("langs") or []
    if prof.get("multi"):
        return f"🟡 MULTI AUDIO ({len(langs)})" if len(langs) >= 2 else "🟡 MULTI AUDIO"
    if len(langs) == 1:
        return f"{_LANG_MARKS[langs[0]]} {langs[0]}"
    return ""


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


def series_title_match(cand: str, res_title: str) -> bool:
    """Is this TMDb show title a SAFE match for the search candidate?

    Accepts only when every token of the show name appears in the candidate
    and the candidate's leftovers are pure numbers (season markers). That
    accepts "Bigg Boss Tamil" for "BIGG BOSS Tamil 10" but rejects the Hindi
    "Bigg Boss" show and any fuzzy "Day Break"-style noise hit.
    """
    a = set(re.findall(r"[a-z0-9]+", str(cand or "").casefold()))
    b = set(re.findall(r"[a-z0-9]+", str(res_title or "").casefold()))
    if not a or not b or not (b <= a):
        return False
    _LANG_OK = {"hindi", "english", "tel", "telugu", "mal", "malayalam",
                "kan", "kannada", "dubbed", "multi", "subbed", "hd", "sd"}
    return all(tok.isdigit() or tok in _LANG_OK for tok in a - b)


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
