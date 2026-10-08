"""Validate addon responses against what stremio-core actually deserializes.

Rules encoded here come from Stremio/stremio-core (development branch):
  src/types/addon/response.rs        -> ResourceResponse::deserialize
  src/types/resource/meta_item.rs    -> MetaItemPreviewLegacy / MetaItem / Video / SeriesInfo
  src/types/resource/stream.rs       -> Stream { #[serde(flatten)] source: StreamSource }
  src/models/common/resource_loadable.rs -> ResourceError::{EmptyContent, UnexpectedResponse}

Key facts:
  * `metas` uses VecSkipError  -> one bad catalog entry is silently DROPPED.
  * `meta`  has NO skip-error  -> one bad field kills the WHOLE meta response,
    which is what renders "There is no information available".
  * Stream's source is flattened, so a stream object must contain one of
    url / ytId / infoHash / externalUrl / rarUrls / ... at the TOP level.
"""
import re

VALID_TYPES = {"movie", "series", "channel", "tv"}
VOD_TYPES = {"movie", "series"}
STREAM_SOURCE_KEYS = {
    "url", "ytId", "infoHash", "externalUrl", "rarUrls", "zipUrls",
    "7zipUrls", "tgzUrls", "tarUrls", "nzbUrl", "playerFrameUrl",
}
YT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
URL_RE = re.compile(r"^https?://")


def _is_url(v):
    return isinstance(v, str) and bool(URL_RE.match(v))


def check_stream(s, where):
    errs = []
    if not isinstance(s, dict):
        return [f"{where}: stream is not an object"]
    if not (set(s) & STREAM_SOURCE_KEYS):
        errs.append(
            f"{where}: stream has NO source key (needs one of "
            f"{sorted(STREAM_SOURCE_KEYS)}); got keys {sorted(s)} -> "
            f"stremio-core rejects it and, inside meta.trailerStreams, "
            f"that kills the whole meta response")
    if "ytId" in s and not YT_ID_RE.match(str(s["ytId"])):
        errs.append(f"{where}: ytId {s['ytId']!r} is not an 11-char YouTube id")
    if "url" in s and not _is_url(s["url"]):
        errs.append(f"{where}: url {s['url']!r} is not an absolute http(s) URL")
    for k in ("name", "description", "title", "thumbnail"):
        if k in s and s[k] is not None and not isinstance(s[k], str):
            errs.append(f"{where}: {k} must be a string, got {type(s[k]).__name__}")
    bh = s.get("behaviorHints")
    if bh is not None:
        if not isinstance(bh, dict):
            errs.append(f"{where}: behaviorHints must be an object")
        elif "videoSize" in bh and not isinstance(bh["videoSize"], int):
            errs.append(f"{where}: behaviorHints.videoSize must be a number")
    return errs


def check_video(v, where):
    errs = []
    if not isinstance(v.get("id"), str) or not v.get("id"):
        errs.append(f"{where}: video.id is required and must be a non-empty string")
    if "title" in v and not isinstance(v["title"], str):
        errs.append(f"{where}: video.title must be a string")
    # series_info is `#[serde(default, flatten)] Option<SeriesInfo>` where
    # SeriesInfo { season: u32, episode: u32 } — BOTH required, unsigned.
    has_s, has_e = "season" in v, "episode" in v
    if has_s != has_e:
        errs.append(f"{where}: video has season={has_s} episode={has_e}; "
                    f"SeriesInfo needs BOTH or NEITHER")
    for k in ("season", "episode"):
        if k in v:
            val = v[k]
            if not isinstance(val, int) or isinstance(val, bool) or val < 0:
                errs.append(f"{where}: video.{k}={val!r} must be a non-negative "
                            f"integer (deserialized as u32)")
    if "thumbnail" in v and v["thumbnail"] is not None and not isinstance(v["thumbnail"], str):
        errs.append(f"{where}: video.thumbnail must be a string")
    return errs


def check_meta(body, requested_type=None, requested_id=None):
    """Validate a /meta/{type}/{id}.json response body."""
    errs = []
    if not isinstance(body, dict) or "meta" not in body:
        return ["response has no 'meta' key -> UnexpectedResponse"]
    m = body["meta"]
    if not m:
        return ["meta is EMPTY {} -> stremio-core cannot build a MetaItem "
                "(id/type/name are required) -> 'no information available'"]

    for k in ("id", "type", "name"):
        if not isinstance(m.get(k), str) or not str(m.get(k)).strip():
            errs.append(f"meta.{k} is REQUIRED and must be a non-empty string "
                        f"(got {m.get(k)!r})")
    if m.get("type") not in VALID_TYPES:
        errs.append(f"meta.type={m.get('type')!r} is not a Stremio content type")
    elif m.get("type") == "tv":
        errs.append("meta.type='tv' means LIVE TV to Stremio (EPG/channel); a "
                    "VOD series must be 'series'")
    if requested_type and m.get("type") in VOD_TYPES and m.get("type") != requested_type:
        errs.append(f"meta.type={m.get('type')!r} != requested type "
                    f"{requested_type!r} (catalog/meta type mismatch)")
    if requested_id and m.get("id") not in (None, requested_id):
        errs.append(f"meta.id={m.get('id')!r} != requested id {requested_id!r}")

    for k in ("poster", "background", "logo"):
        if m.get(k) is not None and m.get(k) != "" and not _is_url(m[k]):
            errs.append(f"meta.{k}={m[k]!r} must be an absolute http(s) URL "
                        f"(parsed with Url::parse; '' is accepted, junk is not)")
    for k in ("name", "description", "releaseInfo", "runtime", "imdbRating"):
        if k in m and m[k] is not None and not isinstance(m[k], str):
            errs.append(f"meta.{k} must be a string, got {type(m[k]).__name__}")
        if isinstance(m.get(k), str) and m[k] in ("None", "null", "None-"):
            errs.append(f"meta.{k}={m[k]!r} is a stringified None -> renders "
                        f"literally in the UI")
    if "moviedb_id" in m:
        v = m["moviedb_id"]
        if v is None:
            errs.append("meta.moviedb_id=None (null breaks some Stremio clients; "
                        "omit the key instead)")
        elif not isinstance(v, str):
            errs.append(f"meta.moviedb_id must be a string, got {type(v).__name__}")
    if "genres" in m and not isinstance(m["genres"], list):
        errs.append(f"meta.genres must be an array, got {type(m['genres']).__name__}")
    if "year" in m and m["year"] is not None and not isinstance(m["year"], (str, int)):
        errs.append(f"meta.year must be a string/number, got {type(m['year']).__name__}")

    for i, ts in enumerate(m.get("trailerStreams") or []):
        errs += check_stream(ts, f"meta.trailerStreams[{i}]")
    for i, t in enumerate(m.get("trailers") or []):
        if not isinstance(t, dict) or not t.get("source") or t.get("type") != "Trailer":
            errs.append(f"meta.trailers[{i}] needs {{'source': <yt id>, "
                        f"'type': 'Trailer'}} (both fields are required)")
    if m.get("type") == "series":
        vids = m.get("videos")
        if vids is not None:
            if not isinstance(vids, list):
                errs.append("meta.videos must be an array")
            else:
                if not vids:
                    errs.append("meta.videos is EMPTY for a series -> the details "
                                "page lists no episodes, nothing is playable")
                for i, v in enumerate(vids):
                    errs += check_video(v, f"meta.videos[{i}]")
    return errs


def check_catalog(body):
    """Validate a /catalog/{type}/{id}.json response body. Entries that fail
    are silently DROPPED by stremio-core (VecSkipError), so a title just
    vanishes from the row."""
    errs = []
    if not isinstance(body, dict) or "metas" not in body:
        return ["response has no 'metas' key -> UnexpectedResponse"]
    metas = body["metas"]
    if metas is None:
        return []
    seen = {}
    for i, m in enumerate(metas):
        w = f"metas[{i}]"
        if not isinstance(m, dict):
            errs.append(f"{w}: not an object")
            continue
        for k in ("id", "type", "name"):
            if not isinstance(m.get(k), str) or not str(m.get(k)).strip():
                errs.append(f"{w}: {k} is REQUIRED and must be a non-empty "
                            f"string (got {m.get(k)!r}) -> entry is DROPPED")
        if m.get("type") not in VALID_TYPES:
            errs.append(f"{w}: type={m.get('type')!r} is not a Stremio content "
                        f"type -> entry is DROPPED")
        elif m.get("type") == "tv":
            errs.append(f"{w}: type='tv' is LIVE TV to Stremio; a VOD series "
                        f"must be 'series'")
        for k in ("poster", "background", "logo"):
            if m.get(k) is not None and m.get(k) != "" and not _is_url(m[k]):
                errs.append(f"{w}: {k}={m[k]!r} is not an absolute http(s) URL "
                            f"-> entry is DROPPED")
        if isinstance(m.get("releaseInfo"), str) and m["releaseInfo"] in ("None", "null"):
            errs.append(f"{w}: releaseInfo={m['releaseInfo']!r} renders literally")
        key = (m.get("type"), m.get("id"))
        if key in seen:
            errs.append(f"{w}: DUPLICATE id {key} (also at metas[{seen[key]}]) — "
                        f"both entries address the same title, so clicking one "
                        f"can open the other")
        else:
            seen[key] = i
    return errs


def check_streams(body):
    errs = []
    if not isinstance(body, dict) or "streams" not in body:
        return ["response has no 'streams' key -> UnexpectedResponse"]
    for i, s in enumerate(body["streams"] or []):
        errs += check_stream(s, f"streams[{i}]")
    return errs
