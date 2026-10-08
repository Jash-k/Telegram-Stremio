"""Read-only audit of the GlobalDB against what Stremio will actually accept.

Every check here maps to a concrete way the Stremio UI breaks:

  * a `meta` response stremio-core cannot deserialize renders
    "There is no information available" (the `meta` key has NO skip-error,
    unlike `metas`, so one bad field kills the whole title);
  * a `metas` entry it cannot deserialize is silently DROPPED from the row;
  * a shared IMDb id makes two different titles answer to the same id, so a
    click can open the wrong one.

Nothing in this module writes. Run it after a migration/self-heal to see how
much of the library is affected, and which titles need a Remap.
"""
import re

from app import db
from app import parser as P

# A bare YouTube id, exactly what TMDb puts in videos.results[].key
_YT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
# …or a URL we can pull an 11-char id out of
_YT_URL_RE = re.compile(r"(?:v=|youtu\.be/|shorts/|embed/)([A-Za-z0-9_-]{11})")

_CATALOG_TYPE = {"movie": {"tamil_movies", "dubbed_movies", "anime_movies",
                           "other_movies", "video_songs"},
                 "series": {"tamil_series", "dubbed_series", "anime_series",
                            "other_series"}}

_SCAN_BATCH = 500
_EXAMPLES = 5


def _examples(bucket: list, doc, fmt) -> None:
    if len(bucket) < _EXAMPLES:
        try:
            bucket.append(fmt(doc))
        except Exception:
            bucket.append(str(doc.get("_id")))


def _usable_trailer(value) -> bool:
    """Would this trailer_yt survive the trip into meta.trailerStreams?"""
    key = str(value or "").strip()
    if not key:
        return True                      # absent is fine — no trailerStreams
    if _YT_ID_RE.match(key):
        return True
    if key.startswith(("http://", "https://")) and _YT_URL_RE.search(key):
        return True
    return False


async def run_audit(limit_examples: int = _EXAMPLES) -> dict:
    """Scan `meta` + `files` and report every Stremio-visible defect."""
    global _EXAMPLES
    _EXAMPLES = max(1, int(limit_examples))

    report = {
        "totals": {},
        "issues": {},
        "ok": True,
    }

    def issue(key: str, label: str, impact: str, fix: str):
        report["issues"].setdefault(key, {
            "label": label, "impact": impact, "fix": fix,
            "count": 0, "examples": [],
        })
        return report["issues"][key]

    # ------------------------------------------------------------------
    # Aggregations first (bounded memory, one round trip each)
    # ------------------------------------------------------------------
    meta_total = await db.col("meta").count_documents({})
    files_total = await db.col("files").count_documents({})
    report["totals"] = {"meta": meta_total, "files": files_total}

    ids_with_files = set()
    file_counts_by_meta = {}
    async for row in db.col("files").aggregate([
        {"$group": {"_id": "$meta_id", "n": {"$sum": 1}}},
    ]):
        if row["_id"] is not None:
            ids_with_files.add(row["_id"])
            file_counts_by_meta[row["_id"]] = row["n"]

    shared_imdb = {}
    async for row in db.col("meta").aggregate([
        {"$match": {"imdb_id": {"$type": "string", "$ne": ""}}},
        {"$group": {"_id": "$imdb_id", "n": {"$sum": 1},
                    "ids": {"$push": "$_id"}, "titles": {"$push": "$title"}}},
        {"$match": {"n": {"$gt": 1}}},
    ]):
        shared_imdb[row["_id"]] = row

    referenced_meta_ids = set()
    async for row in db.col("files").aggregate([
        {"$group": {"_id": "$meta_id"}},
    ]):
        if row["_id"] is not None:
            referenced_meta_ids.add(row["_id"])
    existing_meta_ids = set()
    async for row in db.col("meta").find({}, {"_id": 1}):
        existing_meta_ids.add(row["_id"])

    # Series that have at least one file WITH episode numbers — one pass, so
    # the meta scan below never issues a per-title query.
    metas_with_episodes = set()
    async for row in db.col("files").aggregate([
        {"$match": {"episode_start": {"$ne": None}}},
        {"$group": {"_id": "$meta_id"}},
    ]):
        if row["_id"] is not None:
            metas_with_episodes.add(row["_id"])

    # ------------------------------------------------------------------
    # Issue buckets
    # ------------------------------------------------------------------
    i_trailer = issue(
        "bad_trailer_key",
        "trailer_yt is not a usable YouTube id",
        "CRITICAL — meta.trailerStreams cannot be parsed, so stremio-core "
        "rejects the ENTIRE meta response. The title opens as "
        "\"There is no information available\".",
        "Code fix (already applied in app/routes/stremio.py): emit "
        "{\"ytId\": ...} and drop malformed keys. Optionally clear the field: "
        "db.meta.updateMany({trailer_yt: {...}}, {$unset:{trailer_yt:1}})",
    )
    i_type = issue(
        "bad_media_type",
        "media_type is not a valid Stremio content type (e.g. \"tv\")",
        "CRITICAL — Stremio reads type \"tv\" as a LIVE channel, so a VOD "
        "series renders as an empty channel page.",
        "Code fix normalizes on read. To fix the data: "
        "db.meta.updateMany({media_type:\"tv\"},{$set:{media_type:\"series\"}})",
    )
    i_title = issue(
        "missing_title",
        "meta has no usable title",
        "HIGH — `name` is a required field; the catalog entry is dropped and "
        "the details page shows a blank name.",
        "Re-run the panel's Self-Heal (backfill) or Remap the title to its "
        "TMDb id.",
    )
    i_year = issue(
        "missing_year",
        "meta has no usable year",
        "LOW — used to render the literal string \"None\" as the release "
        "badge; now omitted.",
        "Self-Heal backfills year/rating from TMDb.",
    )
    i_moviedb = issue(
        "missing_tmdb_id",
        "meta has no tmdb_id",
        "LOW — moviedb_id was emitted as null, which breaks some Stremio "
        "clients; now omitted.",
        "Usually a legacy doc — Remap it to a TMDb id.",
    )
    i_shared = issue(
        "shared_imdb_id",
        "one IMDb id is used by several meta docs",
        "HIGH — the catalog used to emit the same id twice; both entries "
        "resolve to whichever doc Mongo returns first, so a click can open a "
        "different title.",
        "Code fix now emits the unique canonical tmdb:<id> for these. Check "
        "whether one of the duplicates is a wrong Remap and delete it.",
    )
    i_orphan_meta = issue(
        "orphan_meta",
        "meta doc with no files",
        "NONE for Stremio (the catalog hides them) — just dead weight.",
        "Optional cleanup.",
    )
    i_orphan_files = issue(
        "orphan_files",
        "files whose meta doc no longer exists",
        "MEDIUM — these files are unplayable and never appear in a catalog.",
        "Run the panel's Self-Heal / re-index so they are re-linked.",
    )
    i_catalog_type = issue(
        "catalog_type_mismatch",
        "meta sits in a catalog whose declared type does not match the title",
        "MEDIUM — a series listed in a movie catalog (or vice versa) makes "
        "Stremio request meta with the wrong type.",
        "Re-run Self-Heal, or Remap the title with the correct media_type.",
    )
    i_pack_only = issue(
        "series_pack_only",
        "series whose files carry no episode numbers",
        "MEDIUM — used to produce meta.videos = [] (an episode list with "
        "nothing in it, so nothing was playable). Now falls back to one row "
        "per season pack.",
        "Set season/episode on the files (panel → file → S/E) if you want "
        "per-episode rows.",
    )
    i_slash = issue(
        "filename_slash",
        "filename contains a path separator",
        "MEDIUM — used to add extra segments to /dl/{token}/{sid}/{name} and "
        "404 the stream. Now percent-encoded with safe=''.",
        "No action needed after the code fix.",
    )
    i_legacy_se = issue(
        "legacy_season_episode_shape",
        "season / episode_start / episode_end stored as arrays (legacy PTN)",
        "LOW — normalized on read, but queries like {season: 3} can miss them.",
        "Run the migration (it calls normalize_global_file_fields).",
    )

    # ------------------------------------------------------------------
    # One streamed pass over meta
    # ------------------------------------------------------------------
    cursor = db.col("meta").find({}).sort("_id", 1)
    scanned = 0
    async for doc in cursor:
        scanned += 1
        mid = doc.get("_id")
        label = lambda d: f"{d.get('_id')} · {(d.get('title') or '(no title)')[:48]}"  # noqa: E731

        if not _usable_trailer(doc.get("trailer_yt")):
            i_trailer["count"] += 1
            _examples(i_trailer["examples"], doc,
                      lambda d: f"{label(d)} · trailer_yt={d.get('trailer_yt')!r}")

        raw_type = doc.get("media_type")
        norm = P.stremio_type(raw_type, "")
        if raw_type is not None and norm != str(raw_type).strip().lower():
            i_type["count"] += 1
            _examples(i_type["examples"], doc,
                      lambda d: f"{label(d)} · media_type={d.get('media_type')!r}")
        stype = norm or "movie"

        if not str(doc.get("title") or "").strip():
            i_title["count"] += 1
            _examples(i_title["examples"], doc, label)

        y = doc.get("year")
        try:
            y_ok = y is not None and 1870 <= int(str(y)[:4]) <= 2100
        except (TypeError, ValueError):
            y_ok = False
        if not y_ok:
            i_year["count"] += 1
            _examples(i_year["examples"], doc, lambda d: f"{label(d)} · year={d.get('year')!r}")

        if doc.get("tmdb_id") in (None, "", 0):
            i_moviedb["count"] += 1
            _examples(i_moviedb["examples"], doc, label)

        cat = doc.get("catalog")
        if cat and cat in _CATALOG_TYPE and stype in _CATALOG_TYPE:
            if cat not in _CATALOG_TYPE[stype]:
                i_catalog_type["count"] += 1
                _examples(i_catalog_type["examples"], doc,
                          lambda d: f"{label(d)} · type={d.get('media_type')!r} catalog={d.get('catalog')!r}")

        if mid not in ids_with_files:
            i_orphan_meta["count"] += 1
            _examples(i_orphan_meta["examples"], doc, label)
        elif stype == "series" and mid not in metas_with_episodes:
            i_pack_only["count"] += 1
            _examples(i_pack_only["examples"], doc,
                      lambda d: f"{label(d)} · {file_counts_by_meta.get(d.get('_id'), 0)} file(s)")

    for imdb, row in shared_imdb.items():
        i_shared["count"] += 1
        if len(i_shared["examples"]) < _EXAMPLES:
            pairs = " | ".join(f"{i} · {str(t or '(no title)')[:32]}"
                               for i, t in zip(row["ids"], row["titles"]))
            i_shared["examples"].append(f"{imdb} -> {pairs}")

    for missing in (referenced_meta_ids - existing_meta_ids):
        i_orphan_files["count"] += 1
        if len(i_orphan_files["examples"]) < _EXAMPLES:
            i_orphan_files["examples"].append(
                f"{missing} ({file_counts_by_meta.get(missing, '?')} file(s))")

    # ------------------------------------------------------------------
    # One streamed pass over files (slash + legacy shapes only)
    # ------------------------------------------------------------------
    async for fdoc in db.col("files").find(
        {}, {"_id": 1, "filename": 1, "season": 1,
             "episode_start": 1, "episode_end": 1}
    ).sort("_id", 1):
        name = str(fdoc.get("filename") or "")
        if "/" in name or "\\" in name:
            i_slash["count"] += 1
            _examples(i_slash["examples"], fdoc,
                      lambda d: f"{d.get('_id')} · {str(d.get('filename'))[:60]!r}")
        if any(isinstance(fdoc.get(k), (list, tuple))
               for k in ("season", "episode_start", "episode_end")):
            i_legacy_se["count"] += 1
            _examples(i_legacy_se["examples"], fdoc,
                      lambda d: f"{d.get('_id')} · season={d.get('season')!r} "
                                f"ep={d.get('episode_start')!r}-{d.get('episode_end')!r}")

    report["totals"]["meta_scanned"] = scanned
    report["issues"] = {k: v for k, v in report["issues"].items() if v["count"]}
    report["ok"] = not any(
        v["count"] for k, v in report["issues"].items()
        if k in ("bad_trailer_key", "bad_media_type", "missing_title",
                 "shared_imdb_id", "catalog_type_mismatch"))
    return report


def format_report(report: dict) -> str:
    """Human-readable text version (for the CLI / a chat command)."""
    t = report.get("totals") or {}
    lines = [
        "GlobalDB → Stremio audit",
        f"  meta docs : {t.get('meta', '?')}",
        f"  files     : {t.get('files', '?')}",
        "",
    ]
    issues = report.get("issues") or {}
    if not issues:
        lines.append("No Stremio-visible defects found.")
        return "\n".join(lines)
    order = sorted(issues.items(), key=lambda kv: -kv[1]["count"])
    for key, v in order:
        lines.append(f"[{v['count']:>6}]  {v['label']}   ({key})")
        lines.append(f"          impact: {v['impact']}")
        for ex in v["examples"]:
            lines.append(f"          e.g. {ex}")
        lines.append("")
    return "\n".join(lines)
