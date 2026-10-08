"""Environment-backed configuration.

Everything the service needs is read from environment variables (Koyeb
secrets). A local `config.env` is also loaded for development convenience.
"""
import os

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.env"))


def _int(key: str, default: int = 0) -> int:
    try:
        return int((os.getenv(key) or "").strip())
    except ValueError:
        return default


def _list(key: str) -> list[str]:
    return [x.strip() for x in (os.getenv(key) or "").split(",") if x.strip()]


# ---------------------------------------------------------------------------
# Telegram userbot (single session — required for indexing AND streaming)
# ---------------------------------------------------------------------------
API_ID = _int("API_ID")
API_HASH = os.getenv("API_HASH", "")
SESSION_STRING = os.getenv("SESSION_STRING", "")

# ---------------------------------------------------------------------------
# Optional bot tokens for streaming high-bitrate channels (e.g. your own 4K
# video-songs channel). Bots have a higher GetFile rate limit than user
# sessions, so routing heavy files through bots avoids flooding the user
# account. Each bot must be an ADMIN of each channel in BOT_CHANNELS.
#
# BOT_TOKENS accepts a comma-separated list (or BOT_TOKEN for a single one).
# Multiple bots load-balance across their independent rate limits.
# ---------------------------------------------------------------------------
BOT_TOKENS = _list("BOT_TOKENS")
if not BOT_TOKENS and os.getenv("BOT_TOKEN"):
    BOT_TOKENS = [x.strip() for x in os.getenv("BOT_TOKEN").split(",") if x.strip()]
BOT_CHANNELS = _list("BOT_CHANNELS")

# ---------------------------------------------------------------------------
# MongoDB (the GlobalDB). Schema-compatible with the existing `dbFyvio` DB:
#   meta, files, unindexed, catalogs, state
# ---------------------------------------------------------------------------
MONGO_URI = os.getenv("MONGO_URI", "")
DB_NAME = os.getenv("DB_NAME", "dbFyvio")

# ---------------------------------------------------------------------------
# TMDb (metadata resolution during indexing)
# ---------------------------------------------------------------------------
TMDB_API = os.getenv("TMDB_API", "")

# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------
BASE_URL = os.getenv("BASE_URL", "").rstrip("/")
PORT = _int("PORT", 8000)

# Single access token embedded in every Stremio URL. Share the same manifest
# URL with your whole family.
API_TOKEN = os.getenv("API_TOKEN", "")

# Admin key for the /api/* management endpoints.
ADMIN_KEY = os.getenv("ADMIN_KEY", "")

# Admin login for the web panel (same as the original project).
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin")

# ---------------------------------------------------------------------------
# Channels to index. Leave empty to auto-derive from the `state` collection
# (i.e. continue indexing whatever channels are already in your GlobalDB).
# Accepts plain ids ("1234567890") or full ids ("-1001234567890").
# ---------------------------------------------------------------------------
CHANNELS = _list("CHANNELS")

# ---------------------------------------------------------------------------
# Streaming tuning (Koyeb free tier friendly)
# ---------------------------------------------------------------------------
# User session: low parallelism (a user account floods easily at >1).
STREAM_PARALLELISM = _int("STREAM_PARALLELISM", 2)  # parallel Telegram reads
STREAM_PREFETCH = _int("STREAM_PREFETCH", 4)        # chunks buffered ahead

# Bots tolerate a much higher GetFile concurrency, so 4K/heavy files streamed
# through bots can use more parallelism for higher throughput.
BOT_STREAM_PARALLELISM = _int("BOT_STREAM_PARALLELISM", 4)
BOT_STREAM_PREFETCH = _int("BOT_STREAM_PREFETCH", 8)

# Media-session POOL per datacenter. Telegram throttles GetFile throughput per
# MTProto connection/auth flow, so spreading parallel chunks across 2-3 media
# sessions to the SAME datacenter meaningfully raises throughput — especially
# for private channels (which only a user session can read). Cheap on RAM.
STREAM_MEDIA_SESSIONS = _int("STREAM_MEDIA_SESSIONS", 3)
BOT_STREAM_MEDIA_SESSIONS = _int("BOT_STREAM_MEDIA_SESSIONS", 4)
# After a flood, stay at low parallelism for this many seconds (was a flat 30s
# that cratered speed after a single tiny flood).
FLOOD_COOLDOWN_SECONDS = _int("FLOOD_COOLDOWN_SECONDS", 12)

# Self-ping interval (minutes) to defeat Koyeb scale-to-zero.
KEEPALIVE_MINUTES = _int("KEEPALIVE_MINUTES", 15)

# --- v17 safety pulse: DM the self-chat when the unindexed backlog grows ---
# past a threshold (max one DM/day). Session drop/recover notices are always
# on; this knob only controls the backlog nudge.
SAFETY_DM_ENABLED = os.getenv("SAFETY_DM_ENABLED", "true").lower() in ("true", "1", "yes")
SAFETY_BACKLOG_THRESHOLD = _int("SAFETY_BACKLOG_THRESHOLD", 25)

# --- External GitHub Actions workflow triggers ------------------------------
# Your scraper / playlist repos run on GitHub Actions. Instead of relying on
# GitHub's flaky scheduled runs, this app pokes each workflow's dispatch API on
# its own timer so it runs reliably. One fine-grained PAT (Actions: Read+write,
# repository access to every target repo) drives all of them.
# Leave GITHUB_DISPATCH_TOKEN blank to disable.
GITHUB_DISPATCH_TOKEN = os.getenv("GITHUB_DISPATCH_TOKEN", "").strip()
GITHUB_DISPATCH_OWNER = os.getenv("GITHUB_DISPATCH_OWNER", "Jash-k").strip()
GITHUB_DISPATCH_REPO = os.getenv("GITHUB_DISPATCH_REPO", "mv_scrapper").strip()
GITHUB_DISPATCH_WORKFLOW = os.getenv("GITHUB_DISPATCH_WORKFLOW", "scrape.yml").strip()
GITHUB_DISPATCH_REF = os.getenv("GITHUB_DISPATCH_REF", "main").strip()
GITHUB_DISPATCH_MINUTES = _int("GITHUB_DISPATCH_MINUTES", 30)

# Multiple targets, one per workflow. Comma-separated; each entry is
#   repo|workflow.yml|minutes|ref|input=value,input2=value2
# with `ref` and the inputs optional (ref defaults to GITHUB_DISPATCH_REF).
# A bare "repo" or "repo|workflow.yml" is accepted too.
#
# Input values are coerced to the type the workflow declares: true/false ->
# boolean, digits -> number, anything else stays a string.
#
# Leave GITHUB_DISPATCH_TARGETS EMPTY to keep the legacy single-target
# behaviour built from the four variables above (nothing breaks on upgrade).
_DEFAULT_DISPATCH_TARGETS = (
    "mv_scrapper|scrape.yml|30,"
    "mv_scrapper|moviesda.yml|360,"
    "live_play|jash-live.yml|60||force=false"
)
GITHUB_DISPATCH_TARGETS_RAW = os.getenv("GITHUB_DISPATCH_TARGETS", "").strip()


def _coerce_input(raw: str):
    low = raw.strip().lower()
    if low in ("true", "false"):
        return low == "true"
    try:
        return int(raw.strip())
    except ValueError:
        pass
    try:
        return float(raw.strip())
    except ValueError:
        return raw.strip()


def _parse_dispatch_targets(raw: str) -> list[dict]:
    """Parse GITHUB_DISPATCH_TARGETS into a list of target dicts.

    Tolerant on purpose: a half-filled secret should never take the dispatcher
    down, it should just fall back to the documented defaults.
    """
    out: list[dict] = []
    for chunk in str(raw or "").split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts = [p.strip() for p in chunk.split("|")]

        # A bare ".yml" first field means "this workflow in the configured repo"
        # (repo|workflow|minutes|ref|inputs) — without it "moviesda.yml|360"
        # would be misread as owner=Jash-k repo=moviesda.yml workflow=360.
        if (parts and parts[0].endswith((".yml", ".yaml")) and "/" not in parts[0]
                and len(parts) >= 2 and not parts[1].endswith((".yml", ".yaml"))
                and (not parts[1] or parts[1].replace("-", "").isdigit())):
            parts = [GITHUB_DISPATCH_REPO] + parts

        repo = parts[0] if len(parts) > 0 else ""
        workflow = parts[1] if len(parts) > 1 else ""
        minutes = parts[2] if len(parts) > 2 else ""
        ref = parts[3] if len(parts) > 3 else ""
        # Inputs are comma-separated key=value pairs; `;` also works since the
        # outer list is comma-separated.
        inputs_raw = parts[4] if len(parts) > 4 else ""

        if not repo:
            continue
        bare = "/" not in repo
        if bare:
            repo = f"{GITHUB_DISPATCH_OWNER}/{repo}"
        if not workflow:
            # A bare repo name means "the workflow configured globally"; an
            # explicit owner/repo with no workflow means GitHub's default file.
            workflow = (GITHUB_DISPATCH_WORKFLOW
                        if bare and repo.endswith(f"/{GITHUB_DISPATCH_REPO}")
                        else "default")
        try:
            interval = int(minutes)
        except (TypeError, ValueError):
            interval = GITHUB_DISPATCH_MINUTES
        inputs: dict = {}
        for pair in inputs_raw.replace(";", ",").split(","):
            if "=" in pair:
                k, v = pair.split("=", 1)
                if k.strip():
                    inputs[k.strip()] = _coerce_input(v)
        out.append({
            "id": f"{repo.split('/', 1)[-1]}/{workflow}",
            "repo": repo,
            "workflow": workflow,
            "minutes": max(5, interval),
            "ref": ref or GITHUB_DISPATCH_REF or "main",
            "inputs": inputs,
        })
    return out


# The live target list: the explicit multi-target variable when set, otherwise
# the documented default (tamilmv 30 min · moviesda 6 h · live_play 1 h), and
# only fall back to the single legacy target if that parsing yields nothing.
GITHUB_DISPATCH_TARGETS = (
    _parse_dispatch_targets(GITHUB_DISPATCH_TARGETS_RAW)
    or _parse_dispatch_targets(_DEFAULT_DISPATCH_TARGETS)
    or [{
        "id": f"{GITHUB_DISPATCH_REPO}/{GITHUB_DISPATCH_WORKFLOW}",
        "repo": f"{GITHUB_DISPATCH_OWNER}/{GITHUB_DISPATCH_REPO}",
        "workflow": GITHUB_DISPATCH_WORKFLOW,
        "minutes": max(5, GITHUB_DISPATCH_MINUTES),
        "ref": GITHUB_DISPATCH_REF or "main",
        "inputs": {},
    }]
)

# Periodic incremental channel re-scan (the every-5-minutes background indexer).
# New files are ALREADY indexed in real time by the live update handlers, so this
# periodic scan is a redundant backup that competes with the web panel and with
# streaming for CPU/network. OFF by default; set true only if you need it.
BACKGROUND_SYNC_ENABLED = os.getenv("BACKGROUND_SYNC_ENABLED", "false").lower() in ("true", "1", "yes")

# One-shot series-metadata self-heal at boot: re-derives season/episode ranges
# from stored filenames with the current parser, relinks mis-matched shows
# (e.g. Bigg Boss Tamil → Hindi Bigg Boss) and retries the unindexed queue.
# Idempotent — after the first successful run it is a fast no-op. Set to
# "false" to skip it (e.g. after you have already repaired once).
SERIES_REPAIR_ON_START = os.getenv("SERIES_REPAIR_ON_START", "true").lower() in ("true", "1", "yes")
BACKGROUND_SYNC_MINUTES = _int("BACKGROUND_SYNC_MINUTES", 5)

# ---------------------------------------------------------------------------
# PreDVD Auto-Leecher & Stream Lifecycle Settings
# ---------------------------------------------------------------------------
PREDVD_ENABLED = os.getenv("PREDVD_ENABLED", "true").lower() in ("true", "1", "yes")
PREDVD_GROUP_ID = os.getenv("PREDVD_GROUP_ID", "")
PREDVD_COMMAND_PREFIX = os.getenv("PREDVD_COMMAND_PREFIX", "/qbleech")
PREDVD_FEED_URL = os.getenv("PREDVD_FEED_URL", "")
PREDVD_MIN_SIZE_MB = _int("PREDVD_MIN_SIZE_MB", 800)
PREDVD_MAX_FILES_PER_MOVIE = _int("PREDVD_MAX_FILES_PER_MOVIE", 1)
PREDVD_POLL_INTERVAL_MINUTES = _int("PREDVD_POLL_INTERVAL_MINUTES", 15)

# --- PreDVD cleanup bot (optional but recommended) --------------------------
# A SEPARATE bot (bot token) that is an ADMIN with "Delete messages" permission
# in your PreDVD dump channel. When the official HD/WEB-DL arrives, the global
# indexer's cleanup removes the old PreDVD DB row AND, via this bot, deletes the
# old PreDVD message from the dump channel. The indexer userbot stays a plain
# member of the dump channel (so it can index it). Leave blank to skip Telegram
# message deletion (the DB row is still removed).
PREDVD_CLEANUP_BOT_TOKEN = os.getenv("PREDVD_CLEANUP_BOT_TOKEN", "").strip()
# The dump channel id (e.g. -1001234567890). ONLY this channel's messages are
# ever deleted by the cleanup bot; regular media channels are never touched.
PREDVD_DUMP_CHANNEL_ID = os.getenv("PREDVD_DUMP_CHANNEL_ID", "").strip()
