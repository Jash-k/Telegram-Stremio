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

# --- External 1TamilMV scraper trigger (GitHub Actions workflow_dispatch) ----
# Your mv_scrapper repo runs on GitHub Actions. Instead of relying on GitHub's
# flaky scheduled runs, this app pokes the workflow's dispatch API on a timer so
# it runs reliably. Set GITHUB_DISPATCH_TOKEN to enable (fine-grained PAT with
# Actions: Read+write on the mv_scrapper repo). Leave blank to disable.
GITHUB_DISPATCH_TOKEN = os.getenv("GITHUB_DISPATCH_TOKEN", "").strip()
GITHUB_DISPATCH_OWNER = os.getenv("GITHUB_DISPATCH_OWNER", "Jash-k").strip()
GITHUB_DISPATCH_REPO = os.getenv("GITHUB_DISPATCH_REPO", "mv_scrapper").strip()
GITHUB_DISPATCH_WORKFLOW = os.getenv("GITHUB_DISPATCH_WORKFLOW", "scrape.yml").strip()
GITHUB_DISPATCH_REF = os.getenv("GITHUB_DISPATCH_REF", "main").strip()
GITHUB_DISPATCH_MINUTES = _int("GITHUB_DISPATCH_MINUTES", 30)

# Periodic incremental channel re-scan (the every-5-minutes background indexer).
# New files are ALREADY indexed in real time by the live update handlers, so this
# periodic scan is a redundant backup that competes with the web panel and with
# streaming for CPU/network. OFF by default; set true only if you need it.
BACKGROUND_SYNC_ENABLED = os.getenv("BACKGROUND_SYNC_ENABLED", "false").lower() in ("true", "1", "yes")
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
