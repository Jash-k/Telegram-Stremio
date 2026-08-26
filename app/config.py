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
# Optional bot token for streaming high-bitrate channels (e.g. your own 4K
# video-songs channel). Bots have a higher GetFile rate limit than user
# sessions, so routing heavy files through a bot avoids flooding the user
# account. The bot must be an ADMIN of each channel listed in BOT_CHANNELS.
# ---------------------------------------------------------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
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
STREAM_PARALLELISM = _int("STREAM_PARALLELISM", 3)  # parallel Telegram reads
STREAM_PREFETCH = _int("STREAM_PREFETCH", 6)        # chunks buffered ahead

# Self-ping interval (minutes) to defeat Koyeb scale-to-zero.
KEEPALIVE_MINUTES = _int("KEEPALIVE_MINUTES", 15)
