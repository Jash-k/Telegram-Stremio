"""MongoDB (GlobalDB) connection + schema indexes.

Schema-compatible with the existing `dbFyvio` GlobalDB:
    meta, files, unindexed, catalogs, state
"""
import asyncio
import time

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import PyMongoError

from . import config
from .logger import LOGGER

_client: AsyncIOMotorClient = None
_db = None
# Health flag used by background loops to skip work (and skip log spam) while
# Atlas is unreachable. Set by the persistent _db_monitor loop.
connected: bool = False
_seeded: bool = False


def is_connected() -> bool:
    return connected

CATALOGS = [
    {"_id": "tamil_movies", "type": "movie", "name": "Tamil Movies", "order": 1},
    {"_id": "tamil_series", "type": "series", "name": "Tamil Series", "order": 2},
    {"_id": "video_songs", "type": "movie", "name": "Video Songs", "order": 3},
    {"_id": "dubbed_movies", "type": "movie", "name": "Dubbed Movies", "order": 4},
    {"_id": "dubbed_series", "type": "series", "name": "Dubbed Series", "order": 5},
    {"_id": "anime_movies", "type": "movie", "name": "Anime Movies", "order": 6},
    {"_id": "anime_series", "type": "series", "name": "Anime Series", "order": 7},
    {"_id": "other_movies", "type": "movie", "name": "Other Movies", "order": 8},
    {"_id": "other_series", "type": "series", "name": "Other Series", "order": 9},
]


def col(name: str):
    """Return a collection handle on the GlobalDB."""
    return _db[name]


async def connect() -> None:
    """Initialize the MongoDB client WITHOUT blocking app startup.

    We assign the DB handle immediately (Motor connects lazily and retries
    automatically on every operation), so uvicorn binds the port and passes
    Koyeb's TCP health check even if Atlas is momentarily unreachable on a
    cold boot. The actual connectivity ping + schema seed run in the background.
    """
    global _client, _db
    if not config.MONGO_URI:
        raise RuntimeError("MONGO_URI is not configured")
    _client = AsyncIOMotorClient(
        config.MONGO_URI,
        serverSelectionTimeoutMS=8000,
        connectTimeoutMS=8000,
        maxPoolSize=50,
    )
    _db = _client[config.DB_NAME]
    asyncio.create_task(_db_monitor())


async def _db_monitor() -> None:
    """Persistent health + seed loop.

    Runs forever (outside the lifespan so it never blocks the health check).
    Pings Atlas on a slow cadence; when reachable it marks `connected=True`
    (background loops resume) and seeds indexes/catalogs once. While down it
    logs a single concise line and keeps `connected=False` so loops skip work
    instead of raising an 8 s timeout every tick.
    """
    global connected, _seeded
    announced_down = False
    while True:
        try:
            await _client.admin.command("ping")
            if not connected:
                LOGGER.info(f"GlobalDB connected: {config.DB_NAME}")
            connected = True
            announced_down = False
            if not _seeded:
                try:
                    await _ensure_indexes_and_seed()
                    _seeded = True
                except Exception as exc:
                    LOGGER.warning("GlobalDB schema seed error (will retry): %s", exc)
        except Exception as exc:
            connected = False
            if not announced_down:
                # First failure — one concise, actionable line; no topology dump.
                LOGGER.error(
                    "GlobalDB unreachable (queries return 503, loops paused). "
                    "Cause is almost always Atlas Network Access not allowing "
                    "this host — add 0.0.0.0/0 in Atlas → Network Access. [%s]",
                    type(exc).__name__,
                )
                announced_down = True
        await asyncio.sleep(15 if connected else 20)


async def _ensure_indexes_and_seed() -> None:
    try:
        schema = await _db["state"].find_one({"_id": "schema"})
        if schema and schema.get("version") == 3:
            return
        await _ensure_indexes()
        await _seed_catalogs()
    except Exception as exc:
        LOGGER.warning(f"Background schema check error: {exc}")


async def _ensure_indexes() -> None:
    specs = {
        "files": [
            [("chat_id", ASCENDING), ("message_id", ASCENDING)],
            # episode bounds kept in separate indexes (Mongo rejects compound
            # indexes with two parallel array paths — legacy rows may be lists).
            [("meta_id", ASCENDING), ("season", ASCENDING), ("episode_start", ASCENDING)],
            [("meta_id", ASCENDING), ("season", ASCENDING), ("episode_end", ASCENDING)],
            [("meta_id", ASCENDING), ("quality", ASCENDING), ("size", DESCENDING)],
            [("indexed_at", DESCENDING)],
        ],
        "meta": [
            [("catalog", ASCENDING), ("updated_at", DESCENDING)],
            [("media_type", ASCENDING), ("updated_at", DESCENDING)],
            [("imdb_id", ASCENDING)],
            [("tmdb_id", ASCENDING)],
            [("aliases", ASCENDING)],
            [("languages", ASCENDING)],
            [("genres", ASCENDING)],
        ],
        "unindexed": [
            [("chat_id", ASCENDING), ("message_id", ASCENDING)],
            [("reason", ASCENDING)],
            [("updated_at", DESCENDING)],
        ],
        "catalogs": [[("order", ASCENDING)]],
    }
    for coll_name, idx_specs in specs.items():
        for keys in idx_specs:
            try:
                await _db[coll_name].create_index(keys)
            except Exception as exc:  # pragma: no cover
                LOGGER.warning(f"index skip {coll_name} {keys}: {exc}")


async def _seed_catalogs() -> None:
    for cat in CATALOGS:
        await _db["catalogs"].update_one({"_id": cat["_id"]}, {"$set": cat}, upsert=True)
    await _db["state"].update_one(
        {"_id": "schema"},
        {"$set": {"version": 3, "updated_at": time.time()}},
        upsert=True,
    )


async def disconnect() -> None:
    global _client, _db
    if _client is not None:
        _client.close()
    _client = None
    _db = None
