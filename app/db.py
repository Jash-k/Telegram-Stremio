"""MongoDB (GlobalDB) connection + schema indexes.

Schema-compatible with the existing `dbFyvio` GlobalDB:
    meta, files, unindexed, catalogs, state
"""
import asyncio
import time

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, DESCENDING

from . import config
from .logger import LOGGER

_client: AsyncIOMotorClient = None
_db = None

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
    global _client, _db
    if not config.MONGO_URI:
        raise RuntimeError("MONGO_URI is not configured")
    _client = AsyncIOMotorClient(
        config.MONGO_URI,
        serverSelectionTimeoutMS=15000,
        connectTimeoutMS=15000,
        maxPoolSize=50,
    )
    # A fresh/cold instance can take a few tries to reach Atlas. Retry the
    # initial ping with backoff instead of crashing the whole app (Koyeb would
    # restart it into a crash loop on a transient network/DNS hiccup).
    last_exc = None
    for attempt in range(1, 6):
        try:
            await _client.admin.command("ping")
            break
        except Exception as exc:
            last_exc = exc
            if attempt < 5:
                LOGGER.warning("GlobalDB ping failed (attempt %d/5): %s — retrying…", attempt, exc)
                await asyncio.sleep(min(5 * attempt, 20))
    else:
        LOGGER.error("GlobalDB unreachable after 5 attempts: %s", last_exc)
        raise last_exc
    _db = _client[config.DB_NAME]
    asyncio.create_task(_ensure_indexes_and_seed())
    LOGGER.info(f"GlobalDB connected: {config.DB_NAME}")


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
