"""Pytest fixtures: run the real Stremio routes against an in-memory Mongo.

`db.connect()` needs a live server, so we hand `app.db` a mongomock client
directly — the route code under test is completely unmodified.

Tests are async (pytest-asyncio): they seed the mocked collections with
`await`, then call the routes through a synchronous TestClient.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("API_TOKEN", "test-token")
os.environ.setdefault("ADMIN_KEY", "test-admin")
os.environ.setdefault("MONGO_URI", "mongodb://localhost/unused")
os.environ.setdefault("BASE_URL", "https://addon.example")
os.environ.setdefault("TMDB_API", "")

mongomock_motor = pytest.importorskip(
    "mongomock_motor",
    reason="pip install -r tests/requirements-dev.txt",
)
pytest.importorskip("pytest_asyncio", reason="pip install -r tests/requirements-dev.txt")

from app import db  # noqa: E402

TOKEN = "test-token"


def pytest_configure(config):
    config.addinivalue_line("markers", "asyncio: async test")


@pytest.fixture()
def client():
    """A TestClient wired to the real stremio router + an empty in-memory DB."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from app.cache import invalidate_all
    from app.routes import stremio

    db._db = mongomock_motor.AsyncMongoMockClient()["testdb"]
    db.connected = True
    invalidate_all()

    app = FastAPI()
    app.include_router(stremio.router)
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c

    invalidate_all()
    db._db = None
    db.connected = False


@pytest.fixture()
async def seed():
    """`await seed.meta(...)`, `await seed.files(...)`, `await seed.catalogs()`."""
    class _Seeder:
        async def meta(self, *docs):
            await db.col("meta").insert_many([dict(d) for d in docs])

        async def files(self, *docs):
            await db.col("files").insert_many([dict(d) for d in docs])

        async def catalogs(self):
            for c in db.CATALOGS:
                await db.col("catalogs").update_one(
                    {"_id": c["_id"]}, {"$set": c}, upsert=True)

        async def clear(self):
            for name in ("meta", "files", "catalogs", "unindexed"):
                await db.col(name).delete_many({})

    return _Seeder()


def meta_doc(_id, **kw):
    """A `meta` document with every field the catalog/meta routes read."""
    doc = {
        "_id": _id, "tmdb_id": 1, "imdb_id": None, "aliases": [_id],
        "title": "Some Title", "year": 2020,
        "poster": "https://image.tmdb.org/t/p/w500/p.jpg",
        "background": "https://image.tmdb.org/t/p/original/b.jpg",
        "description": "A description.", "media_type": "movie",
        "catalog": "other_movies", "genres": ["Drama"], "rating": 7.5,
        "languages": ["English"], "updated_at": 1.0,
    }
    doc.update(kw)
    return doc


def file_doc(meta_id, chat_id, msg_id, filename, **kw):
    """A `files` document with every field the stream route reads."""
    doc = {
        "_id": f"{chat_id}_{msg_id}", "meta_id": meta_id,
        "chat_id": chat_id, "message_id": msg_id, "filename": filename,
        "size": 700_000_000, "size_str": "700 MB", "quality": "1080p",
        "codec": "x264", "audio": "AAC", "resolution": "1080p",
        "source": "digital", "season": None,
        "episode_start": None, "episode_end": None, "indexed_at": 1.0,
    }
    doc.update(kw)
    return doc
