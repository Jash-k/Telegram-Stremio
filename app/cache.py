"""Small in-memory TTL cache for read-heavy JSON responses.

Cuts Atlas round-trips (and the free-tier cold-start) on repeated catalog/meta
browses. Bounded in size so it never threatens the 512 MB instance.
"""
import threading
import time
from collections import OrderedDict

_MAX_ENTRIES = 2000


class TTLCache:
    def __init__(self, max_entries: int = _MAX_ENTRIES):
        self._max = max_entries
        self._data: OrderedDict = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key):
        now = time.monotonic()
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            value, expires = entry
            if expires <= now:
                self._data.pop(key, None)
                return None
            # refresh recency
            self._data.move_to_end(key)
            return value

    def set(self, key, value, ttl: float):
        with self._lock:
            if key in self._data:
                self._data.move_to_end(key)
            self._data[key] = (value, time.monotonic() + ttl)
            while len(self._data) > self._max:
                self._data.popitem(last=False)

    def clear(self):
        with self._lock:
            self._data.clear()

    def __len__(self):
        with self._lock:
            return len(self._data)


# Shared caches (TTLs tuned for a mostly-static library).
manifest_cache = TTLCache()   # key: token -> manifest dict
catalog_cache = TTLCache()    # key: (token, type, id, extra) -> metas dict
meta_cache = TTLCache()       # key: (token, media_type, id) -> meta dict
file_ids_cache = TTLCache()   # key: "ids" -> frozenset of meta_ids that have files

MANIFEST_TTL = 600.0   # 10 min
CATALOG_TTL = 45.0     # 45 s
META_TTL = 300.0       # 5 min
FILE_IDS_TTL = 120.0   # 2 min (orphan filter)


def invalidate_all():
    """Drop every cached response (call on wipe / big index changes)."""
    manifest_cache.clear()
    catalog_cache.clear()
    meta_cache.clear()
    file_ids_cache.clear()
