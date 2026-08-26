"""In-process telemetry for the health dashboard.

Tracks active/recent streams and error counters without any DB writes, so the
health endpoint is effectively free.
"""
import time
from collections import deque

ACTIVE_STREAMS: dict = {}
RECENT_STREAMS = deque(maxlen=50)

# Counters (monotonic since process start).
COUNTERS = {
    "stream_requests": 0,
    "stream_started": 0,
    "stream_finished": 0,
    "stream_errors": 0,
    "lookup_failures": 0,
    "auth_key_duplicated": 0,
    "flood_waits": 0,
}

ERROR_CODES = {
    "lookup_failure": "lookup_failures",
    "auth_key_duplicated": "auth_key_duplicated",
    "flood_wait": "flood_waits",
}


def bump(key: str, n: int = 1) -> None:
    if key in COUNTERS:
        COUNTERS[key] += n


def register_stream(stream_id: str, meta: dict) -> dict:
    entry = {
        "stream_id": stream_id,
        "start_ts": time.time(),
        "last_ts": time.time(),
        "total_bytes": 0,
        "avg_mbps": 0.0,
        "instant_mbps": 0.0,
        "peak_mbps": 0.0,
        "status": "active",
        "meta": meta or {},
    }
    ACTIVE_STREAMS[stream_id] = entry
    return entry


def note_chunk(entry: dict, nbytes: int) -> None:
    now = time.time()
    entry["total_bytes"] += nbytes
    entry["last_ts"] = now
    # Instant speed over a rolling ~2s window (not per-chunk, which spikes to
    # nonsense during prefetch bursts).
    win = entry.setdefault("window", [])
    win.append((now, nbytes))
    cutoff = now - 2.0
    entry["window"] = [(t, b) for t, b in win if t >= cutoff]
    window_bytes = sum(b for _, b in entry["window"])
    if entry["window"]:
        window_time = now - entry["window"][0][0]
    else:
        window_time = 1e-6
    instant = (window_bytes / (1024 * 1024)) / max(window_time, 0.1)
    entry["instant_mbps"] = instant
    if instant > entry.get("peak_mbps", 0.0):
        entry["peak_mbps"] = instant
    total_time = now - entry["start_ts"]
    entry["avg_mbps"] = (entry["total_bytes"] / (1024 * 1024)) / (total_time or 1e-6)


def finish_stream(stream_id: str, status: str = "finished") -> None:
    entry = ACTIVE_STREAMS.pop(stream_id, None)
    if not entry:
        return
    entry.pop("window", None)
    entry["status"] = status
    entry["end_ts"] = time.time()
    entry["duration"] = entry["end_ts"] - entry["start_ts"]
    if status == "finished":
        bump("stream_finished")
    else:
        bump("stream_errors")
    RECENT_STREAMS.appendleft(entry)


def snapshot() -> dict:
    """Return a JSON-safe view of active + recent streams and counters."""
    active = [
        {
            "stream_id": s["stream_id"],
            "title": (s.get("meta") or {}).get("title", ""),
            "status": s["status"],
            "total_bytes": s["total_bytes"],
            "avg_mbps": round(s["avg_mbps"], 3),
            "instant_mbps": round(s["instant_mbps"], 3),
            "peak_mbps": round(s.get("peak_mbps", 0.0), 3),
            "duration": round(time.time() - s["start_ts"], 1),
        }
        for s in ACTIVE_STREAMS.values()
    ]
    recent = [
        {
            "stream_id": s["stream_id"],
            "title": (s.get("meta") or {}).get("title", ""),
            "status": s["status"],
            "total_bytes": s["total_bytes"],
            "avg_mbps": round(s["avg_mbps"], 3),
            "duration": round(s.get("duration", 0.0), 1),
        }
        for s in RECENT_STREAMS
    ]
    return {
        "active_streams": active,
        "recent_streams": recent,
        "counters": dict(COUNTERS),
    }
