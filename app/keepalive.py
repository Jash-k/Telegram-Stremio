"""Self-ping keepalive, external GitHub Actions triggers, safety pulse."""
import asyncio
import time

import httpx

from . import config
from .logger import LOGGER

# How GitHub's dispatch endpoint answers, so the log says something useful
# instead of just a status code.
_DISPATCH_HINTS = {
    401: "bad/expired/revoked token — GitHub did not recognise it at all "
         "(this is checked before permissions, so it is never a scope problem). "
         "Create a new PAT and update the GITHUB_DISPATCH_TOKEN secret, then "
         "redeploy. Run tools/check_github_dispatch.py to confirm.",
    403: "token recognised but refused — fine-grained PAT repository access "
         "does not include this repo, or Actions:write is missing, or an org "
         "SSO token was never authorised.",
    404: "token cannot SEE this repo/workflow, or the workflow filename is "
         "wrong (GitHub hides private repos behind 404, so a typo and a "
         "missing permission look identical).",
    422: "the workflow does not accept workflow_dispatch, a required input is "
         "missing, or `ref` is not a real branch.",
}

# After this many consecutive 401s stop hammering the API: the token is wrong
# and re-trying every 30 minutes only floods the log. Re-check slowly instead.
_AUTH_BACKOFF_AFTER = 3
_AUTH_BACKOFF_SECONDS = 6 * 3600

# --- dispatch state (in-memory, zero DB) — read by the Ops Board ------------
# Legacy top-level keys are still maintained so an older panel/health consumer
# keeps working; `targets` carries the per-workflow detail.
_dispatch_state = {
    "enabled": bool(config.GITHUB_DISPATCH_TOKEN),
    "interval_min": max(5, config.GITHUB_DISPATCH_MINUTES),
    "last_ts": None,
    "next_due": None,
    "last_status": None,
    "total_fired": 0,
    "targets": [],
}

# Per-target runtime state, keyed by target id.
_target_state: dict[str, dict] = {}


def _init_target_state() -> None:
    """(Re)build per-target state from config. Idempotent and side-effect free.

    Called on every dispatch_state() poll, so it must NEVER reset a runtime
    field (next_due / last_status / counters) that the loop has already set.
    """
    now = time.time()
    seen = set()
    for i, t in enumerate(config.GITHUB_DISPATCH_TARGETS):
        tid = t["id"]
        seen.add(tid)
        cur = _target_state.setdefault(tid, {})
        cur.update({
            "id": tid,
            "repo": t["repo"],
            "workflow": t["workflow"],
            "minutes": t["minutes"],
            "ref": t["ref"],
            "has_inputs": bool(t.get("inputs")),
        })
        if cur.get("next_due") is None:
            # Stagger the first fire so a boot doesn't hit the API three times
            # in the same second, and so the board shows distinct countdowns.
            cur["next_due"] = now + 20 + i * 5
        cur.setdefault("last_ts", None)
        cur.setdefault("last_status", None)
        cur.setdefault("total_fired", 0)
        cur.setdefault("auth_failures", 0)
        cur.setdefault("backoff_until", 0)
    for stale in [k for k in _target_state if k not in seen]:
        _target_state.pop(stale, None)
    _dispatch_state["interval_min"] = min(
        (v["minutes"] for v in _target_state.values()), default=30)


def _state_for(target: dict) -> dict:
    """Per-target runtime state, created on demand for ad-hoc targets."""
    st = _target_state.get(target["id"])
    if st is None:
        st = {
            "id": target["id"], "repo": target["repo"],
            "workflow": target["workflow"], "minutes": target.get("minutes", 30),
            "ref": target.get("ref", "main"), "next_due": None, "last_ts": None,
            "last_status": None, "total_fired": 0, "auth_failures": 0,
            "backoff_until": 0,
        }
        _target_state[target["id"]] = st
    return st


def _public_target(st: dict) -> dict:
    return {k: st[k] for k in ("id", "repo", "workflow", "minutes", "ref",
                               "next_due", "last_ts", "last_status",
                               "total_fired") if k in st}


def dispatch_state() -> dict:
    """Live snapshot for /stream-activity (no DB touch, safe to poll fast)."""
    _init_target_state()
    targets = [_public_target(v) for v in _target_state.values()]
    _dispatch_state["targets"] = targets
    out = dict(_dispatch_state)
    out["enabled"] = bool(config.GITHUB_DISPATCH_TOKEN)
    out["targets"] = targets
    out["total_fired"] = sum(t.get("total_fired") or 0 for t in targets)
    due = [t["next_due"] for t in targets if t.get("next_due")]
    out["next_due"] = min(due) if due else None
    if out["enabled"] and not out["last_ts"]:
        out["next_due"] = out["next_due"] or (time.time() + 30)
    # Legacy single-value view: the most recent target result.
    latest = max((t for t in targets if t.get("last_ts")),
                 key=lambda t: t["last_ts"], default=None)
    if latest:
        out["last_ts"] = latest["last_ts"]
        out["last_status"] = latest["last_status"]
    return out


async def keepalive_loop() -> None:
    if not config.BASE_URL:
        LOGGER.warning("[KEEPALIVE] BASE_URL not set — self-ping disabled")
        return
    url = f"{config.BASE_URL}/healthz"
    interval = max(5, config.KEEPALIVE_MINUTES) * 60
    LOGGER.info(f"[KEEPALIVE] pinging {url} every {interval}s")
    while True:
        await asyncio.sleep(interval)
        try:
            async with httpx.AsyncClient(timeout=10.0) as c:
                await c.get(url)
        except Exception:
            LOGGER.debug("[KEEPALIVE] ping failed")


async def _trigger(target: dict) -> str:
    """Fire one workflow_dispatch at a target. Returns the recorded status."""
    url = (f"https://api.github.com/repos/{target['repo']}/actions/workflows/"
           f"{target['workflow']}/dispatches")
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {config.GITHUB_DISPATCH_TOKEN}",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    payload: dict = {"ref": target["ref"]}
    if target.get("inputs"):
        payload["inputs"] = dict(target["inputs"])

    st = _state_for(target)
    now = time.time()
    st["last_ts"] = now
    st["total_fired"] = int(st.get("total_fired") or 0) + 1
    _dispatch_state["last_ts"] = now
    _dispatch_state["total_fired"] = int(_dispatch_state.get("total_fired") or 0) + 1

    try:
        async with httpx.AsyncClient(timeout=20.0) as c:
            resp = await c.post(url, headers=headers, json=payload)
    except Exception as exc:
        st["last_status"] = f"error: {type(exc).__name__}"
        _dispatch_state["last_status"] = st["last_status"]
        LOGGER.warning("[GITHUB] %s dispatch error: %s", target["id"], exc)
        return st["last_status"]

    if resp.status_code == 204:
        st["last_status"] = "ok"
        st["auth_failures"] = 0
        st["backoff_until"] = 0
        _dispatch_state["last_status"] = "ok"
        LOGGER.info("[GITHUB] triggered %s (%s @ %s)",
                    target["id"], target["repo"], target["ref"])
        return "ok"

    st["last_status"] = f"HTTP {resp.status_code}"
    _dispatch_state["last_status"] = st["last_status"]
    hint = _DISPATCH_HINTS.get(resp.status_code, "")
    LOGGER.warning("[GITHUB] %s dispatch failed HTTP %s: %s%s",
                   target["id"], resp.status_code, resp.text[:200],
                   f" — {hint}" if hint else "")

    if resp.status_code == 401:
        st["auth_failures"] = int(st.get("auth_failures") or 0) + 1
        if st["auth_failures"] >= _AUTH_BACKOFF_AFTER:
            st["backoff_until"] = time.time() + _AUTH_BACKOFF_SECONDS
            LOGGER.error(
                "[GITHUB] %d consecutive 401s — the token is invalid. "
                "Backing off to one retry every %d h so the log stays readable. "
                "Fix GITHUB_DISPATCH_TOKEN and redeploy; "
                "tools/check_github_dispatch.py will tell you exactly what is wrong.",
                st["auth_failures"], _AUTH_BACKOFF_SECONDS // 3600)
    return st["last_status"]


async def trigger_target_now(target_id: str) -> dict:
    """Manual 'fire now' for one target (panel button / admin API)."""
    if not config.GITHUB_DISPATCH_TOKEN:
        return {"ok": False, "status": "no GITHUB_DISPATCH_TOKEN configured"}
    _init_target_state()
    st = _target_state.get(target_id)
    if not st:
        return {"ok": False,
                "status": f"unknown target {target_id!r}",
                "known": sorted(_target_state)}
    target = next((t for t in config.GITHUB_DISPATCH_TARGETS
                   if t["id"] == target_id), None)
    if not target:
        return {"ok": False, "status": f"target {target_id!r} is not in config"}
    status = await _trigger(target)
    st["next_due"] = time.time() + target["minutes"] * 60
    return {"ok": status == "ok", "status": status, "target": target_id}


_TICK_SECONDS = 15


async def github_dispatch_loop(tick: int | None = None) -> None:
    """Trigger every configured GitHub Actions workflow on its own cadence.

    One task, one short tick: each target carries its own `next_due`, so a
    30-minute scraper and a 6-hour scraper coexist without drift and without
    one asyncio task per workflow. Reliable alternative to GitHub's best-effort
    cron — a workflow_dispatch event starts a run immediately.
    """
    if not config.GITHUB_DISPATCH_TOKEN:
        LOGGER.info("[GITHUB] workflow auto-trigger disabled "
                    "(set GITHUB_DISPATCH_TOKEN to enable)")
        return

    tick = int(tick or _TICK_SECONDS)
    _init_target_state()
    LOGGER.info("[GITHUB] dispatching %d workflow target(s): %s",
                len(_target_state),
                ", ".join(f"{t['repo']}@{t['workflow']} every {t['minutes']}m"
                          for t in _target_state.values()))

    while True:
        now = time.time()
        for tid, st in list(_target_state.items()):
            target = next((t for t in config.GITHUB_DISPATCH_TARGETS
                           if t["id"] == tid), None)
            if not target:
                continue
            if float(st.get("backoff_until") or 0) > now:
                # Token is known-bad; re-check slowly instead of every interval.
                st["next_due"] = float(st["backoff_until"])
                continue
            if float(st.get("next_due") or 0) > now:
                continue
            await _trigger(target)
            st["next_due"] = time.time() + target["minutes"] * 60
        _dispatch_state["targets"] = [_public_target(v)
                                      for v in _target_state.values()]
        await asyncio.sleep(tick)


# ---------------------------------------------------------------------------
# v17 Safety pulse — DMs that make a headless deploy tell you when it hurts.
#   * session dropped → (silent, it can't DM itself) → restored → one DM
#   * unindexed backlog growing past a threshold → at most one DM per day
# Rides the existing 60-s watchdog loop (throttled below) — no new timers.
# ---------------------------------------------------------------------------

_pulse = {
    "last_run": 0.0,
    "prev_connected": None,
    "down_since": None,
    "last_backlog": None,
    "last_backlog_alert": 0.0,
}


async def safety_tick() -> None:
    """Called from the session watchdog; self-throttles to KEEPALIVE_MINUTES.

    Swallows everything: an alarm system must never become an outage.
    """
    now = time.time()
    if now - _pulse["last_run"] < max(5, config.KEEPALIVE_MINUTES) * 60:
        return
    _pulse["last_run"] = now
    try:
        from . import client as client_mod

        connected = client_mod.is_connected()
        prev = _pulse["prev_connected"]
        _pulse["prev_connected"] = connected

        if not connected:
            if prev is not False:
                _pulse["down_since"] = now
            return  # no session → no way to DM; recovery notice will follow

        if prev is False:  # recovered — say so once
            down = now - (_pulse.get("down_since") or now)
            _pulse["down_since"] = None
            try:
                await client_mod.client.send_message(
                    "me",
                    f"🔌 Userbot session restored after {int(down // 60)} min offline "
                    f"(reconnect attempts: {client_mod.session_status().get('reconnect_attempts', 0)}).",
                )
            except Exception:
                pass

        if not config.SAFETY_DM_ENABLED:
            return
        try:
            from . import db

            count = await db.col("unindexed").count_documents({})
        except Exception:
            return  # DB down: nothing to compare, nothing to alert on
        prev_count = _pulse["last_backlog"]
        _pulse["last_backlog"] = count
        growing = prev_count is not None and count > prev_count
        hot = count >= config.SAFETY_BACKLOG_THRESHOLD
        cool = now - _pulse["last_backlog_alert"] > 86400  # one DM/day max
        if hot and growing and cool:
            _pulse["last_backlog_alert"] = now
            top = ""
            try:
                rows = {}
                async for row in db.col("unindexed").aggregate(
                    [{"$group": {"_id": "$reason", "n": {"$sum": 1}}},
                     {"$sort": {"n": -1}}, {"$limit": 3}]
                ):
                    rows[str(row.get("_id") or "unknown")] = int(row.get("n") or 0)
                if rows:
                    top = "\n" + "\n".join(f"  • {r} ×{n}" for r, n in rows.items())
            except Exception:
                pass
            try:
                await client_mod.client.send_message(
                    "me",
                    f"📥 Backlog: {count} unindexed files (+"
                    f"{count - prev_count} since last check).{top}\n"
                    "Run Self-Heal or check the panel queue.",
                )
            except Exception:
                pass
    except Exception as exc:
        LOGGER.debug("[PULSE] tick failed (ignored): %s", exc)
