"""Self-ping keepalive, external GitHub Actions scraper trigger, safety pulse."""
import asyncio
import time

import httpx

from . import config
from .logger import LOGGER

# --- v17: scraper dispatch state (in-memory, zero DB) — read by Ops Board ---
_dispatch_state = {
    "enabled": bool(config.GITHUB_DISPATCH_TOKEN),
    "interval_min": max(10, config.GITHUB_DISPATCH_MINUTES),
    "last_ts": None,
    "next_due": None,
    "last_status": None,
    "total_fired": 0,
}


def dispatch_state() -> dict:
    """Live snapshot for /stream-activity (no DB touch, safe to poll fast)."""
    out = dict(_dispatch_state)
    out["enabled"] = bool(config.GITHUB_DISPATCH_TOKEN)
    if out["enabled"] and not out["last_ts"]:
        out["next_due"] = out["next_due"] or (time.time() + 30)  # first fire soon after boot
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


async def _trigger_scraper() -> None:
    """Fire one workflow_dispatch event at the mv_scrapper GitHub Actions run."""
    url = (
        f"https://api.github.com/repos/{config.GITHUB_DISPATCH_OWNER}/"
        f"{config.GITHUB_DISPATCH_REPO}/actions/workflows/"
        f"{config.GITHUB_DISPATCH_WORKFLOW}/dispatches"
    )
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {config.GITHUB_DISPATCH_TOKEN}",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    now = time.time()
    try:
        async with httpx.AsyncClient(timeout=20.0) as c:
            resp = await c.post(url, headers=headers, json={"ref": config.GITHUB_DISPATCH_REF})
        _dispatch_state["last_ts"] = now
        _dispatch_state["total_fired"] += 1
        if resp.status_code == 204:
            _dispatch_state["last_status"] = "ok"
            LOGGER.info("[GITHUB] triggered %s/%s workflow (%s)",
                        config.GITHUB_DISPATCH_OWNER, config.GITHUB_DISPATCH_REPO,
                        config.GITHUB_DISPATCH_WORKFLOW)
        else:
            # 401 = bad/expired token; 404 = token can't see repo or wrong
            # workflow filename; 403 = missing Actions:write permission.
            _dispatch_state["last_status"] = f"HTTP {resp.status_code}"
            LOGGER.warning("[GITHUB] dispatch failed HTTP %s: %s",
                           resp.status_code, resp.text[:200])
    except Exception as exc:
        _dispatch_state["last_ts"] = now
        _dispatch_state["total_fired"] += 1
        _dispatch_state["last_status"] = f"error: {type(exc).__name__}"
        LOGGER.warning("[GITHUB] dispatch error: %s", exc)


async def github_dispatch_loop() -> None:
    """Trigger the external 1TamilMV scraper workflow on a fixed interval.

    Reliable alternative to GitHub's best-effort cron: a workflow_dispatch event
    starts a run immediately. No-op unless GITHUB_DISPATCH_TOKEN is configured.
    """
    if not config.GITHUB_DISPATCH_TOKEN:
        LOGGER.info("[GITHUB] scraper auto-trigger disabled (set GITHUB_DISPATCH_TOKEN to enable)")
        return
    interval = max(10, config.GITHUB_DISPATCH_MINUTES) * 60
    LOGGER.info("[GITHUB] triggering %s/%s every %d min",
                config.GITHUB_DISPATCH_OWNER, config.GITHUB_DISPATCH_REPO,
                config.GITHUB_DISPATCH_MINUTES)
    # Small startup stagger so we don't fire the instant the box boots.
    _dispatch_state["next_due"] = time.time() + 20
    await asyncio.sleep(20)
    while True:
        await _trigger_scraper()
        _dispatch_state["next_due"] = time.time() + interval
        await asyncio.sleep(interval)


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
