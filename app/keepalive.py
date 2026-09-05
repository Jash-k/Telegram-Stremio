"""Self-ping keepalive + external GitHub Actions scraper trigger."""
import asyncio

import httpx

from . import config
from .logger import LOGGER


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
    try:
        async with httpx.AsyncClient(timeout=20.0) as c:
            resp = await c.post(url, headers=headers, json={"ref": config.GITHUB_DISPATCH_REF})
        if resp.status_code == 204:
            LOGGER.info("[GITHUB] triggered %s/%s workflow (%s)",
                        config.GITHUB_DISPATCH_OWNER, config.GITHUB_DISPATCH_REPO,
                        config.GITHUB_DISPATCH_WORKFLOW)
        else:
            # 401 = bad/expired token; 404 = token can't see repo or wrong
            # workflow filename; 403 = missing Actions:write permission.
            LOGGER.warning("[GITHUB] dispatch failed HTTP %s: %s",
                           resp.status_code, resp.text[:200])
    except Exception as exc:
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
    await asyncio.sleep(20)
    while True:
        await _trigger_scraper()
        await asyncio.sleep(interval)
