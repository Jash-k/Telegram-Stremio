"""Self-ping keepalive to defeat Koyeb scale-to-zero."""
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
