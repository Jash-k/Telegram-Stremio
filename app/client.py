"""Single Telegram userbot client.

The session string grants access to the channels being indexed, so it powers
both the indexer AND the stream proxy (no bot token needed).
"""
import asyncio

from pyrogram import Client

from . import config
from .logger import LOGGER

client: Client = None
_start_lock = asyncio.Lock()


def build() -> Client:
    return Client(
        name="globaldb_userbot",
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        session_string=config.SESSION_STRING,
        sleep_threshold=20,
        workers=4,
        max_concurrent_transmissions=3,
        no_updates=False,
        in_memory=True,
    )


async def start() -> Client:
    """Start the userbot (used at app startup). Raises on failure."""
    global client
    if client is None:
        client = build()
    if not client.is_connected:
        await client.start()
        me = await client.get_me()
        LOGGER.info(f"Userbot online as {getattr(me, 'first_name', '?')} (@{getattr(me, 'username', '?')})")
    return client


async def ensure_started() -> Client | None:
    """Connect (or reconnect) the userbot if it isn't connected.

    Returns the connected client, or None if a connection cannot be
    established (e.g. session conflict). Never raises.
    """
    global client
    if client is None:
        client = build()
    if client.is_connected:
        return client
    async with _start_lock:
        if client.is_connected:
            return client
        try:
            await client.start()
            me = await client.get_me()
            LOGGER.info(f"Userbot (re)connected as {getattr(me, 'first_name', '?')} (@{getattr(me, 'username', '?')})")
            return client
        except Exception as exc:
            LOGGER.error("Userbot could not connect: %s", exc)
            return None


def is_connected() -> bool:
    return client is not None and client.is_connected


async def stop() -> None:
    global client
    if client is not None and client.is_connected:
        await client.stop()
    client = None
