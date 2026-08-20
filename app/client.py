"""Single Telegram userbot client.

The session string grants access to the channels being indexed, so it powers
both the indexer AND the stream proxy (no bot token needed).
"""
from pyrogram import Client

from . import config
from .logger import LOGGER

client: Client = None


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
    global client
    if client is None:
        client = build()
    if not client.is_connected:
        await client.start()
        me = await client.get_me()
        LOGGER.info(f"Userbot online as {getattr(me, 'first_name', '?')} (@{getattr(me, 'username', '?')})")
    return client


async def stop() -> None:
    global client
    if client is not None and client.is_connected:
        await client.stop()
    client = None
