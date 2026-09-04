"""Dedicated PreDVD cleanup bot.

A SEPARATE bot (its own token) that must be an ADMIN with "Delete messages"
permission in the PreDVD dump channel. When the official digital release
arrives and global cleanup removes the old PreDVD DB row, this bot deletes the
old PreDVD message from the dump channel.

Safety contract:
  * It will only ever delete messages in PREDVD_DUMP_CHANNEL_ID.
  * Regular media channels are NEVER passed here.
  * It connects lazily (only when a real delete is needed) so the free-tier
    RAM footprint stays ~0 when idle.
  * It uses no_updates=True — a bot has no session-lease conflict risk.
"""
import asyncio

from pyrogram import Client

from . import config
from .logger import LOGGER

_client: Client = None
_start_lock = asyncio.Lock()


def is_enabled() -> bool:
    return bool(config.PREDVD_CLEANUP_BOT_TOKEN) and bool(config.PREDVD_DUMP_CHANNEL_ID)


def dump_channel() -> int | None:
    """The canonical dump-channel id, or None if unset/unparsable."""
    raw = str(config.PREDVD_DUMP_CHANNEL_ID or "").strip()
    if not raw:
        return None
    try:
        n = int(raw)
    except ValueError:
        return None
    return n if n < 0 else int(f"-100{n}")


async def _get_client() -> Client | None:
    global _client
    if not is_enabled():
        return None
    if _client is not None and _client.is_connected:
        return _client
    async with _start_lock:
        if _client is not None and _client.is_connected:
            return _client
        try:
            if _client is None:
                _client = Client(
                    name="predvd_cleanup_bot",
                    api_id=config.API_ID,
                    api_hash=config.API_HASH,
                    bot_token=config.PREDVD_CLEANUP_BOT_TOKEN,
                    no_updates=True,
                    in_memory=True,
                    workers=1,
                )
            if not _client.is_connected:
                await _client.start()
                await _client.get_me()
            LOGGER.info("[CLEANUP-BOT] online (PreDVD dump-channel message deletion)")
            return _client
        except Exception as exc:
            LOGGER.error("[CLEANUP-BOT] failed to start: %s", exc)
            return None


async def delete_dump_message(chat_id: int, message_id: int) -> bool:
    """Delete one message, ONLY if it belongs to the configured dump channel.

    Returns True on success (or when the feature is disabled — DB cleanup
    proceeds either way). Never raises.
    """
    dump = dump_channel()
    if dump is None:
        return True
    try:
        if int(chat_id) != int(dump):
            # Hard guard: never delete outside the dump channel.
            LOGGER.warning("[CLEANUP-BOT] refused delete: chat %s != dump %s", chat_id, dump)
            return False
        c = await _get_client()
        if c is None:
            LOGGER.warning("[CLEANUP-BOT] unavailable; DB row removed but Telegram message kept.")
            return False
        await c.delete_messages(chat_id=int(dump), message_ids=int(message_id), revoke=True)
        LOGGER.info("[CLEANUP-BOT] deleted PreDVD msg %s from dump %s", message_id, dump)
        return True
    except Exception as exc:
        LOGGER.warning("[CLEANUP-BOT] delete failed for msg %s: %s", message_id, exc)
        return False


async def stop() -> None:
    global _client
    if _client is not None:
        try:
            if _client.is_connected:
                await _client.stop()
        except Exception:
            pass
        _client = None
