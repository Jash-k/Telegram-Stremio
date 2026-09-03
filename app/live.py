"""Live new/edit/delete handlers that keep the GlobalDB fresh in real time."""
from pyrogram import filters, handlers

from . import config, db
from .indexer import _configured_channels, _process_message, remove_file_reference, resolve_channel_ids
from .logger import LOGGER

_installed = False


async def _in_channel(_, __, update):
    chat_id = getattr(getattr(update, "chat", None), "id", None)
    if chat_id is None:
        return False
    try:
        configured = set(await _configured_channels())
        if not configured:
            return True
        raw_id = int(chat_id)
        canonical_id = raw_id if raw_id < 0 else int(f"-100{raw_id}")
        return raw_id in configured or canonical_id in configured
    except Exception:
        return True


async def _media_in_channel(_, __, update):
    if not await _in_channel(_, __, update):
        return False
    return bool(getattr(update, "video", None) or getattr(update, "document", None) or getattr(update, "animation", None))


media_filter = filters.create(_media_in_channel)
update_filter = filters.create(_in_channel)


async def _index_live(message) -> None:
    if not message or not getattr(message, "chat", None):
        return
    chat_id = int(message.chat.id)
    if not (getattr(message, "video", None) or getattr(message, "document", None) or getattr(message, "animation", None)):
        await remove_file_reference(chat_id, message.id)
        return
    try:
        meta_id = await _process_message(chat_id, message)
        if meta_id:
            from .cleanup import clean_meta_files
            await clean_meta_files(meta_id)
        msg_filter = "VIDEO" if message.video else "DOCUMENT"
        await db.col("state").update_one(
            {"_id": f"sync_{chat_id}_{msg_filter}"},
            {"$max": {"last_id": int(message.id)}},
            upsert=True,
        )
        LOGGER.info(f"[LIVE] Indexed message {message.id} from chat {chat_id} -> {meta_id or 'unindexed/processed'}")
    except Exception as exc:
        LOGGER.error(f"[LIVE] _index_live error for msg {message.id}: {exc}")


async def on_message(client, message):
    try:
        await _index_live(message)
    except Exception as exc:
        LOGGER.error(f"[LIVE] on_message error: {exc}")


async def on_edited(client, message):
    try:
        await _index_live(message)
    except Exception as exc:
        LOGGER.error(f"[LIVE] on_edited error: {exc}")


async def on_deleted(client, messages):
    channel_ids = set(await _configured_channels())
    for message in messages or []:
        chat_id = getattr(getattr(message, "chat", None), "id", None)
        message_id = getattr(message, "id", None)
        if message_id is not None and chat_id is not None:
            try:
                raw_id = int(chat_id)
                canonical_id = raw_id if raw_id < 0 else int(f"-100{raw_id}")
                if not channel_ids or raw_id in channel_ids or canonical_id in channel_ids:
                    await remove_file_reference(int(chat_id), int(message_id))
            except Exception as exc:
                LOGGER.error(f"[LIVE] delete error: {exc}")


def install(client) -> None:
    global _installed
    if _installed or client is None:
        return
    client.add_handler(handlers.MessageHandler(on_message, media_filter))
    client.add_handler(handlers.EditedMessageHandler(on_edited, update_filter))
    client.add_handler(handlers.DeletedMessagesHandler(on_deleted))
    _installed = True
    LOGGER.info("[LIVE] new/edit/delete live watcher handlers installed successfully")
