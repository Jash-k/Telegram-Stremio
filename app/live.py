"""Live new/edit/delete handlers that keep the GlobalDB fresh in real time."""
from pyrogram import filters, handlers

from . import config, db
from .indexer import _configured_channels, _process_message, remove_file_reference
from .logger import LOGGER

_installed = False


async def _in_channel(_, __, update):
    chat_id = getattr(getattr(update, "chat", None), "id", None)
    if chat_id is None:
        return False
    try:
        return chat_id in set(await _configured_channels())
    except Exception:
        return False


async def _has_media(_, __, update):
    return bool(getattr(update, "video", None) or getattr(update, "document", None))


update_filter = filters.create(_in_channel)
media_filter = filters.create(lambda *a: True)  # combined below


async def _media_in_channel(_, __, update):
    if not await _in_channel(_, __, update):
        return False
    return bool(getattr(update, "video", None) or getattr(update, "document", None))


media_filter = filters.create(_media_in_channel)


async def _index_live(message) -> None:
    chat_id = int(message.chat.id)
    if not (getattr(message, "video", None) or getattr(message, "document", None)):
        await remove_file_reference(chat_id, message.id)
        return
    await _process_message(chat_id, message)
    msg_filter = "VIDEO" if message.video else "DOCUMENT"
    await db.col("state").update_one(
        {"_id": f"sync_{chat_id}_{msg_filter}"},
        {"$max": {"last_id": int(message.id)}},
        upsert=True,
    )


async def on_message(client, message):
    try:
        await _index_live(message)
    except Exception as exc:
        LOGGER.error(f"[LIVE] message error: {exc}")


async def on_edited(client, message):
    try:
        await _index_live(message)
    except Exception as exc:
        LOGGER.error(f"[LIVE] edit error: {exc}")


async def on_deleted(client, messages):
    from . import db as _db

    channel_ids = set(await _configured_channels())
    for message in messages or []:
        chat_id = getattr(getattr(message, "chat", None), "id", None)
        message_id = getattr(message, "id", None)
        if chat_id in channel_ids and message_id is not None:
            try:
                await remove_file_reference(int(chat_id), int(message_id))
            except Exception as exc:
                LOGGER.error(f"[LIVE] delete error: {exc}")


def install(client) -> None:
    global _installed
    if _installed:
        return
    client.add_handler(handlers.MessageHandler(on_message, media_filter))
    client.add_handler(handlers.EditedMessageHandler(on_edited, update_filter))
    client.add_handler(handlers.DeletedMessagesHandler(on_deleted))
    _installed = True
    LOGGER.info("[LIVE] new/edit/delete handlers installed")
