from pyrogram import filters
from pyrogram.handlers import DeletedMessagesHandler, EditedMessageHandler, MessageHandler

from Backend.helper.global_db_service import (
    remove_global_file_reference,
    remove_global_file_references,
)
from Backend.helper.global_indexer import _process_message
from Backend.helper.global_search import _resolve_channel_ids
from Backend.helper.settings_manager import SettingsManager
from Backend.logger import LOGGER
from Backend.pyrofork.bot import Userbot

_HANDLERS_INSTALLED = False


def _configured_channel_ids() -> set[int]:
    settings = SettingsManager.current()
    if not settings.global_search:
        return set()
    return set(_resolve_channel_ids(settings.global_search_channels))


async def global_channel_update_filter(_, __, update):
    chat_id = getattr(getattr(update, "chat", None), "id", None)
    return chat_id is not None and chat_id in _configured_channel_ids()


async def global_channel_media_filter(_, __, update):
    return await global_channel_update_filter(_, __, update) and bool(
        getattr(update, "video", None) or getattr(update, "document", None)
    )


global_update_filter = filters.create(global_channel_update_filter)
global_media_filter = filters.create(global_channel_media_filter)


async def _index_live_message(message, edited: bool = False) -> None:
    from Backend import db

    if getattr(db, "global_db", None) is None:
        return
    chat_id = int(message.chat.id)
    if not (getattr(message, "video", None) or getattr(message, "document", None)):
        await remove_global_file_reference(db.global_db, chat_id, message.id)
        return

    meta_id = await _process_message(db, message, chat_id)
    action = "Reconciled edited" if edited else "Indexed new"
    LOGGER.info(
        "[LIVE GLOBAL INDEXER] %s file from %s: %s",
        action,
        chat_id,
        message.id,
    )

    # Push incremental state forward without allowing an out-of-order event to
    # move the checkpoint backwards. Historic state remains independently resumable.
    msg_filter_name = "VIDEO" if message.video else "DOCUMENT"
    sync_key = f"sync_{chat_id}_{msg_filter_name}"
    await db.global_db["state"].update_one(
        {"_id": sync_key},
        {"$max": {"last_id": int(message.id)}},
        upsert=True,
    )

    # Keep the existing duplicate-quality policy, but never use it as a
    # substitute for stale Telegram-reference reconciliation.
    if meta_id:
        from Backend.helper.global_indexer import clean_meta_files

        await clean_meta_files(db, meta_id)


async def live_index_handler(client, message):
    try:
        await _index_live_message(message)
    except Exception as exc:
        LOGGER.error(
            "[LIVE GLOBAL INDEXER] Error processing %s in %s: %s",
            getattr(message, "id", "?"),
            getattr(getattr(message, "chat", None), "id", "?"),
            exc,
        )


async def live_edit_handler(client, message):
    try:
        await _index_live_message(message, edited=True)
    except Exception as exc:
        LOGGER.error(
            "[LIVE GLOBAL INDEXER] Error reconciling edited message %s in %s: %s",
            getattr(message, "id", "?"),
            getattr(getattr(message, "chat", None), "id", "?"),
            exc,
        )


async def live_delete_handler(client, messages):
    from Backend import db

    if getattr(db, "global_db", None) is None:
        return
    channel_ids = _configured_channel_ids()
    references = []
    for message in messages or []:
        chat_id = getattr(getattr(message, "chat", None), "id", None)
        message_id = getattr(message, "id", None)
        if chat_id in channel_ids and message_id is not None:
            references.append((int(chat_id), int(message_id)))
    if not references:
        return
    try:
        deleted = await remove_global_file_references(db.global_db, references)
        LOGGER.info(
            "[LIVE GLOBAL INDEXER] Reconciled %s deleted Telegram message(s); "
            "removed %s indexed file(s).",
            len(references),
            deleted,
        )
    except Exception as exc:
        LOGGER.error("[LIVE GLOBAL INDEXER] Delete reconciliation failed: %s", exc)


def setup_live_indexer():
    global _HANDLERS_INSTALLED
    if Userbot is None or _HANDLERS_INSTALLED:
        return
    Userbot.add_handler(MessageHandler(live_index_handler, global_media_filter))
    Userbot.add_handler(EditedMessageHandler(live_edit_handler, global_update_filter))
    Userbot.add_handler(DeletedMessagesHandler(live_delete_handler))
    _HANDLERS_INSTALLED = True
    LOGGER.info(
        "[LIVE GLOBAL INDEXER] New/edit/delete reconciliation handlers initialized."
    )
