import asyncio
from pyrogram import filters
from pyrogram.handlers import MessageHandler
from Backend.pyrofork.bot import Userbot
from Backend.helper.settings_manager import SettingsManager
from Backend.helper.global_search import _resolve_channel_ids
from Backend.helper.global_indexer import _process_message
from Backend.logger import LOGGER

async def global_channel_filter(_, __, update):
    settings = SettingsManager.current()
    if not settings.global_search_enabled:
        return False
    target_ids = _resolve_channel_ids(settings.global_search_channels)
    if update.chat and update.chat.id in target_ids:
        if update.video or update.document:
            return True
    return False

global_filter = filters.create(global_channel_filter)

async def live_index_handler(client, message):
    from Backend import db
    if getattr(db, "global_db", None) is None:
        return
        
    chat_id = message.chat.id
    try:
        mid = await _process_message(db, message, chat_id)
        if mid:
            LOGGER.info(f"[LIVE GLOBAL INDEXER] Automatically indexed new file from {chat_id}: {message.id}")
            
            # Update last_id to prevent duplicates on manual sync
            msg_filter_name = "VIDEO" if message.video else "DOCUMENT"
            sync_key = f"sync_{chat_id}_{msg_filter_name}"
            
            # Fetch highest last_id to ensure we only push it forward
            sync_state = await db.global_db["state"].find_one({"_id": sync_key}) or {}
            last_id = sync_state.get("last_id", 0)
            if message.id > last_id:
                await db.global_db["state"].update_one(
                    {"_id": sync_key}, 
                    {"$set": {"last_id": message.id}}, 
                    upsert=True
                )
    except Exception as e:
        LOGGER.error(f"[LIVE GLOBAL INDEXER] Error processing {message.id} in {chat_id}: {e}")

def setup_live_indexer():
    if Userbot is not None:
        handler = MessageHandler(live_index_handler, global_filter)
        Userbot.add_handler(handler)
        LOGGER.info("[LIVE GLOBAL INDEXER] Real-time background watcher initialized.")
