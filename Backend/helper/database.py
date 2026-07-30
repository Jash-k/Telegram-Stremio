import re
import secrets
import string
from asyncio import create_task
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import motor.motor_asyncio
from bson import ObjectId
from pydantic import ValidationError
from pymongo import ASCENDING, DESCENDING

from Backend.config import Telegram
from Backend.helper.encrypt import decode_string, encode_string
from Backend.helper.modal import Episode, MovieSchema, QualityDetail, QualityPart, Season, TVShowSchema
from Backend.helper.settings_manager import SettingsManager
from Backend.helper.task_manager import delete_message
from Backend.logger import LOGGER



def convert_objectid_to_str(document: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in document.items():
        if isinstance(value, ObjectId):
            document[key] = str(value)
        elif isinstance(value, list):
            document[key] = [convert_objectid_to_str(item) if isinstance(item, dict) else item for item in value]
        elif isinstance(value, dict):
            document[key] = convert_objectid_to_str(value)
    return document


class Database:
    def __init__(self, db_name: str = "dbFyvio"):
        self.db_uris = Telegram.DATABASE
        self.db_name = db_name

        if len(self.db_uris) < 2:
            raise ValueError("At least 2 database URIs are required (1 for tracking + 1 for storage).")

        self.clients: Dict[str, motor.motor_asyncio.AsyncIOMotorClient] = {}
        self.dbs: Dict[str, motor.motor_asyncio.AsyncIOMotorDatabase] = {}
        self.current_db_index = 1
        
        self.global_client = None
        self.global_db = None

    async def connect(self):
        try:
            for index, uri in enumerate(self.db_uris):
                client = motor.motor_asyncio.AsyncIOMotorClient(uri)
                db_key = "tracking" if index == 0 else f"storage_{index}"
                self.clients[db_key] = client
                self.dbs[db_key] = client[self.db_name]
                db_type = "Tracking" if index == 0 else f"Storage {index}"
                LOGGER.info(f"Connected to MongoDB {db_type} Database on index {index}")
                
            self.current_db_index = len(self.db_uris) - 1
            
            # Connect to dedicated Global Search Database if specified
            from Backend.helper.settings_manager import SettingsManager
            global_uri = SettingsManager.current().global_database_uri
            if global_uri:
                try:
                    self.global_client = motor.motor_asyncio.AsyncIOMotorClient(global_uri)
                    self.global_db = self.global_client[self.db_name]
                    LOGGER.info("Connected to Dedicated Global Search Cluster!")
                except Exception as ge:
                    LOGGER.error(f"Failed to connect to Global Database URI: {ge}")
        except Exception as e:
            LOGGER.error(f"Error connecting to databases: {e}")
            raise
