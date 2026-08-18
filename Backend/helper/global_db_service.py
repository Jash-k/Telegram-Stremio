from typing import Iterable, Optional


def global_file_key(chat_id: int, message_id: int) -> str:
    return f"{int(chat_id)}_{int(message_id)}"


def first_int(value) -> Optional[int]:
    """Normalize PTN scalar/list values to one integer."""
    if isinstance(value, (list, tuple, set)):
        for item in value:
            normalized = first_int(item)
            if normalized is not None:
                return normalized
        return None
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def episode_bounds(value) -> tuple[Optional[int], Optional[int]]:
    """Return inclusive episode bounds from a scalar or PTN list value."""
    if isinstance(value, (list, tuple, set)):
        numbers = [number for item in value if (number := first_int(item)) is not None]
        return (min(numbers), max(numbers)) if numbers else (None, None)
    number = first_int(value)
    return number, number


def normalize_global_file_fields(file_doc: dict, indexed_at=None) -> dict:
    """Normalize legacy scalar/list file coordinates for bulk migration."""
    start_low, start_high = episode_bounds(file_doc.get("episode_start"))
    end_low, end_high = episode_bounds(file_doc.get("episode_end"))
    bounds = [
        value
        for value in (start_low, start_high, end_low, end_high)
        if value is not None
    ]
    normalized = {
        "chat_id": first_int(file_doc.get("chat_id")),
        "message_id": first_int(file_doc.get("message_id")),
        "season": first_int(file_doc.get("season")),
        "episode_start": min(bounds) if bounds else None,
        "episode_end": max(bounds) if bounds else None,
    }
    normalized["indexed_at"] = file_doc.get("indexed_at") or indexed_at
    return normalized


def languages_from_filename(filename: str) -> list[str]:
    """Extract the language labels used by GlobalDB catalog filters."""
    import re

    language_map = {
        "tam": "Tamil",
        "tamil": "Tamil",
        "tel": "Telugu",
        "telugu": "Telugu",
        "hin": "Hindi",
        "hindi": "Hindi",
        "mal": "Malayalam",
        "malayalam": "Malayalam",
        "kan": "Kannada",
        "kannada": "Kannada",
        "eng": "English",
        "english": "English",
        "multi": "Multi",
    }
    value = str(filename or "").lower()
    return sorted(
        {label for token, label in language_map.items() if re.search(rf"\b{token}\b", value)}
    )


def build_global_file_query(
    meta_id: str,
    season: Optional[int] = None,
    episode: Optional[int] = None,
) -> dict:
    """Build a GlobalDB stream query, including full-season pack entries."""
    query: dict = {"meta_id": meta_id}
    normalized_season = first_int(season)
    normalized_episode = first_int(episode)
    if normalized_season is not None:
        query["season"] = normalized_season
    if normalized_season is not None and normalized_episode is not None:
        query["$or"] = [
            {
                "episode_start": {"$lte": normalized_episode},
                "episode_end": {"$gte": normalized_episode},
            },
            {"episode_start": None, "episode_end": None},
        ]
    return query


async def resolve_global_meta(global_db, content_id: str, tmdb_id=None) -> Optional[dict]:
    """Resolve a GlobalDB meta document from TMDb, IMDb, or an alias."""
    if global_db is None:
        return None

    candidates = []
    content_id = str(content_id or "").strip()
    if content_id:
        candidates.append(content_id)
    if tmdb_id not in (None, ""):
        canonical_tmdb = f"tmdb:{tmdb_id}"
        if canonical_tmdb not in candidates:
            candidates.append(canonical_tmdb)

    if candidates:
        meta = await global_db["meta"].find_one({"_id": {"$in": candidates}})
        if meta:
            return meta

    if content_id:
        return await global_db["meta"].find_one(
            {"$or": [{"imdb_id": content_id}, {"aliases": content_id}]}
        )
    return None


async def remove_global_file_references(
    global_db,
    references: Iterable[tuple[int, int]],
) -> int:
    """Remove stale file/unindexed rows and metadata left with no files."""
    if global_db is None:
        return 0

    pairs = {(int(chat_id), int(message_id)) for chat_id, message_id in references}
    if not pairs:
        return 0

    file_ids = [global_file_key(chat_id, message_id) for chat_id, message_id in pairs]
    pair_queries = [
        {"chat_id": {"$in": [chat_id, str(chat_id)]}, "message_id": message_id}
        for chat_id, message_id in pairs
    ]
    selector = {"$or": [{"_id": {"$in": file_ids}}, *pair_queries]}

    existing = await global_db["files"].find(selector, {"meta_id": 1}).to_list(None)
    meta_ids = {row.get("meta_id") for row in existing if row.get("meta_id")}

    deleted = await global_db["files"].delete_many(selector)
    await global_db["unindexed"].delete_many(selector)

    for meta_id in meta_ids:
        if not await global_db["files"].find_one({"meta_id": meta_id}, {"_id": 1}):
            await global_db["meta"].delete_one({"_id": meta_id})

    return int(getattr(deleted, "deleted_count", 0) or 0)


async def remove_global_file_reference(global_db, chat_id: int, message_id: int) -> int:
    return await remove_global_file_references(global_db, [(chat_id, message_id)])
