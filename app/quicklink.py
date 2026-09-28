"""v17.1 quick-link: /li <Title [year]> and /livs <Title [year]>.

Forward a file from any channel into your own admin channel (or Saved
Messages), then REPLY to it with the command — the file gets linked straight
to that exact TMDb title, with zero filename guessing. `/livs` lands the file
in the title's Video Songs collection instead. If the album is a media group,
the whole album links in one go. Already-indexed files (even under a wrong
show) are re-linked, not duplicated.

Everything here reuses the panel's proven writer path (`_map_file`) — one code
path for manual linking, whether it comes from the modal or from the chat.
Security: works ONLY in the userbot's Saved Messages or in chats where the
account is owner/admin, and every failure answers with a one-line message
instead of a stack trace.
"""
import re

from .logger import LOGGER

_YEAR_TAIL_RE = re.compile(r"\b(19|20)\d{2}\b\s*$")
_ALBUM_WINDOW = 12  # Telegram albums cap at 10; scan slightly wider


def split_title_year(query: str) -> tuple[str, int | None]:
    q = (query or "").strip()
    m = _YEAR_TAIL_RE.search(q)
    if m:
        return q[: m.start()].strip(), int(m.group(0))
    return q, None


def _media_of(msg):
    return (getattr(msg, "video", None) or getattr(msg, "document", None)
            or getattr(msg, "animation", None))


async def _authorized(client, msg) -> bool:
    """Self-chat, or a chat where this account is creator/administrator."""
    try:
        me = await client.get_me()
        if not me or msg is None or getattr(msg, "chat", None) is None:
            return False
        chat = msg.chat
        if int(chat.id) == int(me.id):          # Saved Messages
            return True
        if getattr(chat, "creator", False):     # channel/group we own
            return True
        try:
            member = await chat.get_member(me.id)
            status = getattr(getattr(member, "status", None), "value",
                             str(getattr(member, "status", "") or ""))
            return str(status).lower().strip(".'") in ("creator", "administrator")
        except Exception:
            return False
    except Exception:
        return False


async def _collect_targets(client, base_msg) -> list:
    """[base_msg] — or the whole album when base_msg is inside a media group."""
    targets, seen = [], set()

    def add(m):
        if m is not None and m.id not in seen:
            seen.add(m.id)
            targets.append(m)

    mgid = getattr(base_msg, "media_group_id", None)
    if mgid:
        try:
            ids = [base_msg.id + d for d in range(-_ALBUM_WINDOW, _ALBUM_WINDOW + 1) if d != 0]
            for m in await client.get_messages(int(base_msg.chat.id), ids) or []:
                if m and getattr(m, "media_group_id", None) == mgid and _media_of(m):
                    add(m)
        except Exception as exc:
            LOGGER.debug("[LINK] album scan failed: %s", exc)
    out = [base_msg] + [t for t in targets if t.id != base_msg.id]
    return [t for t in out if _media_of(t)] or ([base_msg] if _media_of(base_msg) else [])


async def _resolve_title(client_query: str):
    """Return (tmdb_id, media_type, chosen_result) or (None, None, None).

    Accepts: plain title, title + trailing year, `tt1234567`, `tmdb:1234`
    (bare digits too). Title search prefers movie, falls back to TV, and
    prefers a year-exact candidate when one was given.
    """
    from .metadata import (tmdb_find_by_imdb, tmdb_details, tmdb_search_multi)

    q = (client_query or "").strip()
    if not q:
        return None, None, None

    m = re.fullmatch(r"(?i)(imdb:)?tt\d{7,10}", q)
    if m:
        found = await tmdb_find_by_imdb(q.lower().replace("imdb:", ""))
        if found and found.get("id"):
            mt = "series" if (found.get("first_air_date") and not found.get("release_date")) else "movie"
            return int(found["id"]), mt, found
        return None, None, None

    # NOTE: bare digits are NOT treated as ids — "/li 2025" must fail safe,
    # not link TMDb id 2025. Use an explicit prefix for numeric lookups.
    m = re.fullmatch(r"(?i)tmdb:(\d{1,9})", q)
    if m:
        tid = int(m.group(1))
        for mt in ("movie", "tv"):
            details = await tmdb_details(mt, tid)
            if details and details.get("id"):
                return tid, ("movie" if mt == "movie" else "series"), details
        return None, None, None

    title, year = split_title_year(q)
    if len(title) < 2:
        return None, None, None
    for mt, mt_label in (("movie", "movie"), ("tv", "series")):
        cands = await tmdb_search_multi(title, mt, year, limit=5) or []
        if not cands and mt == "tv":
            continue
        if not cands:
            continue
        pick = cands[0]
        if year:
            for c in cands:
                date = str(c.get("release_date") or c.get("first_air_date") or "")
                if date.startswith(str(year)):
                    pick = c
                    break
        if not pick.get("id"):
            continue
        return int(pick["id"]), mt_label, pick
    return None, None, None


async def quicklink(client, cmd_msg, query: str, as_song: bool) -> str:
    """Entry point used by /li and /livs. Returns the reply text."""
    from . import db

    if not await _authorized(client, cmd_msg):
        return "⛔ These commands only work in Saved Messages or a chat you administer."
    if not db.is_connected():
        return "⚠️ Database is offline right now — try again in a minute."

    reply_id = getattr(cmd_msg, "reply_to_message_id", None)
    target = getattr(cmd_msg, "reply_to_message", None)
    if target is None and reply_id:
        try:
            target = await client.get_messages(int(cmd_msg.chat.id), [reply_id])
            target = target[0] if isinstance(target, (list, tuple)) and target else (None if isinstance(target, list) else target)
        except Exception:
            target = None
    if target is None:
        return (f"↩️ Reply to the shared file with /li <name> — e.g. “{'/livs Coolie 2025' if as_song else '/li Coolie 2025'}”."
                if not query else
                "↩️ Reply to the shared file with this command, then the title — e.g. “/li Coolie 2025”.")
    if not _media_of(target):
        return "⏹ That message has no video/document to link. Reply to the FILE itself."

    if not (query or "").strip():
        return (f"Usage: {'/livs' if as_song else '/li'} <title> [year] — or a tt1234567 / tmdb:1234 id.")

    tmdb_id, media_type, picked = await _resolve_title(query)
    if not tmdb_id:
        return (f"❌ No TMDb match for “{query.strip()[:80]}”. Try adding the year "
                "(/li Coolie 2025), or a tt1234567 / tmdb:1234 id.")

    from .routes.admin import _map_file
    from .metadata import tmdb_details
    from .indexer import video_filename, readable_size, global_file_key

    details = await tmdb_details("movie" if media_type == "movie" else "tv", tmdb_id) or {}
    show_title = (details.get("title") or details.get("name")
                  or picked.get("title") or picked.get("name") or query)[:80]
    year = str(details.get("release_date") or details.get("first_air_date") or "")[:4]

    msgs = await _collect_targets(client, target)
    if not msgs:
        msgs = [target]

    chat_id = int(cmd_msg.chat.id)
    linked, failed = 0, 0
    for m in msgs:
        try:
            media = _media_of(m)
            if media is None:
                continue
            file_key = global_file_key(chat_id, int(m.id))
            fname = video_filename(m) or getattr(media, "file_name", None) or (m.caption or f"shared-{m.id}")
            size = int(getattr(media, "file_size", 0) or 0)
            fdoc = await db.col("files").find_one({"_id": file_key}) or {}
            fdoc.update({
                "_id": file_key,
                "chat_id": chat_id,
                "message_id": int(m.id),
                "filename": str(fname),
                "size": size,
                "size_str": readable_size(size),
            })
            # Manual link = truth: never let filename parsing invent episodes
            # for a movie or a song pack. Series keep the parser's S/E.
            se = ({"season": None, "episode_start": None, "episode_end": None}
                  if (as_song or media_type == "movie") else {})
            await _map_file(fdoc, tmdb_id, media_type, as_song, details, se_override=se)
            await db.col("unindexed").delete_one({"_id": file_key})
            linked += 1
        except Exception as exc:
            failed += 1
            LOGGER.warning("[LINK] %s:%s failed: %s", chat_id, getattr(m, "id", "?"), exc)

    if not linked:
        return f"❌ Could not link that file (TMDb said: {show_title}). See server log for the reason."
    try:
        from .cache import invalidate_all
        invalidate_all()
    except Exception:
        pass
    tag = "🎵 video songs" if as_song else media_type
    note = f" · {failed} skipped" if failed else ""
    return (f"✅ Linked {linked} file(s) → {show_title}"
            + (f" ({year})" if year else "") + f" [{tag}]{note}")
