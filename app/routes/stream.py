"""Byte streaming endpoint (/dl/...) with full HTTP Range support."""
import mimetypes
from urllib.parse import quote, unquote

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from app import config
from app import telemetry
from app.logger import LOGGER
from app.security import require_token
from app.streamer import ClientNotConnected, FileNotFoundError_, Streamer
from app.token import decode_payload

router = APIRouter(tags=["stream"])

_streamers: dict = {}


def _get_streamer_for(client) -> Streamer:
    """Return a Streamer bound to a specific client (cached per client)."""
    key = id(client)
    s = _streamers.get(key)
    if s is None or s.client is not client:
        s = Streamer(client)
        _streamers[key] = s
    return s


async def _pick_streamer(chat_id: int):
    """Choose (client, streamer) for a chat.

    Routes BOT_CHANNELS through a bot (round-robin across bots if multiple),
    everything else through the user session. Returns (None, None) if neither
    is usable.
    """
    from app import client as user_mod
    from app import bot_client

    # Bot pool first for its dedicated channels.
    if bot_client.serves_chat(chat_id):
        bot = await bot_client.pick_bot(chat_id)
        if bot is not None:
            return bot, _get_streamer_for(bot)
        # Bots configured but all down — fall through to the user session.

    user = await user_mod.ensure_started()
    if user is None:
        return None, None
    return user, _get_streamer_for(user)


def parse_range(range_header: str, file_size: int):
    if not range_header:
        return 0, file_size - 1
    try:
        value = range_header.replace("bytes=", "").strip()
        start_s, end_s = value.split("-")
        if start_s == "":
            length = int(end_s)
            start = file_size - length
            end = file_size - 1
        elif end_s == "":
            start = int(start_s)
            end = file_size - 1
        else:
            start = int(start_s)
            end = int(end_s)
    except Exception:
        raise HTTPException(status_code=416, detail="Invalid Range header",
                            headers={"Content-Range": f"bytes */{file_size}"})
    start = max(start, 0)
    end = min(end, file_size - 1)
    if end < start:
        raise HTTPException(status_code=416, detail="Requested Range Not Satisfiable",
                            headers={"Content-Range": f"bytes */{file_size}"})
    return start, end


def _content_disposition(file_name: str) -> str:
    ascii_fallback = file_name.encode("ascii", "ignore").decode("ascii").replace('"', "").strip() or "file"
    return f"inline; filename=\"{ascii_fallback}\"; filename*=UTF-8''{quote(file_name, safe='')}"


@router.get("/dl/{token}/{sid}/{name}")
@router.head("/dl/{token}/{sid}/{name}")
async def download(token: str, sid: str, name: str, request: Request):
    require_token(token)

    try:
        payload = decode_payload(sid)
        chat_id = int(payload["chat_id"])
        msg_id = int(payload["msg_id"])
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid stream id")

    # Make sure the appropriate client is connected before touching Telegram.
    # Bot channels route through the bot; everything else through the userbot.
    client, streamer = await _pick_streamer(chat_id)
    if client is None or streamer is None:
        raise HTTPException(
            status_code=503,
            detail="Streaming unavailable: no Telegram client is connected "
                   "(check SESSION_STRING / BOT_TOKEN).",
        )

    try:
        fid = await streamer.file_properties(chat_id, msg_id)
    except ClientNotConnected:
        raise HTTPException(status_code=503, detail="Streaming unavailable: userbot not connected.")
    except FileNotFoundError_:
        raise HTTPException(status_code=404, detail="File not found")
    except Exception as exc:
        # Classify the error for the health dashboard.
        err = str(exc)
        if "AUTH_KEY_DUPLICATED" in err:
            telemetry.bump("auth_key_duplicated")
        elif "wait of" in err or "FLOOD" in err:
            telemetry.bump("flood_waits")
        else:
            telemetry.bump("lookup_failures")
        LOGGER.error("Stream lookup failed for chat=%s msg=%s: %s", chat_id, msg_id, exc)
        raise HTTPException(status_code=502, detail="Telegram file lookup failed")

    file_size = fid.file_size
    if file_size <= 0:
        raise HTTPException(status_code=404, detail="Empty file")

    range_header = request.headers.get("range", "")
    start, end = parse_range(range_header, file_size)
    length = end - start + 1
    is_range = bool(range_header)

    file_name = fid.file_name or unquote(name) or "video.mkv"
    mime = fid.mime_type or mimetypes.guess_type(file_name)[0] or "video/mp4"

    headers = {
        "Content-Type": mime,
        "Content-Disposition": _content_disposition(file_name),
        "Accept-Ranges": "bytes",
        # CRITICAL: never let Cloudflare/Koyeb edge cache a private stream.
        # A public cacheable response makes the CDN serve the WHOLE file and
        # swallow the Range header, which breaks seeking and hangs the player.
        "Cache-Control": "private, no-store",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges",
    }
    status = 200
    if is_range:
        # Bounded range: a fixed Content-Length is safe and enables seeking.
        headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
        headers["Content-Length"] = str(length)
        status = 206

    if request.method == "HEAD":
        # HEAD must report the full resource size so the player can seek.
        headers["Content-Length"] = str(file_size)
        headers["Accept-Ranges"] = "bytes"
        return Response(status_code=200, headers=headers)

    if not is_range:
        # Full-file stream: deliberately OMIT Content-Length so uvicorn uses
        # chunked transfer encoding. Players read ahead and disconnect before
        # consuming a whole multi-GB file; a declared Content-Length would make
        # that normal disconnect crash with
        # "Response content shorter than Content-Length".
        headers.pop("Content-Length", None)

    try:
        is_bot = bool(getattr(client, "bot_token", None))
        parallelism = config.BOT_STREAM_PARALLELISM if is_bot else config.STREAM_PARALLELISM
        prefetch = config.BOT_STREAM_PREFETCH if is_bot else config.STREAM_PREFETCH
        gen = await streamer.stream(
            fid, start, end,
            chat_id=chat_id, message_id=msg_id, request=request,
            parallelism=parallelism, prefetch=prefetch,
        )
    except ClientNotConnected as exc:
        # Media-session setup can fail (e.g. a rate-limited account). Surface a
        # clean 503 instead of hanging or crashing.
        raise HTTPException(status_code=503, detail=f"Streaming unavailable: {exc}")
    return StreamingResponse(gen, status_code=status, headers=headers, media_type=mime)
