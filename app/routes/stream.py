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

_streamer: Streamer = None
_streamer_client = None


def get_streamer() -> Streamer:
    """Return a Streamer bound to the *current* client (rebuild on change)."""
    global _streamer, _streamer_client
    from app.client import client

    if _streamer is None or _streamer_client is not client:
        _streamer = Streamer(client)
        _streamer_client = client
    return _streamer


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

    # Make sure the userbot is connected before touching Telegram. This also
    # recovers a client that disconnected after startup.
    from app.client import ensure_started

    client = await ensure_started()
    if client is None:
        raise HTTPException(
            status_code=503,
            detail="Streaming unavailable: Telegram userbot is not connected "
                   "(check SESSION_STRING — it may be in use elsewhere).",
        )

    streamer = get_streamer()
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
        "Cache-Control": "public, max-age=3600",
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
        gen = await streamer.stream(fid, start, end, chat_id=chat_id, message_id=msg_id, request=request)
    except ClientNotConnected as exc:
        # Media-session setup can fail (e.g. a rate-limited account). Surface a
        # clean 503 instead of hanging or crashing.
        raise HTTPException(status_code=503, detail=f"Streaming unavailable: {exc}")
    return StreamingResponse(gen, status_code=status, headers=headers, media_type=mime)
