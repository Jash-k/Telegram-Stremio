"""HMAC-signed URL-safe tokens for stream references.

Stream URLs embed `chat_id` + `message_id` so a viewer cannot request
arbitrary Telegram files by tampering with the URL. No extra dependencies.
"""
import base64
import hashlib
import hmac
import json
import os


def _secret() -> bytes:
    # Stable across restarts: derived from the API token (or an explicit key).
    key = os.getenv("STREAM_SECRET") or os.getenv("API_TOKEN") or "global-stremio"
    return key.encode()


def encode_payload(payload: dict) -> str:
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    sig = hmac.new(_secret(), raw, hashlib.sha256).hexdigest()
    body = base64.urlsafe_b64encode(raw).decode().rstrip("=")
    return f"{body}.{sig}"


def decode_payload(token: str) -> dict:
    try:
        body, sig = token.rsplit(".", 1)
        raw = base64.urlsafe_b64decode(body + "=" * (-len(body) % 4))
        expected = hmac.new(_secret(), raw, hashlib.sha256).hexdigest()
    except Exception as exc:  # pragma: no cover - malformed input
        raise ValueError("malformed token") from exc
    if not hmac.compare_digest(expected, sig):
        raise ValueError("bad signature")
    return json.loads(raw)
