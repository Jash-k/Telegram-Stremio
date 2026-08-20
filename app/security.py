"""Auth helpers.

Two accepted methods, mirroring the original project but simplified:
  1. Session cookie (set by POST /login) — used by the web panel.
  2. X-Admin-Key header — for curl/API access.
"""
from fastapi import Header, HTTPException, Request

from app import config
from app.passwords import verify_password


def verify_credentials(username: str, password: str) -> bool:
    if not config.ADMIN_USERNAME and not config.ADMIN_PASSWORD:
        # No credentials configured: accept the ADMIN_KEY as a password.
        return bool(config.ADMIN_KEY) and password == config.ADMIN_KEY
    return username == config.ADMIN_USERNAME and verify_password(password, config.ADMIN_PASSWORD)


def is_authenticated(request: Request) -> bool:
    return bool(request.session.get("authenticated"))


def get_current_user(request: Request):
    if is_authenticated(request):
        return request.session.get("username", "admin")
    return None


async def require_auth(request: Request, x_admin_key: str = Header(default="")):
    """Allow session cookie OR admin key header."""
    if is_authenticated(request):
        return True
    if config.ADMIN_KEY and x_admin_key == config.ADMIN_KEY:
        return True
    raise HTTPException(status_code=401, detail="Not authenticated")


def require_token(token: str) -> None:
    if not config.API_TOKEN or token != config.API_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")
