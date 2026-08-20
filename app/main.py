"""FastAPI application entry point."""
import asyncio
import secrets
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from starlette.middleware.sessions import SessionMiddleware

from app import __version__, config, db
from app.logger import LOGGER
from app.routes import admin, panel, stream, stremio


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()

    if config.SESSION_STRING:
        from app import client as client_mod

        await client_mod.start()
        from app.live import install

        install(client_mod.client)
    else:
        LOGGER.warning("SESSION_STRING not set — indexing/streaming disabled")

    keepalive_task = None
    if config.BASE_URL:
        from app.keepalive import keepalive_loop

        keepalive_task = asyncio.create_task(keepalive_loop())

    yield

    if keepalive_task:
        keepalive_task.cancel()
    from app import client as client_mod

    await client_mod.stop()
    await db.disconnect()


app = FastAPI(title="Global Stremio", version=__version__, lifespan=lifespan)

# CORS is REQUIRED for Stremio web installs: web.stremio.com fetches the
# manifest (and later catalog/meta/stream JSON) from the browser, so the
# responses must allow cross-origin reads or install fails with "failed to fetch".
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Length", "Content-Range", "Accept-Ranges"],
)

# Session cookie auth (mirrors the original project's panel auth).
app.add_middleware(
    SessionMiddleware,
    secret_key=config.ADMIN_KEY or secrets.token_hex(32),
    max_age=7 * 24 * 3600,
)

app.include_router(stremio.router)
app.include_router(stream.router)
app.include_router(admin.router)
app.include_router(panel.router)


# Redirect unauthenticated browser requests to the login page (as the original does).
@app.exception_handler(401)
async def auth_exception_handler(request, exc):
    if request.url.path.startswith("/api/"):
        from fastapi.responses import JSONResponse

        return JSONResponse({"detail": "Not authenticated"}, status_code=401)
    return RedirectResponse(url="/login", status_code=302)


@app.get("/healthz", include_in_schema=False)
async def healthz():
    return {"ok": True}
