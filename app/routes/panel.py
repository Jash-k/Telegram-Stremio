"""Web pages: login, logout, theme, and the GlobalDB management panel."""
from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app import config
from app.security import (
    get_current_user,
    is_authenticated,
    require_auth,
    verify_credentials,
)
from app.themes import DEFAULT_THEME, THEMES, get_theme

router = APIRouter(include_in_schema=False)
templates = Jinja2Templates(directory="app/templates")


def _base_context(request: Request) -> dict:
    theme_name = request.session.get("theme", DEFAULT_THEME)
    return {
        "request": request,
        "theme": get_theme(theme_name),
        "themes": THEMES,
        "current_theme": theme_name,
    }


@router.get("/admin/global", response_class=HTMLResponse)
async def global_manage(request: Request, _: bool = Depends(require_auth)):
    ctx = _base_context(request)
    ctx["current_user"] = get_current_user(request)
    ctx["has_global_db"] = bool(config.MONGO_URI)
    return templates.TemplateResponse(request, "global_manage.html", ctx)


@router.get("/", response_class=HTMLResponse)
async def home(request: Request, _: bool = Depends(require_auth)):
    return await global_manage(request, _)


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if is_authenticated(request):
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(request, "login.html", _base_context(request))


@router.post("/login")
async def login_post(request: Request, username: str = Form(...), password: str = Form(...)):
    if verify_credentials(username, password):
        request.session["authenticated"] = True
        request.session["username"] = username
        return RedirectResponse(url="/", status_code=302)
    ctx = _base_context(request)
    ctx["error"] = "Invalid credentials"
    return templates.TemplateResponse(request, "login.html", ctx)


@router.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=302)


@router.post("/set-theme")
async def set_theme(request: Request, theme: str = Form(...)):
    if theme in THEMES:
        request.session["theme"] = theme
    return RedirectResponse(url=request.headers.get("referer", "/"), status_code=302)
