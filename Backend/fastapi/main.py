import asyncio
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from Backend import __version__
from Backend.fastapi.routes.api_routes import (
    add_custom_catalog_item_api,
    add_subscription_plan_api,
    apply_media_rescan_api,
    assign_plan_api,
    auto_catalog_sync_status_api,
    auto_sync_custom_catalogs_api,
    cancel_dbcheck_api,
    cancel_duplicate_check_api,
    cancel_scan_api,
    clear_cache_api,
    clear_stream_analytics_api,
    create_custom_catalog_api,
    create_token_api,
    grant_lifetime_api,
    set_token_lifetime_api,
    set_token_expiry_api,
    subscription_preflight_api,
    backfill_subscriber_names_api,
    dbcheck_status_api,
    duplicate_check_status_api,
    delete_custom_catalog_api,
    delete_media_api,
    delete_movie_quality_api,
    delete_request_api,
    delete_subscription_plan_api,
    export_config_api,
    import_config_api,
    delete_tv_episode_api,
    delete_tv_quality_api,
    delete_tv_season_api,
    download_logs_api,
    get_admin_stats_api,
    get_db_stats_api,
    get_all_subscribers_api,
    get_all_tokens_api,
    get_auto_catalog_settings_api,
    get_custom_catalog_items_api,
    get_dead_links_api,
    get_media_visibility_api,
    get_requests_api,
    request_popular_api,
    request_search_api,
    request_submit_api,
    get_stream_analytics_api,
    get_subscription_plans_api,
    get_settings_api,
    get_logs_api,
    get_manual_session_api,
    get_system_stats_api,
    get_tools_channels_api,
    bot_admin_scan_api,
    bot_admin_apply_api,
    bot_admin_apply_status_api,
    clear_manual_session_api,
    search_manual_session_api,
    set_manual_session_api,
    health_api,
    health_report_api,
    setup_status_api,
    link_token_user_api,
    list_custom_catalogs_api,
    list_media_api,
    manage_subscriber_api,
    manual_add_media_api,
    list_manual_add_catalogs_api,
    resolve_manual_metadata_api,
    purge_dead_links_api,
    purge_duplicates_api,
    remove_custom_catalog_item_api,
    resolve_telegram_api,
    resolve_subtitle_api,
    list_subtitle_languages_api,
    list_subtitles_api,
    add_subtitles_api,
    remove_subtitle_api,
    restart_app_api,
    revoke_token_api,
    scan_status_api,
    search_catalog_media_api,
    set_media_visibility_api,
    search_media_rescan_api,
    speed_test_api,
    speed_test_stream_api,
    start_dbcheck_api,
    start_duplicate_check_api,
    start_scan_api,
    update_auto_catalog_settings_api,
    update_custom_catalog_api,
    update_media_api,
    update_request_api,
    update_settings_api,
    update_subscription_plan_api,
    update_token_limits_api,
)
from Backend.fastapi.routes.stream_routes import decay_client_failures
from Backend.fastapi.routes.stream_routes import router as stream_router
from Backend.fastapi.routes.stremio_routes import router as stremio_router
from Backend.fastapi.routes.template_routes import (
    admin_access_page,
    admin_dashboard_page,
    admin_requests_page,
    admin_subscriptions_page,
    public_request_page,
    custom_catalogs_page,
    global_manage_page,
    dashboard_page,
    edit_media_page,
    login_page,
    login_post,
    logout,
    media_management_page,
    public_status_page,
    settings_page,
    set_theme,
    stremio_guide_page,
    tools_page,
)
from Backend.fastapi.security.credentials import require_auth
from Backend.pyrofork.bot import work_loads_summary

_CLEANUP_RUNNING = False
_MIGRATE_RUNNING = False

templates = Jinja2Templates(directory="Backend/fastapi/templates")

app = FastAPI(
    title="Telegram Stremio Media Server",
    description="A powerful, self-hosted Telegram Stremio Media Server built with FastAPI, MongoDB, and PyroFork seamlessly integrated with Stremio for automated media streaming and discovery.",
    version=__version__
)

#----- Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

try:
    app.mount("/static", StaticFiles(directory="Backend/fastapi/static"), name="static")
except Exception:
    pass


@app.on_event("startup")
async def _startup():
    asyncio.create_task(decay_client_failures())


#----- Streaming and Stremio routers
app.include_router(stream_router)
app.include_router(stremio_router)


#----- Public routes (no authentication)
@app.get("/login", response_class=HTMLResponse)
async def login_get(request: Request):
    return await login_page(request)

@app.post("/login", response_class=HTMLResponse)
async def login_post_route(request: Request, username: str = Form(...), password: str = Form(...)):
    return await login_post(request, username, password)

@app.get("/logout")
async def logout_route(request: Request):
    return await logout(request)

@app.post("/set-theme")
async def set_theme_route(request: Request, theme: str = Form(...)):
    return await set_theme(request, theme)

@app.get("/status", response_class=HTMLResponse)
async def public_status(request: Request):
    return await public_status_page(request)

@app.get("/stremio", response_class=HTMLResponse)
async def stremio_guide(request: Request):
    return await stremio_guide_page(request)


#----- Protected routes (authentication required)
@app.get("/", response_class=HTMLResponse)
async def root(request: Request, _: bool = Depends(require_auth)):
    return await dashboard_page(request, _)

@app.get("/admin/dashboard", response_class=HTMLResponse)
async def admin_dashboard(request: Request, _: bool = Depends(require_auth)):
    return await admin_dashboard_page(request, _)

@app.get("/media/manage", response_class=HTMLResponse)
async def media_management(request: Request, media_type: str = "movie", custom: bool = False, _: bool = Depends(require_auth)):
    return await media_management_page(request, media_type, custom, _)

@app.get("/catalogs", response_class=HTMLResponse)
async def custom_catalogs(request: Request, _: bool = Depends(require_auth)):
    return await custom_catalogs_page(request, _)

@app.get("/admin/global", response_class=HTMLResponse)
async def global_manage(request: Request, _: bool = Depends(require_auth)):
    return await global_manage_page(request, _)

@app.get("/media/edit", response_class=HTMLResponse)
async def edit_media(request: Request, tmdb_id: int, db_index: int, media_type: str, _: bool = Depends(require_auth)):
    return await edit_media_page(request, tmdb_id, db_index, media_type, _)

@app.get("/api/media/list")
async def list_media(
    media_type: str = Query("movie", regex="^(movie|tv)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(24, ge=1, le=100),
    search: str = Query("", max_length=100),
    custom: bool = Query(False),
    _: bool = Depends(require_auth)
):
    return await list_media_api(media_type, page, page_size, search, custom)

@app.delete("/api/media/delete")
async def delete_media(tmdb_id: int, db_index: int, media_type: str, _: bool = Depends(require_auth)):
    return await delete_media_api(tmdb_id, db_index, media_type)

@app.put("/api/media/update")
async def update_media(request: Request, tmdb_id: int, db_index: int, media_type: str, _: bool = Depends(require_auth)):
    return await update_media_api(request, tmdb_id, db_index, media_type)

@app.delete("/api/media/delete-quality")
async def delete_movie_quality(tmdb_id: int, db_index: int, id: str, _: bool = Depends(require_auth)):
    return await delete_movie_quality_api(tmdb_id, db_index, id)

@app.delete("/api/media/delete-tv-quality")
async def delete_tv_quality(tmdb_id: int, db_index: int, season: int, episode: int, id: str, _: bool = Depends(require_auth)):
    return await delete_tv_quality_api(tmdb_id, db_index, season, episode, id)

@app.delete("/api/media/delete-tv-episode")
async def delete_tv_episode(tmdb_id: int, db_index: int, season: int, episode: int, _: bool = Depends(require_auth)):
    return await delete_tv_episode_api(tmdb_id, db_index, season, episode)

@app.delete("/api/media/delete-tv-season")
async def delete_tv_season(tmdb_id: int, db_index: int, season: int, _: bool = Depends(require_auth)):
    return await delete_tv_season_api(tmdb_id, db_index, season)

@app.get("/api/system/workloads")
async def get_workloads(_: bool = Depends(require_auth)):
    try:
        return {"loads": work_loads_summary()}
    except Exception:
        return {"loads": {}}

@app.post("/api/tokens")
async def create_token(payload: dict, _: bool = Depends(require_auth)):
    return await create_token_api(payload)

@app.put("/api/tokens/{token}")
async def update_token(token: str, payload: dict, _: bool = Depends(require_auth)):
    return await update_token_limits_api(token, payload)

@app.delete("/api/tokens/{token}")
async def revoke_token(token: str, _: bool = Depends(require_auth)):
    return await revoke_token_api(token)

@app.get("/api/system/stats")
async def get_system_stats(_: bool = Depends(require_auth)):
    return await get_system_stats_api()

@app.get("/api/admin/system-stats")
async def admin_system_stats(_: bool = Depends(require_auth)):
    return await get_admin_stats_api()

@app.post("/api/admin/clear-cache")
async def clear_cache(_: bool = Depends(require_auth)):
    return await clear_cache_api()

@app.get("/api/admin/dead-links")
async def get_dead_links(_: bool = Depends(require_auth)):
    return await get_dead_links_api()

@app.get("/api/admin/stream-analytics")
async def get_stream_analytics(_: bool = Depends(require_auth)):
    return await get_stream_analytics_api()

@app.post("/api/admin/clear-analytics")
async def clear_analytics(_: bool = Depends(require_auth)):
    return await clear_stream_analytics_api()

@app.get("/admin/subscriptions", response_class=HTMLResponse)
async def admin_subscriptions(request: Request, _: bool = Depends(require_auth)):
    return await admin_subscriptions_page(request, _)

@app.get("/api/admin/subscriptions/plans")
async def get_subscription_plans(_: bool = Depends(require_auth)):
    return await get_subscription_plans_api()

@app.post("/api/admin/subscriptions/plans")
async def add_subscription_plan(payload: dict, _: bool = Depends(require_auth)):
    return await add_subscription_plan_api(payload)

@app.put("/api/admin/subscriptions/plans/{plan_id}")
async def update_subscription_plan(plan_id: str, payload: dict, _: bool = Depends(require_auth)):
    return await update_subscription_plan_api(plan_id, payload)

@app.delete("/api/admin/subscriptions/plans/{plan_id}")
async def delete_subscription_plan(plan_id: str, _: bool = Depends(require_auth)):
    return await delete_subscription_plan_api(plan_id)

@app.get("/api/admin/subscriptions/users")
async def get_subscribers(_: bool = Depends(require_auth)):
    return await get_all_subscribers_api()

@app.post("/api/admin/subscriptions/users/{user_id}/manage")
async def manage_subscriber(user_id: int, payload: dict, _: bool = Depends(require_auth)):
    return await manage_subscriber_api(user_id, payload)


#----- Access management
@app.get("/admin/access", response_class=HTMLResponse)
async def admin_access(request: Request, _: bool = Depends(require_auth)):
    return await admin_access_page(request, _)

@app.get("/api/admin/access/tokens")
async def get_access_tokens(_: bool = Depends(require_auth)):
    return await get_all_tokens_api()

@app.delete("/api/admin/access/tokens/{token}")
async def delete_access_token(token: str, _: bool = Depends(require_auth)):
    return await revoke_token_api(token)

@app.post("/api/admin/access/users/{user_id}/assign-plan")
async def assign_access_plan(user_id: int, payload: dict, _: bool = Depends(require_auth)):
    days = int(payload.get("days", 0))
    return await assign_plan_api(user_id, days)

@app.patch("/api/admin/access/tokens/{token}/link-user")
async def link_token_to_user(token: str, payload: dict, _: bool = Depends(require_auth)):
    user_id = int(payload.get("user_id", 0))
    if not user_id:
        raise HTTPException(status_code=400, detail="user_id is required.")
    return await link_token_user_api(token, user_id)

@app.patch("/api/admin/access/tokens/{token}/lifetime")
async def set_token_lifetime(token: str, payload: dict, _: bool = Depends(require_auth)):
    return await set_token_lifetime_api(token, payload)

@app.post("/api/admin/access/tokens/{token}/expiry")
async def set_token_expiry(token: str, payload: dict, _: bool = Depends(require_auth)):
    return await set_token_expiry_api(token, payload)

@app.post("/api/admin/access/grant-lifetime")
async def grant_lifetime(_: bool = Depends(require_auth)):
    return await grant_lifetime_api()

@app.get("/api/admin/subscriptions/preflight")
async def subscription_preflight(_: bool = Depends(require_auth)):
    return await subscription_preflight_api()

@app.post("/api/admin/subscriptions/backfill-names")
async def backfill_subscriber_names(_: bool = Depends(require_auth)):
    return await backfill_subscriber_names_api()


#----- Public content request page (no auth)
@app.get("/request", response_class=HTMLResponse)
async def public_request(request: Request):
    return await public_request_page(request)

@app.get("/api/request/search")
async def request_search(q: str = Query("")):
    return await request_search_api(q)

@app.get("/api/request/popular")
async def request_popular():
    return await request_popular_api()

@app.post("/api/request/submit")
async def request_submit(payload: dict, request: Request):
    client_ip = request.client.host if request.client else None
    return await request_submit_api(payload, client_ip)


#----- Admin content requests
@app.get("/admin/requests", response_class=HTMLResponse)
async def admin_requests(request: Request, _: bool = Depends(require_auth)):
    return await admin_requests_page(request, _)

@app.get("/api/admin/requests")
async def get_requests(_: bool = Depends(require_auth)):
    return await get_requests_api()

@app.patch("/api/admin/requests/{request_id}")
async def update_request(request_id: str, payload: dict, _: bool = Depends(require_auth)):
    return await update_request_api(request_id, payload)

@app.delete("/api/admin/requests/{request_id}")
async def delete_request_route(request_id: str, _: bool = Depends(require_auth)):
    return await delete_request_api(request_id)

@app.get("/api/system/speedtest")
async def speed_test(
    quality_id: str = Query(...),
    tmdb_id: int = Query(...),
    db_index: int = Query(...),
    media_type: str = Query(...),
    _: bool = Depends(require_auth)
):
    return await speed_test_api(quality_id, tmdb_id, db_index, media_type)

@app.get("/api/system/speedtest/stream")
async def speed_test_stream(
    quality_id: str = Query(...),
    tmdb_id: int = Query(...),
    db_index: int = Query(...),
    media_type: str = Query(...),
    _: bool = Depends(require_auth)
):
    return await speed_test_stream_api(quality_id, tmdb_id, db_index, media_type)

@app.get("/api/media/rescan/search")
async def search_media_rescan(
    media_type: str,
    query: str,
    year: int | None = None,
    _: bool = Depends(require_auth)
):
    return await search_media_rescan_api(media_type, query, year)

@app.post("/api/media/rescan/apply")
async def apply_media_rescan(
    request: Request,
    tmdb_id: int,
    db_index: int,
    media_type: str,
    _: bool = Depends(require_auth)
):
    return await apply_media_rescan_api(request, tmdb_id, db_index, media_type)


#----- Manual add (custom movie/tv/season/episode/stream)
@app.post("/api/media/resolve-telegram")
async def resolve_telegram(payload: dict, _: bool = Depends(require_auth)):
    return await resolve_telegram_api(payload)

@app.post("/api/media/manual-add")
async def manual_add_media(payload: dict, _: bool = Depends(require_auth)):
    return await manual_add_media_api(payload)

@app.get("/api/media/manual-add/catalogs")
async def manual_add_catalogs(_: bool = Depends(require_auth)):
    return await list_manual_add_catalogs_api()

@app.get("/api/media/manual-add/resolve-meta")
async def manual_add_resolve_meta(media_type: str, selected_id: str, _: bool = Depends(require_auth)):
    return await resolve_manual_metadata_api(media_type, selected_id)


#----- Manual subtitle management
@app.get("/api/media/subtitles/languages")
async def subtitle_languages(_: bool = Depends(require_auth)):
    return list_subtitle_languages_api()

@app.get("/api/media/subtitles")
async def list_subtitles(media_type: str, tmdb_id: int, db_index: int, _: bool = Depends(require_auth)):
    return await list_subtitles_api(media_type, tmdb_id, db_index)

@app.post("/api/media/subtitles/resolve")
async def resolve_subtitle(payload: dict, _: bool = Depends(require_auth)):
    return await resolve_subtitle_api(payload)

@app.post("/api/media/subtitles/add")
async def add_subtitles(payload: dict, _: bool = Depends(require_auth)):
    return await add_subtitles_api(payload)

@app.post("/api/media/subtitles/remove")
async def remove_subtitle_route(payload: dict, _: bool = Depends(require_auth)):
    return await remove_subtitle_api(payload)


#----- Custom catalog management
@app.get("/api/custom-catalogs")
async def list_custom_catalogs(
    tmdb_id: int | None = None,
    db_index: int | None = None,
    media_type: str | None = None,
    _: bool = Depends(require_auth)
):
    return await list_custom_catalogs_api(tmdb_id, db_index, media_type)

@app.post("/api/custom-catalogs")
async def create_custom_catalog(payload: dict, _: bool = Depends(require_auth)):
    return await create_custom_catalog_api(payload)

@app.put("/api/custom-catalogs/{catalog_id}")
async def update_custom_catalog(catalog_id: str, payload: dict, _: bool = Depends(require_auth)):
    return await update_custom_catalog_api(catalog_id, payload)

@app.delete("/api/custom-catalogs/{catalog_id}")
async def delete_custom_catalog(catalog_id: str, _: bool = Depends(require_auth)):
    return await delete_custom_catalog_api(catalog_id)

@app.post("/api/custom-catalogs/media-visibility")
async def set_media_visibility(payload: dict, _: bool = Depends(require_auth)):
    return await set_media_visibility_api(payload)

@app.get("/api/custom-catalogs/media-visibility")
async def get_media_visibility(
    tmdb_id: int,
    db_index: int,
    media_type: str = Query("movie", regex="^(movie|tv|series)$"),
    _: bool = Depends(require_auth)
):
    return await get_media_visibility_api(tmdb_id, db_index, media_type)

@app.get("/api/custom-catalogs/search-media")
async def search_catalog_media(
    query: str,
    media_type: str = Query("movie", regex="^(movie|tv)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(12, ge=1, le=50),
    _: bool = Depends(require_auth)
):
    return await search_catalog_media_api(query, media_type, page, page_size)

@app.post("/api/custom-catalogs/auto-sync")
async def auto_sync_custom_catalogs(
    force_refresh: bool = Query(False),
    _: bool = Depends(require_auth)
):
    return await auto_sync_custom_catalogs_api(force_refresh)

@app.get("/api/custom-catalogs/auto-sync/status")
async def auto_catalog_sync_status(_: bool = Depends(require_auth)):
    return await auto_catalog_sync_status_api()

@app.get("/api/custom-catalogs/auto-sync/settings")
async def get_auto_catalog_settings_route(_: bool = Depends(require_auth)):
    return await get_auto_catalog_settings_api()

@app.put("/api/custom-catalogs/auto-sync/settings")
async def update_auto_catalog_settings_route(payload: dict, _: bool = Depends(require_auth)):
    return await update_auto_catalog_settings_api(payload)

@app.get("/api/custom-catalogs/{catalog_id}/items")
async def get_custom_catalog_items(
    catalog_id: str,
    media_type: str | None = Query(None, regex="^(movie|tv)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(24, ge=1, le=100),
    _: bool = Depends(require_auth)
):
    return await get_custom_catalog_items_api(catalog_id, media_type, page, page_size)

@app.post("/api/custom-catalogs/{catalog_id}/items")
async def add_custom_catalog_item(catalog_id: str, payload: dict, _: bool = Depends(require_auth)):
    return await add_custom_catalog_item_api(catalog_id, payload)

@app.delete("/api/custom-catalogs/{catalog_id}/items")
async def remove_custom_catalog_item(
    catalog_id: str,
    tmdb_id: int,
    db_index: int,
    media_type: str = Query("movie", regex="^(movie|tv)$"),
    _: bool = Depends(require_auth)
):
    return await remove_custom_catalog_item_api(catalog_id, tmdb_id, db_index, media_type)


#----- Settings
@app.get("/admin/settings", response_class=HTMLResponse)
async def admin_settings(request: Request, _: bool = Depends(require_auth)):
    return await settings_page(request, _)

@app.get("/api/admin/settings")
async def get_settings(_: bool = Depends(require_auth)):
    return await get_settings_api()

@app.put("/api/admin/settings")
async def update_settings(payload: dict, _: bool = Depends(require_auth)):
    return await update_settings_api(payload)


#----- System & Maintenance (WebUI replacement for /stats, /log, /restart bot commands)
@app.get("/api/admin/stats")
async def admin_db_stats(_: bool = Depends(require_auth)):
    return await get_db_stats_api()

@app.get("/api/admin/health")
async def admin_health(_: bool = Depends(require_auth)):
    return await health_api()

@app.get("/api/admin/health/report")
async def admin_health_report(fresh: bool = Query(False), _: bool = Depends(require_auth)):
    return await health_report_api(force=fresh)

@app.get("/api/admin/setup-status")
async def admin_setup_status(_: bool = Depends(require_auth)):
    return await setup_status_api()

@app.get("/api/admin/backup/export")
async def admin_backup_export(_: bool = Depends(require_auth)):
    from fastapi.responses import JSONResponse
    data = await export_config_api()
    return JSONResponse(
        content=data,
        headers={"Content-Disposition": 'attachment; filename="telegram-stremio-backup.json"'},
    )

@app.post("/api/admin/backup/import")
async def admin_backup_import(payload: dict, _: bool = Depends(require_auth)):
    return await import_config_api(payload)

@app.get("/api/admin/logs")
async def admin_logs(lines: int = Query(300, ge=1, le=2000), _: bool = Depends(require_auth)):
    return await get_logs_api(lines)

@app.get("/api/admin/logs/download")
async def admin_logs_download(_: bool = Depends(require_auth)):
    return await download_logs_api()

@app.post("/api/admin/restart")
async def admin_restart(_: bool = Depends(require_auth)):
    return await restart_app_api()


#----- Tools (WebUI replacement for /scan, /rescan, /dbcheck bot commands)
@app.get("/admin/tools", response_class=HTMLResponse)
async def admin_tools(request: Request, _: bool = Depends(require_auth)):
    return await tools_page(request, _)

@app.get("/api/admin/tools/channels")
async def tools_channels(_: bool = Depends(require_auth)):
    return await get_tools_channels_api()

@app.get("/api/admin/tools/bot-admin/scan")
async def tools_bot_admin_scan(_: bool = Depends(require_auth)):
    return await bot_admin_scan_api()

@app.post("/api/admin/tools/bot-admin/apply")
async def tools_bot_admin_apply(payload: dict, _: bool = Depends(require_auth)):
    return await bot_admin_apply_api(payload)

@app.get("/api/admin/tools/bot-admin/apply/status")
async def tools_bot_admin_apply_status(_: bool = Depends(require_auth)):
    return await bot_admin_apply_status_api()

@app.get("/api/admin/tools/manual-session")
async def tools_manual_session_get(_: bool = Depends(require_auth)):
    return await get_manual_session_api()

@app.get("/api/admin/tools/manual-session/search")
async def tools_manual_session_search(query: str = Query(""), _: bool = Depends(require_auth)):
    return await search_manual_session_api(query)

@app.post("/api/admin/tools/manual-session")
async def tools_manual_session_set(payload: dict, _: bool = Depends(require_auth)):
    return await set_manual_session_api(payload)

@app.delete("/api/admin/tools/manual-session")
async def tools_manual_session_clear(_: bool = Depends(require_auth)):
    return await clear_manual_session_api()

@app.post("/api/admin/tools/scan/start")
async def tools_scan_start(payload: dict, _: bool = Depends(require_auth)):
    return await start_scan_api(payload)

@app.post("/api/admin/tools/scan/cancel")
async def tools_scan_cancel(_: bool = Depends(require_auth)):
    return await cancel_scan_api()

@app.get("/api/admin/tools/scan/status")
async def tools_scan_status(_: bool = Depends(require_auth)):
    return await scan_status_api()

@app.post("/api/admin/tools/dbcheck/start")
async def tools_dbcheck_start(_: bool = Depends(require_auth)):
    return await start_dbcheck_api()

@app.post("/api/admin/tools/dbcheck/cancel")
async def tools_dbcheck_cancel(_: bool = Depends(require_auth)):
    return await cancel_dbcheck_api()

@app.get("/api/admin/tools/dbcheck/status")
async def tools_dbcheck_status(_: bool = Depends(require_auth)):
    return await dbcheck_status_api()

@app.post("/api/admin/tools/dead-links/purge")
async def tools_purge_dead_links(payload: dict | None = None, _: bool = Depends(require_auth)):
    return await purge_dead_links_api(payload)

@app.post("/api/admin/tools/duplicates/start")
async def tools_duplicates_start(_: bool = Depends(require_auth)):
    return await start_duplicate_check_api()

@app.post("/api/admin/tools/duplicates/cancel")
async def tools_duplicates_cancel(_: bool = Depends(require_auth)):
    return await cancel_duplicate_check_api()

@app.get("/api/admin/tools/duplicates/status")
async def tools_duplicates_status(_: bool = Depends(require_auth)):
    return await duplicate_check_status_api()

@app.post("/api/admin/tools/duplicates/purge")
async def tools_duplicates_purge(payload: dict | None = None, _: bool = Depends(require_auth)):
    return await purge_duplicates_api(payload)


@app.exception_handler(401)
async def auth_exception_handler(request: Request, exc):
    return RedirectResponse(url="/login", status_code=302)

@app.get("/api/admin/global/stats")
async def global_stats(_: bool = Depends(require_auth)):
    from Backend import db
    try:
        if getattr(db, "global_db", None) is None:
            return {"files_count": 0, "catalogs": [], "recent_files": []}
        files_count = await db.global_db["files"].count_documents({})
        
        counts_cursor = db.global_db["meta"].aggregate([{"$group": {"_id": "$catalog", "count": {"$sum": 1}}}])
        counts = {c["_id"]: c["count"] async for c in counts_cursor}
        
        cats_cursor = db.global_db["catalogs"].find()
        catalogs = []
        async for c in cats_cursor:
            c["count"] = counts.get(c["_id"], 0)
            catalogs.append(c)
            
        files_cursor = db.global_db["files"].find().sort("_id", -1).limit(50)
        files = [f async for f in files_cursor]
        return {
            "files_count": files_count,
            "catalogs": catalogs,
            "recent_files": files
        }
    except Exception as e:
        return {"files_count": "ERROR", "catalogs": [], "recent_files": []}

@app.get("/api/admin/global/files/catalog/{catalog_id}")
async def get_global_catalog_files(catalog_id: str, page: int = 1, _: bool = Depends(require_auth)):
    from Backend import db
    try:
        if getattr(db, "global_db", None) is None:
            return {"items": [], "total_pages": 1}
            
        page_size = 30
        skip = (page - 1) * page_size
        
        query = {"catalog": catalog_id}
        total = await db.global_db["meta"].count_documents(query)
        cursor = db.global_db["meta"].find(query).sort([("updated_at", -1), ("_id", -1)]).skip(skip).limit(page_size)
        items = [doc async for doc in cursor]
        
        # Fetch file counts for each meta
        for item in items:
            item["file_count"] = await db.global_db["files"].count_documents({"meta_id": item["_id"]})
        
        return {
            "items": items,
            "total_pages": (total + page_size - 1) // page_size or 1,
            "total_items": total
        }
    except Exception as e:
        return {"items": [], "total_pages": 1, "error": str(e)}

@app.delete("/api/admin/global/meta/{meta_id}")
async def delete_global_meta(meta_id: str, _: bool = Depends(require_auth)):
    from Backend import db
    if getattr(db, "global_db", None) is not None:
        await db.global_db["meta"].delete_one({"_id": meta_id})
        await db.global_db["files"].delete_many({"meta_id": meta_id})
    return {"status": "success"}

@app.delete("/api/admin/global/catalogs/{cat_id}")
async def delete_global_cat(cat_id: str, _: bool = Depends(require_auth)):
    from Backend import db
    if getattr(db, "global_db", None) is not None:
        await db.global_db["catalogs"].delete_one({"_id": cat_id})
    return {"status": "success"}

@app.delete("/api/admin/global/files/{file_id}")
async def delete_global_file(file_id: str, _: bool = Depends(require_auth)):
    from Backend import db
    from Backend.helper.global_db_service import remove_global_file_reference

    if getattr(db, "global_db", None) is not None:
        file_doc = await db.global_db["files"].find_one(
            {"_id": file_id}, {"chat_id": 1, "message_id": 1}
        )
        if file_doc:
            await remove_global_file_reference(
                db.global_db, file_doc["chat_id"], file_doc["message_id"]
            )
        else:
            await db.global_db["unindexed"].delete_one({"_id": file_id})
    return {"status": "success"}

@app.delete("/api/admin/global/files")
async def delete_all_global_files(_: bool = Depends(require_auth)):
    from Backend import db
    if getattr(db, "global_db", None) is not None:
        await db.global_db["files"].delete_many({})
        await db.global_db["meta"].delete_many({})
    return {"status": "success"}

@app.post("/api/admin/global/index/start")
async def start_global_index(request: Request, _: bool = Depends(require_auth)):
    from Backend.helper import global_indexer

    try:
        payload = await request.json()
        target_chat_id = payload.get("chat_id")
        if target_chat_id is not None:
            target_chat_id = int(target_chat_id)
        force_historic = payload.get("force_historic", False)
    except:
        target_chat_id = None
        force_historic = False
        
    from Backend import db
    started = await global_indexer.schedule_global_indexer(
        db, target_chat_id, force_historic
    )
    if not started:
        return {
            "status": "error",
            "message": "Global indexer is disabled or already running on another replica.",
        }
    return {"status": "success"}

@app.post("/api/admin/global/index/stop")
async def stop_global_index(_: bool = Depends(require_auth)):
    from Backend import db
    from Backend.helper import global_indexer

    requested = await global_indexer.request_global_indexer_stop(db)
    if requested:
        return {"status": "success", "message": "Stop requested."}
    return {"status": "error", "message": "Not running"}

@app.get("/api/admin/global/index/status")
async def status_global_index(_: bool = Depends(require_auth)):
    from Backend import db
    from Backend.helper import global_indexer

    return await global_indexer.global_indexer_status(db)


@app.get("/api/admin/global/unindexed")
async def get_unindexed(page: int = 1, search: str = "", _: bool = Depends(require_auth)):
    from Backend import db
    if getattr(db, "global_db", None) is None: return {"items": [], "total_pages": 1, "total_items": 0}
    page_size = 30
    skip = (page - 1) * page_size
    query = {}
    if search:
        query = {"filename": {"$regex": search, "$options": "i"}}
    total = await db.global_db["unindexed"].count_documents(query)
    cursor = db.global_db["unindexed"].find(query).sort("_id", -1).skip(skip).limit(page_size)
    items = [doc async for doc in cursor]
    return {
        "items": items,
        "total_pages": (total + page_size - 1) // page_size or 1,
        "total_items": total
    }


@app.delete("/api/admin/global/unindexed/{file_id}")
async def delete_unindexed(file_id: str, _: bool = Depends(require_auth)):
    from Backend import db
    if getattr(db, "global_db", None) is not None:
        await db.global_db["unindexed"].delete_one({"_id": file_id})
    return {"status": "success"}

@app.post("/api/admin/global/wipe")
async def wipe_global_db(_: bool = Depends(require_auth)):
    from Backend import db
    from Backend.helper import global_indexer

    if getattr(db, "global_db", None) is not None:
        indexer_status = await global_indexer.global_indexer_status(db)
        if indexer_status.get("running"):
            return {
                "status": "error",
                "message": "Stop the GlobalDB indexer before wiping data.",
            }
        await db.global_db["files"].delete_many({})
        await db.global_db["meta"].delete_many({})
        await db.global_db["catalogs"].delete_many({})
        await db.global_db["unindexed"].delete_many({})
        await db.global_db["state"].delete_many({"_id": {"$ne": "schema"}})
    return {"status": "success"}

@app.post("/api/admin/global/files/{file_id}/map")
async def map_any_file(file_id: str, payload: dict, _: bool = Depends(require_auth)):
    from Backend import db
    if getattr(db, "global_db", None) is None: return {"status": "error"}
    
    tmdb_id = payload.get("tmdb_id")
    media_type = payload.get("media_type")
    if not tmdb_id or not media_type: return {"status": "error", "message": "Missing info"}
    
    is_unindexed = True
    file_doc = await db.global_db["unindexed"].find_one({"_id": file_id})
    if not file_doc:
        file_doc = await db.global_db["files"].find_one({"_id": file_id})
        is_unindexed = False
        
    if not file_doc: return {"status": "error", "message": "File not found"}
    
    filename = file_doc["filename"]
    
    from Backend.helper.metadata import _tmdb_details, format_tmdb_image
    tmdb_type = "tv" if media_type == "series" else "movie"
    details = await _tmdb_details(tmdb_type, tmdb_id)
    if not details: return {"status": "error", "message": "Invalid TMDB ID"}
    
    from Backend.helper.global_indexer import determine_catalog
    import PTN
    from Backend.helper.split_files import clean_filename
    try:
        parsed = PTN.parse(clean_filename(filename))
    except:
        parsed = {}
    catalog = determine_catalog(parsed, details, media_type, filename)
    
    doc_id = f"tmdb:{tmdb_id}"
    external_ids = getattr(details, "external_ids", None)
    actual_imdb_id = (
        external_ids.get("imdb_id")
        if isinstance(external_ids, dict)
        else getattr(external_ids, "imdb_id", None)
        if external_ids
        else None
    )
    year_val = getattr(details, "release_date", None) or getattr(details, "first_air_date", "")
    year_number = getattr(year_val, "year", None)
    if year_number is None:
        import re
        year_match = re.search(r"\b(19\d{2}|20\d{2})\b", str(year_val or ""))
        year_number = int(year_match.group(1)) if year_match else None

    update_data = {
        "tmdb_id": int(tmdb_id),
        "imdb_id": actual_imdb_id,
        "aliases": [value for value in (doc_id, actual_imdb_id) if value],
        "title": getattr(details, "title", None) or getattr(details, "name", ""),
        "year": year_number,
        "poster": format_tmdb_image(details.poster_path),
        "background": format_tmdb_image(details.backdrop_path, "original"),
        "description": details.overview,
        "media_type": media_type,
        "catalog": catalog,
        "genres": [g.name for g in (getattr(details, "genres", None) or [])],
        "rating": getattr(details, "vote_average", 0.0),
        "updated_at": __import__("time").time()
    }
    
    lang_map = {"tam": "Tamil", "tamil": "Tamil", "tel": "Telugu", "telugu": "Telugu", "hin": "Hindi", "hindi": "Hindi", "mal": "Malayalam", "malayalam": "Malayalam", "kan": "Kannada", "kannada": "Kannada", "eng": "English", "english": "English", "multi": "Multi"}
    languages = []
    import re
    fname_lower = filename.lower()
    for k, v in lang_map.items():
        if re.search(rf'\b{k}\b', fname_lower):
            if v not in languages:
                languages.append(v)
    if getattr(details, "original_language", "") == "ta" and "Tamil" not in languages:
        languages.append("Tamil")
        
    await db.global_db["meta"].update_one(
        {"_id": doc_id}, 
        {
            "$set": update_data,
            "$addToSet": {"languages": {"$each": languages}}
        }, 
        upsert=True
    )
    
    from Backend.helper.global_db_service import episode_bounds, first_int
    from Backend.helper.split_files import parse_combined_episodes
    combined = parse_combined_episodes(filename)
    parsed_episode_start, parsed_episode_end = episode_bounds(parsed.get("episode"))
    old_meta_id = file_doc.get("meta_id")

    new_file_data = {
        "_id": file_id,
        "meta_id": doc_id,
        "filename": filename,
        "size": file_doc.get("size", 0),
        "size_str": file_doc.get("size_str", ""),
        "quality": parsed.get("resolution", "HD"),
        "chat_id": int(file_doc["chat_id"]),
        "message_id": int(file_doc["message_id"]),
        "season": first_int(combined["season"]) if combined else first_int(parsed.get("season")),
        "episode_start": first_int(combined["start"]) if combined else parsed_episode_start,
        "episode_end": first_int(combined["end"]) if combined else parsed_episode_end,
        "indexed_at": __import__("time").time(),
    }
    await db.global_db["files"].update_one({"_id": file_id}, {"$set": new_file_data}, upsert=True)
    
    if is_unindexed:
        await db.global_db["unindexed"].delete_one({"_id": file_id})
    if old_meta_id and old_meta_id != doc_id:
        remaining = await db.global_db["files"].find_one(
            {"meta_id": old_meta_id}, {"_id": 1}
        )
        if not remaining:
            await db.global_db["meta"].delete_one({"_id": old_meta_id})

    return {"status": "success"}

@app.get("/api/admin/global/channels")
async def get_global_channels(_: bool = Depends(require_auth)):
    from Backend import db
    from Backend.helper.settings_manager import SettingsManager
    from Backend.helper.global_search import _resolve_channel_ids, _get_chat_title
    from Backend.pyrofork.bot import Userbot
    
    settings = SettingsManager.current()
    target_ids = _resolve_channel_ids(settings.global_search_channels)
    
    if getattr(db, "global_db", None) is None: 
        return {"channels": []}
    
    idx_cursor = db.global_db["files"].aggregate([{"$group": {"_id": "$chat_id", "count": {"$sum": 1}}}])
    idx_counts = {}
    async for c in idx_cursor:
        try: idx_counts[int(c["_id"])] = c["count"]
        except: pass
    
    unidx_cursor = db.global_db["unindexed"].aggregate([{"$group": {"_id": "$chat_id", "count": {"$sum": 1}}}])
    unidx_counts = {}
    async for c in unidx_cursor:
        try: unidx_counts[int(c["_id"])] = c["count"]
        except: pass
    
    # Also get state to check if they have a last_id
    state_cursor = db.global_db["state"].find()
    states = [s async for s in state_cursor]
    scanned_chats = set()
    for s in states:
        if s["_id"].startswith("sync_"):
            try:
                scanned_chats.add(int(s["_id"].split("_")[1]))
            except:
                pass
    
    channels = []
    for cid in target_ids:
        # It's scanned if it has state OR has indexed/unindexed files
        is_scanned = cid in scanned_chats or cid in idx_counts or cid in unidx_counts
        
        idx = idx_counts.get(cid, 0)
        unidx = unidx_counts.get(cid, 0)
        
        channels.append({
            "chat_id": cid,
            "indexed": idx if is_scanned else "--",
            "unindexed": unidx if is_scanned else "--",
            "total": (idx + unidx) if is_scanned else "--",
            "is_scanned": is_scanned
        })
        
    for c in channels:
        if Userbot:
            try:
                c["name"] = await _get_chat_title(Userbot, c["chat_id"])
            except:
                c["name"] = str(c["chat_id"])
        else:
            c["name"] = str(c["chat_id"])
            
    return {"channels": channels}

@app.get("/api/admin/global/channels/{chat_id}/files")
async def get_channel_files(chat_id: int, filter: str = "indexed", page: int = 1, search: str = "", _: bool = Depends(require_auth)):
    from Backend import db
    if getattr(db, "global_db", None) is None: return {"items": [], "total_pages": 1}
    
    page_size = 30
    skip = (page - 1) * page_size
    items = []
    total = 0
    
    query = {"chat_id": {"$in": [chat_id, str(chat_id)]}}
    if search:
        query["filename"] = {"$regex": search, "$options": "i"}
    
    if filter == "indexed":
        total = await db.global_db["files"].count_documents(query)
        cursor = db.global_db["files"].find(query).sort("_id", -1).skip(skip).limit(page_size)
        docs = [d async for d in cursor]
        
        meta_ids = list(set([d.get("meta_id") for d in docs if d.get("meta_id")]))
        metas = {m["_id"]: m async for m in db.global_db["meta"].find({"_id": {"$in": meta_ids}})}
        
        for d in docs:
            m = metas.get(d.get("meta_id"), {})
            items.append({
                "_id": d["_id"],
                "filename": d["filename"],
                "size_str": d.get("size_str", ""),
                "status": "indexed",
                "meta_title": m.get("title", "Unknown"),
                "meta_year": m.get("year", ""),
                "tmdb_id": m.get("tmdb_id", "")
            })
    else:
        total = await db.global_db["unindexed"].count_documents(query)
        cursor = db.global_db["unindexed"].find(query).sort("_id", -1).skip(skip).limit(page_size)
        docs = [d async for d in cursor]
        for d in docs:
            items.append({
                "_id": d["_id"],
                "filename": d["filename"],
                "size_str": d.get("size_str", ""),
                "status": "unindexed",
                "reason": d.get("reason", "Unknown")
            })
            
    return {
        "items": items,
        "total_pages": (total + page_size - 1) // page_size or 1,
        "total_items": total
    }

@app.delete("/api/admin/global/unindexed")
async def delete_all_unindexed(_: bool = Depends(require_auth)):
    from Backend import db
    if getattr(db, "global_db", None) is not None:
        await db.global_db["unindexed"].delete_many({})
    return {"status": "success"}

@app.post("/api/admin/global/files/batch_map")
async def map_batch_files(payload: dict, _: bool = Depends(require_auth)):
    from Backend import db
    if getattr(db, "global_db", None) is None: return {"status": "error"}
    
    file_ids = payload.get("file_ids", [])
    input_id = str(payload.get("tmdb_id", "")).strip()
    media_type = payload.get("media_type")
    is_video_song = payload.get("is_video_song", False)
    
    if not file_ids or not input_id or not media_type:
        return {"status": "error", "message": "Missing info"}
        
    tmdb_type = "tv" if media_type == "series" else "movie"
    from Backend.helper.metadata import _tmdb_details, format_tmdb_image, get_tmdb_client
    
    # Resolve IMDb/TMDB input to strict TMDB ID
    tmdb_id = None
    if input_id.startswith("tt") or "imdb.com/title/tt" in input_id:
        import re
        m = re.search(r"(tt\d+)", input_id)
        if m:
            imdb_str = m.group(1)
            try:
                client = get_tmdb_client()
                find_res = await client.find().by_imdb(imdb_str)
                
                # Try to find exactly what they asked for first
                if media_type == "movie" and find_res.movie_results:
                    tmdb_id = find_res.movie_results[0].id
                elif media_type == "series" and find_res.tv_results:
                    tmdb_id = find_res.tv_results[0].id
                    
                # Fallback: if they chose 'movie' but it's a TV show (or vice versa), just grab what TMDB has
                elif find_res.movie_results:
                    tmdb_id = find_res.movie_results[0].id
                elif find_res.tv_results:
                    tmdb_id = find_res.tv_results[0].id
            except Exception:
                pass
    elif "themoviedb.org" in input_id:
        import re
        m = re.search(r"/(?:movie|tv)/(\d+)", input_id)
        if m:
            tmdb_id = int(m.group(1))
    else:
        try:
            tmdb_id = int(input_id.replace("tmdb:", ""))
        except:
            pass

    if not tmdb_id:
        return {"status": "error", "message": "Could not resolve TMDB ID from input"}

    details = await _tmdb_details(tmdb_type, tmdb_id)
    if not details: return {"status": "error", "message": "Invalid TMDB ID"}
    
    from Backend.helper.global_indexer import determine_catalog
    from Backend.helper.split_files import parse_combined_episodes
    import PTN
    
    success_count = 0
    
    doc_id = f"tmdb:{tmdb_id}"
    if is_video_song:
        doc_id = f"song:tmdb:{tmdb_id}"

    external_ids = getattr(details, "external_ids", None)
    actual_imdb_id = (
        external_ids.get("imdb_id")
        if isinstance(external_ids, dict)
        else getattr(external_ids, "imdb_id", None)
        if external_ids
        else None
    )
    public_imdb_id = (
        f"song:{actual_imdb_id}" if is_video_song and actual_imdb_id else actual_imdb_id
    )
    year_val = getattr(details, "release_date", None) or getattr(details, "first_air_date", "")
    year_number = getattr(year_val, "year", None)
    if year_number is None:
        import re
        year_match = re.search(r"\b(19\d{2}|20\d{2})\b", str(year_val or ""))
        year_number = int(year_match.group(1)) if year_match else None

    from Backend.helper.global_db_service import episode_bounds, first_int

    for file_id in file_ids:
        is_unindexed = True
        file_doc = await db.global_db["unindexed"].find_one({"_id": file_id})
        if not file_doc:
            file_doc = await db.global_db["files"].find_one({"_id": file_id})
            is_unindexed = False
        
        if not file_doc:
            continue
            
        filename = file_doc["filename"]
        try:
            parsed = PTN.parse(filename)
        except:
            parsed = {}
        
        catalog = determine_catalog(parsed, details, media_type, filename)
        if is_video_song:
            catalog = "video_songs"

        title_suffix = " (Video Songs)" if is_video_song else ""
        
        update_data = {
            "tmdb_id": int(tmdb_id),
            "imdb_id": public_imdb_id,
            "aliases": [value for value in (doc_id, public_imdb_id) if value],
            "title": (getattr(details, "title", None) or getattr(details, "name", "")) + title_suffix,
            "year": year_number,
            "poster": format_tmdb_image(details.poster_path),
            "background": format_tmdb_image(details.backdrop_path, "original"),
            "description": details.overview,
            "media_type": media_type,
            "catalog": catalog,
            "genres": [g.name for g in (getattr(details, "genres", None) or [])],
            "rating": getattr(details, "vote_average", 0.0),
            "updated_at": __import__("time").time()
        }
        
        lang_map = {"tam": "Tamil", "tamil": "Tamil", "tel": "Telugu", "telugu": "Telugu", "hin": "Hindi", "hindi": "Hindi", "mal": "Malayalam", "malayalam": "Malayalam", "kan": "Kannada", "kannada": "Kannada", "eng": "English", "english": "English", "multi": "Multi"}
        languages = []
        import re
        fname_lower = filename.lower()
        for k, v in lang_map.items():
            if re.search(rf'\b{k}\b', fname_lower):
                if v not in languages:
                    languages.append(v)
        if getattr(details, "original_language", "") == "ta" and "Tamil" not in languages:
            languages.append("Tamil")
            
        await db.global_db["meta"].update_one(
            {"_id": doc_id}, 
            {
                "$set": update_data,
                "$addToSet": {"languages": {"$each": languages}}
            }, 
            upsert=True
        )
        
        combined = parse_combined_episodes(filename)
        parsed_episode_start, parsed_episode_end = episode_bounds(parsed.get("episode"))
        old_meta_id = file_doc.get("meta_id")
        new_file_data = {
            "_id": file_id,
            "meta_id": doc_id,
            "filename": filename,
            "size": file_doc.get("size", 0),
            "size_str": file_doc.get("size_str", ""),
            "quality": parsed.get("resolution", "HD"),
            "chat_id": int(file_doc["chat_id"]),
            "message_id": int(file_doc["message_id"]),
            "season": first_int(combined["season"]) if combined else first_int(parsed.get("season")),
            "episode_start": first_int(combined["start"]) if combined else parsed_episode_start,
            "episode_end": first_int(combined["end"]) if combined else parsed_episode_end,
            "indexed_at": __import__("time").time(),
        }
        await db.global_db["files"].update_one({"_id": file_id}, {"$set": new_file_data}, upsert=True)
        if is_unindexed:
            await db.global_db["unindexed"].delete_one({"_id": file_id})
        if old_meta_id and old_meta_id != doc_id:
            remaining = await db.global_db["files"].find_one(
                {"meta_id": old_meta_id}, {"_id": 1}
            )
            if not remaining:
                await db.global_db["meta"].delete_one({"_id": old_meta_id})
        success_count += 1
            
    return {"status": "success", "count": success_count}
@app.get("/api/admin/global/meta/{meta_id}/files")
async def get_global_meta_files(meta_id: str, _: bool = Depends(require_auth)):
    from Backend import db
    if getattr(db, "global_db", None) is None: return {"items": []}
    cursor = db.global_db["files"].find({"meta_id": meta_id}).sort([("season", 1), ("episode_start", 1)])
    items = [doc async for doc in cursor]
    return {"items": items}

@app.post("/api/admin/global/migrate")
async def migrate_global_db(_: bool = Depends(require_auth)):
    global _MIGRATE_RUNNING
    if _MIGRATE_RUNNING:
        return {"status": "error", "message": "Migration is already running."}
        
    from Backend import db
    import asyncio
    if getattr(db, "global_db", None) is None:
        return {"status": "error", "message": "No global database configured."}
    from Backend.helper import global_indexer
    if (await global_indexer.global_indexer_status(db)).get("running"):
        return {"status": "error", "message": "Stop the indexer before migration."}
    if _CLEANUP_RUNNING:
        return {"status": "error", "message": "Wait for cleanup to finish."}

    _MIGRATE_RUNNING = True
        
    async def run_migrate():
        global _MIGRATE_RUNNING
        from Backend.helper.global_db_service import episode_bounds, first_int
        from Backend.helper.metadata import _tmdb_details
        from Backend.logger import LOGGER

        try:
            total_meta = await db.global_db["meta"].count_documents({})
            cursor = db.global_db["meta"].find({})
            import re
            import time

            last_log = time.time()
            processed = 0
            async for meta in cursor:
                processed += 1
                tmdb_id = first_int(meta.get("tmdb_id"))
                media_type = meta.get("media_type")
                details = None
                if tmdb_id and media_type:
                    details = await _tmdb_details(
                        "tv" if media_type == "series" else "movie", tmdb_id
                    )

                meta_id = str(meta["_id"])
                is_song = meta_id.startswith("song:")
                actual_imdb_id = None
                if details:
                    external_ids = getattr(details, "external_ids", None)
                    actual_imdb_id = (
                        external_ids.get("imdb_id")
                        if isinstance(external_ids, dict)
                        else getattr(external_ids, "imdb_id", None)
                        if external_ids
                        else None
                    )
                public_imdb_id = (
                    f"song:{actual_imdb_id}"
                    if is_song and actual_imdb_id
                    else actual_imdb_id
                )
                existing_imdb_id = str(meta.get("imdb_id") or "")
                if not public_imdb_id and existing_imdb_id.startswith(
                    ("tt", "song:tt")
                ):
                    public_imdb_id = existing_imdb_id

                update_fields = {
                    "tmdb_id": tmdb_id,
                    "imdb_id": public_imdb_id,
                    "aliases": [
                        value for value in (meta_id, public_imdb_id) if value
                    ],
                    "updated_at": time.time(),
                }
                if details:
                    year_value = getattr(details, "release_date", None) or getattr(
                        details, "first_air_date", ""
                    )
                    year_number = getattr(year_value, "year", None)
                    if year_number is None:
                        year_match = re.search(
                            r"\b(19\d{2}|20\d{2})\b", str(year_value or "")
                        )
                        year_number = int(year_match.group(1)) if year_match else None
                    update_fields.update(
                        {
                            "year": year_number,
                            "rating": getattr(details, "vote_average", 0.0),
                        }
                    )

                languages = set(meta.get("languages") or [])
                if details and getattr(details, "original_language", "") == "ta":
                    languages.add("Tamil")
                files_cursor = db.global_db["files"].find({"meta_id": meta_id})
                async for file_doc in files_cursor:
                    filename = file_doc.get("filename", "")
                    fname_lower = filename.lower()
                    language_map = {
                        "tam": "Tamil", "tamil": "Tamil", "tel": "Telugu",
                        "telugu": "Telugu", "hin": "Hindi", "hindi": "Hindi",
                        "mal": "Malayalam", "malayalam": "Malayalam",
                        "kan": "Kannada", "kannada": "Kannada", "eng": "English",
                        "english": "English", "multi": "Multi",
                    }
                    for key, value in language_map.items():
                        if re.search(rf"\b{key}\b", fname_lower):
                            languages.add(value)

                    start_low, start_high = episode_bounds(
                        file_doc.get("episode_start")
                    )
                    end_low, end_high = episode_bounds(file_doc.get("episode_end"))
                    bounds = [
                        value
                        for value in (start_low, start_high, end_low, end_high)
                        if value is not None
                    ]
                    start_episode = min(bounds) if bounds else None
                    end_episode = max(bounds) if bounds else None
                    await db.global_db["files"].update_one(
                        {"_id": file_doc["_id"]},
                        {"$set": {
                            "chat_id": int(file_doc["chat_id"]),
                            "message_id": int(file_doc["message_id"]),
                            "season": first_int(file_doc.get("season")),
                            "episode_start": start_episode,
                            "episode_end": end_episode,
                            "indexed_at": file_doc.get("indexed_at") or time.time(),
                        }},
                    )

                update_fields["languages"] = sorted(languages)
                await db.global_db["meta"].update_one(
                    {"_id": meta_id}, {"$set": update_fields}
                )
                if time.time() - last_log >= 120:
                    LOGGER.info(
                        "[GLOBAL MIGRATE] Processed %s/%s metadata groups.",
                        processed,
                        total_meta,
                    )
                    last_log = time.time()
            await db.global_db["state"].update_one(
                {"_id": "schema"},
                {"$set": {"version": 3, "data_migrated_at": datetime.now(timezone.utc)}},
                upsert=True,
            )
        except Exception as exc:
            LOGGER.error("[GLOBAL MIGRATE] Failed: %s", exc)
        finally:
            _MIGRATE_RUNNING = False

    asyncio.create_task(run_migrate())
    return {"status": "success", "message": "Migration started in the background!"}

@app.post("/api/admin/global/cleanup")
async def cleanup_global_db(_: bool = Depends(require_auth)):
    global _CLEANUP_RUNNING
    if _CLEANUP_RUNNING:
        return {"status": "error", "message": "Cleanup is already running."}
        
    from Backend import db
    from Backend.helper.global_indexer import clean_meta_files
    import asyncio
    
    if getattr(db, "global_db", None) is None:
        return {"status": "error", "message": "No global database configured."}
    from Backend.helper import global_indexer
    if (await global_indexer.global_indexer_status(db)).get("running"):
        return {"status": "error", "message": "Stop the indexer before cleanup."}
    if _MIGRATE_RUNNING:
        return {"status": "error", "message": "Wait for migration to finish."}

    _CLEANUP_RUNNING = True
        
    async def run_cleanup():
        global _CLEANUP_RUNNING
        try:
            meta_ids = await db.global_db["files"].distinct("meta_id")
            import time
            last_log = time.time()
            from Backend.logger import LOGGER
            for i, mid in enumerate(meta_ids):
                await clean_meta_files(db, mid)
                if time.time() - last_log >= 120:
                    LOGGER.info(f"[GLOBAL CLEANUP] Still running... Processed {i+1}/{len(meta_ids)} media groups.")
                    last_log = time.time()
        except Exception as e:
            print("Cleanup Error:", e)
        finally:
            _CLEANUP_RUNNING = False
            
    asyncio.create_task(run_cleanup())
        
    return {"status": "success", "message": "Cleanup started in the background!"}

@app.get("/api/admin/global/tasks/status")
async def global_tasks_status(_: bool = Depends(require_auth)):
    global _CLEANUP_RUNNING, _MIGRATE_RUNNING
    return {
        "cleanup_running": _CLEANUP_RUNNING,
        "migrate_running": _MIGRATE_RUNNING
    }
