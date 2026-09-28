# V17 + V17.1 — SHIPPED (this zip)

## V17.1 quick-link (chat front door for manual TMDB mapping)
- `/li <Title [year]>` — REPLY to a file shared/forwarded into Saved Messages or a
  channel/group you own → links YOUR copy to that exact TMDb title (no filename
  guessing). Already-indexed files are RE-linked (same as panel Remap; orphaned
  wrong-show meta cleaned up).
- `/livs <Title [year]>` — same, but into the title's Video Songs collection
  (`song:tmdb:<id>`, catalog `video_songs`) — mirrors the modal's video-song checkbox.
- Album replies link the WHOLE media group (window scan, dedup, cap-safe).
- Accepts `tt1234567` (IMDb find) and `tmdb:1234`; bare digits are deliberately NOT
  ids (so "/li 2025" can't mis-link). Auto-picks best search hit, year arg prefers
  the exact-year candidate, and the reply always SHOWS what was chosen.
- Authorization: self-chat or creator/admin chats only. Errors are one-line strings.
- `_map_file` now also stores still/trailer fields → chat- and modal-linked titles are
  Stremio-identical to indexer-linked ones. Cache invalidated after each quick-link.


Ops Board default landing tab + system pulse. Nothing existing was moved or removed.

## Panel
- **Ops Board** (new first tab, now the default view): System LED · Mongo (derived from
  /health, never a separate ping) · Indexer (incl. last sweep from the `state` doc) ·
  Scraper dispatch countdown ring (rides the existing 10-s /stream-activity poll) ·
  Leech automator (primary + queue_pending; NO seeder numbers) · Backlog card
  (green-glow at 0) · **Recent Actions (ops_log)** · Stream board mini.
  All buttons call the existing functions; /health and /predvd/status fire only on
  open/refresh — net-new periodic DB polling: 0.
- Health view (previously an orphan with no tab) is reachable via "Full health details",
  with a Back to Ops button.
- Catalog titles: 🔎 find-title box (`?q=` on /files/catalog, regex-escaped).
- Poster imgs: lazy-loaded, hidden on error.
- Self-Heal: "backfill posters/trailers" checkbox (default ON) → ≤100 old titles per run.

## Server
- `app/opslog.py`: one-doc capped log (15) of finished cleanup / self-heal / sweep runs
  with their counts — visible in panel, /stats, and after restarts.
- `indexer`: `indexed_total` + `last_indexed_ts` counters; `schedule_repair()` shared by
  panel + /heal (busy guard in one place); sweep result now logged; `repair_series_index(
  backfill_meta=…)` phase 4 (idempotent; stremio meta cache invalidated after).
- `metadata`: TMDb `append_to_response=external_ids,videos`; `pick_trailer_key`
  (official > Trailer > popularity); `media_fields_from_details` → meta docs gain
  `still_path` (w780), `trailer_yt`, `season_posters` (tv/series) — stored at index time.
- `stremio routes`: `trailerStreams`, `videos[].thumbnail` (season poster / still),
  `videoSize` in BYTES (spec) — zero extra calls at stream time; legacy titles unaffected.
- `keepalive`: `_dispatch_state` (+ `dispatch_state()`), `safety_tick()` — session
  drop→recovery DM + backlog growth DM (≥25, growing, 1/day max, top reasons via one
  aggregate). Rides the existing 60-s watchdog, self-throttled to KEEPALIVE_MINUTES.
  No new asyncio tasks.
- `app/userbot_cmds.py` (self-chat only): /stats /heal /feed /primary list|<n|id>
  /watch add|del|list. Installed through live.install() so reconnects keep them.
- `app/watchlist.py`: `watchlist` collection; indexer hook matches via a 5-min cached
  in-memory snapshot (zero per-file DB reads); one-shot watch → DM "✅ landed"; a dead
  DB/session can never affect indexing.
- Config knobs (all optional): `SAFETY_DM_ENABLED` (default true),
  `SAFETY_BACKLOG_THRESHOLD` (25). No new required env.

## Deliberately NOT built (user cuts)
Ctrl-K palette · poster-wall grid toggle (table already shows posters) · confetti/streak ·
/unidx inline-button command · panel watchlist UI · CSV export · subtitles · trending ·
continue-watching · sparklines · any LLM · brand tags (v16.1 info-chips are the row format) ·
dual userbot accounts.

## Verified
test_v17.py 50/50 · v16.1 E2E unchanged (chips/strict-eps/repair pass) · chip battery 8/8 ·
compileall clean · rendered-panel JS `node --check` pass · Jinja render pass ·
dead-Mongo boot contract identical to v16.1 baseline (503-fail-fast design intact, no new
crash surface).
