# Global Stremio

A **lean, GlobalDB-only** Telegram → Stremio media server, built from scratch
for personal / family use on **Koyeb's free tier**.

Unlike the full [Telegram-Stremio](https://github.com/weebzone/Telegram-Stremio)
project, this has **no local library, no live search, no subscriptions, no
multi-client load balancing** — just the GlobalDB concept:

1. **Index** your Telegram channels into a MongoDB "GlobalDB".
2. **Serve** them to Stremio as browsable catalogs.
3. **Stream** the bytes (with seeking) directly from Telegram.

---

## Architecture

```
Stremio app ──► FastAPI (/stremio/*) ──► MongoDB (GlobalDB)
                     │
                     └──► /dl/* ──► PyroFork userbot ──► Telegram CDN (bytes)
```

### Collections (schema-compatible with the existing `dbFyvio` GlobalDB)

| Collection | Purpose |
|------------|---------|
| `meta`     | One doc per title, keyed `tmdb:{id}` (title, year, poster, catalog, languages…) |
| `files`    | One doc per file, keyed `{chat_id}_{message_id}` (season/episode bounds, quality) |
| `unindexed`| Files that failed to match (admin can map them later) |
| `catalogs` | The 9 fixed catalogs (tamil/dubbed/anime/other movies+series, video songs) |
| `state`    | Sync checkpoints, indexer lease, schema version |

Because it reuses the exact schema, an existing populated GlobalDB
(e.g. your `dbFyvio` DB with 67k files) is served **immediately, no
re-indexing required**.

---

## Quickstart

### 1. Configure

Copy `config.env.example` → `config.env` and fill in:

| Variable | Description |
|----------|-------------|
| `API_ID`, `API_HASH` | From https://my.telegram.org |
| `SESSION_STRING` | User-account session (member of your channels) |
| `MONGO_URI`, `DB_NAME` | Your GlobalDB connection string + `dbFyvio` |
| `TMDB_API` | TMDb API key (for metadata during indexing) |
| `BASE_URL` | Your public URL (Koyeb app URL) |
| `API_TOKEN` | Any random string — the Stremio access token |
| `ADMIN_KEY` | Any random string — for `/api/*` management |

### 2. Run locally

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 3. Install in Stremio

Open in a browser:

```
https://YOUR-URL/stremio/API_TOKEN/manifest.json
```

or paste this into the Stremio "Add addon" → "Web" field:

```
https://YOUR-URL/stremio/API_TOKEN/manifest.json
```

Share the same URL with your whole family.

---

## Deploy on Koyeb (free tier)

**One-shot helper** — this repo ships `koyeb_deploy.sh`, which prompts for
your config, creates Koyeb **secrets** for sensitive values, deploys the
service, and prints your Stremio URL:

```bash
# Install the Koyeb CLI first: https://www.koyeb.com/docs/build-and-deploy/cli
./koyeb_deploy.sh                # interactive
./koyeb_deploy.sh --dry-run      # just print the commands
./koyeb_deploy.sh --app myname --repo github.com/you/global-stremio --branch master
```

It does all of this in one shot:

1. Loads `config.env` if it exists (or prompts).
2. Auto-generates `API_TOKEN` + `ADMIN_KEY`.
3. Creates Koyeb secrets for `SESSION_STRING`, `MONGO_URI`, `ADMIN_PASSWORD`,
   `API_TOKEN`, `ADMIN_KEY`, `TMDB_API`, `API_HASH`.
4. `koyeb service create` — free tier, Frankfurt, Dockerfile builder, port 8000,
   health check on `/healthz`.
5. Sets `BASE_URL` from the assigned `*.koyeb.app` URL.
6. Prints the panel URL + Stremio manifest URL + the force-index curl command.

### Manual steps (if you prefer the dashboard)

1. Push this folder to a GitHub repo.
2. Koyeb → **Create Service** → *Deploy from GitHub* → select the repo.
3. Builder: **Dockerfile**.
4. Under *Environment variables*, add all variables from `config.env.example`
   (use Koyeb **secrets** for the sensitive ones).
5. **Health check**: path `/healthz`, port `8000`.
6. Deploy.

> **Scale-to-zero**: the app self-pings `/healthz` every `KEEPALIVE_MINUTES`
> (default 15) so it stays awake. If it ever does sleep, the sync checkpoints
> mean the next wake simply catches up on new posts.

> **Free-tier tip**: the streaming proxy is the bottleneck at 0.1 vCPU. Keep
> indexed files at 720p and 1–2 concurrent viewers for smooth playback; upgrade
> to a paid instance for multi-user 1080p.

---

## Management Panel (GlobalDB Panel)

Open `https://YOUR-URL/` in a browser and **log in** with `ADMIN_USERNAME` /
`ADMIN_PASSWORD`. This is the **exact same GlobalDB panel** as the original
project (`global_manage.html`), with all features working identically:

- **Connect to Stremio** — shows your addon manifest URL with a one-click **Copy URL** button + an **Install** link (opens Stremio web)
- **Stats** — indexed files + catalogs count
- **Sync All Channels / Stop** — indexer control with live status
- **Maintenance** — Cleanup DB, Migrate Metadata, Wipe Database
- **Catalogs** — per-catalog counts → drill into titles → drill into files
- **Channels** — **add** (by ID or `@username`) and **remove** (with optional data purge) channels, per-channel sync, drill into files
- **Unindexed Queue** — search, single/batch TMDb mapping (incl. video-song), delete, purge-all
- **Theme picker** — the full theme set, in the navbar dropdown

> The panel loads Font Awesome + Tailwind from their CDNs (as the original
> does), so it needs internet access at runtime — fine on Koyeb, but the
> in-app file preview won't show icons/styles.

## Management API

Same paths as the original, under `/api/admin/global/*`, protected by the
session cookie **or** the `X-Admin-Key` header:

| Endpoint | Action |
|----------|--------|
| `GET /stats` | files + per-catalog counts |
| `POST /index/start` · `/index/stop` · `GET /index/status` | indexer control |
| `POST /cleanup` | background quality-dedup |
| `POST /migrate` · `/migrate/stop` · `GET /tasks/status` | resumable migration |
| `POST /wipe` | clear everything (keeps schema) |
| `GET /unindexed` · `DELETE /unindexed` · `DELETE /unindexed/{id}` | failed queue |
| `GET /channels` · `GET /channels/{id}/files` | channel browse |
| `GET /channels/config` | configured channel list |
| `POST /channels/add` · `POST /channels/remove` | add/remove channels (id or @username) |
| `GET /files/catalog/{id}` · `GET /meta/{id}/files` | catalog/meta drilldown |
| `POST /files/batch_map` · `POST /files/{id}/map` | TMDb mapping |
| `DELETE /meta/{id}` · `/catalogs/{id}` · `/files/{id}` | deletes |

Trigger a full historic scan (equivalent to the panel's "Sync"):

```bash
curl -X POST https://YOUR-URL/api/admin/global/index/start \
  -H "X-Admin-Key: YOUR_ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"force_historic": true}'
```

After that, the live handlers keep it updated automatically.

---

## Getting a `SESSION_STRING`

The user account must be a **member** of every channel you want to index.
Generate the string with any Pyrogram/PyroFork session tool, e.g.:

```python
from pyrogram import Client
async def main():
    async with Client("gen", api_id=API_ID, api_hash=API_HASH) as app:
        print(await app.export_session_string())
```

---

## Bot streaming for heavy channels (4K video songs)

A Telegram **user session** has a low `GetFile` concurrency limit (~3.3 MiB/s
at safe parallelism), so high-bitrate files (4K video songs) buffer. A **bot
token** has a higher limit and an independent rate bucket.

To route heavy files through a bot:

1. Create a bot with @BotFather → copy the token.
2. Add the bot as **admin** to your heavy channel(s) (e.g. your 4K video-songs channel).
3. Set:
   ```
   BOT_TOKEN=<token>
   BOT_CHANNELS=-1001234567890
   ```
4. Deploy. Streams from those channels now use the bot; everything else still
   uses your user session. The bot shows its own status in the Health tab.

Bots cost ~20 MB RAM and ~0 CPU (asyncio I/O), so one bot is free-tier safe.

## Streaming quality

- **HTTP Range / seeking** — full support (`Accept-Ranges: bytes`, 206 responses).
- **Direct DC media sessions** — bytes stream from the file's datacenter via
  raw `upload.GetFile` (no whole-file download).
- **Bounded parallelism + prefetch** — `STREAM_PARALLELISM` / `STREAM_PREFETCH`
  tune memory vs. throughput (defaults are free-tier friendly).
- **HEAD requests** supported for player probing.
- **Robust core** — producer/consumer queue, FloodWait retry + jitter,
  file-reference refresh, client-disconnect detection, stall timeout,
  media-session prewarm (ported from upstream `ByteStreamer`).

## Health & monitoring (`GET /api/admin/global/health`)

One endpoint shows everything at a glance:

- **Session** — connected, datacenter, username, last error, reconnect attempts
- **Indexer** — running state, processed, current chat, last error
- **Telemetry** — active/recent streams (bytes, avg/instant/peak Mbps, duration)
  and error counters (`auth_key_duplicated`, `flood_waits`, `lookup_failures`)
- **Catalog counts** — files / titles / unindexed
- **Instance** — hostname + PID (instantly reveals if Koyeb is running 2+ pods)

A **session watchdog** runs in the background: if the userbot disconnects, it
reconnects with exponential backoff (10s → 5m), so a transient drop self-heals
without a restart.

## Performance

- **Response caching** — `manifest` (10 min), `catalog` (45 s), `meta` (5 min)
  are cached in memory, so repeated browses avoid Atlas round-trips entirely
  (~280x faster on cache hits) and survive the free-tier DB cold-start.
- **Pre-computed metadata** — `codec`, `audio`, and `resolution` are stored at
  index time and reused at stream time, so no PTN re-parse happens per request.
- **Fast cleanup** — the dedup filter uses the stored `quality` field + regex
  instead of re-parsing filenames (~125x faster bulk cleanup).

## Security

- Every Stremio endpoint requires the shared `API_TOKEN`.
- Stream URLs are HMAC-signed (`app/token.py`) so viewers can't request
  arbitrary Telegram files by tampering with the URL.
- `ADMIN_KEY` protects the management API.
- **Rotate any MongoDB password that has been shared in plaintext.**
