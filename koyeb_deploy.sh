#!/usr/bin/env bash
# =============================================================================
# Global Stremio — one-shot Koyeb deploy helper
#
#   Collects (or reuses) config, creates Koyeb SECRETS for sensitive values,
#   deploys the service on the free tier with a /healthz check, then prints
#   your Stremio manifest URL.
#
# Usage:
#   ./koyeb_deploy.sh                 # interactive prompts
#   ./koyeb_deploy.sh --dry-run       # just print the commands (no CLI needed)
#   ./koyeb_deploy.sh --app myapp     # set app/service name
#
# Requires: the Koyeb CLI (https://www.koyeb.com/docs/build-and-deploy/cli)
#           and this repo pushed to GitHub.
# =============================================================================
set -euo pipefail

APP_NAME="${APP_NAME:-global-stremio}"
GIT_REPO="${GIT_REPO:-}"          # e.g. github.com/you/global-stremio
GIT_BRANCH="${GIT_BRANCH:-master}"
REGION="${REGION:-fra}"           # Koyeb free tier: fra (Frankfurt) or was (DC)
INSTANCE_TYPE="${INSTANCE_TYPE:-free}"
PORT="${PORT:-8000}"
DRY_RUN=0

# ---------------------------------------------------------------------------
# Parse flags
# ---------------------------------------------------------------------------
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=1 ;;
    --app)     APP_NAME="${2:-$APP_NAME}"; shift ;;
    --repo)    GIT_REPO="${2:-$GIT_REPO}"; shift ;;
    --branch)  GIT_BRANCH="${2:-$GIT_BRANCH}"; shift ;;
  esac
  shift 2>/dev/null || true
done

# ---------------------------------------------------------------------------
# Load existing config.env if present (so re-runs don't re-prompt everything)
# ---------------------------------------------------------------------------
[ -f config.env ] && set -a && . ./config.env && set +a

# ---------------------------------------------------------------------------
# Prompt helpers
# ---------------------------------------------------------------------------
rand() { head -c 24 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 24; }

ask() { # ask VAR PROMPT [DEFAULT]
  local var="$1" prompt="$2" def="${3:-}"
  if [ -z "${!var:-}" ]; then
    if [ -n "$def" ]; then
      read -r -p "$prompt [$def]: " val || true
      export "$var=${val:-$def}"
    else
      read -r -p "$prompt: " val || true
      export "$var=$val"
    fi
  fi
}

# ---------------------------------------------------------------------------
# Gather config
# ---------------------------------------------------------------------------
echo "== Global Stremio · Koyeb deploy =="

ask GIT_REPO     "GitHub repo (github.com/you/global-stremio)"
ask API_ID       "Telegram API_ID (my.telegram.org)"
ask API_HASH     "Telegram API_HASH"
ask SESSION_STRING "Userbot session string (paste, it stays a secret)"
ask MONGO_URI    "GlobalDB MongoDB URI (mongodb+srv://...)"
ask DB_NAME      "DB name" "dbFyvio"
ask TMDB_API     "TMDb API key"
ask ADMIN_USERNAME "Admin panel username" "admin"
ask ADMIN_PASSWORD  "Admin panel password" "admin"

# Auto-generate the two tokens if missing
[ -z "${API_TOKEN:-}" ] && API_TOKEN="$(rand)"
[ -z "${ADMIN_KEY:-}" ] && ADMIN_KEY="$(rand)"

# ---------------------------------------------------------------------------
# Validate required fields
# ---------------------------------------------------------------------------
fail=0
for v in GIT_REPO API_ID API_HASH SESSION_STRING MONGO_URI TMDB_API; do
  if [ -z "${!v:-}" ]; then echo "✗ Missing required: $v"; fail=1; fi
done
[ "$fail" = 1 ] && { echo "Aborting — fill the missing values."; exit 1; }

# ---------------------------------------------------------------------------
# Koyeb app URL is derived: https://SERVICE-ORG.koyeb.app (set after deploy)
# ---------------------------------------------------------------------------
BASE_URL="${BASE_URL:-}"

SECRETS=(SESSION_STRING MONGO_URI ADMIN_PASSWORD API_TOKEN ADMIN_KEY TMDB_API API_HASH)
ENVVARS=(API_ID DB_NAME ADMIN_USERNAME PORT)

sec_args=()
for s in "${SECRETS[@]}"; do
  sec_args+=(--env "$s=@global-stremio-$s")
done

env_args=()
for e in "${ENVVARS[@]}"; do
  [ -n "${!e:-}" ] && env_args+=(--env "$e=${!e}")
done
[ -n "$BASE_URL" ] && env_args+=(--env "BASE_URL=$BASE_URL")

# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
run() {
  echo
  echo "  $ $*"
  [ "$DRY_RUN" = 1 ] && return 0
  "$@"
}

secret_create() { # name value
  if [ "$DRY_RUN" = 1 ]; then
    echo "  $ koyeb secret create global-stremio-$1 --value '<redacted>'"
    return 0
  fi
  # Idempotent: update if it exists, create if not.
  koyeb secret create "global-stremio-$1" --value "$2" 2>/dev/null \
    || koyeb secret update "global-stremio-$1" --value "$2"
}

echo
echo "== Creating Koyeb secrets (sensitive values) =="
for s in "${SECRETS[@]}"; do
  secret_create "$s" "${!s}"
done

echo
echo "== Creating service =="
run koyeb service create "$APP_NAME" \
  --type web \
  --instance-type "$INSTANCE_TYPE" \
  --regions "$REGION" \
  --git "$GIT_REPO" \
  --git-branch "$GIT_BRANCH" \
  --git-builder docker \
  --git-dockerfile Dockerfile \
  --port "$PORT:http" \
  --route "/:$PORT" \
  --checks "$PORT:http:/healthz" \
  "${sec_args[@]}" \
  "${env_args[@]}"

# Set BASE_URL now that the public URL exists (skip if already set).
if [ -z "$BASE_URL" ] && [ "$DRY_RUN" = 0 ]; then
  # Derive the org from the app URL; Koyeb prints it on `koyeb service get`.
  public_url="$(koyeb service get "$APP_NAME" -o json 2>/dev/null | grep -o '"https://[^"]*koyeb\.app"' | head -1 | tr -d '"' || true)"
  if [ -n "$public_url" ]; then
    run koyeb service update "$APP_NAME" --env "BASE_URL=$public_url"
    BASE_URL="$public_url"
  else
    echo "⚠  Could not auto-detect your Koyeb URL. Set BASE_URL manually:"
    echo "   koyeb service update $APP_NAME --env BASE_URL=https://$APP_NAME-YOURORG.koyeb.app"
    BASE_URL="https://$APP_NAME-<org>.koyeb.app"
  fi
fi

echo
echo "============================================================================="
echo " Done!"
echo
echo "  Panel (login with ADMIN_USERNAME / ADMIN_PASSWORD):"
echo "    $BASE_URL"
echo
echo "  Stremio addon manifest (share with family):"
echo "    $BASE_URL/stremio/$API_TOKEN/manifest.json"
echo
echo "  Trigger a full historic index:"
echo "    curl -X POST $BASE_URL/api/admin/global/index/start \\"
echo "      -H 'X-Admin-Key: $ADMIN_KEY' -H 'Content-Type: application/json' \\"
echo "      -d '{\"force_historic\": true}'"
echo
echo "  Save local config for future runs:"
echo "    (a config.env has been written)"
echo "============================================================================="

# Persist a config.env for re-runs / local dev.
{
  echo "API_ID=$API_ID"
  echo "API_HASH=$API_HASH"
  echo "SESSION_STRING=$SESSION_STRING"
  echo "MONGO_URI=$MONGO_URI"
  echo "DB_NAME=$DB_NAME"
  echo "TMDB_API=$TMDB_API"
  echo "ADMIN_USERNAME=$ADMIN_USERNAME"
  echo "ADMIN_PASSWORD=$ADMIN_PASSWORD"
  echo "API_TOKEN=$API_TOKEN"
  echo "ADMIN_KEY=$ADMIN_KEY"
  echo "BASE_URL=$BASE_URL"
  echo "PORT=$PORT"
} > config.env
chmod 600 config.env
echo "(Wrote config.env — keep it private.)"
