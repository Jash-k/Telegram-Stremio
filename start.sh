#!/bin/sh
# Global Stremio — lean GlobalDB-only server.
set -e

exec uvicorn app.main:app \
  --host 0.0.0.0 \
  --port "${PORT:-8000}" \
  --workers 1 \
  --loop uvloop \
  --http httptools \
  --log-level info
