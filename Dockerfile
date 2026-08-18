# The non-slim official image inherits the buildpack-deps toolchain, Git, Bash,
# and CA certificates. Pinning its multi-platform digest lets this build avoid
# apt entirely, which also avoids deployment-builder mirror/repository failures.
FROM python:3.11.15-bookworm@sha256:a8f8fbe1a0edc9e4dddafa64ba73f7e04be7be5ebc23f332362e779e0a2e4e52

# Pin uv by both release and multi-platform digest.
COPY --from=ghcr.io/astral-sh/uv:0.12.0@sha256:606e70c71c852d03f611b1e56a195d08648507018a7057fab82c4974c4eae105 /uv /uvx /bin/

ENV PYTHONUNBUFFERED=1 \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    UV_PYTHON_DOWNLOADS=0 \
    UV_NO_DEV=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# Keep dependency installation cacheable and reproducible. Do not regenerate
# uv.lock during an image build.
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

COPY . .
RUN uv sync --locked --no-dev && chmod +x start.sh

CMD ["bash", "start.sh"]
