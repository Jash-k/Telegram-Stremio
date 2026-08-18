FROM python:3.11-slim-bookworm

# Pin uv separately from the Debian/Python base so a moving uv image cannot
# silently change the OS repositories used by apt.
COPY --from=ghcr.io/astral-sh/uv:0.12.0 /uv /uvx /bin/

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    UV_PYTHON_DOWNLOADS=0 \
    UV_NO_DEV=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"

# Use a stable Debian release, retry transient mirror failures, avoid mixed tab
# indentation, and remove package indexes in the same layer.
RUN set -eux; \
    apt-get -o Acquire::Retries=5 update; \
    apt-get -o Acquire::Retries=5 install -y --no-install-recommends \
        bash \
        build-essential \
        ca-certificates \
        git; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Keep dependency installation cacheable and reproducible. Do not regenerate
# uv.lock during an image build.
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

COPY . .
RUN uv sync --locked --no-dev && chmod +x start.sh

CMD ["bash", "start.sh"]
