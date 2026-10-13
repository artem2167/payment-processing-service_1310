FROM python:3.12.13-slim-bookworm AS base

COPY --from=ghcr.io/astral-sh/uv:0.12.6 /uv /usr/local/bin/uv

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_CACHE=1 \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY app ./app
COPY migrations ./migrations
COPY alembic.ini ./
COPY examples ./examples
RUN uv sync --frozen --no-dev \
    && groupadd --gid 10001 app \
    && useradd --uid 10001 --gid app --no-create-home app

USER app

FROM base AS test
USER root
RUN uv sync --frozen
COPY tests ./tests
USER app
CMD ["pytest", "-q", "-p", "no:cacheprovider"]

FROM base AS runtime
CMD ["uvicorn", "app.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
