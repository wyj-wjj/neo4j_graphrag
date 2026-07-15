#!/usr/bin/env sh
set -eu

uv sync --frozen --group dev
uv run ruff format --check src tests scripts alembic
uv run ruff check src tests scripts alembic
uv run mypy src
uv run pytest -q --cov=graphrag --cov-report=term-missing
uv run python scripts/evaluate.py
uv run python scripts/export_openapi.py
pnpm --dir frontend install --frozen-lockfile
pnpm --dir frontend lint
pnpm --dir frontend test
pnpm --dir frontend build
pnpm --dir frontend test:e2e
