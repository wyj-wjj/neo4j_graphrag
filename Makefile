.PHONY: sync lint test coverage evaluate run openapi acceptance

sync:
	uv sync --frozen --group dev

lint:
	uv run ruff format --check src tests scripts alembic
	uv run ruff check src tests scripts alembic
	uv run mypy src

test:
	uv run pytest -q

coverage:
	uv run pytest -q --cov=graphrag --cov-report=term-missing

evaluate:
	uv run python scripts/evaluate.py

run:
	uv run uvicorn graphrag.main:app --reload

openapi:
	uv run python scripts/export_openapi.py

acceptance:
	./scripts/acceptance.sh
