.PHONY: lint typecheck test up down

lint:
	uv run ruff format --check .
	uv run ruff check .

typecheck:
	uv run mypy

test:
	uv run pytest

up:
	docker compose up -d

down:
	docker compose down
