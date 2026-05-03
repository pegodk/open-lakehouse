.PHONY: test test-integration lint format dagster-dev up down clean

test:
	uv run pytest tests/unit/ -v

test-integration:
	uv run pytest tests/integration/ -v -m integration

lint:
	uv run ruff check src/ tests/

format:
	uv run ruff format src/ tests/

dagster-dev:
	uv run dagster dev

up:
	docker compose up -d

down:
	docker compose down -v

clean:
	rm -rf spark-warehouse/ metastore_db/ derby.log .pytest_cache/ .coverage htmlcov/
