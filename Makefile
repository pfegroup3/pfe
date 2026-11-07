.PHONY: install lint test build up down deploy-dev
install: ; uv pip install -e ".[dev,ml,stream]"
lint: ; ruff check src tests && ruff format --check src tests
test: ; pytest --cov=pfe --cov-report=term-missing
build: ; python -m build --wheel
up: ; docker compose up -d --build
down: ; docker compose down -v
deploy-dev: ; databricks bundle deploy -t dev
