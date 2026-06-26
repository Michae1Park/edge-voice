.PHONY: req install test lint format typecheck ci

req:
	pip-compile \
		--extra=dev \
		--extra-index-url https://download.pytorch.org/whl/cpu \
		-o requirements-dev.txt \
		pyproject.toml

install:
	pip install -e ".[dev]"

test:
	pytest

lint:
	ruff check .

format:
	ruff format .

typecheck:
	mypy --package edge_voice

ci:
	ruff check .
	ruff format --check .
	mypy --package edge_voice
	pytest