.PHONY: env install lint type-check test test-unit test-integration pipeline clean

env:
	python3 -m venv .venv

install:
	.venv/bin/pip install --upgrade pip setuptools wheel
	.venv/bin/pip install -e ".[dev]"

lint:
	ruff check cogcc_pipeline/ tests/

type-check:
	mypy cogcc_pipeline/ tests/

test:
	pytest tests/ -v

test-unit:
	pytest tests/ -v -m unit

test-integration:
	pytest tests/ -v -m integration

pipeline:
	python -m cogcc_pipeline

clean:
	rm -rf dist/ build/ *.egg-info .mypy_cache .ruff_cache __pycache__ cogcc_pipeline/__pycache__ tests/__pycache__
