.PHONY: env install test lint typecheck acquire ingest transform features

env:
	python3 -m venv .venv

install:
	.venv/bin/pip install --upgrade pip setuptools wheel
	.venv/bin/pip install -e ".[dev]"

test:
	pytest tests/ -v

lint:
	ruff check cogcc_pipeline/ tests/

typecheck:
	mypy cogcc_pipeline/

acquire:
	python -m cogcc_pipeline.acquire

ingest:
	python -m cogcc_pipeline.ingest

transform:
	python -m cogcc_pipeline.transform

features:
	python -m cogcc_pipeline.features
