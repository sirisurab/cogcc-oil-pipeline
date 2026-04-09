.PHONY: env install acquire ingest transform features pipeline test lint typecheck

env:
	python3 -m venv .venv

install:
	. .venv/bin/activate && pip install --upgrade pip setuptools wheel && pip install -e ".[dev]"

acquire:
	python -m cogcc_pipeline.pipeline --stages acquire

ingest:
	python -m cogcc_pipeline.pipeline --stages ingest

transform:
	python -m cogcc_pipeline.pipeline --stages transform

features:
	python -m cogcc_pipeline.pipeline --stages features

pipeline: acquire ingest transform features
	cogcc-pipeline

test:
	pytest tests/ -v

lint:
	ruff check cogcc_pipeline/ tests/

typecheck:
	mypy cogcc_pipeline/
