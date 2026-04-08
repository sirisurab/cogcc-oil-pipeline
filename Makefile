.PHONY: env install acquire ingest transform features pipeline test lint typecheck

env:
	python3 -m venv .venv

install:
	python3 -m pip install --upgrade pip setuptools wheel
	pip install -e ".[dev]"

acquire:
	cogcc-pipeline --stages acquire

ingest:
	cogcc-pipeline --stages ingest

transform:
	cogcc-pipeline --stages transform

features:
	cogcc-pipeline --stages features

pipeline: acquire ingest transform features
	cogcc-pipeline

test:
	pytest tests/

lint:
	ruff check .

typecheck:
	mypy cogcc_pipeline/
