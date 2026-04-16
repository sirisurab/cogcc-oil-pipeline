.PHONY: venv install acquire ingest transform features pipeline test lint typecheck

venv:
	python -m venv .venv

install:
	.venv/bin/pip install --upgrade pip && .venv/bin/pip install -e ".[dev]"

acquire:
	cogcc-pipeline --stages acquire

ingest:
	cogcc-pipeline --stages ingest

transform:
	cogcc-pipeline --stages transform

features:
	cogcc-pipeline --stages features

pipeline:
	cogcc-pipeline --stages acquire ingest transform features

test:
	pytest tests/

lint:
	ruff check cogcc_pipeline/

typecheck:
	mypy cogcc_pipeline/
