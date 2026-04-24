.PHONY: venv install acquire ingest transform features pipeline test lint typecheck clean

VENV := .venv
PYTHON := python3
PIP := $(VENV)/bin/pip
PIPELINE := $(VENV)/bin/cogcc-pipeline

venv:
	$(PYTHON) -m venv $(VENV)

install: venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install -e .

acquire:
	$(PIPELINE) acquire

ingest:
	$(PIPELINE) ingest

transform:
	$(PIPELINE) transform

features:
	$(PIPELINE) features

# Full pipeline: chain stage targets — pipeline entry point called once per stage
pipeline: acquire ingest transform features

test:
	$(VENV)/bin/pytest tests/ -m "not integration"

test-integration:
	$(VENV)/bin/pytest tests/ -m integration

test-all:
	$(VENV)/bin/pytest tests/

lint:
	$(VENV)/bin/ruff check cogcc_pipeline/ tests/

typecheck:
	$(VENV)/bin/mypy cogcc_pipeline/

clean:
	rm -rf $(VENV) __pycache__ .mypy_cache .ruff_cache *.egg-info
