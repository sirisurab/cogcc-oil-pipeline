.PHONY: venv install acquire ingest transform features pipeline clean

VENV = .venv
PYTHON = $(VENV)/bin/python
PIP = $(VENV)/bin/pip
CMD = $(VENV)/bin/cogcc-pipeline

venv:
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip

install: venv
	$(PIP) install -e ".[dev]"

acquire:
	$(CMD) acquire

ingest:
	$(CMD) ingest

transform:
	$(CMD) transform

features:
	$(CMD) features

pipeline:
	$(CMD) acquire ingest transform features

clean:
	rm -rf data/raw/* data/interim/* data/processed/* data/features/* logs/
	rm -rf build dist *.egg-info cogcc_pipeline.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -name "*.pyc" -delete
