# COGCC Oil & Gas Production Data Pipeline

End-to-end data pipeline for Colorado Oil and Gas Conservation Commission (COGCC / ECMC) production data. Stages: **acquire → ingest → transform → features**.

## Quick Start

```bash
# Create virtualenv and install
make env
source .venv/bin/activate
make install

# Run full pipeline
make pipeline

# Run individual stages
make acquire
make ingest
make transform
make features

# Tests, lint, type-check
make test
make lint
make typecheck
```

## Pipeline Stages

| Stage | Module | Description |
|-------|--------|-------------|
| Acquire | `cogcc_pipeline/acquire.py` | Download annual ZIP / monthly CSV files from ECMC |
| Ingest | `cogcc_pipeline/ingest.py` | Read CSVs, validate schema, write `data/interim/cogcc_raw.parquet` |
| Transform | `cogcc_pipeline/transform.py` | Clean, deduplicate, flag, and index production data |
| Features | `cogcc_pipeline/features.py` | Engineer ML-ready features, encode categoricals |

## Configuration

All settings in `config.yaml`. Key sections:

- `acquire` — URLs, year range, retry settings, worker count
- `ingest` — raw/interim directories, year filter
- `transform` — interim/processed directories, date filter
- `features` — rolling windows, decline rate clip bounds
- `dask` — LocalCluster settings
- `logging` — log level and file path

## Output Files

| File | Description |
|------|-------------|
| `data/raw/*.csv` | Raw production CSVs |
| `data/raw/download_manifest.json` | Download metadata |
| `data/interim/cogcc_raw.parquet` | Ingested, year-filtered data |
| `data/interim/cogcc_cleaned.parquet` | Cleaned, indexed by well_id |
| `data/interim/cleaning_report.json` | Cleaning statistics |
| `data/processed/cogcc_features.parquet` | Full feature dataset |
| `data/processed/encoder_mappings.json` | Label encoder mappings |
| `data/processed/pipeline_run_report.json` | Run report with timings |

## Project Structure

```
cogcc_pipeline/
  acquire.py      — download, MD5, retry, manifest
  ingest.py       — dtype map, CSV reader, Dask ingestion
  transform.py    — cleaning, dedup, flags, set_index
  features.py     — rolling, cumulative, ratios, encoders
  pipeline.py     — orchestrator, CLI (cogcc-pipeline)
tests/
  conftest.py
  test_acquire.py
  test_ingest.py
  test_transform.py
  test_features.py
  test_pipeline.py
config.yaml
```
