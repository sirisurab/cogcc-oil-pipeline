# COGCC Oil & Gas Production Data Pipeline

A Python data pipeline for downloading, ingesting, cleaning, and engineering ML-ready features
from Colorado ECMC (Energy and Carbon Management Commission) oil and gas production data.

## Pipeline Stages

| Stage | Module | Description |
|-------|--------|-------------|
| Acquire | `cogcc_pipeline/acquire.py` | Download ECMC production CSVs/zips for 2020–present |
| Ingest | `cogcc_pipeline/ingest.py` | Load raw CSVs → interim Parquet via Dask |
| Transform | `cogcc_pipeline/transform.py` | Clean, enrich, validate → processed Parquet |
| Features | `cogcc_pipeline/features.py` | Engineer ML features → features Parquet |

## Setup

```bash
make env        # Create .venv
make install    # Install dependencies
```

## Running the Pipeline

```bash
make acquire    # Download raw data
make ingest     # Ingest to interim Parquet
make transform  # Clean and enrich
make features   # Engineer ML features
```

## Testing

```bash
make test       # Run all unit tests
pytest tests/ -v -m unit          # Unit tests only
pytest tests/ -v -m integration   # Integration tests (requires data)
```

## Configuration

All pipeline parameters are in `config.yaml`:
- `acquire.base_url` — ECMC data download base URL
- `acquire.start_year` — First year to download (default 2020)
- `acquire.max_workers` — Parallel download workers (default 5)
- `ingest.target_start_year` — Filter rows before this year
- `features.rolling_windows` — Month windows for rolling averages

## Data Directory Structure

```
data/
├── raw/          ← Downloaded CSVs (acquire stage)
├── interim/      ← Raw Parquet (ingest stage)
├── processed/    ← Cleaned Parquet (transform stage)
│   └── features/ ← ML-ready Parquet (features stage)
references/
└── production-data-dictionary.md
```
