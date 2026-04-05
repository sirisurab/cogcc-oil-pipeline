# COGCC Production Data Pipeline

A production-grade Python pipeline for downloading, ingesting, cleaning, and feature-engineering COGCC/ECMC oil and gas production data.

## Stages

1. **Acquire** — Downloads annual ZIP archives and the live monthly CSV from the ECMC public portal.
2. **Ingest** — Reads raw CSVs, normalises column names, writes interim Parquet via Dask.
3. **Transform** — Cleans data: replaces sentinels, constructs well IDs and production dates, validates physical bounds, removes duplicates, writes cleaned Parquet.
4. **Features** — Engineers ML features: cumulative production, GOR, water cut, decline rate, rolling averages, lag features, well age, categorical encoding, numeric scaling.

## Quick Start

```bash
make env
make install
make pipeline
```

## Run Tests

```bash
make test
# Unit tests only
make test-unit
```

## Configuration

All pipeline settings live in `config/pipeline_config.yaml`.
