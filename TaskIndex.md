# Task Index

| File | Description |
|---|---|
| `tasks/acquire_tasks.md` | Download COGCC production zip and monthly CSV files for 2020–current year using Dask threaded scheduler |
| `tasks/ingest_tasks.md` | Read raw CSVs, enforce canonical schema from data dictionary, filter to year >= 2020, write partitioned interim Parquet |
| `tasks/transform_tasks.md` | Clean, deduplicate, cast categoricals, validate bounds, set entity index, sort by production date, write processed Parquet |
| `tasks/features_tasks.md` | Compute cumulative volumes, GOR, water cut, decline rates, rolling averages, lag features; write ML-ready Parquet |
| `tasks/pipeline_tasks.md` | Pipeline orchestration, dual-channel logging, Dask scheduler initialization, CLI entry point, build environment (pyproject.toml, Makefile, config.yaml, .gitignore) |
