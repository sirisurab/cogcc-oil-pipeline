# Task Index

| File | Description |
|------|-------------|
| `tasks/acquire_tasks.md` | Data acquisition: download annual ZIP archives and current-year monthly CSV from ECMC; parallel Dask threaded downloads; idempotent file validity checking; project scaffold (pyproject.toml, Makefile, config.yaml) |
| `tasks/ingest_tasks.md`  | Data ingestion: read raw CSVs with explicit schema enforcement from the data dictionary; filter to ReportYear >= 2020; validate schema; log file metadata (row counts, checksums); write consolidated interim Parquet dataset |
| `tasks/transform_tasks.md` | Data transformation and cleaning: construct well_id and production_date derived columns; deduplicate; clean production volumes (negatives, outliers, zero-preservation); cast WellStatus to categorical; set_index shuffle; repartition; sort within partitions; write processed Parquet dataset |
| `tasks/features_tasks.md` | Feature engineering: cumulative production (Np, Gp, Wp); GOR; water cut; decline rates (clipped); rolling averages; lag features; output validation; write ML-ready feature-matrix Parquet dataset; pipeline orchestrator (pipeline.py) and CLI entry point |
