# Task Index

COGCC Production Data Pipeline — Task Specification Files

Each file below is fully self-contained and covers all tasks, design constraints, and
test cases required to implement that pipeline stage. Files are listed in execution order.

| File | Stage | Description |
|---|---|---|
| `tasks/acquire_tasks.md` | Acquire | Project scaffolding, config, utilities, ECMC data download (annual ZIP + monthly CSV), data dictionary parser, parallel download orchestration, file integrity validation, CLI entry point |
| `tasks/ingest_tasks.md` | Ingest | Column name normalisation, single-file CSV reader with encoding fallback, Dask parallel multi-file ingestion, interim Parquet writer, schema completeness validator, top-level ingest entry point |
| `tasks/transform_tasks.md` | Transform | Sentinel value replacement, API well number construction and validation, production date construction, physical bounds validation and outlier flagging, duplicate removal, data quality report generation, well completeness checker, cleaned Parquet writer, top-level transform entry point |
| `tasks/features_tasks.md` | Features | Per-well cumulative production, GOR and water cut, production decline rate, rolling averages and lag features, well age and days-normalised production, per-well feature engineering dispatcher, categorical label encoding, numeric scaling (StandardScaler), Dask-parallel feature engineering, feature matrix Parquet writer and schema validator, top-level features entry point, pipeline orchestrator |
