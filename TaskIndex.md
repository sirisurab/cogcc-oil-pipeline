# Task Index

| File | Description |
|------|-------------|
| `tasks/acquire_tasks.md` | Scaffold, config loader, HTTP downloader with retry, ZIP extractor, manifest writer, per-year acquire worker, Dask threaded parallel runner, idempotency and file-integrity tests |
| `tasks/ingest_tasks.md` | dtype map and meta construction, raw CSV reader with schema enforcement, year filter, schema validation, distributed client management, Dask delayed ingestion runner, Parquet readability and schema-completeness tests |
| `tasks/transform_tasks.md` | production_date and well_id derivation, duplicate removal, negative-value handling, unit outlier flagging, string standardisation, WellStatus categorical cast, date filter, partition sorter, cleaning report, transform runner with set_index/repartition/sort sequence, domain and technical integrity tests |
| `tasks/features_tasks.md` | Time features, per-well rolling statistics, cumulative production (Np/Gp/Wp), decline rate with clip bounds, GOR/water-cut/WOR ratio features, well age, label encoding for operator and formation, encoder mapping writer, features runner, domain correctness and feature schema tests |
| `tasks/orchestrator_tasks.md` | Run report writer, Dask distributed client initialiser, directory bootstrapper, pipeline stage runner with per-stage timing and failure propagation, CLI entry point (argparse), Makefile stage targets, end-to-end pipeline integration tests |
