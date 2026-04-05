# Task Index

| File                           | Description                                                                                     |
|--------------------------------|-------------------------------------------------------------------------------------------------|
| tasks/acquire_tasks.md         | Download ECMC production CSV/zip files for 2020–current year, fetch data dictionary, idempotent parallel acquire with retry |
| tasks/ingest_tasks.md          | Load raw CSVs into consolidated interim Parquet via Dask, validate schema, filter to target year range |
| tasks/transform_tasks.md       | Clean and enrich interim Parquet: rename columns, build well_id and production_date, deduplicate, flag invalid records, fill monthly gaps, write processed Parquet |
| tasks/features_tasks.md        | Engineer ML-ready features: cumulative production, GOR, water cut, decline rate, rolling averages, lag features, encode categoricals, scale numerics, write features Parquet |
