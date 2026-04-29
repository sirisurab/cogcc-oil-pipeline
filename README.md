# COGCC Oil & Gas Production Pipeline

Dask-based pipeline for processing Colorado Oil and Gas Conservation Commission (COGCC / ECMC) monthly well production data. Covers acquisition through feature engineering, producing ML-ready Parquet output partitioned by well.

**Data source:** [Colorado ECMC](https://ecmc.state.co.us/) — annual production ZIP archives and monthly CSV updates

---

## Pipeline Stages

| Stage | Input | Output |
|---|---|---|
| **acquire** | ECMC download URLs (annual ZIPs + monthly CSV) | Raw CSV files in `data/raw/` |
| **ingest** | Raw CSV files | Schema-enforced Parquet in `data/interim/` (rows from 2020 onwards) |
| **transform** | Interim Parquet | Cleaned, deduplicated, indexed Parquet in `data/processed/` |
| **features** | Processed Parquet | ML-ready Parquet with derived features in `data/features/` |

### Derived features

`cum_oil`, `cum_gas`, `cum_water` · `gor`, `water_cut` · `decline_rate` · `rolling_3m_oil`, `rolling_6m_oil`, `rolling_3m_gas`, `rolling_6m_gas` · `lag1_oil`, `lag1_gas`

---

## Setup

```bash
# Create virtual environment and install dependencies
make venv
make install
source .venv/bin/activate
```

---

## Configuration

All settings live in `config.yaml` (committed). Key sections:

```yaml
acquire:
  zip_url_template: "https://ecmc.state.co.us/.../production/{year}_prod_reports.zip"
  monthly_url: "https://ecmc.state.co.us/.../production/monthly_prod.csv"
  start_year: 2020      # downloads annual ZIPs from this year to present
  max_workers: 5
  sleep_seconds: 0.5    # delay between requests to avoid rate limiting

ingest:
  min_year: 2020        # rows before this year are filtered out

dask:
  n_workers: 4
  memory_limit: "3GB"
  dashboard_port: 8787  # http://localhost:8787 when pipeline is running
```

Adjust `n_workers` and `memory_limit` to match available hardware.

---

## Running

```bash
# Full pipeline (all four stages in sequence)
make pipeline

# Individual stages
make acquire
make ingest
make transform
make features

# Equivalent CLI commands (with venv active)
cogcc-pipeline acquire
cogcc-pipeline ingest
cogcc-pipeline transform
cogcc-pipeline features

# Clean all generated data and build artifacts
make clean
```

---

## Testing and Code Quality

```bash
pytest tests/          # all tests
pytest tests/ -m unit  # unit tests only
ruff check cogcc_pipeline/ tests/
mypy cogcc_pipeline/
```

---

## Data Directory Layout

```
data/
  external/          # preserved between runs (not cleared by make clean)
  raw/               # downloaded CSV files (acquire output)
  interim/           # schema-enforced Parquet (ingest output)
  processed/         # cleaned, indexed Parquet (transform output)
  features/          # ML-ready Parquet (features output)
logs/
  pipeline.log       # stage-level logging (INFO + ERROR)
```

---

## Notes

- The acquire stage downloads annual production ZIPs from 2020 to the current year plus the latest monthly CSV. Re-running acquire is idempotent — already-downloaded files are skipped.
- COGCC production data includes "Revised" reports: a well-month pair may appear more than once with different production values. The transform stage deduplicates on `(ApiSequenceNumber, production_date)`, keeping the most recently accepted record.

---

## References

```
references/
  production-data-dictionary.csv    # column schema, dtypes, nullable flags, categories
```