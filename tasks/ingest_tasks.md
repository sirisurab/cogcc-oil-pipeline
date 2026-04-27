# Ingest Stage Tasks

**Module:** `cogcc_pipeline/ingest.py`
**Test file:** `tests/test_ingest.py`

---

## Governing documents

Schema and dtype decisions: ADR-003, `references/production-data-dictionary.csv`.
Scheduler and parallelization: ADR-001.
Parquet partitioning formula and inter-stage format: ADR-004.
Task graph laziness: ADR-005.
Test marking: ADR-008.
Stage input/output contract: stage-manifest-ingest.md.
Output boundary consumed by transform: boundary-ingest-transform.md.

---

## Scope constraints (apply to every task in this file)

- bs4/BeautifulSoup is present for the acquire stage only — do not use it in ingest.
- No schema inference from data. No dynamic column discovery.
- Canonical column names are those defined in `references/production-data-dictionary.csv`.
  Do not invent or guess column names.
- All dtypes derive from the data dictionary — never infer from data (ADR-003).
- Null sentinel selection is dtype-dependent (ADR-003): `np.nan` for float64 columns;
  `pd.NA` is valid only for nullable extension types (Int64, StringDtype, CategoricalDtype,
  bool). Using `pd.NA` on a float64 column raises TypeError at compute time.
- Per-file parallelization must produce delayed DataFrames, not Dask Bag partitions —
  Bag partitions compute to lists and cannot be passed to `dd.from_delayed` (see
  stage-manifest-ingest.md H4).
- Meta derivation for `dd.from_delayed`: call the actual read function on a minimal real
  input and slice to zero rows — do not construct a named helper or empty-frame builder
  (ADR-003).
- The task graph must remain lazy until the Parquet write (ADR-005). No intermediate
  `.compute()` calls inside transformation chains.

---

## Canonical column set

The following column names are authoritative — derived from
`references/production-data-dictionary.csv`. All ingest output must contain exactly these
columns in the order defined by the data dictionary:

`DocNum`, `ReportMonth`, `ReportYear`, `DaysProduced`, `AcceptedDate`, `Revised`,
`OpName`, `OpNumber`, `FacilityId`, `ApiCountyCode`, `ApiSequenceNumber`, `ApiSidetrack`,
`Well`, `WellStatus`, `FormationCode`, `OilProduced`, `OilSales`, `OilAdjustment`,
`OilGravity`, `GasProduced`, `GasSales`, `GasBtuSales`, `GasUsedOnLease`, `GasShrinkage`,
`GasPressureTubing`, `GasPressureCasing`, `WaterProduced`, `WaterPressureTubing`,
`WaterPressureCasing`, `FlaredVented`, `BomInvent`, `EomInvent`

---

## Task 01: Load data dictionary

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `load_data_dictionary(dd_path: str) -> dict`

**Description:** Read `references/production-data-dictionary.csv` from `dd_path` and
return a dict keyed by column name. Each value must be a dict containing: `dtype` (the
pandas type mapping described below), `nullable` (bool, from the `nullable` column), and
`categories` (list of allowed values for categorical columns, or empty list otherwise).

**Dtype mapping** (governed by ADR-003 — derive all dtype assignments from this mapping,
not from data):

| data-dictionary dtype | nullable=no pandas type | nullable=yes pandas type |
|---|---|---|
| int | int64 | Int64 |
| float | float64 | float64 |
| string | object | StringDtype (pd.StringDtype()) |
| bool | bool | pd.BooleanDtype() |
| datetime | datetime64[ns] | datetime64[ns] |
| categorical | pd.CategoricalDtype | pd.CategoricalDtype |

Note: float64 nullable columns use float64 (not a nullable extension type) because the
float64 null sentinel is np.nan, which float64 natively holds — see ADR-003.

**Dependencies:** pandas, csv (stdlib)

**Test cases (all marked `@pytest.mark.unit`):**
- Given the production data dictionary CSV, assert the returned dict contains all 32
  column names defined in the canonical column set.
- Assert that `OilProduced` maps to `float64` with `nullable=True`.
- Assert that `ReportYear` maps to `int64` with `nullable=False`.
- Assert that `WellStatus` maps to a CategoricalDtype whose categories include all values
  declared in the data dictionary (`AB`, `AC`, `DG`, `PA`, `PR`, `SI`, `SO`, `TA`, `WO`).
- Assert that `DocNum` maps to `Int64` (nullable integer extension type) with
  `nullable=True`.
- Assert that `AcceptedDate` maps to `datetime64[ns]`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 02: Read and validate a single raw CSV file

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `read_raw_file(file_path: str, data_dict: dict) -> pd.DataFrame`

**Description:** Read a single raw CSV file and return a pandas DataFrame that conforms to
the canonical schema. This function is the unit of parallelization — it is called once per
file inside a `dask.delayed` wrapper (Task 03). Every return path of this function must
satisfy the same output contract: canonical column names, data-dictionary dtypes, and the
correct null sentinel per dtype (ADR-003).

The function must:
1. Read the CSV with no dtype inference — all columns read as object initially, then cast
   to data-dictionary types (ADR-003).
2. For each canonical column: if the column is present in the CSV, cast it to the
   data-dictionary dtype. If absent and `nullable=yes`, add it as an all-NA column at the
   correct dtype. If absent and `nullable=no`, raise `ValueError` immediately (see
   stage-manifest-ingest.md H3).
3. Return a DataFrame with exactly the canonical columns in canonical order, with
   data-dictionary dtypes enforced on all columns — including on a zero-row DataFrame.

**Error handling:** Raise `ValueError` if a `nullable=no` column is absent from the
source file. Log a warning for any column present in the CSV but not in the data
dictionary (do not drop silently, but do not raise).

**Null sentinel rule (ADR-003):** float64 columns use `np.nan`; nullable extension types
(Int64, StringDtype, BooleanDtype) use `pd.NA`.

**Not required:** row-level validation beyond dtype casting, outlier detection.

**Dependencies:** pandas, numpy, logging (stdlib)

**Test cases (all marked `@pytest.mark.unit`):**
- Given a CSV with all canonical columns present and valid values, assert the returned
  DataFrame has the correct dtypes for every column (spot-check at least 5 columns of
  different types).
- Given a CSV where a `nullable=yes` column (`DocNum`) is absent, assert the function
  returns a DataFrame with that column present, all-NA, and at the correct dtype (Int64).
- Given a CSV where a `nullable=no` column (`ApiCountyCode`) is absent, assert
  `ValueError` is raised.
- Given a CSV with zero data rows (header only), assert the function returns a DataFrame
  with zero rows but with all canonical columns at their correct dtypes — not an empty
  untyped DataFrame.
- Assert that float columns (`OilProduced`, `GasProduced`, `WaterProduced`) use `np.nan`
  as the null sentinel, not `pd.NA` (ADR-003).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 03: Build Dask graph from raw files

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `build_dask_dataframe(file_paths: list[str], data_dict: dict) -> dd.DataFrame`

**Description:** Given a list of raw CSV file paths and the data dictionary (from Task 01),
return a lazy Dask DataFrame constructed from one `dask.delayed` call to `read_raw_file`
per file path. Each delayed unit must compute to a pandas DataFrame — not a list or
collection (stage-manifest-ingest.md H4). Assemble the delayed objects using
`dd.from_delayed`.

Meta derivation (ADR-003): derive the `meta=` argument for `dd.from_delayed` by calling
`read_raw_file` on a minimal real input (first available file) and slicing to zero rows.
Do not construct a named helper or empty-frame builder for meta — the actual function must
be called on real input.

Partition count is not adjusted here — that happens at write time (ADR-004).

**Not required:** filtering, type re-casting, schema re-derivation.

**Dependencies:** dask, dask.delayed, pandas

**Test cases (all marked `@pytest.mark.unit`):**
- Given a list of 3 synthetic CSV file paths (each containing valid data), assert the
  returned object is a `dask.dataframe.DataFrame` (not a pandas DataFrame — TR-17).
- Assert the Dask DataFrame has the correct column names matching the canonical column set.
- Assert the Dask DataFrame's `_meta` dtypes match the data-dictionary dtype mapping for
  all columns.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 04: Filter to target date range

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `filter_year(ddf: dd.DataFrame, min_year: int) -> dd.DataFrame`

**Description:** Return a lazy Dask DataFrame containing only rows where `ReportYear >=
min_year`. The filter is applied using Dask's partition-level operations — row filtering
using string operations on columns produced by type-casting must be done inside a
partition-level function (ADR-004). This function receives a typed DataFrame (from Task
03) so `ReportYear` is already `int64` or `Int64` — no string operations are needed here.
The filtered result must carry the same schema, dtypes, and column order as the input
(boundary-ingest-transform.md).

All return paths (including the case where filtering produces zero matching rows in a
partition) must satisfy the same output contract: same column names, same dtypes, same
column order.

**Not required:** partition re-count adjustment.

**Dependencies:** dask

**Test cases (all marked `@pytest.mark.unit`):**
- Given a Dask DataFrame containing rows for years 2018, 2019, 2020, and 2021, assert that
  `filter_year(ddf, 2020)` returns a DataFrame with only 2020 and 2021 rows.
- Given a Dask DataFrame where all rows have `ReportYear < 2020`, assert the function
  returns a Dask DataFrame with zero rows but with the correct schema — not an error.
- Assert the returned object is a `dask.dataframe.DataFrame`, not a pandas DataFrame
  (TR-17).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 05: Write partitioned Parquet output

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `write_parquet(ddf: dd.DataFrame, output_dir: str) -> None`

**Description:** Repartition the Dask DataFrame to `max(10, min(n_partitions, 50))`
partitions (where `n_partitions` is the current partition count before repartition) and
write to `output_dir` as partitioned Parquet. The repartition step must be the last
operation before writing — no operations may be applied after repartition (ADR-004).
Partition count formula is governed by ADR-004.

The output directory must be created if it does not exist before writing.

**Not required:** compression-codec selection beyond pyarrow defaults, schema embedding
beyond what `to_parquet` provides.

**Dependencies:** dask, pyarrow, pathlib (stdlib)

**Test cases (all marked `@pytest.mark.unit` unless noted):**
- Given a Dask DataFrame with 5 partitions, assert the written Parquet directory contains
  files and the Parquet is readable back into a Dask DataFrame.
- `@pytest.mark.integration` (TR-18): Write a Dask DataFrame to `tmp_path` and assert
  the resulting Parquet files are readable by both `dask.dataframe.read_parquet` and
  `pandas.read_parquet`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 06: Ingest stage orchestrator

**Module:** `cogcc_pipeline/ingest.py`
**Function:** `ingest(config: dict) -> None`

**Description:** Orchestrate the full ingest stage. Reads all raw CSV files from the
configured raw data directory, applies schema enforcement, filters to `year >= 2020`, and
writes partitioned Parquet to the configured interim output directory. The Dask distributed
scheduler must be reused if already running — do not initialize a new cluster inside this
function (build-env-manifest.md). Scheduler choice is governed by ADR-001.

Execution sequence (governed by ADR-005 — task graph must remain lazy until Parquet
write):
1. Load data dictionary (Task 01).
2. Discover all raw CSV files in `config["ingest"]["raw_dir"]`.
3. Build the lazy Dask DataFrame (Task 03).
4. Filter to `ReportYear >= 2020` (Task 04).
5. Write partitioned Parquet (Task 05) — this is the single `.compute()` trigger.

All independent computations must be batched — no intermediate `.compute()` calls before
the write (ADR-005).

**Error handling:** Log a warning if the raw directory contains no CSV files and return
without writing. Raise if the raw directory does not exist.

**Not required:** schema re-derivation, per-file error recovery beyond what Task 02
provides.

**Dependencies:** dask, pathlib (stdlib), logging (stdlib)

**Test cases (all marked `@pytest.mark.unit` unless noted):**
- Given a config pointing to a directory with no CSV files, assert the function logs a
  warning and returns without raising.
- `@pytest.mark.integration` (TR-24): Run `ingest()` on 2–3 real raw source files using
  a config dict with all output paths pointing to `tmp_path`. Assert: (a) interim Parquet
  files are written and readable; (b) all columns carry data-dictionary dtypes; (c) the
  output satisfies all upstream guarantees in boundary-ingest-transform.md; (d) all
  schema-required columns are present.
- `@pytest.mark.integration` (TR-22): Read a sample of at least 3 interim Parquet
  partitions from `tmp_path` and assert all expected columns are present in every
  partition: `ApiCountyCode`, `ApiSequenceNumber`, `ReportYear`, `ReportMonth`,
  `OilProduced`, `OilSales`, `GasProduced`, `GasSales`, `FlaredVented`,
  `GasUsedOnLease`, `WaterProduced`, `DaysProduced`, `Well`, `OpName`, `OpNumber`,
  `DocNum`. Assert no expected column is missing or renamed in any sampled partition.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.
