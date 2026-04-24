# Ingest Stage — Task Specifications

Module: `cogcc_pipeline/ingest.py`
Tests: `tests/test_ingest.py`

Authoritative references (read before implementing; cite — do not restate):
- `/agent_docs/ADRs.md` — ADR-001 (Dask distributed scheduler for CPU-bound
  stages), ADR-002 (scale-first design, no per-row iteration), ADR-003 (data
  dictionary as single source of truth for schema; nullable dtype mapping rules;
  meta derivation rule), ADR-004 (partition count formula), ADR-005 (lazy task
  graph, single compute), ADR-006 (dual-channel logging), ADR-007 (pandas/Dask
  stack, Parquet only), ADR-008 (test strategy)
- `/agent_docs/stage-manifest-ingest.md` — purpose, input/output state, hazards
  H1, H2, H3
- `/agent_docs/boundary-ingest-transform.md` — upstream guarantee
- `/references/production-data-dictionary.csv` — single source of truth for
  column names, dtypes, nullable status, categorical allowed values
- `/task-writer-cogcc.md` — raw file naming convention, data-filtering constraint
  (filter to `ReportYear >= 2020` after reading)
- `/test-requirements.xml` — TR-05, TR-17, TR-18, TR-22, TR-24

All configurable values (raw input directory, interim output directory,
partition count parameters, dtype overrides if any) are read from the `ingest`
section of `config.yaml` per build-env-manifest "Configuration structure".

---

## Task I1 — Data-dictionary-driven schema loader

Module: `cogcc_pipeline/ingest.py`
Function: `load_canonical_schema(dictionary_path: str) -> dict`

Description: Read `/references/production-data-dictionary.csv` and produce the
canonical schema object consumed by every other ingest task. The schema
object exposes, for each column in the dictionary: the canonical column name,
the pandas dtype to apply, nullable status, and declared categorical values
when present. This is the only function in the pipeline that interprets the
data dictionary — per ADR-003 no other function may infer, hardcode, or
heuristically derive column types or names.

Design decisions the coder must make and the governing doc for each:
- Mapping from data-dictionary dtype strings (`int`, `Int64`, `float`, `string`,
  `bool`, `datetime`, `categorical`) to pandas dtypes — governed by ADR-003
  consequences on nullable-aware dtype variants. Columns marked `nullable=yes`
  must use nullable-aware variants; `nullable=no` columns may use
  non-nullable variants only when they cannot hold nulls.
- Categorical handling — allowed values come from the dictionary `categories`
  column (pipe-separated) per ADR-003.

Error handling: raise if the dictionary file is missing, malformed, or declares
an unsupported dtype token.

Test cases (unit):
- Given the project dictionary, assert every column listed in TR-22 is present
  in the returned schema with correct nullable status.
- Assert every nullable column maps to a nullable-aware pandas dtype
  (ADR-003 consequence).
- Assert categorical columns carry the exact declared value set from the
  dictionary's `categories` cell.
- Given a malformed dictionary row, assert the function raises.

Definition of done: Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task I2 — Per-file partition reader with schema enforcement

Module: `cogcc_pipeline/ingest.py`
Function: `read_raw_file_to_frame(raw_path: str, schema: dict) -> pandas.DataFrame`

Description: Read one raw CSV under `data/raw/` into an in-memory pandas
DataFrame whose columns match the canonical schema exactly — canonical names,
data-dictionary dtypes, every nullable column present (filled with NA if absent
from the source), categoricals cast only after invalid values have been replaced
with the null sentinel. This function is the partition-level unit of work
invoked from I3; every path through the function — including the zero-row path
— returns a DataFrame that satisfies the canonical schema (stage-manifest-ingest
H2: schema enforcement applies to all DataFrames regardless of row count).

After schema enforcement, filter rows to `ReportYear >= 2020` per
`/task-writer-cogcc.md` data-filtering constraint.

Design decisions the coder must make and the governing doc for each:
- Source column name → canonical name mapping — governed by ADR-003 and
  `/references/production-data-dictionary.csv`. Columns in the raw file that
  are not in the dictionary are dropped. Renames happen before dtype casting.
- Absent column handling — governed by stage-manifest-ingest H3:
  `nullable=yes` absent → add as all-NA at the correct dtype; `nullable=no`
  absent → raise immediately. These two branches must remain distinct.
- Categorical cleanup — governed by ADR-003: values outside the declared
  category set are replaced with the null sentinel before `astype("category")`.
- Preservation of zero values — raw zeros in numeric columns must remain zeros,
  not be converted to nulls (TR-05).
- No per-row iteration — governed by ADR-002.

Return contract: every return path yields a DataFrame with the exact column
set, dtypes, and order defined by the canonical schema.

Test cases (unit, no Dask):
- Given a synthetic raw CSV covering every column in the dictionary, assert the
  returned frame has canonical names, data-dictionary dtypes, and rows filtered
  to `ReportYear >= 2020`.
- Given a raw CSV missing a `nullable=yes` column, assert the returned frame
  has that column present as all-NA at the dictionary dtype (H3).
- Given a raw CSV missing a `nullable=no` column, assert the function raises
  (H3).
- Given a raw CSV whose `WellStatus` contains a value outside the declared
  category set, assert the resulting category column contains only declared
  values and the invalid value is represented by the null sentinel.
- Given a raw CSV with zero values in numeric columns, assert the output
  preserves zeros — they are not converted to nulls (TR-05).
- Given a raw CSV with zero rows after filtering, assert the returned frame
  has zero rows but carries the full canonical schema (H2).

Definition of done: Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task I3 — Parallel ingest and Parquet write

Module: `cogcc_pipeline/ingest.py`
Function: `ingest(config: dict) -> None`

Description: Stage entry point. Enumerate raw CSV files under the configured
raw directory, read them in parallel using the CPU-bound scheduler, concatenate
the per-file frames into a single Dask DataFrame that carries the canonical
schema, repartition to the contract partition count, and write the result as
partitioned Parquet under the configured interim directory. This function is
the only place in the stage that writes Parquet, and it is the boundary at
which the upstream guarantee in `/agent_docs/boundary-ingest-transform.md` is
established.

Design decisions the coder must make and the governing doc for each:
- Scheduler and parallelization — governed by ADR-001 (CPU-bound distributed
  scheduler for ingest). The stage must reuse an existing client if one is
  running (build-env-manifest "Dask scheduler initialization").
- Meta derivation for any Dask DataFrame constructed from delayed partitions —
  governed by ADR-003 consequence: meta must come from calling the actual
  partition function (I2) on a minimal real input sliced to zero rows.
  Construction of meta by any other means is prohibited.
- Partition count at write time — governed by ADR-004 formula
  `max(10, min(n, 50))`. The repartition is the last operation before the
  Parquet write (ADR-004).
- Lazy graph / single compute — governed by ADR-005. No intermediate `.compute()`
  inside the stage.
- Output format — Parquet only (ADR-007). Overwrite or partition semantics are
  an implementation decision but must preserve the upstream guarantee for
  transform.

Return contract: writes interim Parquet and returns nothing. The written
artifacts must satisfy the upstream guarantees in
`/agent_docs/boundary-ingest-transform.md`.

Test cases:
- Unit: given a config pointing at a tmp raw directory containing 2 synthetic
  CSVs and an interim output under `tmp_path`, run `ingest` and assert the
  interim Parquet is readable, every expected column named in TR-22 is present,
  data-dictionary dtypes are carried (TR-12 dtype portion applies at transform
  but TR-22 schema completeness applies here), and partition count is within
  the ADR-004 range (TR-22).
- Unit: assert that the return type exposed to downstream stages is a Dask
  DataFrame at the point immediately before write — verifying no internal
  `.compute()` was called (TR-17).
- Unit: assert that sampling at least 3 written partitions yields the full
  canonical column set on every sample (TR-22).
- Unit: assert every written Parquet file is readable by a fresh pandas/Dask
  process (TR-18).
- Integration (`@pytest.mark.integration`, TR-24): run `ingest` on 2–3 real raw
  source files with all output paths overridden to `tmp_path` (ADR-008). Assert
  (a) interim Parquet is readable; (b) all columns carry data-dictionary
  dtypes; (c) the upstream guarantees in
  `/agent_docs/boundary-ingest-transform.md` hold; (d) all schema-required
  columns from TR-22 are present.

Definition of done: Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.
