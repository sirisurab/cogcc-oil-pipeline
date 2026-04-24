# Transform Stage — Task Specifications

Module: `cogcc_pipeline/transform.py`
Tests: `tests/test_transform.py`

Authoritative references (read before implementing; cite — do not restate):
- `/agent_docs/ADRs.md` — ADR-001, ADR-002, ADR-003, ADR-004, ADR-005, ADR-006,
  ADR-007, ADR-008
- `/agent_docs/stage-manifest-transform.md` — purpose, input/output state,
  hazards H1–H4
- `/agent_docs/boundary-ingest-transform.md` — what transform can rely on
  receiving (canonical schema established, dtypes enforced, all nullable
  columns present, partition count in contract range)
- `/agent_docs/boundary-transform-features.md` — what transform must deliver
  (entity-indexed, sorted by production_date within each partition,
  categoricals cast to declared sets, invalid values replaced with null
  sentinel, partition count per ADR-004)
- `/references/production-data-dictionary.csv` — dtype and category source
- `/test-requirements.xml` — TR-01, TR-02, TR-04, TR-05, TR-11, TR-12, TR-13,
  TR-14, TR-15, TR-16, TR-17, TR-18, TR-23, TR-25

All configurable values (interim input directory, processed output directory,
entity column name, physical bound thresholds, partition count parameters) are
read from the `transform` section of `config.yaml` per build-env-manifest
"Configuration structure".

The entity column is the well identifier composed from the three API components
(`ApiCountyCode`, `ApiSequenceNumber`, `ApiSidetrack`) per
`/references/production-data-dictionary.csv`. The production_date column is
derived from `ReportYear` + `ReportMonth`. Exact column name for the entity
and for the date is declared in the `transform` section of `config.yaml`.

---

## Task T1 — Partition-level cleaning

Module: `cogcc_pipeline/transform.py`
Function: `clean_partition(pdf: pandas.DataFrame, config: dict) -> pandas.DataFrame`

Description: Single-partition pure pandas function applied across the Dask
DataFrame with `map_partitions`. Given one partition from the interim Parquet,
apply every row-level cleaning rule and return a partition that satisfies the
downstream boundary contract in column set, dtypes, and semantic invariants.
The input partition already carries the canonical schema and data-dictionary
dtypes (boundary-ingest-transform "Downstream reliance") — this function does
not re-establish those.

Cleaning behavior the partition must enforce before returning:
- Build the entity column (well identifier) from the API components per the
  configured rule.
- Build the `production_date` column from `ReportYear` and `ReportMonth` with
  a stable datetime dtype (month-start convention is the coder's choice,
  applied consistently).
- Drop rows missing either the entity column or `production_date`.
- Deduplicate on (entity, `production_date`). Deduplication must be idempotent
  (TR-15) and must never increase row count (TR-15).
- Clip production volumes (`OilProduced`, `GasProduced`, `WaterProduced`,
  `OilSales`, `GasSales`, `WaterPressureTubing`, etc. as applicable) so that
  values that would violate physical-bound invariants (TR-01) are replaced
  with the null sentinel or clipped to the physical bound per the
  configured policy. The distinction between a physical-bound violation (null
  sentinel — sensor error) and a clip-to-zero policy (retain data) is a
  configured decision, not hardcoded.
- Detect unit-inconsistency outliers per TR-02 (oil rates above 50,000
  BBL/month for a single well) and replace them with the null sentinel.
- Preserve zero values (TR-05) — a reported zero remains zero; only nulls
  are treated as missing.
- Normalize `OpName` to stripped uppercase where it is non-null.
- Retain only rows with `ReportYear >= 2020` through current year (already
  filtered in ingest but re-asserted cheaply here per stage-manifest-transform
  output state).

Design decisions and their governing doc:
- Vectorized construction of entity and date columns — ADR-002 prohibits
  per-row iteration.
- Null-sentinel policy for physical-bound violations — TR-01, TR-05.
- Which categoricals to cast here vs. which are cast in T2 — the partition
  boundary contract requires categoricals to carry only declared values at
  stage exit (stage-manifest-transform H2); the coder decides whether cast
  happens per-partition or after concat, but values outside the declared set
  must be replaced with the null sentinel before cast.

Return contract: every return path yields a DataFrame with the exact column
set and dtypes of the stage output schema — including the added entity and
`production_date` columns. This is the contract meta derivation depends on.

Test cases (unit, synthetic pandas frames):
- Given a synthetic partition with rows carrying negative `OilProduced`,
  assert violations are replaced with the null sentinel or clipped per the
  configured policy (TR-01).
- Given a synthetic partition with an oil value above the unit-consistency
  threshold, assert the value is replaced with the null sentinel (TR-02).
- Given a synthetic partition with duplicate (entity, production_date) rows,
  assert the returned frame has no duplicates and row count is not greater
  than the input (TR-15); running `clean_partition` twice on the same input
  produces identical output (idempotency — TR-15).
- Given a synthetic partition with `OilProduced=0`, assert the zero is
  preserved in the output and is not converted to null (TR-05).
- Given a synthetic partition with rows whose `ReportYear` or the built
  entity is null, assert those rows are dropped.
- Given an empty input partition, assert the returned partition is empty but
  carries the full output schema (required for meta derivation — ADR-003,
  TR-23).

Definition of done: Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task T2 — Stage orchestration: clean, sort, index, partition, write

Module: `cogcc_pipeline/transform.py`
Function: `transform(config: dict) -> None`

Description: Stage entry point. Read interim Parquet from the configured
location, apply T1 to every partition, establish the temporal sort on
`production_date`, set the index on the entity column, repartition to the
ADR-004 contract count, and write processed Parquet to the configured output
location. This function is the boundary at which the upstream guarantees in
`/agent_docs/boundary-transform-features.md` are established.

Design decisions and their governing doc:
- Scheduler reuse — build-env-manifest "Dask scheduler initialization": reuse
  existing client; do not initialize a new one.
- `map_partitions` meta derivation — ADR-003 requires the meta to come from
  calling `clean_partition` (T1) on a zero-row real input. No meta
  construction by any other means (TR-23).
- Sort vs. set_index ordering — stage-manifest-transform H1 and H4: the
  distributed shuffle triggered by set_index destroys any prior row ordering,
  and set_index must be the last structural operation before writing. The
  coder resolves how to satisfy both constraints so that sort-by-date within
  each partition holds at stage exit — the boundary contract does not
  prescribe the mechanism.
- Partition count at write — ADR-004 formula; repartition is the last
  operation before the Parquet write (ADR-004 consequence). No operation
  after repartition may alter partition structure (stage-manifest-transform
  H3).
- Lazy graph — ADR-005: no intermediate `.compute()`.
- Logging — ADR-006: reuse the pipeline logger; log count of rows dropped at
  each major step (dedup, null drop, physical-bound drop) using graph-level
  count operations aggregated in a single compute call per ADR-005.

Return contract: writes processed Parquet and returns nothing. The written
artifacts satisfy the upstream guarantees in
`/agent_docs/boundary-transform-features.md`.

Test cases:
- Unit: the return type at the point immediately before Parquet write is a
  Dask DataFrame (TR-17) — no internal `.compute()` on the main dataset.
- Unit: given a synthetic interim Parquet under `tmp_path`, run `transform`
  and assert the processed Parquet is readable (TR-18), the entity index is
  established, every partition is sorted by `production_date`, and partition
  count is within the ADR-004 range.
- Unit: sample two different partitions of the written output and assert
  identical column names and dtypes (TR-14 schema stability).
- Unit: assert the last `production_date` of one partition for a given
  entity is strictly earlier than the first `production_date` of the next
  partition for the same entity (TR-16 sort stability).
- Unit: spot-check a statistically meaningful sample of (entity, month) pairs
  from raw to processed and assert volumes match within the configured
  cleaning rules (TR-11).
- Unit: assert numeric columns carry float dtype, date column carries
  datetime dtype, string/categorical columns carry their declared dtypes
  (TR-12 dtype check).
- Unit: verify that the function uses `map_partitions` with a `meta=` value
  derived from calling T1 on an empty zero-row input; assert the meta
  column list, order, and dtypes match the actual T1 output on a minimal
  real input (TR-23).
- Unit: well-completeness — for a synthetic well reporting every month from
  a start to an end date, assert the processed output contains a record for
  every month in that span with no unexpected gaps (TR-04).
- Unit: run `transform` twice on the same interim input; assert the written
  output row count and content are identical (TR-15 idempotency extends to
  the stage boundary).
- Integration (`@pytest.mark.integration`, TR-25): run `transform` on the
  interim Parquet output of TR-24 with all output paths overridden to
  `tmp_path` (ADR-008). Assert (a) processed Parquet is readable; (b)
  upstream guarantees in `/agent_docs/boundary-transform-features.md` hold —
  entity index, sort order, partition count, categorical cast, null
  sentinels.

Definition of done: Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.
