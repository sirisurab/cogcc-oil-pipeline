# Features Stage — Task Specifications

Module: `cogcc_pipeline/features.py`
Tests: `tests/test_features.py`

Authoritative references (read before implementing; cite — do not restate):
- `/agent_docs/ADRs.md` — ADR-001, ADR-002, ADR-003, ADR-004, ADR-005, ADR-006,
  ADR-007, ADR-008
- `/agent_docs/stage-manifest-features.md` — purpose, input/output state, hazard H1
- `/agent_docs/boundary-transform-features.md` — what features can rely on
  receiving
- `/references/production-data-dictionary.csv`
- `/test-requirements.xml` — TR-03, TR-06, TR-07, TR-08, TR-09, TR-10, TR-14,
  TR-17, TR-18, TR-19, TR-23, TR-26

All configurable values (processed input directory, features output directory,
rolling window sizes, lag sizes, decline-rate clip bounds, train/test cutoff
year, partition count parameters) are read from the `features` section of
`config.yaml` per build-env-manifest "Configuration structure".

The features stage does not re-sort, re-index, re-cast categoricals, or
re-validate invalid values — boundary-transform-features "Downstream reliance"
guarantees those are already in place.

---

## Task F1 — Cumulative production features

Module: `cogcc_pipeline/features.py`
Function: `add_cumulative_features(pdf: pandas.DataFrame, config: dict) -> pandas.DataFrame`

Description: Partition-level pure pandas function applied via `map_partitions`.
Given an input partition sorted by `production_date` within each entity group
(boundary-transform-features downstream reliance), add per-well cumulative
production columns `cum_oil`, `cum_gas`, `cum_water` computed as grouped
cumulative sums over the well identifier. Returns a partition carrying all
input columns plus the three cumulative columns.

Behavior requirements:
- Cumulative values must be monotonically non-decreasing within each well
  group (TR-03).
- Zero-production months produce a flat cumulative segment equal to the
  previous month's cumulative value — they must not decrease and must not
  jump forward (TR-08 a).
- Zero-production months at the start of a well's record yield a cumulative
  of zero until production begins (TR-08 b).
- After a shut-in period, cumulative resumes from the flat value — not reset
  (TR-08 c).
- Vectorized grouped transformation only — no per-row iteration, no iteration
  over groups (ADR-002 consequence on per-entity aggregations).

Return contract: every return path yields a DataFrame with input columns plus
`cum_oil`, `cum_gas`, `cum_water` at float dtype, in a fixed column order —
this is what meta derivation depends on (ADR-003, TR-23).

Test cases (unit, synthetic pandas frames):
- Given a single-well sequence with known monthly production, assert
  cumulative values match hand-computed expectations and are non-decreasing
  (TR-03).
- Given a single-well sequence with mid-sequence zero-production months,
  assert cumulative stays flat across those months and resumes correctly
  (TR-08 a, c).
- Given a single-well sequence with leading zero-production months, assert
  the cumulative stays at zero until production begins (TR-08 b).
- Given an empty partition, assert the returned partition is empty but
  carries the full output schema (TR-23).

Definition of done: Function is implemented, test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages
imported in this task.

---

## Task F2 — Ratio features: GOR and water cut

Module: `cogcc_pipeline/features.py`
Function: `add_ratio_features(pdf: pandas.DataFrame, config: dict) -> pandas.DataFrame`

Description: Partition-level pure pandas function. Adds the `gor` and
`water_cut` feature columns to each partition.

Formulas (TR-09 d, e):
- `gor` = `GasProduced` / `OilProduced` (gas MCF divided by oil BBL — correct
  unit handling, not swapped).
- `water_cut` = `WaterProduced` / (`OilProduced` + `WaterProduced`) — the
  denominator is total liquid, not just oil.

Behavior requirements (TR-06):
- `OilProduced == 0` and `GasProduced > 0` → `gor` is NaN (or the configured
  sentinel); no exception raised.
- `OilProduced == 0` and `GasProduced == 0` → `gor` is 0 or NaN per the
  configured policy; no exception raised.
- `OilProduced > 0` and `GasProduced == 0` → `gor` is exactly `0.0`, not NaN.
- Water cut is valid at exactly 0.0 and exactly 1.0 (TR-10) and must be
  retained — not flagged, not nulled.

Return contract: every return path yields a DataFrame with input columns
plus `gor` and `water_cut` at float dtype, in a fixed column order.

Test cases (unit):
- TR-06 a: `oil=0, gas>0` → `gor` is NaN or configured sentinel, no
  exception.
- TR-06 b: `oil=0, gas=0` → `gor` is 0 or NaN per policy, no exception.
- TR-06 c: `oil>0, gas=0` → `gor == 0.0` exactly.
- TR-09 d: verify `gor` against a hand-computed sequence; assert no unit
  swap.
- TR-09 e: given `oil=10, water=10`, assert `water_cut == 0.5` — denominator
  uses total liquid.
- TR-10 a: `water=0, oil>0` → `water_cut == 0.0`, row retained.
- TR-10 b: `oil=0, water>0` → `water_cut == 1.0`, row retained.
- Empty partition yields empty output with full schema (TR-23).

Definition of done: Function is implemented, test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages
imported in this task.

---

## Task F3 — Rolling-window and lag features

Module: `cogcc_pipeline/features.py`
Function: `add_rolling_and_lag_features(pdf: pandas.DataFrame, config: dict) -> pandas.DataFrame`

Description: Partition-level pure pandas function. Adds rolling averages
and lag features per well, using grouped vectorized transforms (ADR-002).
Rolling windows and lag sizes are configured — at minimum 3-month and
6-month rolling averages for `OilProduced`, `GasProduced`, `WaterProduced`
(TR-09 a) and a lag-1 feature for `OilProduced` (TR-09 c). The function also
computes a 12-month rolling average and month-over-month percent change for
oil and gas, and a per-well peak-production-month boolean flag (month with
max `OilProduced` per well).

Behavior requirements (TR-09):
- Rolling averages match hand-computed values for a known sequence (TR-09 a).
- When the window exceeds available history (first months of a well's life),
  the result is NaN (or the configured partial-window value), not silently
  zero (TR-09 b).
- Lag-1 for month N equals the raw value for month N-1, verified across at
  least 3 consecutive months (TR-09 c).
- Grouped transforms only — no per-row iteration, no iteration over groups
  (ADR-002).

Return contract: every return path yields a DataFrame with input columns
plus the rolling, lag, percent-change, and peak-month columns at the
configured dtypes, in a fixed column order.

Test cases (unit):
- TR-09 a: rolling 3-month and 6-month averages for oil/gas/water match
  hand-computed values on a known sequence.
- TR-09 b: for the first N-1 months of a well's record where N is the
  window size, the rolling result is NaN or the configured partial value —
  not zero or an incorrect value.
- TR-09 c: lag-1 feature for month N equals raw `OilProduced` at month N-1,
  verified for 3 consecutive months.
- Peak-month flag — assert exactly one row per well has the flag set True
  and it corresponds to the month with max `OilProduced` for that well.
- Empty partition yields empty output with full schema (TR-23).

Definition of done: Function is implemented, test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages
imported in this task.

---

## Task F4 — Decline-rate feature with clip bounds

Module: `cogcc_pipeline/features.py`
Function: `add_decline_rate(pdf: pandas.DataFrame, config: dict) -> pandas.DataFrame`

Description: Partition-level pure pandas function. Computes period-over-period
production decline rate per well for `OilProduced` and clips the result to
the configured bounds.

Behavior requirements (TR-07):
- The raw decline-rate value is computed before the clip is applied — a
  shut-in sequence with two consecutive zero-production months must not
  produce an unclipped extreme value that then fails to be clipped due to
  arithmetic issues. Division-by-zero and zero/zero cases must resolve to a
  defined value (NaN or a sentinel) before the clip evaluates.
- Values below the lower bound are clipped exactly to the lower bound
  (default -1.0 per TR-07 a).
- Values above the upper bound are clipped exactly to the upper bound
  (default 10.0 per TR-07 b).
- Values within the bounds pass through unchanged (TR-07 c).
- Bounds come from the `features` section of `config.yaml` — not hardcoded.

Return contract: every return path yields a DataFrame with input columns
plus the `decline_rate` column at float dtype, in a fixed column order.

Test cases (unit):
- TR-07 a: synthetic well sequence producing a computed decline rate below
  the lower bound → output equals exactly the lower bound.
- TR-07 b: synthetic well sequence producing a computed decline rate above
  the upper bound → output equals exactly the upper bound.
- TR-07 c: in-bound value passes through unchanged.
- TR-07 d: two consecutive zero-production months produce a defined
  pre-clip value (NaN or sentinel), not a runtime error or an extreme
  numeric value that bypasses the clip.
- Empty partition yields empty output with full schema (TR-23).

Definition of done: Function is implemented, test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages
imported in this task.
