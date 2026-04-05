
## Eval Run at 2026-04-05 12:56:36

**Status:** ❌ FAILED

### Failures:
- **Type check:**
```
Type check failed. Fix these errors:
/cogcc_pipeline/transform.py:35: error: No overload variant of "where" of "Series" matches argument types "Series[bool]", "NAType"  [call-overload]
/cogcc_pipeline/transform.py:35: note: Possible overload variants:
/cogcc_pipeline/transform.py:35: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: Any | Series[Any] | Callable[..., Any | Series[Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/cogcc_pipeline/transform.py:35: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[Any]
/cogcc_pipeline/transform.py:65: error: Name "pd.NAType" is not defined  [name-defined]
/cogcc_pipeline/features.py:11: error: Skipping analyzing "joblib": module is installed, but missing library stubs or py.typed marker  [import-untyped]
/cogcc_pipeline/features.py:14: error: Skipping analyzing "sklearn.preprocessing": module is installed, but missing library stubs or py.typed marker  [import-untyped]
/cogcc_pipeline/features.py:14: note: See https://mypy.readthedocs.io/en/stable/running_mypy.html#missing-imports
/tests/test_features.py:62: error: Argument 2 to "_single_well_df" has incompatible type "list[int]"; expected "list[float] | None"  [arg-type]
/tests/test_features.py:62: note: "list" is invariant -- see https://mypy.readthedocs.io/en/stable/common_issues.html#variance
/tests/test_features.py:62: note: Consider using "Sequence" instead, which is covariant
/tests/test_features.py:72: error: Argument 2 to "_single_well_df" has incompatible type "list[int]"; expected "list[float] | None"  [arg-type]
/tests/test_features.py:72: note: "list" is invariant -- see https://mypy.readthedocs.io/en/stable/common_issues.html#variance
/tests/test_features.py:72: note: Consider using "Sequence" instead, which is covariant
/tests/test_features.py:80: error: Argument 2 to "_single_well_df" has incompatible type "list[int]"; expected "list[float] | None"  [arg-type]
/tests/test_features.py:80: note: "list" is invariant -- see https://mypy.readthedocs.io/en/stable/common_issues.html#variance
/tests/test_features.py:80: note: Consider using "Sequence" instead, which is covariant
/tests/test_features.py:91: error: Argument 2 to "_single_well_df" has incompatible type "list[int]"; expected "list[float] | None"  [arg-type]
/tests/test_features.py:91: note: "list" is invariant -- see https://mypy.readthedocs.io/en/stable/common_issues.html#variance
/tests/test_features.py:91: note: Consider using "Sequence" instead, which is covariant
/tests/test_features.py:537: error: Skipping analyzing "joblib": module is installed, but missing library stubs or py.typed marker  [import-untyped]
/tests/test_orchestrator.py:115: error: "orchestrate" does not return a value (it only ever returns None)  [func-returns-value]
/tests/test_orchestrator.py:152: error: "orchestrate" does not return a value (it only ever returns None)  [func-returns-value]
Found 11 errors in 4 files (checked 14 source files)

```

- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
......................................................F........F.F...... [ 55%]
.........................................................                [100%]Running teardown with pytest sessionfinish...

=================================== FAILURES ===================================
_____________ test_apply_well_features_parallel_cum_oil_monotonic ______________
tests/test_features.py:448: in test_apply_well_features_parallel_cum_oil_monotonic
    result = apply_well_features_parallel(ddf, [3, 6, 12], logger).compute()
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E     Extra:   ['APICountyCode', 'APISeqNum', 'DocNum', 'GasFlared', 'GasSold', 'GasUsed', 'OilSold', 'OpName', 'OpNum', 'Well']
E     Missing: ['county_encoded', 'op_name_encoded']
_________________ test_validate_features_schema_missing_column _________________
tests/test_features.py:621: in test_validate_features_schema_missing_column
    assert any("decline_rate" in e for e in errors)
E   assert False
E    +  where False = any(<generator object test_validate_features_schema_missing_column.<locals>.<genexpr> at 0x1aedebe00>)
_____________ test_run_features_logs_schema_error_without_raising ______________
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3641: in get_loc
    return self._engine.get_loc(casted_key)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pandas/_libs/index.pyx:168: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/index.pyx:197: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/hashtable_class_helper.pxi:7668: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
pandas/_libs/hashtable_class_helper.pxi:7676: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
E   KeyError: 'OpName'

The above exception was the direct cause of the following exception:
tests/test_features.py:686: in test_run_features_logs_schema_error_without_raising
    result = run_features(config, logger)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/features.py:554: in run_features
    ddf = encode_categoricals(ddf, encoders_path, logger)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/features.py:378: in encode_categoricals
    op_names = sorted(ddf["OpName"].dropna().compute().unique().tolist())  # type: ignore[union-attr]
                      ^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:429: in __getitem__
    return new_collection(self.expr.__getitem__(other))
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/_collections.py:8: in new_collection
    meta = expr._meta
           ^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/functools.py:998: in __get__
    val = self.func(instance)
          ^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_expr.py:2186: in _meta
    return super()._meta
           ^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/functools.py:998: in __get__
    val = self.func(instance)
          ^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_expr.py:562: in _meta
    return self.operation(*args, **self._kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:4378: in __getitem__
    indexer = self.columns.get_loc(key)
              ^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3648: in get_loc
    raise KeyError(key) from err
E   KeyError: 'OpName'
=============================== warnings summary ===============================
tests/test_ingest.py::test_read_raw_csv_filters_to_2020_plus
tests/test_ingest.py::test_read_raw_csv_latin1_fallback
tests/test_ingest.py::test_run_ingest_returns_path
tests/test_ingest.py::test_run_ingest_logs_validation_warnings
  /cogcc_pipeline/ingest.py:204: Pandas4Warning: For backward compatibility, 'str' dtypes are included by select_dtypes when 'object' dtype is specified. This behavior is deprecated and will be removed in a future version. Explicitly pass 'str' to `include` to select them, or to `exclude` to remove them and silence this warning.
  See https://pandas.pydata.org/docs/user_guide/migration-3-strings.html#string-migration-select-dtypes for details on how to write code that works with pandas 2 and 3.
    str_cols = df.select_dtypes(include="object").columns

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
=========================== short test summary info ============================
FAILED tests/test_features.py::test_apply_well_features_parallel_cum_oil_monotonic
FAILED tests/test_features.py::test_validate_features_schema_missing_column
FAILED tests/test_features.py::test_run_features_logs_schema_error_without_raising
3 failed, 126 passed, 4 warnings in 37.93s

```

---

## Eval Run at 2026-04-05 13:19:25

**Status:** ❌ FAILED

### Failures:
- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
......................................................F........F.F...... [ 55%]
.........................................................                [100%]Running teardown with pytest sessionfinish...

=================================== FAILURES ===================================
_____________ test_apply_well_features_parallel_cum_oil_monotonic ______________
tests/test_features.py:448: in test_apply_well_features_parallel_cum_oil_monotonic
    result = apply_well_features_parallel(ddf, [3, 6, 12], logger).compute()
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E     Extra:   ['APICountyCode', 'APISeqNum', 'DocNum', 'GasFlared', 'GasSold', 'GasUsed', 'OilSold', 'OpNum', 'Well']
E     Missing: []
_________________ test_validate_features_schema_missing_column _________________
tests/test_features.py:621: in test_validate_features_schema_missing_column
    assert any("decline_rate" in e for e in errors)
E   assert False
E    +  where False = any(<generator object test_validate_features_schema_missing_column.<locals>.<genexpr> at 0x1aefb3d30>)
_____________ test_run_features_logs_schema_error_without_raising ______________
tests/test_features.py:686: in test_run_features_logs_schema_error_without_raising
    result = run_features(config, logger)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/features.py:553: in run_features
    ddf = encode_categoricals(ddf, encoders_path, logger)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/features.py:377: in encode_categoricals
    op_names = sorted(ddf["OpName"].dropna().compute().unique().tolist())  # type: ignore[union-attr]
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E     Extra:   ['APICountyCode', 'APISeqNum', 'DocNum', 'GasFlared', 'GasSold', 'GasUsed', 'OilSold', 'OpNum', 'Well', 'outlier_flag']
E     Missing: []
=============================== warnings summary ===============================
tests/test_ingest.py::test_read_raw_csv_filters_to_2020_plus
tests/test_ingest.py::test_read_raw_csv_latin1_fallback
tests/test_ingest.py::test_run_ingest_returns_path
tests/test_ingest.py::test_run_ingest_logs_validation_warnings
  /cogcc_pipeline/ingest.py:204: Pandas4Warning: For backward compatibility, 'str' dtypes are included by select_dtypes when 'object' dtype is specified. This behavior is deprecated and will be removed in a future version. Explicitly pass 'str' to `include` to select them, or to `exclude` to remove them and silence this warning.
  See https://pandas.pydata.org/docs/user_guide/migration-3-strings.html#string-migration-select-dtypes for details on how to write code that works with pandas 2 and 3.
    str_cols = df.select_dtypes(include="object").columns

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
=========================== short test summary info ============================
FAILED tests/test_features.py::test_apply_well_features_parallel_cum_oil_monotonic
FAILED tests/test_features.py::test_validate_features_schema_missing_column
FAILED tests/test_features.py::test_run_features_logs_schema_error_without_raising
3 failed, 126 passed, 4 warnings in 12.79s

```

---

## Eval Run at 2026-04-05 13:28:31

**Status:** ❌ FAILED

### Failures:
- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
......................................................F........F.F...... [ 55%]
.........................................................                [100%]Running teardown with pytest sessionfinish...

=================================== FAILURES ===================================
_____________ test_apply_well_features_parallel_cum_oil_monotonic ______________
tests/test_features.py:439: in test_apply_well_features_parallel_cum_oil_monotonic
    result = apply_well_features_parallel(ddf, [3, 6, 12], logger).compute()
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['well_id', 'production_date', 'oil_bbl', 'gas_mcf', 'water_bbl', 'days_produced', 'OpName', 'cum_oil', 'cum_gas', 'cum_water', 'gor', 'water_cut', 'decline_rate', 'oil_bbl_rolling_3m', 'oil_bbl_rolling_6m', 'oil_bbl_rolling_12m', 'oil_bbl_lag1', 'gas_mcf_rolling_3m', 'gas_mcf_rolling_6m', 'gas_mcf_rolling_12m', 'gas_mcf_lag1', 'water_bbl_rolling_3m', 'water_bbl_rolling_6m', 'water_bbl_rolling_12m', 'water_bbl_lag1', 'well_age_months', 'oil_bbl_30d', 'gas_mcf_30d', 'water_bbl_30d']
E   Expected: ['well_id', 'production_date', 'oil_bbl', 'gas_mcf', 'water_bbl', 'days_produced', 'cum_oil', 'cum_gas', 'cum_water', 'gor', 'water_cut', 'decline_rate', 'well_age_months', 'oil_bbl_30d', 'gas_mcf_30d', 'water_bbl_30d', 'OpName', 'oil_bbl_rolling_3m', 'oil_bbl_rolling_6m', 'oil_bbl_rolling_12m', 'oil_bbl_lag1', 'gas_mcf_rolling_3m', 'gas_mcf_rolling_6m', 'gas_mcf_rolling_12m', 'gas_mcf_lag1', 'water_bbl_rolling_3m', 'water_bbl_rolling_6m', 'water_bbl_rolling_12m', 'water_bbl_lag1']
_________________ test_validate_features_schema_missing_column _________________
tests/test_features.py:612: in test_validate_features_schema_missing_column
    assert any("decline_rate" in e for e in errors)
E   assert False
E    +  where False = any(<generator object test_validate_features_schema_missing_column.<locals>.<genexpr> at 0x1ab0ef920>)
_____________ test_run_features_logs_schema_error_without_raising ______________
tests/test_features.py:667: in test_run_features_logs_schema_error_without_raising
    result = run_features(config, logger)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/features.py:553: in run_features
    ddf = encode_categoricals(ddf, encoders_path, logger)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/features.py:377: in encode_categoricals
    op_names = sorted(ddf["OpName"].dropna().compute().unique().tolist())  # type: ignore[union-attr]
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['well_id', 'production_date', 'oil_bbl', 'gas_mcf', 'water_bbl', 'days_produced', 'OpName', 'cum_oil', 'cum_gas', 'cum_water', 'gor', 'water_cut', 'decline_rate', 'oil_bbl_rolling_3m', 'oil_bbl_rolling_6m', 'oil_bbl_rolling_12m', 'oil_bbl_lag1', 'gas_mcf_rolling_3m', 'gas_mcf_rolling_6m', 'gas_mcf_rolling_12m', 'gas_mcf_lag1', 'water_bbl_rolling_3m', 'water_bbl_rolling_6m', 'water_bbl_rolling_12m', 'water_bbl_lag1', 'well_age_months', 'oil_bbl_30d', 'gas_mcf_30d', 'water_bbl_30d']
E   Expected: ['well_id', 'production_date', 'oil_bbl', 'gas_mcf', 'water_bbl', 'days_produced', 'cum_oil', 'cum_gas', 'cum_water', 'gor', 'water_cut', 'decline_rate', 'well_age_months', 'oil_bbl_30d', 'gas_mcf_30d', 'water_bbl_30d', 'OpName', 'oil_bbl_rolling_3m', 'oil_bbl_rolling_6m', 'oil_bbl_rolling_12m', 'oil_bbl_lag1', 'gas_mcf_rolling_3m', 'gas_mcf_rolling_6m', 'gas_mcf_rolling_12m', 'gas_mcf_lag1', 'water_bbl_rolling_3m', 'water_bbl_rolling_6m', 'water_bbl_rolling_12m', 'water_bbl_lag1']
=============================== warnings summary ===============================
tests/test_ingest.py::test_read_raw_csv_filters_to_2020_plus
tests/test_ingest.py::test_read_raw_csv_latin1_fallback
tests/test_ingest.py::test_run_ingest_returns_path
tests/test_ingest.py::test_run_ingest_logs_validation_warnings
  /cogcc_pipeline/ingest.py:204: Pandas4Warning: For backward compatibility, 'str' dtypes are included by select_dtypes when 'object' dtype is specified. This behavior is deprecated and will be removed in a future version. Explicitly pass 'str' to `include` to select them, or to `exclude` to remove them and silence this warning.
  See https://pandas.pydata.org/docs/user_guide/migration-3-strings.html#string-migration-select-dtypes for details on how to write code that works with pandas 2 and 3.
    str_cols = df.select_dtypes(include="object").columns

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
=========================== short test summary info ============================
FAILED tests/test_features.py::test_apply_well_features_parallel_cum_oil_monotonic
FAILED tests/test_features.py::test_validate_features_schema_missing_column
FAILED tests/test_features.py::test_run_features_logs_schema_error_without_raising
3 failed, 126 passed, 4 warnings in 12.37s

```

---

## Eval Run at 2026-04-05 13:35:47

**Status:** ❌ FAILED

### Failures:
- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
.................................................................F...... [ 55%]
.........................................................                [100%]Running teardown with pytest sessionfinish...

=================================== FAILURES ===================================
_____________ test_run_features_logs_schema_error_without_raising ______________
tests/test_features.py:667: in test_run_features_logs_schema_error_without_raising
    result = run_features(config, logger)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/features.py:559: in run_features
    features_path = write_features_parquet(ddf, processed_dir, features_file, logger)
                    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/features.py:485: in write_features_parquet
    ddf.repartition(npartitions=30).to_parquet(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/io/parquet/core.py:158: in __call__
    return self.engine.write_partition(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/io/parquet/arrow.py:824: in write_partition
    t = cls._pandas_to_arrow_table(df, preserve_index=preserve_index, schema=schema)
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/io/parquet/arrow.py:785: in _pandas_to_arrow_table
    raise ValueError(
E   ValueError: Failed to convert partition to expected pyarrow schema:
E       `ArrowInvalid('Float value -1.224745 was truncated converting to int64', 'Conversion failed for column well_age_months with type float64')`
E   
E   Expected partition schema:
E       well_id: large_string
E       production_date: timestamp[ns]
E       oil_bbl: double
E       gas_mcf: double
E       water_bbl: double
E       days_produced: double
E       cum_oil: double
E       cum_gas: double
E       cum_water: double
E       gor: double
E       water_cut: double
E       decline_rate: double
E       oil_bbl_rolling_3m: double
E       oil_bbl_rolling_6m: double
E       oil_bbl_rolling_12m: double
E       oil_bbl_lag1: double
E       gas_mcf_rolling_3m: double
E       gas_mcf_rolling_6m: double
E       gas_mcf_rolling_12m: double
E       gas_mcf_lag1: double
E       water_bbl_rolling_3m: double
E       water_bbl_rolling_6m: double
E       water_bbl_rolling_12m: double
E       water_bbl_lag1: double
E       well_age_months: int64
E       oil_bbl_30d: double
E       gas_mcf_30d: double
E       water_bbl_30d: double
E       op_name_encoded: int32
E       county_encoded: int32
E   
E   Received partition schema:
E       well_id: large_string
E       production_date: timestamp[us]
E       oil_bbl: double
E       gas_mcf: double
E       water_bbl: double
E       days_produced: double
E       cum_oil: double
E       cum_gas: double
E       cum_water: double
E       gor: double
E       water_cut: double
E       decline_rate: double
E       oil_bbl_rolling_3m: double
E       oil_bbl_rolling_6m: double
E       oil_bbl_rolling_12m: double
E       oil_bbl_lag1: double
E       gas_mcf_rolling_3m: double
E       gas_mcf_rolling_6m: double
E       gas_mcf_rolling_12m: double
E       gas_mcf_lag1: double
E       water_bbl_rolling_3m: double
E       water_bbl_rolling_6m: double
E       water_bbl_rolling_12m: double
E       water_bbl_lag1: double
E       well_age_months: double
E       oil_bbl_30d: double
E       gas_mcf_30d: double
E       water_bbl_30d: double
E       op_name_encoded: int32
E       county_encoded: int32
E   
E   This error *may* be resolved by passing in schema information for
E   the mismatched column(s) using the `schema` keyword in `to_parquet`.
=============================== warnings summary ===============================
tests/test_ingest.py::test_read_raw_csv_filters_to_2020_plus
tests/test_ingest.py::test_read_raw_csv_latin1_fallback
tests/test_ingest.py::test_run_ingest_returns_path
tests/test_ingest.py::test_run_ingest_logs_validation_warnings
  /cogcc_pipeline/ingest.py:204: Pandas4Warning: For backward compatibility, 'str' dtypes are included by select_dtypes when 'object' dtype is specified. This behavior is deprecated and will be removed in a future version. Explicitly pass 'str' to `include` to select them, or to `exclude` to remove them and silence this warning.
  See https://pandas.pydata.org/docs/user_guide/migration-3-strings.html#string-migration-select-dtypes for details on how to write code that works with pandas 2 and 3.
    str_cols = df.select_dtypes(include="object").columns

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
=========================== short test summary info ============================
FAILED tests/test_features.py::test_run_features_logs_schema_error_without_raising
1 failed, 128 passed, 4 warnings in 11.20s

```

---

## Eval Run at 2026-04-05 13:42:57

**Status:** ✅ PASSED

---
