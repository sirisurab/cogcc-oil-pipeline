
## Eval Run at 2026-04-27 12:59:18

**Status:** ❌ FAILED

### Failures:
- **Linting:**
```
Linting failed. Fix these errors:
F841 Local variable `schema` is assigned to but never used
   --> tests/test_transform.py:270:5
    |
268 | @pytest.mark.unit
269 | def test_deduplicate_keeps_most_recent_accepted_date() -> None:
270 |     schema = load_data_dictionary(DD_PATH)
    |     ^^^^^^
271 |     p1 = _make_partition(oil=100.0, accepted_date="2021-03-01")
272 |     p2 = _make_partition(oil=120.0, accepted_date="2021-04-01")
    |
help: Remove assignment to unused variable `schema`

Found 1 error.
No fixes available (1 hidden fix can be enabled with the `--unsafe-fixes` option).

```

- **Type check:**
```
Type check failed. Fix these errors:
/cogcc_pipeline/ingest.py:81: error: Incompatible types in assignment (expression has type "object", variable has type "CategoricalDtype")  [assignment]
/cogcc_pipeline/ingest.py:99: error: No overload variant of "where" of "Series" matches argument types "Series[bool]", "NAType"  [call-overload]
/cogcc_pipeline/ingest.py:99: note: Possible overload variants:
/cogcc_pipeline/ingest.py:99: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: Any | Series[Any] | Callable[..., Any | Series[Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/cogcc_pipeline/ingest.py:99: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[Any]
/cogcc_pipeline/ingest.py:123: error: No overload variant of "astype" of "Series" matches argument type "object"  [call-overload]
/cogcc_pipeline/ingest.py:123: note: Error code "call-overload" not covered by "type: ignore" comment
/cogcc_pipeline/ingest.py:123: note: Possible overload variants:
/cogcc_pipeline/ingest.py:123: note:     def astype(self, dtype: type[builtins.bool] | Literal['bool'] | BooleanDtype | Literal['boolean'] | Literal['?', 'b1', 'bool_'] | type[numpy.bool[builtins.bool]] | Literal['bool[pyarrow]', 'boolean[pyarrow]'], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[bool]
/cogcc_pipeline/ingest.py:123: note:     def astype(self, dtype: Literal['int', 'Int8', 'Int16', 'Int32', 'Int64', 'b', 'i1', 'int8', 'byte', 'h', 'i2', 'int16', 'short', 'i', 'i4', 'int32', 'intc', 'l', 'long', 'i8', 'int64', 'int_', 'q', 'longlong', 'p', 'intp', 'int8[pyarrow]', 'int16[pyarrow]', 'int32[pyarrow]', 'int64[pyarrow]', 'UInt8', 'UInt16', 'UInt32', 'UInt64', 'B', 'u1', 'uint8', 'ubyte', 'H', 'u2', 'uint16', 'ushort', 'I', 'u4', 'uint32', 'uintc', 'L', 'ulong', 'u8', 'uint', 'uint64', 'Q', 'ulonglong', 'P', 'uintp', 'uint8[pyarrow]', 'uint16[pyarrow]', 'uint32[pyarrow]', 'uint64[pyarrow]'] | type[int] | Int8Dtype | Int16Dtype | Int32Dtype | Int64Dtype | <14 more items>, copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[int]
/cogcc_pipeline/ingest.py:123: note:     def astype(self, dtype: type[str] | Literal['str'] | StringDtype[None] | Literal['string'] | StringDtype[Literal['python']] | Literal['string[python]'] | Literal['U', 'str_', 'unicode'] | type[str_] | StringDtype[Literal['pyarrow']] | Literal['string[pyarrow]'], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[str]
/cogcc_pipeline/ingest.py:123: note:     def astype(self, dtype: type[bytes] | Literal['bytes'] | Literal['S', 'bytes_'] | type[bytes_] | Literal['binary[pyarrow]'], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[bytes]
/cogcc_pipeline/ingest.py:123: note:     def astype(self, dtype: type[float] | Literal['float'] | Literal['Float32', 'Float64'] | Float32Dtype | Float64Dtype | Literal['e', 'f2', '<f2', 'float16', 'half'] | type[floating[_16Bit]] | Literal['f', 'f4', 'float32', 'single', 'd', 'f8', 'float64', 'double', 'g', 'f16', 'float128', 'longdouble'] | type[floating[_32Bit]] | type[float64] | type[floating[_64Bit | _96Bit | _128Bit]] | Literal['float[pyarrow]', 'double[pyarrow]', 'float16[pyarrow]', 'float32[pyarrow]', 'float64[pyarrow]'], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[float]
/cogcc_pipeline/ingest.py:123: note:     def astype(self, dtype: type[complex] | Literal['complex'] | Literal['F', 'c8', 'complex64', 'csingle', 'D', 'c16', 'complex128', 'cdouble', 'G', 'c32', 'complex256', 'clongdouble'] | type[complexfloating[_32Bit, _32Bit]] | type[complex128] | type[complexfloating[_64Bit | _96Bit | _128Bit, _64Bit | _96Bit | _128Bit]], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[complex]
/cogcc_pipeline/ingest.py:123: note:     def astype(self, dtype: Literal['timedelta64[s]', 'timedelta64[ms]', 'timedelta64[us]', 'timedelta64[ns]', 'm8[s]', 'm8[ms]', 'm8[us]', 'm8[ns]', '<m8[s]', '<m8[ms]', '<m8[us]', '<m8[ns]', 'duration[s][pyarrow]', 'duration[ms][pyarrow]', 'duration[us][pyarrow]', 'duration[ns][pyarrow]', 'timedelta64[Y]', 'timedelta64[M]', 'timedelta64[W]', 'timedelta64[D]', 'timedelta64[h]', 'timedelta64[m]', 'timedelta64[μs]', 'timedelta64[ps]', 'timedelta64[fs]', 'timedelta64[as]', 'm8[Y]', 'm8[M]', 'm8[W]', 'm8[D]', 'm8[h]', 'm8[m]', 'm8[μs]', 'm8[ps]', 'm8[fs]', 'm8[as]', '<m8[Y]', '<m8[M]', '<m8[W]', '<m8[D]', '<m8[h]', '<m8[m]', '<m8[μs]', '<m8[ps]', '<m8[fs]', '<m8[as]'], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[Timedelta]
/cogcc_pipeline/ingest.py:123: note:     def astype(self, dtype: Literal['datetime64[s, UTC]', 'datetime64[ms, UTC]', 'datetime64[us, UTC]', 'datetime64[ns, UTC]', 'datetime64[s]', 'datetime64[ms]', 'datetime64[us]', 'datetime64[ns]', 'M8[s]', 'M8[ms]', 'M8[us]', 'M8[ns]', '<M8[s]', '<M8[ms]', '<M8[us]', '<M8[ns]', 'date32[pyarrow]', 'date64[pyarrow]', 'timestamp[s][pyarrow]', 'timestamp[ms][pyarrow]', 'timestamp[us][pyarrow]', 'timestamp[ns][pyarrow]', 'datetime64[Y]', 'datetime64[M]', 'datetime64[W]', 'datetime64[D]', 'datetime64[h]', 'datetime64[m]', 'datetime64[μs]', 'datetime64[ps]', 'datetime64[fs]', 'datetime64[as]', 'M8[Y]', 'M8[M]', 'M8[W]', 'M8[D]', 'M8[h]', 'M8[m]', 'M8[μs]', 'M8[ps]', 'M8[fs]', 'M8[as]', '<M8[Y]', '<M8[M]', '<M8[W]', '<M8[D]', '<M8[h]', '<M8[m]', '<M8[μs]', '<M8[ps]', '<M8[fs]', '<M8[as]'], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[Timestamp]
/cogcc_pipeline/ingest.py:123: note:     def astype(self, dtype: CategoricalDtype | Literal['category'], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[CategoricalDtype]
/cogcc_pipeline/ingest.py:123: note:     def astype(self, dtype: Literal['object', 'object_', 'O', 'V', 'void'] | type[object] | type[object_] | type[void] | ExtensionDtype | dtype[generic[Any]], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[Any]
/cogcc_pipeline/ingest.py:150: error: Argument "dtype" to "array" has incompatible type "StringDtype[Any] | BooleanDtype | Int64Dtype"; expected "BooleanDtype | Literal['boolean']"  [arg-type]
/cogcc_pipeline/features.py:38: error: No overload variant of "groupby" of "DataFrame" matches argument types "Hashable", "bool"  [call-overload]
/cogcc_pipeline/features.py:38: note: Possible overload variants:
/cogcc_pipeline/features.py:38: note:     def groupby(self, by: str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], Literal[True]]
/cogcc_pipeline/features.py:38: note:     def groupby(self, by: str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], Literal[False]]
/cogcc_pipeline/features.py:38: note:     def groupby(self, by: DatetimeIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timestamp, Literal[True]]
/cogcc_pipeline/features.py:38: note:     def groupby(self, by: DatetimeIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timestamp, Literal[False]]
/cogcc_pipeline/features.py:38: note:     def groupby(self, by: TimedeltaIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timedelta, Literal[True]]
/cogcc_pipeline/features.py:38: note:     def groupby(self, by: TimedeltaIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timedelta, Literal[False]]
/cogcc_pipeline/features.py:38: note:     def groupby(self, by: PeriodIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Period, Literal[True]]
/cogcc_pipeline/features.py:38: note:     def groupby(self, by: PeriodIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Period, Literal[False]]
/cogcc_pipeline/features.py:38: note:     def [IntervalT: Interval[Any]] groupby(self, by: IntervalIndex[IntervalT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[IntervalT, Literal[True]]
/cogcc_pipeline/features.py:38: note:     def [IntervalT: Interval[Any]] groupby(self, by: IntervalIndex[IntervalT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[IntervalT, Literal[False]]
/cogcc_pipeline/features.py:38: note:     def groupby(self, by: MultiIndex | tuple[Any, ...] | list[Any] | ufunc | Callable[..., Any] | list[ufunc | Callable[..., Any]] | list[Series[Any]] | <7 more items> | None = ..., level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[tuple[Hashable, ...], Literal[True]]
/cogcc_pipeline/features.py:38: note:     def groupby(self, by: MultiIndex | tuple[Any, ...] | list[Any] | ufunc | Callable[..., Any] | list[ufunc | Callable[..., Any]] | list[Series[Any]] | <7 more items> | None = ..., level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[tuple[Hashable, ...], Literal[False]]
/cogcc_pipeline/features.py:38: note:     def [SeriesByT: str | bytes | date | bool | int | <6 more items>] groupby(self, by: Series[SeriesByT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[SeriesByT, Literal[True]]
/cogcc_pipeline/features.py:38: note:     def [SeriesByT: str | bytes | date | bool | int | <6 more items>] groupby(self, by: Series[SeriesByT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[SeriesByT, Literal[False]]
/cogcc_pipeline/features.py:38: note:     def groupby(self, by: CategoricalIndex[Any] | Index[Any] | Series[Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Any, Literal[True]]
/cogcc_pipeline/features.py:38: note:     def groupby(self, by: CategoricalIndex[Any] | Index[Any] | Series[Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Any, Literal[False]]
/cogcc_pipeline/features.py:88: error: No overload variant of "groupby" of "DataFrame" matches argument types "Hashable", "bool"  [call-overload]
/cogcc_pipeline/features.py:88: note: Possible overload variants:
/cogcc_pipeline/features.py:88: note:     def groupby(self, by: str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], Literal[True]]
/cogcc_pipeline/features.py:88: note:     def groupby(self, by: str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], Literal[False]]
/cogcc_pipeline/features.py:88: note:     def groupby(self, by: DatetimeIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timestamp, Literal[True]]
/cogcc_pipeline/features.py:88: note:     def groupby(self, by: DatetimeIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timestamp, Literal[False]]
/cogcc_pipeline/features.py:88: note:     def groupby(self, by: TimedeltaIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timedelta, Literal[True]]
/cogcc_pipeline/features.py:88: note:     def groupby(self, by: TimedeltaIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timedelta, Literal[False]]
/cogcc_pipeline/features.py:88: note:     def groupby(self, by: PeriodIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Period, Literal[True]]
/cogcc_pipeline/features.py:88: note:     def groupby(self, by: PeriodIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Period, Literal[False]]
/cogcc_pipeline/features.py:88: note:     def [IntervalT: Interval[Any]] groupby(self, by: IntervalIndex[IntervalT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[IntervalT, Literal[True]]
/cogcc_pipeline/features.py:88: note:     def [IntervalT: Interval[Any]] groupby(self, by: IntervalIndex[IntervalT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[IntervalT, Literal[False]]
/cogcc_pipeline/features.py:88: note:     def groupby(self, by: MultiIndex | tuple[Any, ...] | list[Any] | ufunc | Callable[..., Any] | list[ufunc | Callable[..., Any]] | list[Series[Any]] | <7 more items> | None = ..., level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[tuple[Hashable, ...], Literal[True]]
/cogcc_pipeline/features.py:88: note:     def groupby(self, by: MultiIndex | tuple[Any, ...] | list[Any] | ufunc | Callable[..., Any] | list[ufunc | Callable[..., Any]] | list[Series[Any]] | <7 more items> | None = ..., level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[tuple[Hashable, ...], Literal[False]]
/cogcc_pipeline/features.py:88: note:     def [SeriesByT: str | bytes | date | bool | int | <6 more items>] groupby(self, by: Series[SeriesByT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[SeriesByT, Literal[True]]
/cogcc_pipeline/features.py:88: note:     def [SeriesByT: str | bytes | date | bool | int | <6 more items>] groupby(self, by: Series[SeriesByT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[SeriesByT, Literal[False]]
/cogcc_pipeline/features.py:88: note:     def groupby(self, by: CategoricalIndex[Any] | Index[Any] | Series[Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Any, Literal[True]]
/cogcc_pipeline/features.py:88: note:     def groupby(self, by: CategoricalIndex[Any] | Index[Any] | Series[Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Any, Literal[False]]
/cogcc_pipeline/features.py:112: error: No overload variant of "groupby" of "DataFrame" matches argument types "Hashable", "bool"  [call-overload]
/cogcc_pipeline/features.py:112: note: Possible overload variants:
/cogcc_pipeline/features.py:112: note:     def groupby(self, by: str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], Literal[True]]
/cogcc_pipeline/features.py:112: note:     def groupby(self, by: str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], Literal[False]]
/cogcc_pipeline/features.py:112: note:     def groupby(self, by: DatetimeIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timestamp, Literal[True]]
/cogcc_pipeline/features.py:112: note:     def groupby(self, by: DatetimeIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timestamp, Literal[False]]
/cogcc_pipeline/features.py:112: note:     def groupby(self, by: TimedeltaIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timedelta, Literal[True]]
/cogcc_pipeline/features.py:112: note:     def groupby(self, by: TimedeltaIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timedelta, Literal[False]]
/cogcc_pipeline/features.py:112: note:     def groupby(self, by: PeriodIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Period, Literal[True]]
/cogcc_pipeline/features.py:112: note:     def groupby(self, by: PeriodIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Period, Literal[False]]
/cogcc_pipeline/features.py:112: note:     def [IntervalT: Interval[Any]] groupby(self, by: IntervalIndex[IntervalT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[IntervalT, Literal[True]]
/cogcc_pipeline/features.py:112: note:     def [IntervalT: Interval[Any]] groupby(self, by: IntervalIndex[IntervalT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[IntervalT, Literal[False]]
/cogcc_pipeline/features.py:112: note:     def groupby(self, by: MultiIndex | tuple[Any, ...] | list[Any] | ufunc | Callable[..., Any] | list[ufunc | Callable[..., Any]] | list[Series[Any]] | <7 more items> | None = ..., level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[tuple[Hashable, ...], Literal[True]]
/cogcc_pipeline/features.py:112: note:     def groupby(self, by: MultiIndex | tuple[Any, ...] | list[Any] | ufunc | Callable[..., Any] | list[ufunc | Callable[..., Any]] | list[Series[Any]] | <7 more items> | None = ..., level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[tuple[Hashable, ...], Literal[False]]
/cogcc_pipeline/features.py:112: note:     def [SeriesByT: str | bytes | date | bool | int | <6 more items>] groupby(self, by: Series[SeriesByT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[SeriesByT, Literal[True]]
/cogcc_pipeline/features.py:112: note:     def [SeriesByT: str | bytes | date | bool | int | <6 more items>] groupby(self, by: Series[SeriesByT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[SeriesByT, Literal[False]]
/cogcc_pipeline/features.py:112: note:     def groupby(self, by: CategoricalIndex[Any] | Index[Any] | Series[Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Any, Literal[True]]
/cogcc_pipeline/features.py:112: note:     def groupby(self, by: CategoricalIndex[Any] | Index[Any] | Series[Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Any, Literal[False]]
/cogcc_pipeline/transform.py:96: error: No overload variant of "where" of "Series" matches argument types "Series[bool]", "NAType"  [call-overload]
/cogcc_pipeline/transform.py:96: note: Possible overload variants:
/cogcc_pipeline/transform.py:96: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: Any | Series[Any] | Callable[..., Any | Series[Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/cogcc_pipeline/transform.py:96: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[Any]
/tests/test_transform.py:540: error: "transform" does not return a value (it only ever returns None)  [func-returns-value]
/tests/test_features.py:126: error: Argument 1 to "assert_array_almost_equal" has incompatible type "ndarray[tuple[int], dtype[Any]] | ExtensionArray"; expected "_SupportsArray[dtype[numpy.bool[builtins.bool] | number[Any, int | float | complex]]] | _NestedSequence[_SupportsArray[dtype[numpy.bool[builtins.bool] | number[Any, int | float | complex]]]] | complex | _NestedSequence[complex] | _SupportsArray[dtype[object_]] | _NestedSequence[_SupportsArray[dtype[object_]]]"  [arg-type]
/tests/test_features.py:532: error: Invalid index type "tuple[int, int | slice[Any, Any, Any] | ndarray[tuple[int], dtype[numpy.bool[builtins.bool]]]]" for "_iLocIndexerFrame[DataFrame]"; expected type "tuple[slice[Any, Any, Any], Hashable]"  [index]
Found 11 errors in 5 files (checked 12 source files)

```

- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
...........F..FFF....................................F.FFF.FFFFFFFF..... [ 71%]
...FF.................F......                                            [100%]Running teardown with pytest sessionfinish...

=================================== FAILURES ===================================
______________________ test_acquire_tolerates_none_result ______________________
tests/test_acquire.py:249: in test_acquire_tolerates_none_result
    assert results[1] is None
E   AssertionError: assert PosixPath('/private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-67/test_acquire_tolerates_none_re0/a.csv') is None
____________________ test_cumulative_features_known_values _____________________
tests/test_features.py:123: in test_cumulative_features_known_values
    df = _make_transform_output(oil_vals=[100, 200, 150, 300])
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
tests/test_features.py:62: in _make_transform_output
    df = pd.DataFrame(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:769: in __init__
    mgr = dict_to_mgr(data, index, columns, dtype=dtype, copy=copy)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:460: in dict_to_mgr
    return arrays_to_mgr(arrays, columns, index, dtype=dtype, consolidate=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:118: in arrays_to_mgr
    arrays, refs = _homogenize(arrays, index, dtype)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:597: in _homogenize
    com.require_length_match(val, index)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/common.py:601: in require_length_match
    raise ValueError(
E   ValueError: Length of values (4) does not match length of index (6)
_____________ test_cumulative_features_flat_during_zero_production _____________
tests/test_features.py:132: in test_cumulative_features_flat_during_zero_production
    df = _make_transform_output(oil_vals=[100, 0, 0, 200])
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
tests/test_features.py:62: in _make_transform_output
    df = pd.DataFrame(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:769: in __init__
    mgr = dict_to_mgr(data, index, columns, dtype=dtype, copy=copy)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:460: in dict_to_mgr
    return arrays_to_mgr(arrays, columns, index, dtype=dtype, consolidate=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:118: in arrays_to_mgr
    arrays, refs = _homogenize(arrays, index, dtype)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:597: in _homogenize
    com.require_length_match(val, index)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/common.py:601: in require_length_match
    raise ValueError(
E   ValueError: Length of values (4) does not match length of index (6)
____________________ test_cumulative_features_zero_at_start ____________________
tests/test_features.py:143: in test_cumulative_features_zero_at_start
    df = _make_transform_output(oil_vals=[0, 0, 100, 200])
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
tests/test_features.py:62: in _make_transform_output
    df = pd.DataFrame(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:769: in __init__
    mgr = dict_to_mgr(data, index, columns, dtype=dtype, copy=copy)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:460: in dict_to_mgr
    return arrays_to_mgr(arrays, columns, index, dtype=dtype, consolidate=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:118: in arrays_to_mgr
    arrays, refs = _homogenize(arrays, index, dtype)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:597: in _homogenize
    com.require_length_match(val, index)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/common.py:601: in require_length_match
    raise ValueError(
E   ValueError: Length of values (4) does not match length of index (6)
______________ test_load_data_dictionary_docnum_is_nullable_int64 ______________
tests/test_ingest.py:91: in test_load_data_dictionary_docnum_is_nullable_int64
    assert isinstance(schema["DocNum"]["dtype"], pd.Int64Dtype)
E   AssertionError: assert False
E    +  where False = isinstance('object', <class 'pandas.Int64Dtype'>)
E    +    where <class 'pandas.Int64Dtype'> = pd.Int64Dtype
_________________ test_read_raw_file_correct_dtypes_spot_check _________________
tests/test_ingest.py:110: in test_read_raw_file_correct_dtypes_spot_check
    df = read_raw_file(fp, schema)
         ^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:142: in read_raw_file
    columns[col] = _cast_column(raw_df[col], dtype, nullable)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:106: in _cast_column
    return series.astype(dtype)
           ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/generic.py:6541: in astype
    new_data = self._mgr.astype(dtype=dtype, errors=errors)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:611: in astype
    return self.apply("astype", dtype=dtype, errors=errors)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:442: in apply
    applied = getattr(b, f)(**kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/blocks.py:607: in astype
    new_values = astype_array_safe(values, dtype, errors=errors)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:240: in astype_array_safe
    new_values = astype_array(values, dtype, copy=copy)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:185: in astype_array
    values = _astype_nansafe(values, dtype, copy=copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:81: in _astype_nansafe
    return dtype.construct_array_type()._from_sequence(arr, dtype=dtype, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/masked.py:161: in _from_sequence
    values, mask = cls._coerce_to_array(scalars, dtype=dtype, copy=copy)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:387: in _coerce_to_array
    return coerce_to_array(value, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:226: in coerce_to_array
    raise TypeError("Need to pass bool-like values")
E   TypeError: Need to pass bool-like values
_____________ test_read_raw_file_nullable_absent_column_is_all_na ______________
tests/test_ingest.py:128: in test_read_raw_file_nullable_absent_column_is_all_na
    df = read_raw_file(str(p), schema)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:142: in read_raw_file
    columns[col] = _cast_column(raw_df[col], dtype, nullable)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:106: in _cast_column
    return series.astype(dtype)
           ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/generic.py:6541: in astype
    new_data = self._mgr.astype(dtype=dtype, errors=errors)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:611: in astype
    return self.apply("astype", dtype=dtype, errors=errors)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:442: in apply
    applied = getattr(b, f)(**kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/blocks.py:607: in astype
    new_values = astype_array_safe(values, dtype, errors=errors)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:240: in astype_array_safe
    new_values = astype_array(values, dtype, copy=copy)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:185: in astype_array
    values = _astype_nansafe(values, dtype, copy=copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:81: in _astype_nansafe
    return dtype.construct_array_type()._from_sequence(arr, dtype=dtype, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/masked.py:161: in _from_sequence
    values, mask = cls._coerce_to_array(scalars, dtype=dtype, copy=copy)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:387: in _coerce_to_array
    return coerce_to_array(value, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:226: in coerce_to_array
    raise TypeError("Need to pass bool-like values")
E   TypeError: Need to pass bool-like values
___________ test_read_raw_file_raises_on_missing_nonnullable_column ____________
tests/test_ingest.py:147: in test_read_raw_file_raises_on_missing_nonnullable_column
    read_raw_file(str(p), schema)
cogcc_pipeline/ingest.py:142: in read_raw_file
    columns[col] = _cast_column(raw_df[col], dtype, nullable)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:106: in _cast_column
    return series.astype(dtype)
           ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/generic.py:6541: in astype
    new_data = self._mgr.astype(dtype=dtype, errors=errors)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:611: in astype
    return self.apply("astype", dtype=dtype, errors=errors)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:442: in apply
    applied = getattr(b, f)(**kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/blocks.py:607: in astype
    new_values = astype_array_safe(values, dtype, errors=errors)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:240: in astype_array_safe
    new_values = astype_array(values, dtype, copy=copy)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:185: in astype_array
    values = _astype_nansafe(values, dtype, copy=copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:81: in _astype_nansafe
    return dtype.construct_array_type()._from_sequence(arr, dtype=dtype, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/masked.py:161: in _from_sequence
    values, mask = cls._coerce_to_array(scalars, dtype=dtype, copy=copy)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:387: in _coerce_to_array
    return coerce_to_array(value, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:226: in coerce_to_array
    raise TypeError("Need to pass bool-like values")
E   TypeError: Need to pass bool-like values
________________ test_read_raw_file_float_cols_use_nan_not_pdNA ________________
tests/test_ingest.py:176: in test_read_raw_file_float_cols_use_nan_not_pdNA
    df = read_raw_file(str(p), schema)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:142: in read_raw_file
    columns[col] = _cast_column(raw_df[col], dtype, nullable)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:106: in _cast_column
    return series.astype(dtype)
           ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/generic.py:6541: in astype
    new_data = self._mgr.astype(dtype=dtype, errors=errors)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:611: in astype
    return self.apply("astype", dtype=dtype, errors=errors)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:442: in apply
    applied = getattr(b, f)(**kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/blocks.py:607: in astype
    new_values = astype_array_safe(values, dtype, errors=errors)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:240: in astype_array_safe
    new_values = astype_array(values, dtype, copy=copy)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:185: in astype_array
    values = _astype_nansafe(values, dtype, copy=copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:81: in _astype_nansafe
    return dtype.construct_array_type()._from_sequence(arr, dtype=dtype, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/masked.py:161: in _from_sequence
    values, mask = cls._coerce_to_array(scalars, dtype=dtype, copy=copy)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:387: in _coerce_to_array
    return coerce_to_array(value, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:226: in coerce_to_array
    raise TypeError("Need to pass bool-like values")
E   TypeError: Need to pass bool-like values
__________________ test_build_dask_dataframe_returns_dask_df ___________________
tests/test_ingest.py:191: in test_build_dask_dataframe_returns_dask_df
    ddf = build_dask_dataframe(fps, schema)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:174: in build_dask_dataframe
    meta = read_raw_file(file_paths[0], data_dict).iloc[0:0]
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:142: in read_raw_file
    columns[col] = _cast_column(raw_df[col], dtype, nullable)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:106: in _cast_column
    return series.astype(dtype)
           ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/generic.py:6541: in astype
    new_data = self._mgr.astype(dtype=dtype, errors=errors)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:611: in astype
    return self.apply("astype", dtype=dtype, errors=errors)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:442: in apply
    applied = getattr(b, f)(**kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/blocks.py:607: in astype
    new_values = astype_array_safe(values, dtype, errors=errors)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:240: in astype_array_safe
    new_values = astype_array(values, dtype, copy=copy)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:185: in astype_array
    values = _astype_nansafe(values, dtype, copy=copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:81: in _astype_nansafe
    return dtype.construct_array_type()._from_sequence(arr, dtype=dtype, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/masked.py:161: in _from_sequence
    values, mask = cls._coerce_to_array(scalars, dtype=dtype, copy=copy)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:387: in _coerce_to_array
    return coerce_to_array(value, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:226: in coerce_to_array
    raise TypeError("Need to pass bool-like values")
E   TypeError: Need to pass bool-like values
_______________ test_build_dask_dataframe_has_canonical_columns ________________
tests/test_ingest.py:199: in test_build_dask_dataframe_has_canonical_columns
    ddf = build_dask_dataframe(fps, schema)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:174: in build_dask_dataframe
    meta = read_raw_file(file_paths[0], data_dict).iloc[0:0]
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:142: in read_raw_file
    columns[col] = _cast_column(raw_df[col], dtype, nullable)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:106: in _cast_column
    return series.astype(dtype)
           ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/generic.py:6541: in astype
    new_data = self._mgr.astype(dtype=dtype, errors=errors)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:611: in astype
    return self.apply("astype", dtype=dtype, errors=errors)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:442: in apply
    applied = getattr(b, f)(**kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/blocks.py:607: in astype
    new_values = astype_array_safe(values, dtype, errors=errors)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:240: in astype_array_safe
    new_values = astype_array(values, dtype, copy=copy)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:185: in astype_array
    values = _astype_nansafe(values, dtype, copy=copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:81: in _astype_nansafe
    return dtype.construct_array_type()._from_sequence(arr, dtype=dtype, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/masked.py:161: in _from_sequence
    values, mask = cls._coerce_to_array(scalars, dtype=dtype, copy=copy)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:387: in _coerce_to_array
    return coerce_to_array(value, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:226: in coerce_to_array
    raise TypeError("Need to pass bool-like values")
E   TypeError: Need to pass bool-like values
____________ test_build_dask_dataframe_meta_dtypes_match_data_dict _____________
tests/test_ingest.py:207: in test_build_dask_dataframe_meta_dtypes_match_data_dict
    ddf = build_dask_dataframe(fps, schema)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:174: in build_dask_dataframe
    meta = read_raw_file(file_paths[0], data_dict).iloc[0:0]
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:142: in read_raw_file
    columns[col] = _cast_column(raw_df[col], dtype, nullable)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:106: in _cast_column
    return series.astype(dtype)
           ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/generic.py:6541: in astype
    new_data = self._mgr.astype(dtype=dtype, errors=errors)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:611: in astype
    return self.apply("astype", dtype=dtype, errors=errors)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:442: in apply
    applied = getattr(b, f)(**kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/blocks.py:607: in astype
    new_values = astype_array_safe(values, dtype, errors=errors)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:240: in astype_array_safe
    new_values = astype_array(values, dtype, copy=copy)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:185: in astype_array
    values = _astype_nansafe(values, dtype, copy=copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:81: in _astype_nansafe
    return dtype.construct_array_type()._from_sequence(arr, dtype=dtype, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/masked.py:161: in _from_sequence
    values, mask = cls._coerce_to_array(scalars, dtype=dtype, copy=copy)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:387: in _coerce_to_array
    return coerce_to_array(value, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:226: in coerce_to_array
    raise TypeError("Need to pass bool-like values")
E   TypeError: Need to pass bool-like values
___________________ test_filter_year_keeps_only_target_years ___________________
tests/test_ingest.py:226: in test_filter_year_keeps_only_target_years
    ddf = build_dask_dataframe([fp], schema)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:174: in build_dask_dataframe
    meta = read_raw_file(file_paths[0], data_dict).iloc[0:0]
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:142: in read_raw_file
    columns[col] = _cast_column(raw_df[col], dtype, nullable)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:106: in _cast_column
    return series.astype(dtype)
           ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/generic.py:6541: in astype
    new_data = self._mgr.astype(dtype=dtype, errors=errors)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:611: in astype
    return self.apply("astype", dtype=dtype, errors=errors)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:442: in apply
    applied = getattr(b, f)(**kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/blocks.py:607: in astype
    new_values = astype_array_safe(values, dtype, errors=errors)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:240: in astype_array_safe
    new_values = astype_array(values, dtype, copy=copy)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:185: in astype_array
    values = _astype_nansafe(values, dtype, copy=copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:81: in _astype_nansafe
    return dtype.construct_array_type()._from_sequence(arr, dtype=dtype, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/masked.py:161: in _from_sequence
    values, mask = cls._coerce_to_array(scalars, dtype=dtype, copy=copy)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:387: in _coerce_to_array
    return coerce_to_array(value, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:226: in coerce_to_array
    raise TypeError("Need to pass bool-like values")
E   TypeError: Need to pass bool-like values
____________ test_filter_year_all_before_min_returns_empty_dask_df _____________
tests/test_ingest.py:237: in test_filter_year_all_before_min_returns_empty_dask_df
    ddf = build_dask_dataframe([fp], schema)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:174: in build_dask_dataframe
    meta = read_raw_file(file_paths[0], data_dict).iloc[0:0]
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:142: in read_raw_file
    columns[col] = _cast_column(raw_df[col], dtype, nullable)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:106: in _cast_column
    return series.astype(dtype)
           ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/generic.py:6541: in astype
    new_data = self._mgr.astype(dtype=dtype, errors=errors)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:611: in astype
    return self.apply("astype", dtype=dtype, errors=errors)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:442: in apply
    applied = getattr(b, f)(**kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/blocks.py:607: in astype
    new_values = astype_array_safe(values, dtype, errors=errors)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:240: in astype_array_safe
    new_values = astype_array(values, dtype, copy=copy)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:185: in astype_array
    values = _astype_nansafe(values, dtype, copy=copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:81: in _astype_nansafe
    return dtype.construct_array_type()._from_sequence(arr, dtype=dtype, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/masked.py:161: in _from_sequence
    values, mask = cls._coerce_to_array(scalars, dtype=dtype, copy=copy)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:387: in _coerce_to_array
    return coerce_to_array(value, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:226: in coerce_to_array
    raise TypeError("Need to pass bool-like values")
E   TypeError: Need to pass bool-like values
___________________ test_filter_year_returns_dask_dataframe ____________________
tests/test_ingest.py:248: in test_filter_year_returns_dask_dataframe
    ddf = build_dask_dataframe([fp], schema)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:174: in build_dask_dataframe
    meta = read_raw_file(file_paths[0], data_dict).iloc[0:0]
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:142: in read_raw_file
    columns[col] = _cast_column(raw_df[col], dtype, nullable)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:106: in _cast_column
    return series.astype(dtype)
           ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/generic.py:6541: in astype
    new_data = self._mgr.astype(dtype=dtype, errors=errors)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:611: in astype
    return self.apply("astype", dtype=dtype, errors=errors)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:442: in apply
    applied = getattr(b, f)(**kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/blocks.py:607: in astype
    new_values = astype_array_safe(values, dtype, errors=errors)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:240: in astype_array_safe
    new_values = astype_array(values, dtype, copy=copy)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:185: in astype_array
    values = _astype_nansafe(values, dtype, copy=copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:81: in _astype_nansafe
    return dtype.construct_array_type()._from_sequence(arr, dtype=dtype, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/masked.py:161: in _from_sequence
    values, mask = cls._coerce_to_array(scalars, dtype=dtype, copy=copy)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:387: in _coerce_to_array
    return coerce_to_array(value, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:226: in coerce_to_array
    raise TypeError("Need to pass bool-like values")
E   TypeError: Need to pass bool-like values
__________________ test_write_parquet_creates_readable_files ___________________
tests/test_ingest.py:262: in test_write_parquet_creates_readable_files
    ddf = build_dask_dataframe(fps, schema)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:174: in build_dask_dataframe
    meta = read_raw_file(file_paths[0], data_dict).iloc[0:0]
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:142: in read_raw_file
    columns[col] = _cast_column(raw_df[col], dtype, nullable)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:106: in _cast_column
    return series.astype(dtype)
           ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/generic.py:6541: in astype
    new_data = self._mgr.astype(dtype=dtype, errors=errors)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:611: in astype
    return self.apply("astype", dtype=dtype, errors=errors)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:442: in apply
    applied = getattr(b, f)(**kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/blocks.py:607: in astype
    new_values = astype_array_safe(values, dtype, errors=errors)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:240: in astype_array_safe
    new_values = astype_array(values, dtype, copy=copy)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:185: in astype_array
    values = _astype_nansafe(values, dtype, copy=copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:81: in _astype_nansafe
    return dtype.construct_array_type()._from_sequence(arr, dtype=dtype, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/masked.py:161: in _from_sequence
    values, mask = cls._coerce_to_array(scalars, dtype=dtype, copy=copy)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:387: in _coerce_to_array
    return coerce_to_array(value, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:226: in coerce_to_array
    raise TypeError("Need to pass bool-like values")
E   TypeError: Need to pass bool-like values
_________________ test_add_production_date_dtype_is_datetime64 _________________
tests/test_transform.py:159: in test_add_production_date_dtype_is_datetime64
    assert result["production_date"].dtype == np.dtype("datetime64[ns]")
E   AssertionError: assert dtype('<M8[us]') == dtype('<M8[ns]')
E    +  where dtype('<M8[us]') = 0   2022-03-01\nName: production_date, dtype: datetime64[us].dtype
E    +  and   dtype('<M8[ns]') = <class 'numpy.dtype'>('datetime64[ns]')
E    +    where <class 'numpy.dtype'> = np.dtype
_________________ test_add_production_date_zero_row_partition __________________
tests/test_transform.py:168: in test_add_production_date_zero_row_partition
    assert result["production_date"].dtype == np.dtype("datetime64[ns]")
E   AssertionError: assert dtype('<M8[s]') == dtype('<M8[ns]')
E    +  where dtype('<M8[s]') = Series([], Name: production_date, dtype: datetime64[s]).dtype
E    +  and   dtype('<M8[ns]') = <class 'numpy.dtype'>('datetime64[ns]')
E    +    where <class 'numpy.dtype'> = np.dtype
________________ test_transform_partition_production_date_dtype ________________
tests/test_transform.py:363: in test_transform_partition_production_date_dtype
    assert result["production_date"].dtype == np.dtype("datetime64[ns]")
E   AssertionError: assert dtype('<M8[us]') == dtype('<M8[ns]')
E    +  where dtype('<M8[us]') = 0   2021-03-01\nName: production_date, dtype: datetime64[us].dtype
E    +  and   dtype('<M8[ns]') = <class 'numpy.dtype'>('datetime64[ns]')
E    +    where <class 'numpy.dtype'> = np.dtype
=============================== warnings summary ===============================
tests/test_transform.py::test_cast_categoricals_replaces_invalid_value
tests/test_transform.py::test_transform_partition_full_pipeline
  /tests/test_transform.py:97: Pandas4Warning: Constructing a Categorical with a dtype and values containing non-null entries not in that dtype's categories is deprecated and will raise in a future version.
    df[col] = df[col].astype(dtype)

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
=========================== short test summary info ============================
FAILED tests/test_acquire.py::test_acquire_tolerates_none_result - AssertionE...
FAILED tests/test_features.py::test_cumulative_features_known_values - ValueE...
FAILED tests/test_features.py::test_cumulative_features_flat_during_zero_production
FAILED tests/test_features.py::test_cumulative_features_zero_at_start - Value...
FAILED tests/test_ingest.py::test_load_data_dictionary_docnum_is_nullable_int64
FAILED tests/test_ingest.py::test_read_raw_file_correct_dtypes_spot_check - T...
FAILED tests/test_ingest.py::test_read_raw_file_nullable_absent_column_is_all_na
FAILED tests/test_ingest.py::test_read_raw_file_raises_on_missing_nonnullable_column
FAILED tests/test_ingest.py::test_read_raw_file_float_cols_use_nan_not_pdNA
FAILED tests/test_ingest.py::test_build_dask_dataframe_returns_dask_df - Type...
FAILED tests/test_ingest.py::test_build_dask_dataframe_has_canonical_columns
FAILED tests/test_ingest.py::test_build_dask_dataframe_meta_dtypes_match_data_dict
FAILED tests/test_ingest.py::test_filter_year_keeps_only_target_years - TypeE...
FAILED tests/test_ingest.py::test_filter_year_all_before_min_returns_empty_dask_df
FAILED tests/test_ingest.py::test_filter_year_returns_dask_dataframe - TypeEr...
FAILED tests/test_ingest.py::test_write_parquet_creates_readable_files - Type...
FAILED tests/test_transform.py::test_add_production_date_dtype_is_datetime64
FAILED tests/test_transform.py::test_add_production_date_zero_row_partition
FAILED tests/test_transform.py::test_transform_partition_production_date_dtype
19 failed, 82 passed, 14 deselected, 2 warnings in 10.29s

```

- **Integration Tests:**
```
Integration Tests failed. Fix these errors:
F..FFFFFF.FFF.                                                           [100%]Running teardown with pytest sessionfinish...

=================================== FAILURES ===================================
_____________ test_download_file_real_download_produces_valid_file _____________
tests/test_acquire.py:202: in test_download_file_real_download_produces_valid_file
    assert result is not None
E   assert None is not None
------------------------------ Captured log call -------------------------------
WARNING  cogcc_pipeline.acquire:acquire.py:97 Download failed for year=2024 url=https://ecmc.state.co.us/documents/data/downloads/production/monthly_prod.csv: Connection refused by Responses - the call doesn't match any registered mock.

Request: 
- GET https://ecmc.state.co.us/documents/data/downloads/production/monthly_prod.csv

Available matches:
____________________________ test_features_full_run ____________________________
tests/test_features.py:545: in test_features_full_run
    features(cfg)
cogcc_pipeline/features.py:182: in features
    write_parquet(result, output_dir)
cogcc_pipeline/features.py:154: in write_parquet
    ddf.repartition(npartitions=n_out).to_parquet(str(out))
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
E       `ArrowNotImplementedError('Unsupported cast from large_string to null using function cast_null', 'Conversion failed for column WellStatus with type category')`
E   
E   Expected partition schema:
E       DocNum: large_string
E       ReportMonth: int64
E       ReportYear: int64
E       DaysProduced: int64
E       AcceptedDate: timestamp[us]
E       Revised: bool
E       OpName: large_string
E       OpNumber: int64
E       FacilityId: int64
E       ApiCountyCode: large_string
E       ApiSidetrack: large_string
E       Well: large_string
E       WellStatus: dictionary<values=null, indices=int8, ordered=0>
E       FormationCode: large_string
E       OilProduced: double
E       OilSales: double
E       OilAdjustment: double
E       OilGravity: double
E       GasProduced: double
E       GasSales: double
E       GasBtuSales: double
E       GasUsedOnLease: double
E       GasShrinkage: double
E       GasPressureTubing: double
E       GasPressureCasing: double
E       WaterProduced: double
E       WaterPressureTubing: double
E       WaterPressureCasing: double
E       FlaredVented: double
E       BomInvent: double
E       EomInvent: double
E       production_date: timestamp[us]
E       cum_oil: double
E       cum_gas: double
E       cum_water: double
E       gor: double
E       water_cut: double
E       decline_rate_oil: double
E       oil_rolling_3m: double
E       oil_rolling_6m: double
E       oil_lag_1: double
E       gas_rolling_3m: double
E       gas_rolling_6m: double
E       gas_lag_1: double
E       water_rolling_3m: double
E       water_rolling_6m: double
E       water_lag_1: double
E       ApiSequenceNumber: large_string
E   
E   Received partition schema:
E       DocNum: large_string
E       ReportMonth: int64
E       ReportYear: int64
E       DaysProduced: int64
E       AcceptedDate: timestamp[us]
E       Revised: bool
E       OpName: large_string
E       OpNumber: int64
E       FacilityId: int64
E       ApiCountyCode: large_string
E       ApiSidetrack: large_string
E       Well: large_string
E       WellStatus: dictionary<values=large_string, indices=int8, ordered=0>
E       FormationCode: large_string
E       OilProduced: double
E       OilSales: double
E       OilAdjustment: double
E       OilGravity: double
E       GasProduced: double
E       GasSales: double
E       GasBtuSales: double
E       GasUsedOnLease: double
E       GasShrinkage: double
E       GasPressureTubing: double
E       GasPressureCasing: double
E       WaterProduced: double
E       WaterPressureTubing: double
E       WaterPressureCasing: double
E       FlaredVented: double
E       BomInvent: double
E       EomInvent: double
E       production_date: timestamp[us]
E       cum_oil: double
E       cum_gas: double
E       cum_water: double
E       gor: double
E       water_cut: double
E       decline_rate_oil: double
E       oil_rolling_3m: double
E       oil_rolling_6m: double
E       oil_lag_1: double
E       gas_rolling_3m: double
E       gas_rolling_6m: double
E       gas_lag_1: double
E       water_rolling_3m: double
E       water_rolling_6m: double
E       water_lag_1: double
E       ApiSequenceNumber: large_string
E   
E   This error *may* be resolved by passing in schema information for
E   the mismatched column(s) using the `schema` keyword in `to_parquet`.
__________________ test_features_all_expected_derived_columns __________________
tests/test_features.py:578: in test_features_all_expected_derived_columns
    features(cfg)
cogcc_pipeline/features.py:182: in features
    write_parquet(result, output_dir)
cogcc_pipeline/features.py:154: in write_parquet
    ddf.repartition(npartitions=n_out).to_parquet(str(out))
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
E       `ArrowNotImplementedError('Unsupported cast from large_string to null using function cast_null', 'Conversion failed for column WellStatus with type category')`
E   
E   Expected partition schema:
E       DocNum: large_string
E       ReportMonth: int64
E       ReportYear: int64
E       DaysProduced: int64
E       AcceptedDate: timestamp[us]
E       Revised: bool
E       OpName: large_string
E       OpNumber: int64
E       FacilityId: int64
E       ApiCountyCode: large_string
E       ApiSidetrack: large_string
E       Well: large_string
E       WellStatus: dictionary<values=null, indices=int8, ordered=0>
E       FormationCode: large_string
E       OilProduced: double
E       OilSales: double
E       OilAdjustment: double
E       OilGravity: double
E       GasProduced: double
E       GasSales: double
E       GasBtuSales: double
E       GasUsedOnLease: double
E       GasShrinkage: double
E       GasPressureTubing: double
E       GasPressureCasing: double
E       WaterProduced: double
E       WaterPressureTubing: double
E       WaterPressureCasing: double
E       FlaredVented: double
E       BomInvent: double
E       EomInvent: double
E       production_date: timestamp[us]
E       cum_oil: double
E       cum_gas: double
E       cum_water: double
E       gor: double
E       water_cut: double
E       decline_rate_oil: double
E       oil_rolling_3m: double
E       oil_rolling_6m: double
E       oil_lag_1: double
E       gas_rolling_3m: double
E       gas_rolling_6m: double
E       gas_lag_1: double
E       water_rolling_3m: double
E       water_rolling_6m: double
E       water_lag_1: double
E       ApiSequenceNumber: large_string
E   
E   Received partition schema:
E       DocNum: large_string
E       ReportMonth: int64
E       ReportYear: int64
E       DaysProduced: int64
E       AcceptedDate: timestamp[us]
E       Revised: bool
E       OpName: large_string
E       OpNumber: int64
E       FacilityId: int64
E       ApiCountyCode: large_string
E       ApiSidetrack: large_string
E       Well: large_string
E       WellStatus: dictionary<values=large_string, indices=int8, ordered=0>
E       FormationCode: large_string
E       OilProduced: double
E       OilSales: double
E       OilAdjustment: double
E       OilGravity: double
E       GasProduced: double
E       GasSales: double
E       GasBtuSales: double
E       GasUsedOnLease: double
E       GasShrinkage: double
E       GasPressureTubing: double
E       GasPressureCasing: double
E       WaterProduced: double
E       WaterPressureTubing: double
E       WaterPressureCasing: double
E       FlaredVented: double
E       BomInvent: double
E       EomInvent: double
E       production_date: timestamp[us]
E       cum_oil: double
E       cum_gas: double
E       cum_water: double
E       gor: double
E       water_cut: double
E       decline_rate_oil: double
E       oil_rolling_3m: double
E       oil_rolling_6m: double
E       oil_lag_1: double
E       gas_rolling_3m: double
E       gas_rolling_6m: double
E       gas_lag_1: double
E       water_rolling_3m: double
E       water_rolling_6m: double
E       water_lag_1: double
E       ApiSequenceNumber: large_string
E   
E   This error *may* be resolved by passing in schema information for
E   the mismatched column(s) using the `schema` keyword in `to_parquet`.
________________ test_write_parquet_readable_by_pandas_and_dask ________________
tests/test_ingest.py:276: in test_write_parquet_readable_by_pandas_and_dask
    ddf = build_dask_dataframe([fp], schema)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:174: in build_dask_dataframe
    meta = read_raw_file(file_paths[0], data_dict).iloc[0:0]
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:142: in read_raw_file
    columns[col] = _cast_column(raw_df[col], dtype, nullable)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:106: in _cast_column
    return series.astype(dtype)
           ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/generic.py:6541: in astype
    new_data = self._mgr.astype(dtype=dtype, errors=errors)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:611: in astype
    return self.apply("astype", dtype=dtype, errors=errors)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:442: in apply
    applied = getattr(b, f)(**kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/blocks.py:607: in astype
    new_values = astype_array_safe(values, dtype, errors=errors)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:240: in astype_array_safe
    new_values = astype_array(values, dtype, copy=copy)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:185: in astype_array
    values = _astype_nansafe(values, dtype, copy=copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:81: in _astype_nansafe
    return dtype.construct_array_type()._from_sequence(arr, dtype=dtype, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/masked.py:161: in _from_sequence
    values, mask = cls._coerce_to_array(scalars, dtype=dtype, copy=copy)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:387: in _coerce_to_array
    return coerce_to_array(value, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:226: in coerce_to_array
    raise TypeError("Need to pass bool-like values")
E   TypeError: Need to pass bool-like values
________________ test_ingest_writes_parquet_with_correct_schema ________________
tests/test_ingest.py:314: in test_ingest_writes_parquet_with_correct_schema
    ingest(cfg)
cogcc_pipeline/ingest.py:213: in ingest
    ddf = build_dask_dataframe(file_paths, data_dict)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:174: in build_dask_dataframe
    meta = read_raw_file(file_paths[0], data_dict).iloc[0:0]
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:142: in read_raw_file
    columns[col] = _cast_column(raw_df[col], dtype, nullable)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:106: in _cast_column
    return series.astype(dtype)
           ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/generic.py:6541: in astype
    new_data = self._mgr.astype(dtype=dtype, errors=errors)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:611: in astype
    return self.apply("astype", dtype=dtype, errors=errors)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:442: in apply
    applied = getattr(b, f)(**kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/blocks.py:607: in astype
    new_values = astype_array_safe(values, dtype, errors=errors)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:240: in astype_array_safe
    new_values = astype_array(values, dtype, copy=copy)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:185: in astype_array
    values = _astype_nansafe(values, dtype, copy=copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:81: in _astype_nansafe
    return dtype.construct_array_type()._from_sequence(arr, dtype=dtype, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/masked.py:161: in _from_sequence
    values, mask = cls._coerce_to_array(scalars, dtype=dtype, copy=copy)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:387: in _coerce_to_array
    return coerce_to_array(value, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:226: in coerce_to_array
    raise TypeError("Need to pass bool-like values")
E   TypeError: Need to pass bool-like values
___________________ test_ingest_all_expected_columns_present ___________________
tests/test_ingest.py:331: in test_ingest_all_expected_columns_present
    ingest(cfg)
cogcc_pipeline/ingest.py:213: in ingest
    ddf = build_dask_dataframe(file_paths, data_dict)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:174: in build_dask_dataframe
    meta = read_raw_file(file_paths[0], data_dict).iloc[0:0]
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:142: in read_raw_file
    columns[col] = _cast_column(raw_df[col], dtype, nullable)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:106: in _cast_column
    return series.astype(dtype)
           ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/generic.py:6541: in astype
    new_data = self._mgr.astype(dtype=dtype, errors=errors)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:611: in astype
    return self.apply("astype", dtype=dtype, errors=errors)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:442: in apply
    applied = getattr(b, f)(**kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/blocks.py:607: in astype
    new_values = astype_array_safe(values, dtype, errors=errors)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:240: in astype_array_safe
    new_values = astype_array(values, dtype, copy=copy)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:185: in astype_array
    values = _astype_nansafe(values, dtype, copy=copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:81: in _astype_nansafe
    return dtype.construct_array_type()._from_sequence(arr, dtype=dtype, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/masked.py:161: in _from_sequence
    values, mask = cls._coerce_to_array(scalars, dtype=dtype, copy=copy)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:387: in _coerce_to_array
    return coerce_to_array(value, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:226: in coerce_to_array
    raise TypeError("Need to pass bool-like values")
E   TypeError: Need to pass bool-like values
___________________________ test_pipeline_end_to_end ___________________________
cogcc_pipeline/pipeline.py:124: in main
    fn(config)
cogcc_pipeline/ingest.py:213: in ingest
    ddf = build_dask_dataframe(file_paths, data_dict)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:174: in build_dask_dataframe
    meta = read_raw_file(file_paths[0], data_dict).iloc[0:0]
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:142: in read_raw_file
    columns[col] = _cast_column(raw_df[col], dtype, nullable)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:106: in _cast_column
    return series.astype(dtype)
           ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/generic.py:6541: in astype
    new_data = self._mgr.astype(dtype=dtype, errors=errors)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:611: in astype
    return self.apply("astype", dtype=dtype, errors=errors)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:442: in apply
    applied = getattr(b, f)(**kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/blocks.py:607: in astype
    new_values = astype_array_safe(values, dtype, errors=errors)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:240: in astype_array_safe
    new_values = astype_array(values, dtype, copy=copy)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:185: in astype_array
    values = _astype_nansafe(values, dtype, copy=copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:81: in _astype_nansafe
    return dtype.construct_array_type()._from_sequence(arr, dtype=dtype, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/masked.py:161: in _from_sequence
    values, mask = cls._coerce_to_array(scalars, dtype=dtype, copy=copy)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:387: in _coerce_to_array
    return coerce_to_array(value, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:226: in coerce_to_array
    raise TypeError("Need to pass bool-like values")
E   TypeError: Need to pass bool-like values

During handling of the above exception, another exception occurred:
tests/test_pipeline.py:224: in test_pipeline_end_to_end
    main(["--config", cfg_path, "ingest", "transform", "features"])
cogcc_pipeline/pipeline.py:132: in main
    sys.exit(1)
E   SystemExit: 1
----------------------------- Captured stdout call -----------------------------
2026-04-27 12:59:08,748 INFO cogcc_pipeline.pipeline: Starting pipeline — stages: ['ingest', 'transform', 'features']
2026-04-27 12:59:10,201 INFO distributed.http.proxy: To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2026-04-27 12:59:10,252 INFO distributed.scheduler: State start
2026-04-27 12:59:10,259 INFO distributed.scheduler:   Scheduler at:     tcp://127.0.0.1:61467
2026-04-27 12:59:10,260 INFO distributed.scheduler:   dashboard at:  http://127.0.0.1:8799/status
2026-04-27 12:59:10,260 INFO distributed.scheduler: Registering Worker plugin shuffle
2026-04-27 12:59:10,276 INFO distributed.nanny:         Start Nanny at: 'tcp://127.0.0.1:61470'
2026-04-27 12:59:11,932 INFO distributed.scheduler: Register worker addr: tcp://127.0.0.1:61472 name: 0
2026-04-27 12:59:11,973 INFO distributed.scheduler: Starting worker compute stream, tcp://127.0.0.1:61472
2026-04-27 12:59:11,974 INFO distributed.core: Starting established connection to tcp://127.0.0.1:61474
2026-04-27 12:59:12,034 INFO distributed.scheduler: Receive client connection: Client-cbe62978-4262-11f1-867e-3aab0162cd38
2026-04-27 12:59:12,035 INFO distributed.core: Starting established connection to tcp://127.0.0.1:61475
2026-04-27 12:59:12,037 INFO cogcc_pipeline.pipeline: Dask dashboard: http://127.0.0.1:8799/status
2026-04-27 12:59:12,037 INFO cogcc_pipeline.pipeline: Dask distributed scheduler initialized
2026-04-27 12:59:12,039 INFO cogcc_pipeline.pipeline: Starting stage: ingest
2026-04-27 12:59:12,066 ERROR cogcc_pipeline.pipeline: Stage 'ingest' failed after 0.0s: Need to pass bool-like values
Traceback (most recent call last):
  File "/cogcc_pipeline/pipeline.py", line 124, in main
    fn(config)
  File "/cogcc_pipeline/ingest.py", line 213, in ingest
    ddf = build_dask_dataframe(file_paths, data_dict)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/cogcc_pipeline/ingest.py", line 174, in build_dask_dataframe
    meta = read_raw_file(file_paths[0], data_dict).iloc[0:0]
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/cogcc_pipeline/ingest.py", line 142, in read_raw_file
    columns[col] = _cast_column(raw_df[col], dtype, nullable)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/cogcc_pipeline/ingest.py", line 106, in _cast_column
    return series.astype(dtype)
           ^^^^^^^^^^^^^^^^^^^^
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/generic.py", line 6541, in astype
    new_data = self._mgr.astype(dtype=dtype, errors=errors)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py", line 611, in astype
    return self.apply("astype", dtype=dtype, errors=errors)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py", line 442, in apply
    applied = getattr(b, f)(**kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/blocks.py", line 607, in astype
    new_values = astype_array_safe(values, dtype, errors=errors)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py", line 240, in astype_array_safe
    new_values = astype_array(values, dtype, copy=copy)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py", line 185, in astype_array
    values = _astype_nansafe(values, dtype, copy=copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py", line 81, in _astype_nansafe
    return dtype.construct_array_type()._from_sequence(arr, dtype=dtype, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/masked.py", line 161, in _from_sequence
    values, mask = cls._coerce_to_array(scalars, dtype=dtype, copy=copy)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py", line 387, in _coerce_to_array
    return coerce_to_array(value, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py", line 226, in coerce_to_array
    raise TypeError("Need to pass bool-like values")
TypeError: Need to pass bool-like values
___________________________ test_transform_full_run ____________________________
tests/test_transform.py:491: in test_transform_full_run
    ddf = build_dask_dataframe([str(fp)], schema)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:174: in build_dask_dataframe
    meta = read_raw_file(file_paths[0], data_dict).iloc[0:0]
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:142: in read_raw_file
    columns[col] = _cast_column(raw_df[col], dtype, nullable)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:106: in _cast_column
    return series.astype(dtype)
           ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/generic.py:6541: in astype
    new_data = self._mgr.astype(dtype=dtype, errors=errors)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:611: in astype
    return self.apply("astype", dtype=dtype, errors=errors)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:442: in apply
    applied = getattr(b, f)(**kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/blocks.py:607: in astype
    new_values = astype_array_safe(values, dtype, errors=errors)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:240: in astype_array_safe
    new_values = astype_array(values, dtype, copy=copy)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:185: in astype_array
    values = _astype_nansafe(values, dtype, copy=copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:81: in _astype_nansafe
    return dtype.construct_array_type()._from_sequence(arr, dtype=dtype, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/masked.py:161: in _from_sequence
    values, mask = cls._coerce_to_array(scalars, dtype=dtype, copy=copy)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:387: in _coerce_to_array
    return coerce_to_array(value, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:226: in coerce_to_array
    raise TypeError("Need to pass bool-like values")
E   TypeError: Need to pass bool-like values
_____________________ test_transform_return_value_is_none ______________________
tests/test_transform.py:528: in test_transform_return_value_is_none
    ddf = build_dask_dataframe([str(fp)], schema)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:174: in build_dask_dataframe
    meta = read_raw_file(file_paths[0], data_dict).iloc[0:0]
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:142: in read_raw_file
    columns[col] = _cast_column(raw_df[col], dtype, nullable)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:106: in _cast_column
    return series.astype(dtype)
           ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/generic.py:6541: in astype
    new_data = self._mgr.astype(dtype=dtype, errors=errors)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:611: in astype
    return self.apply("astype", dtype=dtype, errors=errors)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:442: in apply
    applied = getattr(b, f)(**kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/blocks.py:607: in astype
    new_values = astype_array_safe(values, dtype, errors=errors)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:240: in astype_array_safe
    new_values = astype_array(values, dtype, copy=copy)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:185: in astype_array
    values = _astype_nansafe(values, dtype, copy=copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:81: in _astype_nansafe
    return dtype.construct_array_type()._from_sequence(arr, dtype=dtype, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/masked.py:161: in _from_sequence
    values, mask = cls._coerce_to_array(scalars, dtype=dtype, copy=copy)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:387: in _coerce_to_array
    return coerce_to_array(value, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:226: in coerce_to_array
    raise TypeError("Need to pass bool-like values")
E   TypeError: Need to pass bool-like values
_____________________ test_transform_row_count_lte_interim _____________________
tests/test_transform.py:556: in test_transform_row_count_lte_interim
    ddf = build_dask_dataframe([str(fp)], schema)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:174: in build_dask_dataframe
    meta = read_raw_file(file_paths[0], data_dict).iloc[0:0]
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:142: in read_raw_file
    columns[col] = _cast_column(raw_df[col], dtype, nullable)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:106: in _cast_column
    return series.astype(dtype)
           ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/generic.py:6541: in astype
    new_data = self._mgr.astype(dtype=dtype, errors=errors)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:611: in astype
    return self.apply("astype", dtype=dtype, errors=errors)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/managers.py:442: in apply
    applied = getattr(b, f)(**kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/blocks.py:607: in astype
    new_values = astype_array_safe(values, dtype, errors=errors)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:240: in astype_array_safe
    new_values = astype_array(values, dtype, copy=copy)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:185: in astype_array
    values = _astype_nansafe(values, dtype, copy=copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/astype.py:81: in _astype_nansafe
    return dtype.construct_array_type()._from_sequence(arr, dtype=dtype, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/masked.py:161: in _from_sequence
    values, mask = cls._coerce_to_array(scalars, dtype=dtype, copy=copy)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:387: in _coerce_to_array
    return coerce_to_array(value, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/boolean.py:226: in coerce_to_array
    raise TypeError("Need to pass bool-like values")
E   TypeError: Need to pass bool-like values
=========================== short test summary info ============================
FAILED tests/test_acquire.py::test_download_file_real_download_produces_valid_file
FAILED tests/test_features.py::test_features_full_run - ValueError: Failed to...
FAILED tests/test_features.py::test_features_all_expected_derived_columns - V...
FAILED tests/test_ingest.py::test_write_parquet_readable_by_pandas_and_dask
FAILED tests/test_ingest.py::test_ingest_writes_parquet_with_correct_schema
FAILED tests/test_ingest.py::test_ingest_all_expected_columns_present - TypeE...
FAILED tests/test_pipeline.py::test_pipeline_end_to_end - SystemExit: 1
FAILED tests/test_transform.py::test_transform_full_run - TypeError: Need to ...
FAILED tests/test_transform.py::test_transform_return_value_is_none - TypeErr...
FAILED tests/test_transform.py::test_transform_row_count_lte_interim - TypeEr...
10 failed, 4 passed, 101 deselected in 14.41s
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 7614, in retire_workers
    logger.info(
Message: "Retire worker addresses (stimulus_id='%s') %s"
Arguments: ('retire-workers-1777312757.000813', (0,))
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 611, in close
    logger.info("Closing Nanny at %r. Reason: %s", self.address_safe, reason)
Message: 'Closing Nanny at %r. Reason: %s'
Arguments: ('tcp://127.0.0.1:61470', 'nanny-close')
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 619, in close
    await self.kill(timeout=timeout, reason=reason)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 400, in kill
    await self.process.kill(reason=reason, timeout=timeout)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 858, in kill
    logger.info("Nanny asking worker to close. Reason: %s", reason)
Message: 'Nanny asking worker to close. Reason: %s'
Arguments: ('nanny-close',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4653, in add_worker
    await self.handle_worker(comm, address)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 6225, in handle_worker
    await self.handle_stream(comm=comm, extra={"worker": worker})
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 908, in handle_stream
    logger.info(
Message: "Received 'close-stream' from %s; closing."
Arguments: ('tcp://127.0.0.1:61474',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4653, in add_worker
    await self.handle_worker(comm, address)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 6229, in handle_worker
    await self.remove_worker(
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 5444, in remove_worker
    logger.info(
Message: "Remove worker addr: tcp://127.0.0.1:61472 name: 0 (stimulus_id='handle-worker-cleanup-1777312757.015976')"
Arguments: ()
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4653, in add_worker
    await self.handle_worker(comm, address)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 6229, in handle_worker
    await self.remove_worker(
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 5582, in remove_worker
    logger.info("Lost all workers")
Message: 'Lost all workers'
Arguments: ()
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 626, in close
    logger.info("Nanny at %r closed.", self.address_safe)
Message: 'Nanny at %r closed.'
Arguments: ('tcp://127.0.0.1:61470',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4343, in close
    logger.info("Closing scheduler. Reason: %s", reason)
Message: 'Closing scheduler. Reason: %s'
Arguments: ('unknown',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4371, in close
    logger.info("Scheduler closing all comms")
Message: 'Scheduler closing all comms'
Arguments: ()

```

---

## Eval Run at 2026-04-27 13:09:46

**Status:** ❌ FAILED

### Failures:
- **Type check:**
```
Type check failed. Fix these errors:
/cogcc_pipeline/ingest.py:108: error: No overload variant of "where" of "Series" matches argument types "Series[bool]", "None"  [call-overload]
/cogcc_pipeline/ingest.py:108: note: Possible overload variants:
/cogcc_pipeline/ingest.py:108: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: Any | Series[Any] | Callable[..., Any | Series[Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/cogcc_pipeline/ingest.py:108: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[Any]
/cogcc_pipeline/ingest.py:142: error: No overload variant of "astype" of "Series" matches argument type "object"  [call-overload]
/cogcc_pipeline/ingest.py:142: note: Error code "call-overload" not covered by "type: ignore" comment
/cogcc_pipeline/ingest.py:142: note: Possible overload variants:
/cogcc_pipeline/ingest.py:142: note:     def astype(self, dtype: type[builtins.bool] | Literal['bool'] | BooleanDtype | Literal['boolean'] | Literal['?', 'b1', 'bool_'] | type[numpy.bool[builtins.bool]] | Literal['bool[pyarrow]', 'boolean[pyarrow]'], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[bool]
/cogcc_pipeline/ingest.py:142: note:     def astype(self, dtype: Literal['int', 'Int8', 'Int16', 'Int32', 'Int64', 'b', 'i1', 'int8', 'byte', 'h', 'i2', 'int16', 'short', 'i', 'i4', 'int32', 'intc', 'l', 'long', 'i8', 'int64', 'int_', 'q', 'longlong', 'p', 'intp', 'int8[pyarrow]', 'int16[pyarrow]', 'int32[pyarrow]', 'int64[pyarrow]', 'UInt8', 'UInt16', 'UInt32', 'UInt64', 'B', 'u1', 'uint8', 'ubyte', 'H', 'u2', 'uint16', 'ushort', 'I', 'u4', 'uint32', 'uintc', 'L', 'ulong', 'u8', 'uint', 'uint64', 'Q', 'ulonglong', 'P', 'uintp', 'uint8[pyarrow]', 'uint16[pyarrow]', 'uint32[pyarrow]', 'uint64[pyarrow]'] | type[int] | Int8Dtype | Int16Dtype | Int32Dtype | Int64Dtype | <14 more items>, copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[int]
/cogcc_pipeline/ingest.py:142: note:     def astype(self, dtype: type[str] | Literal['str'] | StringDtype[None] | Literal['string'] | StringDtype[Literal['python']] | Literal['string[python]'] | Literal['U', 'str_', 'unicode'] | type[str_] | StringDtype[Literal['pyarrow']] | Literal['string[pyarrow]'], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[str]
/cogcc_pipeline/ingest.py:142: note:     def astype(self, dtype: type[bytes] | Literal['bytes'] | Literal['S', 'bytes_'] | type[bytes_] | Literal['binary[pyarrow]'], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[bytes]
/cogcc_pipeline/ingest.py:142: note:     def astype(self, dtype: type[float] | Literal['float'] | Literal['Float32', 'Float64'] | Float32Dtype | Float64Dtype | Literal['e', 'f2', '<f2', 'float16', 'half'] | type[floating[_16Bit]] | Literal['f', 'f4', 'float32', 'single', 'd', 'f8', 'float64', 'double', 'g', 'f16', 'float128', 'longdouble'] | type[floating[_32Bit]] | type[float64] | type[floating[_64Bit | _96Bit | _128Bit]] | Literal['float[pyarrow]', 'double[pyarrow]', 'float16[pyarrow]', 'float32[pyarrow]', 'float64[pyarrow]'], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[float]
/cogcc_pipeline/ingest.py:142: note:     def astype(self, dtype: type[complex] | Literal['complex'] | Literal['F', 'c8', 'complex64', 'csingle', 'D', 'c16', 'complex128', 'cdouble', 'G', 'c32', 'complex256', 'clongdouble'] | type[complexfloating[_32Bit, _32Bit]] | type[complex128] | type[complexfloating[_64Bit | _96Bit | _128Bit, _64Bit | _96Bit | _128Bit]], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[complex]
/cogcc_pipeline/ingest.py:142: note:     def astype(self, dtype: Literal['timedelta64[s]', 'timedelta64[ms]', 'timedelta64[us]', 'timedelta64[ns]', 'm8[s]', 'm8[ms]', 'm8[us]', 'm8[ns]', '<m8[s]', '<m8[ms]', '<m8[us]', '<m8[ns]', 'duration[s][pyarrow]', 'duration[ms][pyarrow]', 'duration[us][pyarrow]', 'duration[ns][pyarrow]', 'timedelta64[Y]', 'timedelta64[M]', 'timedelta64[W]', 'timedelta64[D]', 'timedelta64[h]', 'timedelta64[m]', 'timedelta64[μs]', 'timedelta64[ps]', 'timedelta64[fs]', 'timedelta64[as]', 'm8[Y]', 'm8[M]', 'm8[W]', 'm8[D]', 'm8[h]', 'm8[m]', 'm8[μs]', 'm8[ps]', 'm8[fs]', 'm8[as]', '<m8[Y]', '<m8[M]', '<m8[W]', '<m8[D]', '<m8[h]', '<m8[m]', '<m8[μs]', '<m8[ps]', '<m8[fs]', '<m8[as]'], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[Timedelta]
/cogcc_pipeline/ingest.py:142: note:     def astype(self, dtype: Literal['datetime64[s, UTC]', 'datetime64[ms, UTC]', 'datetime64[us, UTC]', 'datetime64[ns, UTC]', 'datetime64[s]', 'datetime64[ms]', 'datetime64[us]', 'datetime64[ns]', 'M8[s]', 'M8[ms]', 'M8[us]', 'M8[ns]', '<M8[s]', '<M8[ms]', '<M8[us]', '<M8[ns]', 'date32[pyarrow]', 'date64[pyarrow]', 'timestamp[s][pyarrow]', 'timestamp[ms][pyarrow]', 'timestamp[us][pyarrow]', 'timestamp[ns][pyarrow]', 'datetime64[Y]', 'datetime64[M]', 'datetime64[W]', 'datetime64[D]', 'datetime64[h]', 'datetime64[m]', 'datetime64[μs]', 'datetime64[ps]', 'datetime64[fs]', 'datetime64[as]', 'M8[Y]', 'M8[M]', 'M8[W]', 'M8[D]', 'M8[h]', 'M8[m]', 'M8[μs]', 'M8[ps]', 'M8[fs]', 'M8[as]', '<M8[Y]', '<M8[M]', '<M8[W]', '<M8[D]', '<M8[h]', '<M8[m]', '<M8[μs]', '<M8[ps]', '<M8[fs]', '<M8[as]'], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[Timestamp]
/cogcc_pipeline/ingest.py:142: note:     def astype(self, dtype: CategoricalDtype | Literal['category'], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[CategoricalDtype]
/cogcc_pipeline/ingest.py:142: note:     def astype(self, dtype: Literal['object', 'object_', 'O', 'V', 'void'] | type[object] | type[object_] | type[void] | ExtensionDtype | dtype[generic[Any]], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[Any]
/cogcc_pipeline/features.py:40: error: Incompatible types in assignment (expression has type "DataFrameGroupBy[tuple[Hashable, ...], Literal[True]]", variable has type "DataFrameGroupBy[str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], Literal[True]]")  [assignment]
/cogcc_pipeline/features.py:88: error: Incompatible types in assignment (expression has type "DataFrameGroupBy[tuple[Hashable, ...], Literal[True]]", variable has type "DataFrameGroupBy[str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], Literal[True]]")  [assignment]
/cogcc_pipeline/features.py:111: error: Incompatible types in assignment (expression has type "DataFrameGroupBy[tuple[Hashable, ...], Literal[True]]", variable has type "DataFrameGroupBy[str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], Literal[True]]")  [assignment]
/cogcc_pipeline/transform.py:96: error: No overload variant of "where" of "Series" matches argument types "Series[bool]", "None"  [call-overload]
/cogcc_pipeline/transform.py:96: note: Possible overload variants:
/cogcc_pipeline/transform.py:96: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: Any | Series[Any] | Callable[..., Any | Series[Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/cogcc_pipeline/transform.py:96: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[Any]
/tests/test_features.py:532: error: Invalid index type "tuple[int, int | slice[Any, Any, Any] | ndarray[tuple[int], dtype[numpy.bool[builtins.bool]]]]" for "_iAtIndexerFrame"; expected type "tuple[int, int]"  [index]
Found 7 errors in 4 files (checked 12 source files)

```

- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
..............FFF......................................F................ [ 71%]
.............................                                            [100%]Running teardown with pytest sessionfinish...

=================================== FAILURES ===================================
____________________ test_cumulative_features_known_values _____________________
tests/test_features.py:123: in test_cumulative_features_known_values
    df = _make_transform_output(oil_vals=[100, 200, 150, 300])
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
tests/test_features.py:62: in _make_transform_output
    df = pd.DataFrame(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:769: in __init__
    mgr = dict_to_mgr(data, index, columns, dtype=dtype, copy=copy)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:460: in dict_to_mgr
    return arrays_to_mgr(arrays, columns, index, dtype=dtype, consolidate=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:118: in arrays_to_mgr
    arrays, refs = _homogenize(arrays, index, dtype)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:597: in _homogenize
    com.require_length_match(val, index)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/common.py:601: in require_length_match
    raise ValueError(
E   ValueError: Length of values (4) does not match length of index (6)
_____________ test_cumulative_features_flat_during_zero_production _____________
tests/test_features.py:132: in test_cumulative_features_flat_during_zero_production
    df = _make_transform_output(oil_vals=[100, 0, 0, 200])
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
tests/test_features.py:62: in _make_transform_output
    df = pd.DataFrame(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:769: in __init__
    mgr = dict_to_mgr(data, index, columns, dtype=dtype, copy=copy)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:460: in dict_to_mgr
    return arrays_to_mgr(arrays, columns, index, dtype=dtype, consolidate=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:118: in arrays_to_mgr
    arrays, refs = _homogenize(arrays, index, dtype)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:597: in _homogenize
    com.require_length_match(val, index)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/common.py:601: in require_length_match
    raise ValueError(
E   ValueError: Length of values (4) does not match length of index (6)
____________________ test_cumulative_features_zero_at_start ____________________
tests/test_features.py:143: in test_cumulative_features_zero_at_start
    df = _make_transform_output(oil_vals=[0, 0, 100, 200])
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
tests/test_features.py:62: in _make_transform_output
    df = pd.DataFrame(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:769: in __init__
    mgr = dict_to_mgr(data, index, columns, dtype=dtype, copy=copy)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:460: in dict_to_mgr
    return arrays_to_mgr(arrays, columns, index, dtype=dtype, consolidate=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:118: in arrays_to_mgr
    arrays, refs = _homogenize(arrays, index, dtype)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:597: in _homogenize
    com.require_length_match(val, index)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/common.py:601: in require_length_match
    raise ValueError(
E   ValueError: Length of values (4) does not match length of index (6)
_________________ test_read_raw_file_correct_dtypes_spot_check _________________
tests/test_ingest.py:113: in test_read_raw_file_correct_dtypes_spot_check
    assert df["Well"].dtype == pd.StringDtype()
E   AssertionError: assert dtype('O') == <StringDtype(na_value=<NA>)>
E    +  where dtype('O') = 0    Well A\nName: Well, dtype: object.dtype
E    +  and   <StringDtype(na_value=<NA>)> = <class 'pandas.StringDtype'>()
E    +    where <class 'pandas.StringDtype'> = pd.StringDtype
=============================== warnings summary ===============================
tests/test_transform.py::test_cast_categoricals_replaces_invalid_value
tests/test_transform.py::test_transform_partition_full_pipeline
  /tests/test_transform.py:97: Pandas4Warning: Constructing a Categorical with a dtype and values containing non-null entries not in that dtype's categories is deprecated and will raise in a future version.
    df[col] = df[col].astype(dtype)

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
=========================== short test summary info ============================
FAILED tests/test_features.py::test_cumulative_features_known_values - ValueE...
FAILED tests/test_features.py::test_cumulative_features_flat_during_zero_production
FAILED tests/test_features.py::test_cumulative_features_zero_at_start - Value...
FAILED tests/test_ingest.py::test_read_raw_file_correct_dtypes_spot_check - A...
4 failed, 97 passed, 14 deselected, 2 warnings in 6.87s

```

- **Integration Tests:**
```
Integration Tests failed. Fix these errors:
F.............                                                           [100%]Running teardown with pytest sessionfinish...

=================================== FAILURES ===================================
_____________ test_download_file_real_download_produces_valid_file _____________
tests/test_acquire.py:202: in test_download_file_real_download_produces_valid_file
    assert result is not None
E   assert None is not None
------------------------------ Captured log call -------------------------------
WARNING  cogcc_pipeline.acquire:acquire.py:97 Download failed for year=2024 url=https://ecmc.state.co.us/documents/data/downloads/production/monthly_prod.csv: Connection refused by Responses - the call doesn't match any registered mock.

Request: 
- GET https://ecmc.state.co.us/documents/data/downloads/production/monthly_prod.csv

Available matches:
=========================== short test summary info ============================
FAILED tests/test_acquire.py::test_download_file_real_download_produces_valid_file
1 failed, 13 passed, 101 deselected in 12.25s
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 7614, in retire_workers
    logger.info(
Message: "Retire worker addresses (stimulus_id='%s') %s"
Arguments: ('retire-workers-1777313385.69204', (0,))
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 611, in close
    logger.info("Closing Nanny at %r. Reason: %s", self.address_safe, reason)
Message: 'Closing Nanny at %r. Reason: %s'
Arguments: ('tcp://127.0.0.1:61563', 'nanny-close')
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 619, in close
    await self.kill(timeout=timeout, reason=reason)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 400, in kill
    await self.process.kill(reason=reason, timeout=timeout)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 858, in kill
    logger.info("Nanny asking worker to close. Reason: %s", reason)
Message: 'Nanny asking worker to close. Reason: %s'
Arguments: ('nanny-close',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4653, in add_worker
    await self.handle_worker(comm, address)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 6225, in handle_worker
    await self.handle_stream(comm=comm, extra={"worker": worker})
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 908, in handle_stream
    logger.info(
Message: "Received 'close-stream' from %s; closing."
Arguments: ('tcp://127.0.0.1:61567',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4653, in add_worker
    await self.handle_worker(comm, address)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 6229, in handle_worker
    await self.remove_worker(
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 5444, in remove_worker
    logger.info(
Message: "Remove worker addr: tcp://127.0.0.1:61565 name: 0 (stimulus_id='handle-worker-cleanup-1777313385.707541')"
Arguments: ()
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4653, in add_worker
    await self.handle_worker(comm, address)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 6229, in handle_worker
    await self.remove_worker(
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 5582, in remove_worker
    logger.info("Lost all workers")
Message: 'Lost all workers'
Arguments: ()
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 626, in close
    logger.info("Nanny at %r closed.", self.address_safe)
Message: 'Nanny at %r closed.'
Arguments: ('tcp://127.0.0.1:61563',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4343, in close
    logger.info("Closing scheduler. Reason: %s", reason)
Message: 'Closing scheduler. Reason: %s'
Arguments: ('unknown',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4371, in close
    logger.info("Scheduler closing all comms")
Message: 'Scheduler closing all comms'
Arguments: ()

```

---

## Eval Run at 2026-04-27 13:19:00

**Status:** ❌ FAILED

### Failures:
- **Type check:**
```
Type check failed. Fix these errors:
/cogcc_pipeline/ingest.py:142: error: No overload variant of "astype" of "Series" matches argument type "str"  [call-overload]
/cogcc_pipeline/ingest.py:142: note: Possible overload variants:
/cogcc_pipeline/ingest.py:142: note:     def astype(self, dtype: type[builtins.bool] | Literal['bool'] | BooleanDtype | Literal['boolean'] | Literal['?', 'b1', 'bool_'] | type[numpy.bool[builtins.bool]] | Literal['bool[pyarrow]', 'boolean[pyarrow]'], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[bool]
/cogcc_pipeline/ingest.py:142: note:     def astype(self, dtype: Literal['int', 'Int8', 'Int16', 'Int32', 'Int64', 'b', 'i1', 'int8', 'byte', 'h', 'i2', 'int16', 'short', 'i', 'i4', 'int32', 'intc', 'l', 'long', 'i8', 'int64', 'int_', 'q', 'longlong', 'p', 'intp', 'int8[pyarrow]', 'int16[pyarrow]', 'int32[pyarrow]', 'int64[pyarrow]', 'UInt8', 'UInt16', 'UInt32', 'UInt64', 'B', 'u1', 'uint8', 'ubyte', 'H', 'u2', 'uint16', 'ushort', 'I', 'u4', 'uint32', 'uintc', 'L', 'ulong', 'u8', 'uint', 'uint64', 'Q', 'ulonglong', 'P', 'uintp', 'uint8[pyarrow]', 'uint16[pyarrow]', 'uint32[pyarrow]', 'uint64[pyarrow]'] | type[int] | Int8Dtype | Int16Dtype | Int32Dtype | Int64Dtype | <14 more items>, copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[int]
/cogcc_pipeline/ingest.py:142: note:     def astype(self, dtype: type[str] | Literal['str'] | StringDtype[None] | Literal['string'] | StringDtype[Literal['python']] | Literal['string[python]'] | Literal['U', 'str_', 'unicode'] | type[str_] | StringDtype[Literal['pyarrow']] | Literal['string[pyarrow]'], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[str]
/cogcc_pipeline/ingest.py:142: note:     def astype(self, dtype: type[bytes] | Literal['bytes'] | Literal['S', 'bytes_'] | type[bytes_] | Literal['binary[pyarrow]'], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[bytes]
/cogcc_pipeline/ingest.py:142: note:     def astype(self, dtype: type[float] | Literal['float'] | Literal['Float32', 'Float64'] | Float32Dtype | Float64Dtype | Literal['e', 'f2', '<f2', 'float16', 'half'] | type[floating[_16Bit]] | Literal['f', 'f4', 'float32', 'single', 'd', 'f8', 'float64', 'double', 'g', 'f16', 'float128', 'longdouble'] | type[floating[_32Bit]] | type[float64] | type[floating[_64Bit | _96Bit | _128Bit]] | Literal['float[pyarrow]', 'double[pyarrow]', 'float16[pyarrow]', 'float32[pyarrow]', 'float64[pyarrow]'], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[float]
/cogcc_pipeline/ingest.py:142: note:     def astype(self, dtype: type[complex] | Literal['complex'] | Literal['F', 'c8', 'complex64', 'csingle', 'D', 'c16', 'complex128', 'cdouble', 'G', 'c32', 'complex256', 'clongdouble'] | type[complexfloating[_32Bit, _32Bit]] | type[complex128] | type[complexfloating[_64Bit | _96Bit | _128Bit, _64Bit | _96Bit | _128Bit]], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[complex]
/cogcc_pipeline/ingest.py:142: note:     def astype(self, dtype: Literal['timedelta64[s]', 'timedelta64[ms]', 'timedelta64[us]', 'timedelta64[ns]', 'm8[s]', 'm8[ms]', 'm8[us]', 'm8[ns]', '<m8[s]', '<m8[ms]', '<m8[us]', '<m8[ns]', 'duration[s][pyarrow]', 'duration[ms][pyarrow]', 'duration[us][pyarrow]', 'duration[ns][pyarrow]', 'timedelta64[Y]', 'timedelta64[M]', 'timedelta64[W]', 'timedelta64[D]', 'timedelta64[h]', 'timedelta64[m]', 'timedelta64[μs]', 'timedelta64[ps]', 'timedelta64[fs]', 'timedelta64[as]', 'm8[Y]', 'm8[M]', 'm8[W]', 'm8[D]', 'm8[h]', 'm8[m]', 'm8[μs]', 'm8[ps]', 'm8[fs]', 'm8[as]', '<m8[Y]', '<m8[M]', '<m8[W]', '<m8[D]', '<m8[h]', '<m8[m]', '<m8[μs]', '<m8[ps]', '<m8[fs]', '<m8[as]'], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[Timedelta]
/cogcc_pipeline/ingest.py:142: note:     def astype(self, dtype: Literal['datetime64[s, UTC]', 'datetime64[ms, UTC]', 'datetime64[us, UTC]', 'datetime64[ns, UTC]', 'datetime64[s]', 'datetime64[ms]', 'datetime64[us]', 'datetime64[ns]', 'M8[s]', 'M8[ms]', 'M8[us]', 'M8[ns]', '<M8[s]', '<M8[ms]', '<M8[us]', '<M8[ns]', 'date32[pyarrow]', 'date64[pyarrow]', 'timestamp[s][pyarrow]', 'timestamp[ms][pyarrow]', 'timestamp[us][pyarrow]', 'timestamp[ns][pyarrow]', 'datetime64[Y]', 'datetime64[M]', 'datetime64[W]', 'datetime64[D]', 'datetime64[h]', 'datetime64[m]', 'datetime64[μs]', 'datetime64[ps]', 'datetime64[fs]', 'datetime64[as]', 'M8[Y]', 'M8[M]', 'M8[W]', 'M8[D]', 'M8[h]', 'M8[m]', 'M8[μs]', 'M8[ps]', 'M8[fs]', 'M8[as]', '<M8[Y]', '<M8[M]', '<M8[W]', '<M8[D]', '<M8[h]', '<M8[m]', '<M8[μs]', '<M8[ps]', '<M8[fs]', '<M8[as]'], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[Timestamp]
/cogcc_pipeline/ingest.py:142: note:     def astype(self, dtype: CategoricalDtype | Literal['category'], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[CategoricalDtype]
/cogcc_pipeline/ingest.py:142: note:     def astype(self, dtype: Literal['object', 'object_', 'O', 'V', 'void'] | type[object] | type[object_] | type[void] | ExtensionDtype | dtype[generic[Any]], copy: bool = ..., errors: Literal['ignore', 'raise'] = ...) -> Series[Any]
/tests/test_features.py:540: error: Argument 1 to "int" has incompatible type "int | slice[Any, Any, Any] | ndarray[tuple[int], dtype[numpy.bool[builtins.bool]]]"; expected "str | Buffer | SupportsInt | SupportsIndex | SupportsTrunc"  [arg-type]
Found 2 errors in 2 files (checked 12 source files)

```

- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
..............FFF......................................F................ [ 71%]
.............................                                            [100%]Running teardown with pytest sessionfinish...

=================================== FAILURES ===================================
____________________ test_cumulative_features_known_values _____________________
tests/test_features.py:131: in test_cumulative_features_known_values
    df = _make_transform_output(oil_vals=[100, 200, 150, 300])
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
tests/test_features.py:70: in _make_transform_output
    df = pd.DataFrame(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:769: in __init__
    mgr = dict_to_mgr(data, index, columns, dtype=dtype, copy=copy)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:460: in dict_to_mgr
    return arrays_to_mgr(arrays, columns, index, dtype=dtype, consolidate=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:118: in arrays_to_mgr
    arrays, refs = _homogenize(arrays, index, dtype)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:597: in _homogenize
    com.require_length_match(val, index)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/common.py:601: in require_length_match
    raise ValueError(
E   ValueError: Length of values (6) does not match length of index (4)
_____________ test_cumulative_features_flat_during_zero_production _____________
tests/test_features.py:140: in test_cumulative_features_flat_during_zero_production
    df = _make_transform_output(oil_vals=[100, 0, 0, 200])
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
tests/test_features.py:70: in _make_transform_output
    df = pd.DataFrame(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:769: in __init__
    mgr = dict_to_mgr(data, index, columns, dtype=dtype, copy=copy)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:460: in dict_to_mgr
    return arrays_to_mgr(arrays, columns, index, dtype=dtype, consolidate=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:118: in arrays_to_mgr
    arrays, refs = _homogenize(arrays, index, dtype)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:597: in _homogenize
    com.require_length_match(val, index)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/common.py:601: in require_length_match
    raise ValueError(
E   ValueError: Length of values (6) does not match length of index (4)
____________________ test_cumulative_features_zero_at_start ____________________
tests/test_features.py:151: in test_cumulative_features_zero_at_start
    df = _make_transform_output(oil_vals=[0, 0, 100, 200])
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
tests/test_features.py:70: in _make_transform_output
    df = pd.DataFrame(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:769: in __init__
    mgr = dict_to_mgr(data, index, columns, dtype=dtype, copy=copy)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:460: in dict_to_mgr
    return arrays_to_mgr(arrays, columns, index, dtype=dtype, consolidate=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:118: in arrays_to_mgr
    arrays, refs = _homogenize(arrays, index, dtype)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:597: in _homogenize
    com.require_length_match(val, index)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/common.py:601: in require_length_match
    raise ValueError(
E   ValueError: Length of values (6) does not match length of index (4)
_________________ test_read_raw_file_correct_dtypes_spot_check _________________
tests/test_ingest.py:115: in test_read_raw_file_correct_dtypes_spot_check
    assert df["AcceptedDate"].dtype == np.dtype("datetime64[ns]")
E   AssertionError: assert dtype('<M8[us]') == dtype('<M8[ns]')
E    +  where dtype('<M8[us]') = 0   2021-04-01\nName: AcceptedDate, dtype: datetime64[us].dtype
E    +  and   dtype('<M8[ns]') = <class 'numpy.dtype'>('datetime64[ns]')
E    +    where <class 'numpy.dtype'> = np.dtype
=============================== warnings summary ===============================
tests/test_transform.py::test_cast_categoricals_replaces_invalid_value
tests/test_transform.py::test_transform_partition_full_pipeline
  /tests/test_transform.py:97: Pandas4Warning: Constructing a Categorical with a dtype and values containing non-null entries not in that dtype's categories is deprecated and will raise in a future version.
    df[col] = df[col].astype(dtype)

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
=========================== short test summary info ============================
FAILED tests/test_features.py::test_cumulative_features_known_values - ValueE...
FAILED tests/test_features.py::test_cumulative_features_flat_during_zero_production
FAILED tests/test_features.py::test_cumulative_features_zero_at_start - Value...
FAILED tests/test_ingest.py::test_read_raw_file_correct_dtypes_spot_check - A...
4 failed, 97 passed, 14 deselected, 2 warnings in 7.35s

```

---

## Eval Run at 2026-04-27 13:22:41

**Status:** ✅ PASSED

---
