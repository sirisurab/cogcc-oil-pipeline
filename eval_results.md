
## Eval Run at 2026-04-24 05:52:17

**Status:** ❌ FAILED

### Failures:
- **Linting:**
```
Linting failed. Fix these errors:
F841 Local variable `numeric` is assigned to but never used
   --> cogcc_pipeline/features.py:181:17
    |
179 |             out_col = f"{col}_rolling_{w}m"
180 |             if col in df_reset.columns:
181 |                 numeric = pd.to_numeric(df_reset[col], errors="coerce")
    |                 ^^^^^^^
182 |                 df_reset[out_col] = df_reset.groupby(entity_col)[col].transform(
183 |                     lambda s, window=w: (
    |
help: Remove assignment to unused variable `numeric`

F841 Local variable `scheduler` is assigned to but never used
   --> cogcc_pipeline/ingest.py:269:9
    |
267 |         scheduler = "distributed"
268 |     except ValueError:
269 |         scheduler = "synchronous"
    |         ^^^^^^^^^
270 |
271 |     # Meta derivation per ADR-003: call actual function on zero-row real input
    |
help: Remove assignment to unused variable `scheduler`

F841 Local variable `results1` is assigned to but never used
   --> tests/test_acquire.py:309:9
    |
308 |     with patch("cogcc_pipeline.acquire.time.sleep"):
309 |         results1 = acquire(cfg)
    |         ^^^^^^^^
310 |
311 |     files_after_run1 = list(raw_dir.glob("*.csv"))
    |
help: Remove assignment to unused variable `results1`

F841 Local variable `schema` is assigned to but never used
   --> tests/test_transform.py:152:5
    |
150 |     from cogcc_pipeline.ingest import load_canonical_schema
151 |
152 |     schema = load_canonical_schema(DICT_PATH)
    |     ^^^^^^
153 |     base_cols = {
154 |         "ApiCountyCode": ["01"] * rows,
    |
help: Remove assignment to unused variable `schema`

Found 4 errors.
No fixes available (4 hidden fixes can be enabled with the `--unsafe-fixes` option).

```

- **Type check:**
```
Type check failed. Fix these errors:
/cogcc_pipeline/transform.py:64: error: No overload variant of "where" of "Series" matches argument types "Series[bool]", "NAType"  [call-overload]
/cogcc_pipeline/transform.py:64: note: Possible overload variants:
/cogcc_pipeline/transform.py:64: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: Any | Series[Any] | Callable[..., Any | Series[Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/cogcc_pipeline/transform.py:64: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[Any]
/cogcc_pipeline/transform.py:128: error: No overload variant of "where" of "Series" matches argument types "Any", "NAType"  [call-overload]
/cogcc_pipeline/transform.py:128: note: Possible overload variants:
/cogcc_pipeline/transform.py:128: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: Any | Series[Any] | Callable[..., Any | Series[Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/cogcc_pipeline/transform.py:128: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[Any]
/cogcc_pipeline/transform.py:135: error: No overload variant of "where" of "Series" matches argument types "Series[bool]", "NAType"  [call-overload]
/cogcc_pipeline/transform.py:135: note: Possible overload variants:
/cogcc_pipeline/transform.py:135: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: Any | Series[Any] | Callable[..., Any | Series[Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/cogcc_pipeline/transform.py:135: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[Any]
/cogcc_pipeline/ingest.py:163: error: No overload variant of "array" matches argument types "list[NAType]", "str"  [call-overload]
/cogcc_pipeline/ingest.py:163: note: Error code "call-overload" not covered by "type: ignore" comment
/cogcc_pipeline/ingest.py:163: note: Possible overload variants:
/cogcc_pipeline/ingest.py:163: note:     def array(data: Sequence[Just[float]], dtype: Literal['Float32', 'Float64'] | Float32Dtype | Float64Dtype | None = ..., copy: bool = ...) -> FloatingArray
/cogcc_pipeline/ingest.py:163: note:     def array(data: tuple[Any, ...] | MutableSequence[Any], dtype: type[builtins.bool] | Literal['bool'] | type[int] | Literal['int'] | type[float] | Literal['float'] | type[complex] | Literal['complex'] | type[bytes] | Literal['bytes'] | type[object] | Literal['object'] | Literal['?', 'b1', 'bool_'] | type[numpy.bool[builtins.bool]] | Literal['b', 'i1', 'int8', 'byte', 'h', 'i2', 'int16', 'short', 'i', 'i4', 'int32', 'intc', 'l', 'long', 'l', 'i8', 'int64', 'int_', 'long', 'q', 'longlong', 'p', 'intp'] | type[signedinteger[_8Bit]] | type[signedinteger[_16Bit]] | type[signedinteger[_32Bit]] | type[signedinteger[_32Bit | _64Bit]] | type[signedinteger[_64Bit]] | type[signedinteger[_32Bit | _64Bit]] | Literal['B', 'u1', 'uint8', 'ubyte', 'H', 'u2', 'uint16', 'ushort', 'I', 'u4', 'uint32', 'uintc', 'L', 'ulong', 'L', 'u8', 'uint', 'ulong', 'uint64', 'Q', 'ulonglong', 'P', 'uintp'] | type[unsignedinteger[_8Bit]] | type[unsignedinteger[_16Bit]] | type[unsignedinteger[_32Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | type[unsignedinteger[_64Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | Literal['e', 'f2', '<f2', 'float16', 'half'] | type[floating[_16Bit]] | Literal['f', 'f4', 'float32', 'single', 'd', 'f8', 'float64', 'double', 'g', 'f16', 'float128', 'longdouble'] | type[floating[_32Bit]] | type[float64] | type[floating[_64Bit | _96Bit | _128Bit]] | Literal['F', 'c8', 'complex64', 'csingle', 'D', 'c16', 'complex128', 'cdouble', 'G', 'c32', 'complex256', 'clongdouble'] | type[complexfloating[_32Bit, _32Bit]] | type[complex128] | type[complexfloating[_64Bit | _96Bit | _128Bit, _64Bit | _96Bit | _128Bit]] | Literal['U', 'str_', 'unicode'] | type[str_] | Literal['S', 'bytes_'] | type[bytes_] | Literal['object_', 'O'] | type[object_] | Literal['V', 'void'] | type[void], copy: bool = ...) -> NumpyExtensionArray
/cogcc_pipeline/ingest.py:163: note:     def array(data: Sequence[NAType | None], dtype: type[builtins.bool] | Literal['bool'] | type[int] | Literal['int'] | type[float] | Literal['float'] | type[complex] | Literal['complex'] | type[bytes] | Literal['bytes'] | type[object] | Literal['object'] | Literal['?', 'b1', 'bool_'] | type[numpy.bool[builtins.bool]] | Literal['b', 'i1', 'int8', 'byte', 'h', 'i2', 'int16', 'short', 'i', 'i4', 'int32', 'intc', 'l', 'long', 'l', 'i8', 'int64', 'int_', 'long', 'q', 'longlong', 'p', 'intp'] | type[signedinteger[_8Bit]] | type[signedinteger[_16Bit]] | type[signedinteger[_32Bit]] | type[signedinteger[_32Bit | _64Bit]] | type[signedinteger[_64Bit]] | type[signedinteger[_32Bit | _64Bit]] | Literal['B', 'u1', 'uint8', 'ubyte', 'H', 'u2', 'uint16', 'ushort', 'I', 'u4', 'uint32', 'uintc', 'L', 'ulong', 'L', 'u8', 'uint', 'ulong', 'uint64', 'Q', 'ulonglong', 'P', 'uintp'] | type[unsignedinteger[_8Bit]] | type[unsignedinteger[_16Bit]] | type[unsignedinteger[_32Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | type[unsignedinteger[_64Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | Literal['e', 'f2', '<f2', 'float16', 'half'] | type[floating[_16Bit]] | Literal['f', 'f4', 'float32', 'single', 'd', 'f8', 'float64', 'double', 'g', 'f16', 'float128', 'longdouble'] | type[floating[_32Bit]] | type[float64] | type[floating[_64Bit | _96Bit | _128Bit]] | Literal['F', 'c8', 'complex64', 'csingle', 'D', 'c16', 'complex128', 'cdouble', 'G', 'c32', 'complex256', 'clongdouble'] | type[complexfloating[_32Bit, _32Bit]] | type[complex128] | type[complexfloating[_64Bit | _96Bit | _128Bit, _64Bit | _96Bit | _128Bit]] | Literal['U', 'str_', 'unicode'] | type[str_] | Literal['S', 'bytes_'] | type[bytes_] | Literal['object_', 'O'] | type[object_] | Literal['V', 'void'] | type[void] | None = ..., copy: bool = ...) -> NumpyExtensionArray
/cogcc_pipeline/ingest.py:163: note:     def array(data: Sequence[Timedelta] | Series[Timedelta] | TimedeltaArray | TimedeltaIndex | ndarray[tuple[int], dtype[timedelta64[timedelta | int | None]]], dtype: Literal['timedelta64[s]', 'timedelta64[ms]', 'timedelta64[us]', 'timedelta64[ns]', 'm8[s]', 'm8[ms]', 'm8[us]', 'm8[ns]', '<m8[s]', '<m8[ms]', '<m8[us]', '<m8[ns]'] | Literal['duration[s][pyarrow]', 'duration[ms][pyarrow]', 'duration[us][pyarrow]', 'duration[ns][pyarrow]'] | None = ..., copy: bool = ...) -> TimedeltaArray
/cogcc_pipeline/ingest.py:163: note:     def array(data: Sequence[builtins.bool | numpy.bool[builtins.bool] | Just[float] | NAType | None], dtype: BooleanDtype | Literal['boolean'], copy: bool = ...) -> BooleanArray
/cogcc_pipeline/ingest.py:163: note:     def array(data: Sequence[builtins.bool | numpy.bool[builtins.bool] | NAType | None], dtype: None = ..., copy: bool = ...) -> BooleanArray
/cogcc_pipeline/ingest.py:163: note:     def array(data: ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | BooleanArray, dtype: BooleanDtype | Literal['boolean'] | None = ..., copy: bool = ...) -> BooleanArray
/cogcc_pipeline/ingest.py:163: note:     def array(data: Sequence[float | integer[Any] | NAType | None], dtype: Literal['Int8', 'Int16', 'Int32', 'Int64'] | Int8Dtype | Int16Dtype | Int32Dtype | Int64Dtype | Literal['UInt8', 'UInt16', 'UInt32', 'UInt64'] | UInt8Dtype | UInt16Dtype | UInt32Dtype | UInt64Dtype, copy: bool = ...) -> IntegerArray
/cogcc_pipeline/ingest.py:163: note:     def array(data: Sequence[int | integer[Any] | NAType | None], dtype: None = ..., copy: bool = ...) -> IntegerArray
/cogcc_pipeline/ingest.py:163: note:     def array(data: ndarray[tuple[Any, ...], dtype[integer[Any]]] | IntegerArray, dtype: Literal['Int8', 'Int16', 'Int32', 'Int64'] | Int8Dtype | Int16Dtype | Int32Dtype | Int64Dtype | Literal['UInt8', 'UInt16', 'UInt32', 'UInt64'] | UInt8Dtype | UInt16Dtype | UInt32Dtype | UInt64Dtype | None = ..., copy: bool = ...) -> IntegerArray
/cogcc_pipeline/ingest.py:163: note:     def array(data: Sequence[float | floating[Any] | NAType | None] | ndarray[tuple[Any, ...], dtype[floating[Any]]] | FloatingArray, dtype: Literal['Float32', 'Float64'] | Float32Dtype | Float64Dtype | None = ..., copy: bool = ...) -> FloatingArray
/cogcc_pipeline/ingest.py:163: note:     def array(data: tuple[Just[float] | str | datetime | datetime64[date | int | None] | NaTType | None, ...] | MutableSequence[Just[float] | str | datetime | datetime64[date | int | None] | NaTType | None] | ndarray[tuple[Any, ...], dtype[Any]] | DatetimeArray, dtype: DatetimeTZDtype | Literal['datetime64[s, UTC]', 'datetime64[ms, UTC]', 'datetime64[us, UTC]', 'datetime64[ns, UTC]'] | dtype[datetime64[date | int | None]] | Literal['datetime64[s]', 'datetime64[ms]', 'datetime64[us]', 'datetime64[ns]', 'M8[s]', 'M8[ms]', 'M8[us]', 'M8[ns]', '<M8[s]', '<M8[ms]', '<M8[us]', '<M8[ns]'], copy: bool = ...) -> DatetimeArray
/cogcc_pipeline/ingest.py:163: note:     def array(data: Sequence[datetime | NaTType | None] | Sequence[datetime64[date | int | None] | NaTType | None] | ndarray[tuple[Any, ...], dtype[datetime64[date | int | None]]] | DatetimeArray, dtype: None = ..., copy: bool = ...) -> DatetimeArray
/cogcc_pipeline/ingest.py:163: note:     def array(data: tuple[Just[float] | str | str_ | NAType | None, ...] | MutableSequence[Just[float] | str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[Any]] | BaseStringArray[None], dtype: StringDtype[Never], copy: bool = ...) -> BaseStringArray[None]
/cogcc_pipeline/ingest.py:163: note:     def array(data: tuple[Just[float] | str | str_ | NAType | None, ...] | MutableSequence[Just[float] | str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[Any]] | BaseStringArray[None], dtype: StringDtype[Literal['pyarrow']] | Literal['string[pyarrow]'], copy: bool = ...) -> ArrowStringArray
/cogcc_pipeline/ingest.py:163: note:     def array(data: tuple[Just[float] | str | str_ | NAType | None, ...] | MutableSequence[Just[float] | str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[Any]] | BaseStringArray[None], dtype: StringDtype[Literal['python']] | Literal['string[python]'], copy: bool = ...) -> StringArray
/cogcc_pipeline/ingest.py:163: note:     def array(data: tuple[Just[float] | str | str_ | NAType | None, ...] | MutableSequence[Just[float] | str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[Any]] | BaseStringArray[None], dtype: StringDtype[None] | Literal['string'], copy: bool = ...) -> BaseStringArray[None]
/cogcc_pipeline/ingest.py:163: note:     def array(data: tuple[str | str_ | NAType | None, ...] | MutableSequence[str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[str_]] | BaseStringArray[None], dtype: None = ..., copy: bool = ...) -> BaseStringArray[None]
/cogcc_pipeline/ingest.py:163: note:     def array(data: tuple[Any, ...] | MutableSequence[Any], dtype: None = ..., copy: bool = ...) -> NumpyExtensionArray
/cogcc_pipeline/ingest.py:163: note:     def array(data: ndarray[tuple[Any, ...], dtype[Any]] | NumpyExtensionArray | RangeIndex, dtype: type[builtins.bool] | Literal['bool'] | type[int] | Literal['int'] | type[float] | Literal['float'] | type[complex] | Literal['complex'] | type[bytes] | Literal['bytes'] | type[object] | Literal['object'] | Literal['?', 'b1', 'bool_'] | type[numpy.bool[builtins.bool]] | Literal['b', 'i1', 'int8', 'byte', 'h', 'i2', 'int16', 'short', 'i', 'i4', 'int32', 'intc', 'l', 'long', 'l', 'i8', 'int64', 'int_', 'long', 'q', 'longlong', 'p', 'intp'] | type[signedinteger[_8Bit]] | type[signedinteger[_16Bit]] | type[signedinteger[_32Bit]] | type[signedinteger[_32Bit | _64Bit]] | type[signedinteger[_64Bit]] | type[signedinteger[_32Bit | _64Bit]] | Literal['B', 'u1', 'uint8', 'ubyte', 'H', 'u2', 'uint16', 'ushort', 'I', 'u4', 'uint32', 'uintc', 'L', 'ulong', 'L', 'u8', 'uint', 'ulong', 'uint64', 'Q', 'ulonglong', 'P', 'uintp'] | type[unsignedinteger[_8Bit]] | type[unsignedinteger[_16Bit]] | type[unsignedinteger[_32Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | type[unsignedinteger[_64Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | Literal['e', 'f2', '<f2', 'float16', 'half'] | type[floating[_16Bit]] | Literal['f', 'f4', 'float32', 'single', 'd', 'f8', 'float64', 'double', 'g', 'f16', 'float128', 'longdouble'] | type[floating[_32Bit]] | type[float64] | type[floating[_64Bit | _96Bit | _128Bit]] | Literal['F', 'c8', 'complex64', 'csingle', 'D', 'c16', 'complex128', 'cdouble', 'G', 'c32', 'complex256', 'clongdouble'] | type[complexfloating[_32Bit, _32Bit]] | type[complex128] | type[complexfloating[_64Bit | _96Bit | _128Bit, _64Bit | _96Bit | _128Bit]] | Literal['U', 'str_', 'unicode'] | type[str_] | Literal['S', 'bytes_'] | type[bytes_] | Literal['object_', 'O'] | type[object_] | Literal['V', 'void'] | type[void] | None = ..., copy: bool = ...) -> NumpyExtensionArray
/cogcc_pipeline/ingest.py:219: error: Incompatible types in assignment (expression has type "Categorical", target has type "Series[Any]")  [assignment]
/cogcc_pipeline/ingest.py:219: error: Argument "dtype" to "Categorical" has incompatible type "str"; expected "CategoricalDtype | None"  [arg-type]
/cogcc_pipeline/features.py:54: error: No overload variant of "groupby" of "DataFrame" matches argument type "Hashable"  [call-overload]
/cogcc_pipeline/features.py:54: note: Possible overload variants:
/cogcc_pipeline/features.py:54: note:     def groupby(self, by: str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], Literal[True]]
/cogcc_pipeline/features.py:54: note:     def groupby(self, by: str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], Literal[False]]
/cogcc_pipeline/features.py:54: note:     def groupby(self, by: DatetimeIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timestamp, Literal[True]]
/cogcc_pipeline/features.py:54: note:     def groupby(self, by: DatetimeIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timestamp, Literal[False]]
/cogcc_pipeline/features.py:54: note:     def groupby(self, by: TimedeltaIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timedelta, Literal[True]]
/cogcc_pipeline/features.py:54: note:     def groupby(self, by: TimedeltaIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timedelta, Literal[False]]
/cogcc_pipeline/features.py:54: note:     def groupby(self, by: PeriodIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Period, Literal[True]]
/cogcc_pipeline/features.py:54: note:     def groupby(self, by: PeriodIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Period, Literal[False]]
/cogcc_pipeline/features.py:54: note:     def [IntervalT: Interval[Any]] groupby(self, by: IntervalIndex[IntervalT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[IntervalT, Literal[True]]
/cogcc_pipeline/features.py:54: note:     def [IntervalT: Interval[Any]] groupby(self, by: IntervalIndex[IntervalT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[IntervalT, Literal[False]]
/cogcc_pipeline/features.py:54: note:     def groupby(self, by: MultiIndex | tuple[Any, ...] | list[Any] | ufunc | Callable[..., Any] | list[ufunc | Callable[..., Any]] | list[Series[Any]] | <7 more items> | None = ..., level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[tuple[Hashable, ...], Literal[True]]
/cogcc_pipeline/features.py:54: note:     def groupby(self, by: MultiIndex | tuple[Any, ...] | list[Any] | ufunc | Callable[..., Any] | list[ufunc | Callable[..., Any]] | list[Series[Any]] | <7 more items> | None = ..., level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[tuple[Hashable, ...], Literal[False]]
/cogcc_pipeline/features.py:54: note:     def [SeriesByT: str | bytes | date | bool | int | <6 more items>] groupby(self, by: Series[SeriesByT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[SeriesByT, Literal[True]]
/cogcc_pipeline/features.py:54: note:     def [SeriesByT: str | bytes | date | bool | int | <6 more items>] groupby(self, by: Series[SeriesByT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[SeriesByT, Literal[False]]
/cogcc_pipeline/features.py:54: note:     def groupby(self, by: CategoricalIndex[Any] | Index[Any] | Series[Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Any, Literal[True]]
/cogcc_pipeline/features.py:54: note:     def groupby(self, by: CategoricalIndex[Any] | Index[Any] | Series[Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Any, Literal[False]]
/cogcc_pipeline/features.py:182: error: No overload variant of "groupby" of "DataFrame" matches argument type "Hashable"  [call-overload]
/cogcc_pipeline/features.py:182: note: Possible overload variants:
/cogcc_pipeline/features.py:182: note:     def groupby(self, by: str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], Literal[True]]
/cogcc_pipeline/features.py:182: note:     def groupby(self, by: str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], Literal[False]]
/cogcc_pipeline/features.py:182: note:     def groupby(self, by: DatetimeIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timestamp, Literal[True]]
/cogcc_pipeline/features.py:182: note:     def groupby(self, by: DatetimeIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timestamp, Literal[False]]
/cogcc_pipeline/features.py:182: note:     def groupby(self, by: TimedeltaIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timedelta, Literal[True]]
/cogcc_pipeline/features.py:182: note:     def groupby(self, by: TimedeltaIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timedelta, Literal[False]]
/cogcc_pipeline/features.py:182: note:     def groupby(self, by: PeriodIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Period, Literal[True]]
/cogcc_pipeline/features.py:182: note:     def groupby(self, by: PeriodIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Period, Literal[False]]
/cogcc_pipeline/features.py:182: note:     def [IntervalT: Interval[Any]] groupby(self, by: IntervalIndex[IntervalT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[IntervalT, Literal[True]]
/cogcc_pipeline/features.py:182: note:     def [IntervalT: Interval[Any]] groupby(self, by: IntervalIndex[IntervalT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[IntervalT, Literal[False]]
/cogcc_pipeline/features.py:182: note:     def groupby(self, by: MultiIndex | tuple[Any, ...] | list[Any] | ufunc | Callable[..., Any] | list[ufunc | Callable[..., Any]] | list[Series[Any]] | <7 more items> | None = ..., level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[tuple[Hashable, ...], Literal[True]]
/cogcc_pipeline/features.py:182: note:     def groupby(self, by: MultiIndex | tuple[Any, ...] | list[Any] | ufunc | Callable[..., Any] | list[ufunc | Callable[..., Any]] | list[Series[Any]] | <7 more items> | None = ..., level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[tuple[Hashable, ...], Literal[False]]
/cogcc_pipeline/features.py:182: note:     def [SeriesByT: str | bytes | date | bool | int | <6 more items>] groupby(self, by: Series[SeriesByT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[SeriesByT, Literal[True]]
/cogcc_pipeline/features.py:182: note:     def [SeriesByT: str | bytes | date | bool | int | <6 more items>] groupby(self, by: Series[SeriesByT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[SeriesByT, Literal[False]]
/cogcc_pipeline/features.py:182: note:     def groupby(self, by: CategoricalIndex[Any] | Index[Any] | Series[Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Any, Literal[True]]
/cogcc_pipeline/features.py:182: note:     def groupby(self, by: CategoricalIndex[Any] | Index[Any] | Series[Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Any, Literal[False]]
/cogcc_pipeline/features.py:184: error: "float" has no attribute "rolling"  [attr-defined]
/cogcc_pipeline/features.py:197: error: No overload variant of "groupby" of "DataFrame" matches argument type "Hashable"  [call-overload]
/cogcc_pipeline/features.py:197: note: Possible overload variants:
/cogcc_pipeline/features.py:197: note:     def groupby(self, by: str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], Literal[True]]
/cogcc_pipeline/features.py:197: note:     def groupby(self, by: str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], Literal[False]]
/cogcc_pipeline/features.py:197: note:     def groupby(self, by: DatetimeIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timestamp, Literal[True]]
/cogcc_pipeline/features.py:197: note:     def groupby(self, by: DatetimeIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timestamp, Literal[False]]
/cogcc_pipeline/features.py:197: note:     def groupby(self, by: TimedeltaIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timedelta, Literal[True]]
/cogcc_pipeline/features.py:197: note:     def groupby(self, by: TimedeltaIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timedelta, Literal[False]]
/cogcc_pipeline/features.py:197: note:     def groupby(self, by: PeriodIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Period, Literal[True]]
/cogcc_pipeline/features.py:197: note:     def groupby(self, by: PeriodIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Period, Literal[False]]
/cogcc_pipeline/features.py:197: note:     def [IntervalT: Interval[Any]] groupby(self, by: IntervalIndex[IntervalT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[IntervalT, Literal[True]]
/cogcc_pipeline/features.py:197: note:     def [IntervalT: Interval[Any]] groupby(self, by: IntervalIndex[IntervalT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[IntervalT, Literal[False]]
/cogcc_pipeline/features.py:197: note:     def groupby(self, by: MultiIndex | tuple[Any, ...] | list[Any] | ufunc | Callable[..., Any] | list[ufunc | Callable[..., Any]] | list[Series[Any]] | <7 more items> | None = ..., level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[tuple[Hashable, ...], Literal[True]]
/cogcc_pipeline/features.py:197: note:     def groupby(self, by: MultiIndex | tuple[Any, ...] | list[Any] | ufunc | Callable[..., Any] | list[ufunc | Callable[..., Any]] | list[Series[Any]] | <7 more items> | None = ..., level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[tuple[Hashable, ...], Literal[False]]
/cogcc_pipeline/features.py:197: note:     def [SeriesByT: str | bytes | date | bool | int | <6 more items>] groupby(self, by: Series[SeriesByT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[SeriesByT, Literal[True]]
/cogcc_pipeline/features.py:197: note:     def [SeriesByT: str | bytes | date | bool | int | <6 more items>] groupby(self, by: Series[SeriesByT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[SeriesByT, Literal[False]]
/cogcc_pipeline/features.py:197: note:     def groupby(self, by: CategoricalIndex[Any] | Index[Any] | Series[Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Any, Literal[True]]
/cogcc_pipeline/features.py:197: note:     def groupby(self, by: CategoricalIndex[Any] | Index[Any] | Series[Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Any, Literal[False]]
/cogcc_pipeline/features.py:198: error: "float" has no attribute "shift"  [attr-defined]
/cogcc_pipeline/features.py:208: error: No overload variant of "groupby" of "DataFrame" matches argument type "Hashable"  [call-overload]
/cogcc_pipeline/features.py:208: note: Possible overload variants:
/cogcc_pipeline/features.py:208: note:     def groupby(self, by: str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], Literal[True]]
/cogcc_pipeline/features.py:208: note:     def groupby(self, by: str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], Literal[False]]
/cogcc_pipeline/features.py:208: note:     def groupby(self, by: DatetimeIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timestamp, Literal[True]]
/cogcc_pipeline/features.py:208: note:     def groupby(self, by: DatetimeIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timestamp, Literal[False]]
/cogcc_pipeline/features.py:208: note:     def groupby(self, by: TimedeltaIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timedelta, Literal[True]]
/cogcc_pipeline/features.py:208: note:     def groupby(self, by: TimedeltaIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timedelta, Literal[False]]
/cogcc_pipeline/features.py:208: note:     def groupby(self, by: PeriodIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Period, Literal[True]]
/cogcc_pipeline/features.py:208: note:     def groupby(self, by: PeriodIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Period, Literal[False]]
/cogcc_pipeline/features.py:208: note:     def [IntervalT: Interval[Any]] groupby(self, by: IntervalIndex[IntervalT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[IntervalT, Literal[True]]
/cogcc_pipeline/features.py:208: note:     def [IntervalT: Interval[Any]] groupby(self, by: IntervalIndex[IntervalT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[IntervalT, Literal[False]]
/cogcc_pipeline/features.py:208: note:     def groupby(self, by: MultiIndex | tuple[Any, ...] | list[Any] | ufunc | Callable[..., Any] | list[ufunc | Callable[..., Any]] | list[Series[Any]] | <7 more items> | None = ..., level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[tuple[Hashable, ...], Literal[True]]
/cogcc_pipeline/features.py:208: note:     def groupby(self, by: MultiIndex | tuple[Any, ...] | list[Any] | ufunc | Callable[..., Any] | list[ufunc | Callable[..., Any]] | list[Series[Any]] | <7 more items> | None = ..., level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[tuple[Hashable, ...], Literal[False]]
/cogcc_pipeline/features.py:208: note:     def [SeriesByT: str | bytes | date | bool | int | <6 more items>] groupby(self, by: Series[SeriesByT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[SeriesByT, Literal[True]]
/cogcc_pipeline/features.py:208: note:     def [SeriesByT: str | bytes | date | bool | int | <6 more items>] groupby(self, by: Series[SeriesByT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[SeriesByT, Literal[False]]
/cogcc_pipeline/features.py:208: note:     def groupby(self, by: CategoricalIndex[Any] | Index[Any] | Series[Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Any, Literal[True]]
/cogcc_pipeline/features.py:208: note:     def groupby(self, by: CategoricalIndex[Any] | Index[Any] | Series[Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Any, Literal[False]]
/cogcc_pipeline/features.py:209: error: "float" has no attribute "pct_change"  [attr-defined]
/cogcc_pipeline/features.py:219: error: No overload variant of "groupby" of "DataFrame" matches argument type "Hashable"  [call-overload]
/cogcc_pipeline/features.py:219: note: Possible overload variants:
/cogcc_pipeline/features.py:219: note:     def groupby(self, by: str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], Literal[True]]
/cogcc_pipeline/features.py:219: note:     def groupby(self, by: str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], Literal[False]]
/cogcc_pipeline/features.py:219: note:     def groupby(self, by: DatetimeIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timestamp, Literal[True]]
/cogcc_pipeline/features.py:219: note:     def groupby(self, by: DatetimeIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timestamp, Literal[False]]
/cogcc_pipeline/features.py:219: note:     def groupby(self, by: TimedeltaIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timedelta, Literal[True]]
/cogcc_pipeline/features.py:219: note:     def groupby(self, by: TimedeltaIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timedelta, Literal[False]]
/cogcc_pipeline/features.py:219: note:     def groupby(self, by: PeriodIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Period, Literal[True]]
/cogcc_pipeline/features.py:219: note:     def groupby(self, by: PeriodIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Period, Literal[False]]
/cogcc_pipeline/features.py:219: note:     def [IntervalT: Interval[Any]] groupby(self, by: IntervalIndex[IntervalT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[IntervalT, Literal[True]]
/cogcc_pipeline/features.py:219: note:     def [IntervalT: Interval[Any]] groupby(self, by: IntervalIndex[IntervalT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[IntervalT, Literal[False]]
/cogcc_pipeline/features.py:219: note:     def groupby(self, by: MultiIndex | tuple[Any, ...] | list[Any] | ufunc | Callable[..., Any] | list[ufunc | Callable[..., Any]] | list[Series[Any]] | <7 more items> | None = ..., level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[tuple[Hashable, ...], Literal[True]]
/cogcc_pipeline/features.py:219: note:     def groupby(self, by: MultiIndex | tuple[Any, ...] | list[Any] | ufunc | Callable[..., Any] | list[ufunc | Callable[..., Any]] | list[Series[Any]] | <7 more items> | None = ..., level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[tuple[Hashable, ...], Literal[False]]
/cogcc_pipeline/features.py:219: note:     def [SeriesByT: str | bytes | date | bool | int | <6 more items>] groupby(self, by: Series[SeriesByT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[SeriesByT, Literal[True]]
/cogcc_pipeline/features.py:219: note:     def [SeriesByT: str | bytes | date | bool | int | <6 more items>] groupby(self, by: Series[SeriesByT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[SeriesByT, Literal[False]]
/cogcc_pipeline/features.py:219: note:     def groupby(self, by: CategoricalIndex[Any] | Index[Any] | Series[Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Any, Literal[True]]
/cogcc_pipeline/features.py:219: note:     def groupby(self, by: CategoricalIndex[Any] | Index[Any] | Series[Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Any, Literal[False]]
/cogcc_pipeline/features.py:262: error: No overload variant of "groupby" of "DataFrame" matches argument type "Hashable"  [call-overload]
/cogcc_pipeline/features.py:262: note: Possible overload variants:
/cogcc_pipeline/features.py:262: note:     def groupby(self, by: str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], Literal[True]]
/cogcc_pipeline/features.py:262: note:     def groupby(self, by: str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], Literal[False]]
/cogcc_pipeline/features.py:262: note:     def groupby(self, by: DatetimeIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timestamp, Literal[True]]
/cogcc_pipeline/features.py:262: note:     def groupby(self, by: DatetimeIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timestamp, Literal[False]]
/cogcc_pipeline/features.py:262: note:     def groupby(self, by: TimedeltaIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timedelta, Literal[True]]
/cogcc_pipeline/features.py:262: note:     def groupby(self, by: TimedeltaIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Timedelta, Literal[False]]
/cogcc_pipeline/features.py:262: note:     def groupby(self, by: PeriodIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Period, Literal[True]]
/cogcc_pipeline/features.py:262: note:     def groupby(self, by: PeriodIndex, level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Period, Literal[False]]
/cogcc_pipeline/features.py:262: note:     def [IntervalT: Interval[Any]] groupby(self, by: IntervalIndex[IntervalT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[IntervalT, Literal[True]]
/cogcc_pipeline/features.py:262: note:     def [IntervalT: Interval[Any]] groupby(self, by: IntervalIndex[IntervalT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[IntervalT, Literal[False]]
/cogcc_pipeline/features.py:262: note:     def groupby(self, by: MultiIndex | tuple[Any, ...] | list[Any] | ufunc | Callable[..., Any] | list[ufunc | Callable[..., Any]] | list[Series[Any]] | <7 more items> | None = ..., level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[tuple[Hashable, ...], Literal[True]]
/cogcc_pipeline/features.py:262: note:     def groupby(self, by: MultiIndex | tuple[Any, ...] | list[Any] | ufunc | Callable[..., Any] | list[ufunc | Callable[..., Any]] | list[Series[Any]] | <7 more items> | None = ..., level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[tuple[Hashable, ...], Literal[False]]
/cogcc_pipeline/features.py:262: note:     def [SeriesByT: str | bytes | date | bool | int | <6 more items>] groupby(self, by: Series[SeriesByT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[SeriesByT, Literal[True]]
/cogcc_pipeline/features.py:262: note:     def [SeriesByT: str | bytes | date | bool | int | <6 more items>] groupby(self, by: Series[SeriesByT], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[SeriesByT, Literal[False]]
/cogcc_pipeline/features.py:262: note:     def groupby(self, by: CategoricalIndex[Any] | Index[Any] | Series[Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[True] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Any, Literal[True]]
/cogcc_pipeline/features.py:262: note:     def groupby(self, by: CategoricalIndex[Any] | Index[Any] | Series[Any], level: Hashable | Sequence[Hashable] | None = ..., as_index: Literal[False] = ..., sort: bool = ..., group_keys: bool = ..., observed: bool | Literal[_NoDefault.no_default] = ..., dropna: bool = ...) -> DataFrameGroupBy[Any, Literal[False]]
/cogcc_pipeline/features.py:263: error: "float" has no attribute "shift"  [attr-defined]
Found 16 errors in 3 files (checked 13 source files)

```

- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/cogcc
configfile: pytest.ini
plugins: anyio-4.12.1, mock-3.15.1, repeat-0.9.4, xdist-3.8.0, langsmith-0.7.32, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 79 items / 4 deselected / 75 selected

tests/test_acquire.py ..............                                     [ 18%]
tests/test_features.py .........................                         [ 52%]
tests/test_ingest.py ......FFFFFFFFFF.                                   [ 74%]
tests/test_transform.py .....F......F...F.F                              [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
________________ test_read_raw_file_canonical_names_and_dtypes _________________
tests/test_ingest.py:172: in test_read_raw_file_canonical_names_and_dtypes
    df = read_raw_file_to_frame(str(csv_path), schema)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:126: in read_raw_file_to_frame
    empty_frame = _empty_frame(schema)
                  ^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:219: in _empty_frame
    cols[col_name] = pd.Categorical([], dtype="object")
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/categorical.py:395: in __init__
    dtype = CategoricalDtype._from_values_or_dtype(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/dtypes.py:334: in _from_values_or_dtype
    raise ValueError(f"Unknown dtype {dtype!r}")
E   ValueError: Unknown dtype 'object'
__________________ test_read_raw_file_missing_nullable_column __________________
tests/test_ingest.py:187: in test_read_raw_file_missing_nullable_column
    df = read_raw_file_to_frame(str(csv_path), schema)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:126: in read_raw_file_to_frame
    empty_frame = _empty_frame(schema)
                  ^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:219: in _empty_frame
    cols[col_name] = pd.Categorical([], dtype="object")
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/categorical.py:395: in __init__
    dtype = CategoricalDtype._from_values_or_dtype(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/dtypes.py:334: in _from_values_or_dtype
    raise ValueError(f"Unknown dtype {dtype!r}")
E   ValueError: Unknown dtype 'object'
_____________ test_read_raw_file_missing_nonnullable_column_raises _____________
tests/test_ingest.py:202: in test_read_raw_file_missing_nonnullable_column_raises
    read_raw_file_to_frame(str(csv_path), schema)
cogcc_pipeline/ingest.py:126: in read_raw_file_to_frame
    empty_frame = _empty_frame(schema)
                  ^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:219: in _empty_frame
    cols[col_name] = pd.Categorical([], dtype="object")
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/categorical.py:395: in __init__
    dtype = CategoricalDtype._from_values_or_dtype(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/dtypes.py:334: in _from_values_or_dtype
    raise ValueError(f"Unknown dtype {dtype!r}")
E   ValueError: Unknown dtype 'object'
________________ test_read_raw_file_invalid_wellstatus_replaced ________________
tests/test_ingest.py:216: in test_read_raw_file_invalid_wellstatus_replaced
    df = read_raw_file_to_frame(str(csv_path), schema)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:126: in read_raw_file_to_frame
    empty_frame = _empty_frame(schema)
                  ^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:219: in _empty_frame
    cols[col_name] = pd.Categorical([], dtype="object")
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/categorical.py:395: in __init__
    dtype = CategoricalDtype._from_values_or_dtype(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/dtypes.py:334: in _from_values_or_dtype
    raise ValueError(f"Unknown dtype {dtype!r}")
E   ValueError: Unknown dtype 'object'
______________________ test_read_raw_file_preserves_zeros ______________________
tests/test_ingest.py:233: in test_read_raw_file_preserves_zeros
    df = read_raw_file_to_frame(str(csv_path), schema)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:126: in read_raw_file_to_frame
    empty_frame = _empty_frame(schema)
                  ^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:219: in _empty_frame
    cols[col_name] = pd.Categorical([], dtype="object")
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/categorical.py:395: in __init__
    dtype = CategoricalDtype._from_values_or_dtype(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/dtypes.py:334: in _from_values_or_dtype
    raise ValueError(f"Unknown dtype {dtype!r}")
E   ValueError: Unknown dtype 'object'
____________ test_read_raw_file_empty_after_filter_has_full_schema _____________
tests/test_ingest.py:246: in test_read_raw_file_empty_after_filter_has_full_schema
    df = read_raw_file_to_frame(str(csv_path), schema)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:126: in read_raw_file_to_frame
    empty_frame = _empty_frame(schema)
                  ^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:219: in _empty_frame
    cols[col_name] = pd.Categorical([], dtype="object")
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/categorical.py:395: in __init__
    dtype = CategoricalDtype._from_values_or_dtype(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/dtypes.py:334: in _from_values_or_dtype
    raise ValueError(f"Unknown dtype {dtype!r}")
E   ValueError: Unknown dtype 'object'
_____________________ test_ingest_writes_readable_parquet ______________________
tests/test_ingest.py:278: in test_ingest_writes_readable_parquet
    ingest(cfg)
cogcc_pipeline/ingest.py:273: in ingest
    meta_df = _empty_frame(schema)
              ^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:219: in _empty_frame
    cols[col_name] = pd.Categorical([], dtype="object")
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/categorical.py:395: in __init__
    dtype = CategoricalDtype._from_values_or_dtype(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/dtypes.py:334: in _from_values_or_dtype
    raise ValueError(f"Unknown dtype {dtype!r}")
E   ValueError: Unknown dtype 'object'
_________________ test_ingest_partition_count_in_adr004_range __________________
tests/test_ingest.py:307: in test_ingest_partition_count_in_adr004_range
    ingest(cfg)
cogcc_pipeline/ingest.py:273: in ingest
    meta_df = _empty_frame(schema)
              ^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:219: in _empty_frame
    cols[col_name] = pd.Categorical([], dtype="object")
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/categorical.py:395: in __init__
    dtype = CategoricalDtype._from_values_or_dtype(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/dtypes.py:334: in _from_values_or_dtype
    raise ValueError(f"Unknown dtype {dtype!r}")
E   ValueError: Unknown dtype 'object'
_______________ test_ingest_multiple_partitions_have_full_schema _______________
tests/test_ingest.py:333: in test_ingest_multiple_partitions_have_full_schema
    ingest(cfg)
cogcc_pipeline/ingest.py:273: in ingest
    meta_df = _empty_frame(schema)
              ^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:219: in _empty_frame
    cols[col_name] = pd.Categorical([], dtype="object")
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/categorical.py:395: in __init__
    dtype = CategoricalDtype._from_values_or_dtype(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/dtypes.py:334: in _from_values_or_dtype
    raise ValueError(f"Unknown dtype {dtype!r}")
E   ValueError: Unknown dtype 'object'
______________________ test_ingest_parquet_readable_fresh ______________________
tests/test_ingest.py:364: in test_ingest_parquet_readable_fresh
    ingest(cfg)
cogcc_pipeline/ingest.py:273: in ingest
    meta_df = _empty_frame(schema)
              ^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:219: in _empty_frame
    cols[col_name] = pd.Categorical([], dtype="object")
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/categorical.py:395: in __init__
    dtype = CategoricalDtype._from_values_or_dtype(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/dtypes.py:334: in _from_values_or_dtype
    raise ValueError(f"Unknown dtype {dtype!r}")
E   ValueError: Unknown dtype 'object'
________________ test_clean_partition_null_entity_rows_dropped _________________
tests/test_transform.py:120: in test_clean_partition_null_entity_rows_dropped
    result = clean_partition(pdf, TRANSFORM_CFG)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/transform.py:90: in clean_partition
    return _empty_clean_partition(pdf, config)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/transform.py:164: in _empty_clean_partition
    df["WellStatus"] = pd.Categorical([], categories=_WELL_STATUS_CATEGORIES)
    ^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:4672: in __setitem__
    self._set_item(key, value)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:4872: in _set_item
    value, refs = self._sanitize_column(value)
                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:5742: in _sanitize_column
    com.require_length_match(value, self.index)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/common.py:601: in require_length_match
    raise ValueError(
E   ValueError: Length of values (0) does not match length of index (1)
________________ test_transform_schema_stable_across_partitions ________________
tests/test_transform.py:294: in test_transform_schema_stable_across_partitions
    assert dict(df1.dtypes) == dict(df2.dtypes)
E   AssertionError: assert {'AcceptedDat...e=<NA>)>, ...} == {'AcceptedDat...e=<NA>)>, ...}
E     
E     Omitting 32 identical items, use -vv to show
E     Differing items:
E     {'WellStatus': CategoricalDtype(categories=[], ordered=False, categories_dtype=object)} != {'WellStatus': CategoricalDtype(categories=['AC'], ordered=False, categories_dtype=str)}
E     Use -v to get more diff
_______________________ test_transform_well_completeness _______________________
tests/test_transform.py:428: in test_transform_well_completeness
    transform(cfg)
cogcc_pipeline/transform.py:239: in transform
    ddf_out.to_parquet(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:594: in to_parquet
    i_offset, fmd, metadata_file_exists, extra_write_kwargs = engine.initialize_write(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/io/parquet/arrow.py:654: in initialize_write
    inferred_schema = pyarrow_schema_dispatch(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/utils.py:782: in __call__
    return meth(arg, *args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/backends.py:240: in get_pyarrow_schema_pandas
    return pa.Schema.from_pandas(obj, preserve_index=preserve_index)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pyarrow/types.pxi:3148: in pyarrow.lib.Schema.from_pandas
    ???
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pyarrow/pandas_compat.py:588: in dataframe_to_types
    type_ = pa.array(c, from_pandas=True).type
            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pyarrow/array.pxi:365: in pyarrow.lib.array
    ???
pyarrow/array.pxi:91: in pyarrow.lib._ndarray_to_array
    ???
pyarrow/error.pxi:92: in pyarrow.lib.check_status
    ???
E   pyarrow.lib.ArrowInvalid: Could not convert <object object at 0x165659c60> with type object: did not recognize Python value type when inferring an Arrow data type
______________ test_transform_map_partitions_meta_matches_output _______________
tests/test_transform.py:452: in test_transform_map_partitions_meta_matches_output
    empty_input = _empty_frame(schema)
                  ^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:219: in _empty_frame
    cols[col_name] = pd.Categorical([], dtype="object")
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/categorical.py:395: in __init__
    dtype = CategoricalDtype._from_values_or_dtype(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/dtypes.py:334: in _from_values_or_dtype
    raise ValueError(f"Unknown dtype {dtype!r}")
E   ValueError: Unknown dtype 'object'
=============================== warnings summary ===============================
tests/test_acquire.py: 11 warnings
  /cogcc_pipeline/acquire.py:124: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
    "downloaded_at": datetime.utcnow().isoformat(),

tests/test_acquire.py::test_download_target_success_csv
tests/test_acquire.py::test_download_target_zip_extracts_csv_removes_zip
tests/test_acquire.py::test_acquire_idempotent_file_count
tests/test_acquire.py::test_acquire_file_integrity
  /cogcc_pipeline/acquire.py:193: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
    "downloaded_at": datetime.utcnow().isoformat(),

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
=========================== short test summary info ============================
FAILED tests/test_ingest.py::test_read_raw_file_canonical_names_and_dtypes - ...
FAILED tests/test_ingest.py::test_read_raw_file_missing_nullable_column - Val...
FAILED tests/test_ingest.py::test_read_raw_file_missing_nonnullable_column_raises
FAILED tests/test_ingest.py::test_read_raw_file_invalid_wellstatus_replaced
FAILED tests/test_ingest.py::test_read_raw_file_preserves_zeros - ValueError:...
FAILED tests/test_ingest.py::test_read_raw_file_empty_after_filter_has_full_schema
FAILED tests/test_ingest.py::test_ingest_writes_readable_parquet - ValueError...
FAILED tests/test_ingest.py::test_ingest_partition_count_in_adr004_range - Va...
FAILED tests/test_ingest.py::test_ingest_multiple_partitions_have_full_schema
FAILED tests/test_ingest.py::test_ingest_parquet_readable_fresh - ValueError:...
FAILED tests/test_transform.py::test_clean_partition_null_entity_rows_dropped
FAILED tests/test_transform.py::test_transform_schema_stable_across_partitions
FAILED tests/test_transform.py::test_transform_well_completeness - pyarrow.li...
FAILED tests/test_transform.py::test_transform_map_partitions_meta_matches_output
========== 14 failed, 61 passed, 4 deselected, 15 warnings in 32.73s ===========

```

- **Integration Tests:**
```
Integration Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/cogcc
configfile: pytest.ini
plugins: anyio-4.12.1, mock-3.15.1, repeat-0.9.4, xdist-3.8.0, langsmith-0.7.32, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 79 items / 75 deselected / 4 selected

tests/test_integration.py FFFF                                           [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
___________________________ test_ingest_integration ____________________________
tests/test_integration.py:142: in test_ingest_integration
    ingest(cfg)
cogcc_pipeline/ingest.py:273: in ingest
    meta_df = _empty_frame(schema)
              ^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:219: in _empty_frame
    cols[col_name] = pd.Categorical([], dtype="object")
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/categorical.py:395: in __init__
    dtype = CategoricalDtype._from_values_or_dtype(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/dtypes.py:334: in _from_values_or_dtype
    raise ValueError(f"Unknown dtype {dtype!r}")
E   ValueError: Unknown dtype 'object'
__________________________ test_transform_integration __________________________
tests/test_integration.py:208: in test_transform_integration
    ingest(
cogcc_pipeline/ingest.py:273: in ingest
    meta_df = _empty_frame(schema)
              ^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:219: in _empty_frame
    cols[col_name] = pd.Categorical([], dtype="object")
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/categorical.py:395: in __init__
    dtype = CategoricalDtype._from_values_or_dtype(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/dtypes.py:334: in _from_values_or_dtype
    raise ValueError(f"Unknown dtype {dtype!r}")
E   ValueError: Unknown dtype 'object'
__________________________ test_features_integration ___________________________
tests/test_integration.py:270: in test_features_integration
    ingest(
cogcc_pipeline/ingest.py:273: in ingest
    meta_df = _empty_frame(schema)
              ^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:219: in _empty_frame
    cols[col_name] = pd.Categorical([], dtype="object")
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/categorical.py:395: in __init__
    dtype = CategoricalDtype._from_values_or_dtype(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/dtypes.py:334: in _from_values_or_dtype
    raise ValueError(f"Unknown dtype {dtype!r}")
E   ValueError: Unknown dtype 'object'
___________________________ test_end_to_end_pipeline ___________________________
tests/test_integration.py:351: in test_end_to_end_pipeline
    ingest(
cogcc_pipeline/ingest.py:273: in ingest
    meta_df = _empty_frame(schema)
              ^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:219: in _empty_frame
    cols[col_name] = pd.Categorical([], dtype="object")
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/categorical.py:395: in __init__
    dtype = CategoricalDtype._from_values_or_dtype(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/dtypes/dtypes.py:334: in _from_values_or_dtype
    raise ValueError(f"Unknown dtype {dtype!r}")
E   ValueError: Unknown dtype 'object'
=========================== short test summary info ============================
FAILED tests/test_integration.py::test_ingest_integration - ValueError: Unkno...
FAILED tests/test_integration.py::test_transform_integration - ValueError: Un...
FAILED tests/test_integration.py::test_features_integration - ValueError: Unk...
FAILED tests/test_integration.py::test_end_to_end_pipeline - ValueError: Unkn...
======================= 4 failed, 75 deselected in 3.90s =======================

```

---

## Eval Run at 2026-04-24 06:00:08

**Status:** ❌ FAILED

### Failures:
- **Type check:**
```
Type check failed. Fix these errors:
/cogcc_pipeline/transform.py:64: error: No overload variant of "where" of "Series" matches argument types "Series[bool]", "None"  [call-overload]
/cogcc_pipeline/transform.py:64: note: Possible overload variants:
/cogcc_pipeline/transform.py:64: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: Any | Series[Any] | Callable[..., Any | Series[Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/cogcc_pipeline/transform.py:64: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[Any]
/cogcc_pipeline/transform.py:128: error: No overload variant of "where" of "Series" matches argument types "Any", "None"  [call-overload]
/cogcc_pipeline/transform.py:128: note: Possible overload variants:
/cogcc_pipeline/transform.py:128: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: Any | Series[Any] | Callable[..., Any | Series[Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/cogcc_pipeline/transform.py:128: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[Any]
/cogcc_pipeline/transform.py:135: error: No overload variant of "where" of "Series" matches argument types "Series[bool]", "None"  [call-overload]
/cogcc_pipeline/transform.py:135: note: Possible overload variants:
/cogcc_pipeline/transform.py:135: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: Any | Series[Any] | Callable[..., Any | Series[Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/cogcc_pipeline/transform.py:135: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[Any]
/cogcc_pipeline/ingest.py:164: error: List item 0 has incompatible type "NAType"; expected "Just[float] | str | datetime | datetime64[date | int | None] | NaTType | None"  [list-item]
/cogcc_pipeline/ingest.py:164: note: Following member(s) of "NAType" have conflicts:
/cogcc_pipeline/ingest.py:164: note:     __class__: expected "type[float]", got "type[NAType]"
/cogcc_pipeline/ingest.py:165: error: Argument "dtype" to "array" has incompatible type "dtype[generic[Any]] | ExtensionDtype | Literal['object']"; expected "DatetimeTZDtype | Literal['datetime64[s, UTC]', 'datetime64[ms, UTC]', 'datetime64[us, UTC]', 'datetime64[ns, UTC]'] | dtype[datetime64[date | int | None]] | Literal['datetime64[s]', 'datetime64[ms]', 'datetime64[us]', 'datetime64[ns]', 'M8[s]', 'M8[ms]', 'M8[us]', 'M8[ns]', '<M8[s]', '<M8[ms]', '<M8[us]', '<M8[ns]']"  [arg-type]
/cogcc_pipeline/ingest.py:226: error: Incompatible types in assignment (expression has type "Categorical", target has type "Series[Any]")  [assignment]
/cogcc_pipeline/features.py:54: error: No overload variant of "__getitem__" of "DataFrameGroupBy" matches argument type "Hashable"  [call-overload]
/cogcc_pipeline/features.py:54: note: Possible overload variants:
/cogcc_pipeline/features.py:54: note:     def __getitem__(self, str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], /) -> SeriesGroupBy[Any, str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any]]
/cogcc_pipeline/features.py:54: note:     def __getitem__(self, Iterable[Hashable], /) -> DataFrameGroupBy[str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any], Literal[True]]
/cogcc_pipeline/features.py:183: error: "float" has no attribute "rolling"  [attr-defined]
/cogcc_pipeline/features.py:197: error: "float" has no attribute "shift"  [attr-defined]
/cogcc_pipeline/features.py:197: note: Error code "attr-defined" not covered by "type: ignore" comment
/cogcc_pipeline/features.py:208: error: "float" has no attribute "pct_change"  [attr-defined]
/cogcc_pipeline/features.py:208: note: Error code "attr-defined" not covered by "type: ignore" comment
/cogcc_pipeline/features.py:262: error: "float" has no attribute "shift"  [attr-defined]
/cogcc_pipeline/features.py:262: note: Error code "attr-defined" not covered by "type: ignore" comment
Found 11 errors in 3 files (checked 13 source files)

```

- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/cogcc
configfile: pytest.ini
plugins: anyio-4.12.1, mock-3.15.1, repeat-0.9.4, xdist-3.8.0, langsmith-0.7.32, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 79 items / 4 deselected / 75 selected

tests/test_acquire.py ..............                                     [ 18%]
tests/test_features.py .........................                         [ 52%]
tests/test_ingest.py ............FFFF.                                   [ 74%]
tests/test_transform.py ............F...F.F                              [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
_____________________ test_ingest_writes_readable_parquet ______________________
tests/test_ingest.py:278: in test_ingest_writes_readable_parquet
    ingest(cfg)
cogcc_pipeline/ingest.py:295: in ingest
    ddf.to_parquet(
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
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:382: in check_meta
    raise ValueError(
E   ValueError: Metadata mismatch found in `from_delayed`.
E   
E   Partition type: `pandas.DataFrame`
E   +----------------+----------------+----------------+
E   | Column         | Found          | Expected       |
E   +----------------+----------------+----------------+
E   | 'AcceptedDate' | datetime64[us] | datetime64[ns] |
E   | 'WellStatus'   | category       | category       |
E   +----------------+----------------+----------------+
_________________ test_ingest_partition_count_in_adr004_range __________________
tests/test_ingest.py:307: in test_ingest_partition_count_in_adr004_range
    ingest(cfg)
cogcc_pipeline/ingest.py:295: in ingest
    ddf.to_parquet(
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
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:382: in check_meta
    raise ValueError(
E   ValueError: Metadata mismatch found in `from_delayed`.
E   
E   Partition type: `pandas.DataFrame`
E   +----------------+----------------+----------------+
E   | Column         | Found          | Expected       |
E   +----------------+----------------+----------------+
E   | 'AcceptedDate' | datetime64[us] | datetime64[ns] |
E   | 'WellStatus'   | category       | category       |
E   +----------------+----------------+----------------+
_______________ test_ingest_multiple_partitions_have_full_schema _______________
tests/test_ingest.py:333: in test_ingest_multiple_partitions_have_full_schema
    ingest(cfg)
cogcc_pipeline/ingest.py:295: in ingest
    ddf.to_parquet(
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
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:382: in check_meta
    raise ValueError(
E   ValueError: Metadata mismatch found in `from_delayed`.
E   
E   Partition type: `pandas.DataFrame`
E   +----------------+----------------+----------------+
E   | Column         | Found          | Expected       |
E   +----------------+----------------+----------------+
E   | 'AcceptedDate' | datetime64[us] | datetime64[ns] |
E   | 'WellStatus'   | category       | category       |
E   +----------------+----------------+----------------+
______________________ test_ingest_parquet_readable_fresh ______________________
tests/test_ingest.py:364: in test_ingest_parquet_readable_fresh
    ingest(cfg)
cogcc_pipeline/ingest.py:295: in ingest
    ddf.to_parquet(
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
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:382: in check_meta
    raise ValueError(
E   ValueError: Metadata mismatch found in `from_delayed`.
E   
E   Partition type: `pandas.DataFrame`
E   +----------------+----------------+----------------+
E   | Column         | Found          | Expected       |
E   +----------------+----------------+----------------+
E   | 'AcceptedDate' | datetime64[us] | datetime64[ns] |
E   | 'WellStatus'   | category       | category       |
E   +----------------+----------------+----------------+
________________ test_transform_schema_stable_across_partitions ________________
tests/test_transform.py:291: in test_transform_schema_stable_across_partitions
    assert dict(df1.dtypes) == dict(df2.dtypes)
E   AssertionError: assert {'AcceptedDat...e=<NA>)>, ...} == {'AcceptedDat...e=<NA>)>, ...}
E     
E     Omitting 32 identical items, use -vv to show
E     Differing items:
E     {'WellStatus': CategoricalDtype(categories=[], ordered=False, categories_dtype=object)} != {'WellStatus': CategoricalDtype(categories=['AB', 'AC', 'DG', 'PA', 'PR', 'SI', 'SO', 'TA', 'WO'], ordered=False, categories_dtype=str)}
E     Use -v to get more diff
_______________________ test_transform_well_completeness _______________________
tests/test_transform.py:425: in test_transform_well_completeness
    transform(cfg)
cogcc_pipeline/transform.py:245: in transform
    ddf_out.to_parquet(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:594: in to_parquet
    i_offset, fmd, metadata_file_exists, extra_write_kwargs = engine.initialize_write(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/io/parquet/arrow.py:654: in initialize_write
    inferred_schema = pyarrow_schema_dispatch(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/utils.py:782: in __call__
    return meth(arg, *args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/backends.py:240: in get_pyarrow_schema_pandas
    return pa.Schema.from_pandas(obj, preserve_index=preserve_index)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pyarrow/types.pxi:3148: in pyarrow.lib.Schema.from_pandas
    ???
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pyarrow/pandas_compat.py:588: in dataframe_to_types
    type_ = pa.array(c, from_pandas=True).type
            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pyarrow/array.pxi:365: in pyarrow.lib.array
    ???
pyarrow/array.pxi:91: in pyarrow.lib._ndarray_to_array
    ???
pyarrow/error.pxi:92: in pyarrow.lib.check_status
    ???
E   pyarrow.lib.ArrowInvalid: Could not convert <object object at 0x166b65c60> with type object: did not recognize Python value type when inferring an Arrow data type
______________ test_transform_map_partitions_meta_matches_output _______________
tests/test_transform.py:457: in test_transform_map_partitions_meta_matches_output
    assert list(meta_df.columns) == list(actual.columns)
E   AssertionError: assert ['DocNum', 'R...Revised', ...] == ['ApiCountyCo...roduced', ...]
E     
E     At index 0 diff: 'DocNum' != 'ApiCountyCode'
E     Left contains 15 more items, first extra item: 'GasProduced'
E     Use -v to get more diff
=============================== warnings summary ===============================
tests/test_acquire.py: 11 warnings
  /cogcc_pipeline/acquire.py:124: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
    "downloaded_at": datetime.utcnow().isoformat(),

tests/test_acquire.py::test_download_target_success_csv
tests/test_acquire.py::test_download_target_zip_extracts_csv_removes_zip
tests/test_acquire.py::test_acquire_idempotent_file_count
tests/test_acquire.py::test_acquire_file_integrity
  /cogcc_pipeline/acquire.py:193: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
    "downloaded_at": datetime.utcnow().isoformat(),

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
=========================== short test summary info ============================
FAILED tests/test_ingest.py::test_ingest_writes_readable_parquet - ValueError...
FAILED tests/test_ingest.py::test_ingest_partition_count_in_adr004_range - Va...
FAILED tests/test_ingest.py::test_ingest_multiple_partitions_have_full_schema
FAILED tests/test_ingest.py::test_ingest_parquet_readable_fresh - ValueError:...
FAILED tests/test_transform.py::test_transform_schema_stable_across_partitions
FAILED tests/test_transform.py::test_transform_well_completeness - pyarrow.li...
FAILED tests/test_transform.py::test_transform_map_partitions_meta_matches_output
=========== 7 failed, 68 passed, 4 deselected, 15 warnings in 23.18s ===========

```

- **Integration Tests:**
```
Integration Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/cogcc
configfile: pytest.ini
plugins: anyio-4.12.1, mock-3.15.1, repeat-0.9.4, xdist-3.8.0, langsmith-0.7.32, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 79 items / 75 deselected / 4 selected

tests/test_integration.py FFFF                                           [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
___________________________ test_ingest_integration ____________________________
tests/test_integration.py:142: in test_ingest_integration
    ingest(cfg)
cogcc_pipeline/ingest.py:295: in ingest
    ddf.to_parquet(
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
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:382: in check_meta
    raise ValueError(
E   ValueError: Metadata mismatch found in `from_delayed`.
E   
E   Partition type: `pandas.DataFrame`
E   +----------------+----------------+----------------+
E   | Column         | Found          | Expected       |
E   +----------------+----------------+----------------+
E   | 'AcceptedDate' | datetime64[us] | datetime64[ns] |
E   | 'WellStatus'   | category       | category       |
E   +----------------+----------------+----------------+
__________________________ test_transform_integration __________________________
tests/test_integration.py:208: in test_transform_integration
    ingest(
cogcc_pipeline/ingest.py:295: in ingest
    ddf.to_parquet(
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
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:382: in check_meta
    raise ValueError(
E   ValueError: Metadata mismatch found in `from_delayed`.
E   
E   Partition type: `pandas.DataFrame`
E   +----------------+----------------+----------------+
E   | Column         | Found          | Expected       |
E   +----------------+----------------+----------------+
E   | 'AcceptedDate' | datetime64[us] | datetime64[ns] |
E   | 'WellStatus'   | category       | category       |
E   +----------------+----------------+----------------+
__________________________ test_features_integration ___________________________
tests/test_integration.py:270: in test_features_integration
    ingest(
cogcc_pipeline/ingest.py:295: in ingest
    ddf.to_parquet(
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
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:382: in check_meta
    raise ValueError(
E   ValueError: Metadata mismatch found in `from_delayed`.
E   
E   Partition type: `pandas.DataFrame`
E   +----------------+----------------+----------------+
E   | Column         | Found          | Expected       |
E   +----------------+----------------+----------------+
E   | 'AcceptedDate' | datetime64[us] | datetime64[ns] |
E   | 'WellStatus'   | category       | category       |
E   +----------------+----------------+----------------+
___________________________ test_end_to_end_pipeline ___________________________
tests/test_integration.py:351: in test_end_to_end_pipeline
    ingest(
cogcc_pipeline/ingest.py:295: in ingest
    ddf.to_parquet(
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
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:382: in check_meta
    raise ValueError(
E   ValueError: Metadata mismatch found in `from_delayed`.
E   
E   Partition type: `pandas.DataFrame`
E   +----------------+----------------+----------------+
E   | Column         | Found          | Expected       |
E   +----------------+----------------+----------------+
E   | 'AcceptedDate' | datetime64[us] | datetime64[ns] |
E   | 'WellStatus'   | category       | category       |
E   +----------------+----------------+----------------+
=========================== short test summary info ============================
FAILED tests/test_integration.py::test_ingest_integration - ValueError: Metad...
FAILED tests/test_integration.py::test_transform_integration - ValueError: Me...
FAILED tests/test_integration.py::test_features_integration - ValueError: Met...
FAILED tests/test_integration.py::test_end_to_end_pipeline - ValueError: Meta...
======================= 4 failed, 75 deselected in 4.61s =======================

```

---

## Eval Run at 2026-04-24 06:09:37

**Status:** ❌ FAILED

### Failures:
- **Type check:**
```
Type check failed. Fix these errors:
/cogcc_pipeline/ingest.py:169: error: List item 0 has incompatible type "NAType"; expected "Just[float] | str | datetime | datetime64[date | int | None] | NaTType | None"  [list-item]
/cogcc_pipeline/ingest.py:169: note: Following member(s) of "NAType" have conflicts:
/cogcc_pipeline/ingest.py:169: note:     __class__: expected "type[float]", got "type[NAType]"
/cogcc_pipeline/ingest.py:170: error: Argument "dtype" to "array" has incompatible type "dtype[generic[Any]] | ExtensionDtype"; expected "DatetimeTZDtype | Literal['datetime64[s, UTC]', 'datetime64[ms, UTC]', 'datetime64[us, UTC]', 'datetime64[ns, UTC]'] | dtype[datetime64[date | int | None]] | Literal['datetime64[s]', 'datetime64[ms]', 'datetime64[us]', 'datetime64[ns]', 'M8[s]', 'M8[ms]', 'M8[us]', 'M8[ns]', '<M8[s]', '<M8[ms]', '<M8[us]', '<M8[ns]']"  [arg-type]
/cogcc_pipeline/features.py:183: error: "float" has no attribute "rolling"  [attr-defined]
Found 3 errors in 2 files (checked 13 source files)

```

- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/cogcc
configfile: pytest.ini
plugins: anyio-4.12.1, mock-3.15.1, repeat-0.9.4, xdist-3.8.0, langsmith-0.7.32, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 79 items / 4 deselected / 75 selected

tests/test_acquire.py ..............                                     [ 18%]
tests/test_features.py .........................                         [ 52%]
tests/test_ingest.py .................                                   [ 74%]
tests/test_transform.py ............F.....F                              [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
________________ test_transform_schema_stable_across_partitions ________________
tests/test_transform.py:291: in test_transform_schema_stable_across_partitions
    assert dict(df1.dtypes) == dict(df2.dtypes)
E   AssertionError: assert {'AcceptedDat...e=<NA>)>, ...} == {'AcceptedDat...e=<NA>)>, ...}
E     
E     Omitting 32 identical items, use -vv to show
E     Differing items:
E     {'WellStatus': CategoricalDtype(categories=[], ordered=False, categories_dtype=object)} != {'WellStatus': CategoricalDtype(categories=['AB', 'AC', 'DG', 'PA', 'PR', 'SI', 'SO', 'TA', 'WO'], ordered=False, categories_dtype=str)}
E     Use -v to get more diff
______________ test_transform_map_partitions_meta_matches_output _______________
tests/test_transform.py:457: in test_transform_map_partitions_meta_matches_output
    assert list(meta_df.columns) == list(actual.columns)
E   AssertionError: assert ['DocNum', 'R...Revised', ...] == ['ApiCountyCo...roduced', ...]
E     
E     At index 0 diff: 'DocNum' != 'ApiCountyCode'
E     Left contains 15 more items, first extra item: 'GasProduced'
E     Use -v to get more diff
=============================== warnings summary ===============================
tests/test_acquire.py: 11 warnings
  /cogcc_pipeline/acquire.py:124: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
    "downloaded_at": datetime.utcnow().isoformat(),

tests/test_acquire.py::test_download_target_success_csv
tests/test_acquire.py::test_download_target_zip_extracts_csv_removes_zip
tests/test_acquire.py::test_acquire_idempotent_file_count
tests/test_acquire.py::test_acquire_file_integrity
  /cogcc_pipeline/acquire.py:193: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
    "downloaded_at": datetime.utcnow().isoformat(),

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
=========================== short test summary info ============================
FAILED tests/test_transform.py::test_transform_schema_stable_across_partitions
FAILED tests/test_transform.py::test_transform_map_partitions_meta_matches_output
=========== 2 failed, 73 passed, 4 deselected, 15 warnings in 25.57s ===========

```

---

## Eval Run at 2026-04-24 06:18:02

**Status:** ❌ FAILED

### Failures:
- **Linting:**
```
Linting failed. Fix these errors:
F841 Local variable `original_cols` is assigned to but never used
  --> cogcc_pipeline/transform.py:89:5
   |
87 |     df = pdf.copy()
88 |     # Track original columns to ensure consistent output column order
89 |     original_cols = list(pdf.columns)
   |     ^^^^^^^^^^^^^
90 |
91 |     # --- Build entity column (well identifier) from API components ---
   |
help: Remove assignment to unused variable `original_cols`

F841 Local variable `original_cols` is assigned to but never used
   --> cogcc_pipeline/transform.py:214:5
    |
213 |     df = pdf.iloc[:0].copy()
214 |     original_cols = list(pdf.columns)
    |     ^^^^^^^^^^^^^
215 |     if entity_col not in df.columns:
216 |         df[entity_col] = pd.Series([], dtype="string")
    |
help: Remove assignment to unused variable `original_cols`

Found 2 errors.
No fixes available (2 hidden fixes can be enabled with the `--unsafe-fixes` option).

```

- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/cogcc
configfile: pytest.ini
plugins: anyio-4.12.1, mock-3.15.1, repeat-0.9.4, xdist-3.8.0, langsmith-0.7.32, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 79 items / 4 deselected / 75 selected

tests/test_acquire.py ..............                                     [ 18%]
tests/test_features.py .........................                         [ 52%]
tests/test_ingest.py .................                                   [ 74%]
tests/test_transform.py ............F.....F                              [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
________________ test_transform_schema_stable_across_partitions ________________
tests/test_transform.py:291: in test_transform_schema_stable_across_partitions
    assert dict(df1.dtypes) == dict(df2.dtypes)
E   AssertionError: assert {'AcceptedDat...e=<NA>)>, ...} == {'AcceptedDat...e=<NA>)>, ...}
E     
E     Omitting 32 identical items, use -vv to show
E     Differing items:
E     {'WellStatus': CategoricalDtype(categories=[], ordered=False, categories_dtype=object)} != {'WellStatus': CategoricalDtype(categories=['AB', 'AC', 'DG', 'PA', 'PR', 'SI', 'SO', 'TA', 'WO'], ordered=False, categories_dtype=str)}
E     Use -v to get more diff
______________ test_transform_map_partitions_meta_matches_output _______________
tests/test_transform.py:457: in test_transform_map_partitions_meta_matches_output
    assert list(meta_df.columns) == list(actual.columns)
E   AssertionError: assert ['ApiCountyCo...roduced', ...] == ['ApiCountyCo...roduced', ...]
E     
E     At index 17 diff: 'DaysProduced' != 'well_id'
E     Left contains 15 more items, first extra item: 'OpNumber'
E     Use -v to get more diff
=============================== warnings summary ===============================
tests/test_acquire.py: 11 warnings
  /cogcc_pipeline/acquire.py:124: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
    "downloaded_at": datetime.utcnow().isoformat(),

tests/test_acquire.py::test_download_target_success_csv
tests/test_acquire.py::test_download_target_zip_extracts_csv_removes_zip
tests/test_acquire.py::test_acquire_idempotent_file_count
tests/test_acquire.py::test_acquire_file_integrity
  /cogcc_pipeline/acquire.py:193: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
    "downloaded_at": datetime.utcnow().isoformat(),

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
=========================== short test summary info ============================
FAILED tests/test_transform.py::test_transform_schema_stable_across_partitions
FAILED tests/test_transform.py::test_transform_map_partitions_meta_matches_output
=========== 2 failed, 73 passed, 4 deselected, 15 warnings in 24.28s ===========

```

---

## Eval Run at 2026-04-24 06:23:24

**Status:** ❌ FAILED

### Failures:
- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/cogcc
configfile: pytest.ini
plugins: anyio-4.12.1, mock-3.15.1, repeat-0.9.4, xdist-3.8.0, langsmith-0.7.32, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 79 items / 4 deselected / 75 selected

tests/test_acquire.py ..............                                     [ 18%]
tests/test_features.py .........................                         [ 52%]
tests/test_ingest.py .................                                   [ 74%]
tests/test_transform.py ............F.....F                              [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
________________ test_transform_schema_stable_across_partitions ________________
tests/test_transform.py:291: in test_transform_schema_stable_across_partitions
    assert dict(df1.dtypes) == dict(df2.dtypes)
E   AssertionError: assert {'AcceptedDat...e=<NA>)>, ...} == {'AcceptedDat...e=<NA>)>, ...}
E     
E     Omitting 32 identical items, use -vv to show
E     Differing items:
E     {'WellStatus': CategoricalDtype(categories=[], ordered=False, categories_dtype=object)} != {'WellStatus': CategoricalDtype(categories=['AB', 'AC', 'DG', 'PA', 'PR', 'SI', 'SO', 'TA', 'WO'], ordered=False, categories_dtype=str)}
E     Use -v to get more diff
______________ test_transform_map_partitions_meta_matches_output _______________
tests/test_transform.py:460: in test_transform_map_partitions_meta_matches_output
    assert meta_df[col].dtype == actual[col].dtype, (
E   AssertionError: dtype mismatch for ApiCountyCode: meta=string, actual=str
E   assert <StringDtype(na_value=<NA>)> == <StringDtype(na_value=nan)>
E    +  where <StringDtype(na_value=<NA>)> = Series([], Name: ApiCountyCode, dtype: string).dtype
E    +  and   <StringDtype(na_value=nan)> = 0    01\nName: ApiCountyCode, dtype: str.dtype
=============================== warnings summary ===============================
tests/test_acquire.py: 11 warnings
  /cogcc_pipeline/acquire.py:124: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
    "downloaded_at": datetime.utcnow().isoformat(),

tests/test_acquire.py::test_download_target_success_csv
tests/test_acquire.py::test_download_target_zip_extracts_csv_removes_zip
tests/test_acquire.py::test_acquire_idempotent_file_count
tests/test_acquire.py::test_acquire_file_integrity
  /cogcc_pipeline/acquire.py:193: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
    "downloaded_at": datetime.utcnow().isoformat(),

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
=========================== short test summary info ============================
FAILED tests/test_transform.py::test_transform_schema_stable_across_partitions
FAILED tests/test_transform.py::test_transform_map_partitions_meta_matches_output
=========== 2 failed, 73 passed, 4 deselected, 15 warnings in 26.27s ===========

```

---

## Eval Run at 2026-04-24 06:27:35

**Status:** ❌ FAILED

### Failures:
- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/cogcc
configfile: pytest.ini
plugins: anyio-4.12.1, mock-3.15.1, repeat-0.9.4, xdist-3.8.0, langsmith-0.7.32, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 79 items / 4 deselected / 75 selected

tests/test_acquire.py ..............                                     [ 18%]
tests/test_features.py .........................                         [ 52%]
tests/test_ingest.py .................                                   [ 74%]
tests/test_transform.py ..................F                              [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
______________ test_transform_map_partitions_meta_matches_output _______________
tests/test_transform.py:460: in test_transform_map_partitions_meta_matches_output
    assert meta_df[col].dtype == actual[col].dtype, (
E   AssertionError: dtype mismatch for OilProduced: meta=Float64, actual=float64
E   assert Float64Dtype() == dtype('float64')
E    +  where Float64Dtype() = Series([], Name: OilProduced, dtype: Float64).dtype
E    +  and   dtype('float64') = 0    100.0\nName: OilProduced, dtype: float64.dtype
=============================== warnings summary ===============================
tests/test_acquire.py: 11 warnings
  /cogcc_pipeline/acquire.py:124: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
    "downloaded_at": datetime.utcnow().isoformat(),

tests/test_acquire.py::test_download_target_success_csv
tests/test_acquire.py::test_download_target_zip_extracts_csv_removes_zip
tests/test_acquire.py::test_acquire_idempotent_file_count
tests/test_acquire.py::test_acquire_file_integrity
  /cogcc_pipeline/acquire.py:193: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
    "downloaded_at": datetime.utcnow().isoformat(),

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
=========================== short test summary info ============================
FAILED tests/test_transform.py::test_transform_map_partitions_meta_matches_output
=========== 1 failed, 74 passed, 4 deselected, 15 warnings in 23.25s ===========

```

---

## Eval Run at 2026-04-24 06:30:59

**Status:** ❌ FAILED

### Failures:
- **Type check:**
```
Type check failed. Fix these errors:
/cogcc_pipeline/transform.py:220: error: Item "dtype[generic[Any]]" of "dtype[generic[Any]] | ExtensionDtype" has no attribute "numpy_dtype"  [union-attr]
/cogcc_pipeline/transform.py:220: error: Item "ExtensionDtype" of "dtype[generic[Any]] | ExtensionDtype" has no attribute "numpy_dtype"  [union-attr]
/cogcc_pipeline/transform.py:275: error: Item "dtype[generic[Any]]" of "dtype[generic[Any]] | ExtensionDtype" has no attribute "numpy_dtype"  [union-attr]
/cogcc_pipeline/transform.py:275: error: Item "ExtensionDtype" of "dtype[generic[Any]] | ExtensionDtype" has no attribute "numpy_dtype"  [union-attr]
Found 4 errors in 1 file (checked 13 source files)

```

- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/cogcc
configfile: pytest.ini
plugins: anyio-4.12.1, mock-3.15.1, repeat-0.9.4, xdist-3.8.0, langsmith-0.7.32, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 79 items / 4 deselected / 75 selected

tests/test_acquire.py ..............                                     [ 18%]
tests/test_features.py .........................                         [ 52%]
tests/test_ingest.py .................                                   [ 74%]
tests/test_transform.py ..................F                              [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
______________ test_transform_map_partitions_meta_matches_output _______________
tests/test_transform.py:460: in test_transform_map_partitions_meta_matches_output
    assert meta_df[col].dtype == actual[col].dtype, (
E   AssertionError: dtype mismatch for DaysProduced: meta=int64, actual=object
E   assert dtype('int64') == dtype('O')
E    +  where dtype('int64') = Series([], Name: DaysProduced, dtype: int64).dtype
E    +  and   dtype('O') = 0    <NA>\nName: DaysProduced, dtype: object.dtype
=============================== warnings summary ===============================
tests/test_acquire.py: 11 warnings
  /cogcc_pipeline/acquire.py:124: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
    "downloaded_at": datetime.utcnow().isoformat(),

tests/test_acquire.py::test_download_target_success_csv
tests/test_acquire.py::test_download_target_zip_extracts_csv_removes_zip
tests/test_acquire.py::test_acquire_idempotent_file_count
tests/test_acquire.py::test_acquire_file_integrity
  /cogcc_pipeline/acquire.py:193: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
    "downloaded_at": datetime.utcnow().isoformat(),

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
=========================== short test summary info ============================
FAILED tests/test_transform.py::test_transform_map_partitions_meta_matches_output
=========== 1 failed, 74 passed, 4 deselected, 15 warnings in 26.31s ===========

```

---

## Eval Run at 2026-04-24 06:35:35

**Status:** ❌ FAILED

### Failures:
- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/cogcc
configfile: pytest.ini
plugins: anyio-4.12.1, mock-3.15.1, repeat-0.9.4, xdist-3.8.0, langsmith-0.7.32, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 79 items / 4 deselected / 75 selected

tests/test_acquire.py ..............                                     [ 18%]
tests/test_features.py .........................                         [ 52%]
tests/test_ingest.py .................                                   [ 74%]
tests/test_transform.py FFFFF..F..........F                              [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
__________________ test_clean_partition_negative_oil_replaced __________________
tests/test_transform.py:77: in test_clean_partition_negative_oil_replaced
    result = clean_partition(pdf, TRANSFORM_CFG)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/transform.py:231: in clean_partition
    df[col] = pd.Series([pd.NA] * len(df), dtype=target_dtype)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/series.py:514: in __init__
    data = sanitize_array(data, index, dtype, copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/construction.py:665: in sanitize_array
    subarr = _try_cast(data, dtype, copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/construction.py:848: in _try_cast
    subarr = np.asarray(arr, dtype=dtype)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E   TypeError: float() argument must be a string or a real number, not 'NAType'
____________ test_clean_partition_oil_above_unit_threshold_replaced ____________
tests/test_transform.py:85: in test_clean_partition_oil_above_unit_threshold_replaced
    result = clean_partition(pdf, TRANSFORM_CFG)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/transform.py:231: in clean_partition
    df[col] = pd.Series([pd.NA] * len(df), dtype=target_dtype)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/series.py:514: in __init__
    data = sanitize_array(data, index, dtype, copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/construction.py:665: in sanitize_array
    subarr = _try_cast(data, dtype, copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/construction.py:848: in _try_cast
    subarr = np.asarray(arr, dtype=dtype)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E   TypeError: float() argument must be a string or a real number, not 'NAType'
___________________ test_clean_partition_dedup_reduces_rows ____________________
tests/test_transform.py:94: in test_clean_partition_dedup_reduces_rows
    result = clean_partition(pdf, TRANSFORM_CFG)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/transform.py:231: in clean_partition
    df[col] = pd.Series([pd.NA] * len(df), dtype=target_dtype)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/series.py:514: in __init__
    data = sanitize_array(data, index, dtype, copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/construction.py:665: in sanitize_array
    subarr = _try_cast(data, dtype, copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/construction.py:848: in _try_cast
    subarr = np.asarray(arr, dtype=dtype)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E   TypeError: float() argument must be a string or a real number, not 'NAType'
____________________ test_clean_partition_dedup_idempotent _____________________
tests/test_transform.py:103: in test_clean_partition_dedup_idempotent
    result1 = clean_partition(pdf, TRANSFORM_CFG)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/transform.py:231: in clean_partition
    df[col] = pd.Series([pd.NA] * len(df), dtype=target_dtype)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/series.py:514: in __init__
    data = sanitize_array(data, index, dtype, copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/construction.py:665: in sanitize_array
    subarr = _try_cast(data, dtype, copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/construction.py:848: in _try_cast
    subarr = np.asarray(arr, dtype=dtype)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E   TypeError: float() argument must be a string or a real number, not 'NAType'
___________________ test_clean_partition_zero_oil_preserved ____________________
tests/test_transform.py:112: in test_clean_partition_zero_oil_preserved
    result = clean_partition(pdf, TRANSFORM_CFG)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/transform.py:231: in clean_partition
    df[col] = pd.Series([pd.NA] * len(df), dtype=target_dtype)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/series.py:514: in __init__
    data = sanitize_array(data, index, dtype, copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/construction.py:665: in sanitize_array
    subarr = _try_cast(data, dtype, copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/construction.py:848: in _try_cast
    subarr = np.asarray(arr, dtype=dtype)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E   TypeError: float() argument must be a string or a real number, not 'NAType'
____________________ test_clean_partition_opname_normalized ____________________
tests/test_transform.py:139: in test_clean_partition_opname_normalized
    result = clean_partition(pdf, TRANSFORM_CFG)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/transform.py:231: in clean_partition
    df[col] = pd.Series([pd.NA] * len(df), dtype=target_dtype)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/series.py:514: in __init__
    data = sanitize_array(data, index, dtype, copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/construction.py:665: in sanitize_array
    subarr = _try_cast(data, dtype, copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/construction.py:848: in _try_cast
    subarr = np.asarray(arr, dtype=dtype)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E   TypeError: float() argument must be a string or a real number, not 'NAType'
______________ test_transform_map_partitions_meta_matches_output _______________
tests/test_transform.py:454: in test_transform_map_partitions_meta_matches_output
    actual = clean_partition(pdf, TRANSFORM_CFG)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
cogcc_pipeline/transform.py:231: in clean_partition
    df[col] = pd.Series([pd.NA] * len(df), dtype=target_dtype)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/series.py:514: in __init__
    data = sanitize_array(data, index, dtype, copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/construction.py:665: in sanitize_array
    subarr = _try_cast(data, dtype, copy)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/construction.py:848: in _try_cast
    subarr = np.asarray(arr, dtype=dtype)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E   TypeError: float() argument must be a string or a real number, not 'NAType'
=============================== warnings summary ===============================
tests/test_acquire.py: 11 warnings
  /cogcc_pipeline/acquire.py:124: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
    "downloaded_at": datetime.utcnow().isoformat(),

tests/test_acquire.py::test_download_target_success_csv
tests/test_acquire.py::test_download_target_zip_extracts_csv_removes_zip
tests/test_acquire.py::test_acquire_idempotent_file_count
tests/test_acquire.py::test_acquire_file_integrity
  /cogcc_pipeline/acquire.py:193: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
    "downloaded_at": datetime.utcnow().isoformat(),

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
=========================== short test summary info ============================
FAILED tests/test_transform.py::test_clean_partition_negative_oil_replaced - ...
FAILED tests/test_transform.py::test_clean_partition_oil_above_unit_threshold_replaced
FAILED tests/test_transform.py::test_clean_partition_dedup_reduces_rows - Typ...
FAILED tests/test_transform.py::test_clean_partition_dedup_idempotent - TypeE...
FAILED tests/test_transform.py::test_clean_partition_zero_oil_preserved - Typ...
FAILED tests/test_transform.py::test_clean_partition_opname_normalized - Type...
FAILED tests/test_transform.py::test_transform_map_partitions_meta_matches_output
=========== 7 failed, 68 passed, 4 deselected, 15 warnings in 25.00s ===========

```

---

## Eval Run at 2026-04-24 06:37:59

**Status:** ❌ FAILED

### Failures:
- **Type check:**
```
Type check failed. Fix these errors:
/cogcc_pipeline/transform.py:234: error: Incompatible types in assignment (expression has type "NaTType", variable has type "float")  [assignment]
/cogcc_pipeline/transform.py:236: error: Incompatible types in assignment (expression has type "NAType", variable has type "float")  [assignment]
Found 2 errors in 1 file (checked 13 source files)

```

- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/cogcc
configfile: pytest.ini
plugins: anyio-4.12.1, mock-3.15.1, repeat-0.9.4, xdist-3.8.0, langsmith-0.7.32, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 79 items / 4 deselected / 75 selected

tests/test_acquire.py ..............                                     [ 18%]
tests/test_features.py .........................                         [ 52%]
tests/test_ingest.py .................                                   [ 74%]
tests/test_transform.py ..................F                              [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
______________ test_transform_map_partitions_meta_matches_output _______________
tests/test_transform.py:460: in test_transform_map_partitions_meta_matches_output
    assert meta_df[col].dtype == actual[col].dtype, (
E   AssertionError: dtype mismatch for DaysProduced: meta=int64, actual=float64
E   assert dtype('int64') == dtype('float64')
E    +  where dtype('int64') = Series([], Name: DaysProduced, dtype: int64).dtype
E    +  and   dtype('float64') = 0   NaN\nName: DaysProduced, dtype: float64.dtype
=============================== warnings summary ===============================
tests/test_acquire.py: 11 warnings
  /cogcc_pipeline/acquire.py:124: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
    "downloaded_at": datetime.utcnow().isoformat(),

tests/test_acquire.py::test_download_target_success_csv
tests/test_acquire.py::test_download_target_zip_extracts_csv_removes_zip
tests/test_acquire.py::test_acquire_idempotent_file_count
tests/test_acquire.py::test_acquire_file_integrity
  /cogcc_pipeline/acquire.py:193: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
    "downloaded_at": datetime.utcnow().isoformat(),

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
=========================== short test summary info ============================
FAILED tests/test_transform.py::test_transform_map_partitions_meta_matches_output
=========== 1 failed, 74 passed, 4 deselected, 15 warnings in 13.42s ===========

```

---

## Eval Run at 2026-04-24 06:41:39

**Status:** ❌ FAILED

### Failures:
- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/cogcc
configfile: pytest.ini
plugins: anyio-4.12.1, mock-3.15.1, repeat-0.9.4, xdist-3.8.0, langsmith-0.7.32, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 79 items / 4 deselected / 75 selected

tests/test_acquire.py ..............                                     [ 18%]
tests/test_features.py .........................                         [ 52%]
tests/test_ingest.py .................                                   [ 74%]
tests/test_transform.py ..................F                              [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
______________ test_transform_map_partitions_meta_matches_output _______________
tests/test_transform.py:460: in test_transform_map_partitions_meta_matches_output
    assert meta_df[col].dtype == actual[col].dtype, (
E   AssertionError: dtype mismatch for OpNumber: meta=float64, actual=string
E   assert dtype('float64') == <StringDtype(na_value=<NA>)>
E    +  where dtype('float64') = Series([], Name: OpNumber, dtype: float64).dtype
E    +  and   <StringDtype(na_value=<NA>)> = 0    <NA>\nName: OpNumber, dtype: string.dtype
=============================== warnings summary ===============================
tests/test_acquire.py: 11 warnings
  /cogcc_pipeline/acquire.py:124: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
    "downloaded_at": datetime.utcnow().isoformat(),

tests/test_acquire.py::test_download_target_success_csv
tests/test_acquire.py::test_download_target_zip_extracts_csv_removes_zip
tests/test_acquire.py::test_acquire_idempotent_file_count
tests/test_acquire.py::test_acquire_file_integrity
  /cogcc_pipeline/acquire.py:193: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
    "downloaded_at": datetime.utcnow().isoformat(),

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
=========================== short test summary info ============================
FAILED tests/test_transform.py::test_transform_map_partitions_meta_matches_output
=========== 1 failed, 74 passed, 4 deselected, 15 warnings in 23.34s ===========

```

---

## Eval Run at 2026-04-24 06:43:49

**Status:** ❌ FAILED

### Failures:
- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/cogcc
configfile: pytest.ini
plugins: anyio-4.12.1, mock-3.15.1, repeat-0.9.4, xdist-3.8.0, langsmith-0.7.32, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 79 items / 4 deselected / 75 selected

tests/test_acquire.py ..............                                     [ 18%]
tests/test_features.py .........................                         [ 52%]
tests/test_ingest.py .................                                   [ 74%]
tests/test_transform.py ..................F                              [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
______________ test_transform_map_partitions_meta_matches_output _______________
tests/test_transform.py:460: in test_transform_map_partitions_meta_matches_output
    assert meta_df[col].dtype == actual[col].dtype, (
E   AssertionError: dtype mismatch for Revised: meta=bool, actual=string
E   assert dtype('bool') == <StringDtype(na_value=<NA>)>
E    +  where dtype('bool') = Series([], Name: Revised, dtype: bool).dtype
E    +  and   <StringDtype(na_value=<NA>)> = 0    <NA>\nName: Revised, dtype: string.dtype
=============================== warnings summary ===============================
tests/test_acquire.py: 11 warnings
  /cogcc_pipeline/acquire.py:124: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
    "downloaded_at": datetime.utcnow().isoformat(),

tests/test_acquire.py::test_download_target_success_csv
tests/test_acquire.py::test_download_target_zip_extracts_csv_removes_zip
tests/test_acquire.py::test_acquire_idempotent_file_count
tests/test_acquire.py::test_acquire_file_integrity
  /cogcc_pipeline/acquire.py:193: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
    "downloaded_at": datetime.utcnow().isoformat(),

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
=========================== short test summary info ============================
FAILED tests/test_transform.py::test_transform_map_partitions_meta_matches_output
=========== 1 failed, 74 passed, 4 deselected, 15 warnings in 13.63s ===========

```

---

## Eval Run at 2026-04-24 06:46:34

**Status:** ✅ PASSED

---
