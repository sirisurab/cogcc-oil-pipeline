
## Eval Run at 2026-04-09 05:13:25

**Status:** ❌ FAILED

### Failures:
- **Linting:**
```
Linting failed. Fix these errors:
E741 Ambiguous variable name: `l`
   --> cogcc_pipeline/features.py:173:75
    |
171 |     for lag in lags:
172 |         new_col = f"oil_lag_{lag}m"
173 |         result = df.groupby("well_id")["OilProduced"].transform(lambda s, l=lag: s.shift(l))
    |                                                                           ^
174 |         df[new_col] = result.astype(pd.Float64Dtype())
    |

F841 Local variable `result` is assigned to but never used
   --> tests/test_features.py:268:5
    |
266 |     # pct_change * -1: (old - new) / old → high for near-zero decline
267 |     df = _make_well_df(["W1", "W1", "W1"], [100.0, 1.0, 0.001])
268 |     result = _add_decline_rates(df)
    |     ^^^^^^
269 |     # row 2: (1-0.001)/1 * -1 ... actually (prev-cur)/prev = (1-0.001)/1 = 0.999, * -1 = -0.999
270 |     # Not above 10. Let's try: 0.001 → 0.0001
    |
help: Remove assignment to unused variable `result`

Found 2 errors.
No fixes available (1 hidden fix can be enabled with the `--unsafe-fixes` option).

```

- **Type check:**
```
Type check failed. Fix these errors:
/cogcc_pipeline/transform.py:120: error: No overload variant of "where" of "Series" matches argument types "Series[bool]", "NAType"  [call-overload]
/cogcc_pipeline/transform.py:120: note: Possible overload variants:
/cogcc_pipeline/transform.py:120: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: Any | Series[Any] | Callable[..., Any | Series[Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/cogcc_pipeline/transform.py:120: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[Any]
/cogcc_pipeline/ingest.py:101: error: Argument "dtype" to "read_csv" has incompatible type "dict[str, object]"; expected "ExtensionDtype | str | dtype[generic[Any]] | type[complex] | type[bool] | type[object] | type[str] | Mapping[Hashable, ExtensionDtype | str | dtype[generic[Any]] | type[complex] | type[bool] | type[object] | type[str]] | defaultdict[Any, Any] | None"  [arg-type]
/cogcc_pipeline/ingest.py:151: error: No overload variant of "array" matches argument types "list[Never]", "object"  [call-overload]
/cogcc_pipeline/ingest.py:151: note: Possible overload variants:
/cogcc_pipeline/ingest.py:151: note:     def array(data: Sequence[Just[float]], dtype: Literal['Float32', 'Float64'] | Float32Dtype | Float64Dtype | None = ..., copy: bool = ...) -> FloatingArray
/cogcc_pipeline/ingest.py:151: note:     def array(data: tuple[Any, ...] | MutableSequence[Any], dtype: type[builtins.bool] | Literal['bool'] | type[int] | Literal['int'] | type[float] | Literal['float'] | type[complex] | Literal['complex'] | type[bytes] | Literal['bytes'] | type[object] | Literal['object'] | Literal['?', 'b1', 'bool_'] | type[numpy.bool[builtins.bool]] | Literal['b', 'i1', 'int8', 'byte', 'h', 'i2', 'int16', 'short', 'i', 'i4', 'int32', 'intc', 'l', 'long', 'l', 'i8', 'int64', 'int_', 'long', 'q', 'longlong', 'p', 'intp'] | type[signedinteger[_8Bit]] | type[signedinteger[_16Bit]] | type[signedinteger[_32Bit]] | type[signedinteger[_32Bit | _64Bit]] | type[signedinteger[_64Bit]] | type[signedinteger[_32Bit | _64Bit]] | Literal['B', 'u1', 'uint8', 'ubyte', 'H', 'u2', 'uint16', 'ushort', 'I', 'u4', 'uint32', 'uintc', 'L', 'ulong', 'L', 'u8', 'uint', 'ulong', 'uint64', 'Q', 'ulonglong', 'P', 'uintp'] | type[unsignedinteger[_8Bit]] | type[unsignedinteger[_16Bit]] | type[unsignedinteger[_32Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | type[unsignedinteger[_64Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | Literal['e', 'f2', '<f2', 'float16', 'half'] | type[floating[_16Bit]] | Literal['f', 'f4', 'float32', 'single', 'd', 'f8', 'float64', 'double', 'g', 'f16', 'float128', 'longdouble'] | type[floating[_32Bit]] | type[float64] | type[floating[_64Bit | _96Bit | _128Bit]] | Literal['F', 'c8', 'complex64', 'csingle', 'D', 'c16', 'complex128', 'cdouble', 'G', 'c32', 'complex256', 'clongdouble'] | type[complexfloating[_32Bit, _32Bit]] | type[complex128] | type[complexfloating[_64Bit | _96Bit | _128Bit, _64Bit | _96Bit | _128Bit]] | Literal['U', 'str_', 'unicode'] | type[str_] | Literal['S', 'bytes_'] | type[bytes_] | Literal['object_', 'O'] | type[object_] | Literal['V', 'void'] | type[void], copy: bool = ...) -> NumpyExtensionArray
/cogcc_pipeline/ingest.py:151: note:     def array(data: Sequence[NAType | None], dtype: type[builtins.bool] | Literal['bool'] | type[int] | Literal['int'] | type[float] | Literal['float'] | type[complex] | Literal['complex'] | type[bytes] | Literal['bytes'] | type[object] | Literal['object'] | Literal['?', 'b1', 'bool_'] | type[numpy.bool[builtins.bool]] | Literal['b', 'i1', 'int8', 'byte', 'h', 'i2', 'int16', 'short', 'i', 'i4', 'int32', 'intc', 'l', 'long', 'l', 'i8', 'int64', 'int_', 'long', 'q', 'longlong', 'p', 'intp'] | type[signedinteger[_8Bit]] | type[signedinteger[_16Bit]] | type[signedinteger[_32Bit]] | type[signedinteger[_32Bit | _64Bit]] | type[signedinteger[_64Bit]] | type[signedinteger[_32Bit | _64Bit]] | Literal['B', 'u1', 'uint8', 'ubyte', 'H', 'u2', 'uint16', 'ushort', 'I', 'u4', 'uint32', 'uintc', 'L', 'ulong', 'L', 'u8', 'uint', 'ulong', 'uint64', 'Q', 'ulonglong', 'P', 'uintp'] | type[unsignedinteger[_8Bit]] | type[unsignedinteger[_16Bit]] | type[unsignedinteger[_32Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | type[unsignedinteger[_64Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | Literal['e', 'f2', '<f2', 'float16', 'half'] | type[floating[_16Bit]] | Literal['f', 'f4', 'float32', 'single', 'd', 'f8', 'float64', 'double', 'g', 'f16', 'float128', 'longdouble'] | type[floating[_32Bit]] | type[float64] | type[floating[_64Bit | _96Bit | _128Bit]] | Literal['F', 'c8', 'complex64', 'csingle', 'D', 'c16', 'complex128', 'cdouble', 'G', 'c32', 'complex256', 'clongdouble'] | type[complexfloating[_32Bit, _32Bit]] | type[complex128] | type[complexfloating[_64Bit | _96Bit | _128Bit, _64Bit | _96Bit | _128Bit]] | Literal['U', 'str_', 'unicode'] | type[str_] | Literal['S', 'bytes_'] | type[bytes_] | Literal['object_', 'O'] | type[object_] | Literal['V', 'void'] | type[void] | None = ..., copy: bool = ...) -> NumpyExtensionArray
/cogcc_pipeline/ingest.py:151: note:     def array(data: Sequence[Timedelta] | Series[Timedelta] | TimedeltaArray | TimedeltaIndex | ndarray[tuple[int], dtype[timedelta64[timedelta | int | None]]], dtype: Literal['timedelta64[s]', 'timedelta64[ms]', 'timedelta64[us]', 'timedelta64[ns]', 'm8[s]', 'm8[ms]', 'm8[us]', 'm8[ns]', '<m8[s]', '<m8[ms]', '<m8[us]', '<m8[ns]'] | Literal['duration[s][pyarrow]', 'duration[ms][pyarrow]', 'duration[us][pyarrow]', 'duration[ns][pyarrow]'] | None = ..., copy: bool = ...) -> TimedeltaArray
/cogcc_pipeline/ingest.py:151: note:     def array(data: Sequence[builtins.bool | numpy.bool[builtins.bool] | Just[float] | NAType | None], dtype: BooleanDtype | Literal['boolean'], copy: bool = ...) -> BooleanArray
/cogcc_pipeline/ingest.py:151: note:     def array(data: Sequence[builtins.bool | numpy.bool[builtins.bool] | NAType | None], dtype: None = ..., copy: bool = ...) -> BooleanArray
/cogcc_pipeline/ingest.py:151: note:     def array(data: ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | BooleanArray, dtype: BooleanDtype | Literal['boolean'] | None = ..., copy: bool = ...) -> BooleanArray
/cogcc_pipeline/ingest.py:151: note:     def array(data: Sequence[float | integer[Any] | NAType | None], dtype: Literal['Int8', 'Int16', 'Int32', 'Int64'] | Int8Dtype | Int16Dtype | Int32Dtype | Int64Dtype | Literal['UInt8', 'UInt16', 'UInt32', 'UInt64'] | UInt8Dtype | UInt16Dtype | UInt32Dtype | UInt64Dtype, copy: bool = ...) -> IntegerArray
/cogcc_pipeline/ingest.py:151: note:     def array(data: Sequence[int | integer[Any] | NAType | None], dtype: None = ..., copy: bool = ...) -> IntegerArray
/cogcc_pipeline/ingest.py:151: note:     def array(data: ndarray[tuple[Any, ...], dtype[integer[Any]]] | IntegerArray, dtype: Literal['Int8', 'Int16', 'Int32', 'Int64'] | Int8Dtype | Int16Dtype | Int32Dtype | Int64Dtype | Literal['UInt8', 'UInt16', 'UInt32', 'UInt64'] | UInt8Dtype | UInt16Dtype | UInt32Dtype | UInt64Dtype | None = ..., copy: bool = ...) -> IntegerArray
/cogcc_pipeline/ingest.py:151: note:     def array(data: Sequence[float | floating[Any] | NAType | None] | ndarray[tuple[Any, ...], dtype[floating[Any]]] | FloatingArray, dtype: Literal['Float32', 'Float64'] | Float32Dtype | Float64Dtype | None = ..., copy: bool = ...) -> FloatingArray
/cogcc_pipeline/ingest.py:151: note:     def array(data: tuple[Just[float] | str | datetime | datetime64[date | int | None] | NaTType | None, ...] | MutableSequence[Just[float] | str | datetime | datetime64[date | int | None] | NaTType | None] | ndarray[tuple[Any, ...], dtype[Any]] | DatetimeArray, dtype: DatetimeTZDtype | Literal['datetime64[s, UTC]', 'datetime64[ms, UTC]', 'datetime64[us, UTC]', 'datetime64[ns, UTC]'] | dtype[datetime64[date | int | None]] | Literal['datetime64[s]', 'datetime64[ms]', 'datetime64[us]', 'datetime64[ns]', 'M8[s]', 'M8[ms]', 'M8[us]', 'M8[ns]', '<M8[s]', '<M8[ms]', '<M8[us]', '<M8[ns]'], copy: bool = ...) -> DatetimeArray
/cogcc_pipeline/ingest.py:151: note:     def array(data: Sequence[datetime | NaTType | None] | Sequence[datetime64[date | int | None] | NaTType | None] | ndarray[tuple[Any, ...], dtype[datetime64[date | int | None]]] | DatetimeArray, dtype: None = ..., copy: bool = ...) -> DatetimeArray
/cogcc_pipeline/ingest.py:151: note:     def array(data: tuple[Just[float] | str | str_ | NAType | None, ...] | MutableSequence[Just[float] | str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[Any]] | BaseStringArray[None], dtype: StringDtype[Never], copy: bool = ...) -> BaseStringArray[None]
/cogcc_pipeline/ingest.py:151: note:     def array(data: tuple[Just[float] | str | str_ | NAType | None, ...] | MutableSequence[Just[float] | str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[Any]] | BaseStringArray[None], dtype: StringDtype[Literal['pyarrow']] | Literal['string[pyarrow]'], copy: bool = ...) -> ArrowStringArray
/cogcc_pipeline/ingest.py:151: note:     def array(data: tuple[Just[float] | str | str_ | NAType | None, ...] | MutableSequence[Just[float] | str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[Any]] | BaseStringArray[None], dtype: StringDtype[Literal['python']] | Literal['string[python]'], copy: bool = ...) -> StringArray
/cogcc_pipeline/ingest.py:151: note:     def array(data: tuple[Just[float] | str | str_ | NAType | None, ...] | MutableSequence[Just[float] | str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[Any]] | BaseStringArray[None], dtype: StringDtype[None] | Literal['string'], copy: bool = ...) -> BaseStringArray[None]
/cogcc_pipeline/ingest.py:151: note:     def array(data: tuple[str | str_ | NAType | None, ...] | MutableSequence[str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[str_]] | BaseStringArray[None], dtype: None = ..., copy: bool = ...) -> BaseStringArray[None]
/cogcc_pipeline/ingest.py:151: note:     def array(data: tuple[Any, ...] | MutableSequence[Any], dtype: None = ..., copy: bool = ...) -> NumpyExtensionArray
/cogcc_pipeline/ingest.py:151: note:     def array(data: ndarray[tuple[Any, ...], dtype[Any]] | NumpyExtensionArray | RangeIndex, dtype: type[builtins.bool] | Literal['bool'] | type[int] | Literal['int'] | type[float] | Literal['float'] | type[complex] | Literal['complex'] | type[bytes] | Literal['bytes'] | type[object] | Literal['object'] | Literal['?', 'b1', 'bool_'] | type[numpy.bool[builtins.bool]] | Literal['b', 'i1', 'int8', 'byte', 'h', 'i2', 'int16', 'short', 'i', 'i4', 'int32', 'intc', 'l', 'long', 'l', 'i8', 'int64', 'int_', 'long', 'q', 'longlong', 'p', 'intp'] | type[signedinteger[_8Bit]] | type[signedinteger[_16Bit]] | type[signedinteger[_32Bit]] | type[signedinteger[_32Bit | _64Bit]] | type[signedinteger[_64Bit]] | type[signedinteger[_32Bit | _64Bit]] | Literal['B', 'u1', 'uint8', 'ubyte', 'H', 'u2', 'uint16', 'ushort', 'I', 'u4', 'uint32', 'uintc', 'L', 'ulong', 'L', 'u8', 'uint', 'ulong', 'uint64', 'Q', 'ulonglong', 'P', 'uintp'] | type[unsignedinteger[_8Bit]] | type[unsignedinteger[_16Bit]] | type[unsignedinteger[_32Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | type[unsignedinteger[_64Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | Literal['e', 'f2', '<f2', 'float16', 'half'] | type[floating[_16Bit]] | Literal['f', 'f4', 'float32', 'single', 'd', 'f8', 'float64', 'double', 'g', 'f16', 'float128', 'longdouble'] | type[floating[_32Bit]] | type[float64] | type[floating[_64Bit | _96Bit | _128Bit]] | Literal['F', 'c8', 'complex64', 'csingle', 'D', 'c16', 'complex128', 'cdouble', 'G', 'c32', 'complex256', 'clongdouble'] | type[complexfloating[_32Bit, _32Bit]] | type[complex128] | type[complexfloating[_64Bit | _96Bit | _128Bit, _64Bit | _96Bit | _128Bit]] | Literal['U', 'str_', 'unicode'] | type[str_] | Literal['S', 'bytes_'] | type[bytes_] | Literal['object_', 'O'] | type[object_] | Literal['V', 'void'] | type[void] | None = ..., copy: bool = ...) -> NumpyExtensionArray
/tests/conftest.py:143: error: Value expression in dictionary comprehension has incompatible type "object"; expected type "ExtensionDtype | str | dtype[generic[Any]] | type[object]"  [misc]
/tests/conftest.py:158: error: Argument 1 to "array" has incompatible type "list[str]"; expected "tuple[Just[float] | str | str_ | NAType | None, ...] | MutableSequence[Just[float] | str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[Any]] | BaseStringArray[None]"  [arg-type]
/tests/conftest.py:158: error: Argument "dtype" to "array" has incompatible type "StringDtype[None]"; expected "StringDtype[Never]"  [arg-type]
/tests/test_features.py:40: error: Argument 1 to "array" has incompatible type "list[str]"; expected "tuple[Just[float] | str | str_ | NAType | None, ...] | MutableSequence[Just[float] | str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[Any]] | BaseStringArray[None]"  [arg-type]
/tests/test_features.py:40: error: Argument "dtype" to "array" has incompatible type "StringDtype[None]"; expected "StringDtype[Never]"  [arg-type]
/tests/test_transform.py:209: error: Argument 1 to "to_datetime" has incompatible type "list[NaTType]"; expected "Sequence[float | date] | list[str] | tuple[float | str | date, ...] | ndarray[tuple[Any, ...], dtype[datetime64[date | int | None]]] | ndarray[tuple[Any, ...], dtype[str_]] | ndarray[tuple[Any, ...], dtype[integer[Any]]] | ndarray[tuple[Any, ...], dtype[floating[Any]]] | Index[Any] | ExtensionArray"  [arg-type]
Found 9 errors in 5 files (checked 12 source files)

```

- **Unit Tests:**
```
Unit Tests failed. Fix these errors:

```

---

## Eval Run at 2026-04-09 05:36:06

**Status:** ❌ FAILED

### Failures:
- **Type check:**
```
Type check failed. Fix these errors:
/cogcc_pipeline/ingest.py:102: error: Argument "dtype" to "read_csv" has incompatible type "dict[str, Any]"; expected "ExtensionDtype | str | dtype[generic[Any]] | type[complex] | type[bool] | type[object] | type[str] | Mapping[Hashable, ExtensionDtype | str | dtype[generic[Any]] | type[complex] | type[bool] | type[object] | type[str]] | defaultdict[Any, Any] | None"  [arg-type]
/cogcc_pipeline/ingest.py:152: error: No overload variant of "Series" matches argument types "list[Never]", "object"  [call-overload]
/cogcc_pipeline/ingest.py:152: note: Possible overload variants:
/cogcc_pipeline/ingest.py:152: note:     def [S1: str | bytes | bool | int | float | complex | <6 more items> | type[str] | list[str] | date | time | datetime | timedelta] __new__(cls, data: Sequence[Never], index: Mapping[Any, Any] | ExtensionArray | ndarray[tuple[Any, ...], dtype[Any]] | Index[Any] | Series[Any] | SequenceNotStr[Any] | range | KeysView[Any] | None = ..., dtype: None = ..., name: Hashable = ..., copy: bool | None = ...) -> Series[Any]
/cogcc_pipeline/ingest.py:152: note:     def [S1: str | bytes | bool | int | float | complex | <6 more items> | type[str] | list[str] | date | time | datetime | timedelta] __new__(cls, data: Sequence[list[str]], index: Mapping[Any, Any] | ExtensionArray | ndarray[tuple[Any, ...], dtype[Any]] | Index[Any] | Series[Any] | SequenceNotStr[Any] | range | KeysView[Any] | None = ..., dtype: None = ..., name: Hashable = ..., copy: bool | None = ...) -> Series[list[str]]
/cogcc_pipeline/ingest.py:152: note:     def [S1: str | bytes | bool | int | float | complex | <6 more items> | type[str] | list[str] | date | time | datetime | timedelta] __new__(cls, data: Sequence[str], index: Mapping[Any, Any] | ExtensionArray | ndarray[tuple[Any, ...], dtype[Any]] | Index[Any] | Series[Any] | SequenceNotStr[Any] | range | KeysView[Any] | None = ..., dtype: ExtensionDtype | str | dtype[generic[Any]] | type[complex] | type[bool] | type[object] | type[str] | None = ..., name: Hashable = ..., copy: bool | None = ...) -> Series[str]
/cogcc_pipeline/ingest.py:152: note:     def [S1: str | bytes | bool | int | float | complex | <6 more items> | type[str] | list[str] | date | time | datetime | timedelta, HashableT1: Hashable] __new__(cls, data: DatetimeIndex | Sequence[datetime64[date | int | None] | datetime | date] | dict[HashableT1, datetime64[date | int | None] | datetime | date] | datetime64[date | int | None] | datetime | date, index: Mapping[Any, Any] | ExtensionArray | ndarray[tuple[Any, ...], dtype[Any]] | Index[Any] | Series[Any] | SequenceNotStr[Any] | range | KeysView[Any] | None = ..., dtype: Literal['datetime64[s, UTC]', 'datetime64[ms, UTC]', 'datetime64[us, UTC]', 'datetime64[ns, UTC]'] | Literal['datetime64[s]', 'datetime64[ms]', 'datetime64[us]', 'datetime64[ns]', 'M8[s]', 'M8[ms]', 'M8[us]', 'M8[ns]', '<M8[s]', '<M8[ms]', '<M8[us]', '<M8[ns]'] | Literal['date32[pyarrow]', 'date64[pyarrow]', 'timestamp[s][pyarrow]', 'timestamp[ms][pyarrow]', 'timestamp[us][pyarrow]', 'timestamp[ns][pyarrow]'] = ..., name: Hashable = ..., copy: bool | None = ...) -> Series[Timestamp]
/cogcc_pipeline/ingest.py:152: note:     def [S1: str | bytes | bool | int | float | complex | <6 more items> | type[str] | list[str] | date | time | datetime | timedelta] __new__(cls, data: Sequence[datetime | timedelta64[timedelta | int | None]] | ndarray[tuple[Any, ...], dtype[datetime64[date | int | None]]] | DatetimeArray, index: Mapping[Any, Any] | ExtensionArray | ndarray[tuple[Any, ...], dtype[Any]] | Index[Any] | Series[Any] | SequenceNotStr[Any] | range | KeysView[Any] | None = ..., *, dtype: Literal['datetime64[s, UTC]', 'datetime64[ms, UTC]', 'datetime64[us, UTC]', 'datetime64[ns, UTC]'] | Literal['datetime64[s]', 'datetime64[ms]', 'datetime64[us]', 'datetime64[ns]', 'M8[s]', 'M8[ms]', 'M8[us]', 'M8[ns]', '<M8[s]', '<M8[ms]', '<M8[us]', '<M8[ns]'] | Literal['date32[pyarrow]', 'date64[pyarrow]', 'timestamp[s][pyarrow]', 'timestamp[ms][pyarrow]', 'timestamp[us][pyarrow]', 'timestamp[ns][pyarrow]'], name: Hashable = ..., copy: bool | None = ...) -> Series[Timestamp]
/cogcc_pipeline/ingest.py:152: note:     def [S1: str | bytes | bool | int | float | complex | <6 more items> | type[str] | list[str] | date | time | datetime | timedelta] __new__(cls, data: ExtensionArray | ndarray[tuple[Any, ...], dtype[Any]] | dict[str, ndarray[tuple[Any, ...], dtype[Any]]] | SequenceNotStr[Any], index: Mapping[Any, Any] | ExtensionArray | ndarray[tuple[Any, ...], dtype[Any]] | Index[Any] | Series[Any] | SequenceNotStr[Any] | range | KeysView[Any] | None = ..., *, dtype: CategoricalDtype | Literal['category'], name: Hashable = ..., copy: bool | None = ...) -> Series[CategoricalDtype]
/cogcc_pipeline/ingest.py:152: note:     def [S1: str | bytes | bool | int | float | complex | <6 more items> | type[str] | list[str] | date | time | datetime | timedelta] __new__(cls, data: PeriodIndex | Sequence[Period], index: Mapping[Any, Any] | ExtensionArray | ndarray[tuple[Any, ...], dtype[Any]] | Index[Any] | Series[Any] | SequenceNotStr[Any] | range | KeysView[Any] | None = ..., dtype: PeriodDtype = ..., name: Hashable = ..., copy: bool | None = ...) -> Series[Period]
/cogcc_pipeline/ingest.py:152: note:     def [S1: str | bytes | bool | int | float | complex | <6 more items> | type[str] | list[str] | date | time | datetime | timedelta] __new__(cls, data: Sequence[BaseOffset], index: Mapping[Any, Any] | ExtensionArray | ndarray[tuple[Any, ...], dtype[Any]] | Index[Any] | Series[Any] | SequenceNotStr[Any] | range | KeysView[Any] | None = ..., dtype: PeriodDtype = ..., name: Hashable = ..., copy: bool | None = ...) -> Series[BaseOffset]
/cogcc_pipeline/ingest.py:152: note:     def [S1: str | bytes | bool | int | float | complex | <6 more items> | type[str] | list[str] | date | time | datetime | timedelta, HashableT1: Hashable] __new__(cls, data: TimedeltaIndex | Sequence[timedelta64[timedelta | int | None] | timedelta] | dict[HashableT1, timedelta64[timedelta | int | None] | timedelta] | timedelta64[timedelta | int | None] | timedelta, index: Mapping[Any, Any] | ExtensionArray | ndarray[tuple[Any, ...], dtype[Any]] | Index[Any] | Series[Any] | SequenceNotStr[Any] | range | KeysView[Any] | None = ..., dtype: Literal['timedelta64[s]', 'timedelta64[ms]', 'timedelta64[us]', 'timedelta64[ns]', 'm8[s]', 'm8[ms]', 'm8[us]', 'm8[ns]', '<m8[s]', '<m8[ms]', '<m8[us]', '<m8[ns]'] | Literal['duration[s][pyarrow]', 'duration[ms][pyarrow]', 'duration[us][pyarrow]', 'duration[ns][pyarrow]'] = ..., name: Hashable = ..., copy: bool | None = ...) -> Series[Timedelta]
/cogcc_pipeline/ingest.py:152: note:     def [S1: str | bytes | bool | int | float | complex | <6 more items> | type[str] | list[str] | date | time | datetime | timedelta, OrderableT: int | float | Timestamp | Timedelta] __new__(cls, data: IntervalIndex[Interval[OrderableT]] | Interval[OrderableT] | Sequence[Interval[OrderableT]] | dict[Hashable, Interval[OrderableT]], index: Mapping[Any, Any] | ExtensionArray | ndarray[tuple[Any, ...], dtype[Any]] | Index[Any] | Series[Any] | SequenceNotStr[Any] | range | KeysView[Any] | None = ..., dtype: Literal['Interval'] = ..., name: Hashable = ..., copy: bool | None = ...) -> Series[Interval[OrderableT]]
/cogcc_pipeline/ingest.py:152: note:     def [S1: str | bytes | bool | int | float | complex | <6 more items> | type[str] | list[str] | date | time | datetime | timedelta] __new__(cls, data: Sequence[builtins.bool | numpy.bool[builtins.bool]], index: Mapping[Any, Any] | ExtensionArray | ndarray[tuple[Any, ...], dtype[Any]] | Index[Any] | Series[Any] | SequenceNotStr[Any] | range | KeysView[Any] | None = ..., dtype: Literal['bool', 'boolean', '?', 'b1', 'bool_', 'bool[pyarrow]', 'boolean[pyarrow]'] | type[builtins.bool] | BooleanDtype | type[numpy.bool[builtins.bool]] | None = ..., name: Hashable = ..., copy: bool | None = ...) -> Series[bool]
/cogcc_pipeline/ingest.py:152: note:     def [S1: str | bytes | bool | int | float | complex | <6 more items> | type[str] | list[str] | date | time | datetime | timedelta] __new__(cls, data: Sequence[int | integer[Any]], index: Mapping[Any, Any] | ExtensionArray | ndarray[tuple[Any, ...], dtype[Any]] | Index[Any] | Series[Any] | SequenceNotStr[Any] | range | KeysView[Any] | None = ..., dtype: Literal['int', 'Int8', 'Int16', 'Int32', 'Int64', 'b', 'i1', 'int8', 'byte', 'h', 'i2', 'int16', 'short', 'i', 'i4', 'int32', 'intc', 'l', 'long', 'i8', 'int64', 'int_', 'q', 'longlong', 'p', 'intp', 'int8[pyarrow]', 'int16[pyarrow]', 'int32[pyarrow]', 'int64[pyarrow]', 'UInt8', 'UInt16', 'UInt32', 'UInt64', 'B', 'u1', 'uint8', 'ubyte', 'H', 'u2', 'uint16', 'ushort', 'I', 'u4', 'uint32', 'uintc', 'L', 'ulong', 'u8', 'uint', 'uint64', 'Q', 'ulonglong', 'P', 'uintp', 'uint8[pyarrow]', 'uint16[pyarrow]', 'uint32[pyarrow]', 'uint64[pyarrow]'] | type[int] | Int8Dtype | Int16Dtype | Int32Dtype | Int64Dtype | <14 more items> | None = ..., name: Hashable = ..., copy: bool | None = ...) -> Series[int]
/cogcc_pipeline/ingest.py:152: note:     def [S1: str | bytes | bool | int | float | complex | <6 more items> | type[str] | list[str] | date | time | datetime | timedelta] __new__(cls, data: Sequence[float | floating[Any]] | ndarray[tuple[Any, ...], dtype[floating[Any]]] | FloatingArray, index: Mapping[Any, Any] | ExtensionArray | ndarray[tuple[Any, ...], dtype[Any]] | Index[Any] | Series[Any] | SequenceNotStr[Any] | range | KeysView[Any] | None = ..., dtype: None = ..., name: Hashable = ..., copy: bool | None = ...) -> Series[float]
/cogcc_pipeline/ingest.py:152: note:     def [S1: str | bytes | bool | int | float | complex | <6 more items> | type[str] | list[str] | date | time | datetime | timedelta] __new__(cls, data: Mapping[Any, Any] | ExtensionArray | ndarray[tuple[Any, ...], dtype[Any]] | Index[Any] | Series[Any] | SequenceNotStr[Any] | range | KeysView[Any], index: None = ..., *, dtype: type[float] | Literal['float'] | Literal['Float32', 'Float64'] | Float32Dtype | Float64Dtype | Literal['e', 'f2', '<f2', 'float16', 'half'] | type[floating[_16Bit]] | Literal['f', 'f4', 'float32', 'single', 'd', 'f8', 'float64', 'double', 'g', 'f16', 'float128', 'longdouble'] | type[floating[_32Bit]] | type[float64] | type[floating[_64Bit | _96Bit | _128Bit]] | Literal['float[pyarrow]', 'double[pyarrow]', 'float16[pyarrow]', 'float32[pyarrow]', 'float64[pyarrow]'], name: Hashable = ..., copy: bool | None = ...) -> Series[float]
/cogcc_pipeline/ingest.py:152: note:     def [S1: str | bytes | bool | int | float | complex | <6 more items> | type[str] | list[str] | date | time | datetime | timedelta] __new__(cls, data: Mapping[Any, Any] | ExtensionArray | ndarray[tuple[Any, ...], dtype[Any]] | Index[Any] | Series[Any] | SequenceNotStr[Any] | range | KeysView[Any], index: Mapping[Any, Any] | ExtensionArray | ndarray[tuple[Any, ...], dtype[Any]] | Index[Any] | Series[Any] | SequenceNotStr[Any] | range | KeysView[Any], dtype: type[float] | Literal['float'] | Literal['Float32', 'Float64'] | Float32Dtype | Float64Dtype | Literal['e', 'f2', '<f2', 'float16', 'half'] | type[floating[_16Bit]] | Literal['f', 'f4', 'float32', 'single', 'd', 'f8', 'float64', 'double', 'g', 'f16', 'float128', 'longdouble'] | type[floating[_32Bit]] | type[float64] | type[floating[_64Bit | _96Bit | _128Bit]] | Literal['float[pyarrow]', 'double[pyarrow]', 'float16[pyarrow]', 'float32[pyarrow]', 'float64[pyarrow]'], name: Hashable = ..., copy: bool | None = ...) -> Series[float]
/cogcc_pipeline/ingest.py:152: note:     def [S1: str | bytes | bool | int | float | complex | <6 more items> | type[str] | list[str] | date | time | datetime | timedelta, HashableT1: Hashable] __new__(cls, data: str | bytes | date | datetime | timedelta | <16 more items> | None, index: Mapping[Any, Any] | ExtensionArray | ndarray[tuple[Any, ...], dtype[Any]] | Index[Any] | Series[Any] | SequenceNotStr[Any] | range | KeysView[Any] | None = ..., *, dtype: type[S1], name: Hashable = ..., copy: bool | None = ...) -> Series[S1]
/cogcc_pipeline/ingest.py:152: note:     def [S1: str | bytes | bool | int | float | complex | <6 more items> | type[str] | list[str] | date | time | datetime | timedelta, HashableT1: Hashable] __new__(cls, data: S1 | ExtensionArray | ndarray[tuple[Any, ...], dtype[Any]] | dict[str, ndarray[tuple[Any, ...], dtype[Any]]] | Sequence[S1] | IndexOpsMixin[S1, Any] | dict[HashableT1, S1] | KeysView[S1] | ValuesView[S1], index: Mapping[Any, Any] | ExtensionArray | ndarray[tuple[Any, ...], dtype[Any]] | Index[Any] | Series[Any] | SequenceNotStr[Any] | range | KeysView[Any] | None = ..., dtype: ExtensionDtype | str | dtype[generic[Any]] | type[complex] | type[bool] | type[object] | type[str] | None = ..., name: Hashable = ..., copy: bool | None = ...) -> Series[S1]
/cogcc_pipeline/ingest.py:152: note:     def [S1: str | bytes | bool | int | float | complex | <6 more items> | type[str] | list[str] | date | time | datetime | timedelta, HashableT1: Hashable] __new__(cls, data: str | bytes | date | datetime | timedelta | <19 more items> | None = ..., index: Mapping[Any, Any] | ExtensionArray | ndarray[tuple[Any, ...], dtype[Any]] | Index[Any] | Series[Any] | SequenceNotStr[Any] | range | KeysView[Any] | None = ..., dtype: ExtensionDtype | str | dtype[generic[Any]] | type[complex] | type[bool] | type[object] | type[str] | None = ..., name: Hashable = ..., copy: bool | None = ...) -> Series[Any]
/tests/conftest.py:144: error: Argument "dtype" to "read_csv" has incompatible type "dict[str, Any]"; expected "ExtensionDtype | str | dtype[generic[Any]] | type[complex] | type[bool] | type[object] | type[str] | Mapping[Hashable, ExtensionDtype | str | dtype[generic[Any]] | type[complex] | type[bool] | type[object] | type[str]] | defaultdict[Any, Any] | None"  [arg-type]
Found 3 errors in 2 files (checked 12 source files)

```

- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
...............................................................F.F...... [ 66%]
..........F...........F......F.......                                    [100%]Running teardown with pytest sessionfinish...

=================================== FAILURES ===================================
__________________________ test_read_raw_file_dtypes ___________________________
tests/test_ingest.py:77: in test_read_raw_file_dtypes
    assert df["AcceptedDate"].dtype == "datetime64[ns]"
E   AssertionError: assert dtype('<M8[us]') == 'datetime64[ns]'
E    +  where dtype('<M8[us]') = 0   2021-01-15\n1   2021-01-15\n2   2021-01-15\n3   2021-01-15\n4   2021-01-15\nName: AcceptedDate, dtype: datetime64[us].dtype
___________________ test_read_raw_file_missing_column_raises ___________________
pandas/_libs/lib.pyx:2478: in pandas._libs.lib.maybe_convert_numeric
    ???
E   ValueError: Unable to parse string "x"

During handling of the above exception, another exception occurred:
tests/test_ingest.py:108: in test_read_raw_file_missing_column_raises
    read_raw_file(csv_path, config_fixture)
cogcc_pipeline/ingest.py:108: in read_raw_file
    df = _read("utf-8-sig")
         ^^^^^^^^^^^^^^^^^^
cogcc_pipeline/ingest.py:100: in _read
    return pd.read_csv(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/io/parsers/readers.py:873: in read_csv
    return _read(filepath_or_buffer, kwds)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/io/parsers/readers.py:306: in _read
    return parser.read(nrows)
           ^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/io/parsers/readers.py:1947: in read
    ) = self._engine.read(  # type: ignore[attr-defined]
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/io/parsers/c_parser_wrapper.py:215: in read
    chunks = self._reader.read_low_memory(nrows)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pandas/_libs/parsers.pyx:832: in pandas._libs.parsers.TextReader.read_low_memory
    ???
pandas/_libs/parsers.pyx:911: in pandas._libs.parsers.TextReader._read_rows
    ???
pandas/_libs/parsers.pyx:1009: in pandas._libs.parsers.TextReader._convert_column_data
    ???
pandas/_libs/parsers.pyx:1048: in pandas._libs.parsers.TextReader._convert_tokens
    ???
pandas/_libs/parsers.pyx:1166: in pandas._libs.parsers.TextReader._convert_with_dtype
    ???
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/numeric.py:302: in _from_sequence_of_strings
    scalars = to_numeric(strings, errors="raise", dtype_backend="numpy_nullable")
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/tools/numeric.py:235: in to_numeric
    values, new_mask = lib.maybe_convert_numeric(  # type: ignore[call-overload]
pandas/_libs/lib.pyx:2521: in pandas._libs.lib.maybe_convert_numeric
    ???
E   ValueError: Unable to parse string "x" at position 0
_______________________ test_build_production_date_dtype _______________________
tests/test_transform.py:158: in test_build_production_date_dtype
    assert result.dtype == "datetime64[ns]"
E   AssertionError: assert dtype('<M8[us]') == 'datetime64[ns]'
E    +  where dtype('<M8[us]') = 0   2021-06-01\ndtype: datetime64[us].dtype
______________________ test_cast_well_status_valid_values ______________________
tests/test_transform.py:313: in test_cast_well_status_valid_values
    df = _minimal_df(WellStatus=pd.array(["AC", "SI", "PA"], dtype=pd.StringDtype()))
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
tests/test_transform.py:66: in _minimal_df
    return pd.DataFrame(base)
           ^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:769: in __init__
    mgr = dict_to_mgr(data, index, columns, dtype=dtype, copy=copy)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:460: in dict_to_mgr
    return arrays_to_mgr(arrays, columns, index, dtype=dtype, consolidate=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:113: in arrays_to_mgr
    index = _extract_index(arrays)
            ^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:643: in _extract_index
    raise ValueError("All arrays must be of the same length")
E   ValueError: All arrays must be of the same length
________________ test_transform_partition_production_date_dtype ________________
tests/test_transform.py:386: in test_transform_partition_production_date_dtype
    assert result["production_date"].dtype == "datetime64[ns]"
E   AssertionError: assert dtype('<M8[us]') == 'datetime64[ns]'
E    +  where dtype('<M8[us]') = 0   2021-06-01\nName: production_date, dtype: datetime64[us].dtype
=========================== short test summary info ============================
FAILED tests/test_ingest.py::test_read_raw_file_dtypes - AssertionError: asse...
FAILED tests/test_ingest.py::test_read_raw_file_missing_column_raises - Value...
FAILED tests/test_transform.py::test_build_production_date_dtype - AssertionE...
FAILED tests/test_transform.py::test_cast_well_status_valid_values - ValueErr...
FAILED tests/test_transform.py::test_transform_partition_production_date_dtype
5 failed, 104 passed in 5.51s

```

---

## Eval Run at 2026-04-09 05:52:22

**Status:** ❌ FAILED

### Failures:
- **Type check:**
```
Type check failed. Fix these errors:
/tests/test_transform.py:159: error: Argument 1 to "issubdtype" has incompatible type "dtype[generic[Any]] | ExtensionDtype"; expected "type[Any] | dtype[Any] | _HasDType[dtype[Any]] | _HasNumPyDType[dtype[Any]] | tuple[Any, Any] | list[Any] | _DTypeDict | str | None"  [arg-type]
/tests/test_transform.py:387: error: Argument 1 to "issubdtype" has incompatible type "dtype[generic[Any]] | ExtensionDtype"; expected "type[Any] | dtype[Any] | _HasDType[dtype[Any]] | _HasNumPyDType[dtype[Any]] | tuple[Any, Any] | list[Any] | _DTypeDict | str | None"  [arg-type]
Found 2 errors in 1 file (checked 12 source files)

```

---

## Eval Run at 2026-04-09 05:59:48

**Status:** ✅ PASSED

---
