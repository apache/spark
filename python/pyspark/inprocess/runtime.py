#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


"""Arrow CDI entry points called on the executor's dedicated JEP interpreter thread.

Functions are registered once per task and released when that task finishes. Calls
pass only a handle and CDI addresses, so large closures are not copied per batch.
"""

import sys
import traceback as _traceback
from typing import Any, Callable, Iterable, Optional, Sequence

import pyarrow as pa
import pyarrow.compute as pc

from pyspark import cloudpickle
from pyspark.errors import PySparkRuntimeError
from pyspark.sql.pandas.types import to_arrow_type
from pyspark.sql.types import _parse_datatype_json_string
from pyspark.util import try_simplify_traceback

_UDF_TRACEBACK_SENTINEL = "__INPROCESS_UDF_TRACEBACK__:"
NullChecker = Callable[[pa.Array], None]
_udfs: dict[str, tuple[Callable[..., pa.Array], pa.DataType, NullChecker, bool, bool]] = {}


def _format_exception(hide: bool, simplified: bool) -> str:
    kind, error, tb = sys.exc_info()
    if hide:
        return "".join(_traceback.format_exception_only(kind, error))
    if simplified and tb is not None:
        simple_tb = try_simplify_traceback(tb)
        if simple_tb is not None:
            tb = simple_tb
            if error is not None:
                error.__cause__ = None
    return "".join(_traceback.format_exception(kind, error, tb))


def _inprocess_register(
    handle: str,
    serialized_udf: Any,
    return_type_json: str,
    timezone: str,
    python_version: str,
    large_var_types: bool = False,
    hide_traceback: bool = False,
    simplified_traceback: bool = False,
) -> None:
    try:
        embedded_version = "%d.%d" % sys.version_info[:2]
        if python_version != embedded_version:
            raise PySparkRuntimeError(
                errorClass="PYTHON_VERSION_MISMATCH",
                messageParameters={
                    "worker_version": embedded_version,
                    "driver_version": python_version,
                },
            )
        # JEP exposes direct ByteBuffers through the buffer protocol. Unpickle a separate
        # function per task without iterating over a PyJArray one JNI call per byte.
        func = cloudpickle.loads(memoryview(serialized_udf))
        expected_type = to_arrow_type(
            _parse_datatype_json_string(return_type_json),
            timezone=timezone,
            prefers_large_types=large_var_types,
            error_on_duplicated_field_names_in_struct=True,
        )
        checker = _null_checker(expected_type) or (lambda array: None)
        _udfs[handle] = (func, expected_type, checker, hide_traceback, simplified_traceback)
    except BaseException:
        # In JEP, an uncaught SystemExit can terminate the entire executor JVM.
        raise RuntimeError(
            _UDF_TRACEBACK_SENTINEL + _format_exception(hide_traceback, simplified_traceback)
        ) from None


def _inprocess_release(handles: Iterable[str]) -> None:
    for handle in handles:
        _udfs.pop(handle, None)


def _nullable_type(data_type: pa.DataType) -> pa.DataType:
    def nullable_field(field: pa.Field) -> pa.Field:
        return pa.field(field.name, _nullable_type(field.type), nullable=True)

    if pa.types.is_struct(data_type):
        return pa.struct([nullable_field(field) for field in data_type])
    if pa.types.is_list(data_type):
        return pa.list_(nullable_field(data_type.value_field))
    if pa.types.is_large_list(data_type):
        return pa.large_list(nullable_field(data_type.value_field))
    if pa.types.is_map(data_type):
        return pa.map_(
            _nullable_type(data_type.key_type),
            nullable_field(data_type.item_field),
            keys_sorted=data_type.keys_sorted,
        )
    return data_type


def _null_checker(expected_type: pa.DataType) -> Optional[NullChecker]:
    """Compile checks only for required fields and their ancestors, once per registration."""

    def field_checker(field: pa.Field) -> Optional[NullChecker]:
        nested = _null_checker(field.type)
        if field.nullable:
            return nested

        def check(values: pa.Array) -> None:
            if values.null_count:
                raise ValueError(
                    f"In-process UDF returned nulls in non-nullable field {field.name}"
                )
            if nested is not None:
                nested(values)

        return check

    if pa.types.is_struct(expected_type):
        fields = [(i, field_checker(f)) for i, f in enumerate(expected_type)]
        checks = [(i, check) for i, check in fields if check is not None]
        if not checks:
            return None

        def check_struct(array: pa.Array) -> None:
            # Only children of valid parents are logically visible.
            visible = pc.filter(array, pc.is_valid(array)) if array.null_count else array
            for i, check in checks:
                check(visible.field(i))

        return check_struct
    if pa.types.is_list(expected_type) or pa.types.is_large_list(expected_type):
        check = field_checker(expected_type.value_field)
        if check is not None:
            return lambda array: check(pc.list_flatten(array))
    if pa.types.is_map(expected_type):
        key_check = field_checker(expected_type.key_field)
        item_check = field_checker(expected_type.item_field)

        def check_map(array: pa.Array) -> None:
            visible = pc.filter(array, pc.is_valid(array)) if array.null_count else array
            start = visible.offsets[0].as_py()
            length = visible.offsets[-1].as_py() - start
            if key_check is not None:
                key_check(visible.keys.slice(start, length))
            if item_check is not None:
                item_check(visible.items.slice(start, length))

        return check_map
    return None


def _has_offset(array: pa.Array) -> bool:
    if array.offset:
        return True
    if pa.types.is_struct(array.type):
        return any(_has_offset(array.field(i)) for i in range(array.type.num_fields))
    if pa.types.is_list(array.type) or pa.types.is_large_list(array.type):
        return _has_offset(array.values)
    if pa.types.is_map(array.type):
        return _has_offset(array.values)
    return False


def _with_schema(array: pa.Array, expected_type: pa.DataType) -> pa.Array:
    # Rebind buffers after validating logical nullability. Arrow cast checks hidden child
    # slots too, rejecting null children underneath null parents. from_buffers preserves
    # those masks and applies the declared names, metadata and nullability without casting.
    children = None
    if pa.types.is_struct(expected_type):
        children = [_with_schema(array.field(i), f.type) for i, f in enumerate(expected_type)]
    elif pa.types.is_list(expected_type) or pa.types.is_large_list(expected_type):
        children = [_with_schema(array.values, expected_type.value_type)]
    elif pa.types.is_map(expected_type):
        entries_type = pa.struct([expected_type.key_field, expected_type.item_field])
        children = [_with_schema(array.values, entries_type)]
    return pa.Array.from_buffers(
        expected_type,
        len(array),
        array.buffers()[: array.type.num_buffers],
        null_count=array.null_count,
        children=children,
    )


def _validate_result(
    result: pa.Array,
    expected_rows: int,
    expected_type: pa.DataType,
    null_checker: Optional[NullChecker] = None,
) -> pa.Array:
    if not isinstance(result, pa.Array):
        raise TypeError(f"In-process UDF must return a pyarrow.Array, got {type(result).__name__}")
    if len(result) != expected_rows:
        raise ValueError(f"In-process UDF returned {len(result)} rows; expected {expected_rows}")
    if _nullable_type(result.type) != _nullable_type(expected_type):
        raise TypeError(f"In-process UDF returned {result.type}; expected {expected_type}")
    result.validate()
    checker = null_checker if null_checker is not None else _null_checker(expected_type)
    if checker is not None:
        checker(result)
    # Arrow Java's CDI importer does not honor ArrowArray.offset, including child offsets.
    # Concatenation materializes the logical slice, preserving validity and nested values.
    if _has_offset(result):
        result = pa.concat_arrays([result])
    return _with_schema(result, expected_type)


def _inprocess_invoke(
    handle: str,
    input_array_ptrs: Sequence[int],
    input_schema_ptrs: Sequence[int],
    output_array_ptr: int,
    output_schema_ptr: int,
    expected_rows: int,
    argument_names: Optional[Sequence[str]] = None,
) -> None:
    """Consume input CDI structs and export a validated, row-preserving result.

    The caller owns the struct memory and releases unconsumed exports on failure.
    Each batch owns its buffers; retained Python inputs are never overwritten.
    """
    hide_traceback = simplified_traceback = False
    try:
        udf_func, expected_type, checker, hide_traceback, simplified_traceback = _udfs[handle]
        if len(input_array_ptrs) != len(input_schema_ptrs):
            raise ValueError("Mismatched input ArrowArray and ArrowSchema pointer counts")
        input_arrays = [
            pa.Array._import_from_c(int(ap), int(sp))
            for ap, sp in zip(input_array_ptrs, input_schema_ptrs)
        ]
        names = argument_names if argument_names is not None else [""] * len(input_arrays)
        if len(names) != len(input_arrays):
            raise ValueError("Mismatched input argument names")
        args = [value for name, value in zip(names, input_arrays) if not name]
        kwargs = {str(name): value for name, value in zip(names, input_arrays) if name}
        result = _validate_result(
            udf_func(*args, **kwargs), int(expected_rows), expected_type, checker
        )
        result._export_to_c(int(output_array_ptr), int(output_schema_ptr))
    except BaseException:
        raise RuntimeError(
            _UDF_TRACEBACK_SENTINEL + _format_exception(hide_traceback, simplified_traceback)
        ) from None
