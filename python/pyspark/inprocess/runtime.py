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
from typing import Any, Callable, Iterable, Optional, Sequence

import pyarrow as pa
import pyarrow.compute as pc

from pyspark import cloudpickle
from pyspark.errors import PySparkRuntimeError
from pyspark.sql.pandas.utils import require_minimum_pyarrow_version
from pyspark.util import _format_exception

_UDF_TRACEBACK_SENTINEL = "__INPROCESS_UDF_TRACEBACK__:"
NullChecker = Callable[[pa.Array], None]
_udfs: dict[str, tuple[Callable[..., pa.Array], pa.DataType, NullChecker, bool, bool, bool]] = {}


def _jep_safe_message(message: str) -> str:
    # JEP uses JNI modified UTF-8 for exception text. Keep the transport ASCII and
    # escape NUL explicitly; ordinary UTF-8 and embedded NUL are not safe here.
    return message.encode("ascii", "backslashreplace").decode("ascii").replace("\0", "\\x00")


def _inprocess_register(
    handle: str,
    serialized_udf: Any,
    schema_ptr: int,
    python_version: str,
    hide_traceback: bool = False,
    simplified_traceback: bool = False,
    traceback_with_locals: bool = False,
) -> None:
    try:
        require_minimum_pyarrow_version()
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
        # The JVM is the single source of truth for Arrow layout and logical metadata.
        expected_type = pa.Field._import_from_c(schema_ptr).type
        checker = _null_checker(expected_type) or (lambda array: None)
        _udfs[handle] = (
            func,
            expected_type,
            checker,
            hide_traceback,
            simplified_traceback,
            traceback_with_locals,
        )
    except BaseException as error:
        # In JEP, an uncaught SystemExit can terminate the entire executor JVM.
        raise RuntimeError(
            _UDF_TRACEBACK_SENTINEL
            + _jep_safe_message(
                _format_exception(
                    error, hide_traceback, simplified_traceback, traceback_with_locals
                )
            )
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


# The predicate is deliberately conservative: hidden nulls may request a check, but a
# null-free superset proves that all visible values satisfy the required-field contract.
NullCheckPlan = tuple[Callable[[pa.Array], bool], NullChecker]


def _null_check_plan(expected_type: pa.DataType) -> Optional[NullCheckPlan]:
    def field_plan(field: pa.Field) -> Optional[NullCheckPlan]:
        nested = _null_check_plan(field.type)
        if field.nullable:
            return nested

        def needs_check(values: pa.Array) -> bool:
            return bool(values.null_count) or (nested is not None and nested[0](values))

        def check(values: pa.Array) -> None:
            if values.null_count:
                raise ValueError(
                    f"In-process UDF returned nulls in non-nullable field {field.name}"
                )
            if nested is not None:
                nested[1](values)

        return needs_check, check

    if pa.types.is_struct(expected_type):
        fields = [(i, field_plan(f)) for i, f in enumerate(expected_type)]
        checks = [(i, plan) for i, plan in fields if plan is not None]
        if not checks:
            return None

        def needs_struct(array: pa.Array) -> bool:
            return any(plan[0](array.field(i)) for i, plan in checks)

        def check_struct(array: pa.Array) -> None:
            valid = None
            for i, (needs, check) in checks:
                values = array.field(i)
                if needs(values):
                    if array.null_count:
                        if valid is None:
                            valid = pc.is_valid(array)
                        # Filter only the child requiring a check, not its sibling payloads.
                        values = pc.filter(values, valid)
                    check(values)

        return needs_struct, check_struct
    if pa.types.is_list(expected_type) or pa.types.is_large_list(expected_type):
        plan = field_plan(expected_type.value_field)
        if plan is not None:

            def check_list(array: pa.Array) -> None:
                if plan[0](array.values):
                    plan[1](pc.list_flatten(array))

            return lambda array: plan[0](array.values), check_list
    if pa.types.is_map(expected_type):
        key_plan = _null_check_plan(expected_type.key_type)
        item_plan = field_plan(expected_type.item_field)
        # Arrow validation rejects null keys already; only their descendants need checks.
        checks = [(i, p) for i, p in enumerate((key_plan, item_plan)) if p is not None]
        if not checks:
            return None

        def entries(array: pa.Array) -> pa.Array:
            start = array.offsets[0].as_py()
            length = array.offsets[-1].as_py() - start
            # values.field honors the entries struct's offset; keys/items do not.
            return array.values.slice(start, length)

        def needs_map(array: pa.Array) -> bool:
            values = entries(array)
            return any(plan[0](values.field(i)) for i, plan in checks)

        def check_map(array: pa.Array) -> None:
            if needs_map(array):
                visible = pc.filter(array, pc.is_valid(array)) if array.null_count else array
                values = entries(visible)
                for i, (needs, check) in checks:
                    if needs(values.field(i)):
                        check(values.field(i))

        return needs_map, check_map
    return None


def _null_checker(expected_type: pa.DataType) -> Optional[NullChecker]:
    plan = _null_check_plan(expected_type)
    return plan[1] if plan is not None else None


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
    hide_traceback = simplified_traceback = traceback_with_locals = False
    try:
        (
            udf_func,
            expected_type,
            checker,
            hide_traceback,
            simplified_traceback,
            traceback_with_locals,
        ) = _udfs[handle]
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
    except BaseException as error:
        raise RuntimeError(
            _UDF_TRACEBACK_SENTINEL
            + _jep_safe_message(
                _format_exception(
                    error, hide_traceback, simplified_traceback, traceback_with_locals
                )
            )
        ) from None
