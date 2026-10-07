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

import re
import sys
from typing import Any, Callable, Iterable, NamedTuple, Optional, Sequence

import pyarrow as pa
import pyarrow.compute as pc

from pyspark import cloudpickle
from pyspark.errors import PySparkRuntimeError
from pyspark.util import _format_exception

_UDF_TRACEBACK_SENTINEL = "__INPROCESS_UDF_TRACEBACK__:"
NullChecker = Callable[[pa.Array], None]


class _Registration(NamedTuple):
    func: Callable[..., pa.Array]
    expected_type: pa.DataType
    # The expected type as _nullable_type normalizes it, to compare result types with.
    expected_key: pa.DataType
    checker: NullChecker
    hide_traceback: bool
    simplified_traceback: bool
    traceback_with_locals: bool
    full_validation: bool


_udfs: dict[str, _Registration] = {}
# Pin exported buffers until the task has released its CDI references. This keeps Python
# finalizers on the interpreter thread, including for NumPy-backed results.
_results: dict[str, pa.Array] = {}


def _jep_safe_message(message: str) -> str:
    # JNI modified UTF-8 agrees with UTF-8 for BMP characters except NUL/surrogates.
    return re.sub(
        r"[\x00\ud800-\udfff\U00010000-\U0010ffff]",
        lambda match: match.group().encode("unicode_escape").decode("ascii"),
        message,
    )


def _inprocess_register(
    handle: str,
    serialized_udf: Any,
    schema_ptr: int,
    python_version: str,
    hide_traceback: bool = False,
    simplified_traceback: bool = False,
    traceback_with_locals: bool = False,
    full_validation: bool = True,
) -> None:
    try:
        # The interpreter's bootstrap has checked the PyArrow version once for its lifetime.
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
        if not callable(func):
            raise TypeError("In-process UDF command must contain a callable; use inprocess_udf")
        # The JVM is the single source of truth for Arrow layout and logical metadata.
        expected_type = pa.Field._import_from_c(schema_ptr).type
        checker = _null_checker(expected_type) or (lambda array: None)
        _udfs[handle] = _Registration(
            func,
            expected_type,
            _nullable_type(expected_type),
            checker,
            hide_traceback,
            simplified_traceback,
            traceback_with_locals,
            full_validation,
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
        _results.pop(handle, None)
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
            keys_sorted=False,
        )
    # These physical representations depend on session settings unavailable to the UDF.
    if pa.types.is_timestamp(data_type) and data_type.tz is not None:
        return pa.timestamp(data_type.unit, tz="UTC")
    if pa.types.is_large_string(data_type):
        return pa.string()
    if pa.types.is_large_binary(data_type):
        return pa.binary()
    return data_type


def _offset_width(data_type: pa.DataType) -> int:
    if (
        pa.types.is_string(data_type)
        or pa.types.is_binary(data_type)
        or pa.types.is_list(data_type)
        or pa.types.is_map(data_type)
    ):
        return 4
    if (
        pa.types.is_large_string(data_type)
        or pa.types.is_large_binary(data_type)
        or pa.types.is_large_list(data_type)
    ):
        return 8
    return 0


def _child_arrays(array: pa.Array) -> list:
    # List and map values ignore the parent's offset; struct fields are sliced to match it.
    data_type = array.type
    if (
        pa.types.is_list(data_type)
        or pa.types.is_large_list(data_type)
        or pa.types.is_fixed_size_list(data_type)
        or pa.types.is_map(data_type)
    ):
        return [array.values]
    if pa.types.is_struct(data_type):
        return [array.field(i) for i in range(data_type.num_fields)]
    if pa.types.is_dictionary(data_type):
        return [array.dictionary]
    return []


def _has_offsets_buffers(array: pa.Array) -> bool:
    width = _offset_width(array.type)
    if width:
        offsets = array.buffers()[1]
        if offsets is None or offsets.size < (array.offset + len(array) + 1) * width:
            return False
    return all(_has_offsets_buffers(child) for child in _child_arrays(array))


def _rebuild(
    array: pa.Array,
    level: Callable[[pa.Array], Optional[pa.Array]],
    nullable_fields: bool = False,
) -> Optional[pa.Array]:
    """Rebuild ``array`` around the levels that ``level`` replaces, or return None if none.

    ``level`` returns a replacement for a level, or None to look at its children instead.
    Ancestors of a replaced level keep their own buffers. With ``nullable_fields``, rebuilt
    levels have nullable fields, and maps become the equivalent lists of entries, so that
    they can hold nulls under null parents whatever the replaced children are.
    """
    replaced = level(array)
    if replaced is not None:
        return replaced
    data_type = array.type
    children = _child_arrays(array)
    rebuilt = [_rebuild(child, level, nullable_fields) for child in children]
    if all(child is None for child in rebuilt):
        return None
    children = [child if new is None else new for child, new in zip(children, rebuilt)]
    if pa.types.is_struct(data_type):
        fields = [f.with_type(c.type) for f, c in zip(data_type, children)]
        if nullable_fields:
            fields = [f.with_nullable(True) for f in fields]
        mask = array.is_null() if array.null_count else None
        return pa.StructArray.from_arrays(children, fields=fields, mask=mask)
    if pa.types.is_dictionary(data_type):
        return pa.DictionaryArray.from_arrays(array.indices, children[0])
    if nullable_fields:
        child = pa.field("item", children[0].type)
        if pa.types.is_fixed_size_list(data_type):
            data_type = pa.list_(child, data_type.list_size)
        elif pa.types.is_large_list(data_type):
            data_type = pa.large_list(child)
        else:
            data_type = pa.list_(child)
    return pa.Array.from_buffers(
        data_type,
        len(array),
        array.buffers()[: data_type.num_buffers],
        null_count=array.null_count,
        offset=array.offset,
        children=children,
    )


def _repair_offsets(array: pa.Array) -> Optional[pa.Array]:
    """Return a copy whose zero-length levels have offsets buffers, or None if unchanged.

    Arrow permits a zero-length variable-width, list or map array without an offsets buffer,
    or with a zero-size one, e.g. from PyArrow's IPC reader. Concatenation can crash on it,
    and Arrow Java reads past it. Validation already rejects such buffers at other lengths.
    """

    def level(array: pa.Array) -> Optional[pa.Array]:
        if len(array) == 0 and not _has_offsets_buffers(array):
            return pa.array([], type=array.type)
        return None

    return _rebuild(array, level)


def _canonical_type(data_type: pa.DataType) -> pa.DataType:
    # Representations that Arrow casts to the type Spark declares without changing values,
    # as the worker's schema enforcement does. Other differences must be cast explicitly.
    if pa.types.is_dictionary(data_type):
        return _canonical_type(data_type.value_type)
    if pa.types.is_string_view(data_type):
        return pa.string()
    if pa.types.is_binary_view(data_type) or pa.types.is_fixed_size_binary(data_type):
        return pa.binary()
    if pa.types.is_struct(data_type):
        return pa.struct([f.with_type(_canonical_type(f.type)) for f in data_type])
    if (
        pa.types.is_list(data_type)
        or pa.types.is_large_list(data_type)
        or pa.types.is_fixed_size_list(data_type)
    ):
        field = data_type.value_field
        return pa.list_(field.with_type(_canonical_type(field.type)))
    if pa.types.is_map(data_type):
        field = data_type.item_field
        return pa.map_(
            _canonical_type(data_type.key_type),
            field.with_type(_canonical_type(field.type)),
            keys_sorted=data_type.keys_sorted,
        )
    return data_type


def _nullable_fields(data_type: pa.DataType) -> pa.DataType:
    # A cast target that keeps the declared types, but cannot reject hidden null children.
    if pa.types.is_struct(data_type):
        return pa.struct(
            [f.with_type(_nullable_fields(f.type)).with_nullable(True) for f in data_type]
        )
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        field = data_type.value_field
        child = field.with_type(_nullable_fields(field.type)).with_nullable(True)
        return pa.list_(child) if pa.types.is_list(data_type) else pa.large_list(child)
    if pa.types.is_map(data_type):
        field = data_type.item_field
        return pa.map_(
            _nullable_fields(data_type.key_type),
            field.with_type(_nullable_fields(field.type)).with_nullable(True),
            keys_sorted=data_type.keys_sorted,
        )
    return data_type


def _strings_as_binary(array: pa.Array) -> Optional[pa.Array]:
    """Rebind each string level as binary over the same buffers, or return None if none.

    Full validation then checks every offset, but not UTF-8: Spark strings may hold invalid
    UTF-8, which workers accept too. Unlike ``Array.view`` of the whole array, the rebound
    levels are nullable, so null children under null parents of non-nullable fields pass, as
    Spark writes them, and each level keeps its own length. ``Array.validate`` already
    rejects null map keys.
    """

    binary_types = {
        pa.string(): pa.binary(),
        pa.large_string(): pa.large_binary(),
        pa.string_view(): pa.binary_view(),
    }

    def level(array: pa.Array) -> Optional[pa.Array]:
        # A leaf has no fields, so viewing it keeps its buffers, length and offset.
        binary = binary_types.get(array.type)
        return None if binary is None else array.view(binary)

    return _rebuild(array, level, nullable_fields=True)


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
            if len(array) == 0:
                return array.values.slice(0, 0)
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
    # Dictionaries, fixed-size lists and views are cast away before this is called.
    return bool(array.offset) or any(_has_offset(c) for c in _child_arrays(array))


def _with_schema(array: pa.Array, expected_type: pa.DataType) -> pa.Array:
    # Rebind buffers after validating logical nullability. Arrow cast checks hidden child
    # slots too, rejecting null children underneath null parents. from_buffers preserves
    # those masks and applies the declared names, metadata and nullability without casting.
    if array.type != expected_type and (
        pa.types.is_string(expected_type)
        or pa.types.is_large_string(expected_type)
        or pa.types.is_binary(expected_type)
        or pa.types.is_large_binary(expected_type)
    ):
        return pc.cast(array, expected_type, safe=True)
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
    full_validation: bool = True,
    expected_key: Optional[pa.DataType] = None,
) -> pa.Array:
    if not isinstance(result, pa.Array):
        raise TypeError(f"In-process UDF must return a pyarrow.Array, got {type(result).__name__}")
    if len(result) != expected_rows:
        raise ValueError(f"In-process UDF returned {len(result)} rows; expected {expected_rows}")
    if expected_key is None:
        expected_key = _nullable_type(expected_type)
    convert = _nullable_type(result.type) != expected_key
    if convert and _nullable_type(_canonical_type(result.type)) != expected_key:
        raise TypeError(f"In-process UDF returned {result.type}; expected {expected_type}")
    result.validate()
    if full_validation:
        # Validate every offset before conversion, null checks, normalization or JVM access.
        binary = _strings_as_binary(result)
        (result if binary is None else binary).validate(full=True)
    repaired = _repair_offsets(result)
    if repaired is not None:
        result = repaired
    if convert:
        result = result.cast(_nullable_fields(expected_type))
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
            expected_key,
            checker,
            hide_traceback,
            simplified_traceback,
            traceback_with_locals,
            full_validation,
        ) = _udfs[handle]
        # The task closes the preceding batch's CDI references before invoking again.
        _results.pop(handle, None)
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
        output = udf_func(*args, **kwargs)
        try:
            result = _validate_result(
                output,
                int(expected_rows),
                expected_type,
                checker,
                full_validation,
                expected_key,
            )
        except BaseException:
            # The unvalidated result is a local of this and the validating frame. Capturing
            # locals would call repr() on it, which reads its possibly malformed buffers.
            traceback_with_locals = False
            raise
        _results[handle] = result
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
