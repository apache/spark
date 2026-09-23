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

import pyarrow as pa
import pyarrow.compute as pc

from pyspark import cloudpickle
from pyspark.errors import PySparkRuntimeError
from pyspark.sql.pandas.types import to_arrow_type
from pyspark.sql.types import _parse_datatype_json_string

_UDF_TRACEBACK_SENTINEL = "__INPROCESS_UDF_TRACEBACK__:"
_udfs: dict = {}


def _inprocess_register(handle, serialized_udf, return_type_json, timezone, python_version):
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
        # JEP's PyJArray does not implement the buffer protocol. Convert once per task.
        func = cloudpickle.loads(bytes(b & 0xFF for b in serialized_udf))
        expected_type = to_arrow_type(
            _parse_datatype_json_string(return_type_json),
            timezone=timezone,
            error_on_duplicated_field_names_in_struct=True,
        )
        _udfs[handle] = (func, expected_type)
    except BaseException:
        # In JEP, an uncaught SystemExit can terminate the entire executor JVM.
        raise RuntimeError(_UDF_TRACEBACK_SENTINEL + _traceback.format_exc()) from None


def _inprocess_release(handles):
    for handle in handles:
        _udfs.pop(handle, None)


def _nullable_type(data_type):
    def nullable_field(field):
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


def _check_nested_nulls(array, expected_type):
    def check_field(values, field):
        if not field.nullable and values.null_count:
            raise ValueError(f"In-process UDF returned nulls in non-nullable field {field.name}")
        _check_nested_nulls(values, field.type)

    if pa.types.is_struct(expected_type):
        # Children under a null parent do not contribute values to the result.
        visible = pc.filter(array, pc.is_valid(array))
        for i, field in enumerate(expected_type):
            check_field(visible.field(i), field)
    elif pa.types.is_list(expected_type) or pa.types.is_large_list(expected_type):
        check_field(pc.list_flatten(array), expected_type.value_field)
    elif pa.types.is_map(expected_type):
        visible = pa.concat_arrays([pc.filter(array, pc.is_valid(array))])
        check_field(visible.keys, expected_type.key_field)
        check_field(visible.items, expected_type.item_field)


def _has_offset(array):
    if array.offset:
        return True
    if pa.types.is_struct(array.type):
        return any(_has_offset(array.field(i)) for i in range(array.type.num_fields))
    if pa.types.is_list(array.type) or pa.types.is_large_list(array.type):
        return _has_offset(array.values)
    if pa.types.is_map(array.type):
        return _has_offset(array.keys) or _has_offset(array.items)
    return False


def _validate_result(result, expected_rows: int, expected_type: pa.DataType):
    if not isinstance(result, pa.Array):
        raise TypeError(f"In-process UDF must return a pyarrow.Array, got {type(result).__name__}")
    if len(result) != expected_rows:
        raise ValueError(f"In-process UDF returned {len(result)} rows; expected {expected_rows}")
    if _nullable_type(result.type) != _nullable_type(expected_type):
        raise TypeError(f"In-process UDF returned {result.type}; expected {expected_type}")
    result.validate()
    _check_nested_nulls(result, expected_type)
    if result.type != expected_type:
        result = result.cast(expected_type)
    # Arrow Java's CDI importer does not honor ArrowArray.offset, including child offsets.
    # Concatenation materializes the logical slice, preserving validity and nested values.
    if _has_offset(result):
        result = pa.concat_arrays([result])
    return result


def _inprocess_invoke(
    handle,
    input_array_ptrs,
    input_schema_ptrs,
    output_array_ptr: int,
    output_schema_ptr: int,
    expected_rows: int,
) -> None:
    """Consume input CDI structs and export a validated, row-preserving result.

    The caller owns the struct memory and releases unconsumed exports on failure.
    Each batch owns its buffers; retained Python inputs are never overwritten.
    """
    try:
        udf_func, expected_type = _udfs[handle]
        if len(input_array_ptrs) != len(input_schema_ptrs):
            raise ValueError("Mismatched input ArrowArray and ArrowSchema pointer counts")
        input_arrays = [
            pa.Array._import_from_c(int(ap), int(sp))
            for ap, sp in zip(input_array_ptrs, input_schema_ptrs)
        ]
        result = _validate_result(udf_func(*input_arrays), int(expected_rows), expected_type)
        result._export_to_c(int(output_array_ptr), int(output_schema_ptr))
    except BaseException:
        raise RuntimeError(_UDF_TRACEBACK_SENTINEL + _traceback.format_exc()) from None
