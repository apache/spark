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


"""
In-process Python UDF runtime entry point.

``_inprocess_invoke`` is imported into the jep SharedInterpreter's global namespace
during executor initialization (see ``InProcessPythonRuntime.initialize()``), then called
directly from the JVM via ``interp.invoke("_inprocess_invoke", ...)``.

Both input and output use the Arrow C Data Interface (CDI). The JVM pre-allocates
ArrowArray/ArrowSchema C structs for every input column and for the output, passing
their native addresses as Python ints. Input arrays are reconstructed via
``pa.Array._import_from_c`` (zero-copy). The output is written via ``arr._export_to_c``
into the JVM-owned structs (zero-copy).

jep type conversions (Java -> Python):
    byte[]                -> bytes (or sequence of signed ints; masked to unsigned below)
    List<Long> (boxed)    -> list of Python ints
    Long                  -> int
"""

import traceback as _traceback
from functools import lru_cache

import pyarrow as pa

from pyspark import cloudpickle
from pyspark.sql.pandas.types import to_arrow_type
from pyspark.sql.types import _parse_datatype_json_string

_UDF_TRACEBACK_SENTINEL = "__INPROCESS_UDF_TRACEBACK__:"


@lru_cache(maxsize=128)
def _load_udf(serialized_udf: bytes, return_type_json: str, timezone: str):
    return (
        cloudpickle.loads(serialized_udf),
        to_arrow_type(_parse_datatype_json_string(return_type_json), timezone=timezone),
    )


def _validate_result(result, expected_rows: int, expected_type: pa.DataType) -> None:
    if not isinstance(result, pa.Array):
        raise TypeError(f"In-process UDF must return a pyarrow.Array, got {type(result).__name__}")
    if len(result) != expected_rows:
        raise ValueError(f"In-process UDF returned {len(result)} rows; expected {expected_rows}")
    if result.type != expected_type:
        raise TypeError(f"In-process UDF returned {result.type}; expected {expected_type}")
    result.validate()


def _inprocess_invoke(
    serialized_udf,
    input_array_ptrs,
    input_schema_ptrs,
    output_array_ptr: int,
    output_schema_ptr: int,
    expected_rows: int,
    return_type_json: str,
    timezone: str,
) -> None:
    """Consume input CDI structs and export a validated, row-preserving result.

    The caller owns the struct memory and releases any unconsumed exports on failure.
    Imported input arrays and exported output buffers follow Arrow's release callbacks.
    """
    udf_key = bytes(b & 0xFF for b in serialized_udf)
    udf_func, expected_type = _load_udf(udf_key, return_type_json, timezone)
    if len(input_array_ptrs) != len(input_schema_ptrs):
        raise ValueError("Mismatched input ArrowArray and ArrowSchema pointer counts")
    input_arrays = [
        pa.Array._import_from_c(int(ap), int(sp))
        for ap, sp in zip(input_array_ptrs, input_schema_ptrs)
    ]
    try:
        result = udf_func(*input_arrays)
        _validate_result(result, int(expected_rows), expected_type)
        result._export_to_c(int(output_array_ptr), int(output_schema_ptr))
    except Exception:
        raise RuntimeError(_UDF_TRACEBACK_SENTINEL + _traceback.format_exc()) from None
