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

import pyarrow as pa
import cloudpickle

# Sentinel prefix embedded in the RuntimeError message when a UDF raises an exception.
# The JVM side detects this prefix to distinguish UDF logic errors (which carry a full
# Python traceback) from infrastructure errors (interpreter not initialized, CDI failure,
# etc., which propagate as plain JepException messages without this prefix).
_UDF_TRACEBACK_SENTINEL = "__INPROCESS_UDF_TRACEBACK__:"

# UDF deserialization cache: cloudpickle bytes -> callable
# Avoids re-deserializing the same UDF for every batch on this executor.
_udf_cache: dict = {}


def _inprocess_invoke(
    serialized_udf,
    input_array_ptrs,
    input_schema_ptrs,
    output_array_ptr: int,
    output_schema_ptr: int,
) -> None:
    """
    Execute a Python UDF in-process for one Arrow batch.

    Called from JVM via jep. Arguments are automatically type-converted by jep.

    Args:
        serialized_udf:    cloudpickle bytes of the Python function (Java byte[])
        input_array_ptrs:  native addresses of JVM-allocated input ArrowArray C structs
                           (Java List<Long> -> Python list of ints)
        input_schema_ptrs: native addresses of JVM-allocated input ArrowSchema C structs
                           (Java List<Long> -> Python list of ints)
        output_array_ptr:  native address of a JVM-allocated output ArrowArray C struct
                           (Java Long -> Python int)
        output_schema_ptr: native address of a JVM-allocated output ArrowSchema C struct
                           (Java Long -> Python int)

    Returns:
        None -- the result is written directly into the JVM-owned ArrowArray/ArrowSchema
        structs via ``arr._export_to_c``. No Python-side lifecycle management needed:
        input arrays are freed when this function returns (CPython refcount drops to 0);
        output lifecycle is managed by Arrow Java's CDI release callback.
    """
    # jep converts Java byte[] to a sequence of signed Java integers (-128..127).
    # Mask each byte to unsigned (0..255) before constructing Python bytes.
    udf_key = bytes(b & 0xFF for b in serialized_udf)

    # Deserialize UDF once; cache for subsequent batches on this executor
    if udf_key not in _udf_cache:
        _udf_cache[udf_key] = cloudpickle.loads(udf_key)
    udf_func = _udf_cache[udf_key]

    # Reconstruct PyArrow arrays from CDI struct addresses (zero-copy).
    # _import_from_c takes ownership of the CDI structs; when these local variables
    # go out of scope at function return, CPython immediately decrements their refcounts
    # and the CDI release callbacks decrement the buffer references on the JVM side.
    input_arrays = [
        pa.Array._import_from_c(int(ap), int(sp))
        for ap, sp in zip(input_array_ptrs, input_schema_ptrs)
    ]

    # Execute UDF and export result.
    # Any exception from user code (TypeError, ValueError, etc.) or from a bad return
    # value (wrong pa.Array type causing _export_to_c to fail) is caught here and
    # re-raised with the full Python traceback embedded in the message.  Infrastructure
    # errors above this block (CDI import failure, cloudpickle deserialization failure)
    # propagate as-is so the Scala side can tell them apart.
    try:
        if len(input_arrays) == 1:
            result = udf_func(input_arrays[0])
        else:
            result = udf_func(*input_arrays)
    except Exception:
        raise RuntimeError(
            _UDF_TRACEBACK_SENTINEL + _traceback.format_exc()
        ) from None

    # Export result into the JVM-pre-allocated ArrowArray/ArrowSchema C structs.
    # PyArrow fills the structs in-place and registers its own CDI release callback.
    # The JVM calls Data.importVector to wrap the buffers (zero-copy). When the
    # imported FieldVector is closed, Arrow Java invokes PyArrow's release callback,
    # which decrements the Python array refcount -- no Python-side registry needed.
    result._export_to_c(int(output_array_ptr), int(output_schema_ptr))
