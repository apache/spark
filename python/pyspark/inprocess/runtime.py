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

jep type conversions (Java -> Python):
    byte[]           -> bytes
    List<Map>        -> list[dict]   (Java Long values inside are Python int)
    Integer          -> int
    Long             -> int
"""

import cloudpickle

from pyspark.inprocess.bridge import addresses_to_array

# UDF deserialization cache: cloudpickle bytes -> callable
# Avoids re-deserializing the same UDF for every batch on this executor.
_udf_cache: dict = {}


def _inprocess_invoke(
    serialized_udf,
    input_addrs,
    num_rows: int,
    output_array_ptr: int,
    output_schema_ptr: int,
) -> None:
    """
    Execute a Python UDF in-process for one Arrow batch.

    Called from JVM via jep. Arguments are automatically type-converted by jep.

    Args:
        serialized_udf:    cloudpickle bytes of the Python function (Java byte[])
        input_addrs:       list of address dicts, one per input column
                           (Java List<Map<String,Object>>)
        num_rows:          number of rows in the batch (Java Integer)
        output_array_ptr:  native address of a JVM-allocated ArrowArray C struct
                           (Java Long -> Python int)
        output_schema_ptr: native address of a JVM-allocated ArrowSchema C struct
                           (Java Long -> Python int)

    Returns:
        None -- the result is written directly into the JVM-owned ArrowArray and
        ArrowSchema structs via ``arr._export_to_c``. No Python-side cleanup needed:
        lifecycle is managed entirely by Arrow Java and PyArrow's CDI release callback.
    """
    # jep converts Java byte[] to a sequence of signed Java integers (-128..127).
    # Mask each byte to unsigned (0..255) before constructing Python bytes.
    udf_key = bytes(b & 0xFF for b in serialized_udf)

    # Deserialize UDF once; cache for subsequent batches on this executor
    if udf_key not in _udf_cache:
        _udf_cache[udf_key] = cloudpickle.loads(udf_key)
    udf_func = _udf_cache[udf_key]

    # Reconstruct PyArrow arrays from native buffer addresses (zero-copy)
    input_arrays = [addresses_to_array(addrs) for addrs in input_addrs]

    # Execute UDF -- receives pa.Array per input column, returns pa.Array
    if len(input_arrays) == 1:
        result = udf_func(input_arrays[0])
    else:
        result = udf_func(*input_arrays)

    # Export result into the JVM-pre-allocated ArrowArray/ArrowSchema C structs.
    # PyArrow fills the structs in-place and registers its own CDI release callback.
    # The JVM calls Data.importVector to wrap the buffers (zero-copy). When the
    # imported FieldVector is closed, Arrow Java invokes PyArrow's release callback,
    # which decrements the Python array refcount -- no Python-side registry needed.
    result._export_to_c(int(output_array_ptr), int(output_schema_ptr))
