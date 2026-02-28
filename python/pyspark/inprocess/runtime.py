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

jep type conversions (Java → Python):
    byte[]           → bytes
    List<Map>        → list[dict]   (Java Long values inside are Python int)
    Integer          → int

jep type conversions (Python → Java):
    dict             → java.util.LinkedHashMap<String, Object>
    int              → java.lang.Integer (small) or java.lang.Long (large)
    bytes            → byte[]
    None             → null
"""

import cloudpickle

from pyspark.inprocess.bridge import addresses_to_array, array_to_addresses, release_export

# UDF deserialization cache: cloudpickle bytes → callable
# Avoids re-deserializing the same UDF for every batch on this executor.
_udf_cache: dict = {}


def _inprocess_invoke(serialized_udf, input_addrs, num_rows: int) -> dict:
    """
    Execute a Python UDF in-process for one Arrow batch.

    Called from JVM via jep. Arguments are automatically type-converted by jep.

    Args:
        serialized_udf:  cloudpickle bytes of the Python function (Java byte[])
        input_addrs:     list of address dicts, one per input column
                         (Java List<Map<String,Object>>)
        num_rows:        number of rows in the batch (Java Integer)

    Returns:
        dict of native buffer addresses — jep converts to Java Map<String, Object>.
        See ``InProcessArrowBridge.foreignToColumn`` for key spec.
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

    # Execute UDF — receives pa.Array per input column, returns pa.Array
    if len(input_arrays) == 1:
        result = udf_func(input_arrays[0])
    else:
        result = udf_func(*input_arrays)

    # Export result as native buffer addresses for zero-copy transfer to JVM.
    # The array is kept alive in bridge._live_arrays until the JVM calls _release_export.
    return array_to_addresses(result)


def _release_export(export_id: int) -> None:
    """
    Release the Python-side array export identified by ``export_id``.

    Called from JVM via jep after all rows from a batch have been consumed and
    the JVM has closed the Arrow vectors that referenced the array's buffers.
    """
    release_export(export_id)
