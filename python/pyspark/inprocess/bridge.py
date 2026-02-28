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
Arrow ↔ PyArrow zero-copy bridge for in-process Python UDF execution.

Input path (JVM → Python, zero-copy):
    The JVM passes native memory addresses of Arrow column buffers.
    ``addresses_to_array`` calls ``pa.foreign_buffer(addr, size)`` to wrap that
    memory as a PyArrow Buffer without copying — the same physical bytes are
    accessible from both the JVM (as an ArrowBuf) and Python (as a pa.Buffer).

Output path (Python → JVM, one copy):
    ``array_to_result`` serializes the result PyArrow array's data buffer to
    Python ``bytes``. The JVM copies these bytes into a new Arrow vector.

Phase 1 supported Arrow format strings (fixed-width types):
    "l"  int64   (LongType)
    "i"  int32   (IntegerType)
    "g"  float64 (DoubleType)
    "f"  float32 (FloatType)
    "b"  bool    (BooleanType)
    "s"  int16   (ShortType)
    "c"  int8    (ByteType)
"""

import pyarrow as pa

# Arrow format string → PyArrow type (Phase 1: fixed-width only)
_FORMAT_TO_TYPE = {
    "l": pa.int64(),
    "i": pa.int32(),
    "g": pa.float64(),
    "f": pa.float32(),
    "b": pa.bool_(),
    "s": pa.int16(),
    "c": pa.int8(),
}


def addresses_to_array(addrs: dict) -> pa.Array:
    """
    Reconstruct a ``pa.Array`` from native buffer addresses. Zero-copy.

    ``pa.foreign_buffer(addr, size)`` creates a ``pa.Buffer`` that wraps
    existing native memory at ``addr`` without copying. The JVM's ArrowBuf
    and the Python pa.Buffer point to the same physical bytes.

    Args:
        addrs: dict with keys:
            type_str      (str)  Arrow format string, e.g. "l" for int64
            num_rows      (int)  number of rows
            validity_addr (int)  native address of validity bitmap; 0 = no nulls
            validity_size (int)  size in bytes of validity bitmap
            values_addr   (int)  native address of values buffer
            values_size   (int)  size in bytes of values buffer

    Returns:
        pa.Array wrapping the JVM's native Arrow memory (zero-copy)
    """
    type_str = addrs["type_str"]
    num_rows = int(addrs["num_rows"])
    arrow_type = _FORMAT_TO_TYPE[type_str]

    validity_addr = int(addrs["validity_addr"])
    validity_size = int(addrs["validity_size"])
    validity_buf = (
        pa.foreign_buffer(validity_addr, validity_size)
        if validity_addr != 0
        else None
    )

    values_addr = int(addrs["values_addr"])
    values_size = int(addrs["values_size"])
    values_buf = pa.foreign_buffer(values_addr, values_size)

    # Fixed-width Arrow layout: [validity_bitmap, values]
    return pa.Array.from_buffers(arrow_type, num_rows, [validity_buf, values_buf])


def array_to_result(arr: pa.Array) -> dict:
    """
    Serialize a ``pa.Array`` to a dict of raw bytes for copying back to JVM.

    The dict is converted by jep to a ``java.util.Map<String, Object>`` which
    ``InProcessArrowBridge.resultToColumn`` uses to reconstruct an Arrow vector.

    Args:
        arr: result PyArrow array from the UDF

    Returns:
        dict with keys:
            num_rows       (int)   number of rows
            null_count     (int)   number of null rows
            values_bytes   (bytes) data buffer contents
            validity_bytes (bytes) validity bitmap bytes, or None if no nulls
    """
    bufs = arr.buffers()
    # Only return the bytes that hold actual data (buffers may be over-allocated).
    # Validity bitmap: ceil(num_rows / 8) bytes.
    valid_bytes_len = (len(arr) + 7) // 8
    validity_bytes = bufs[0].to_pybytes()[:valid_bytes_len] if bufs[0] is not None else None
    # Values: ceil(num_rows * bit_width / 8) bytes.
    if len(bufs) > 1 and bufs[1] is not None:
        actual_bytes = (len(arr) * arr.type.bit_width + 7) // 8
        values_bytes = bufs[1].to_pybytes()[:actual_bytes]
    else:
        values_bytes = b""
    return {
        "num_rows": len(arr),
        "null_count": arr.null_count,
        "validity_bytes": validity_bytes,
        "values_bytes": values_bytes,
    }
