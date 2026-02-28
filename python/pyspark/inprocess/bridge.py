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

Output path (Python → JVM, zero-copy for values):
    ``array_to_addresses`` returns the native buffer addresses of the result
    PyArrow array.  The JVM calls ``wrapForeignAllocation`` to wrap those
    addresses as Arrow ``ArrowBuf`` objects without copying.  The PyArrow array
    is kept alive in the module-level ``_live_arrays`` registry until the JVM
    calls ``release_export`` after consuming all rows from the batch.

    Validity bitmap: when ``null_count == 0`` the JVM allocates a small
    all-valid bitmap (O(n/8) bytes) instead of sharing Python's buffer.

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


# Export registry: maps export_id → pa.Array to keep arrays alive while JVM holds
# references to their native buffers.  Entries are removed by release_export().
_live_arrays: dict = {}
_next_export_id: list = [0]  # single-element mutable list used as an int counter


def array_to_addresses(arr: pa.Array) -> dict:
    """
    Export a ``pa.Array`` for zero-copy access by JVM.

    Stores ``arr`` in ``_live_arrays`` so its underlying native buffers remain
    live until the JVM calls ``release_export``.

    Args:
        arr: result PyArrow array from the UDF

    Returns:
        dict with keys (jep converts to ``java.util.Map<String, Object>``):
            export_id      (int)  unique registry key; pass to release_export when done
            num_rows       (int)  number of rows
            null_count     (int)  number of null rows
            validity_addr  (int)  native address of validity bitmap; 0 if no nulls
            validity_size  (int)  size in bytes of validity bitmap; 0 if no nulls
            values_addr    (int)  native address of values buffer (0 for empty arrays)
            values_size    (int)  size in bytes of values buffer
    """
    export_id = _next_export_id[0]
    _next_export_id[0] += 1
    _live_arrays[export_id] = arr

    bufs = arr.buffers()
    validity_buf = bufs[0] if len(bufs) > 0 else None
    values_buf = bufs[1] if len(bufs) > 1 else None

    # Pass non-zero validity address only when there are actual nulls.
    # When null_count == 0, the JVM creates an all-valid bitmap locally (O(n/8)).
    null_count = arr.null_count
    validity_addr = validity_buf.address if (null_count > 0 and validity_buf is not None) else 0
    validity_size = validity_buf.size if (null_count > 0 and validity_buf is not None) else 0
    values_addr = values_buf.address if values_buf is not None else 0
    values_size = values_buf.size if values_buf is not None else 0

    return {
        "export_id": export_id,
        "num_rows": len(arr),
        "null_count": null_count,
        "validity_addr": validity_addr,
        "validity_size": validity_size,
        "values_addr": values_addr,
        "values_size": values_size,
    }


def release_export(export_id: int) -> None:
    """
    Release the exported array for ``export_id``, allowing Python GC.

    Removes the array from ``_live_arrays``.  Must only be called after the
    JVM has closed all Arrow vectors that reference the array's native buffers.

    Args:
        export_id: the id returned by ``array_to_addresses``
    """
    _live_arrays.pop(export_id, None)
