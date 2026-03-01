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
Arrow <-> PyArrow zero-copy bridge for in-process Python UDF execution.

Input path (JVM -> Python, zero-copy):
    The JVM passes native memory addresses of Arrow column buffers.
    ``addresses_to_array`` calls ``pa.foreign_buffer(addr, size)`` to wrap that
    memory as a PyArrow Buffer without copying -- the same physical bytes are
    accessible from both the JVM (as an ArrowBuf) and Python (as a pa.Buffer).

Output path (Python -> JVM, zero-copy via Arrow C Data Interface):
    The JVM pre-allocates ``ArrowArray`` and ``ArrowSchema`` C structs via
    ``ArrowArray.allocateNew(allocator)`` / ``ArrowSchema.allocateNew(allocator)``
    and passes their native addresses to Python.  Python calls
    ``arr._export_to_c(output_array_ptr, output_schema_ptr)`` to fill those
    JVM-owned structs in-place.  The JVM then calls ``Data.importVector`` which:
      - Copies the struct snapshot, calls ``markReleased()`` + ``close()`` on the
        original ArrowArray (so the JVM struct memory is freed), and wraps the
        data buffers via ``ReferenceCountedArrowArray`` (zero-copy ForeignAlloc).
      - When the imported FieldVector is closed, the reference count drops to
        zero, PyArrow's C ``release`` callback is invoked, decrementing the
        Python array refcount and allowing garbage collection.

    No Python-side registry or cleanup is needed: PyArrow's internal reference
    counting, triggered by the CDI ``release`` callback, manages the lifetime of
    the backing array.

Phase 1 supported input Arrow format strings (fixed-width types):
    "l"  int64   (LongType)
    "i"  int32   (IntegerType)
    "g"  float64 (DoubleType)
    "f"  float32 (FloatType)
    "b"  bool    (BooleanType)
    "s"  int16   (ShortType)
    "c"  int8    (ByteType)

Output types are determined automatically from the ArrowSchema format string
written by PyArrow into the JVM-allocated struct during ``_export_to_c``.
"""

import pyarrow as pa

# Arrow format string -> PyArrow type (Phase 1: fixed-width only, used for input path)
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
