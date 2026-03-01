/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.python

import java.util.{HashMap => JHashMap, Map => JMap}

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}

import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ArrowColumnVector

/**
 * Bridges JVM Arrow column buffers with Python PyArrow arrays for in-process UDF execution.
 *
 * Input path (JVM to Python, zero-copy):
 *   Extracts native memory addresses from ArrowColumnVector's ArrowBuf
 *   and passes them as long integers to Python. Python calls `pa.foreign_buffer(addr, size)`
 *   to wrap the same native memory as a PyArrow Buffer -- no memcpy.
 *
 * Output path (Python to JVM, zero-copy via Arrow C Data Interface):
 *   Python exports the result array via the Arrow C Data Interface (CDI):
 *   ``arr._export_to_c(array_ptr, schema_ptr)`` fills ``ArrowArray`` and
 *   ``ArrowSchema`` C structs. The JVM calls ``Data.importVector`` with those
 *   addresses to reconstruct the ``FieldVector`` without copying. When the JVM
 *   closes the vector, Arrow Java invokes the CDI release callback, which
 *   PyArrow uses to release the backing Python array.
 *
 * Phase 1 supported input types: LongType, IntegerType, DoubleType, FloatType,
 * BooleanType, ShortType, ByteType. Output types are determined automatically
 * from the ArrowSchema format string embedded in the CDI structs.
 */
private[python] object InProcessArrowBridge {

  /**
   * Arrow format string for a Spark SQL DataType.
   * Matches the format expected by PyArrow's `pa.lib.ensure_type()`.
   */
  def arrowFormatString(dataType: DataType): String = dataType match {
    case LongType => "l" // int64
    case IntegerType => "i" // int32
    case DoubleType => "g" // float64
    case FloatType => "f" // float32
    case BooleanType => "b" // bool (bit-packed)
    case ShortType => "s" // int16
    case ByteType => "c" // int8
    case other =>
      throw new UnsupportedOperationException(
        s"Phase 1 in-process UDF does not support type: $other. " +
        s"Supported: LongType, IntegerType, DoubleType, FloatType, " +
        s"BooleanType, ShortType, ByteType.")
  }

  /**
   * Extract native buffer addresses from an [[ArrowColumnVector]] and pack them into a
   * Java Map for passing to Python via jep.
   *
   * Returned map keys:
   *   type_str      - Arrow format string (e.g. "l" for int64)
   *   num_rows      - row count (Integer)
   *   validity_addr - native address of validity bitmap (Long); 0 if no nulls
   *   validity_size - size in bytes of validity bitmap (Long); 0 if no nulls
   *   values_addr   - native address of values buffer (Long)
   *   values_size   - size in bytes of values buffer (Long)
   *
   * Zero-copy: no data is copied; only addresses are recorded.
   */
  def extractAddresses(col: ArrowColumnVector, numRows: Int): JMap[String, AnyRef] = {
    val vector = col.getValueVector
    val typeStr = arrowFormatString(col.dataType())

    val dataBuffer = vector.getDataBuffer
    val validityBuffer = vector.getValidityBuffer

    val valuesAddr: Long = if (dataBuffer != null && dataBuffer.capacity() > 0) {
      dataBuffer.memoryAddress()
    } else {
      0L
    }
    val valuesSize: Long = if (dataBuffer != null) dataBuffer.capacity() else 0L

    // Pass validity_addr=0 when there are no nulls; Python skips creating the bitmap
    val (validityAddr, validitySize): (Long, Long) =
      if (vector.getNullCount == 0) {
        (0L, 0L)
      } else {
        val addr = if (validityBuffer != null) validityBuffer.memoryAddress() else 0L
        val size = if (validityBuffer != null) validityBuffer.capacity() else 0L
        (addr, size)
      }

    val map = new JHashMap[String, AnyRef]()
    map.put("type_str", typeStr)
    map.put("num_rows", Integer.valueOf(numRows))
    map.put("validity_addr", java.lang.Long.valueOf(validityAddr))
    map.put("validity_size", java.lang.Long.valueOf(validitySize))
    map.put("values_addr", java.lang.Long.valueOf(valuesAddr))
    map.put("values_size", java.lang.Long.valueOf(valuesSize))
    map
  }

  /**
   * Reconstruct an [[ArrowColumnVector]] from JVM-allocated Arrow C Data Interface structs.
   *
   * The JVM pre-allocates [[ArrowArray]] and [[ArrowSchema]] before invoking Python.
   * Python fills them via ``arr._export_to_c(array_ptr, schema_ptr)``. This method
   * calls [[Data.importVector]] to wrap Python's Arrow buffers (zero-copy).
   *
   * Lifecycle:
   *  - [[Data.importVector]] internally calls ``ArrayImporter.importArray()``, which
   *    copies the struct snapshot, calls ``markReleased()`` + ``close()`` on ``arrowArray``
   *    (idempotent -- caller's try-finally close is safe), and wraps the data buffers via
   *    ``ReferenceCountedArrowArray`` (ForeignAllocation, zero-copy).
   *  - [[ArrowSchema]] is NOT closed by importVector; the caller must close it.
   *  - When the returned [[ArrowColumnVector]] is closed, the reference count drops to
   *    zero, PyArrow's C ``release`` callback is invoked, and the Python array is GC'd.
   */
  def cdiToColumn(arrowArray: ArrowArray, arrowSchema: ArrowSchema): ArrowColumnVector = {
    val vector = Data.importVector(ArrowUtils.rootAllocator, arrowArray, arrowSchema, null)
    new ArrowColumnVector(vector)
  }
}
