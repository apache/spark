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

import java.util.{ArrayList => JArrayList, HashMap => JHashMap, Map => JMap}

import org.apache.arrow.memory.{ArrowBuf, ForeignAllocation}
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.message.ArrowFieldNode

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
 * Output path (Python to JVM, zero-copy for values):
 *   Python returns native buffer addresses via `array_to_addresses`. JVM calls
 *   `wrapForeignAllocation` on each address, making Arrow vectors reference Python's
 *   memory directly without copying. The PyArrow array is kept alive in Python's
 *   `_live_arrays` registry until the JVM closes the vectors and calls `_release_export`.
 *   The validity bitmap is zero-copy when null_count > 0; when null_count == 0, the JVM
 *   allocates a small all-valid bitmap (O(n/8) bytes).
 *
 * Phase 1 supported types: LongType, IntegerType, DoubleType, FloatType, BooleanType,
 * ShortType, ByteType. Complex types (StringType, ArrayType, StructType, MapType)
 * are deferred to Phase 2.
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
   * Reconstruct an [[ArrowColumnVector]] from the zero-copy address dict returned by Python.
   *
   * Expected map keys (from `pyspark.inprocess.bridge.array_to_addresses`):
   *   export_id      - Integer or Long (Python _live_arrays registry key)
   *   num_rows       - Integer or Long
   *   null_count     - Integer or Long
   *   validity_addr  - Long (native address of validity bitmap; 0 if no nulls)
   *   validity_size  - Long (size in bytes; 0 if no nulls)
   *   values_addr    - Long (native address of values buffer)
   *   values_size    - Long (size in bytes of values buffer)
   *
   * Zero-copy for values: wraps Python's native Arrow buffer via ForeignAllocation.
   * The caller must call [[InProcessPythonRuntime.releaseExport]] with the returned
   * export ID after closing the returned [[ArrowColumnVector]].
   *
   * @return (ArrowColumnVector wrapping Python's buffers, export ID for release)
   */
  def foreignToColumn(
      resultMap: JMap[String, AnyRef],
      returnType: DataType): (ArrowColumnVector, Int) = {
    val exportId = toInt(resultMap.get("export_id"))
    val numRows = toInt(resultMap.get("num_rows"))
    val nullCount = toInt(resultMap.get("null_count"))
    val valuesAddr = toLong(resultMap.get("values_addr"))
    val valuesSize = toLong(resultMap.get("values_size"))
    val validityAddr = toLong(resultMap.get("validity_addr"))
    val validitySize = toLong(resultMap.get("validity_size"))

    val allocator = ArrowUtils.rootAllocator
      .newChildAllocator("inprocess-udf-result", 0, Long.MaxValue)

    // Zero-copy: wrap Python's values buffer as a ForeignAllocation ArrowBuf.
    // release0() is a no-op -- lifetime is managed via releaseExport(exportId).
    val valuesBuf: ArrowBuf = if (valuesAddr != 0L && valuesSize > 0L) {
      allocator.wrapForeignAllocation(new ForeignAllocation(valuesSize, valuesAddr) {
        override protected def release0(): Unit = ()
      })
    } else {
      allocator.buffer(0)
    }

    // Validity: zero-copy when there are actual nulls; otherwise pass a
    // zero-capacity buffer -- loadValidityBuffer creates an all-valid bitmap.
    val validityBuf: ArrowBuf = if (validityAddr != 0L && validitySize > 0L) {
      allocator.wrapForeignAllocation(new ForeignAllocation(validitySize, validityAddr) {
        override protected def release0(): Unit = ()
      })
    } else {
      allocator.buffer(0)
    }

    val fieldNode = new ArrowFieldNode(numRows, nullCount)
    val bufList = new JArrayList[ArrowBuf](2)
    bufList.add(validityBuf)
    bufList.add(valuesBuf)

    val vector: FieldVector = returnType match {
      case LongType => new BigIntVector("result", allocator)
      case IntegerType => new IntVector("result", allocator)
      case DoubleType => new Float8Vector("result", allocator)
      case FloatType => new Float4Vector("result", allocator)
      case ShortType => new SmallIntVector("result", allocator)
      case ByteType => new TinyIntVector("result", allocator)
      case BooleanType => new BitVector("result", allocator)
      case other =>
        allocator.close()
        throw new UnsupportedOperationException(
          s"Unsupported return type for in-process UDF: $other")
    }

    vector.loadFieldBuffers(fieldNode, bufList)
    (new ArrowColumnVector(vector), exportId)
  }

  private def toInt(v: AnyRef): Int = v match {
    case i: Integer => i.intValue()
    case l: java.lang.Long => l.intValue()
    case _ => v.toString.toInt
  }

  private def toLong(v: AnyRef): Long = v match {
    case l: java.lang.Long => l.longValue()
    case i: Integer => i.longValue()
    case _ => v.toString.toLong
  }
}
