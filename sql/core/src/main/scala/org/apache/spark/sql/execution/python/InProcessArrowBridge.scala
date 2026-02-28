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

import org.apache.arrow.vector._

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
 * Output path (Python to JVM, one copy):
 *   Python returns the result array's data as raw bytes. JVM allocates a new Arrow vector and
 *   copies the bytes in. This one copy is acceptable; the expensive direction is input.
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
   * Reconstruct an [[ArrowColumnVector]] from the result dict returned by Python.
   *
   * Expected map keys (from `pyspark.inprocess.bridge.array_to_result`):
   *   num_rows       - Integer or Long
   *   null_count     - Integer or Long
   *   values_bytes   - byte[]  (data buffer contents)
   *   validity_bytes - byte[] or null (validity bitmap; null means all rows non-null)
   *
   * One copy: allocates a new Arrow vector and copies the result bytes into it.
   */
  def resultToColumn(resultMap: JMap[String, AnyRef], returnType: DataType): ArrowColumnVector = {
    val numRows = toInt(resultMap.get("num_rows"))
    val nullCount = toInt(resultMap.get("null_count"))
    val valuesBytes = resultMap.get("values_bytes").asInstanceOf[Array[Byte]]
    val validityBytes = resultMap.get("validity_bytes") match {
      case b: Array[Byte] => b
      case _ => null
    }

    val allocator = ArrowUtils.rootAllocator
      .newChildAllocator("inprocess-udf-result", 0, Long.MaxValue)

    // BooleanType (BitVector) uses bit-packed storage where the data and validity
    // bitmaps are managed differently from fixed-width byte-per-value types.
    // Use setSafe(i, v) which correctly sets both the data bit and validity bit
    // for each row, rather than writing raw bytes to the data buffer directly.
    if (returnType == BooleanType) {
      val bitVec = new BitVector("result", allocator)
      bitVec.allocateNew(numRows)
      for (i <- 0 until numRows) {
        val byteIdx = i >> 3
        val bitIdx = i & 7
        val isNull = nullCount > 0 && (
          validityBytes == null ||
          byteIdx >= validityBytes.length ||
          ((validityBytes(byteIdx).toInt & 0xFF) >> bitIdx & 1) == 0
        )
        if (!isNull) {
          val bitVal =
            if (valuesBytes != null && byteIdx < valuesBytes.length) {
              (valuesBytes(byteIdx).toInt & 0xFF) >> bitIdx & 1
            } else {
              0
            }
          bitVec.setSafe(i, bitVal)
        }
        // null rows: remain null (setSafe not called, validity bit stays 0)
      }
      bitVec.setValueCount(numRows)
      return new ArrowColumnVector(bitVec)
    }

    val vector: FieldVector = returnType match {
      case LongType =>
        val v = new BigIntVector("result", allocator); v.allocateNew(numRows); v
      case IntegerType =>
        val v = new IntVector("result", allocator); v.allocateNew(numRows); v
      case DoubleType =>
        val v = new Float8Vector("result", allocator); v.allocateNew(numRows); v
      case FloatType =>
        val v = new Float4Vector("result", allocator); v.allocateNew(numRows); v
      case ShortType =>
        val v = new SmallIntVector("result", allocator); v.allocateNew(numRows); v
      case ByteType =>
        val v = new TinyIntVector("result", allocator); v.allocateNew(numRows); v
      case other =>
        allocator.close()
        throw new UnsupportedOperationException(
          s"Unsupported return type for in-process UDF: $other")
    }

    // Copy values bytes into the vector's data buffer
    if (valuesBytes != null && valuesBytes.nonEmpty) {
      vector.getDataBuffer.setBytes(0, valuesBytes, 0, valuesBytes.length)
    }

    // Set the validity bitmap.
    // allocateNew() leaves the validity buffer zeroed (all null), so we must
    // explicitly mark rows as valid. When there are no nulls, fill every byte
    // with 0xFF. When there are nulls, copy the per-row bitmap from Python.
    if (nullCount == 0) {
      val validityBuf = vector.getValidityBuffer
      val capBytes = validityBuf.capacity().toInt
      val allValid = Array.fill[Byte](capBytes)(0xFF.toByte)
      validityBuf.setBytes(0, allValid, 0, capBytes)
    } else if (validityBytes != null) {
      vector.getValidityBuffer.setBytes(0, validityBytes, 0, validityBytes.length)
    }

    vector.setValueCount(numRows)
    new ArrowColumnVector(vector)
  }

  private def toInt(v: AnyRef): Int = v match {
    case i: Integer => i.intValue()
    case l: java.lang.Long => l.intValue()
    case _ => v.toString.toInt
  }
}
