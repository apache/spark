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

package org.apache.spark.sql.vectorized

import java.math.{BigDecimal => JavaBigDecimal}
import java.nio.ByteOrder

import scala.util.{Failure, Random, Success, Try, Using}

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex._
import org.apache.arrow.vector.types.pojo.{ArrowType, FieldType}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.unsafe.types.UTF8String

class ArrowColumnVectorSuite extends SparkFunSuite {

  test("boolean") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("boolean", 0, Long.MaxValue)
    val vector = ArrowUtils.toArrowField("boolean", BooleanType, nullable = true, null)
      .createVector(allocator).asInstanceOf[BitVector]
    vector.allocateNew()

    (0 until 10).foreach { i =>
      vector.setSafe(i, if (i % 2 == 0) 1 else 0)
    }
    vector.setNull(10)
    vector.setValueCount(11)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === BooleanType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    (0 until 10).foreach { i =>
      assert(columnVector.getBoolean(i) === (i % 2 == 0))
    }
    assert(columnVector.isNullAt(10))

    assert(columnVector.getBooleans(0, 10) === (0 until 10).map(i => (i % 2 == 0)))

    columnVector.close()
    allocator.close()
  }

  test("byte") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("byte", 0, Long.MaxValue)
    val vector = ArrowUtils.toArrowField("byte", ByteType, nullable = true, null)
      .createVector(allocator).asInstanceOf[TinyIntVector]
    vector.allocateNew()

    (0 until 10).foreach { i =>
      vector.setSafe(i, i.toByte)
    }
    vector.setNull(10)
    vector.setValueCount(11)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === ByteType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    (0 until 10).foreach { i =>
      assert(columnVector.getByte(i) === i.toByte)
    }
    assert(columnVector.isNullAt(10))

    assert(columnVector.getBytes(0, 10) === (0 until 10).map(i => i.toByte))

    columnVector.close()
    allocator.close()
  }

  test("short") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("short", 0, Long.MaxValue)
    val vector = ArrowUtils.toArrowField("short", ShortType, nullable = true, null)
      .createVector(allocator).asInstanceOf[SmallIntVector]
    vector.allocateNew()

    (0 until 10).foreach { i =>
      vector.setSafe(i, i.toShort)
    }
    vector.setNull(10)
    vector.setValueCount(11)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === ShortType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    (0 until 10).foreach { i =>
      assert(columnVector.getShort(i) === i.toShort)
    }
    assert(columnVector.isNullAt(10))

    assert(columnVector.getShorts(0, 10) === (0 until 10).map(i => i.toShort))

    columnVector.close()
    allocator.close()
  }

  test("int") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("int", 0, Long.MaxValue)
    val vector = ArrowUtils.toArrowField("int", IntegerType, nullable = true, null)
      .createVector(allocator).asInstanceOf[IntVector]
    vector.allocateNew()

    (0 until 10).foreach { i =>
      vector.setSafe(i, i)
    }
    vector.setNull(10)
    vector.setValueCount(11)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === IntegerType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    (0 until 10).foreach { i =>
      assert(columnVector.getInt(i) === i)
    }
    assert(columnVector.isNullAt(10))

    assert(columnVector.getInts(0, 10) === (0 until 10))

    columnVector.close()
    allocator.close()
  }

  test("long") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("long", 0, Long.MaxValue)
    val vector = ArrowUtils.toArrowField("long", LongType, nullable = true, null)
      .createVector(allocator).asInstanceOf[BigIntVector]
    vector.allocateNew()

    (0 until 10).foreach { i =>
      vector.setSafe(i, i.toLong)
    }
    vector.setNull(10)
    vector.setValueCount(11)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === LongType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    (0 until 10).foreach { i =>
      assert(columnVector.getLong(i) === i.toLong)
    }
    assert(columnVector.isNullAt(10))

    assert(columnVector.getLongs(0, 10) === (0 until 10).map(i => i.toLong))

    columnVector.close()
    allocator.close()
  }

  test("float") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("float", 0, Long.MaxValue)
    val vector = ArrowUtils.toArrowField("float", FloatType, nullable = true, null)
      .createVector(allocator).asInstanceOf[Float4Vector]
    vector.allocateNew()

    (0 until 10).foreach { i =>
      vector.setSafe(i, i.toFloat)
    }
    vector.setNull(10)
    vector.setValueCount(11)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === FloatType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    (0 until 10).foreach { i =>
      assert(columnVector.getFloat(i) === i.toFloat)
    }
    assert(columnVector.isNullAt(10))

    assert(columnVector.getFloats(0, 10) === (0 until 10).map(i => i.toFloat))

    columnVector.close()
    allocator.close()
  }

  test("double") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("double", 0, Long.MaxValue)
    val vector = ArrowUtils.toArrowField("double", DoubleType, nullable = true, null)
      .createVector(allocator).asInstanceOf[Float8Vector]
    vector.allocateNew()

    (0 until 10).foreach { i =>
      vector.setSafe(i, i.toDouble)
    }
    vector.setNull(10)
    vector.setValueCount(11)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === DoubleType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    (0 until 10).foreach { i =>
      assert(columnVector.getDouble(i) === i.toDouble)
    }
    assert(columnVector.isNullAt(10))

    assert(columnVector.getDoubles(0, 10) === (0 until 10).map(i => i.toDouble))

    columnVector.close()
    allocator.close()
  }

  test("decimal precisions and scales") {
    val random = new Random(1024)
    for (precision <- 1 to Decimal.MAX_LONG_DIGITS) {
      val limit = BigInt(10).pow(precision).toLong
      val boundaries = (1 until precision).flatMap { digit =>
        val power = BigInt(10).pow(digit).toLong
        Seq(power - 1, power, power + 1, 1 - power, -power, -power - 1)
      }
      val values = Seq(0L, 1L, -1L, limit - 1, 1 - limit) ++ boundaries ++
        Seq.fill(32)(random.nextLong() % limit)
      for (scale <- 0 to precision) {
        withDecimalVector(precision, scale) { vector =>
          vector.allocateNew(values.size + 1)
          values.zipWithIndex.foreach { case (value, row) =>
            vector.set(row, JavaBigDecimal.valueOf(value, scale))
          }
          vector.setNull(values.size)
          vector.setValueCount(values.size + 1)
          val column = new ArrowColumnVector(vector)
          assert(column.dataType === DecimalType(precision, scale))
          assert(column.numNulls === 1)
          for (row <- values.indices.reverse) {
            checkDecimal(vector, column, row, precision, scale)
          }
          assert(column.getDecimal(values.size, precision, scale) === null)
        }
      }
    }
  }

  test("decimal native word order") {
    val values = Seq(0L, 12345L, -12345L, 999999999999999999L, -999999999999999999L)
    for (order <- Seq(ByteOrder.LITTLE_ENDIAN, ByteOrder.BIG_ENDIAN)) {
      withDecimalVector(18, 2) { vector =>
        vector.allocateNew(values.size + 1)
        values.zipWithIndex.foreach { case (low, row) =>
          vector.set(row, JavaBigDecimal.valueOf(low, 2))
          val offset = row.toLong * DecimalVector.TYPE_WIDTH
          val high = low >> 63
          // Model native-word positions; ArrowBuf reads each long in native byte order.
          vector.getDataBuffer.setLong(offset, if (order == ByteOrder.LITTLE_ENDIAN) low else high)
          vector.getDataBuffer.setLong(offset + java.lang.Long.BYTES,
            if (order == ByteOrder.LITTLE_ENDIAN) high else low)
        }
        vector.setNull(values.size)
        vector.setValueCount(values.size + 1)
        val accessor = new ArrowColumnVector.SmallDecimalAccessor(vector, order)
        values.indices.foreach { row =>
          assert(accessor.getDecimal(row, 18, 2).toJavaBigDecimal ===
            JavaBigDecimal.valueOf(values(row), 2))
        }
        assert(accessor.getDecimal(values.size, 18, 2) === null)
      }
    }
  }

  test("decimal requested precision and scale") {
    withDecimalVector(5, 2) { vector =>
      vector.allocateNew(4)
      Seq("123.45", "-123.45", "999.95").zipWithIndex.foreach { case (value, row) =>
        vector.set(row, new JavaBigDecimal(value))
      }
      vector.setNull(3)
      vector.setValueCount(4)
      val column = new ArrowColumnVector(vector)
      assert(column.getDecimal(0, 5, 1).toJavaBigDecimal === new JavaBigDecimal("123.5"))
      assert(column.getDecimal(1, 5, 1).toJavaBigDecimal === new JavaBigDecimal("-123.5"))
      checkDecimal(vector, column, 0, 38, 18)
      for (row <- 0 until 3; precision <- Seq(2, 4, 5, 8, 19); scale <- 0 to 3) {
        checkDecimal(vector, column, row, precision, scale)
      }
      assert(column.getDecimal(3, 2, 0) === null)
    }
  }

  test("decimal wide source precision") {
    for (precision <- Seq(19, 20, 38)) {
      withDecimalVector(precision, 2) { vector =>
        val largest = BigInt(10).pow(precision) - 1
        vector.allocateNew(4)
        vector.set(0, new JavaBigDecimal(largest.bigInteger, 2))
        vector.set(1, new JavaBigDecimal((-largest).bigInteger, 2))
        vector.set(2, new JavaBigDecimal("1.23"))
        vector.setNull(3)
        vector.setValueCount(4)
        val column = new ArrowColumnVector(vector)
        for (row <- 0 until 3; targetPrecision <- Seq(18, precision)) {
          checkDecimal(vector, column, row, targetPrecision, 2)
        }
        assert(column.getDecimal(3, precision, 2) === null)
      }
    }
  }

  test("decimal buffer values outside declared precision") {
    withDecimalVector(18, 0) { vector =>
      val values = Seq(
        (BigInt(1) << 64) + 7, -(BigInt(1) << 64) + 7,
        BigInt(10).pow(18), -BigInt(10).pow(18),
        BigInt(Long.MaxValue), BigInt(Long.MinValue),
        BigInt(Long.MaxValue) + 1, BigInt(Long.MinValue) - 1)
      vector.allocateNew(values.size)
      values.zipWithIndex.foreach { case (value, row) =>
        // The byte setter permits values inconsistent with the declared precision.
        vector.setBigEndian(row, value.toByteArray)
      }
      vector.setValueCount(values.size)
      val column = new ArrowColumnVector(vector)
      for (row <- values.indices; precision <- Seq(18, 38)) {
        checkDecimal(vector, column, row, precision, 0)
      }
      vector.setNull(0)
      assert(column.getDecimal(0, 18, 0) === null)
    }
  }

  test("decimal slices and returned values remain independent") {
    withDecimalVector(10, 2) { vector =>
      vector.allocateNew(4)
      vector.set(0, new JavaBigDecimal("999.99"))
      vector.set(1, new JavaBigDecimal("-12.34"))
      vector.setNull(2)
      vector.set(3, new JavaBigDecimal("56.78"))
      vector.setValueCount(4)
      val transfer = vector.getTransferPair(vector.getAllocator)
      transfer.splitAndTransfer(1, 3)
      Using.resource(transfer.getTo.asInstanceOf[DecimalVector]) { slice =>
        val column = new ArrowColumnVector(slice)
        val first = column.getDecimal(0, 10, 2)
        val repeated = column.getDecimal(0, 10, 2)
        val last = column.getDecimal(2, 10, 2)
        assert(first ne repeated)
        assert(first.toJavaBigDecimal === new JavaBigDecimal("-12.34"))
        first.set(0L)
        assert(repeated.toJavaBigDecimal === new JavaBigDecimal("-12.34"))
        assert(last.toJavaBigDecimal === new JavaBigDecimal("56.78"))
        assert(column.getDecimal(1, 10, 2) === null)
      }
    }
  }

  test("decimal negative scale") {
    val conf = new SQLConf
    val key = SQLConf.LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED.key
    SQLConf.withExistingConf(conf) {
      conf.setConfString(key, "true")
      withDecimalVector(5, -2) { vector =>
        vector.allocateNew(1)
        vector.set(0, new JavaBigDecimal("1.23E+4"))
        vector.setValueCount(1)
        val column = new ArrowColumnVector(vector)
        Seq((5, -2), (8, 0), (5, -3)).foreach { case (precision, scale) =>
          checkDecimal(vector, column, 0, precision, scale)
        }
        assert(column.getDecimal(0, 5, -2).toLong === 12300L)
        conf.setConfString(key, "false")
        checkDecimal(vector, column, 0, 5, -2)
      }
    }
  }

  test("decimal checked integral conversions") {
    val values = Seq(
      "127.999999999999999", "-128.999999999999999",
      "32767.9999999999999", "-32768.9999999999999",
      "2147483647.99999999", "-2147483648.99999999", "123456789.999999999")
    values.foreach { value =>
      val input = new JavaBigDecimal(value)
      val precision = input.precision()
      val scale = input.scale()
      withDecimalVector(precision, scale) { vector =>
        vector.allocateNew(1)
        vector.set(0, input)
        vector.setValueCount(1)
        val expected = Decimal(vector.getObject(0), precision, scale)
        val actual = new ArrowColumnVector(vector).getDecimal(0, precision, scale)
        checkDecimalResult(expected.roundToByte(), actual.roundToByte())
        checkDecimalResult(expected.roundToShort(), actual.roundToShort())
        checkDecimalResult(expected.roundToInt(), actual.roundToInt())
        checkDecimalResult(expected.roundToLong(), actual.roundToLong())
      }
    }
  }

  test("string") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("string", 0, Long.MaxValue)
    val vector = ArrowUtils.toArrowField("string", StringType, nullable = true, null)
      .createVector(allocator).asInstanceOf[VarCharVector]
    vector.allocateNew()

    (0 until 10).foreach { i =>
      val utf8 = s"str$i".getBytes("utf8")
      vector.setSafe(i, utf8, 0, utf8.length)
    }
    vector.setNull(10)
    vector.setValueCount(11)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === StringType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    (0 until 10).foreach { i =>
      assert(columnVector.getUTF8String(i) === UTF8String.fromString(s"str$i"))
    }
    assert(columnVector.isNullAt(10))

    columnVector.close()
    allocator.close()
  }

  test("large_string") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("string", 0, Long.MaxValue)
    val vector = ArrowUtils.toArrowField("string", StringType, nullable = true, null, true)
      .createVector(allocator).asInstanceOf[LargeVarCharVector]
    vector.allocateNew()

    (0 until 10).foreach { i =>
      val utf8 = s"str$i".getBytes("utf8")
      vector.setSafe(i, utf8, 0, utf8.length)
    }
    vector.setNull(10)
    vector.setValueCount(11)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === StringType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    (0 until 10).foreach { i =>
      assert(columnVector.getUTF8String(i) === UTF8String.fromString(s"str$i"))
    }
    assert(columnVector.isNullAt(10))

    columnVector.close()
    allocator.close()
  }

  test("binary") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("binary", 0, Long.MaxValue)
    val vector = ArrowUtils.toArrowField("binary", BinaryType, nullable = true, null, false)
      .createVector(allocator).asInstanceOf[VarBinaryVector]
    vector.allocateNew()

    (0 until 10).foreach { i =>
      val utf8 = s"str$i".getBytes("utf8")
      vector.setSafe(i, utf8, 0, utf8.length)
    }
    vector.setNull(10)
    vector.setValueCount(11)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === BinaryType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    (0 until 10).foreach { i =>
      assert(columnVector.getBinary(i) === s"str$i".getBytes("utf8"))
    }
    assert(columnVector.isNullAt(10))

    columnVector.close()
    allocator.close()
  }

  test("large_binary") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("binary", 0, Long.MaxValue)
    val vector = ArrowUtils.toArrowField("binary", BinaryType, nullable = true, null, true)
      .createVector(allocator).asInstanceOf[LargeVarBinaryVector]
    vector.allocateNew()

    (0 until 10).foreach { i =>
      val utf8 = s"str$i".getBytes("utf8")
      vector.setSafe(i, utf8, 0, utf8.length)
    }
    vector.setNull(10)
    vector.setValueCount(11)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === BinaryType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    (0 until 10).foreach { i =>
      assert(columnVector.getBinary(i) === s"str$i".getBytes("utf8"))
    }
    assert(columnVector.isNullAt(10))

    columnVector.close()
    allocator.close()
  }

  test("string_view") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("string_view", 0, Long.MaxValue)
    val vector = new ViewVarCharVector("stringView", allocator)
    vector.allocateNew()

    // Mix short (inline, <= 12 bytes) and long (stored in a data buffer, > 12 bytes) values to
    // exercise both view-storage paths.
    val values = (0 until 10).map { i =>
      if (i % 2 == 0) s"str$i" else s"a-long-string-value-$i"
    }
    values.zipWithIndex.foreach { case (s, i) =>
      val utf8 = s.getBytes("utf8")
      vector.setSafe(i, utf8, 0, utf8.length)
    }
    vector.setNull(10)
    vector.setValueCount(11)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === StringType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    values.zipWithIndex.foreach { case (s, i) =>
      assert(columnVector.getUTF8String(i) === UTF8String.fromString(s))
    }
    assert(columnVector.isNullAt(10))

    columnVector.close()
    allocator.close()
  }

  test("binary_view") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("binary_view", 0, Long.MaxValue)
    val vector = new ViewVarBinaryVector("binaryView", allocator)
    vector.allocateNew()

    // Mix short (inline, <= 12 bytes) and long (stored in a data buffer, > 12 bytes) values to
    // exercise both view-storage paths.
    val values = (0 until 10).map { i =>
      if (i % 2 == 0) s"str$i" else s"a-long-binary-value-$i"
    }
    values.zipWithIndex.foreach { case (s, i) =>
      val utf8 = s.getBytes("utf8")
      vector.setSafe(i, utf8, 0, utf8.length)
    }
    vector.setNull(10)
    vector.setValueCount(11)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === BinaryType)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    values.zipWithIndex.foreach { case (s, i) =>
      assert(columnVector.getBinary(i) === s.getBytes("utf8"))
    }
    assert(columnVector.isNullAt(10))

    columnVector.close()
    allocator.close()
  }

  test("string_view with multiple data buffers") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("string_view", 0, Long.MaxValue)
    val vector = new ViewVarCharVector("stringView", allocator)
    // Keep the variadic data buffers small (16 * 8 = 128 bytes each) so the long values below
    // spill into multiple buffers, exercising the non-zero buffer-index branch of the accessor.
    vector.setInitialCapacity(16, 8)
    vector.allocateNew()

    val values = (0 until 16).map(i => s"a-long-string-value-spilling-over-$i")
    values.zipWithIndex.foreach { case (s, i) =>
      val utf8 = s.getBytes("utf8")
      vector.setSafe(i, utf8, 0, utf8.length)
    }
    vector.setValueCount(16)
    // The values must not fit in a single data buffer, otherwise this test exercises nothing
    // beyond the plain string_view test.
    assert(vector.getDataBuffers.size() > 1)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === StringType)
    values.zipWithIndex.foreach { case (s, i) =>
      assert(columnVector.getUTF8String(i) === UTF8String.fromString(s))
    }

    columnVector.close()
    allocator.close()
  }

  test("array") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("array", 0, Long.MaxValue)
    val vector = ArrowUtils.toArrowField("array", ArrayType(IntegerType), nullable = true, null)
      .createVector(allocator).asInstanceOf[ListVector]
    vector.allocateNew()
    val elementVector = vector.getDataVector().asInstanceOf[IntVector]

    // [1, 2]
    vector.startNewValue(0)
    elementVector.setSafe(0, 1)
    elementVector.setSafe(1, 2)
    vector.endValue(0, 2)

    // [3, null, 5]
    vector.startNewValue(1)
    elementVector.setSafe(2, 3)
    elementVector.setNull(3)
    elementVector.setSafe(4, 5)
    vector.endValue(1, 3)

    // null

    // []
    vector.startNewValue(3)
    vector.endValue(3, 0)

    elementVector.setValueCount(5)
    vector.setValueCount(4)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === ArrayType(IntegerType))
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    val array0 = columnVector.getArray(0)
    assert(array0.numElements() === 2)
    assert(array0.getInt(0) === 1)
    assert(array0.getInt(1) === 2)

    val array1 = columnVector.getArray(1)
    assert(array1.numElements() === 3)
    assert(array1.getInt(0) === 3)
    assert(array1.isNullAt(1))
    assert(array1.getInt(2) === 5)

    assert(columnVector.isNullAt(2))

    val array3 = columnVector.getArray(3)
    assert(array3.numElements() === 0)

    columnVector.close()
    allocator.close()
  }

  test("array_view") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("array_view", 0, Long.MaxValue)
    val vector = ListViewVector.empty("arrayView", allocator)
    vector.addOrGetVector(FieldType.nullable(new ArrowType.Int(8 * 4, true)))
    vector.allocateNew()
    val elementVector = vector.getDataVector().asInstanceOf[IntVector]

    // [1, 2]
    vector.startNewValue(0)
    elementVector.setSafe(0, 1)
    elementVector.setSafe(1, 2)
    vector.endValue(0, 2)

    // [3, null, 5]
    vector.startNewValue(1)
    elementVector.setSafe(2, 3)
    elementVector.setNull(3)
    elementVector.setSafe(4, 5)
    vector.endValue(1, 3)

    // null

    // []
    vector.startNewValue(3)
    vector.endValue(3, 0)

    elementVector.setValueCount(5)
    vector.setValueCount(4)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === ArrayType(IntegerType))
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    val array0 = columnVector.getArray(0)
    assert(array0.numElements() === 2)
    assert(array0.getInt(0) === 1)
    assert(array0.getInt(1) === 2)

    val array1 = columnVector.getArray(1)
    assert(array1.numElements() === 3)
    assert(array1.getInt(0) === 3)
    assert(array1.isNullAt(1))
    assert(array1.getInt(2) === 5)

    assert(columnVector.isNullAt(2))

    val array3 = columnVector.getArray(3)
    assert(array3.numElements() === 0)

    columnVector.close()
    allocator.close()
  }

  test("non nullable struct") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("struct", 0, Long.MaxValue)
    val schema = new StructType().add("int", IntegerType).add("long", LongType)
    val vector = ArrowUtils.toArrowField("struct", schema, nullable = false, null)
      .createVector(allocator).asInstanceOf[StructVector]

    vector.allocateNew()
    val intVector = vector.getChildByOrdinal(0).asInstanceOf[IntVector]
    val longVector = vector.getChildByOrdinal(1).asInstanceOf[BigIntVector]

    vector.setIndexDefined(0)
    intVector.setSafe(0, 1)
    longVector.setSafe(0, 1L)

    vector.setIndexDefined(1)
    intVector.setSafe(1, 2)
    longVector.setNull(1)

    vector.setValueCount(2)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === schema)
    assert(!columnVector.hasNull)
    assert(columnVector.numNulls === 0)

    val row0 = columnVector.getStruct(0)
    assert(row0.getInt(0) === 1)
    assert(row0.getLong(1) === 1L)

    val row1 = columnVector.getStruct(1)
    assert(row1.getInt(0) === 2)
    assert(row1.isNullAt(1))

    columnVector.close()
    allocator.close()
  }

  test("struct") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("struct", 0, Long.MaxValue)
    val schema = new StructType().add("int", IntegerType).add("long", LongType)
    val vector = ArrowUtils.toArrowField("struct", schema, nullable = true, null)
      .createVector(allocator).asInstanceOf[StructVector]
    vector.allocateNew()
    val intVector = vector.getChildByOrdinal(0).asInstanceOf[IntVector]
    val longVector = vector.getChildByOrdinal(1).asInstanceOf[BigIntVector]

    // (1, 1L)
    vector.setIndexDefined(0)
    intVector.setSafe(0, 1)
    longVector.setSafe(0, 1L)

    // (2, null)
    vector.setIndexDefined(1)
    intVector.setSafe(1, 2)
    longVector.setNull(1)

    // (null, 3L)
    vector.setIndexDefined(2)
    intVector.setNull(2)
    longVector.setSafe(2, 3L)

    // null
    vector.setNull(3)

    // (5, 5L)
    vector.setIndexDefined(4)
    intVector.setSafe(4, 5)
    longVector.setSafe(4, 5L)

    intVector.setValueCount(5)
    longVector.setValueCount(5)
    vector.setValueCount(5)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === schema)
    assert(columnVector.hasNull)
    assert(columnVector.numNulls === 1)

    val row0 = columnVector.getStruct(0)
    assert(row0.getInt(0) === 1)
    assert(row0.getLong(1) === 1L)

    val row1 = columnVector.getStruct(1)
    assert(row1.getInt(0) === 2)
    assert(row1.isNullAt(1))

    val row2 = columnVector.getStruct(2)
    assert(row2.isNullAt(0))
    assert(row2.getLong(1) === 3L)

    assert(columnVector.isNullAt(3))

    val row4 = columnVector.getStruct(4)
    assert(row4.getInt(0) === 5)
    assert(row4.getLong(1) === 5L)

    columnVector.close()
    allocator.close()
  }

  test ("SPARK-38086: subclassing") {
    class ChildArrowColumnVector(vector: ValueVector, n: Int)
      extends ArrowColumnVector(vector: ValueVector) {

      override def getValueVector: ValueVector = accessor.vector
      override def getInt(rowId: Int): Int = accessor.getInt(rowId) + n
    }

    val allocator = ArrowUtils.rootAllocator.newChildAllocator("int", 0, Long.MaxValue)
    val vector = ArrowUtils.toArrowField("int", IntegerType, nullable = true, null)
      .createVector(allocator).asInstanceOf[IntVector]
    vector.allocateNew()

    (0 until 10).foreach { i =>
      vector.setSafe(i, i)
    }

    val columnVector = new ChildArrowColumnVector(vector, 1)
    assert(columnVector.dataType === IntegerType)
    assert(!columnVector.hasNull)

    val intVector = columnVector.getValueVector.asInstanceOf[IntVector]
    (0 until 10).foreach { i =>
      assert(columnVector.getInt(i) === i + 1)
      assert(intVector.get(i) === i)
    }

    columnVector.close()
    allocator.close()
  }

  test("struct with TimestampNTZType") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("struct", 0, Long.MaxValue)
    val schema = new StructType().add("ts", TimestampNTZType)
    val vector = ArrowUtils.toArrowField("struct", schema, nullable = true, null)
      .createVector(allocator).asInstanceOf[StructVector]
    vector.allocateNew()
    val timestampVector = vector.getChildByOrdinal(0).asInstanceOf[TimeStampMicroVector]

    vector.setIndexDefined(0)
    timestampVector.setSafe(0, 1000L)

    timestampVector.setValueCount(1)
    vector.setValueCount(1)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === schema)

    val row0 = columnVector.getStruct(0)
    assert(row0.get(0, TimestampNTZType) === 1000L)

    columnVector.close()
    allocator.close()
  }

  private def withDecimalVector(precision: Int, scale: Int)(f: DecimalVector => Unit): Unit = {
    Using.resource(new RootAllocator()) { allocator =>
      Using.resource(new DecimalVector("decimal", allocator, precision, scale))(f)
    }
  }

  private def checkDecimal(
      vector: DecimalVector,
      column: ArrowColumnVector,
      row: Int,
      precision: Int,
      scale: Int): Unit = {
    def result(decimal: Decimal): (JavaBigDecimal, Int, Int, Long, UnsafeRow) = {
      val writer = new UnsafeRowWriter(1)
      writer.write(0, decimal, precision, scale)
      (decimal.toJavaBigDecimal, decimal.precision, decimal.scale, decimal.toLong, writer.getRow)
    }
    checkDecimalResult(
      result(Decimal(vector.getObject(row), precision, scale)),
      result(column.getDecimal(row, precision, scale)))
  }

  private def checkDecimalResult[T](expected: => T, actual: => T): Unit = {
    Try(expected) match {
      case Success(value) => assert(actual === value)
      case Failure(error) =>
        val actualError = intercept[Exception](actual)
        assert(actualError.getClass === error.getClass)
        assert(actualError.getMessage === error.getMessage)
    }
  }
}
