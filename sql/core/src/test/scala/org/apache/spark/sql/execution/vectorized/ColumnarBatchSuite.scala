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

package org.apache.spark.sql.execution.vectorized

import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import java.nio.ByteOrder

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.{RandomDataGenerator, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.CalendarInterval

class ColumnarBatchSuite extends SparkFunSuite {
  test("Null Apis") {
    (MemoryMode.ON_HEAP :: MemoryMode.OFF_HEAP :: Nil).foreach { memMode => {
      val reference = mutable.ArrayBuffer.empty[Boolean]

      val column = ColumnVector.allocate(1024, IntegerType, memMode)
      var idx = 0
      assert(column.anyNullsSet() == false)

      column.putNotNull(idx)
      reference += false
      idx += 1
      assert(column.anyNullsSet() == false)

      column.putNull(idx)
      reference += true
      idx += 1
      assert(column.anyNullsSet() == true)
      assert(column.numNulls() == 1)

      column.putNulls(idx, 3)
      reference += true
      reference += true
      reference += true
      idx += 3
      assert(column.anyNullsSet() == true)

      column.putNotNulls(idx, 4)
      reference += false
      reference += false
      reference += false
      reference += false
      idx += 4
      assert(column.anyNullsSet() == true)
      assert(column.numNulls() == 4)

      reference.zipWithIndex.foreach { v =>
        assert(v._1 == column.isNullAt(v._2))
        if (memMode == MemoryMode.OFF_HEAP) {
          val addr = column.nullsNativeAddress()
          assert(v._1 == (Platform.getByte(null, addr + v._2) == 1), "index=" + v._2)
        }
      }
      column.close
    }}
  }

  test("Byte Apis") {
    (MemoryMode.ON_HEAP :: MemoryMode.OFF_HEAP :: Nil).foreach { memMode => {
      val reference = mutable.ArrayBuffer.empty[Byte]

      val column = ColumnVector.allocate(1024, ByteType, memMode)
      var idx = 0

      val values = (1 :: 2 :: 3 :: 4 :: 5 :: Nil).map(_.toByte).toArray
      column.putBytes(idx, 2, values, 0)
      reference += 1
      reference += 2
      idx += 2

      column.putBytes(idx, 3, values, 2)
      reference += 3
      reference += 4
      reference += 5
      idx += 3

      column.putByte(idx, 9)
      reference += 9
      idx += 1

      column.putBytes(idx, 3, 4)
      reference += 4
      reference += 4
      reference += 4
      idx += 3

      reference.zipWithIndex.foreach { v =>
        assert(v._1 == column.getByte(v._2), "MemoryMode" + memMode)
        if (memMode == MemoryMode.OFF_HEAP) {
          val addr = column.valuesNativeAddress()
          assert(v._1 == Platform.getByte(null, addr + v._2))
        }
      }
    }}
  }

  test("Int Apis") {
    (MemoryMode.ON_HEAP :: MemoryMode.OFF_HEAP :: Nil).foreach { memMode => {
      val seed = System.currentTimeMillis()
      val random = new Random(seed)
      val reference = mutable.ArrayBuffer.empty[Int]

      val column = ColumnVector.allocate(1024, IntegerType, memMode)
      var idx = 0

      val values = (1 :: 2 :: 3 :: 4 :: 5 :: Nil).toArray
      column.putInts(idx, 2, values, 0)
      reference += 1
      reference += 2
      idx += 2

      column.putInts(idx, 3, values, 2)
      reference += 3
      reference += 4
      reference += 5
      idx += 3

      val littleEndian = new Array[Byte](8)
      littleEndian(0) = 7
      littleEndian(1) = 1
      littleEndian(4) = 6
      littleEndian(6) = 1

      column.putIntsLittleEndian(idx, 1, littleEndian, 4)
      column.putIntsLittleEndian(idx + 1, 1, littleEndian, 0)
      reference += 6 + (1 << 16)
      reference += 7 + (1 << 8)
      idx += 2

      column.putIntsLittleEndian(idx, 2, littleEndian, 0)
      reference += 7 + (1 << 8)
      reference += 6 + (1 << 16)
      idx += 2

      while (idx < column.capacity) {
        val single = random.nextBoolean()
        if (single) {
          val v = random.nextInt()
          column.putInt(idx, v)
          reference += v
          idx += 1
        } else {
          val n = math.min(random.nextInt(column.capacity / 20), column.capacity - idx)
          column.putInts(idx, n, n + 1)
          var i = 0
          while (i < n) {
            reference += (n + 1)
            i += 1
          }
          idx += n
        }
      }

      reference.zipWithIndex.foreach { v =>
        assert(v._1 == column.getInt(v._2), "Seed = " + seed + " Mem Mode=" + memMode)
        if (memMode == MemoryMode.OFF_HEAP) {
          val addr = column.valuesNativeAddress()
          assert(v._1 == Platform.getInt(null, addr + 4 * v._2))
        }
      }
      column.close
    }}
  }

  test("Long Apis") {
    (MemoryMode.ON_HEAP :: MemoryMode.OFF_HEAP :: Nil).foreach { memMode => {
      val seed = System.currentTimeMillis()
      val random = new Random(seed)
      val reference = mutable.ArrayBuffer.empty[Long]

      val column = ColumnVector.allocate(1024, LongType, memMode)
      var idx = 0

      val values = (1L :: 2L :: 3L :: 4L :: 5L :: Nil).toArray
      column.putLongs(idx, 2, values, 0)
      reference += 1
      reference += 2
      idx += 2

      column.putLongs(idx, 3, values, 2)
      reference += 3
      reference += 4
      reference += 5
      idx += 3

      val littleEndian = new Array[Byte](16)
      littleEndian(0) = 7
      littleEndian(1) = 1
      littleEndian(8) = 6
      littleEndian(10) = 1

      column.putLongsLittleEndian(idx, 1, littleEndian, 8)
      column.putLongsLittleEndian(idx + 1, 1, littleEndian, 0)
      reference += 6 + (1 << 16)
      reference += 7 + (1 << 8)
      idx += 2

      column.putLongsLittleEndian(idx, 2, littleEndian, 0)
      reference += 7 + (1 << 8)
      reference += 6 + (1 << 16)
      idx += 2

      while (idx < column.capacity) {
        val single = random.nextBoolean()
        if (single) {
          val v = random.nextLong()
          column.putLong(idx, v)
          reference += v
          idx += 1
        } else {

          val n = math.min(random.nextInt(column.capacity / 20), column.capacity - idx)
          column.putLongs(idx, n, n + 1)
          var i = 0
          while (i < n) {
            reference += (n + 1)
            i += 1
          }
          idx += n
        }
      }


      reference.zipWithIndex.foreach { v =>
        assert(v._1 == column.getLong(v._2), "idx=" + v._2 +
            " Seed = " + seed + " MemMode=" + memMode)
        if (memMode == MemoryMode.OFF_HEAP) {
          val addr = column.valuesNativeAddress()
          assert(v._1 == Platform.getLong(null, addr + 8 * v._2))
        }
      }
    }}
  }

  test("Double APIs") {
    (MemoryMode.ON_HEAP :: MemoryMode.OFF_HEAP :: Nil).foreach { memMode => {
      val seed = System.currentTimeMillis()
      val random = new Random(seed)
      val reference = mutable.ArrayBuffer.empty[Double]

      val column = ColumnVector.allocate(1024, DoubleType, memMode)
      var idx = 0

      val values = (1.0 :: 2.0 :: 3.0 :: 4.0 :: 5.0 :: Nil).toArray
      column.putDoubles(idx, 2, values, 0)
      reference += 1.0
      reference += 2.0
      idx += 2

      column.putDoubles(idx, 3, values, 2)
      reference += 3.0
      reference += 4.0
      reference += 5.0
      idx += 3

      val buffer = new Array[Byte](16)
      Platform.putDouble(buffer, Platform.BYTE_ARRAY_OFFSET, 2.234)
      Platform.putDouble(buffer, Platform.BYTE_ARRAY_OFFSET + 8, 1.123)

      if (ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)) {
        // Ensure array contains Liitle Endian doubles
        var bb = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN)
        Platform.putDouble(buffer, Platform.BYTE_ARRAY_OFFSET, bb.getDouble(0))
        Platform.putDouble(buffer, Platform.BYTE_ARRAY_OFFSET + 8, bb.getDouble(8))
      }

      column.putDoubles(idx, 1, buffer, 8)
      column.putDoubles(idx + 1, 1, buffer, 0)
      reference += 1.123
      reference += 2.234
      idx += 2

      column.putDoubles(idx, 2, buffer, 0)
      reference += 2.234
      reference += 1.123
      idx += 2

      while (idx < column.capacity) {
        val single = random.nextBoolean()
        if (single) {
          val v = random.nextDouble()
          column.putDouble(idx, v)
          reference += v
          idx += 1
        } else {
          val n = math.min(random.nextInt(column.capacity / 20), column.capacity - idx)
          val v = random.nextDouble()
          column.putDoubles(idx, n, v)
          var i = 0
          while (i < n) {
            reference += v
            i += 1
          }
          idx += n
        }
      }

      reference.zipWithIndex.foreach { v =>
        assert(v._1 == column.getDouble(v._2), "Seed = " + seed + " MemMode=" + memMode)
        if (memMode == MemoryMode.OFF_HEAP) {
          val addr = column.valuesNativeAddress()
          assert(v._1 == Platform.getDouble(null, addr + 8 * v._2))
        }
      }
      column.close
    }}
  }

  test("String APIs") {
    (MemoryMode.ON_HEAP :: MemoryMode.OFF_HEAP :: Nil).foreach { memMode => {
      val reference = mutable.ArrayBuffer.empty[String]

      val column = ColumnVector.allocate(6, BinaryType, memMode)
      assert(column.arrayData().elementsAppended == 0)
      var idx = 0

      val values = ("Hello" :: "abc" :: Nil).toArray
      column.putByteArray(idx, values(0).getBytes(StandardCharsets.UTF_8),
        0, values(0).getBytes(StandardCharsets.UTF_8).length)
      reference += values(0)
      idx += 1
      assert(column.arrayData().elementsAppended == 5)

      column.putByteArray(idx, values(1).getBytes(StandardCharsets.UTF_8),
        0, values(1).getBytes(StandardCharsets.UTF_8).length)
      reference += values(1)
      idx += 1
      assert(column.arrayData().elementsAppended == 8)

      // Just put llo
      val offset = column.putByteArray(idx, values(0).getBytes(StandardCharsets.UTF_8),
        2, values(0).getBytes(StandardCharsets.UTF_8).length - 2)
      reference += "llo"
      idx += 1
      assert(column.arrayData().elementsAppended == 11)

      // Put the same "ll" at offset. This should not allocate more memory in the column.
      column.putArray(idx, offset, 2)
      reference += "ll"
      idx += 1
      assert(column.arrayData().elementsAppended == 11)

      // Put a long string
      val s = "abcdefghijklmnopqrstuvwxyz"
      column.putByteArray(idx, (s + s).getBytes(StandardCharsets.UTF_8))
      reference += (s + s)
      idx += 1
      assert(column.arrayData().elementsAppended == 11 + (s + s).length)

      reference.zipWithIndex.foreach { v =>
        assert(v._1.length == column.getArrayLength(v._2), "MemoryMode=" + memMode)
        assert(v._1 == column.getUTF8String(v._2).toString,
          "MemoryMode" + memMode)
      }

      column.reset()
      assert(column.arrayData().elementsAppended == 0)
    }}
  }

  test("Int Array") {
    (MemoryMode.ON_HEAP :: MemoryMode.OFF_HEAP :: Nil).foreach { memMode => {
      val column = ColumnVector.allocate(10, new ArrayType(IntegerType, true), memMode)

      // Fill the underlying data with all the arrays back to back.
      val data = column.arrayData();
      var i = 0
      while (i < 6) {
        data.putInt(i, i)
        i += 1
      }

      // Populate it with arrays [0], [1, 2], [], [3, 4, 5]
      column.putArray(0, 0, 1)
      column.putArray(1, 1, 2)
      column.putArray(2, 2, 0)
      column.putArray(3, 3, 3)

      val a1 = ColumnVectorUtils.toPrimitiveJavaArray(column.getArray(0)).asInstanceOf[Array[Int]]
      val a2 = ColumnVectorUtils.toPrimitiveJavaArray(column.getArray(1)).asInstanceOf[Array[Int]]
      val a3 = ColumnVectorUtils.toPrimitiveJavaArray(column.getArray(2)).asInstanceOf[Array[Int]]
      val a4 = ColumnVectorUtils.toPrimitiveJavaArray(column.getArray(3)).asInstanceOf[Array[Int]]
      assert(a1 === Array(0))
      assert(a2 === Array(1, 2))
      assert(a3 === Array.empty[Int])
      assert(a4 === Array(3, 4, 5))

      // Verify the ArrayData APIs
      assert(column.getArray(0).length == 1)
      assert(column.getArray(0).getInt(0) == 0)

      assert(column.getArray(1).length == 2)
      assert(column.getArray(1).getInt(0) == 1)
      assert(column.getArray(1).getInt(1) == 2)

      assert(column.getArray(2).length == 0)

      assert(column.getArray(3).length == 3)
      assert(column.getArray(3).getInt(0) == 3)
      assert(column.getArray(3).getInt(1) == 4)
      assert(column.getArray(3).getInt(2) == 5)

      // Add a longer array which requires resizing
      column.reset
      val array = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
      assert(data.capacity == 10)
      data.reserve(array.length)
      assert(data.capacity == array.length * 2)
      data.putInts(0, array.length, array, 0)
      column.putArray(0, 0, array.length)
      assert(ColumnVectorUtils.toPrimitiveJavaArray(column.getArray(0)).asInstanceOf[Array[Int]]
        === array)
    }}
  }

  test("Struct Column") {
    (MemoryMode.ON_HEAP :: MemoryMode.OFF_HEAP :: Nil).foreach { memMode => {
      val schema = new StructType().add("int", IntegerType).add("double", DoubleType)
      val column = ColumnVector.allocate(1024, schema, memMode)

      val c1 = column.getChildColumn(0)
      val c2 = column.getChildColumn(1)
      assert(c1.dataType() == IntegerType)
      assert(c2.dataType() == DoubleType)

      c1.putInt(0, 123)
      c2.putDouble(0, 3.45)
      c1.putInt(1, 456)
      c2.putDouble(1, 5.67)

      val s = column.getStruct(0)
      assert(s.columns()(0).getInt(0) == 123)
      assert(s.columns()(0).getInt(1) == 456)
      assert(s.columns()(1).getDouble(0) == 3.45)
      assert(s.columns()(1).getDouble(1) == 5.67)

      assert(s.getInt(0) == 123)
      assert(s.getDouble(1) == 3.45)

      val s2 = column.getStruct(1)
      assert(s2.getInt(0) == 456)
      assert(s2.getDouble(1) == 5.67)
    }}
  }

  test("ColumnarBatch basic") {
    (MemoryMode.ON_HEAP :: MemoryMode.OFF_HEAP :: Nil).foreach { memMode => {
      val schema = new StructType()
        .add("intCol", IntegerType)
        .add("doubleCol", DoubleType)
        .add("intCol2", IntegerType)
        .add("string", BinaryType)

      val batch = ColumnarBatch.allocate(schema, memMode)
      assert(batch.numCols() == 4)
      assert(batch.numRows() == 0)
      assert(batch.numValidRows() == 0)
      assert(batch.capacity() > 0)
      assert(batch.rowIterator().hasNext == false)

      // Add a row [1, 1.1, NULL]
      batch.column(0).putInt(0, 1)
      batch.column(1).putDouble(0, 1.1)
      batch.column(2).putNull(0)
      batch.column(3).putByteArray(0, "Hello".getBytes(StandardCharsets.UTF_8))
      batch.setNumRows(1)

      // Verify the results of the row.
      assert(batch.numCols() == 4)
      assert(batch.numRows() == 1)
      assert(batch.numValidRows() == 1)
      assert(batch.rowIterator().hasNext == true)
      assert(batch.rowIterator().hasNext == true)

      assert(batch.column(0).getInt(0) == 1)
      assert(batch.column(0).isNullAt(0) == false)
      assert(batch.column(1).getDouble(0) == 1.1)
      assert(batch.column(1).isNullAt(0) == false)
      assert(batch.column(2).isNullAt(0) == true)
      assert(batch.column(3).getUTF8String(0).toString == "Hello")

      // Verify the iterator works correctly.
      val it = batch.rowIterator()
      assert(it.hasNext())
      val row = it.next()
      assert(row.getInt(0) == 1)
      assert(row.isNullAt(0) == false)
      assert(row.getDouble(1) == 1.1)
      assert(row.isNullAt(1) == false)
      assert(row.isNullAt(2) == true)
      assert(batch.column(3).getUTF8String(0).toString == "Hello")
      assert(it.hasNext == false)
      assert(it.hasNext == false)

      // Filter out the row.
      row.markFiltered()
      assert(batch.numRows() == 1)
      assert(batch.numValidRows() == 0)
      assert(batch.rowIterator().hasNext == false)

      // Reset and add 3 rows
      batch.reset()
      assert(batch.numRows() == 0)
      assert(batch.numValidRows() == 0)
      assert(batch.rowIterator().hasNext == false)

      // Add rows [NULL, 2.2, 2, "abc"], [3, NULL, 3, ""], [4, 4.4, 4, "world]
      batch.column(0).putNull(0)
      batch.column(1).putDouble(0, 2.2)
      batch.column(2).putInt(0, 2)
      batch.column(3).putByteArray(0, "abc".getBytes(StandardCharsets.UTF_8))

      batch.column(0).putInt(1, 3)
      batch.column(1).putNull(1)
      batch.column(2).putInt(1, 3)
      batch.column(3).putByteArray(1, "".getBytes(StandardCharsets.UTF_8))

      batch.column(0).putInt(2, 4)
      batch.column(1).putDouble(2, 4.4)
      batch.column(2).putInt(2, 4)
      batch.column(3).putByteArray(2, "world".getBytes(StandardCharsets.UTF_8))
      batch.setNumRows(3)

      def rowEquals(x: InternalRow, y: Row): Unit = {
        assert(x.isNullAt(0) == y.isNullAt(0))
        if (!x.isNullAt(0)) assert(x.getInt(0) == y.getInt(0))

        assert(x.isNullAt(1) == y.isNullAt(1))
        if (!x.isNullAt(1)) assert(x.getDouble(1) == y.getDouble(1))

        assert(x.isNullAt(2) == y.isNullAt(2))
        if (!x.isNullAt(2)) assert(x.getInt(2) == y.getInt(2))

        assert(x.isNullAt(3) == y.isNullAt(3))
        if (!x.isNullAt(3)) assert(x.getString(3) == y.getString(3))
      }

      // Verify
      assert(batch.numRows() == 3)
      assert(batch.numValidRows() == 3)
      val it2 = batch.rowIterator()
      rowEquals(it2.next(), Row(null, 2.2, 2, "abc"))
      rowEquals(it2.next(), Row(3, null, 3, ""))
      rowEquals(it2.next(), Row(4, 4.4, 4, "world"))
      assert(!it.hasNext)

      // Filter out some rows and verify
      batch.markFiltered(1)
      assert(batch.numValidRows() == 2)
      val it3 = batch.rowIterator()
      rowEquals(it3.next(), Row(null, 2.2, 2, "abc"))
      rowEquals(it3.next(), Row(4, 4.4, 4, "world"))
      assert(!it.hasNext)

      batch.markFiltered(2)
      assert(batch.numValidRows() == 1)
      val it4 = batch.rowIterator()
      rowEquals(it4.next(), Row(null, 2.2, 2, "abc"))

      batch.close
    }}
  }

  private def doubleEquals(d1: Double, d2: Double): Boolean = {
    if (d1.isNaN && d2.isNaN) {
      true
    } else {
      d1 == d2
    }
  }

  private def compareStruct(fields: Seq[StructField], r1: InternalRow, r2: Row, seed: Long) {
    fields.zipWithIndex.foreach { case (field: StructField, ordinal: Int) =>
      assert(r1.isNullAt(ordinal) == r2.isNullAt(ordinal), "Seed = " + seed)
      if (!r1.isNullAt(ordinal)) {
        field.dataType match {
          case BooleanType => assert(r1.getBoolean(ordinal) == r2.getBoolean(ordinal),
            "Seed = " + seed)
          case ByteType => assert(r1.getByte(ordinal) == r2.getByte(ordinal), "Seed = " + seed)
          case ShortType => assert(r1.getShort(ordinal) == r2.getShort(ordinal), "Seed = " + seed)
          case IntegerType => assert(r1.getInt(ordinal) == r2.getInt(ordinal), "Seed = " + seed)
          case LongType => assert(r1.getLong(ordinal) == r2.getLong(ordinal), "Seed = " + seed)
          case FloatType => assert(doubleEquals(r1.getFloat(ordinal), r2.getFloat(ordinal)),
            "Seed = " + seed)
          case DoubleType => assert(doubleEquals(r1.getDouble(ordinal), r2.getDouble(ordinal)),
            "Seed = " + seed)
          case t: DecimalType =>
            val d1 = r1.getDecimal(ordinal, t.precision, t.scale).toBigDecimal
            val d2 = r2.getDecimal(ordinal)
            assert(d1.compare(d2) == 0, "Seed = " + seed)
          case StringType =>
            assert(r1.getString(ordinal) == r2.getString(ordinal), "Seed = " + seed)
          case CalendarIntervalType =>
            assert(r1.getInterval(ordinal) === r2.get(ordinal).asInstanceOf[CalendarInterval])
          case ArrayType(childType, n) =>
            val a1 = r1.getArray(ordinal).array
            val a2 = r2.getList(ordinal).toArray
            assert(a1.length == a2.length, "Seed = " + seed)
            childType match {
              case DoubleType =>
                var i = 0
                while (i < a1.length) {
                  assert(doubleEquals(a1(i).asInstanceOf[Double], a2(i).asInstanceOf[Double]),
                    "Seed = " + seed)
                  i += 1
                }
              case FloatType =>
                var i = 0
                while (i < a1.length) {
                  assert(doubleEquals(a1(i).asInstanceOf[Float], a2(i).asInstanceOf[Float]),
                    "Seed = " + seed)
                  i += 1
                }
              case t: DecimalType =>
                var i = 0
                while (i < a1.length) {
                  assert((a1(i) == null) == (a2(i) == null), "Seed = " + seed)
                  if (a1(i) != null) {
                    val d1 = a1(i).asInstanceOf[Decimal].toBigDecimal
                    val d2 = a2(i).asInstanceOf[java.math.BigDecimal]
                    assert(d1.compare(d2) == 0, "Seed = " + seed)
                  }
                  i += 1
                }
              case _ => assert(a1 === a2, "Seed = " + seed)
            }
          case StructType(childFields) =>
            compareStruct(childFields, r1.getStruct(ordinal, fields.length),
              r2.getStruct(ordinal), seed)
          case _ =>
            throw new NotImplementedError("Not implemented " + field.dataType)
        }
      }
    }
  }

  test("Convert rows") {
    (MemoryMode.ON_HEAP :: MemoryMode.OFF_HEAP :: Nil).foreach { memMode => {
      val rows = Row(1, 2L, "a", 1.2, 'b'.toByte) :: Row(4, 5L, "cd", 2.3, 'a'.toByte) :: Nil
      val schema = new StructType()
        .add("i1", IntegerType)
        .add("l2", LongType)
        .add("string", StringType)
        .add("d", DoubleType)
        .add("b", ByteType)

      val batch = ColumnVectorUtils.toBatch(schema, memMode, rows.iterator.asJava)
      assert(batch.numRows() == 2)
      assert(batch.numCols() == 5)

      val it = batch.rowIterator()
      val referenceIt = rows.iterator
      while (it.hasNext) {
        compareStruct(schema, it.next(), referenceIt.next(), 0)
      }
      batch.close()
    }
  }}

  /**
   * This test generates a random schema data, serializes it to column batches and verifies the
   * results.
   */
  def testRandomRows(flatSchema: Boolean, numFields: Int) {
    // TODO: Figure out why StringType doesn't work on jenkins.
    val types = Array(
      BooleanType, ByteType, FloatType, DoubleType, IntegerType, LongType, ShortType,
      DecimalType.ShortDecimal, DecimalType.IntDecimal, DecimalType.ByteDecimal,
      DecimalType.FloatDecimal, DecimalType.LongDecimal, new DecimalType(5, 2),
      new DecimalType(12, 2), new DecimalType(30, 10), CalendarIntervalType)
    val seed = System.nanoTime()
    val NUM_ROWS = 200
    val NUM_ITERS = 1000
    val random = new Random(seed)
    var i = 0
    while (i < NUM_ITERS) {
      val schema = if (flatSchema) {
        RandomDataGenerator.randomSchema(random, numFields, types)
      } else {
        RandomDataGenerator.randomNestedSchema(random, numFields, types)
      }
      val rows = mutable.ArrayBuffer.empty[Row]
      var j = 0
      while (j < NUM_ROWS) {
        val row = RandomDataGenerator.randomRow(random, schema)
        rows += row
        j += 1
      }
      (MemoryMode.ON_HEAP :: MemoryMode.OFF_HEAP :: Nil).foreach { memMode => {
        val batch = ColumnVectorUtils.toBatch(schema, memMode, rows.iterator.asJava)
        assert(batch.numRows() == NUM_ROWS)

        val it = batch.rowIterator()
        val referenceIt = rows.iterator
        var k = 0
        while (it.hasNext) {
          compareStruct(schema, it.next(), referenceIt.next(), seed)
          k += 1
        }
        batch.close()
      }}
      i += 1
    }
  }

  test("Random flat schema") {
    testRandomRows(true, 15)
  }

  test("Random nested schema") {
    testRandomRows(false, 30)
  }

  test("null filtered columns") {
    val NUM_ROWS = 10
    val schema = new StructType()
      .add("key", IntegerType, nullable = false)
      .add("value", StringType, nullable = true)
    for (numNulls <- List(0, NUM_ROWS / 2, NUM_ROWS)) {
      val rows = mutable.ArrayBuffer.empty[Row]
      for (i <- 0 until NUM_ROWS) {
        val row = if (i < numNulls) Row.fromSeq(Seq(i, null)) else Row.fromSeq(Seq(i, i.toString))
        rows += row
      }
      (MemoryMode.ON_HEAP :: MemoryMode.OFF_HEAP :: Nil).foreach { memMode => {
        val batch = ColumnVectorUtils.toBatch(schema, memMode, rows.iterator.asJava)
        batch.filterNullsInColumn(1)
        batch.setNumRows(NUM_ROWS)
        assert(batch.numRows() == NUM_ROWS)
        val it = batch.rowIterator()
        // Top numNulls rows should be filtered
        var k = numNulls
        while (it.hasNext) {
          assert(it.next().getInt(0) == k)
          k += 1
        }
        assert(k == NUM_ROWS)
        batch.close()
      }}
    }
  }

  test("mutable ColumnarBatch rows") {
    val NUM_ITERS = 10
    val types = Array(
      BooleanType, FloatType, DoubleType, IntegerType, LongType, ShortType,
      DecimalType.ShortDecimal, DecimalType.IntDecimal, DecimalType.ByteDecimal,
      DecimalType.FloatDecimal, DecimalType.LongDecimal, new DecimalType(5, 2),
      new DecimalType(12, 2), new DecimalType(30, 10))
    for (i <- 0 to NUM_ITERS) {
      val random = new Random(System.nanoTime())
      val schema = RandomDataGenerator.randomSchema(random, numFields = 20, types)
      val oldRow = RandomDataGenerator.randomRow(random, schema)
      val newRow = RandomDataGenerator.randomRow(random, schema)

      (MemoryMode.ON_HEAP :: MemoryMode.OFF_HEAP :: Nil).foreach { memMode =>
        val batch = ColumnVectorUtils.toBatch(schema, memMode, (oldRow :: Nil).iterator.asJava)
        val columnarBatchRow = batch.getRow(0)
        newRow.toSeq.zipWithIndex.foreach(i => columnarBatchRow.update(i._2, i._1))
        compareStruct(schema, columnarBatchRow, newRow, 0)
        batch.close()
      }
    }
  }
}
