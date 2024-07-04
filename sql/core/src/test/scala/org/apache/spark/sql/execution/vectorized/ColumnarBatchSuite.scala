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

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.time.LocalDateTime
import java.util

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions
import scala.util.Random

import org.apache.arrow.vector.IntVector
import org.apache.parquet.bytes.ByteBufferInputStream

import org.apache.spark.SparkFunSuite
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.{RandomDataGenerator, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapBuilder, DateTimeUtils, GenericArrayData, MapData}
import org.apache.spark.sql.execution.RowToColumnConverter
import org.apache.spark.sql.execution.datasources.parquet.VectorizedPlainValuesReader
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnarBatchRow, ColumnVector}
import org.apache.spark.tags.ExtendedSQLTest
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String, VariantVal}
import org.apache.spark.util.ArrayImplicits._

@ExtendedSQLTest
class ColumnarBatchSuite extends SparkFunSuite {

  private def allocate(capacity: Int, dt: DataType, memMode: MemoryMode): WritableColumnVector = {
    if (memMode == MemoryMode.OFF_HEAP) {
      new OffHeapColumnVector(capacity, dt)
    } else {
      new OnHeapColumnVector(capacity, dt)
    }
  }

  private def testVector(
      name: String,
      size: Int,
      dt: DataType)(
      block: WritableColumnVector => Unit): Unit = {
    test(name) {
      Seq(MemoryMode.ON_HEAP, MemoryMode.OFF_HEAP).foreach { mode =>
        val vector = allocate(size, dt, mode)
        try block(vector) finally {
          vector.close()
        }
      }
    }
  }

  testVector("Null APIs", 1024, IntegerType) {
    column =>
      val reference = mutable.ArrayBuffer.empty[Boolean]
      var idx = 0
      assert(!column.hasNull)
      assert(column.numNulls() == 0)

      column.appendNotNull()
      reference += false
      assert(!column.hasNull)
      assert(column.numNulls() == 0)

      column.appendNotNulls(3)
      (1 to 3).foreach(_ => reference += false)
      assert(!column.hasNull)
      assert(column.numNulls() == 0)

      column.appendNull()
      reference += true
      assert(column.hasNull)
      assert(column.numNulls() == 1)

      column.appendNulls(3)
      (1 to 3).foreach(_ => reference += true)
      assert(column.hasNull)
      assert(column.numNulls() == 4)

      idx = column.elementsAppended

      column.putNotNull(idx)
      reference += false
      idx += 1
      assert(column.hasNull)
      assert(column.numNulls() == 4)

      column.putNull(idx)
      reference += true
      idx += 1
      assert(column.hasNull)
      assert(column.numNulls() == 5)

      column.putNulls(idx, 3)
      reference += true
      reference += true
      reference += true
      idx += 3
      assert(column.hasNull)
      assert(column.numNulls() == 8)

      column.putNotNulls(idx, 4)
      reference += false
      reference += false
      reference += false
      reference += false
      idx += 4
      assert(column.hasNull)
      assert(column.numNulls() == 8)

      reference.zipWithIndex.foreach { v =>
        assert(v._1 == column.isNullAt(v._2))
      }
  }

  testVector("Boolean APIs", 1024, BooleanType) {
    column =>
      val reference = mutable.ArrayBuffer.empty[Boolean]

      var values = Array(true, false, true, false, false)
      var bits = values.foldRight(0)((b, i) => i << 1 | (if (b) 1 else 0)).toByte
      column.appendBooleans(2, bits, 0)
      reference ++= values.slice(0, 2)

      column.appendBooleans(3, bits, 2)
      reference ++= values.slice(2, 5)

      column.appendBooleans(6, true)
      reference ++= Array.fill(6)(true)

      column.appendBoolean(false)
      reference += false

      var idx = column.elementsAppended

      values = Array(true, true, false, true, false, true, false, true)
      bits = values.foldRight(0)((b, i) => i << 1 | (if (b) 1 else 0)).toByte
      column.putBooleans(idx, 2, bits, 0)
      reference ++= values.slice(0, 2)
      idx += 2

      column.putBooleans(idx, 3, bits, 2)
      reference ++= values.slice(2, 5)
      idx += 3

      column.putBooleans(idx, bits)
      reference ++= values
      idx += 8

      column.putBoolean(idx, false)
      reference += false
      idx += 1

      column.putBooleans(idx, 3, true)
      reference ++= Array.fill(3)(true)
      idx += 3

      implicit def intToByte(i: Int): Byte = i.toByte
      val buf = ByteBuffer.wrap(Array(0x33, 0x5A, 0xA5, 0xCC, 0x0F, 0xF0, 0xEE, 0x77, 0x88))
      val reader = new VectorizedPlainValuesReader()
      reader.initFromPage(0, ByteBufferInputStream.wrap(buf))

      reader.skipBooleans(1) // bit index 0

      column.putBoolean(idx, reader.readBoolean) // bit index 1
      reference += true
      idx += 1

      column.putBoolean(idx, reader.readBoolean) // bit index 2
      reference += false
      idx += 1

      reader.skipBooleans(5) // bit index [3, 7]

      column.putBoolean(idx, reader.readBoolean) // bit index 8
      reference += false
      idx += 1

      reader.skipBooleans(8) // bit index [9, 16]
      reader.skipBooleans(0) // no-op

      column.putBoolean(idx, reader.readBoolean) // bit index 17
      reference += false
      idx += 1

      reader.skipBooleans(16) // bit index [18, 33]

      reader.readBooleans(4, column, idx) // bit index [34, 37]
      reference ++= Array(true, true, false, false)
      idx += 4

      reader.readBooleans(11, column, idx) // bit index [38, 48]
      reference ++= Array(false, false, false, false, false, false, true, true, true, true, false)
      idx += 11

      reader.skipBooleans(7) // bit index [49, 55]

      reader.readBooleans(9, column, idx) // bit index [56, 64]
      reference ++= Array(true, true, true, false, true, true, true, false, false)
      idx += 9

      reference.zipWithIndex.foreach { v =>
        assert(v._1 == column.getBoolean(v._2), "VectorType=" + column.getClass.getSimpleName)
      }
  }

  testVector("Byte APIs", 1024, ByteType) {
    column =>
      val reference = mutable.ArrayBuffer.empty[Byte]

      var values = (10 :: 20 :: 30 :: 40 :: 50 :: Nil).map(_.toByte).toArray
      column.appendBytes(2, values, 0)
      reference += 10.toByte
      reference += 20.toByte

      column.appendBytes(3, values, 2)
      reference += 30.toByte
      reference += 40.toByte
      reference += 50.toByte

      column.appendBytes(6, 60.toByte)
      (1 to 6).foreach(_ => reference += 60.toByte)

      column.appendByte(70.toByte)
      reference += 70.toByte

      var idx = column.elementsAppended

      values = (1 :: 2 :: 3 :: 4 :: 5 :: Nil).map(_.toByte).toArray
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
        assert(v._1 == column.getByte(v._2), "VectorType=" + column.getClass.getSimpleName)
      }
  }

  testVector("Short APIs", 1024, ShortType) {
    column =>
      val seed = System.currentTimeMillis()
      val random = new Random(seed)
      val reference = mutable.ArrayBuffer.empty[Short]

      var values = (10 :: 20 :: 30 :: 40 :: 50 :: Nil).map(_.toShort).toArray
      column.appendShorts(2, values, 0)
      reference += 10.toShort
      reference += 20.toShort

      column.appendShorts(3, values, 2)
      reference += 30.toShort
      reference += 40.toShort
      reference += 50.toShort

      column.appendShorts(6, 60.toShort)
      (1 to 6).foreach(_ => reference += 60.toShort)

      column.appendShort(70.toShort)
      reference += 70.toShort

      var idx = column.elementsAppended

      values = (1 :: 2 :: 3 :: 4 :: 5 :: Nil).map(_.toShort).toArray
      column.putShorts(idx, 2, values, 0)
      reference += 1
      reference += 2
      idx += 2

      column.putShorts(idx, 3, values, 2)
      reference += 3
      reference += 4
      reference += 5
      idx += 3

      column.putShort(idx, 9)
      reference += 9
      idx += 1

      column.putShorts(idx, 3, 4)
      reference += 4
      reference += 4
      reference += 4
      idx += 3

      while (idx < column.capacity) {
        val single = random.nextBoolean()
        if (single) {
          val v = random.nextInt().toShort
          column.putShort(idx, v)
          reference += v
          idx += 1
        } else {
          val n = math.min(random.nextInt(column.capacity / 20), column.capacity - idx)
          val v = (n + 1).toShort
          column.putShorts(idx, n, v)
          var i = 0
          while (i < n) {
            reference += v
            i += 1
          }
          idx += n
        }
      }

      reference.zipWithIndex.foreach { v =>
        assert(v._1 == column.getShort(v._2),
          "Seed = " + seed + " VectorType=" + column.getClass.getSimpleName)
      }
  }

  testVector("Int APIs", 1024, IntegerType) {
    column =>
      val seed = System.currentTimeMillis()
      val random = new Random(seed)
      val reference = mutable.ArrayBuffer.empty[Int]

      var values = (10 :: 20 :: 30 :: 40 :: 50 :: Nil).toArray
      column.appendInts(2, values, 0)
      reference += 10
      reference += 20

      column.appendInts(3, values, 2)
      reference += 30
      reference += 40
      reference += 50

      column.appendInts(6, 60)
      (1 to 6).foreach(_ => reference += 60)

      column.appendInt(70)
      reference += 70

      var idx = column.elementsAppended

      values = (1 :: 2 :: 3 :: 4 :: 5 :: Nil).toArray
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
        assert(v._1 == column.getInt(v._2),
          "Seed = " + seed + " VectorType=" + column.getClass.getSimpleName)
      }
  }

  testVector("Long APIs", 1024, LongType) {
    column =>
      val seed = System.currentTimeMillis()
      val random = new Random(seed)
      val reference = mutable.ArrayBuffer.empty[Long]

      var values = (10L :: 20L :: 30L :: 40L :: 50L :: Nil).toArray
      column.appendLongs(2, values, 0)
      reference += 10L
      reference += 20L

      column.appendLongs(3, values, 2)
      reference += 30L
      reference += 40L
      reference += 50L

      column.appendLongs(6, 60L)
      (1 to 6).foreach(_ => reference += 60L)

      column.appendLong(70L)
      reference += 70L

      var idx = column.elementsAppended

      values = (1L :: 2L :: 3L :: 4L :: 5L :: Nil).toArray
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
          " Seed = " + seed + " VectorType=" + column.getClass.getSimpleName)
      }
  }

  testVector("Float APIs", 1024, FloatType) {
    column =>
      val seed = System.currentTimeMillis()
      val random = new Random(seed)
      val reference = mutable.ArrayBuffer.empty[Float]

      var values = (.1f :: .2f :: .3f :: .4f :: .5f :: Nil).toArray
      column.appendFloats(2, values, 0)
      reference += .1f
      reference += .2f

      column.appendFloats(3, values, 2)
      reference += .3f
      reference += .4f
      reference += .5f

      column.appendFloats(6, .6f)
      (1 to 6).foreach(_ => reference += .6f)

      column.appendFloat(.7f)
      reference += .7f

      var idx = column.elementsAppended

      values = (1.0f :: 2.0f :: 3.0f :: 4.0f :: 5.0f :: Nil).toArray
      column.putFloats(idx, 2, values, 0)
      reference += 1.0f
      reference += 2.0f
      idx += 2

      column.putFloats(idx, 3, values, 2)
      reference += 3.0f
      reference += 4.0f
      reference += 5.0f
      idx += 3

      val buffer = new Array[Byte](8)
      Platform.putFloat(buffer, Platform.BYTE_ARRAY_OFFSET, 2.234f)
      Platform.putFloat(buffer, Platform.BYTE_ARRAY_OFFSET + 4, 1.123f)

      column.putFloats(idx, 1, buffer, 4)
      column.putFloats(idx + 1, 1, buffer, 0)
      reference += 1.123f
      reference += 2.234f
      idx += 2

      column.putFloats(idx, 2, buffer, 0)
      reference += 2.234f
      reference += 1.123f
      idx += 2

      while (idx < column.capacity) {
        val single = random.nextBoolean()
        if (single) {
          val v = random.nextFloat()
          column.putFloat(idx, v)
          reference += v
          idx += 1
        } else {
          val n = math.min(random.nextInt(column.capacity / 20), column.capacity - idx)
          val v = random.nextFloat()
          column.putFloats(idx, n, v)
          var i = 0
          while (i < n) {
            reference += v
            i += 1
          }
          idx += n
        }
      }

      reference.zipWithIndex.foreach { v =>
        assert(v._1 == column.getFloat(v._2),
          "Seed = " + seed + " VectorType=" + column.getClass.getSimpleName)
      }
  }

  testVector("[SPARK-31703] Float API - Little Endian", 1024, FloatType) {
    column =>
      val seed = System.currentTimeMillis()
      val random = new Random(seed)
      val reference = mutable.ArrayBuffer.empty[Float]

      var idx = 0

      val littleEndian = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
      littleEndian.putFloat(0, 1.357f)
      littleEndian.putFloat(4, 2.468f)
      val arr = new Array[Byte](littleEndian.remaining)
      littleEndian.get(arr)

      column.putFloatsLittleEndian(idx, 1, arr, 4)
      column.putFloatsLittleEndian(idx + 1, 1, arr, 0)
      reference += 2.468f
      reference += 1.357f
      idx += 2

      column.putFloatsLittleEndian(idx, 2, arr, 0)
      reference += 1.357f
      reference += 2.468f
      idx += 2

      while (idx < column.capacity) {
        val single = random.nextBoolean()
        if (single) {
          val v = random.nextFloat()
          column.putFloat(idx, v)
          reference += v
          idx += 1
        } else {
          val n = math.min(random.nextInt(column.capacity / 20), column.capacity - idx)
          val v = random.nextFloat()
          column.putFloats(idx, n, v)
          var i = 0
          while (i < n) {
            reference += v
            i += 1
          }
          idx += n
        }
      }

      reference.zipWithIndex.foreach { v =>
        assert(v._1 == column.getFloat(v._2),
          "Seed = " + seed + " VectorType=" + column.getClass.getSimpleName)
      }
  }

  testVector("Double APIs", 1024, DoubleType) {
    column =>
      val seed = System.currentTimeMillis()
      val random = new Random(seed)
      val reference = mutable.ArrayBuffer.empty[Double]

      var values = (.1 :: .2 :: .3 :: .4 :: .5 :: Nil).toArray
      column.appendDoubles(2, values, 0)
      reference += .1
      reference += .2

      column.appendDoubles(3, values, 2)
      reference += .3
      reference += .4
      reference += .5

      column.appendDoubles(6, .6)
      (1 to 6).foreach(_ => reference += .6)

      column.appendDouble(.7)
      reference += .7

      var idx = column.elementsAppended

      values = (1.0 :: 2.0 :: 3.0 :: 4.0 :: 5.0 :: Nil).toArray
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
        assert(v._1 == column.getDouble(v._2),
          "Seed = " + seed + " VectorType=" + column.getClass.getSimpleName)
      }
  }

  testVector("[SPARK-31703] Double API - Little Endian", 1024, DoubleType) {
    column =>
      val seed = System.currentTimeMillis()
      val random = new Random(seed)
      val reference = mutable.ArrayBuffer.empty[Double]

      var idx = 0

      val littleEndian = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN)
      littleEndian.putDouble(0, 1.357)
      littleEndian.putDouble(8, 2.468)
      val arr = new Array[Byte](littleEndian.remaining)
      littleEndian.get(arr)

      column.putDoublesLittleEndian(idx, 1, arr, 8)
      column.putDoublesLittleEndian(idx + 1, 1, arr, 0)
      reference += 2.468
      reference += 1.357
      idx += 2

      column.putDoublesLittleEndian(idx, 2, arr, 0)
      reference += 1.357
      reference += 2.468
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
        assert(v._1 == column.getDouble(v._2),
          "Seed = " + seed + " VectorType=" + column.getClass.getSimpleName)
      }
  }

  testVector("String APIs", 7, StringType) {
    column =>
      val reference = mutable.ArrayBuffer.empty[String]

      assert(column.arrayData().elementsAppended == 0)

      val str = "string"
      column.appendByteArray(str.getBytes(StandardCharsets.UTF_8),
        0, str.getBytes(StandardCharsets.UTF_8).length)
      reference += str
      assert(column.arrayData().elementsAppended == 6)

      var idx = column.elementsAppended

      val values = ("Hello" :: "abc" :: Nil).toArray
      column.putByteArray(idx, values(0).getBytes(StandardCharsets.UTF_8),
        0, values(0).getBytes(StandardCharsets.UTF_8).length)
      reference += values(0)
      idx += 1
      assert(column.arrayData().elementsAppended == 11)

      column.putByteArray(idx, values(1).getBytes(StandardCharsets.UTF_8),
        0, values(1).getBytes(StandardCharsets.UTF_8).length)
      reference += values(1)
      idx += 1
      assert(column.arrayData().elementsAppended == 14)

      // Just put llo
      val offset = column.putByteArray(idx, values(0).getBytes(StandardCharsets.UTF_8),
        2, values(0).getBytes(StandardCharsets.UTF_8).length - 2)
      reference += "llo"
      idx += 1
      assert(column.arrayData().elementsAppended == 17)

      // Put the same "ll" at offset. This should not allocate more memory in the column.
      column.putArray(idx, offset, 2)
      reference += "ll"
      idx += 1
      assert(column.arrayData().elementsAppended == 17)

      // Put a long string
      val s = "abcdefghijklmnopqrstuvwxyz"
      column.putByteArray(idx, (s + s).getBytes(StandardCharsets.UTF_8))
      reference += (s + s)
      idx += 1
      assert(column.arrayData().elementsAppended == 17 + (s + s).length)

      column.putNull(idx)
      assert(column.getUTF8String(idx) == null)
      idx += 1

      reference.zipWithIndex.foreach { v =>
        val errMsg = "VectorType=" + column.getClass.getSimpleName
        assert(v._1.length == column.getArrayLength(v._2), errMsg)
        assert(v._1 == column.getUTF8String(v._2).toString, errMsg)
      }

      column.reset()
      assert(column.arrayData().elementsAppended == 0)
  }

  testVector("CalendarInterval APIs", 5, CalendarIntervalType) {
    column =>
      val reference = mutable.ArrayBuffer.empty[CalendarInterval]

      val months = column.getChild(0)
      val days = column.getChild(1)
      val microseconds = column.getChild(2)
      assert(months.dataType() == IntegerType)
      assert(days.dataType() == IntegerType)
      assert(microseconds.dataType() == LongType)

      Seq(new CalendarInterval(1, 10, 100),
        new CalendarInterval(0, 0, 2000),
        new CalendarInterval(20, 0, 0),
        new CalendarInterval(0, 200, 0)).zipWithIndex.foreach { case (v, i) =>
          column.putInterval(i, v)
          reference += v
      }

      reference.zipWithIndex.foreach { case (v, i) =>
        val errMsg = "VectorType=" + column.getClass.getSimpleName
        assert(v == column.getInterval(i), errMsg)
        if (v == null) assert(column.isNullAt(i), errMsg)
      }

      column.close()
  }

  testVector("Int Array", 10, new ArrayType(IntegerType, true)) {
    column =>

      // Fill the underlying data with all the arrays back to back.
      val data = column.arrayData()
      var i = 0
      while (i < 6) {
        data.putInt(i, i)
        i += 1
      }

      // Populate it with arrays [0], [1, 2], null, [], [3, 4, 5]
      column.putArray(0, 0, 1)
      column.putArray(1, 1, 2)
      column.putNull(2)
      column.putArray(3, 3, 0)
      column.putArray(4, 3, 3)

      assert(column.getArray(0).numElements == 1)
      assert(column.getArray(1).numElements == 2)
      assert(column.isNullAt(2))
      assert(column.getArray(2) == null)
      assert(column.getArray(3).numElements == 0)
      assert(column.getArray(4).numElements == 3)

      val a1 = ColumnVectorUtils.toJavaIntArray(column.getArray(0))
      val a2 = ColumnVectorUtils.toJavaIntArray(column.getArray(1))
      val a3 = ColumnVectorUtils.toJavaIntArray(column.getArray(3))
      val a4 = ColumnVectorUtils.toJavaIntArray(column.getArray(4))
      assert(a1 === Array(0))
      assert(a2 === Array(1, 2))
      assert(a3 === Array.empty[Int])
      assert(a4 === Array(3, 4, 5))

      // Verify the ArrayData get APIs
      assert(column.getArray(0).getInt(0) == 0)

      assert(column.getArray(1).getInt(0) == 1)
      assert(column.getArray(1).getInt(1) == 2)

      assert(column.getArray(4).getInt(0) == 3)
      assert(column.getArray(4).getInt(1) == 4)
      assert(column.getArray(4).getInt(2) == 5)

      // Add a longer array which requires resizing
      column.reset()
      val array = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
      assert(data.capacity == 10)
      data.reserve(array.length)
      assert(data.capacity == array.length * 2)
      data.putInts(0, array.length, array, 0)
      column.putArray(0, 0, array.length)
      assert(ColumnVectorUtils.toJavaIntArray(column.getArray(0)) === array)
  }

  test("toArray for primitive types") {
    (MemoryMode.ON_HEAP :: MemoryMode.OFF_HEAP :: Nil).foreach { memMode =>
      val len = 4

      val columnBool = allocate(len, new ArrayType(BooleanType, false), memMode)
      val boolArray = Array(false, true, false, true)
      boolArray.zipWithIndex.foreach { case (v, i) => columnBool.arrayData.putBoolean(i, v) }
      columnBool.putArray(0, 0, len)
      assert(columnBool.getArray(0).toBooleanArray === boolArray)
      columnBool.close()

      val columnByte = allocate(len, new ArrayType(ByteType, false), memMode)
      val byteArray = Array[Byte](0, 1, 2, 3)
      byteArray.zipWithIndex.foreach { case (v, i) => columnByte.arrayData.putByte(i, v) }
      columnByte.putArray(0, 0, len)
      assert(columnByte.getArray(0).toByteArray === byteArray)
      columnByte.close()

      val columnShort = allocate(len, new ArrayType(ShortType, false), memMode)
      val shortArray = Array[Short](0, 1, 2, 3)
      shortArray.zipWithIndex.foreach { case (v, i) => columnShort.arrayData.putShort(i, v) }
      columnShort.putArray(0, 0, len)
      assert(columnShort.getArray(0).toShortArray === shortArray)
      columnShort.close()

      val columnInt = allocate(len, new ArrayType(IntegerType, false), memMode)
      val intArray = Array(0, 1, 2, 3)
      intArray.zipWithIndex.foreach { case (v, i) => columnInt.arrayData.putInt(i, v) }
      columnInt.putArray(0, 0, len)
      assert(columnInt.getArray(0).toIntArray === intArray)
      columnInt.close()

      val columnLong = allocate(len, new ArrayType(LongType, false), memMode)
      val longArray = Array[Long](0, 1, 2, 3)
      longArray.zipWithIndex.foreach { case (v, i) => columnLong.arrayData.putLong(i, v) }
      columnLong.putArray(0, 0, len)
      assert(columnLong.getArray(0).toLongArray === longArray)
      columnLong.close()

      val columnFloat = allocate(len, new ArrayType(FloatType, false), memMode)
      val floatArray = Array(0.0F, 1.1F, 2.2F, 3.3F)
      floatArray.zipWithIndex.foreach { case (v, i) => columnFloat.arrayData.putFloat(i, v) }
      columnFloat.putArray(0, 0, len)
      assert(columnFloat.getArray(0).toFloatArray === floatArray)
      columnFloat.close()

      val columnDouble = allocate(len, new ArrayType(DoubleType, false), memMode)
      val doubleArray = Array(0.0, 1.1, 2.2, 3.3)
      doubleArray.zipWithIndex.foreach { case (v, i) => columnDouble.arrayData.putDouble(i, v) }
      columnDouble.putArray(0, 0, len)
      assert(columnDouble.getArray(0).toDoubleArray === doubleArray)
      columnDouble.close()
    }
  }

  test("Int Map") {
    (MemoryMode.ON_HEAP :: MemoryMode.OFF_HEAP :: Nil).foreach { memMode =>
      val column = allocate(10, new MapType(IntegerType, IntegerType, false), memMode)
      (0 to 1).foreach { colIndex =>
        val data = column.getChild(colIndex)
        (0 to 5).foreach {i =>
          data.putInt(i, i * (colIndex + 1))
        }
      }

      // Populate it with maps [0->0], [1->2, 2->4], null, [], [3->6, 4->8, 5->10]
      column.putArray(0, 0, 1)
      column.putArray(1, 1, 2)
      column.putNull(2)
      assert(column.getMap(2) == null)
      column.putArray(3, 3, 0)
      column.putArray(4, 3, 3)

      assert(column.getMap(0).numElements == 1)
      assert(column.getMap(1).numElements == 2)
      assert(column.isNullAt(2))
      assert(column.getMap(3).numElements == 0)
      assert(column.getMap(4).numElements == 3)

      val a1 = ColumnVectorUtils.toJavaIntMap(column.getMap(0))
      val a2 = ColumnVectorUtils.toJavaIntMap(column.getMap(1))
      val a4 = ColumnVectorUtils.toJavaIntMap(column.getMap(3))
      val a5 = ColumnVectorUtils.toJavaIntMap(column.getMap(4))

      assert(a1.asScala == Map(0 -> 0))
      assert(a2.asScala == Map(1 -> 2, 2 -> 4))
      assert(a4.asScala == Map())
      assert(a5.asScala == Map(3 -> 6, 4 -> 8, 5 -> 10))

      def toScalaMap(mapData: MapData): Map[Int, Int] = {
        val keys = mapData.keyArray().toSeq[Int](IntegerType)
        val values = mapData.valueArray().toSeq[Int](IntegerType)
        (keys zip values).toMap
      }
      assert(toScalaMap(column.getMap(0).copy()) === Map(0 -> 0))
      assert(toScalaMap(column.getMap(1).copy()) === Map(1 -> 2, 2 -> 4))
      assert(toScalaMap(column.getMap(3).copy()) === Map())
      assert(toScalaMap(column.getMap(4).copy()) === Map(3 -> 6, 4 -> 8, 5 -> 10))

      column.close()
    }
  }

  testVector(
    "Struct Column",
    10,
    new StructType().add("int", IntegerType).add("double", DoubleType)) { column =>
      val c1 = column.getChild(0)
      val c2 = column.getChild(1)
      assert(c1.dataType() == IntegerType)
      assert(c2.dataType() == DoubleType)

      c1.putInt(0, 123)
      c2.putDouble(0, 3.45)

      column.putNull(1)
      assert(column.getStruct(1) == null)

      c1.putInt(2, 456)
      c2.putDouble(2, 5.67)

      val s = column.getStruct(0)
      assert(s.getInt(0) == 123)
      assert(s.getDouble(1) == 3.45)

      assert(column.isNullAt(1))
      assert(column.getStruct(1) == null)

      val s2 = column.getStruct(2)
      assert(s2.getInt(0) == 456)
      assert(s2.getDouble(1) == 5.67)
  }

  testVector("Nest Array in Array", 10, new ArrayType(new ArrayType(IntegerType, true), true)) {
    column =>
      val childColumn = column.arrayData()
      val data = column.arrayData().arrayData()
      (0 until 6).foreach {
        case 3 => data.putNull(3)
        case i => data.putInt(i, i)
      }
      // Arrays in child column: [0], [1, 2], [], [null, 4, 5]
      childColumn.putArray(0, 0, 1)
      childColumn.putArray(1, 1, 2)
      childColumn.putArray(2, 2, 0)
      childColumn.putArray(3, 3, 3)
      // Arrays in column: [[0]], [[1, 2], []], [[], [null, 4, 5]], null
      column.putArray(0, 0, 1)
      column.putArray(1, 1, 2)
      column.putArray(2, 2, 2)
      column.putNull(3)

      assert(column.getArray(0).getArray(0).toIntArray() === Array(0))
      assert(column.getArray(1).getArray(0).toIntArray() === Array(1, 2))
      assert(column.getArray(1).getArray(1).toIntArray() === Array())
      assert(column.getArray(2).getArray(0).toIntArray() === Array())
      assert(column.getArray(2).getArray(1).isNullAt(0))
      assert(column.getArray(2).getArray(1).getInt(1) === 4)
      assert(column.getArray(2).getArray(1).getInt(2) === 5)
      assert(column.isNullAt(3))
  }

  private val structType: StructType = new StructType().add("i", IntegerType).add("l", LongType)

  testVector(
    "Nest Struct in Array",
    10,
    new ArrayType(structType, true)) { column =>
      val data = column.arrayData()
      val c0 = data.getChild(0)
      val c1 = data.getChild(1)
      // Structs in child column: (0, 0), (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)
      (0 until 6).foreach { i =>
        c0.putInt(i, i)
        c1.putLong(i, i * 10)
      }
      // Arrays in column: [(0, 0), (1, 10)], [(1, 10), (2, 20), (3, 30)],
      // [(4, 40), (5, 50)]
      column.putArray(0, 0, 2)
      column.putArray(1, 1, 3)
      column.putArray(2, 4, 2)

      assert(column.getArray(0).getStruct(0, 2).toSeq(structType) === Seq(0, 0))
      assert(column.getArray(0).getStruct(1, 2).toSeq(structType) === Seq(1, 10))
      assert(column.getArray(1).getStruct(0, 2).toSeq(structType) === Seq(1, 10))
      assert(column.getArray(1).getStruct(1, 2).toSeq(structType) === Seq(2, 20))
      assert(column.getArray(1).getStruct(2, 2).toSeq(structType) === Seq(3, 30))
      assert(column.getArray(2).getStruct(0, 2).toSeq(structType) === Seq(4, 40))
      assert(column.getArray(2).getStruct(1, 2).toSeq(structType) === Seq(5, 50))
  }

  testVector(
    "Nest Array in Struct",
    10,
    new StructType()
      .add("int", IntegerType)
      .add("array", new ArrayType(IntegerType, true))) { column =>
      val c0 = column.getChild(0)
      val c1 = column.getChild(1)
      c0.putInt(0, 0)
      c0.putInt(1, 1)
      c0.putInt(2, 2)
      val c1Child = c1.arrayData()
      (0 until 6).foreach { i =>
        c1Child.putInt(i, i)
      }
      // Arrays in c1: [0, 1], [2], [3, 4, 5]
      c1.putArray(0, 0, 2)
      c1.putArray(1, 2, 1)
      c1.putArray(2, 3, 3)

      assert(column.getStruct(0).getInt(0) === 0)
      assert(column.getStruct(0).getArray(1).toIntArray() === Array(0, 1))
      assert(column.getStruct(1).getInt(0) === 1)
      assert(column.getStruct(1).getArray(1).toIntArray() === Array(2))
      assert(column.getStruct(2).getInt(0) === 2)
      assert(column.getStruct(2).getArray(1).toIntArray() === Array(3, 4, 5))
  }

  private val subSchema: StructType = new StructType()
    .add("int", IntegerType)
    .add("int", IntegerType)
  testVector(
    "Nest Struct in Struct",
    10,
    new StructType().add("int", IntegerType).add("struct", subSchema)) { column =>
      val c0 = column.getChild(0)
      val c1 = column.getChild(1)
      c0.putInt(0, 0)
      c0.putInt(1, 1)
      c0.putInt(2, 2)
      val c1c0 = c1.getChild(0)
      val c1c1 = c1.getChild(1)
      // Structs in c1: (7, 70), (8, 80), (9, 90)
      c1c0.putInt(0, 7)
      c1c0.putInt(1, 8)
      c1c0.putInt(2, 9)
      c1c1.putInt(0, 70)
      c1c1.putInt(1, 80)
      c1c1.putInt(2, 90)

      assert(column.getStruct(0).getInt(0) === 0)
      assert(column.getStruct(0).getStruct(1, 2).toSeq(subSchema) === Seq(7, 70))
      assert(column.getStruct(1).getInt(0) === 1)
      assert(column.getStruct(1).getStruct(1, 2).toSeq(subSchema) === Seq(8, 80))
      assert(column.getStruct(2).getInt(0) === 2)
      assert(column.getStruct(2).getStruct(1, 2).toSeq(subSchema) === Seq(9, 90))
  }

  test("ColumnarBatch basic") {
    (MemoryMode.ON_HEAP :: MemoryMode.OFF_HEAP :: Nil).foreach { memMode => {
      val schema = new StructType()
        .add("intCol", IntegerType)
        .add("doubleCol", DoubleType)
        .add("intCol2", IntegerType)
        .add("string", BinaryType)

      val capacity = 4 * 1024
      val columns = schema.fields.map { field =>
        allocate(capacity, field.dataType, memMode)
      }
      val batch = new ColumnarBatch(columns.toArray)
      assert(batch.numCols() == 4)
      assert(batch.numRows() == 0)
      assert(batch.rowIterator().hasNext == false)

      // Add a row [1, 1.1, NULL]
      columns(0).putInt(0, 1)
      columns(1).putDouble(0, 1.1)
      columns(2).putNull(0)
      columns(3).putByteArray(0, "Hello".getBytes(StandardCharsets.UTF_8))
      batch.setNumRows(1)

      // Verify the results of the row.
      assert(batch.numCols() == 4)
      assert(batch.numRows() == 1)
      assert(batch.rowIterator().hasNext)
      assert(batch.rowIterator().hasNext)

      assert(columns(0).getInt(0) == 1)
      assert(columns(0).isNullAt(0) == false)
      assert(columns(1).getDouble(0) == 1.1)
      assert(columns(1).isNullAt(0) == false)
      assert(columns(2).isNullAt(0))
      assert(columns(3).getUTF8String(0).toString == "Hello")

      // Verify the iterator works correctly.
      val it = batch.rowIterator()
      assert(it.hasNext())
      val row = it.next()
      assert(row.getInt(0) == 1)
      assert(row.isNullAt(0) == false)
      assert(row.getDouble(1) == 1.1)
      assert(row.isNullAt(1) == false)
      assert(row.isNullAt(2))
      assert(columns(3).getUTF8String(0).toString == "Hello")
      assert(it.hasNext == false)
      assert(it.hasNext == false)

      // Reset and add 3 rows
      columns.foreach(_.reset())
      // Add rows [NULL, 2.2, 2, "abc"], [3, NULL, 3, ""], [4, 4.4, 4, "world]
      columns(0).putNull(0)
      columns(1).putDouble(0, 2.2)
      columns(2).putInt(0, 2)
      columns(3).putByteArray(0, "abc".getBytes(StandardCharsets.UTF_8))

      columns(0).putInt(1, 3)
      columns(1).putNull(1)
      columns(2).putInt(1, 3)
      columns(3).putByteArray(1, "".getBytes(StandardCharsets.UTF_8))

      columns(0).putInt(2, 4)
      columns(1).putDouble(2, 4.4)
      columns(2).putInt(2, 4)
      columns(3).putByteArray(2, "world".getBytes(StandardCharsets.UTF_8))
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
      val it2 = batch.rowIterator()
      rowEquals(it2.next(), Row(null, 2.2, 2, "abc"))
      rowEquals(it2.next(), Row(3, null, 3, ""))
      rowEquals(it2.next(), Row(4, 4.4, 4, "world"))
      assert(!it.hasNext)

      batch.close()
    }}
  }

  test("ColumnarBatch customization") {
    (MemoryMode.ON_HEAP :: MemoryMode.OFF_HEAP :: Nil).foreach { memMode => {
      val schema = new StructType()
        .add("intCol", IntegerType)
        .add("doubleCol", DoubleType)
        .add("intCol2", IntegerType)
        .add("string", BinaryType)

      val capacity = 4 * 1024
      val columns = schema.fields.map { field =>
        allocate(capacity, field.dataType, memMode)
      }
      val batch = new CustomizedColumnarBatch(columns.toArray)
      assert(batch.numCols() == 4)
      assert(batch.numRows() == 0)
      assert(batch.rowIterator().hasNext == false)

      // Add a row [1, 1.1, NULL, "Hello"]
      columns(0).putInt(0, 1)
      columns(1).putDouble(0, 1.1)
      columns(2).putNull(0)
      columns(3).putByteArray(0, "Hello".getBytes(StandardCharsets.UTF_8))
      batch.setNumRows(1)

      // Verify the results of the row.
      assert(batch.numCols() == 4)
      assert(batch.numRows() == 1)
      // rowId 0 is skipped
      assert(batch.rowIterator().hasNext == false)

      // Reset and add 3 rows
      columns.foreach(_.reset())
      // Add rows [NULL, 2.2, 2, "abc"], [3, NULL, 3, ""], [4, 4.4, 4, "world"]
      columns(0).putNull(0)
      columns(1).putDouble(0, 2.2)
      columns(2).putInt(0, 2)
      columns(3).putByteArray(0, "abc".getBytes(StandardCharsets.UTF_8))

      columns(0).putInt(1, 3)
      columns(1).putNull(1)
      columns(2).putInt(1, 3)
      columns(3).putByteArray(1, "".getBytes(StandardCharsets.UTF_8))

      columns(0).putInt(2, 4)
      columns(1).putDouble(2, 4.4)
      columns(2).putInt(2, 4)
      columns(3).putByteArray(2, "world".getBytes(StandardCharsets.UTF_8))
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
      val it2 = batch.rowIterator()
      // Only second row is valid
      rowEquals(it2.next(), Row(3, null, 3, ""))
      assert(!it2.hasNext)

      batch.close()
    }}
  }

  class CustomizedColumnarBatch(columns: Array[ColumnVector]) extends ColumnarBatch(columns) {
    val skipRowIds = List(0, 2)
    override def rowIterator(): util.Iterator[InternalRow] = {
      val maxRows: Int = numRows
      val row = new ColumnarBatchRow(columns)
      new util.Iterator[InternalRow]() {
        var rowId = 0

        override def hasNext: Boolean = {
          while (skipRowIds.contains(rowId)) {
            rowId += 1
          }
          rowId < maxRows
        }

        override def next: InternalRow = {
          while (skipRowIds.contains(rowId)) {
            rowId += 1
          }

          if (rowId >= maxRows) throw new NoSuchElementException
          row.rowId = rowId
          rowId += 1
          row
        }
      }
    }
  }

  private def doubleEquals(d1: Double, d2: Double): Boolean = {
    if (d1.isNaN && d2.isNaN) {
      true
    } else {
      d1 == d2
    }
  }

  private def compareStruct(fields: Seq[StructField], r1: InternalRow, r2: Row,
      seed: Long): Unit = {
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
          case DateType =>
            assert(r1.getInt(ordinal) == DateTimeUtils.fromJavaDate(r2.getDate(ordinal)),
              "Seed = " + seed)
          case TimestampType =>
            assert(r1.getLong(ordinal) ==
              DateTimeUtils.fromJavaTimestamp(r2.getTimestamp(ordinal)),
              "Seed = " + seed)
          case TimestampNTZType =>
            assert(r1.getLong(ordinal) ==
              DateTimeUtils.localDateTimeToMicros(r2.getAs[LocalDateTime](ordinal)),
              "Seed = " + seed)
          case t: DecimalType =>
            val d1 = r1.getDecimal(ordinal, t.precision, t.scale).toBigDecimal
            val d2 = r2.getDecimal(ordinal)
            assert(d1.compare(d2) == 0, "Seed = " + seed)
          case StringType =>
            assert(r1.getString(ordinal) == r2.getString(ordinal), "Seed = " + seed)
          case BinaryType =>
            assert(r1.getBinary(ordinal) sameElements r2.getAs[Array[Byte]](ordinal),
              "Seed = " + seed)
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
              case StringType =>
                var i = 0
                while (i < a1.length) {
                  assert((a1(i) == null) == (a2(i) == null), "Seed = " + seed)
                  if (a1(i) != null) {
                    val s1 = a1(i).asInstanceOf[UTF8String].toString
                    val s2 = a2(i).asInstanceOf[String]
                    assert(s1 === s2, "Seed = " + seed)
                  }
                  i += 1
                }
              case DateType =>
                var i = 0
                while (i < a1.length) {
                  assert((a1(i) == null) == (a2(i) == null), "Seed = " + seed)
                  if (a1(i) != null) {
                    val i1 = a1(i).asInstanceOf[Int]
                    val i2 = DateTimeUtils.fromJavaDate(a2(i).asInstanceOf[Date])
                    assert(i1 === i2, "Seed = " + seed)
                  }
                  i += 1
                }
              case TimestampType =>
                var i = 0
                while (i < a1.length) {
                  assert((a1(i) == null) == (a2(i) == null), "Seed = " + seed)
                  if (a1(i) != null) {
                    val i1 = a1(i).asInstanceOf[Long]
                    val i2 = DateTimeUtils.fromJavaTimestamp(a2(i).asInstanceOf[Timestamp])
                    assert(i1 === i2, "Seed = " + seed)
                  }
                  i += 1
                }
              case TimestampNTZType =>
                var i = 0
                while (i < a1.length) {
                  assert((a1(i) == null) == (a2(i) == null), "Seed = " + seed)
                  if (a1(i) != null) {
                    val i1 = a1(i).asInstanceOf[Long]
                    val i2 = DateTimeUtils.localDateTimeToMicros(a2(i).asInstanceOf[LocalDateTime])
                    assert(i1 === i2, "Seed = " + seed)
                  }
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
            compareStruct(childFields.toImmutableArraySeq, r1.getStruct(ordinal, fields.length),
              r2.getStruct(ordinal), seed)
          case _ =>
            throw new UnsupportedOperationException("Not implemented " + field.dataType)
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
  def testRandomRows(flatSchema: Boolean, numFields: Int): Unit = {
    val types = Array(
      BooleanType, ByteType, FloatType, DoubleType, IntegerType, LongType, ShortType,
      DecimalType.ShortDecimal, DecimalType.IntDecimal, DecimalType.ByteDecimal,
      DecimalType.FloatDecimal, DecimalType.LongDecimal, new DecimalType(5, 2),
      new DecimalType(12, 2), new DecimalType(30, 10), CalendarIntervalType,
      DateType, StringType, BinaryType, TimestampType, TimestampNTZType)
    val seed = System.nanoTime()
    val NUM_ROWS = 200
    val NUM_ITERS = 1000
    val random = new Random(seed)
    var i = 0
    while (i < NUM_ITERS) {
      val schema = if (flatSchema) {
        RandomDataGenerator.randomSchema(random, numFields, types.toImmutableArraySeq)
      } else {
        RandomDataGenerator.randomNestedSchema(random, numFields, types.toImmutableArraySeq)
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

  test("exceeding maximum capacity should throw an error") {
    (MemoryMode.ON_HEAP :: MemoryMode.OFF_HEAP :: Nil).foreach { memMode =>
      val column = allocate(1, ByteType, memMode)
      column.MAX_CAPACITY = 15
      column.appendBytes(5, 0.toByte)
      // Successfully allocate twice the requested capacity
      assert(column.capacity == 10)
      column.appendBytes(10, 0.toByte)
      // Allocated capacity doesn't exceed MAX_CAPACITY
      assert(column.capacity == 15)
      val ex = intercept[RuntimeException] {
        // Over-allocating beyond MAX_CAPACITY throws an exception
        column.appendBytes(10, 0.toByte)
      }
      assert(ex.getMessage.contains(s"Cannot reserve additional contiguous bytes in the " +
        s"vectorized reader"))
    }
  }

  test("create columnar batch from Arrow column vectors") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("int", 0, Long.MaxValue)
    val vector1 = ArrowUtils.toArrowField("int1", IntegerType, nullable = true, null)
      .createVector(allocator).asInstanceOf[IntVector]
    vector1.allocateNew()
    val vector2 = ArrowUtils.toArrowField("int2", IntegerType, nullable = true, null)
      .createVector(allocator).asInstanceOf[IntVector]
    vector2.allocateNew()

    (0 until 10).foreach { i =>
      vector1.setSafe(i, i)
      vector2.setSafe(i + 1, i)
    }
    vector1.setNull(10)
    vector1.setValueCount(11)
    vector2.setNull(0)
    vector2.setValueCount(11)

    val columnVectors = Seq(new ArrowColumnVector(vector1), new ArrowColumnVector(vector2))

    val schema = StructType(Seq(StructField("int1", IntegerType), StructField("int2", IntegerType)))
    val batch = new ColumnarBatch(columnVectors.toArray)
    batch.setNumRows(11)

    assert(batch.numCols() == 2)
    assert(batch.numRows() == 11)

    val rowIter = batch.rowIterator().asScala
    rowIter.zipWithIndex.foreach { case (row, i) =>
      if (i == 10) {
        assert(row.isNullAt(0))
      } else {
        assert(row.getInt(0) == i)
      }
      if (i == 0) {
        assert(row.isNullAt(1))
      } else {
        assert(row.getInt(1) == i - 1)
      }
    }

    batch.close()
    allocator.close()
  }

  test("RowToColumnConverter") {
    val schema = StructType(
      StructField("str", StringType) ::
        StructField("bool", BooleanType) ::
        StructField("byte", ByteType) ::
        StructField("short", ShortType) ::
        StructField("int", IntegerType) ::
        StructField("long", LongType) ::
        StructField("float", FloatType) ::
        StructField("double", DoubleType) ::
        StructField("decimal", DecimalType(25, 5)) ::
        StructField("date", DateType) ::
        StructField("ts", TimestampType) ::
        StructField("cal", CalendarIntervalType) ::
        StructField("arr_of_int", ArrayType(IntegerType)) ::
        StructField("int_and_int", StructType(
          StructField("int1", IntegerType, false) ::
            StructField("int2", IntegerType) ::
            Nil
        )) ::
        StructField("int_to_int", MapType(IntegerType, IntegerType)) ::
        StructField("binary", BinaryType) ::
        StructField("ts_ntz", TimestampNTZType) ::
        StructField("variant", VariantType) ::
        Nil)
    var mapBuilder = new ArrayBasedMapBuilder(IntegerType, IntegerType)
    mapBuilder.put(1, 10)
    mapBuilder.put(20, null)

    val tsString1 = "2015-01-01 23:50:59.123"
    val ts1 = DateTimeUtils.fromJavaTimestamp(java.sql.Timestamp.valueOf(tsString1))
    val tsNTZ1 =
      DateTimeUtils.localDateTimeToMicros(LocalDateTime.parse(tsString1.replace(" ", "T")))
    val tsString2 = "1880-01-05 12:45:21.321"
    val ts2 = DateTimeUtils.fromJavaTimestamp(java.sql.Timestamp.valueOf(tsString2))
    val tsNTZ2 =
      DateTimeUtils.localDateTimeToMicros(LocalDateTime.parse(tsString2.replace(" ", "T")))

    val variantVal1 = new VariantVal(Array[Byte](1, 2, 3), Array[Byte](4, 5))
    val variantVal2 = new VariantVal(Array[Byte](6), Array[Byte](7, 8))

    val row1 = new GenericInternalRow(Array[Any](
      UTF8String.fromString("a string"),
      true,
      1.toByte,
      2.toShort,
      3,
      Long.MaxValue,
      0.25.toFloat,
      0.75D,
      Decimal("1234.23456"),
      DateTimeUtils.fromJavaDate(java.sql.Date.valueOf("2015-01-01")),
      ts1,
      new CalendarInterval(1, 0, 0),
      new GenericArrayData(Array(1, 2, 3, 4, null)),
      new GenericInternalRow(Array[Any](5.asInstanceOf[Any], 10)),
      mapBuilder.build(),
      "Spark SQL".getBytes(),
      tsNTZ1,
      variantVal1
    ))

    mapBuilder = new ArrayBasedMapBuilder(IntegerType, IntegerType)
    mapBuilder.put(30, null)
    mapBuilder.put(40, 50)
    val row2 = new GenericInternalRow(Array[Any](
      UTF8String.fromString("second string"),
      false,
      -1.toByte,
      17.toShort,
      Int.MinValue,
      987654321L,
      Float.NaN,
      Double.PositiveInfinity,
      Decimal("0.01000"),
      DateTimeUtils.fromJavaDate(java.sql.Date.valueOf("1875-12-12")),
      ts2,
      new CalendarInterval(-10, -50, -100),
      new GenericArrayData(Array(5, 10, -100)),
      new GenericInternalRow(Array[Any](20.asInstanceOf[Any], null)),
      mapBuilder.build(),
      "Parquet".getBytes(),
      tsNTZ2,
      variantVal2
    ))

    val row3 = new GenericInternalRow(Array[Any](
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null
    ))

    val converter = new RowToColumnConverter(schema)
    val columns = OnHeapColumnVector.allocateColumns(3, schema)
    val batch = new ColumnarBatch(columns.toArray, 3)
    try {
      converter.convert(row1, columns.toArray)
      converter.convert(row2, columns.toArray)
      converter.convert(row3, columns.toArray)

      assert(columns(0).dataType() == StringType)
      assert(columns(0).getUTF8String(0).toString == "a string")
      assert(columns(0).getUTF8String(1).toString == "second string")
      assert(columns(0).isNullAt(2))

      assert(columns(1).dataType() == BooleanType)
      assert(columns(1).getBoolean(0) == true)
      assert(columns(1).getBoolean(1) == false)
      assert(columns(1).isNullAt(2))

      assert(columns(2).dataType() == ByteType)
      assert(columns(2).getByte(0) == 1.toByte)
      assert(columns(2).getByte(1) == -1.toByte)
      assert(columns(2).isNullAt(2))

      assert(columns(3).dataType() == ShortType)
      assert(columns(3).getShort(0) == 2.toShort)
      assert(columns(3).getShort(1) == 17.toShort)
      assert(columns(3).isNullAt(2))

      assert(columns(4).dataType() == IntegerType)
      assert(columns(4).getInt(0) == 3)
      assert(columns(4).getInt(1) == Int.MinValue)
      assert(columns(4).isNullAt(2))

      assert(columns(5).dataType() == LongType)
      assert(columns(5).getLong(0) == Long.MaxValue)
      assert(columns(5).getLong(1) == 987654321L)
      assert(columns(5).isNullAt(2))

      assert(columns(6).dataType() == FloatType)
      assert(columns(6).getFloat(0) == 0.25.toFloat)
      assert(columns(6).getFloat(1).isNaN)
      assert(columns(6).isNullAt(2))

      assert(columns(7).dataType() == DoubleType)
      assert(columns(7).getDouble(0) == 0.75D)
      assert(columns(7).getDouble(1) == Double.PositiveInfinity)
      assert(columns(7).isNullAt(2))

      assert(columns(8).dataType() == DecimalType(25, 5))
      assert(columns(8).getDecimal(0, 25, 5) == Decimal("1234.23456"))
      assert(columns(8).getDecimal(1, 25, 5) == Decimal("0.01000"))
      assert(columns(8).isNullAt(2))

      assert(columns(9).dataType() == DateType)
      assert(columns(9).getInt(0) ==
        DateTimeUtils.fromJavaDate(java.sql.Date.valueOf("2015-01-01")))
      assert(columns(9).getInt(1) ==
        DateTimeUtils.fromJavaDate(java.sql.Date.valueOf("1875-12-12")))
      assert(columns(9).isNullAt(2))

      assert(columns(10).dataType() == TimestampType)
      assert(columns(10).getLong(0) == ts1)
      assert(columns(10).getLong(1) == ts2)
      assert(columns(10).isNullAt(2))

      assert(columns(11).dataType() == CalendarIntervalType)
      assert(columns(11).getInterval(0) == new CalendarInterval(1, 0, 0))
      assert(columns(11).getInterval(1) == new CalendarInterval(-10, -50, -100))
      assert(columns(11).isNullAt(2))

      assert(columns(12).dataType() == ArrayType(IntegerType))
      val arr1 = columns(12).getArray(0)
      assert(arr1.numElements() == 5)
      assert(arr1.getInt(0) == 1)
      assert(arr1.getInt(1) == 2)
      assert(arr1.getInt(2) == 3)
      assert(arr1.getInt(3) == 4)
      assert(arr1.isNullAt(4))

      val arr2 = columns(12).getArray(1)
      assert(arr2.numElements() == 3)
      assert(arr2.getInt(0) == 5)
      assert(arr2.getInt(1) == 10)
      assert(arr2.getInt(2) == -100)

      assert(columns(12).isNullAt(2))

      assert(columns(13).dataType() == StructType(
        StructField("int1", IntegerType, false) ::
          StructField("int2", IntegerType) ::
          Nil
      ))
      val struct1 = columns(13).getStruct(0)
      assert(struct1.getInt(0) == 5)
      assert(struct1.getInt(1) == 10)
      val struct2 = columns(13).getStruct(1)
      assert(struct2.getInt(0) == 20)
      assert(struct2.isNullAt(1))
      assert(columns(13).isNullAt(2))

      assert(columns(14).dataType() == MapType(IntegerType, IntegerType))
      val map1 = columns(14).getMap(0)
      assert(map1.numElements() == 2)
      assert(map1.keyArray().getInt(0) == 1)
      assert(map1.valueArray().getInt(0) == 10)
      assert(map1.keyArray().getInt(1) == 20)
      assert(map1.valueArray().isNullAt(1))

      val map2 = columns(14).getMap(1)
      assert(map2.numElements() == 2)
      assert(map2.keyArray().getInt(0) == 30)
      assert(map2.valueArray().isNullAt(0))
      assert(map2.keyArray().getInt(1) == 40)
      assert(map2.valueArray().getInt(1) == 50)
      assert(columns(14).isNullAt(2))

      assert(columns(15).dataType() == BinaryType)
      assert(new String(columns(15).getBinary(0)) == "Spark SQL")
      assert(new String(columns(15).getBinary(1)) == "Parquet")
      assert(columns(15).isNullAt(2))

      assert(columns(16).dataType() == TimestampNTZType)
      assert(columns(16).getLong(0) == tsNTZ1)
      assert(columns(16).getLong(1) == tsNTZ2)
      assert(columns(16).isNullAt(2))

      assert(columns(17).dataType() == VariantType)
      assert(columns(17).getVariant(0).debugString() == variantVal1.debugString())
      assert(columns(17).getVariant(1).debugString() == variantVal2.debugString())
      assert(columns(17).isNullAt(2))
      assert(columns(17).getChild(0).isNullAt(2))
      assert(columns(17).getChild(1).isNullAt(2))
    } finally {
      batch.close()
    }
  }

  test("SPARK-37161: RowToColumnConverter for AnsiIntervalType") {
    DataTypeTestUtils.yearMonthIntervalTypes.foreach { dt =>
      val schema = new StructType().add(dt.typeName, dt)
      val converter = new RowToColumnConverter(schema)
      val columns = OnHeapColumnVector.allocateColumns(10, schema)
      try {
        assert(columns(0).dataType() == dt)
        (0 until 9).foreach { i =>
          val row = new GenericInternalRow(Array[Any](i))
          converter.convert(row, columns.toArray)
          assert(columns(0).getInt(i) == i)
        }
        converter.convert(new GenericInternalRow(Array[Any](null)), columns.toArray)
        assert(columns(0).isNullAt(9))
      } finally {
        columns.foreach(_.close())
      }
    }
    DataTypeTestUtils.dayTimeIntervalTypes.foreach { dt =>
      val schema = new StructType().add(dt.typeName, dt)
      val converter = new RowToColumnConverter(schema)
      val columns = OnHeapColumnVector.allocateColumns(10, schema)
      try {
        assert(columns(0).dataType() == dt)
        (0 until 9).foreach { i =>
          val row = new GenericInternalRow(Array[Any](i.toLong))
          converter.convert(row, columns.toArray)
          assert(columns(0).getLong(i) == i)
        }
        converter.convert(new GenericInternalRow(Array[Any](null)), columns.toArray)
        assert(columns(0).isNullAt(9))
      } finally {
        columns.foreach(_.close())
      }
    }
  }

  testVector("Decimal API", 4, DecimalType.IntDecimal) {
    column =>

      val reference = mutable.ArrayBuffer.empty[Decimal]

      var idx = 0
      column.putDecimal(idx, new Decimal().set(10), 10)
      reference += new Decimal().set(10)
      idx += 1

      column.putDecimal(idx, new Decimal().set(20), 10)
      reference += new Decimal().set(20)
      idx += 1

      column.putNull(idx)
      assert(column.getDecimal(idx, 10, 0) == null)
      reference += null
      idx += 1

      column.putDecimal(idx, new Decimal().set(30), 10)
      reference += new Decimal().set(30)

      reference.zipWithIndex.foreach { case (v, i) =>
        val errMsg = "VectorType=" + column.getClass.getSimpleName
        assert(v == column.getDecimal(i, 10, 0), errMsg)
        if (v == null) assert(column.isNullAt(i), errMsg)
      }

      column.close()
  }

  testVector("Binary APIs", 4, BinaryType) {
    column =>

      val reference = mutable.ArrayBuffer.empty[String]
      var idx = 0
      column.putByteArray(idx, "Hello".getBytes(StandardCharsets.UTF_8))
      reference += "Hello"
      idx += 1

      column.putByteArray(idx, "World".getBytes(StandardCharsets.UTF_8))
      reference += "World"
      idx += 1

      column.putNull(idx)
      reference += null
      idx += 1

      column.putByteArray(idx, "abc".getBytes(StandardCharsets.UTF_8))
      reference += "abc"

      reference.zipWithIndex.foreach { case (v, i) =>
        val errMsg = "VectorType=" + column.getClass.getSimpleName
        if (v != null) {
          assert(v == new String(column.getBinary(i)), errMsg)
        } else {
          assert(column.isNullAt(i), errMsg)
          assert(column.getBinary(i) == null, errMsg)
        }
      }

      column.close()
  }

  testVector("Timestamp without timezone", 10, TimestampNTZType) {
    column =>
      val dt = TimestampNTZType
      (0 until 10).foreach { i =>
        column.putLong(i, i)
      }
      val bachRow = new ColumnarBatchRow(Array(column))
      (0 until 10).foreach { i =>
        bachRow.rowId = i
        assert(bachRow.get(0, dt) === i)
        val batchRowCopy = bachRow.copy()
        assert(batchRowCopy.get(0, dt) === i)
      }
  }

  testVector("WritableColumnVector.reserve(): requested capacity is negative", 1024, ByteType) {
    column =>
      val ex = intercept[RuntimeException] { column.reserve(-1) }
      assert(ex.getMessage.contains(
          "Cannot reserve additional contiguous bytes in the vectorized reader (integer overflow)"))
  }

  DataTypeTestUtils.yearMonthIntervalTypes.foreach { dt =>
    testVector(dt.typeName, 10, dt) {
      column =>
        (0 until 10).foreach{ i =>
          column.putInt(i, i)
        }
        val bachRow = new ColumnarBatchRow(Array(column))
        (0 until 10).foreach { i =>
          bachRow.rowId = i
          assert(bachRow.get(0, dt) === i)
          val batchRowCopy = bachRow.copy()
          assert(batchRowCopy.get(0, dt) === i)
        }
    }
  }

  DataTypeTestUtils.dayTimeIntervalTypes.foreach { dt =>
    testVector(dt.typeName, 10, dt) {
      column =>
        (0 until 10).foreach{ i =>
          column.putLong(i, i)
        }
        val bachRow = new ColumnarBatchRow(Array(column))
        (0 until 10).foreach { i =>
          bachRow.rowId = i
          assert(bachRow.get(0, dt) === i)
          val batchRowCopy = bachRow.copy()
          assert(batchRowCopy.get(0, dt) === i)
        }
    }
  }
}
