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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.execution.columnar.ColumnAccessor
import org.apache.spark.sql.execution.columnar.compression.ColumnBuilderHelper
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarArray
import org.apache.spark.unsafe.types.UTF8String

class ColumnVectorSuite extends SparkFunSuite with BeforeAndAfterEach {
  private def withVector(
      vector: WritableColumnVector)(
      block: WritableColumnVector => Unit): Unit = {
    try block(vector) finally vector.close()
  }

  private def withVectors(
      size: Int,
      dt: DataType)(
      block: WritableColumnVector => Unit): Unit = {
    withVector(new OnHeapColumnVector(size, dt))(block)
    withVector(new OffHeapColumnVector(size, dt))(block)
  }

  private def testVectors(
      name: String,
      size: Int,
      dt: DataType)(
      block: WritableColumnVector => Unit): Unit = {
    test(name) {
      withVectors(size, dt)(block)
    }
  }

  testVectors("boolean", 10, BooleanType) { testVector =>
    (0 until 10).foreach { i =>
      testVector.appendBoolean(i % 2 == 0)
    }

    val array = new ColumnarArray(testVector, 0, 10)
    val arrayCopy = array.copy()

    (0 until 10).foreach { i =>
      assert(array.get(i, BooleanType) === (i % 2 == 0))
      assert(arrayCopy.get(i, BooleanType) === (i % 2 == 0))
    }
  }

  testVectors("byte", 10, ByteType) { testVector =>
    (0 until 10).foreach { i =>
      testVector.appendByte(i.toByte)
    }

    val array = new ColumnarArray(testVector, 0, 10)
    val arrayCopy = array.copy()

    (0 until 10).foreach { i =>
      assert(array.get(i, ByteType) === i.toByte)
      assert(arrayCopy.get(i, ByteType) === i.toByte)
    }
  }

  testVectors("short", 10, ShortType) { testVector =>
    (0 until 10).foreach { i =>
      testVector.appendShort(i.toShort)
    }

    val array = new ColumnarArray(testVector, 0, 10)
    val arrayCopy = array.copy()

    (0 until 10).foreach { i =>
      assert(array.get(i, ShortType) === i.toShort)
      assert(arrayCopy.get(i, ShortType) === i.toShort)
    }
  }

  testVectors("int", 10, IntegerType) { testVector =>
    (0 until 10).foreach { i =>
      testVector.appendInt(i)
    }

    val array = new ColumnarArray(testVector, 0, 10)
    val arrayCopy = array.copy()

    (0 until 10).foreach { i =>
      assert(array.get(i, IntegerType) === i)
      assert(arrayCopy.get(i, IntegerType) === i)
    }
  }

  testVectors("date", 10, DateType) { testVector =>
    (0 until 10).foreach { i =>
      testVector.appendInt(i)
    }

    val array = new ColumnarArray(testVector, 0, 10)
    val arrayCopy = array.copy()

    (0 until 10).foreach { i =>
      assert(array.get(i, DateType) === i)
      assert(arrayCopy.get(i, DateType) === i)
    }
  }

  testVectors("long", 10, LongType) { testVector =>
    (0 until 10).foreach { i =>
      testVector.appendLong(i)
    }

    val array = new ColumnarArray(testVector, 0, 10)
    val arrayCopy = array.copy()

    (0 until 10).foreach { i =>
      assert(array.get(i, LongType) === i)
      assert(arrayCopy.get(i, LongType) === i)
    }
  }

  testVectors("timestamp", 10, TimestampType) { testVector =>
    (0 until 10).foreach { i =>
      testVector.appendLong(i)
    }

    val array = new ColumnarArray(testVector, 0, 10)
    val arrayCopy = array.copy()

    (0 until 10).foreach { i =>
      assert(array.get(i, TimestampType) === i)
      assert(arrayCopy.get(i, TimestampType) === i)
    }
  }

  testVectors("float", 10, FloatType) { testVector =>
    (0 until 10).foreach { i =>
      testVector.appendFloat(i.toFloat)
    }

    val array = new ColumnarArray(testVector, 0, 10)
    val arrayCopy = array.copy()

    (0 until 10).foreach { i =>
      assert(array.get(i, FloatType) === i.toFloat)
      assert(arrayCopy.get(i, FloatType) === i.toFloat)
    }
  }

  testVectors("double", 10, DoubleType) { testVector =>
    (0 until 10).foreach { i =>
      testVector.appendDouble(i.toDouble)
    }

    val array = new ColumnarArray(testVector, 0, 10)
    val arrayCopy = array.copy()

    (0 until 10).foreach { i =>
      assert(array.get(i, DoubleType) === i.toDouble)
      assert(arrayCopy.get(i, DoubleType) === i.toDouble)
    }
  }

  testVectors("string", 10, StringType) { testVector =>
    (0 until 10).map { i =>
      val utf8 = s"str$i".getBytes("utf8")
      testVector.appendByteArray(utf8, 0, utf8.length)
    }

    val array = new ColumnarArray(testVector, 0, 10)
    val arrayCopy = array.copy()

    (0 until 10).foreach { i =>
      assert(array.get(i, StringType) === UTF8String.fromString(s"str$i"))
      assert(arrayCopy.get(i, StringType) === UTF8String.fromString(s"str$i"))
    }
  }

  testVectors("binary", 10, BinaryType) { testVector =>
    (0 until 10).map { i =>
      val utf8 = s"str$i".getBytes("utf8")
      testVector.appendByteArray(utf8, 0, utf8.length)
    }

    val array = new ColumnarArray(testVector, 0, 10)
    val arrayCopy = array.copy()

    (0 until 10).foreach { i =>
      val utf8 = s"str$i".getBytes("utf8")
      assert(array.get(i, BinaryType) === utf8)
      assert(arrayCopy.get(i, BinaryType) === utf8)
    }
  }

  DataTypeTestUtils.yearMonthIntervalTypes.foreach {
    dt =>
      testVectors(dt.typeName,
        10,
        dt) { testVector =>
        (0 until 10).foreach { i =>
          testVector.appendInt(i)
        }

        val array = new ColumnarArray(testVector, 0, 10)
        val arrayCopy = array.copy()

        (0 until 10).foreach { i =>
          assert(array.get(i, dt) === i)
          assert(arrayCopy.get(i, dt) === i)
        }
      }
  }

  DataTypeTestUtils.dayTimeIntervalTypes.foreach {
    dt =>
      testVectors(dt.typeName,
        10,
        dt) { testVector =>
        (0 until 10).foreach { i =>
          testVector.appendLong(i)
        }

        val array = new ColumnarArray(testVector, 0, 10)
        val arrayCopy = array.copy()

        (0 until 10).foreach { i =>
          assert(array.get(i, dt) === i)
          assert(arrayCopy.get(i, dt) === i)
        }
      }
  }

  testVectors("mutable ColumnarRow", 10, IntegerType) { testVector =>
    val mutableRow = new MutableColumnarRow(Array(testVector))
    (0 until 10).foreach { i =>
      mutableRow.rowId = i
      mutableRow.setInt(0, 10 - i)
    }
    (0 until 10).foreach { i =>
      mutableRow.rowId = i
      assert(mutableRow.getInt(0) === (10 - i))
    }
  }

  val arrayType: ArrayType = ArrayType(IntegerType, containsNull = true)
  testVectors("array", 10, arrayType) { testVector =>

    val data = testVector.arrayData()
    var i = 0
    while (i < 6) {
      data.putInt(i, i)
      i += 1
    }

    // Populate it with arrays [0], [1, 2], [], [3, 4, 5]
    testVector.putArray(0, 0, 1)
    testVector.putArray(1, 1, 2)
    testVector.putArray(2, 3, 0)
    testVector.putArray(3, 3, 3)

    assert(testVector.getArray(0).toIntArray() === Array(0))
    assert(testVector.getArray(1).toIntArray() === Array(1, 2))
    assert(testVector.getArray(2).toIntArray() === Array.empty[Int])
    assert(testVector.getArray(3).toIntArray() === Array(3, 4, 5))
  }

  testVectors("SPARK-35898: array append", 1, arrayType) { testVector =>
    // Populate it with arrays [0], [1, 2], [], [3, 4, 5]
    val data = testVector.arrayData()
    testVector.appendArray(1)
    data.appendInt(0)
    testVector.appendArray(2)
    data.appendInt(1)
    data.appendInt(2)
    testVector.appendArray(0)
    testVector.appendArray(3)
    data.appendInt(3)
    data.appendInt(4)
    data.appendInt(5)

    assert(testVector.getArray(0).toIntArray === Array(0))
    assert(testVector.getArray(1).toIntArray === Array(1, 2))
    assert(testVector.getArray(2).toIntArray === Array.empty[Int])
    assert(testVector.getArray(3).toIntArray === Array(3, 4, 5))
  }

  val mapType: MapType = MapType(IntegerType, StringType)
  testVectors("SPARK-35898: map", 5, mapType) { testVector =>
    val keys = testVector.getChild(0)
    val values = testVector.getChild(1)
    var i = 0
    while (i < 6) {
      keys.appendInt(i)
      val utf8 = s"str$i".getBytes("utf8")
      values.appendByteArray(utf8, 0, utf8.length)
      i += 1
    }

    testVector.putArray(0, 0, 1)
    testVector.putArray(1, 1, 2)
    testVector.putArray(2, 3, 0)
    testVector.putArray(3, 3, 3)

    assert(testVector.getMap(0).keyArray().toIntArray === Array(0))
    assert(testVector.getMap(0).valueArray().toArray[UTF8String](StringType) ===
      Array(UTF8String.fromString(s"str0")))
    assert(testVector.getMap(1).keyArray().toIntArray === Array(1, 2))
    assert(testVector.getMap(1).valueArray().toArray[UTF8String](StringType) ===
      (1 to 2).map(i => UTF8String.fromString(s"str$i")).toArray)
    assert(testVector.getMap(2).keyArray().toIntArray === Array.empty[Int])
    assert(testVector.getMap(2).valueArray().toArray[UTF8String](StringType) ===
      Array.empty[UTF8String])
    assert(testVector.getMap(3).keyArray().toIntArray === Array(3, 4, 5))
    assert(testVector.getMap(3).valueArray().toArray[UTF8String](StringType) ===
      (3 to 5).map(i => UTF8String.fromString(s"str$i")).toArray)
  }

  testVectors("SPARK-35898: map append", 1, mapType) { testVector =>
    val keys = testVector.getChild(0)
    val values = testVector.getChild(1)
    def appendPair(i: Int): Unit = {
      keys.appendInt(i)
      val utf8 = s"str$i".getBytes("utf8")
      values.appendByteArray(utf8, 0, utf8.length)
    }

    // Populate it with the maps [0 -> str0], [1 -> str1, 2 -> str2], [],
    // [3 -> str3, 4 -> str4, 5 -> str5]
    testVector.appendArray(1)
    appendPair(0)
    testVector.appendArray(2)
    appendPair(1)
    appendPair(2)
    testVector.appendArray(0)
    testVector.appendArray(3)
    appendPair(3)
    appendPair(4)
    appendPair(5)

    assert(testVector.getMap(0).keyArray().toIntArray === Array(0))
    assert(testVector.getMap(0).valueArray().toArray[UTF8String](StringType) ===
      Array(UTF8String.fromString(s"str0")))
    assert(testVector.getMap(1).keyArray().toIntArray === Array(1, 2))
    assert(testVector.getMap(1).valueArray().toArray[UTF8String](StringType) ===
      (1 to 2).map(i => UTF8String.fromString(s"str$i")).toArray)
    assert(testVector.getMap(2).keyArray().toIntArray === Array.empty[Int])
    assert(testVector.getMap(2).valueArray().toArray[UTF8String](StringType) ===
      Array.empty[UTF8String])
    assert(testVector.getMap(3).keyArray().toIntArray === Array(3, 4, 5))
    assert(testVector.getMap(3).valueArray().toArray[UTF8String](StringType) ===
      (3 to 5).map(i => UTF8String.fromString(s"str$i")).toArray)
  }

  val structType: StructType = new StructType().add("int", IntegerType).add("double", DoubleType)
  testVectors("struct", 10, structType) { testVector =>
    val c1 = testVector.getChild(0)
    val c2 = testVector.getChild(1)
    c1.putInt(0, 123)
    c2.putDouble(0, 3.45)
    c1.putInt(1, 456)
    c2.putDouble(1, 5.67)

    assert(testVector.getStruct(0).get(0, IntegerType) === 123)
    assert(testVector.getStruct(0).get(1, DoubleType) === 3.45)
    assert(testVector.getStruct(1).get(0, IntegerType) === 456)
    assert(testVector.getStruct(1).get(1, DoubleType) === 5.67)
  }

  test("[SPARK-22092] off-heap column vector reallocation corrupts array data") {
    withVector(new OffHeapColumnVector(8, arrayType)) { testVector =>
      val data = testVector.arrayData()
      (0 until 8).foreach(i => data.putInt(i, i))
      (0 until 8).foreach(i => testVector.putArray(i, i, 1))

      // Increase vector's capacity and reallocate the data to new bigger buffers.
      testVector.reserve(16)

      // Check that none of the values got lost/overwritten.
      (0 until 8).foreach { i =>
        assert(testVector.getArray(i).toIntArray() === Array(i))
      }
    }
  }

  test("[SPARK-22092] off-heap column vector reallocation corrupts struct nullability") {
    withVector(new OffHeapColumnVector(8, structType)) { testVector =>
      (0 until 8).foreach(i => if (i % 2 == 0) testVector.putNull(i) else testVector.putNotNull(i))
      testVector.reserve(16)
      (0 until 8).foreach(i => assert(testVector.isNullAt(i) == (i % 2 == 0)))
    }
  }

  test("CachedBatch boolean Apis") {
    val dataType = BooleanType
    val columnBuilder = ColumnBuilderHelper(dataType, 1024, "col", true)
    val row = new SpecificInternalRow(Array(dataType))

    row.setNullAt(0)
    columnBuilder.appendFrom(row, 0)
    for (i <- 1 until 16) {
      row.setBoolean(0, i % 2 == 0)
      columnBuilder.appendFrom(row, 0)
    }

    withVectors(16, dataType) { testVector =>
      val columnAccessor = ColumnAccessor(dataType, columnBuilder.build)
      ColumnAccessor.decompress(columnAccessor, testVector, 16)

      assert(testVector.isNullAt(0))
      for (i <- 1 until 16) {
        assert(testVector.isNullAt(i) == false)
        assert(testVector.getBoolean(i) == (i % 2 == 0))
      }
    }
  }

  test("CachedBatch byte Apis") {
    val dataType = ByteType
    val columnBuilder = ColumnBuilderHelper(dataType, 1024, "col", true)
    val row = new SpecificInternalRow(Array(dataType))

    row.setNullAt(0)
    columnBuilder.appendFrom(row, 0)
    for (i <- 1 until 16) {
      row.setByte(0, i.toByte)
      columnBuilder.appendFrom(row, 0)
    }

    withVectors(16, dataType) { testVector =>
      val columnAccessor = ColumnAccessor(dataType, columnBuilder.build)
      ColumnAccessor.decompress(columnAccessor, testVector, 16)

      assert(testVector.isNullAt(0))
      for (i <- 1 until 16) {
        assert(testVector.isNullAt(i) == false)
        assert(testVector.getByte(i) == i)
      }
    }
  }

  test("CachedBatch short Apis") {
    val dataType = ShortType
    val columnBuilder = ColumnBuilderHelper(dataType, 1024, "col", true)
    val row = new SpecificInternalRow(Array(dataType))

    row.setNullAt(0)
    columnBuilder.appendFrom(row, 0)
    for (i <- 1 until 16) {
      row.setShort(0, i.toShort)
      columnBuilder.appendFrom(row, 0)
    }

    withVectors(16, dataType) { testVector =>
      val columnAccessor = ColumnAccessor(dataType, columnBuilder.build)
      ColumnAccessor.decompress(columnAccessor, testVector, 16)

      assert(testVector.isNullAt(0))
      for (i <- 1 until 16) {
        assert(testVector.isNullAt(i) == false)
        assert(testVector.getShort(i) == i)
      }
    }
  }

  test("CachedBatch int Apis") {
    val dataType = IntegerType
    val columnBuilder = ColumnBuilderHelper(dataType, 1024, "col", true)
    val row = new SpecificInternalRow(Array(dataType))

    row.setNullAt(0)
    columnBuilder.appendFrom(row, 0)
    for (i <- 1 until 16) {
      row.setInt(0, i)
      columnBuilder.appendFrom(row, 0)
    }

    withVectors(16, dataType) { testVector =>
      val columnAccessor = ColumnAccessor(dataType, columnBuilder.build)
      ColumnAccessor.decompress(columnAccessor, testVector, 16)

      assert(testVector.isNullAt(0))
      for (i <- 1 until 16) {
        assert(testVector.isNullAt(i) == false)
        assert(testVector.getInt(i) == i)
      }
    }
  }

  test("CachedBatch long Apis") {
    val dataType = LongType
    val columnBuilder = ColumnBuilderHelper(dataType, 1024, "col", true)
    val row = new SpecificInternalRow(Array(dataType))

    row.setNullAt(0)
    columnBuilder.appendFrom(row, 0)
    for (i <- 1 until 16) {
      row.setLong(0, i.toLong)
      columnBuilder.appendFrom(row, 0)
    }

    withVectors(16, dataType) { testVector =>
      val columnAccessor = ColumnAccessor(dataType, columnBuilder.build)
      ColumnAccessor.decompress(columnAccessor, testVector, 16)

      assert(testVector.isNullAt(0))
      for (i <- 1 until 16) {
        assert(testVector.isNullAt(i) == false)
        assert(testVector.getLong(i) == i.toLong)
      }
    }
  }

  test("CachedBatch float Apis") {
    val dataType = FloatType
    val columnBuilder = ColumnBuilderHelper(dataType, 1024, "col", true)
    val row = new SpecificInternalRow(Array(dataType))

    row.setNullAt(0)
    columnBuilder.appendFrom(row, 0)
    for (i <- 1 until 16) {
      row.setFloat(0, i.toFloat)
      columnBuilder.appendFrom(row, 0)
    }

    withVectors(16, dataType) { testVector =>
      val columnAccessor = ColumnAccessor(dataType, columnBuilder.build)
      ColumnAccessor.decompress(columnAccessor, testVector, 16)

      assert(testVector.isNullAt(0))
      for (i <- 1 until 16) {
        assert(testVector.isNullAt(i) == false)
        assert(testVector.getFloat(i) == i.toFloat)
      }
    }
  }

  test("CachedBatch double Apis") {
    val dataType = DoubleType
    val columnBuilder = ColumnBuilderHelper(dataType, 1024, "col", true)
    val row = new SpecificInternalRow(Array(dataType))

    row.setNullAt(0)
    columnBuilder.appendFrom(row, 0)
    for (i <- 1 until 16) {
      row.setDouble(0, i.toDouble)
      columnBuilder.appendFrom(row, 0)
    }

    withVectors(16, dataType) { testVector =>
      val columnAccessor = ColumnAccessor(dataType, columnBuilder.build)
      ColumnAccessor.decompress(columnAccessor, testVector, 16)

      assert(testVector.isNullAt(0))
      for (i <- 1 until 16) {
        assert(testVector.isNullAt(i) == false)
        assert(testVector.getDouble(i) == i.toDouble)
      }
    }
  }

  DataTypeTestUtils.yearMonthIntervalTypes.foreach { dt =>
    val structType = new StructType().add(dt.typeName, dt)
    testVectors("ColumnarRow " + dt.typeName, 10, structType) { v =>
      val column = v.getChild(0)
      (0 until 10).foreach { i =>
        column.putInt(i, i)
      }
      (0 until 10).foreach { i =>
        val row = v.getStruct(i)
        val rowCopy = row.copy()
        assert(row.get(0, dt) === i)
        assert(rowCopy.get(0, dt) === i)
      }
    }
  }
  DataTypeTestUtils.dayTimeIntervalTypes.foreach { dt =>
    val structType = new StructType().add(dt.typeName, dt)
    testVectors("ColumnarRow " + dt.typeName, 10, structType) { v =>
      val column = v.getChild(0)
      (0 until 10).foreach { i =>
        column.putLong(i, i)
      }
      (0 until 10).foreach { i =>
        val row = v.getStruct(i)
        val rowCopy = row.copy()
        assert(row.get(0, dt) === i)
        assert(rowCopy.get(0, dt) === i)
      }
    }
  }
}

