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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.execution.columnar.{ColumnAccessor, ColumnDictionary}
import org.apache.spark.sql.execution.columnar.compression.ColumnBuilderHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarArray
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._

class ColumnVectorSuite extends SparkFunSuite with SQLHelper {
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

  testVectors("timestamp_ntz", 10, TimestampNTZType) { testVector =>
    (0 until 10).foreach { i =>
      testVector.appendLong(i)
    }

    val array = new ColumnarArray(testVector, 0, 10)
    val arrayCopy = array.copy()

    (0 until 10).foreach { i =>
      assert(array.get(i, TimestampNTZType) === i)
      assert(arrayCopy.get(i, TimestampNTZType) === i)
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

  testVectors("mutable ColumnarRow with TimestampNTZType", 10, TimestampNTZType) { testVector =>
    val mutableRow = new MutableColumnarRow(Array(testVector))
    (0 until 10).foreach { i =>
      mutableRow.rowId = i
      mutableRow.setLong(0, 10 - i)
    }
    (0 until 10).foreach { i =>
      mutableRow.rowId = i
      assert(mutableRow.get(0, TimestampNTZType) === (10 - i))
      assert(mutableRow.copy().get(0, TimestampNTZType) === (10 - i))
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
    .add("ts", TimestampNTZType)
  testVectors("struct", 10, structType) { testVector =>
    val c1 = testVector.getChild(0)
    val c2 = testVector.getChild(1)
    val c3 = testVector.getChild(2)
    c1.putInt(0, 123)
    c2.putDouble(0, 3.45)
    c3.putLong(0, 1000L)
    c1.putInt(1, 456)
    c2.putDouble(1, 5.67)
    c3.putLong(1, 2000L)

    assert(testVector.getStruct(0).get(0, IntegerType) === 123)
    assert(testVector.getStruct(0).get(1, DoubleType) === 3.45)
    assert(testVector.getStruct(0).get(2, TimestampNTZType) === 1000L)
    assert(testVector.getStruct(1).get(0, IntegerType) === 456)
    assert(testVector.getStruct(1).get(1, DoubleType) === 5.67)
    assert(testVector.getStruct(1).get(2, TimestampNTZType) === 2000L)
  }

  testVectors("SPARK-44805: getInts with dictionary", 3, IntegerType) { testVector =>
    val dict = new ColumnDictionary(Array[Int](7, 8, 9))
    testVector.setDictionary(dict)
    testVector.reserveDictionaryIds(3)
    testVector.getDictionaryIds.putInt(0, 0)
    testVector.getDictionaryIds.putInt(1, 1)
    testVector.getDictionaryIds.putInt(2, 2)

    assert(testVector.getInts(0, 3)(0) == 7)
    assert(testVector.getInts(0, 3)(1) == 8)
    assert(testVector.getInts(0, 3)(2) == 9)
  }

  testVectors("SPARK-44805: getShorts with dictionary", 3, ShortType) { testVector =>
    val dict = new ColumnDictionary(Array[Int](7, 8, 9))
    testVector.setDictionary(dict)
    testVector.reserveDictionaryIds(3)
    testVector.getDictionaryIds.putInt(0, 0)
    testVector.getDictionaryIds.putInt(1, 1)
    testVector.getDictionaryIds.putInt(2, 2)

    assert(testVector.getShorts(0, 3)(0) == 7)
    assert(testVector.getShorts(0, 3)(1) == 8)
    assert(testVector.getShorts(0, 3)(2) == 9)
  }

  testVectors("SPARK-44805: getBytes with dictionary", 3, ByteType) { testVector =>
    val dict = new ColumnDictionary(Array[Int](7, 8, 9))
    testVector.setDictionary(dict)
    testVector.reserveDictionaryIds(3)
    testVector.getDictionaryIds.putInt(0, 0)
    testVector.getDictionaryIds.putInt(1, 1)
    testVector.getDictionaryIds.putInt(2, 2)

    assert(testVector.getBytes(0, 3)(0) == 7)
    assert(testVector.getBytes(0, 3)(1) == 8)
    assert(testVector.getBytes(0, 3)(2) == 9)
  }

  testVectors("SPARK-44805: getLongs with dictionary", 3, LongType) { testVector =>
    val dict = new ColumnDictionary(Array[Long](2147483648L, 2147483649L, 2147483650L))
    testVector.setDictionary(dict)
    testVector.reserveDictionaryIds(3)
    testVector.getDictionaryIds.putInt(0, 0)
    testVector.getDictionaryIds.putInt(1, 1)
    testVector.getDictionaryIds.putInt(2, 2)

    assert(testVector.getLongs(0, 3)(0) == 2147483648L)
    assert(testVector.getLongs(0, 3)(1) == 2147483649L)
    assert(testVector.getLongs(0, 3)(2) == 2147483650L)
  }

  testVectors("SPARK-44805: getFloats with dictionary", 3, FloatType) { testVector =>
    val dict = new ColumnDictionary(Array[Float](0.1f, 0.2f, 0.3f))
    testVector.setDictionary(dict)
    testVector.reserveDictionaryIds(3)
    testVector.getDictionaryIds.putInt(0, 0)
    testVector.getDictionaryIds.putInt(1, 1)
    testVector.getDictionaryIds.putInt(2, 2)

    assert(testVector.getFloats(0, 3)(0) == 0.1f)
    assert(testVector.getFloats(0, 3)(1) == 0.2f)
    assert(testVector.getFloats(0, 3)(2) == 0.3f)
  }

  testVectors("SPARK-44805: getDoubles with dictionary", 3, DoubleType) { testVector =>
    val dict = new ColumnDictionary(Array[Double](1342.17727d, 1342.17728d, 1342.17729d))
    testVector.setDictionary(dict)
    testVector.reserveDictionaryIds(3)
    testVector.getDictionaryIds.putInt(0, 0)
    testVector.getDictionaryIds.putInt(1, 1)
    testVector.getDictionaryIds.putInt(2, 2)

    assert(testVector.getDoubles(0, 3)(0) == 1342.17727d)
    assert(testVector.getDoubles(0, 3)(1) == 1342.17728d)
    assert(testVector.getDoubles(0, 3)(2) == 1342.17729d)
  }

  def check(expected: Seq[Any], testVector: WritableColumnVector): Unit = {
    expected.zipWithIndex.foreach {
      case (v: Integer, idx) =>
        assert(testVector.getInt(idx) == v)
        assert(testVector.getInts(0, testVector.capacity)(idx) == v)
      case (v: Short, idx) =>
        assert(testVector.getShort(idx) == v)
        assert(testVector.getShorts(0, testVector.capacity)(idx) == v)
      case (v: Byte, idx) =>
        assert(testVector.getByte(idx) == v)
        assert(testVector.getBytes(0, testVector.capacity)(idx) == v)
      case (v: Long, idx) =>
        assert(testVector.getLong(idx) == v)
        assert(testVector.getLongs(0, testVector.capacity)(idx) == v)
      case (v: Float, idx) =>
        assert(testVector.getFloat(idx) == v)
        assert(testVector.getFloats(0, testVector.capacity)(idx) == v)
      case (v: Double, idx) =>
        assert(testVector.getDouble(idx) == v)
        assert(testVector.getDoubles(0, testVector.capacity)(idx) == v)
      case (null, idx) => testVector.isNullAt(idx)
      case (_, idx) => assert(false, s"Unexpected value at $idx")
    }

    // Verify ColumnarArray.copy() works as expected
    val arr = new ColumnarArray(testVector, 0, testVector.capacity)
    assert(arr.toSeq(testVector.dataType) == expected)
    assert(arr.copy().toSeq(testVector.dataType) == expected)

    if (expected.nonEmpty) {
      val withOffset = new ColumnarArray(testVector, 1, testVector.capacity - 1)
      assert(withOffset.toSeq(testVector.dataType) == expected.tail)
      assert(withOffset.copy().toSeq(testVector.dataType) == expected.tail)
    }
  }

  testVectors("getInts with dictionary and nulls", 3, IntegerType) { testVector =>
    // Validate without dictionary
    val expected = Seq(1, null, 3)
    expected.foreach {
      case i: Integer => testVector.appendInt(i)
      case _ => testVector.appendNull()
    }
    check(expected, testVector)

    // Validate with dictionary
    val expectedDictionary = Seq(7, null, 9)
    val dictArray = (Seq(-1, -1) ++ expectedDictionary.map {
      case i: Integer => i.toInt
      case _ => -1
    }).toArray
    val dict = new ColumnDictionary(dictArray)
    testVector.setDictionary(dict)
    testVector.reserveDictionaryIds(3)
    testVector.getDictionaryIds.putInt(0, 2)
    testVector.getDictionaryIds.putInt(1, -1) // This is a null, so the entry should be ignored
    testVector.getDictionaryIds.putInt(2, 4)
    check(expectedDictionary, testVector)
  }

  testVectors("getShorts with dictionary and nulls", 3, ShortType) { testVector =>
    // Validate without dictionary
    val expected = Seq(1.toShort, null, 3.toShort)
    expected.foreach {
      case i: Short => testVector.appendShort(i)
      case _ => testVector.appendNull()
    }
    check(expected, testVector)

    // Validate with dictionary
    val expectedDictionary = Seq(7.toShort, null, 9.toShort)
    val dictArray = (Seq(-1, -1) ++ expectedDictionary.map {
      case i: Short => i.toInt
      case _ => -1
    }).toArray
    val dict = new ColumnDictionary(dictArray)
    testVector.setDictionary(dict)
    testVector.reserveDictionaryIds(3)
    testVector.getDictionaryIds.putInt(0, 2)
    testVector.getDictionaryIds.putInt(1, -1) // This is a null, so the entry should be ignored
    testVector.getDictionaryIds.putInt(2, 4)
    check(expectedDictionary, testVector)
  }

  testVectors("getBytes with dictionary and nulls", 3, ByteType) { testVector =>
    // Validate without dictionary
    val expected = Seq(1.toByte, null, 3.toByte)
    expected.foreach {
      case i: Byte => testVector.appendByte(i)
      case _ => testVector.appendNull()
    }
    check(expected, testVector)

    // Validate with dictionary
    val expectedDictionary = Seq(7.toByte, null, 9.toByte)
    val dictArray = (Seq(-1, -1) ++ expectedDictionary.map {
      case i: Byte => i.toInt
      case _ => -1
    }).toArray
    val dict = new ColumnDictionary(dictArray)
    testVector.setDictionary(dict)
    testVector.reserveDictionaryIds(3)
    testVector.getDictionaryIds.putInt(0, 2)
    testVector.getDictionaryIds.putInt(1, -1) // This is a null, so the entry should be ignored
    testVector.getDictionaryIds.putInt(2, 4)
    check(expectedDictionary, testVector)
  }

  testVectors("getLongs with dictionary and nulls", 3, LongType) { testVector =>
    // Validate without dictionary
    val expected = Seq(2147483L, null, 2147485L)
    expected.foreach {
      case i: Long => testVector.appendLong(i)
      case _ => testVector.appendNull()
    }
    check(expected, testVector)

    // Validate with dictionary
    val expectedDictionary = Seq(2147483648L, null, 2147483650L)
    val dictArray = (Seq(-1L, -1L) ++ expectedDictionary.map {
      case i: Long => i
      case _ => -1L
    }).toArray
    val dict = new ColumnDictionary(dictArray)
    testVector.setDictionary(dict)
    testVector.reserveDictionaryIds(3)
    testVector.getDictionaryIds.putInt(0, 2)
    testVector.getDictionaryIds.putInt(1, -1) // This is a null, so the entry should be ignored
    testVector.getDictionaryIds.putInt(2, 4)
    check(expectedDictionary, testVector)
  }

  testVectors("getFloats with dictionary and nulls", 3, FloatType) { testVector =>
    // Validate without dictionary
    val expected = Seq(1.1f, null, 3.3f)
    expected.foreach {
      case i: Float => testVector.appendFloat(i)
      case _ => testVector.appendNull()
    }
    check(expected, testVector)

    // Validate with dictionary
    val expectedDictionary = Seq(0.1f, null, 0.3f)
    val dictArray = (Seq(-1f, -1f) ++ expectedDictionary.map {
      case i: Float => i
      case _ => -1f
    }).toArray
    val dict = new ColumnDictionary(dictArray)
    testVector.setDictionary(dict)
    testVector.reserveDictionaryIds(3)
    testVector.getDictionaryIds.putInt(0, 2)
    testVector.getDictionaryIds.putInt(1, -1) // This is a null, so the entry should be ignored
    testVector.getDictionaryIds.putInt(2, 4)
    check(expectedDictionary, testVector)
  }

  testVectors("getDoubles with dictionary and nulls", 3, DoubleType) { testVector =>
    // Validate without dictionary
    val expected = Seq(1.1d, null, 3.3d)
    expected.foreach {
      case i: Double => testVector.appendDouble(i)
      case _ => testVector.appendNull()
    }
    check(expected, testVector)

    // Validate with dictionary
    val expectedDictionary = Seq(1342.17727d, null, 1342.17729d)
    val dictArray = (Seq(-1d, -1d) ++ expectedDictionary.map {
      case i: Double => i
      case _ => -1d
    }).toArray
    val dict = new ColumnDictionary(dictArray)
    testVector.setDictionary(dict)
    testVector.reserveDictionaryIds(3)
    testVector.getDictionaryIds.putInt(0, 2)
    testVector.getDictionaryIds.putInt(1, -1) // This is a null, so the entry should be ignored
    testVector.getDictionaryIds.putInt(2, 4)
    check(expectedDictionary, testVector)
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
    val row = new SpecificInternalRow(Array(dataType).toImmutableArraySeq)

    row.setNullAt(0)
    columnBuilder.appendFrom(row, 0)
    for (i <- 1 until 16) {
      row.setBoolean(0, i % 2 == 0)
      columnBuilder.appendFrom(row, 0)
    }

    withVectors(16, dataType) { testVector =>
      val columnAccessor = ColumnAccessor(dataType, columnBuilder.build())
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
    val row = new SpecificInternalRow(Array(dataType).toImmutableArraySeq)

    row.setNullAt(0)
    columnBuilder.appendFrom(row, 0)
    for (i <- 1 until 16) {
      row.setByte(0, i.toByte)
      columnBuilder.appendFrom(row, 0)
    }

    withVectors(16, dataType) { testVector =>
      val columnAccessor = ColumnAccessor(dataType, columnBuilder.build())
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
    val row = new SpecificInternalRow(Array(dataType).toImmutableArraySeq)

    row.setNullAt(0)
    columnBuilder.appendFrom(row, 0)
    for (i <- 1 until 16) {
      row.setShort(0, i.toShort)
      columnBuilder.appendFrom(row, 0)
    }

    withVectors(16, dataType) { testVector =>
      val columnAccessor = ColumnAccessor(dataType, columnBuilder.build())
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
    val row = new SpecificInternalRow(Array(dataType).toImmutableArraySeq)

    row.setNullAt(0)
    columnBuilder.appendFrom(row, 0)
    for (i <- 1 until 16) {
      row.setInt(0, i)
      columnBuilder.appendFrom(row, 0)
    }

    withVectors(16, dataType) { testVector =>
      val columnAccessor = ColumnAccessor(dataType, columnBuilder.build())
      ColumnAccessor.decompress(columnAccessor, testVector, 16)

      assert(testVector.isNullAt(0))
      for (i <- 1 until 16) {
        assert(testVector.isNullAt(i) == false)
        assert(testVector.getInt(i) == i)
      }
    }
  }

  test("CachedBatch long Apis") {
    Seq(LongType, TimestampType, TimestampNTZType).foreach { dataType =>
      val columnBuilder = ColumnBuilderHelper(dataType, 1024, "col", true)
      val row = new SpecificInternalRow(Array(dataType).toImmutableArraySeq)

      row.setNullAt(0)
      columnBuilder.appendFrom(row, 0)
      for (i <- 1 until 16) {
        row.setLong(0, i.toLong)
        columnBuilder.appendFrom(row, 0)
      }

      withVectors(16, dataType) { testVector =>
        val columnAccessor = ColumnAccessor(dataType, columnBuilder.build())
        ColumnAccessor.decompress(columnAccessor, testVector, 16)

        assert(testVector.isNullAt(0))
        for (i <- 1 until 16) {
          assert(testVector.isNullAt(i) == false)
          assert(testVector.getLong(i) == i.toLong)
        }
      }
    }
  }

  test("CachedBatch float Apis") {
    val dataType = FloatType
    val columnBuilder = ColumnBuilderHelper(dataType, 1024, "col", true)
    val row = new SpecificInternalRow(Array(dataType).toImmutableArraySeq)

    row.setNullAt(0)
    columnBuilder.appendFrom(row, 0)
    for (i <- 1 until 16) {
      row.setFloat(0, i.toFloat)
      columnBuilder.appendFrom(row, 0)
    }

    withVectors(16, dataType) { testVector =>
      val columnAccessor = ColumnAccessor(dataType, columnBuilder.build())
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
    val row = new SpecificInternalRow(Array(dataType).toImmutableArraySeq)

    row.setNullAt(0)
    columnBuilder.appendFrom(row, 0)
    for (i <- 1 until 16) {
      row.setDouble(0, i.toDouble)
      columnBuilder.appendFrom(row, 0)
    }

    withVectors(16, dataType) { testVector =>
      val columnAccessor = ColumnAccessor(dataType, columnBuilder.build())
      ColumnAccessor.decompress(columnAccessor, testVector, 16)

      assert(testVector.isNullAt(0))
      for (i <- 1 until 16) {
        assert(testVector.isNullAt(i) == false)
        assert(testVector.getDouble(i) == i.toDouble)
      }
    }
  }

  test("SPARK-44239: Test column vector reserve policy") {
    withSQLConf(
      SQLConf.VECTORIZED_HUGE_VECTOR_THRESHOLD.key -> "300",
      SQLConf.VECTORIZED_HUGE_VECTOR_RESERVE_RATIO.key -> "1.2") {
      val dataType = ByteType

      Array(new OnHeapColumnVector(80, dataType),
        new OffHeapColumnVector(80, dataType)).foreach { vector =>
        try {
          // The new capacity of small vector = request capacity * 2 and will not be reset
          vector.appendBytes(100, 0)
          assert(vector.capacity == 200)
          vector.reset()
          assert(vector.capacity == 200)

          // The new capacity of huge vector = (request capacity - HUGE_VECTOR_THRESHOLD) * 1.2 +
          // HUGE_VECTOR_THRESHOLD * 2 = 300 * 1.2 and will be reset.
          vector.appendBytes(300, 0)
          assert(vector.capacity == 360)
          vector.reset()
          assert(vector.capacity == 80)
        } finally {
          vector.close()
        }
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

