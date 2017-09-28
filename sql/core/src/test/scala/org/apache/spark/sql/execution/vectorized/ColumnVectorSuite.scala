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
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class ColumnVectorSuite extends SparkFunSuite with BeforeAndAfterEach {
  private def withVector(
      vector: ColumnVector)(
      block: ColumnVector => Unit): Unit = {
    try block(vector) finally vector.close()
  }

  private def testVectors(
      name: String,
      size: Int,
      dt: DataType)(
      block: ColumnVector => Unit): Unit = {
    test(name) {
      withVector(new OnHeapColumnVector(size, dt))(block)
      withVector(new OffHeapColumnVector(size, dt))(block)
    }
  }

  testVectors("boolean", 10, BooleanType) { testVector =>
    (0 until 10).foreach { i =>
      testVector.appendBoolean(i % 2 == 0)
    }

    val array = new ColumnVector.Array(testVector)

    (0 until 10).foreach { i =>
      assert(array.getBoolean(i) === (i % 2 == 0))
    }
  }

  testVectors("byte", 10, ByteType) { testVector =>
    (0 until 10).foreach { i =>
      testVector.appendByte(i.toByte)
    }

    val array = new ColumnVector.Array(testVector)

    (0 until 10).foreach { i =>
      assert(array.getByte(i) === (i.toByte))
    }
  }

  testVectors("short", 10, ShortType) { testVector =>
    (0 until 10).foreach { i =>
      testVector.appendShort(i.toShort)
    }

    val array = new ColumnVector.Array(testVector)

    (0 until 10).foreach { i =>
      assert(array.getShort(i) === (i.toShort))
    }
  }

  testVectors("int", 10, IntegerType) { testVector =>
    (0 until 10).foreach { i =>
      testVector.appendInt(i)
    }

    val array = new ColumnVector.Array(testVector)

    (0 until 10).foreach { i =>
      assert(array.getInt(i) === i)
    }
  }

  testVectors("long", 10, LongType) { testVector =>
    (0 until 10).foreach { i =>
      testVector.appendLong(i)
    }

    val array = new ColumnVector.Array(testVector)

    (0 until 10).foreach { i =>
      assert(array.getLong(i) === i)
    }
  }

  testVectors("float", 10, FloatType) { testVector =>
    (0 until 10).foreach { i =>
      testVector.appendFloat(i.toFloat)
    }

    val array = new ColumnVector.Array(testVector)

    (0 until 10).foreach { i =>
      assert(array.getFloat(i) === i.toFloat)
    }
  }

  testVectors("double", 10, DoubleType) { testVector =>
    (0 until 10).foreach { i =>
      testVector.appendDouble(i.toDouble)
    }

    val array = new ColumnVector.Array(testVector)

    (0 until 10).foreach { i =>
      assert(array.getDouble(i) === i.toDouble)
    }
  }

  testVectors("string", 10, StringType) { testVector =>
    (0 until 10).map { i =>
      val utf8 = s"str$i".getBytes("utf8")
      testVector.appendByteArray(utf8, 0, utf8.length)
    }

    val array = new ColumnVector.Array(testVector)

    (0 until 10).foreach { i =>
      assert(array.getUTF8String(i) === UTF8String.fromString(s"str$i"))
    }
  }

  testVectors("binary", 10, BinaryType) { testVector =>
    (0 until 10).map { i =>
      val utf8 = s"str$i".getBytes("utf8")
      testVector.appendByteArray(utf8, 0, utf8.length)
    }

    val array = new ColumnVector.Array(testVector)

    (0 until 10).foreach { i =>
      val utf8 = s"str$i".getBytes("utf8")
      assert(array.getBinary(i) === utf8)
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

    val array = new ColumnVector.Array(testVector)

    assert(array.getArray(0).toIntArray() === Array(0))
    assert(array.getArray(1).asInstanceOf[ArrayData].toIntArray() === Array(1, 2))
    assert(array.getArray(2).asInstanceOf[ArrayData].toIntArray() === Array.empty[Int])
    assert(array.getArray(3).asInstanceOf[ArrayData].toIntArray() === Array(3, 4, 5))
  }

  val structType: StructType = new StructType().add("int", IntegerType).add("double", DoubleType)
  testVectors("struct", 10, structType) { testVector =>
    val c1 = testVector.getChildColumn(0)
    val c2 = testVector.getChildColumn(1)
    c1.putInt(0, 123)
    c2.putDouble(0, 3.45)
    c1.putInt(1, 456)
    c2.putDouble(1, 5.67)

    val array = new ColumnVector.Array(testVector)

    assert(array.getStruct(0, 2).asInstanceOf[ColumnarBatch.Row].getInt(0) === 123)
    assert(array.getStruct(0, 2).asInstanceOf[ColumnarBatch.Row].getDouble(1) === 3.45)
    assert(array.getStruct(1, 2).asInstanceOf[ColumnarBatch.Row].getInt(0) === 456)
    assert(array.getStruct(1, 2).asInstanceOf[ColumnarBatch.Row].getDouble(1) === 5.67)
  }

  test("[SPARK-22092] off-heap column vector reallocation corrupts array data") {
    withVector(new OffHeapColumnVector(8, arrayType)) { testVector =>
      val data = testVector.arrayData()
      (0 until 8).foreach(i => data.putInt(i, i))
      (0 until 8).foreach(i => testVector.putArray(i, i, 1))

      // Increase vector's capacity and reallocate the data to new bigger buffers.
      testVector.reserve(16)

      // Check that none of the values got lost/overwritten.
      val array = new ColumnVector.Array(testVector)
      (0 until 8).foreach { i =>
        assert(array.getArray(i).toIntArray() === Array(i))
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
}
