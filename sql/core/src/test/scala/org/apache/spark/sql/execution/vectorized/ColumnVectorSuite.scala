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

  var testVector: WritableColumnVector = _

  private def allocate(capacity: Int, dt: DataType): WritableColumnVector = {
    new OnHeapColumnVector(capacity, dt)
  }

  override def afterEach(): Unit = {
    testVector.close()
  }

  test("boolean") {
    testVector = allocate(10, BooleanType)
    (0 until 10).foreach { i =>
      testVector.appendBoolean(i % 2 == 0)
    }

    val array = new ColumnVector.Array(testVector)

    (0 until 10).foreach { i =>
      assert(array.get(i, BooleanType) === (i % 2 == 0))
    }
  }

  test("byte") {
    testVector = allocate(10, ByteType)
    (0 until 10).foreach { i =>
      testVector.appendByte(i.toByte)
    }

    val array = new ColumnVector.Array(testVector)

    (0 until 10).foreach { i =>
      assert(array.get(i, ByteType) === (i.toByte))
    }
  }

  test("short") {
    testVector = allocate(10, ShortType)
    (0 until 10).foreach { i =>
      testVector.appendShort(i.toShort)
    }

    val array = new ColumnVector.Array(testVector)

    (0 until 10).foreach { i =>
      assert(array.get(i, ShortType) === (i.toShort))
    }
  }

  test("int") {
    testVector = allocate(10, IntegerType)
    (0 until 10).foreach { i =>
      testVector.appendInt(i)
    }

    val array = new ColumnVector.Array(testVector)

    (0 until 10).foreach { i =>
      assert(array.get(i, IntegerType) === i)
    }
  }

  test("long") {
    testVector = allocate(10, LongType)
    (0 until 10).foreach { i =>
      testVector.appendLong(i)
    }

    val array = new ColumnVector.Array(testVector)

    (0 until 10).foreach { i =>
      assert(array.get(i, LongType) === i)
    }
  }

  test("float") {
    testVector = allocate(10, FloatType)
    (0 until 10).foreach { i =>
      testVector.appendFloat(i.toFloat)
    }

    val array = new ColumnVector.Array(testVector)

    (0 until 10).foreach { i =>
      assert(array.get(i, FloatType) === i.toFloat)
    }
  }

  test("double") {
    testVector = allocate(10, DoubleType)
    (0 until 10).foreach { i =>
      testVector.appendDouble(i.toDouble)
    }

    val array = new ColumnVector.Array(testVector)

    (0 until 10).foreach { i =>
      assert(array.get(i, DoubleType) === i.toDouble)
    }
  }

  test("string") {
    testVector = allocate(10, StringType)
    (0 until 10).map { i =>
      val utf8 = s"str$i".getBytes("utf8")
      testVector.appendByteArray(utf8, 0, utf8.length)
    }

    val array = new ColumnVector.Array(testVector)

    (0 until 10).foreach { i =>
      assert(array.get(i, StringType) === UTF8String.fromString(s"str$i"))
    }
  }

  test("binary") {
    testVector = allocate(10, BinaryType)
    (0 until 10).map { i =>
      val utf8 = s"str$i".getBytes("utf8")
      testVector.appendByteArray(utf8, 0, utf8.length)
    }

    val array = new ColumnVector.Array(testVector)

    (0 until 10).foreach { i =>
      val utf8 = s"str$i".getBytes("utf8")
      assert(array.get(i, BinaryType) === utf8)
    }
  }

  test("array") {
    val arrayType = ArrayType(IntegerType, true)
    testVector = allocate(10, arrayType)

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

    assert(array.get(0, arrayType).asInstanceOf[ArrayData].toIntArray() === Array(0))
    assert(array.get(1, arrayType).asInstanceOf[ArrayData].toIntArray() === Array(1, 2))
    assert(array.get(2, arrayType).asInstanceOf[ArrayData].toIntArray() === Array.empty[Int])
    assert(array.get(3, arrayType).asInstanceOf[ArrayData].toIntArray() === Array(3, 4, 5))
  }

  test("struct") {
    val schema = new StructType().add("int", IntegerType).add("double", DoubleType)
    testVector = allocate(10, schema)
    val c1 = testVector.getChildColumn(0)
    val c2 = testVector.getChildColumn(1)
    c1.putInt(0, 123)
    c2.putDouble(0, 3.45)
    c1.putInt(1, 456)
    c2.putDouble(1, 5.67)

    val array = new ColumnVector.Array(testVector)

    assert(array.get(0, schema).asInstanceOf[ColumnarBatch.Row].get(0, IntegerType) === 123)
    assert(array.get(0, schema).asInstanceOf[ColumnarBatch.Row].get(1, DoubleType) === 3.45)
    assert(array.get(1, schema).asInstanceOf[ColumnarBatch.Row].get(0, IntegerType) === 456)
    assert(array.get(1, schema).asInstanceOf[ColumnarBatch.Row].get(1, DoubleType) === 5.67)
  }
}
