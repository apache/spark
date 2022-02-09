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

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex._

import org.apache.spark.SparkFunSuite
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

  test("binary") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("binary", 0, Long.MaxValue)
    val vector = ArrowUtils.toArrowField("binary", BinaryType, nullable = true, null)
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
}
