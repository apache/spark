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

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.arrow.ArrowUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class ArrowColumnVectorSuite extends SparkFunSuite {

  test("boolean") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("boolean", 0, Long.MaxValue)
    val vector = ArrowUtils.toArrowField("boolean", BooleanType, nullable = true, null)
      .createVector(allocator).asInstanceOf[NullableBitVector]
    vector.allocateNew()
    val mutator = vector.getMutator()

    (0 until 10).foreach { i =>
      mutator.setSafe(i, if (i % 2 == 0) 1 else 0)
    }
    mutator.setNull(10)
    mutator.setValueCount(11)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === BooleanType)
    assert(columnVector.anyNullsSet)
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
      .createVector(allocator).asInstanceOf[NullableTinyIntVector]
    vector.allocateNew()
    val mutator = vector.getMutator()

    (0 until 10).foreach { i =>
      mutator.setSafe(i, i.toByte)
    }
    mutator.setNull(10)
    mutator.setValueCount(11)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === ByteType)
    assert(columnVector.anyNullsSet)
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
      .createVector(allocator).asInstanceOf[NullableSmallIntVector]
    vector.allocateNew()
    val mutator = vector.getMutator()

    (0 until 10).foreach { i =>
      mutator.setSafe(i, i.toShort)
    }
    mutator.setNull(10)
    mutator.setValueCount(11)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === ShortType)
    assert(columnVector.anyNullsSet)
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
      .createVector(allocator).asInstanceOf[NullableIntVector]
    vector.allocateNew()
    val mutator = vector.getMutator()

    (0 until 10).foreach { i =>
      mutator.setSafe(i, i)
    }
    mutator.setNull(10)
    mutator.setValueCount(11)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === IntegerType)
    assert(columnVector.anyNullsSet)
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
      .createVector(allocator).asInstanceOf[NullableBigIntVector]
    vector.allocateNew()
    val mutator = vector.getMutator()

    (0 until 10).foreach { i =>
      mutator.setSafe(i, i.toLong)
    }
    mutator.setNull(10)
    mutator.setValueCount(11)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === LongType)
    assert(columnVector.anyNullsSet)
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
      .createVector(allocator).asInstanceOf[NullableFloat4Vector]
    vector.allocateNew()
    val mutator = vector.getMutator()

    (0 until 10).foreach { i =>
      mutator.setSafe(i, i.toFloat)
    }
    mutator.setNull(10)
    mutator.setValueCount(11)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === FloatType)
    assert(columnVector.anyNullsSet)
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
      .createVector(allocator).asInstanceOf[NullableFloat8Vector]
    vector.allocateNew()
    val mutator = vector.getMutator()

    (0 until 10).foreach { i =>
      mutator.setSafe(i, i.toDouble)
    }
    mutator.setNull(10)
    mutator.setValueCount(11)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === DoubleType)
    assert(columnVector.anyNullsSet)
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
      .createVector(allocator).asInstanceOf[NullableVarCharVector]
    vector.allocateNew()
    val mutator = vector.getMutator()

    (0 until 10).foreach { i =>
      val utf8 = s"str$i".getBytes("utf8")
      mutator.setSafe(i, utf8, 0, utf8.length)
    }
    mutator.setNull(10)
    mutator.setValueCount(11)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === StringType)
    assert(columnVector.anyNullsSet)
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
      .createVector(allocator).asInstanceOf[NullableVarBinaryVector]
    vector.allocateNew()
    val mutator = vector.getMutator()

    (0 until 10).foreach { i =>
      val utf8 = s"str$i".getBytes("utf8")
      mutator.setSafe(i, utf8, 0, utf8.length)
    }
    mutator.setNull(10)
    mutator.setValueCount(11)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === BinaryType)
    assert(columnVector.anyNullsSet)
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
    val mutator = vector.getMutator()
    val elementVector = vector.getDataVector().asInstanceOf[NullableIntVector]
    val elementMutator = elementVector.getMutator()

    // [1, 2]
    mutator.startNewValue(0)
    elementMutator.setSafe(0, 1)
    elementMutator.setSafe(1, 2)
    mutator.endValue(0, 2)

    // [3, null, 5]
    mutator.startNewValue(1)
    elementMutator.setSafe(2, 3)
    elementMutator.setNull(3)
    elementMutator.setSafe(4, 5)
    mutator.endValue(1, 3)

    // null

    // []
    mutator.startNewValue(3)
    mutator.endValue(3, 0)

    elementMutator.setValueCount(5)
    mutator.setValueCount(4)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === ArrayType(IntegerType))
    assert(columnVector.anyNullsSet)
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

  test("struct") {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("struct", 0, Long.MaxValue)
    val schema = new StructType().add("int", IntegerType).add("long", LongType)
    val vector = ArrowUtils.toArrowField("struct", schema, nullable = true, null)
      .createVector(allocator).asInstanceOf[NullableMapVector]
    vector.allocateNew()
    val mutator = vector.getMutator()
    val intVector = vector.getChildByOrdinal(0).asInstanceOf[NullableIntVector]
    val intMutator = intVector.getMutator()
    val longVector = vector.getChildByOrdinal(1).asInstanceOf[NullableBigIntVector]
    val longMutator = longVector.getMutator()

    // (1, 1L)
    mutator.setIndexDefined(0)
    intMutator.setSafe(0, 1)
    longMutator.setSafe(0, 1L)

    // (2, null)
    mutator.setIndexDefined(1)
    intMutator.setSafe(1, 2)
    longMutator.setNull(1)

    // (null, 3L)
    mutator.setIndexDefined(2)
    intMutator.setNull(2)
    longMutator.setSafe(2, 3L)

    // null
    mutator.setNull(3)

    // (5, 5L)
    mutator.setIndexDefined(4)
    intMutator.setSafe(4, 5)
    longMutator.setSafe(4, 5L)

    intMutator.setValueCount(5)
    longMutator.setValueCount(5)
    mutator.setValueCount(5)

    val columnVector = new ArrowColumnVector(vector)
    assert(columnVector.dataType === schema)
    assert(columnVector.anyNullsSet)
    assert(columnVector.numNulls === 1)

    val row0 = columnVector.getStruct(0, 2)
    assert(row0.getInt(0) === 1)
    assert(row0.getLong(1) === 1L)

    val row1 = columnVector.getStruct(1, 2)
    assert(row1.getInt(0) === 2)
    assert(row1.isNullAt(1))

    val row2 = columnVector.getStruct(2, 2)
    assert(row2.isNullAt(0))
    assert(row2.getLong(1) === 3L)

    assert(columnVector.isNullAt(3))

    val row4 = columnVector.getStruct(4, 2)
    assert(row4.getInt(0) === 5)
    assert(row4.getLong(1) === 5L)

    columnVector.close()
    allocator.close()
  }
}
