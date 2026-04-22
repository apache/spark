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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, SpecificInternalRow, VectorAvg, VectorSum}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.{ArrayType, FloatType}

class VectorAggSuite extends SparkFunSuite {

  // Helper to create a VectorSum instance with buffer
  def createVectorSum(): (VectorSum, InternalRow, BoundReference) = {
    val input = new BoundReference(0, ArrayType(FloatType), nullable = true)
    val agg = new VectorSum(input)
    val buffer = new SpecificInternalRow(agg.aggBufferAttributes.map(_.dataType))
    agg.initialize(buffer)
    (agg, buffer, input)
  }

  // Helper to create a VectorAvg instance with buffer
  def createVectorAvg(): (VectorAvg, InternalRow, BoundReference) = {
    val input = new BoundReference(0, ArrayType(FloatType), nullable = true)
    val agg = new VectorAvg(input)
    val buffer = new SpecificInternalRow(agg.aggBufferAttributes.map(_.dataType))
    agg.initialize(buffer)
    (agg, buffer, input)
  }

  // Helper to create input row with float array
  def createInputRow(values: Array[Float]): InternalRow = {
    InternalRow(ArrayData.toArrayData(values))
  }

  // Helper to create input row with null
  def createNullInputRow(): InternalRow = {
    val row = new SpecificInternalRow(Seq(ArrayType(FloatType)))
    row.setNullAt(0)
    row
  }

  // Helper to create input row with array containing null elements
  def createInputRowWithNullElement(values: Array[java.lang.Float]): InternalRow = {
    val arrayData = new GenericArrayData(values.map {
      case null => null
      case v => v.floatValue().asInstanceOf[AnyRef]
    })
    InternalRow(arrayData)
  }

  // Helper to extract result as float array
  def evalAsFloatArray(agg: VectorSum, buffer: InternalRow): Array[Float] = {
    val result = agg.eval(buffer)
    if (result == null) return null
    val arrayData = result.asInstanceOf[ArrayData]
    (0 until arrayData.numElements()).map(i => arrayData.getFloat(i)).toArray
  }

  def evalAsFloatArray(agg: VectorAvg, buffer: InternalRow): Array[Float] = {
    val result = agg.eval(buffer)
    if (result == null) return null
    val arrayData = result.asInstanceOf[ArrayData]
    (0 until arrayData.numElements()).map(i => arrayData.getFloat(i)).toArray
  }

  // Asserts that two floats are approximately equal within a tolerance.
  def assertFloatEquals(actual: Float, expected: Float, tolerance: Float = 1e-5f): Unit = {
    assert(
      java.lang.Float.isNaN(expected) && java.lang.Float.isNaN(actual) ||
        math.abs(actual - expected) <= tolerance,
      s"Expected $expected but got $actual (tolerance: $tolerance)"
    )
  }

  // Asserts that two float arrays are approximately equal element-wise.
  def assertFloatArrayEquals(
      actual: Array[Float],
      expected: Array[Float],
      tolerance: Float = 1e-5f): Unit = {
    assert(actual.length == expected.length,
      s"Array lengths differ: ${actual.length} vs ${expected.length}")
    actual.zip(expected).zipWithIndex.foreach { case ((a, e), i) =>
      assertFloatEquals(a, e, tolerance)
    }
  }

  test("VectorSum - empty buffer returns null") {
    val (agg, buffer, _) = createVectorSum()
    assert(agg.eval(buffer) === null)
  }

  test("VectorSum - single vector") {
    val (agg, buffer, _) = createVectorSum()
    agg.update(buffer, createInputRow(Array(1.0f, 2.0f, 3.0f)))
    val result = evalAsFloatArray(agg, buffer)
    assert(result === Array(1.0f, 2.0f, 3.0f))
  }

  test("VectorSum - multiple vectors") {
    val (agg, buffer, _) = createVectorSum()
    agg.update(buffer, createInputRow(Array(1.0f, 2.0f)))
    agg.update(buffer, createInputRow(Array(3.0f, 4.0f)))
    agg.update(buffer, createInputRow(Array(5.0f, 6.0f)))
    val result = evalAsFloatArray(agg, buffer)
    assert(result === Array(9.0f, 12.0f))
  }

  test("VectorSum - null vectors are skipped") {
    val (agg, buffer, _) = createVectorSum()
    agg.update(buffer, createInputRow(Array(1.0f, 2.0f)))
    agg.update(buffer, createNullInputRow())
    agg.update(buffer, createInputRow(Array(3.0f, 4.0f)))
    val result = evalAsFloatArray(agg, buffer)
    assert(result === Array(4.0f, 6.0f))
  }

  test("VectorSum - all null vectors returns null") {
    val (agg, buffer, _) = createVectorSum()
    agg.update(buffer, createNullInputRow())
    agg.update(buffer, createNullInputRow())
    assert(agg.eval(buffer) === null)
  }

  test("VectorSum - empty vectors") {
    val (agg, buffer, _) = createVectorSum()
    agg.update(buffer, createInputRow(Array.empty[Float]))
    agg.update(buffer, createInputRow(Array.empty[Float]))
    val result = evalAsFloatArray(agg, buffer)
    assert(result === Array.empty[Float])
  }

  test("VectorSum - merge two buffers") {
    val (agg, buffer1, _) = createVectorSum()
    val (_, buffer2, _) = createVectorSum()

    // Partition 1: [1, 2] + [3, 4] = [4, 6]
    agg.update(buffer1, createInputRow(Array(1.0f, 2.0f)))
    agg.update(buffer1, createInputRow(Array(3.0f, 4.0f)))

    // Partition 2: [5, 6] + [7, 8] = [12, 14]
    agg.update(buffer2, createInputRow(Array(5.0f, 6.0f)))
    agg.update(buffer2, createInputRow(Array(7.0f, 8.0f)))

    // Merge: [4, 6] + [12, 14] = [16, 20]
    agg.merge(buffer1, buffer2)

    val result = evalAsFloatArray(agg, buffer1)
    assert(result === Array(16.0f, 20.0f))
  }

  test("VectorSum - merge with empty buffer") {
    val (agg, buffer1, _) = createVectorSum()
    val (_, buffer2, _) = createVectorSum()

    agg.update(buffer1, createInputRow(Array(1.0f, 2.0f)))
    // buffer2 is empty (no updates)

    agg.merge(buffer1, buffer2)
    val result = evalAsFloatArray(agg, buffer1)
    assert(result === Array(1.0f, 2.0f))
  }

  test("VectorSum - merge empty buffer with non-empty") {
    val (agg, buffer1, _) = createVectorSum()
    val (_, buffer2, _) = createVectorSum()

    // buffer1 is empty
    agg.update(buffer2, createInputRow(Array(1.0f, 2.0f)))

    agg.merge(buffer1, buffer2)
    val result = evalAsFloatArray(agg, buffer1)
    assert(result === Array(1.0f, 2.0f))
  }

  test("VectorSum - merge two empty buffers") {
    val (agg, buffer1, _) = createVectorSum()
    val (_, buffer2, _) = createVectorSum()

    agg.merge(buffer1, buffer2)
    assert(agg.eval(buffer1) === null)
  }

  test("VectorSum - special float: NaN propagates") {
    val (agg, buffer, _) = createVectorSum()
    agg.update(buffer, createInputRow(Array(1.0f, 2.0f)))
    agg.update(buffer, createInputRow(Array(Float.NaN, 3.0f)))
    val result = evalAsFloatArray(agg, buffer)
    assert(result(0).isNaN)
    assert(result(1) === 5.0f)
  }

  test("VectorSum - special float: Infinity") {
    val (agg, buffer, _) = createVectorSum()
    agg.update(buffer, createInputRow(Array(1.0f, 2.0f)))
    agg.update(buffer, createInputRow(Array(Float.PositiveInfinity, 3.0f)))
    val result = evalAsFloatArray(agg, buffer)
    assert(result(0) === Float.PositiveInfinity)
    assert(result(1) === 5.0f)
  }

  test("VectorSum - special float: Negative Infinity") {
    val (agg, buffer, _) = createVectorSum()
    agg.update(buffer, createInputRow(Array(1.0f, 2.0f)))
    agg.update(buffer, createInputRow(Array(Float.NegativeInfinity, 3.0f)))
    val result = evalAsFloatArray(agg, buffer)
    assert(result(0) === Float.NegativeInfinity)
    assert(result(1) === 5.0f)
  }

  test("VectorAvg - empty buffer returns null") {
    val (agg, buffer, _) = createVectorAvg()
    assert(agg.eval(buffer) === null)
  }

  test("VectorAvg - single vector") {
    val (agg, buffer, _) = createVectorAvg()
    agg.update(buffer, createInputRow(Array(1.0f, 2.0f, 3.0f)))
    val result = evalAsFloatArray(agg, buffer)
    assert(result === Array(1.0f, 2.0f, 3.0f))
  }

  test("VectorAvg - multiple vectors") {
    val (agg, buffer, _) = createVectorAvg()
    agg.update(buffer, createInputRow(Array(1.0f, 1.0f)))
    agg.update(buffer, createInputRow(Array(2.0f, 2.0f)))
    agg.update(buffer, createInputRow(Array(3.0f, 3.0f)))
    val result = evalAsFloatArray(agg, buffer)
    assert(result === Array(2.0f, 2.0f))
  }

  test("VectorAvg - null vectors are skipped") {
    val (agg, buffer, _) = createVectorAvg()
    agg.update(buffer, createInputRow(Array(1.0f, 2.0f)))
    agg.update(buffer, createNullInputRow())
    agg.update(buffer, createInputRow(Array(3.0f, 4.0f)))
    val result = evalAsFloatArray(agg, buffer)
    // Average of [1,2] and [3,4] = [2, 3]
    assert(result === Array(2.0f, 3.0f))
  }

  test("VectorAvg - all null vectors returns null") {
    val (agg, buffer, _) = createVectorAvg()
    agg.update(buffer, createNullInputRow())
    agg.update(buffer, createNullInputRow())
    assert(agg.eval(buffer) === null)
  }

  test("VectorAvg - empty vectors") {
    val (agg, buffer, _) = createVectorAvg()
    agg.update(buffer, createInputRow(Array.empty[Float]))
    agg.update(buffer, createInputRow(Array.empty[Float]))
    val result = evalAsFloatArray(agg, buffer)
    assert(result === Array.empty[Float])
  }

  test("VectorAvg - merge two buffers") {
    val (agg, buffer1, _) = createVectorAvg()
    val (_, buffer2, _) = createVectorAvg()

    // Partition 1: avg([1,2], [3,4]) = [2, 3], count=2
    agg.update(buffer1, createInputRow(Array(1.0f, 2.0f)))
    agg.update(buffer1, createInputRow(Array(3.0f, 4.0f)))

    // Partition 2: avg([5,6], [7,8]) = [6, 7], count=2
    agg.update(buffer2, createInputRow(Array(5.0f, 6.0f)))
    agg.update(buffer2, createInputRow(Array(7.0f, 8.0f)))

    // Merged average: (2*[2,3] + 2*[6,7]) / 4 = [4, 5]
    agg.merge(buffer1, buffer2)

    val result = evalAsFloatArray(agg, buffer1)
    assert(result === Array(4.0f, 5.0f))
  }

  test("VectorAvg - merge with different counts") {
    val (agg, buffer1, _) = createVectorAvg()
    val (_, buffer2, _) = createVectorAvg()

    // Partition 1: single vector [10, 20], count=1
    agg.update(buffer1, createInputRow(Array(10.0f, 20.0f)))

    // Partition 2: three vectors, avg = [2, 2], count=3
    agg.update(buffer2, createInputRow(Array(1.0f, 1.0f)))
    agg.update(buffer2, createInputRow(Array(2.0f, 2.0f)))
    agg.update(buffer2, createInputRow(Array(3.0f, 3.0f)))

    // Merged: (1*[10,20] + 3*[2,2]) / 4 = [16/4, 26/4] = [4, 6.5]
    agg.merge(buffer1, buffer2)

    val result = evalAsFloatArray(agg, buffer1)
    assert(result === Array(4.0f, 6.5f))
  }

  test("VectorAvg - merge with empty buffer") {
    val (agg, buffer1, _) = createVectorAvg()
    val (_, buffer2, _) = createVectorAvg()

    agg.update(buffer1, createInputRow(Array(1.0f, 2.0f)))
    // buffer2 is empty

    agg.merge(buffer1, buffer2)
    val result = evalAsFloatArray(agg, buffer1)
    assert(result === Array(1.0f, 2.0f))
  }

  test("VectorAvg - merge empty buffer with non-empty") {
    val (agg, buffer1, _) = createVectorAvg()
    val (_, buffer2, _) = createVectorAvg()

    // buffer1 is empty
    agg.update(buffer2, createInputRow(Array(1.0f, 2.0f)))

    agg.merge(buffer1, buffer2)
    val result = evalAsFloatArray(agg, buffer1)
    assert(result === Array(1.0f, 2.0f))
  }

  test("VectorAvg - merge two empty buffers") {
    val (agg, buffer1, _) = createVectorAvg()
    val (_, buffer2, _) = createVectorAvg()

    agg.merge(buffer1, buffer2)
    assert(agg.eval(buffer1) === null)
  }

  test("VectorAvg - special float: NaN propagates") {
    val (agg, buffer, _) = createVectorAvg()
    agg.update(buffer, createInputRow(Array(1.0f, 2.0f)))
    agg.update(buffer, createInputRow(Array(Float.NaN, 4.0f)))
    val result = evalAsFloatArray(agg, buffer)
    assert(result(0).isNaN)
    assert(result(1) === 3.0f)
  }

  test("VectorAvg - special float: Infinity") {
    val (agg, buffer, _) = createVectorAvg()
    agg.update(buffer, createInputRow(Array(1.0f, 2.0f)))
    agg.update(buffer, createInputRow(Array(Float.PositiveInfinity, 4.0f)))
    val result = evalAsFloatArray(agg, buffer)
    assert(result(0) === Float.PositiveInfinity)
    assert(result(1) === 3.0f)
  }

  test("VectorAvg - numerical stability with running average") {
    val (agg, buffer, _) = createVectorAvg()
    // Add many vectors to test numerical stability of running average
    for (i <- 1 to 100) {
      agg.update(buffer, createInputRow(Array(i.toFloat, i.toFloat)))
    }
    val result = evalAsFloatArray(agg, buffer)
    // Average of 1 to 100 = 50.5
    assertFloatArrayEquals(result, Array(50.5f, 50.5f), tolerance = 1e-3f)
  }

  test("VectorSum - mathematical correctness: element-wise sum") {
    val (agg, buffer, _) = createVectorSum()
    agg.update(buffer, createInputRow(Array(1.0f, 2.0f, 3.0f)))
    agg.update(buffer, createInputRow(Array(10.0f, 20.0f, 30.0f)))
    val result = evalAsFloatArray(agg, buffer)
    // [1, 2, 3] + [10, 20, 30] = [11, 22, 33]
    assert(result === Array(11.0f, 22.0f, 33.0f))
  }

  test("VectorAvg - mathematical correctness: element-wise average") {
    val (agg, buffer, _) = createVectorAvg()
    agg.update(buffer, createInputRow(Array(0.0f, 0.0f)))
    agg.update(buffer, createInputRow(Array(10.0f, 20.0f)))
    val result = evalAsFloatArray(agg, buffer)
    // avg([0, 0], [10, 20]) = [5, 10]
    assert(result === Array(5.0f, 10.0f))
  }

  test("VectorAvg - mathematical correctness: negative values") {
    val (agg, buffer, _) = createVectorAvg()
    agg.update(buffer, createInputRow(Array(-5.0f, 10.0f)))
    agg.update(buffer, createInputRow(Array(5.0f, -10.0f)))
    val result = evalAsFloatArray(agg, buffer)
    // avg([-5, 10], [5, -10]) = [0, 0]
    assert(result === Array(0.0f, 0.0f))
  }

  test("VectorSum - vectors with null elements are skipped") {
    val (agg, buffer, _) = createVectorSum()
    agg.update(buffer, createInputRow(Array(1.0f, 2.0f)))
    agg.update(buffer, createInputRowWithNullElement(Array(null, 10.0f)))
    agg.update(buffer, createInputRow(Array(3.0f, 4.0f)))
    val result = evalAsFloatArray(agg, buffer)
    // Vector with null element is skipped, so [1, 2] + [3, 4] = [4, 6]
    assert(result === Array(4.0f, 6.0f))
  }

  test("VectorAvg - vectors with null elements are skipped") {
    val (agg, buffer, _) = createVectorAvg()
    agg.update(buffer, createInputRow(Array(1.0f, 2.0f)))
    agg.update(buffer, createInputRowWithNullElement(Array(null, 10.0f)))
    agg.update(buffer, createInputRow(Array(3.0f, 4.0f)))
    val result = evalAsFloatArray(agg, buffer)
    // Vector with null element is skipped, so avg([1, 2], [3, 4]) = [2, 3]
    assert(result === Array(2.0f, 3.0f))
  }

  test("VectorSum - only vectors with null elements returns null") {
    val (agg, buffer, _) = createVectorSum()
    agg.update(buffer, createInputRowWithNullElement(Array(1.0f, null)))
    agg.update(buffer, createInputRowWithNullElement(Array(null, 2.0f)))
    assert(agg.eval(buffer) === null)
  }

  test("VectorAvg - only vectors with null elements returns null") {
    val (agg, buffer, _) = createVectorAvg()
    agg.update(buffer, createInputRowWithNullElement(Array(1.0f, null)))
    agg.update(buffer, createInputRowWithNullElement(Array(null, 2.0f)))
    assert(agg.eval(buffer) === null)
  }

  test("VectorSum - mix of null vectors and vectors with null elements") {
    val (agg, buffer, _) = createVectorSum()
    agg.update(buffer, createNullInputRow())
    agg.update(buffer, createInputRowWithNullElement(Array(1.0f, null)))
    agg.update(buffer, createInputRow(Array(1.0f, 2.0f)))
    agg.update(buffer, createInputRow(Array(3.0f, 4.0f)))
    val result = evalAsFloatArray(agg, buffer)
    // Only valid vectors are summed: [1, 2] + [3, 4] = [4, 6]
    assert(result === Array(4.0f, 6.0f))
  }

  test("VectorSum - large vectors (16 elements)") {
    val (agg, buffer, _) = createVectorSum()
    val vec1 = Array(1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f,
                     9.0f, 10.0f, 11.0f, 12.0f, 13.0f, 14.0f, 15.0f, 16.0f)
    val vec2 = Array(16.0f, 15.0f, 14.0f, 13.0f, 12.0f, 11.0f, 10.0f, 9.0f,
                     8.0f, 7.0f, 6.0f, 5.0f, 4.0f, 3.0f, 2.0f, 1.0f)
    agg.update(buffer, createInputRow(vec1))
    agg.update(buffer, createInputRow(vec2))
    val result = evalAsFloatArray(agg, buffer)
    // Each element should sum to 17
    assert(result === Array.fill(16)(17.0f))
  }

  test("VectorAvg - large vectors (16 elements)") {
    val (agg, buffer, _) = createVectorAvg()
    val vec1 = Array(1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f,
                     9.0f, 10.0f, 11.0f, 12.0f, 13.0f, 14.0f, 15.0f, 16.0f)
    val vec2 = Array(16.0f, 15.0f, 14.0f, 13.0f, 12.0f, 11.0f, 10.0f, 9.0f,
                     8.0f, 7.0f, 6.0f, 5.0f, 4.0f, 3.0f, 2.0f, 1.0f)
    agg.update(buffer, createInputRow(vec1))
    agg.update(buffer, createInputRow(vec2))
    val result = evalAsFloatArray(agg, buffer)
    // Each element average should be 8.5
    assert(result === Array.fill(16)(8.5f))
  }

  test("VectorSum - large vector with null element is skipped") {
    val (agg, buffer, _) = createVectorSum()
    val vec1 = Array[java.lang.Float](1.0f, 2.0f, 3.0f, 4.0f, 5.0f, null, 7.0f, 8.0f,
                     9.0f, 10.0f, 11.0f, 12.0f, 13.0f, 14.0f, 15.0f, 16.0f)
    val vec2 = Array(16.0f, 15.0f, 14.0f, 13.0f, 12.0f, 11.0f, 10.0f, 9.0f,
                     8.0f, 7.0f, 6.0f, 5.0f, 4.0f, 3.0f, 2.0f, 1.0f)
    agg.update(buffer, createInputRowWithNullElement(vec1))
    agg.update(buffer, createInputRow(vec2))
    val result = evalAsFloatArray(agg, buffer)
    // First vector is skipped due to null element, result is just vec2
    assert(result === vec2)
  }

  test("VectorSum - single element vectors") {
    val (agg, buffer, _) = createVectorSum()
    agg.update(buffer, createInputRow(Array(5.0f)))
    agg.update(buffer, createInputRow(Array(3.0f)))
    val result = evalAsFloatArray(agg, buffer)
    assert(result === Array(8.0f))
  }

  test("VectorAvg - single element vectors") {
    val (agg, buffer, _) = createVectorAvg()
    agg.update(buffer, createInputRow(Array(5.0f)))
    agg.update(buffer, createInputRow(Array(3.0f)))
    val result = evalAsFloatArray(agg, buffer)
    assert(result === Array(4.0f))
  }
}
