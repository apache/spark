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
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckFailure
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, CreateArray, Literal, SpecificInternalRow}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

class ApproxCountDistinctForIntervalsSuite extends SparkFunSuite {

  test("fails analysis if parameters are invalid") {
    def assertEqual[T](left: T, right: T): Unit = {
      assert(left == right)
    }

    val wrongColumnTypes = Seq(BinaryType, BooleanType, StringType, ArrayType(IntegerType),
      MapType(IntegerType, IntegerType), StructType(Seq(StructField("s", IntegerType))))
    wrongColumnTypes.foreach { dataType =>
      val wrongColumn = new ApproxCountDistinctForIntervals(
        AttributeReference("a", dataType)(),
        endpointsExpression = CreateArray(Seq(1, 10).map(Literal(_))))
      assert(
        wrongColumn.checkInputDataTypes() match {
          case TypeCheckFailure(msg)
            if msg.contains("requires (numeric or timestamp or date) type") => true
          case _ => false
        })
    }

    var wrongEndpoints = new ApproxCountDistinctForIntervals(
      AttributeReference("a", DoubleType)(),
      endpointsExpression = Literal(0.5d))
    assert(
      wrongEndpoints.checkInputDataTypes() match {
        case TypeCheckFailure(msg) if msg.contains("requires array type") => true
        case _ => false
      })

    wrongEndpoints = new ApproxCountDistinctForIntervals(
      AttributeReference("a", DoubleType)(),
      endpointsExpression = CreateArray(Seq(AttributeReference("b", DoubleType)())))
    assertEqual(
      wrongEndpoints.checkInputDataTypes(),
      TypeCheckFailure("The intervals provided must be constant literals"))

    wrongEndpoints = new ApproxCountDistinctForIntervals(
      AttributeReference("a", DoubleType)(),
      endpointsExpression = CreateArray(Array(10L).map(Literal(_))))
    assertEqual(
      wrongEndpoints.checkInputDataTypes(),
      TypeCheckFailure("The number of endpoints must be >= 2 to construct intervals"))
  }

  /** Create an IntervalDistinctApprox instance and an input and output buffer. */
  private def createEstimator(
      endpoints: Array[Double],
      rsd: Double = 0.05,
      dt: DataType = IntegerType): (ApproxCountDistinctForIntervals, InternalRow, InternalRow) = {
    val input = new SpecificInternalRow(Seq(dt))
    val ida = ApproxCountDistinctForIntervals(
      BoundReference(0, dt, nullable = true), CreateArray(endpoints.map(Literal(_))), rsd)
    val buffer = createBuffer(ida)
    (ida, input, buffer)
  }

  private def createBuffer(ida: ApproxCountDistinctForIntervals): InternalRow = {
    val buffer = new SpecificInternalRow(ida.aggBufferAttributes.map(_.dataType))
    ida.initialize(buffer)
    buffer
  }

  test("merging IntervalDistinctApprox instances") {
    val (ida, input, buffer1a) = createEstimator(Array[Double](0, 10, 2000, 345678, 1000000))
    val buffer1b = createBuffer(ida)
    val buffer2 = createBuffer(ida)

    // Create the
    // Add the lower half
    var i = 0
    while (i < 500000) {
      input.setInt(0, i)
      ida.update(buffer1a, input)
      i += 1
    }

    // Add the upper half
    i = 500000
    while (i < 1000000) {
      input.setInt(0, i)
      ida.update(buffer1b, input)
      i += 1
    }

    // Merge the lower and upper halves.
    ida.merge(buffer1a, buffer1b)

    // Create the other buffer in reverse
    i = 999999
    while (i >= 0) {
      input.setInt(0, i)
      ida.update(buffer2, input)
      i -= 1
    }

    // Check if the buffers are equal.
    assert(buffer2 == buffer1a, "Buffers should be equal")
  }

  test("test findHllppIndex(value) for values in the range") {
    def checkHllppIndex(
        endpoints: Array[Double],
        value: Double,
        expectedIntervalIndex: Int): Unit = {
      val ida = ApproxCountDistinctForIntervals(
        BoundReference(0, DoubleType, nullable = true), CreateArray(endpoints.map(Literal(_))))
      assert(ida.findHllppIndex(value) == expectedIntervalIndex)
    }
    val endpoints = Array[Double](0, 3, 6, 10)
    // value is found (value is an interval boundary)
    checkHllppIndex(endpoints = endpoints, value = 0, expectedIntervalIndex = 0)
    checkHllppIndex(endpoints = endpoints, value = 3, expectedIntervalIndex = 0)
    checkHllppIndex(endpoints = endpoints, value = 6, expectedIntervalIndex = 1)
    checkHllppIndex(endpoints = endpoints, value = 10, expectedIntervalIndex = 2)
    // value is not found
    checkHllppIndex(endpoints = endpoints, value = 2, expectedIntervalIndex = 0)
    checkHllppIndex(endpoints = endpoints, value = 4, expectedIntervalIndex = 1)
    checkHllppIndex(endpoints = endpoints, value = 8, expectedIntervalIndex = 2)

    // value is the same as multiple boundaries
    checkHllppIndex(endpoints = Array(7, 7, 7, 9), value = 7, expectedIntervalIndex = 0)
    checkHllppIndex(endpoints = Array(3, 5, 7, 7, 7), value = 7, expectedIntervalIndex = 1)
    checkHllppIndex(endpoints = Array(1, 3, 5, 7, 7, 9), value = 7, expectedIntervalIndex = 2)
  }

  test("basic operations: update, merge, eval...") {
    val endpoints = Array[Double](0, 0.33, 0.6, 0.6, 0.6, 1.0)
    val data: Seq[Double] = Seq(0, 0.6, 0.3, 1, 0.6, 0.5, 0.6, 0.33)
    val expectedNdvs = Array[Long](3, 2, 1, 1, 1)

    Seq(0.01, 0.05, 0.1).foreach { relativeSD =>
      val (ida, input, buffer) = createEstimator(endpoints.reverse, relativeSD)
      // ida.endpoints will be sorted into ascending order
      assert(ida.endpoints.sameElements(endpoints))

      data.grouped(4).foreach { group =>
        val (partialIda, partialInput, partialBuffer) =
          createEstimator(endpoints, relativeSD, DoubleType)
        group.foreach { x =>
          partialInput.setDouble(0, x)
          partialIda.update(partialBuffer, partialInput)
        }
        ida.merge(buffer, partialBuffer)
      }
      // before eval(), for intervals with the same endpoints, only the first interval counts the
      // value
      checkNDVs(
        ndvs = ida.hllppResults(buffer),
        expectedNdvs = Array(3, 2, 0, 0, 1),
        rsd = relativeSD)

      // A value out of the whole range will not change the buffer
      input.setInt(0, 2)
      ida.update(buffer, input)
      checkNDVs(
        ndvs = ida.hllppResults(buffer),
        expectedNdvs = Array(3, 2, 0, 0, 1),
        rsd = relativeSD)

      // after eval(), set the others to 1
      checkNDVs(
        ndvs = ida.eval(buffer).asInstanceOf[ArrayData].toLongArray(),
        expectedNdvs = expectedNdvs,
        rsd = relativeSD)
    }
  }

  private def checkNDVs(ndvs: Array[Long], expectedNdvs: Array[Long], rsd: Double): Unit = {
    assert(ndvs.length == expectedNdvs.length)
    for (i <- ndvs.indices) {
      val ndv = ndvs(i)
      val expectedNdv = expectedNdvs(i)
      if (expectedNdv == 0) {
        assert(ndv == 0)
      } else if (expectedNdv > 0) {
        assert(ndv > 0)
        val error = math.abs((ndv / expectedNdv.toDouble) - 1.0d)
        assert(error <= rsd * 3.0d, "Error should be within 3 std. errors.")
      }
    }
  }
}
