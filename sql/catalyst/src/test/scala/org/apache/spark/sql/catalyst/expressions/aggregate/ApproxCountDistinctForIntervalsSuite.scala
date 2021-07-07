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

import java.sql.{Date, Timestamp}
import java.time.LocalDateTime

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckFailure
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, CreateArray, Literal, SpecificInternalRow}
import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils}
import org.apache.spark.sql.types._

class ApproxCountDistinctForIntervalsSuite extends SparkFunSuite {

  test("fails analysis if parameters are invalid") {
    val wrongColumnTypes = Seq(BinaryType, BooleanType, StringType, ArrayType(IntegerType),
      MapType(IntegerType, IntegerType), StructType(Seq(StructField("s", IntegerType))))
    wrongColumnTypes.foreach { dataType =>
      val wrongColumn = ApproxCountDistinctForIntervals(
        AttributeReference("a", dataType)(),
        endpointsExpression = CreateArray(Seq(1, 10).map(Literal(_))))
      assert(
        wrongColumn.checkInputDataTypes() match {
          case TypeCheckFailure(msg)
            if msg.contains("requires (numeric or timestamp or date or timestamp_ntz) type") => true
          case _ => false
        })
    }

    var wrongEndpoints = ApproxCountDistinctForIntervals(
      AttributeReference("a", DoubleType)(),
      endpointsExpression = Literal(0.5d))
    assert(
      wrongEndpoints.checkInputDataTypes() match {
        case TypeCheckFailure(msg) if msg.contains("requires array type") => true
        case _ => false
      })

    wrongEndpoints = ApproxCountDistinctForIntervals(
      AttributeReference("a", DoubleType)(),
      endpointsExpression = CreateArray(Seq(AttributeReference("b", DoubleType)())))
    assert(wrongEndpoints.checkInputDataTypes() ==
      TypeCheckFailure("The endpoints provided must be constant literals"))

    wrongEndpoints = ApproxCountDistinctForIntervals(
      AttributeReference("a", DoubleType)(),
      endpointsExpression = CreateArray(Array(10L).map(Literal(_))))
    assert(wrongEndpoints.checkInputDataTypes() ==
      TypeCheckFailure("The number of endpoints must be >= 2 to construct intervals"))

    wrongEndpoints = ApproxCountDistinctForIntervals(
      AttributeReference("a", DoubleType)(),
      endpointsExpression = CreateArray(Array("foobar").map(Literal(_))))
    assert(wrongEndpoints.checkInputDataTypes() ==
        TypeCheckFailure("Endpoints require (numeric or timestamp or date) type"))
  }

  /** Create an ApproxCountDistinctForIntervals instance and an input and output buffer. */
  private def createEstimator[T](
      endpoints: Array[T],
      dt: DataType,
      rsd: Double = 0.05): (ApproxCountDistinctForIntervals, InternalRow, Array[Long]) = {
    val input = new SpecificInternalRow(Seq(dt))
    val aggFunc = ApproxCountDistinctForIntervals(
      BoundReference(0, dt, nullable = true), CreateArray(endpoints.map(Literal(_))), rsd)
    (aggFunc, input, aggFunc.createAggregationBuffer())
  }

  test("merging ApproxCountDistinctForIntervals instances") {
    val (aggFunc, input, buffer1a) =
      createEstimator(Array[Int](0, 10, 2000, 345678, 1000000), IntegerType)
    val buffer1b = aggFunc.createAggregationBuffer()
    val buffer2 = aggFunc.createAggregationBuffer()

    // Add the lower half to `buffer1a`.
    var i = 0
    while (i < 500000) {
      input.setInt(0, i)
      aggFunc.update(buffer1a, input)
      i += 1
    }

    // Add the upper half to `buffer1b`.
    i = 500000
    while (i < 1000000) {
      input.setInt(0, i)
      aggFunc.update(buffer1b, input)
      i += 1
    }

    // Merge the lower and upper halves to `buffer1a`.
    aggFunc.merge(buffer1a, buffer1b)

    // Create the other buffer in reverse.
    i = 999999
    while (i >= 0) {
      input.setInt(0, i)
      aggFunc.update(buffer2, input)
      i -= 1
    }

    // Check if the buffers are equal.
    assert(buffer2.sameElements(buffer1a), "Buffers should be equal")
  }

  test("test findHllppIndex(value) for values in the range") {
    def checkHllppIndex(
        endpoints: Array[Double],
        value: Double,
        expectedIntervalIndex: Int): Unit = {
      val aggFunc = ApproxCountDistinctForIntervals(
        BoundReference(0, DoubleType, nullable = true), CreateArray(endpoints.map(Literal(_))))
      assert(aggFunc.findHllppIndex(value) == expectedIntervalIndex)
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

  test("round trip serialization") {
    val (aggFunc, _, _) = createEstimator(Array(1, 2), DoubleType)
    val longArray = (1L to 100L).toArray
    val roundtrip = aggFunc.deserialize(aggFunc.serialize(longArray))
    assert(roundtrip.sameElements(longArray))
  }

  test("basic operations: update, merge, eval...") {
    val endpoints = Array[Double](0, 0.33, 0.6, 0.6, 0.6, 1.0)
    val data: Seq[Double] = Seq(0, 0.6, 0.3, 1, 0.6, 0.5, 0.6, 0.33)

    Seq(0.01, 0.05, 0.1).foreach { relativeSD =>
      val (aggFunc, input, buffer) = createEstimator(endpoints, DoubleType, relativeSD)

      data.grouped(4).foreach { group =>
        val (partialAggFunc, partialInput, partialBuffer) =
          createEstimator(endpoints, DoubleType, relativeSD)
        group.foreach { x =>
          partialInput.setDouble(0, x)
          partialAggFunc.update(partialBuffer, partialInput)
        }
        aggFunc.merge(buffer, partialBuffer)
      }
      // before eval(), for intervals with the same endpoints, only the first interval counts the
      // value
      checkNDVs(
        ndvs = aggFunc.hllppResults(buffer),
        expectedNdvs = Array(3, 2, 0, 0, 1),
        rsd = relativeSD)

      // A value out of the whole range will not change the buffer
      input.setDouble(0, 2.0)
      aggFunc.update(buffer, input)
      checkNDVs(
        ndvs = aggFunc.hllppResults(buffer),
        expectedNdvs = Array(3, 2, 0, 0, 1),
        rsd = relativeSD)

      // after eval(), set the others to 1
      checkNDVs(
        ndvs = aggFunc.eval(buffer).asInstanceOf[ArrayData].toLongArray(),
        expectedNdvs = Array(3, 2, 1, 1, 1),
        rsd = relativeSD)
    }
  }

  test("test for different input types: numeric/date/timestamp") {
    val intEndpoints = Array[Int](0, 33, 60, 60, 60, 100)
    val intRecords: Seq[Int] = Seq(0, 60, 30, 100, 60, 50, 60, 33)
    val inputs = Seq(
      (intRecords, intEndpoints, IntegerType),
      (intRecords.map(DateTimeUtils.toJavaDate),
          intEndpoints.map(DateTimeUtils.toJavaDate), DateType),
      (intRecords.map(DateTimeUtils.toJavaTimestamp(_)),
          intEndpoints.map(DateTimeUtils.toJavaTimestamp(_)), TimestampType),
      (intRecords.map(DateTimeUtils.microsToLocalDateTime(_)),
        intEndpoints.map(DateTimeUtils.microsToLocalDateTime(_)), TimestampNTZType)
    )

    inputs.foreach { case (records, endpoints, dataType) =>
      val (aggFunc, input, buffer) = createEstimator(endpoints, dataType)
      records.foreach { r =>
        // convert to internal type value
        val value = r match {
          case d: Date => DateTimeUtils.fromJavaDate(d)
          case t: Timestamp => DateTimeUtils.fromJavaTimestamp(t)
          case ldt: LocalDateTime => DateTimeUtils.localDateTimeToMicros(ldt)
          case _ => r
        }
        input.update(0, value)
        aggFunc.update(buffer, input)
      }
      checkNDVs(
        ndvs = aggFunc.eval(buffer).asInstanceOf[ArrayData].toLongArray(),
        expectedNdvs = Array(3, 2, 1, 1, 1),
        rsd = aggFunc.relativeSD)
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
