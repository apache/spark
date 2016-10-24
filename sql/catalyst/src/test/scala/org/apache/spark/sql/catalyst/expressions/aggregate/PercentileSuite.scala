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
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

class PercentileSuite extends SparkFunSuite {

  test("high level interface, update, merge, eval...") {
    val count = 10000
    val data = (1 to count)
    val percentages = Array(0, 0.25, 0.5, 0.75, 1)
    val expectedPercentiles = Array(1, 2500.75, 5000.5, 7500.25, 10000)
    val childExpression = Cast(BoundReference(0, IntegerType, nullable = false), DoubleType)
    val percentageExpression = CreateArray(percentages.toSeq.map(Literal(_)))
    val agg = new Percentile(childExpression, percentageExpression)

    assert(agg.nullable)
    val group = (0 until data.length)
    // Don't use group buffer for now.
    val groupBuffer = InternalRow.empty
    group.foreach { index =>
      val input = InternalRow(data(index))
      agg.update(groupBuffer, input)
    }

    // Don't support partial aggregations for now.
    val mergeBuffer = InternalRow.empty
    agg.eval(mergeBuffer) match {
      case arrayData: ArrayData =>
        val percentiles = arrayData.toDoubleArray()
        assert(percentiles.zip(expectedPercentiles)
          .forall(pair => pair._1 == pair._2))
    }
  }

  test("low level interface, update, merge, eval...") {
    val childExpression = Cast(BoundReference(0, IntegerType, nullable = true), DoubleType)
    val inputAggregationBufferOffset = 1
    val mutableAggregationBufferOffset = 2
    val percentage = 0.5

    // Test update.
    val agg = new Percentile(childExpression, Literal(percentage))
      .withNewInputAggBufferOffset(inputAggregationBufferOffset)
      .withNewMutableAggBufferOffset(mutableAggregationBufferOffset)

    val mutableAggBuffer = new GenericInternalRow(
      new Array[Any](mutableAggregationBufferOffset + 1))
    agg.initialize(mutableAggBuffer)
    val dataCount = 10
    (1 to dataCount).foreach { data =>
      agg.update(mutableAggBuffer, InternalRow(data))
    }

    // Test eval
    val expectedPercentile = 5.5
    assert(agg.eval(mutableAggBuffer).asInstanceOf[Double] == expectedPercentile)
  }

  test("call from sql query") {
    // sql, single percentile
    assertEqual(
      s"percentile(`a`, 0.5D)",
      new Percentile("a".attr, Literal(0.5)).sql: String)

    // sql, array of percentile
    assertEqual(
      s"percentile(`a`, array(0.25D, 0.5D, 0.75D))",
      new Percentile("a".attr, CreateArray(Seq(0.25, 0.5, 0.75).map(Literal(_)))).sql: String)

    // sql(isDistinct = false), single percentile
    assertEqual(
      s"percentile(`a`, 0.5D)",
      new Percentile("a".attr, Literal(0.5)).sql(isDistinct = false))

    // sql(isDistinct = false), array of percentile
    assertEqual(
      s"percentile(`a`, array(0.25D, 0.5D, 0.75D))",
      new Percentile("a".attr, CreateArray(Seq(0.25, 0.5, 0.75).map(Literal(_))))
        .sql(isDistinct = false))

    // sql(isDistinct = true), single percentile
    assertEqual(
      s"percentile(DISTINCT `a`, 0.5D)",
      new Percentile("a".attr, Literal(0.5)).sql(isDistinct = true))

    // sql(isDistinct = true), array of percentile
    assertEqual(
      s"percentile(DISTINCT `a`, array(0.25D, 0.5D, 0.75D))",
      new Percentile("a".attr, CreateArray(Seq(0.25, 0.5, 0.75).map(Literal(_))))
        .sql(isDistinct = true))
  }

  test("fail analysis if childExpression is invalid") {
    val validDataTypes = Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType,
      NullType)
    val percentage = Literal(0.5)

    validDataTypes.foreach { dataType =>
      val child = AttributeReference("a", dataType)()
      val percentile = new Percentile(child, percentage)
      assertEqual(percentile.checkInputDataTypes(), TypeCheckSuccess)
    }

    val invalidDataTypes = Seq(BooleanType, StringType, DateType, TimestampType, CalendarIntervalType)

    invalidDataTypes.foreach { dataType =>
      val child = AttributeReference("a", dataType)()
      val percentile = new Percentile(child, percentage)
      assertEqual(percentile.checkInputDataTypes(),
        TypeCheckFailure(s"function percentile requires numeric types, not $dataType"))
    }
  }

  test("fails analysis if percentage(s) are invalid") {
    val child = Cast(BoundReference(0, IntegerType, nullable = false), DoubleType)
    val groupBuffer = InternalRow.empty
    val input = InternalRow(1)

    val validPercentages = Seq(Literal(0), Literal(0.5), Literal(1),
      CreateArray(Seq(0, 0.5, 1).map(Literal(_))))

    validPercentages.foreach { percentage =>
      val percentile1 = new Percentile(child, percentage)
      // Make sure the inputs are not empty.
      percentile1.update(groupBuffer, input)
      // No exception should be thrown.
      assert(percentile1.eval() != null)
    }

    val invalidPercentages = Seq(Literal(-0.5), Literal(1.5), Literal(2),
      CreateArray(Seq(-0.5, 0, 2).map(Literal(_))))

    invalidPercentages.foreach { percentage =>
      val percentile2 = new Percentile(child, percentage)
      percentile2.update(groupBuffer, input)
      intercept[IllegalArgumentException](percentile2.eval(),
        s"Percentage values must be between 0.0 and 1.0")
    }

    val nonLiteralPercentage = Literal("val")

    val percentile3 = new Percentile(child, nonLiteralPercentage)
    percentile3.update(groupBuffer, input)
    intercept[AnalysisException](percentile3.eval(),
      s"Invalid data type ${nonLiteralPercentage.dataType} for parameter percentage")
  }

  test("null handling") {
    val childExpression = Cast(BoundReference(0, IntegerType, nullable = true), DoubleType)
    val agg = new Percentile(childExpression, Literal(0.5))
    val buffer = new GenericInternalRow(new Array[Any](1))
    agg.initialize(buffer)
    // Empty aggregation buffer
    assert(agg.eval(buffer) == null)
    // Empty input row
    agg.update(buffer, InternalRow(null))
    assert(agg.eval(buffer) == null)

    // Add some non-empty row
    agg.update(buffer, InternalRow(0))
    assert(agg.eval(buffer) != null)
  }

  private def assertEqual[T](left: T, right: T): Unit = {
    assert(left == right)
  }
}
