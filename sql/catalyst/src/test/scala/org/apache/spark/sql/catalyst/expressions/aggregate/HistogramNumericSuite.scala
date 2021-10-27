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
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.dsl.expressions.{DslString, DslSymbol}
import org.apache.spark.sql.catalyst.dsl.plans.DslLogicalPlan
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, BoundReference, Cast, GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.util.NumericHistogram

class HistogramNumericSuite extends SparkFunSuite {

  private val random = new java.util.Random()

  private val data = (0 until 10000).map { _ =>
    random.nextInt(10000)
  }

  test("serialize and de-serialize") {

    // Check empty serialize and de-serialize
    val emptyBuffer = new NumericHistogram()
    emptyBuffer.allocate(5)
    assert(compareEquals(emptyBuffer,
      NumericHistogramSerializer.deserialize(NumericHistogramSerializer.serialize(emptyBuffer))))

    val buffer = new NumericHistogram()
    buffer.allocate(data.size / 3)
    data.foreach { value =>
      buffer.add(value)
    }
    assert(compareEquals(buffer,
      NumericHistogramSerializer.deserialize(NumericHistogramSerializer.serialize(buffer))))

    val agg = new HistogramNumeric(BoundReference(0, DoubleType, true), Literal(5))
    assert(compareEquals(agg.deserialize(agg.serialize(buffer)), buffer))
  }

  test("class NumericHistogram, basic operations") {
    val valueCount = 5
    Seq(3, 5).foreach { nBins: Int =>
      val buffer = new NumericHistogram()
      buffer.allocate(nBins)
      (1 to valueCount).grouped(nBins).foreach { group =>
        val partialBuffer = new NumericHistogram()
        partialBuffer.allocate(nBins)
        group.foreach(x => partialBuffer.add(x))
        buffer.merge(partialBuffer)
      }
      val sum = (0 until buffer.getUsedBins).map { i =>
        val coord = buffer.getBin(i)
        coord.x * coord.y
      }.sum
      assert(sum <= (1 to valueCount).sum)
    }
  }

  test("class HistogramNumeric, sql string") {
    val defaultAccuracy = ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY
    assertEqual(s"histogram_numeric(a, 3)",
      new HistogramNumeric("a".attr, Literal(3)).sql: String)

    // sql(isDistinct = true), array of percentile
    assertEqual(s"histogram_numeric(DISTINCT a, 3)",
      new HistogramNumeric("a".attr, Literal(3)).sql(isDistinct = true))
  }

  test("class HistogramNumeric, fails analysis if nBins is not a constant") {
    val attribute = AttributeReference("a", IntegerType)()
    val wrongNB = new HistogramNumeric(attribute, nBins = AttributeReference("b", IntegerType)())

    assertEqual(
      wrongNB.checkInputDataTypes(),
      TypeCheckFailure("histogram_numeric needs the nBins provided must be a constant literal.")
    )
  }

  test("class HistogramNumeric, fails analysis if nBins is invalid") {
    val attribute = AttributeReference("a", IntegerType)()
    val wrongNB = new HistogramNumeric(attribute, nBins = Literal(1))

    assertEqual(
      wrongNB.checkInputDataTypes(),
      TypeCheckFailure("histogram_numeric needs nBins to be at least 2, but you supplied 1.")
    )
  }

  test("class HistogramNumeric, automatically add type casting for parameters") {
    val testRelation = LocalRelation('a.int)

    // accuracy types must be integral, no type casting
    val nBinsExpressions = Seq(
      Literal(2.toByte),
      Literal(100.toShort),
      Literal(100),
      Literal(1000L))

    nBinsExpressions.foreach { nBins =>
      val agg = new HistogramNumeric(UnresolvedAttribute("a"), nBins)
      val analyzed = testRelation.select(agg).analyze.expressions.head
      analyzed match {
        case Alias(agg: HistogramNumeric, _) =>
          assert(agg.resolved)
          assert(agg.child.dataType == IntegerType)
          assert(agg.nBins.dataType == IntegerType)
        case _ => fail()
      }
    }
  }

  test("HistogramNumeric: nulls in nBins expression") {
    assert(new HistogramNumeric(
      AttributeReference("a", DoubleType)(),
      Literal(null, IntegerType)).checkInputDataTypes() ===
      TypeCheckFailure("histogram_numeric needs nBins value must not be null."))
  }

  test("class HistogramNumeric, null handling") {
    val childExpression = Cast(BoundReference(0, IntegerType, nullable = true), DoubleType)
    val agg = new HistogramNumeric(childExpression, Literal(5))
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

  private def compareEquals(left: NumericHistogram, right: NumericHistogram): Boolean = {
    left.getNumBins == right.getNumBins && left.getUsedBins == right.getUsedBins &&
      (0 until left.getUsedBins).forall { i =>
        val leftCoord = left.getBin(i)
        val rightCoord = right.getBin(i)
        leftCoord.x == rightCoord.x && leftCoord.y == rightCoord.y
      }
  }

  private def assertEqual[T](left: T, right: T): Unit = {
    assert(left == right)
  }
}
