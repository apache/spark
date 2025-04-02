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

import java.sql.Timestamp
import java.time.{Duration, Period}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, DslString, StringToAttributeConversionHelper}
import org.apache.spark.sql.catalyst.dsl.plans.DslLogicalPlan
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, BoundReference, Cast, GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.NumericHistogram

class HistogramNumericSuite extends SparkFunSuite with SQLHelper {

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
      DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> "`nb`",
          "inputType" -> "\"INT\"",
          "inputExpr" -> "\"b\""
        )
      )
    )
  }

  test("class HistogramNumeric, fails analysis if nBins is invalid") {
    val attribute = AttributeReference("a", IntegerType)()
    val wrongNB = new HistogramNumeric(attribute, nBins = Literal(1))

    assertEqual(
      wrongNB.checkInputDataTypes(),
      DataTypeMismatch(
        errorSubClass = "VALUE_OUT_OF_RANGE",
        messageParameters = Map(
          "exprName" -> "nb",
          "valueRange" -> s"[2, ${Int.MaxValue}]",
          "currentValue" -> "1"
        )
      )
    )
  }

  test("class HistogramNumeric, automatically add type casting for parameters") {
    // These are the types of input relations under test. We exercise the unit test with several
    // input column types to inspect the behavior of query analysis for the aggregate function.
    val relations = Seq(LocalRelation($"a".double),
      LocalRelation($"a".int),
      LocalRelation($"a".timestamp),
      LocalRelation($"a".dayTimeInterval()),
      LocalRelation($"a".yearMonthInterval()))

    // These are the types of the second 'nbins' argument to the aggregate function.
    // These accuracy types must be integral, no type casting is allowed.
    val nBinsExpressions = Seq(
      Literal(2.toByte),
      Literal(100.toShort),
      Literal(100),
      Literal(1000L))

    // Iterate through each of the input relation column types and 'nbins' expression types under
    // test.
    for {
      relation <- relations
      nBins <- nBinsExpressions
    } {
      // We expect each relation under test to have exactly one output attribute.
      assert(relation.output.length == 1)
      val relationAttributeType = relation.output(0).dataType
      val agg = new HistogramNumeric(UnresolvedAttribute("a"), nBins)
      val analyzed = relation.select(agg).analyze.expressions.head
      analyzed match {
        case Alias(agg: HistogramNumeric, _) =>
          assert(agg.resolved)
          assert(agg.child.dataType == relationAttributeType)
          assert(agg.nBins.dataType == IntegerType)
          // We expect the output type of the histogram aggregate function to be an array of structs
          // where the first element of each struct has the same type as the original input
          // attribute.
          val expectedType =
          ArrayType(
            StructType(Seq(
              StructField("x", relationAttributeType, nullable = true),
              StructField("y", DoubleType, nullable = true))))
          assert(agg.dataType == expectedType)
        case _ => fail()
      }
    }
  }

  test("HistogramNumeric: nulls in nBins expression") {
    assertEqual(
      new HistogramNumeric(
        AttributeReference("a", DoubleType)(),
        Literal(null, IntegerType)).checkInputDataTypes(),
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_NULL",
        messageParameters = Map("exprName" -> "nb")
      )
    )
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

  test("class HistogramNumeric, exercise many different numeric input types") {
    val inputs = Seq(
      (Literal(null),
        Literal(null),
        Literal(null)),
      (Literal(0),
        Literal(1),
        Literal(2)),
      (Literal(0L),
        Literal(1L),
        Literal(2L)),
      (Literal(0.toShort),
        Literal(1.toShort),
        Literal(2.toShort)),
      (Literal(0F),
        Literal(1F),
        Literal(2F)),
      (Literal(0D),
        Literal(1D),
        Literal(2D)),
      (Literal(Timestamp.valueOf("2017-03-01 00:00:00")),
        Literal(Timestamp.valueOf("2017-03-02 00:00:00")),
        Literal(Timestamp.valueOf("2017-03-03 00:00:00"))),
      (Literal(Duration.ofSeconds(1111)),
        Literal(Duration.ofSeconds(1211)),
        Literal(Duration.ofSeconds(1311))),
      (Literal(Period.ofMonths(10)),
        Literal(Period.ofMonths(11)),
        Literal(Period.ofMonths(12))))
    for ((left, middle, right) <- inputs) {
      // Check that the 'propagateInputType' bit correctly toggles the output type.
      withSQLConf(SQLConf.HISTOGRAM_NUMERIC_PROPAGATE_INPUT_TYPE.key -> "false") {
        val aggDoubleOutputType = new HistogramNumeric(
          BoundReference(0, left.dataType, nullable = true), Literal(5))
        assert(aggDoubleOutputType.dataType match {
          case ArrayType(StructType(Array(
          StructField("x", DoubleType, _, _),
          StructField("y", _, _, _))), true) => true
        })
      }
      val aggPropagateOutputType = new HistogramNumeric(
        BoundReference(0, left.dataType, nullable = true), Literal(5))
      assert(aggPropagateOutputType.left.dataType ==
        (aggPropagateOutputType.dataType match {
          case
            ArrayType(StructType(Array(
            StructField("x", lhs@_, true, _),
            StructField("y", _, true, _))), true) => lhs
        }))
      // Now consume some input values and check the result.
      val buffer = new GenericInternalRow(new Array[Any](1))
      aggPropagateOutputType.initialize(buffer)
      // Consume three non-empty rows in the aggregation.
      aggPropagateOutputType.update(buffer, InternalRow(left.value))
      aggPropagateOutputType.update(buffer, InternalRow(middle.value))
      aggPropagateOutputType.update(buffer, InternalRow(right.value))
      // Evaluate the aggregate function.
      val result = aggPropagateOutputType.eval(buffer)
      if (left.dataType != NullType) {
        assert(result != null)
        // Sanity-check the sum of the heights.
        var ys = 0.0
        result match {
          case v: GenericArrayData =>
            for (row <- v.array) {
              row match {
                case r: GenericInternalRow =>
                  assert(r.values.length == 2)
                  ys += r.values(1).asInstanceOf[Double]
              }
            }
        }
        assert(ys > 1)
      }
      // As a basic sanity check, the sum of the heights of the bins should be greater than one.
    }
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
