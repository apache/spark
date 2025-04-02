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

import java.sql.Date

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, BoundReference, Cast, CreateArray, DecimalLiteral, GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.expressions.Cast._
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile.{PercentileDigest, PercentileDigestSerializer}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.util.{ArrayData, QuantileSummaries}
import org.apache.spark.sql.catalyst.util.QuantileSummaries.Stats
import org.apache.spark.sql.types.{ArrayType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, IntegralType, LongType}
import org.apache.spark.util.SizeEstimator

class ApproximatePercentileSuite extends SparkFunSuite {

  private val random = new java.util.Random()

  private val data = (0 until 10000).map { _ =>
    random.nextInt(10000)
  }

  test("serialize and de-serialize") {
    val serializer = new PercentileDigestSerializer

    // Check empty serialize and de-serialize
    val emptyBuffer = new PercentileDigest(relativeError = 0.01)
    assert(compareEquals(emptyBuffer, serializer.deserialize(serializer.serialize(emptyBuffer))))

    val buffer = new PercentileDigest(relativeError = 0.01)
    data.foreach { value =>
      buffer.add(value)
    }
    assert(compareEquals(buffer, serializer.deserialize(serializer.serialize(buffer))))

    val agg = new ApproximatePercentile(BoundReference(0, DoubleType, true), Literal(0.5))
    assert(compareEquals(agg.deserialize(agg.serialize(buffer)), buffer))
  }

  test("class PercentileDigest, basic operations") {
    val valueCount = 10000
    val percentages = Array(0.25, 0.5, 0.75)
    Seq(0.0001, 0.001, 0.01, 0.1).foreach { relativeError =>
      val buffer = new PercentileDigest(relativeError)
      (1 to valueCount).grouped(10).foreach { group =>
        val partialBuffer = new PercentileDigest(relativeError)
        group.foreach(x => partialBuffer.add(x))
        buffer.merge(partialBuffer)
      }
      val expectedPercentiles = percentages.map(_ * valueCount)
      val approxPercentiles = buffer.getPercentiles(Array(0.25, 0.5, 0.75))
      expectedPercentiles.zip(approxPercentiles).foreach { pair =>
        val (expected, estimate) = pair
        assert((estimate - expected) / valueCount <= relativeError)
      }
    }
  }

  test("class PercentileDigest, makes sure the memory foot print is bounded") {
    val relativeError = 0.01
    val memoryFootPrintUpperBound = {
      val headBufferSize =
        SizeEstimator.estimate(new Array[Double](QuantileSummaries.defaultHeadSize))
      val bufferSize = SizeEstimator.estimate(new Stats(0, 0, 0)) * (1 / relativeError) * 2
      // A safe upper bound
      (headBufferSize + bufferSize) * 2
    }

    Seq(100, 1000, 10000, 100000, 1000000, 10000000).foreach { count =>
      val buffer = new PercentileDigest(relativeError)
      // Worst case, data is linear sorted
      (0 until count).foreach(buffer.add(_))
      assert(SizeEstimator.estimate(buffer) < memoryFootPrintUpperBound)
    }
  }

  test("class ApproximatePercentile, high level interface, update, merge, eval...") {
    val count = 10000
    val data = (1 until 10000).toSeq
    val percentages = Array(0.25D, 0.5D, 0.75D)
    val accuracy = 10000
    val expectedPercentiles = percentages.map(count * _)
    val childExpression = Cast(BoundReference(0, IntegerType, nullable = false), DoubleType)
    val percentageExpression = CreateArray(percentages.toSeq.map(Literal(_)))
    val accuracyExpression = Literal(10000)
    val agg = new ApproximatePercentile(childExpression, percentageExpression, accuracyExpression)

    assert(agg.nullable)
    val group1 = (0 until data.length / 2)
    val group1Buffer = agg.createAggregationBuffer()
    group1.foreach { index =>
      val input = InternalRow(data(index))
      agg.update(group1Buffer, input)
    }

    val group2 = (data.length / 2 until data.length)
    val group2Buffer = agg.createAggregationBuffer()
    group2.foreach { index =>
      val input = InternalRow(data(index))
      agg.update(group2Buffer, input)
    }

    val mergeBuffer = agg.createAggregationBuffer()
    agg.merge(mergeBuffer, group1Buffer)
    agg.merge(mergeBuffer, group2Buffer)

    agg.eval(mergeBuffer) match {
      case arrayData: ArrayData =>
        val error = count / accuracy
        val percentiles = arrayData.toDoubleArray()
        assert(percentiles.zip(expectedPercentiles)
          .forall(pair => Math.abs(pair._1 - pair._2) < error))
    }
  }

  test("class ApproximatePercentile, low level interface, update, merge, eval...") {
    val childExpression = Cast(BoundReference(0, IntegerType, nullable = true), DoubleType)
    val inputAggregationBufferOffset = 1
    val mutableAggregationBufferOffset = 2
    val percentage = 0.5D

    // Phase one, partial mode aggregation
    val agg = new ApproximatePercentile(childExpression, Literal(percentage))
      .withNewInputAggBufferOffset(inputAggregationBufferOffset)
      .withNewMutableAggBufferOffset(mutableAggregationBufferOffset)

    val mutableAggBuffer = new GenericInternalRow(
      new Array[Any](mutableAggregationBufferOffset + 1))
    agg.initialize(mutableAggBuffer)
    val dataCount = 10
    (1 to dataCount).foreach { data =>
      agg.update(mutableAggBuffer, InternalRow(data))
    }
    agg.serializeAggregateBufferInPlace(mutableAggBuffer)

    // Serialize the aggregation buffer
    val serialized = mutableAggBuffer.getBinary(mutableAggregationBufferOffset)
    val inputAggBuffer = new GenericInternalRow(Array[Any](null, serialized))

    // Phase 2: final mode aggregation
    // Re-initialize the aggregation buffer
    agg.initialize(mutableAggBuffer)
    agg.merge(mutableAggBuffer, inputAggBuffer)
    val expectedPercentile = dataCount * percentage
    assert(Math.abs(agg.eval(mutableAggBuffer).asInstanceOf[Double] - expectedPercentile) < 0.1)
  }

  test("class ApproximatePercentile, sql string") {
    val defaultAccuracy = ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY
    // sql, single percentile
    assertEqual(
      s"percentile_approx(a, 0.5D, $defaultAccuracy)",
      new ApproximatePercentile("a".attr, percentageExpression = Literal(0.5D)).sql: String)

    // sql, array of percentile
    assertEqual(
      s"percentile_approx(a, array(0.25D, 0.5D, 0.75D), $defaultAccuracy)",
      new ApproximatePercentile(
        "a".attr,
        percentageExpression = CreateArray(Seq(0.25D, 0.5D, 0.75D).map(Literal(_)))
      ).sql: String)

    // sql(isDistinct = false), single percentile
    assertEqual(
      s"percentile_approx(a, 0.5D, $defaultAccuracy)",
      new ApproximatePercentile("a".attr, percentageExpression = Literal(0.5D))
        .sql(isDistinct = false))

    // sql(isDistinct = false), array of percentile
    assertEqual(
      s"percentile_approx(a, array(0.25D, 0.5D, 0.75D), $defaultAccuracy)",
      new ApproximatePercentile(
        "a".attr,
        percentageExpression = CreateArray(Seq(0.25D, 0.5D, 0.75D).map(Literal(_)))
      ).sql(isDistinct = false))

    // sql(isDistinct = true), single percentile
    assertEqual(
      s"percentile_approx(DISTINCT a, 0.5D, $defaultAccuracy)",
      new ApproximatePercentile("a".attr, percentageExpression = Literal(0.5D))
        .sql(isDistinct = true))

    // sql(isDistinct = true), array of percentile
    assertEqual(
      s"percentile_approx(DISTINCT a, array(0.25D, 0.5D, 0.75D), $defaultAccuracy)",
      new ApproximatePercentile(
        "a".attr,
        percentageExpression = CreateArray(Seq(0.25D, 0.5D, 0.75D).map(Literal(_)))
      ).sql(isDistinct = true))
  }

  test("class ApproximatePercentile, fails analysis if percentage or accuracy is not a constant") {
    val attribute = AttributeReference("a", DoubleType)()
    val accuracyExpression = AttributeReference("b", IntegerType)()
    val wrongAccuracy = new ApproximatePercentile(
      attribute,
      percentageExpression = Literal(0.5D),
      accuracyExpression = accuracyExpression)

    assertEqual(
      wrongAccuracy.checkInputDataTypes(),
      DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> toSQLId("accuracy"),
          "inputType" -> toSQLType(accuracyExpression.dataType),
          "inputExpr" -> toSQLExpr(accuracyExpression)
        )
      )
    )

    val wrongPercentage = new ApproximatePercentile(
      attribute,
      percentageExpression = attribute,
      accuracyExpression = Literal(10000))

    assertEqual(
      wrongPercentage.checkInputDataTypes(),
      DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> toSQLId("percentage"),
          "inputType" -> toSQLType(attribute.dataType),
          "inputExpr" -> toSQLExpr(attribute)
        )
      )
    )
  }

  test("class ApproximatePercentile, fails analysis if parameters are invalid") {
    val wrongAccuracyExpression = Literal(-1)
    val wrongAccuracy = new ApproximatePercentile(
      AttributeReference("a", DoubleType)(),
      percentageExpression = Literal(0.5D),
      accuracyExpression = wrongAccuracyExpression)
    assertEqual(
      wrongAccuracy.checkInputDataTypes(),
      DataTypeMismatch(
        errorSubClass = "VALUE_OUT_OF_RANGE",
        messageParameters = Map(
          "exprName" -> "accuracy",
          "valueRange" -> s"(0, ${Int.MaxValue}]",
          "currentValue" ->
            toSQLValue(wrongAccuracyExpression.eval().asInstanceOf[Number].longValue, LongType))
      )
    )

    val correctPercentageExpressions = Seq(
      Literal(0.1f, FloatType),
      Literal(Decimal(0.2), DecimalType(2, 1)),
      Literal(0D),
      Literal(1D),
      Literal(0.5D),
      CreateArray(Seq(0D, 1D, 0.5D).map(Literal(_)))
    )
    correctPercentageExpressions.foreach { percentageExpression =>
      val correctPercentage = new ApproximatePercentile(
        AttributeReference("a", DoubleType)(),
        percentageExpression = percentageExpression,
        accuracyExpression = Literal(100))

      // no exception should be thrown
      correctPercentage.checkInputDataTypes()
    }

    val wrongPercentageExpressions = Seq(
      Literal(1.1D),
      Literal(-0.5D),
      CreateArray(Seq(0D, 0.5D, 1.1D).map(Literal(_)))
    )

    wrongPercentageExpressions.foreach { percentageExpression =>
      val wrongPercentage = new ApproximatePercentile(
        AttributeReference("a", DoubleType)(),
        percentageExpression = percentageExpression,
        accuracyExpression = Literal(100))

      percentageExpression.eval() match {
        case array: ArrayData =>
          assertEqual(wrongPercentage.checkInputDataTypes(),
            DataTypeMismatch(
              errorSubClass = "VALUE_OUT_OF_RANGE",
              messageParameters = Map(
                "exprName" -> "percentage",
                "valueRange" -> "[0.0, 1.0]",
                "currentValue" ->
                  array.toDoubleArray().map(toSQLValue(_, DoubleType)).mkString(",")
              )
            )
          )
        case other =>
          assertEqual(wrongPercentage.checkInputDataTypes(),
            DataTypeMismatch(
              errorSubClass = "VALUE_OUT_OF_RANGE",
              messageParameters = Map(
                "exprName" -> "percentage",
                "valueRange" -> "[0.0, 1.0]",
                "currentValue" ->
                  Array(other).map(toSQLValue(_, DoubleType)).mkString(",")
              )
            )
          )
      }
    }
  }

  test("class ApproximatePercentile, automatically add type casting for parameters") {
    val testRelation = LocalRelation($"a".int)

    // accuracy types must be integral, no type casting
    val accuracyExpressions = Seq(
      Literal(1.toByte),
      Literal(100.toShort),
      Literal(100),
      Literal(1000L))
    // Compatible percentage types: float, decimal, string
    val percentageExpressions = Seq(Literal(0.3f), DecimalLiteral(0.5),
      Literal("0.2"),
      CreateArray(Seq(Literal(0.3f), Literal(0.5D), DecimalLiteral(0.7))))

    accuracyExpressions.foreach { accuracyExpression =>
      percentageExpressions.foreach { percentageExpression =>
        val agg = new ApproximatePercentile(
          UnresolvedAttribute("a"),
          percentageExpression,
          accuracyExpression)
        val analyzed = testRelation.select(agg).analyze.expressions.head
        analyzed match {
          case Alias(agg: ApproximatePercentile, _) =>
            assert(agg.resolved)
            assert(agg.child.dataType == IntegerType)
            assert(agg.percentageExpression.dataType == DoubleType ||
              agg.percentageExpression.dataType == ArrayType(DoubleType, containsNull = false))
            assert(agg.accuracyExpression.dataType.isInstanceOf[IntegralType])
          case _ => fail()
        }
      }
    }
  }

  test("ApproximatePercentile: nulls in percentage expression") {

    assert(new ApproximatePercentile(
      AttributeReference("a", DoubleType)(),
      percentageExpression = Literal(null, DoubleType)).checkInputDataTypes() ===
      DataTypeMismatch(errorSubClass = "UNEXPECTED_NULL", Map("exprName" -> "percentage")))

    val nullPercentageExprs =
      Seq(CreateArray(Seq(null).map(Literal(_))), CreateArray(Seq(0.1D, null).map(Literal(_))))
    nullPercentageExprs.foreach {
      percentageExpression =>
        val wrongPercentage = new ApproximatePercentile(
          AttributeReference("a", DoubleType)(),
          percentageExpression = percentageExpression,
          accuracyExpression = Literal(100))
        assert(wrongPercentage.checkInputDataTypes() ==
          DataTypeMismatch(
            errorSubClass = "UNEXPECTED_INPUT_TYPE",
            messageParameters = Map(
              "paramIndex" -> ordinalNumber(1),
              "requiredType" -> "(\"DOUBLE\" or \"ARRAY<DOUBLE>\")",
              "inputSql" -> toSQLExpr(percentageExpression),
              "inputType" -> "\"ARRAY<VOID>\"")
          )
        )
    }
  }

  test("ApproximatePercentile: invalid accuracy expressions") {
    val invalidAccuracies = Seq(null, 1.2f, 1.9d, BigDecimal(1.9), new Date(0), "1.5")
    invalidAccuracies.foreach { acc =>
      val wrongPercentage = new ApproximatePercentile(
        AttributeReference("a", DoubleType)(),
        percentageExpression = Literal(0.5),
        accuracyExpression = Literal(acc))
      assert(wrongPercentage.checkInputDataTypes() ==
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> ordinalNumber(2),
            "requiredType" -> "\"INTEGRAL\"",
            "inputSql" -> toSQLExpr(Literal(acc)),
            "inputType" -> toSQLType(Literal(acc).dataType)
          )
        )
      )
    }
  }

  test("class ApproximatePercentile, null handling") {
    val childExpression = Cast(BoundReference(0, IntegerType, nullable = true), DoubleType)
    val agg = new ApproximatePercentile(childExpression, Literal(0.5D))
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

  private def compareEquals(left: PercentileDigest, right: PercentileDigest): Boolean = {
    val leftSummary = left.quantileSummaries
    val rightSummary = right.quantileSummaries
    leftSummary.compressThreshold == rightSummary.compressThreshold &&
      leftSummary.relativeError == rightSummary.relativeError &&
      leftSummary.count == rightSummary.count &&
      leftSummary.sampled.sameElements(rightSummary.sampled)
  }

  private def assertEqual[T](left: T, right: T): Unit = {
    assert(left == right)
  }
}
