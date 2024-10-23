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
import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Cast._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.OpenHashMap

class PercentileSuite extends SparkFunSuite {

  private val random = new java.util.Random()

  private val data = (0 until 10000).map { _ =>
    random.nextInt(10000)
  }

  test("serialize and de-serialize") {
    val agg = new Percentile(BoundReference(0, IntegerType, true), Literal(0.5))

    // Check empty serialize and deserialize
    val buffer = new OpenHashMap[AnyRef, Long]()
    assert(compareEquals(agg.deserialize(agg.serialize(buffer)), buffer))

    // Check non-empty buffer serialize and deserialize.
    data.foreach { key =>
      buffer.changeValue(Integer.valueOf(key), 1L, _ + 1L)
    }
    assert(compareEquals(agg.deserialize(agg.serialize(buffer)), buffer))
  }

  test("class Percentile, high level interface, update, merge, eval...") {
    val count = 10000
    val percentages = Seq(0, 0.25, 0.5, 0.75, 1)
    val expectedPercentiles = Seq(1, 2500.75, 5000.5, 7500.25, 10000)
    val childExpression = Cast(BoundReference(0, IntegerType, nullable = false), DoubleType)
    val percentageExpression = CreateArray(percentages.toSeq.map(Literal(_)))
    val agg = new Percentile(childExpression, percentageExpression)

    // Test with rows without frequency
    val rows = (1 to count).map(x => Seq(x))
    runTest(agg, rows, expectedPercentiles)

    // Test with row with frequency. Second and third columns are frequency in Int and Long
    val countForFrequencyTest = 1000
    val rowsWithFrequency = (1 to countForFrequencyTest).map(x => Seq(x, x):+ x.toLong)
    val expectedPercentilesWithFrequency = Seq(1.0, 500.0, 707.0, 866.0, 1000.0)

    val frequencyExpressionInt = BoundReference(1, IntegerType, nullable = false)
    val aggInt = new Percentile(childExpression, percentageExpression, frequencyExpressionInt)
    runTest(aggInt, rowsWithFrequency, expectedPercentilesWithFrequency)

    val frequencyExpressionLong = BoundReference(2, LongType, nullable = false)
    val aggLong = new Percentile(childExpression, percentageExpression, frequencyExpressionLong)
    runTest(aggLong, rowsWithFrequency, expectedPercentilesWithFrequency)

    // Run test with Flatten data
    val flattenRows = (1 to countForFrequencyTest).flatMap(current =>
      (1 to current).map(y => current )).map(Seq(_))
    runTest(agg, flattenRows, expectedPercentilesWithFrequency)
  }

  private def runTest(agg: Percentile,
      rows : Seq[Seq[Any]],
      expectedPercentiles : Seq[Double]): Unit = {
    assert(agg.nullable)
    val group1 = (0 until rows.length / 2)
    val group1Buffer = agg.createAggregationBuffer()
    group1.foreach { index =>
      val input = InternalRow(rows(index): _*)
      agg.update(group1Buffer, input)
    }

    val group2 = (rows.length / 2 until rows.length)
    val group2Buffer = agg.createAggregationBuffer()
    group2.foreach { index =>
      val input = InternalRow(rows(index): _*)
      agg.update(group2Buffer, input)
    }

    val mergeBuffer = agg.createAggregationBuffer()
    agg.merge(mergeBuffer, group1Buffer)
    agg.merge(mergeBuffer, group2Buffer)

    agg.eval(mergeBuffer) match {
      case arrayData: ArrayData =>
        val percentiles = arrayData.toDoubleArray()
        assert(percentiles.zip(expectedPercentiles)
          .forall(pair => pair._1 == pair._2))
    }
  }

  test("class Percentile, low level interface, update, merge, eval...") {
    val childExpression = Cast(BoundReference(0, IntegerType, nullable = true), DoubleType)
    val inputAggregationBufferOffset = 1
    val mutableAggregationBufferOffset = 2
    val percentage = 0.5

    // Phase one, partial mode aggregation
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
    agg.serializeAggregateBufferInPlace(mutableAggBuffer)

    // Serialize the aggregation buffer
    val serialized = mutableAggBuffer.getBinary(mutableAggregationBufferOffset)
    val inputAggBuffer = new GenericInternalRow(Array[Any](null, serialized))

    // Phase 2: final mode aggregation
    // Re-initialize the aggregation buffer
    agg.initialize(mutableAggBuffer)
    agg.merge(mutableAggBuffer, inputAggBuffer)
    val expectedPercentile = 5.5
    assert(agg.eval(mutableAggBuffer).asInstanceOf[Double] == expectedPercentile)
  }

  test("fail analysis if childExpression is invalid") {
    val validDataTypes = Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType)
    val percentage = Literal(0.5)

    validDataTypes.foreach { dataType =>
      val child = AttributeReference("a", dataType)()
      val percentile = new Percentile(child, percentage)
      assertEqual(percentile.checkInputDataTypes(), TypeCheckSuccess)
    }

    val validFrequencyTypes = Seq(ByteType, ShortType, IntegerType, LongType)
    for (dataType <- validDataTypes;
      frequencyType <- validFrequencyTypes)  {
      val child = AttributeReference("a", dataType)()
      val frq = AttributeReference("frq", frequencyType)()
      val percentile = new Percentile(child, percentage, frq)
      assertEqual(percentile.checkInputDataTypes(), TypeCheckSuccess)
    }

    val invalidDataTypes = Seq(BooleanType, StringType, DateType, TimestampType,
      CalendarIntervalType, NullType)

    invalidDataTypes.foreach { dataType =>
      val child = AttributeReference("a", dataType)()
      val percentile = new Percentile(child, percentage)
      assert(percentile.checkInputDataTypes() ==
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> ordinalNumber(0),
            "requiredType" -> ("(\"NUMERIC\" or \"INTERVAL DAY TO SECOND\" " +
              "or \"INTERVAL YEAR TO MONTH\")"),
            "inputSql" -> "\"a\"",
            "inputType" -> toSQLType(dataType)
          )
        )
      )
    }

    val invalidFrequencyDataTypes = Seq(FloatType, DoubleType, BooleanType,
        StringType, DateType, TimestampType,
      CalendarIntervalType, NullType)

    for(dataType <- invalidDataTypes;
        frequencyType <- validFrequencyTypes) {
      val child = AttributeReference("a", dataType)()
      val frq = AttributeReference("frq", frequencyType)()
      val percentile = new Percentile(child, percentage, frq)
      assert(percentile.checkInputDataTypes() ==
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> ordinalNumber(0),
            "requiredType" -> ("(\"NUMERIC\" or \"INTERVAL DAY TO SECOND\" " +
              "or \"INTERVAL YEAR TO MONTH\")"),
            "inputSql" -> "\"a\"",
            "inputType" -> toSQLType(dataType)
          )
        )
      )
    }

    for(dataType <- validDataTypes;
        frequencyType <- invalidFrequencyDataTypes) {
      val child = AttributeReference("a", dataType)()
      val frq = AttributeReference("frq", frequencyType)()
      val percentile = new Percentile(child, percentage, frq)
      assert(percentile.checkInputDataTypes() ==
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> ordinalNumber(2),
            "requiredType" -> "\"INTEGRAL\"",
            "inputSql" -> "\"frq\"",
            "inputType" -> toSQLType(frequencyType)
          )
        )
      )
    }
  }

  test("fails analysis if percentage(s) are invalid") {
    val child = Cast(BoundReference(0, IntegerType, nullable = false), DoubleType)
    val validPercentages = Seq(Literal(0D), Literal(0.5), Literal(1D),
      CreateArray(Seq(0, 0.5, 1).map(Literal(_))))

    validPercentages.foreach { percentage =>
      val percentile1 = new Percentile(child, percentage)
      assertEqual(percentile1.checkInputDataTypes(), TypeCheckSuccess)
    }

    val invalidPercentages = Seq(Literal(-0.5), Literal(1.5), Literal(2D),
      CreateArray(Seq(-0.5, 0, 2).map(Literal(_))))

    invalidPercentages.foreach { percentage =>
      val percentile2 = new Percentile(child, percentage)
      percentage.eval() match {
        case array: ArrayData =>
          assertEqual(percentile2.checkInputDataTypes(),
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
          assertEqual(percentile2.checkInputDataTypes(),
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

    val nonFoldablePercentage = Seq(NonFoldableLiteral(0.5),
      CreateArray(Seq(0, 0.5, 1).map(NonFoldableLiteral(_))))

    nonFoldablePercentage.foreach { percentage =>
      val percentile3 = new Percentile(child, percentage)
      assertEqual(percentile3.checkInputDataTypes(),
        DataTypeMismatch(
          errorSubClass = "NON_FOLDABLE_INPUT",
          messageParameters = Map(
            "inputName" -> toSQLId("percentage"),
            "inputType" -> toSQLType(percentage.dataType),
            "inputExpr" -> toSQLExpr(percentage))
        )
      )
    }

    val invalidDataTypes = Seq(ByteType, ShortType, IntegerType, LongType, FloatType,
      BooleanType, StringType, DateType, TimestampType, CalendarIntervalType, NullType)

    invalidDataTypes.foreach { dataType =>
      val percentage = Literal.default(dataType)
      val percentile4 = new Percentile(child, percentage)
      val checkResult = percentile4.checkInputDataTypes()
      assert(checkResult ==
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> ordinalNumber(1),
            "requiredType" -> "\"DOUBLE\"",
            "inputSql" -> toSQLExpr(percentage),
            "inputType" -> toSQLType(dataType)
          )
        )
      )
    }
  }

  test("class ApproximatePercentile, automatically add type casting for parameters") {
    val testRelation = LocalRelation(AttributeReference("a", IntegerType)())

    // Compatible percentage types: float, decimal, string
    val percentageExpressions = Seq(Literal(0.3f), DecimalLiteral(0.5), Literal("0.2"),
      CreateArray(Seq(Literal(0.3f), Literal(0.5D), DecimalLiteral(0.7))))

    percentageExpressions.foreach { percentageExpression =>
      val agg = new Percentile(
        UnresolvedAttribute("a"),
        percentageExpression)
      val analyzed = testRelation.select(agg).analyze.expressions.head
      analyzed match {
        case Alias(agg: Percentile, _) =>
          assert(agg.resolved)
          assert(agg.child.dataType == IntegerType)
          assert(agg.percentageExpression.dataType == DoubleType ||
            agg.percentageExpression.dataType == ArrayType(DoubleType, containsNull = false))
        case _ => fail()
      }
    }
  }

  test("nulls in percentage expression") {
    assert(new Percentile(
      AttributeReference("a", DoubleType)(),
      percentageExpression = Literal(null, DoubleType)).checkInputDataTypes() ===
      DataTypeMismatch(errorSubClass = "UNEXPECTED_NULL", Map("exprName" -> "percentage")))

    val nullPercentageExprs =
      Seq(CreateArray(Seq(null).map(Literal(_))), CreateArray(Seq(0.1D, null).map(Literal(_))))

    nullPercentageExprs.foreach { percentageExpression =>
      val wrongPercentage = new Percentile(
        AttributeReference("a", DoubleType)(),
        percentageExpression = percentageExpression)
        assert(wrongPercentage.checkInputDataTypes() ==
          DataTypeMismatch(
            errorSubClass = "UNEXPECTED_INPUT_TYPE",
            messageParameters = Map(
              "paramIndex" -> ordinalNumber(1),
              "requiredType" -> "\"ARRAY<DOUBLE>\"",
              "inputSql" -> toSQLExpr(percentageExpression),
              "inputType" -> "\"ARRAY<VOID>\""
            )
          )
        )
    }
  }

  test("null handling") {

    // Percentile without frequency column
    val childExpression = Cast(BoundReference(0, IntegerType, nullable = true), DoubleType)
    val agg = new Percentile(childExpression, Literal(0.5))
    val buffer = new GenericInternalRow(new Array[Any](1))
    agg.initialize(buffer)

    // Empty aggregation buffer
    assert(agg.eval(buffer) == null)

    // Empty input row
    agg.update(buffer, InternalRow(null))
    assert(agg.eval(buffer) == null)

    // Percentile with Frequency column
    val frequencyExpression = Cast(BoundReference(1, IntegerType, nullable = true), IntegerType)
    val aggWithFrequency = new Percentile(childExpression, Literal(0.5), frequencyExpression)
    val bufferWithFrequency = new GenericInternalRow(new Array[Any](2))
    aggWithFrequency.initialize(bufferWithFrequency)

    // Empty aggregation buffer
    assert(aggWithFrequency.eval(bufferWithFrequency) == null)
    // Empty input row
    aggWithFrequency.update(bufferWithFrequency, InternalRow(null, null))
    assert(aggWithFrequency.eval(bufferWithFrequency) == null)

    // Add some non-empty row with empty frequency column
    aggWithFrequency.update(bufferWithFrequency, InternalRow(0, null))
    assert(aggWithFrequency.eval(bufferWithFrequency) == null)

    // Add some non-empty row with zero frequency
    aggWithFrequency.update(bufferWithFrequency, InternalRow(1, 0))
    assert(aggWithFrequency.eval(bufferWithFrequency) == null)

    // Add some non-empty row with positive frequency
    aggWithFrequency.update(bufferWithFrequency, InternalRow(0, 1))
    assert(aggWithFrequency.eval(bufferWithFrequency) != null)
  }

  test("negatives frequency column handling") {
    val childExpression = Cast(BoundReference(0, IntegerType, nullable = true), DoubleType)
    val freqExpression = Cast(BoundReference(1, IntegerType, nullable = true), IntegerType)
    val agg = new Percentile(childExpression, Literal(0.5), freqExpression)
    val buffer = new GenericInternalRow(new Array[Any](2))
    agg.initialize(buffer)

    checkError(
      exception =
        intercept[SparkIllegalArgumentException]{
          // Add some non-empty row with negative frequency
          agg.update(buffer, InternalRow(1, -5))
          agg.eval(buffer)
        },
      condition = "NEGATIVE_VALUES_IN_FREQUENCY_EXPRESSION",
      parameters = Map("frequencyExpression" -> "CAST(boundreference() AS INT)"))
  }

  private def compareEquals(
      left: OpenHashMap[AnyRef, Long], right: OpenHashMap[AnyRef, Long]): Boolean = {
    left.size == right.size && left.forall { case (key, count) =>
      right.apply(key) == count
    }
  }

  private def assertEqual[T](left: T, right: T): Unit = {
    assert(left == right)
  }
}
