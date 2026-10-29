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
import org.apache.spark.unsafe.types.TimestampNanosVal
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

  test("SPARK-57557: Percentile and median support TIME type") {
    // The return type mirrors the input TIME type, preserving precision.
    assert(new Percentile(BoundReference(0, TimeType(), nullable = false), Literal(0.5))
      .dataType === TimeType())
    assert(Median(BoundReference(0, TimeType(3), nullable = false)).dataType === TimeType(3))

    // The exact median of two TIME values interpolates (continuous distribution): the midpoint is
    // returned as the internal Long value (nanos-of-day), e.g. median(0L, 10L) -> 5L.
    val agg = new Percentile(BoundReference(0, TimeType(), nullable = false), Literal(0.5))
    val buffer = agg.createAggregationBuffer()
    agg.update(buffer, InternalRow(0L))
    agg.update(buffer, InternalRow(10L))
    assert(agg.eval(buffer) === 5L)
  }

  private def nanosTs(epochMicros: Long, nanosWithinMicro: Int): TimestampNanosVal =
    TimestampNanosVal.fromParts(epochMicros, nanosWithinMicro.toShort)

  private def evalNanos(agg: PercentileBase, values: Seq[TimestampNanosVal]): Any = {
    val buffer = agg.createAggregationBuffer()
    values.foreach(v => agg.update(buffer, InternalRow(v)))
    agg.eval(buffer)
  }

  test("SPARK-60542: percentile family over nanosecond timestamps keeps the input type") {
    Seq(7, 8, 9).flatMap(p => Seq(TimestampNTZNanosType(p), TimestampLTZNanosType(p))).foreach {
      dt =>
        val child = BoundReference(0, dt, nullable = true)
        Seq(new Percentile(child, Literal(0.5)), PercentileDisc(child, Literal(0.5))).foreach {
          agg =>
            assert(agg.checkInputDataTypes() === TypeCheckSuccess)
            assert(agg.dataType === dt)
        }
        val percentages = CreateArray(Seq(Literal(0.25), Literal(0.75)))
        assert(new Percentile(child, percentages).dataType === ArrayType(dt, false))
        assert(Median(child).dataType === dt)
        assert(PercentileCont(child, Literal(0.5)).dataType === dt)
    }
  }

  test("SPARK-60542: percentile over nanosecond timestamps interpolates exactly") {
    // 2020-01-01 12:00:00.123456 UTC.
    val micros = 1577880000123456L
    val p9 = BoundReference(0, TimestampNTZNanosType(9), nullable = true)
    // Both inputs map to the same double, so a double interpolation can't return .123456790.
    assert(evalNanos(new Percentile(p9, Literal(0.5)),
      Seq(nanosTs(micros, 789), nanosTs(micros, 791))) === nanosTs(micros, 790))
    // The exact midpoint .1234567895 is floored to the precision.
    assert(evalNanos(new Percentile(p9, Literal(0.5)),
      Seq(nanosTs(micros, 789), nanosTs(micros, 790))) === nanosTs(micros, 789))
    // The result carries into the next microsecond.
    assert(evalNanos(new Percentile(p9, Literal(0.5)),
      Seq(nanosTs(micros, 999), nanosTs(micros + 1, 1))) === nanosTs(micros + 1, 0))
    // Pre-epoch: the midpoint of -1000ns and 1ns is -499.5ns, floored to -500ns.
    assert(evalNanos(new Percentile(p9, Literal(0.5)),
      Seq(nanosTs(-1, 0), nanosTs(0, 1))) === nanosTs(-1, 500))
    // 0001-01-01 00:00:00 and 9999-12-31 23:59:59.999999999: the span overflows a long in nanos.
    assert(evalNanos(new Percentile(p9, Literal(0.5)),
      Seq(nanosTs(-62135596800000000L, 0), nanosTs(253402300799999999L, 999))) ===
      nanosTs(95633351999999999L, 999))
    // Single value, p = 0 and p = 1, and an array of percentages.
    val values = Seq(nanosTs(micros, 100), nanosTs(micros, 200), nanosTs(micros, 900))
    assert(evalNanos(new Percentile(p9, Literal(0.5)), values.take(1)) === values.head)
    assert(evalNanos(new Percentile(p9, Literal(0.0)), values) === values.head)
    assert(evalNanos(new Percentile(p9, Literal(1.0)), values) === values.last)
    val percentages = CreateArray(Seq(Literal(0.25), Literal(0.75)))
    assert(evalNanos(new Percentile(p9, percentages), values).asInstanceOf[ArrayData]
      .toSeq[TimestampNanosVal](TimestampNTZNanosType(9)) ===
      Seq(nanosTs(micros, 150), nanosTs(micros, 550)))
    // Descending order (percentile_cont ... ORDER BY c DESC) mirrors the ascending result.
    val twoValues = Seq(nanosTs(0, 0), nanosTs(0, 3))
    assert(evalNanos(new Percentile(p9, Literal(0.25), reverse = true), twoValues) ===
      nanosTs(0, 2))
    assert(evalNanos(new Percentile(p9, Literal(0.75)), twoValues) === nanosTs(0, 2))

    // Results are floored to the 100ns (p = 7) and 10ns (p = 8) grids.
    val p7 = BoundReference(0, TimestampLTZNanosType(7), nullable = true)
    assert(evalNanos(new Percentile(p7, Literal(0.5)),
      Seq(nanosTs(micros, 100), nanosTs(micros, 400))) === nanosTs(micros, 200))
    assert(evalNanos(new Percentile(p7, Literal(0.5)),
      Seq(nanosTs(-1, 0), nanosTs(0, 100))) === nanosTs(-1, 500))
    val p8 = BoundReference(0, TimestampNTZNanosType(8), nullable = true)
    assert(evalNanos(new Percentile(p8, Literal(0.5)),
      Seq(nanosTs(micros, 10), nanosTs(micros, 40))) === nanosTs(micros, 20))
  }

  test("SPARK-60542: percentile over nanosecond timestamps uses an exact position") {
    val p9 = BoundReference(0, TimestampNTZNanosType(9), nullable = true)
    // 90 * 0.7 is 62.99999999999999 as a double, but the exact position is the 63rd input value.
    val spaced = (0 to 90).map(nanosTs(0, _))
    assert(evalNanos(new Percentile(p9, Literal(0.7)), spaced) === spaced(63))
    assert(evalNanos(new Percentile(p9, Literal(0.3)), Seq(nanosTs(0, 0), nanosTs(0, 10))) ===
      nanosTs(0, 3))
    // 3 * 0.7 is 2.0999999999999996 as a double; ascending 0.7 and descending 0.3 agree.
    val values = Seq(nanosTs(0, 100), nanosTs(0, 200), nanosTs(0, 300), nanosTs(1, 300))
    val p7 = BoundReference(0, TimestampLTZNanosType(7), nullable = true)
    Seq(p9, p7).foreach { child =>
      assert(evalNanos(new Percentile(child, Literal(0.7)), values) === nanosTs(0, 400))
      assert(evalNanos(new Percentile(child, Literal(0.3), reverse = true), values) ===
        nanosTs(0, 400))
    }
    // A tenth of a one-year gap is a whole number of nanoseconds.
    val year = 365L * 24 * 60 * 60 * 1000 * 1000
    assert(evalNanos(new Percentile(p9, Literal(0.7)),
      Seq(nanosTs(0, 0), nanosTs(0, 1), nanosTs(0, 2), nanosTs(year, 2))) ===
      nanosTs(year / 10, 2))
  }

  test("SPARK-60542: NaN percentage over nanosecond timestamps matches other types") {
    // NaN passes the range check; as for other types, the first value in sort order is returned.
    val child = BoundReference(0, TimestampNTZNanosType(9), nullable = true)
    val values = Seq(nanosTs(0, 100), nanosTs(0, 200), nanosTs(0, 300))
    val nan = Literal(Double.NaN)
    assert(evalNanos(new Percentile(child, nan), values) === values.head)
    assert(evalNanos(new Percentile(child, nan, reverse = true), values) === values.last)
    assert(evalNanos(new Percentile(child, CreateArray(Seq(Literal(0.5), nan))), values)
      .asInstanceOf[ArrayData].toSeq[TimestampNanosVal](TimestampNTZNanosType(9)) ===
      Seq(values(1), values.head))
    assert(evalNanos(PercentileDisc(child, nan), values) === values.head)
    assert(evalNanos(PercentileDisc(child, nan, legacyCalculation = true), values) === values.head)
    val doubles = new Percentile(BoundReference(0, DoubleType, nullable = true), nan)
    val buffer = doubles.createAggregationBuffer()
    Seq(1.0, 2.0, 3.0).foreach(v => doubles.update(buffer, InternalRow(v)))
    assert(doubles.eval(buffer) === 1.0)
  }

  test("SPARK-60542: percentile_disc over nanosecond timestamps returns an input value") {
    val child = BoundReference(0, TimestampNTZNanosType(9), nullable = true)
    val values = (1 to 4).map(nanosTs(1577880000123456L, _))
    assert(evalNanos(PercentileDisc(child, Literal(0.6)), values) === values(2))
    assert(evalNanos(PercentileDisc(child, Literal(0.6), legacyCalculation = true), values) ===
      values(1))
    assert(evalNanos(PercentileDisc(child, Literal(0.6), reverse = true), values) === values(1))
  }

  test("SPARK-60542: percentile over nanosecond timestamps merges serialized buffers") {
    val child = BoundReference(0, TimestampLTZNanosType(9), nullable = true)
    val agg = new Percentile(child, Literal(0.5))
    val buffer1 = agg.createAggregationBuffer()
    Seq(nanosTs(-1, 999), null).foreach(v => agg.update(buffer1, InternalRow(v)))
    val buffer2 = agg.createAggregationBuffer()
    agg.update(buffer2, InternalRow(nanosTs(0, 3)))
    val merged = agg.merge(
      agg.deserialize(agg.serialize(buffer1)), agg.deserialize(agg.serialize(buffer2)))
    assert(agg.eval(merged) === nanosTs(0, 1))
    assert(agg.eval(agg.createAggregationBuffer()) === null)
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
            "requiredType" -> ("((\"NUMERIC\" or \"INTERVAL DAY TO SECOND\" " +
              "or \"INTERVAL YEAR TO MONTH\") or \"TIME\" " +
              "or \"(TIMESTAMP_LTZ(P) OR TIMESTAMP_NTZ(P) WITH P IN [7, 9])\")"),
            "inputSql" -> "\"a\"",
            "inputType" -> toSQLType(dataType)
          )
        )
      )
    }

    val invalidFrequencyDataTypes = Seq(FloatType, DoubleType, BooleanType,
        StringType, DateType, TimestampType,
      CalendarIntervalType, NullType)

    for (dataType <- invalidDataTypes;
        frequencyType <- validFrequencyTypes) {
      val child = AttributeReference("a", dataType)()
      val frq = AttributeReference("frq", frequencyType)()
      val percentile = new Percentile(child, percentage, frq)
      assert(percentile.checkInputDataTypes() ==
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> ordinalNumber(0),
            "requiredType" -> ("((\"NUMERIC\" or \"INTERVAL DAY TO SECOND\" " +
              "or \"INTERVAL YEAR TO MONTH\") or \"TIME\" " +
              "or \"(TIMESTAMP_LTZ(P) OR TIMESTAMP_NTZ(P) WITH P IN [7, 9])\")"),
            "inputSql" -> "\"a\"",
            "inputType" -> toSQLType(dataType)
          )
        )
      )
    }

    for (dataType <- validDataTypes;
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
      parameters = Map("frequencyExpression" -> "\"boundreference()\"", "negativeValue" -> "-5L"))
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
