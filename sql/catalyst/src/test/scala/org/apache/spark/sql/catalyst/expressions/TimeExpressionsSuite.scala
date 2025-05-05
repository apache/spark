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

package org.apache.spark.sql.catalyst.expressions

import java.time.LocalTime

import org.apache.spark.{SPARK_DOC_ROOT, SparkDateTimeException, SparkFunSuite}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{DataTypeMismatch, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.Cast.{toSQLId, toSQLValue}
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._
import org.apache.spark.sql.types.{Decimal, DecimalType, IntegerType, StringType, TimeType}

class TimeExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {
  test("ParseToTime") {
    checkEvaluation(new ToTime(Literal("00:00:00"), Literal.create(null)), null)
    checkEvaluation(new ToTime(Literal("00:00:00"), NonFoldableLiteral(null, StringType)), null)
    checkEvaluation(new ToTime(Literal(null, StringType), Literal("HH:mm:ss")), null)

    checkEvaluation(new ToTime(Literal("00:00:00")), localTime())
    checkEvaluation(new ToTime(Literal("23-59-00.000999"), Literal("HH-mm-ss.SSSSSS")),
      localTime(23, 59, 0, 999))
    checkEvaluation(
      new ToTime(Literal("12.00.59.90909"), NonFoldableLiteral("HH.mm.ss.SSSSS")),
      localTime(12, 0, 59, 909090))
    checkEvaluation(
      new ToTime(NonFoldableLiteral(" 12:00.909 "), Literal(" HH:mm.SSS ")),
      localTime(12, 0, 0, 909000))
    checkEvaluation(
      new ToTime(
        NonFoldableLiteral("12 hours 123 millis"),
        NonFoldableLiteral("HH 'hours' SSS 'millis'")),
      localTime(12, 0, 0, 123000))

    checkErrorInExpression[SparkDateTimeException](
      expression = new ToTime(Literal("100:50")),
      condition = "CANNOT_PARSE_TIME",
      parameters = Map("input" -> "'100:50'", "format" -> "'HH:mm:ss.SSSSSS'"))
    checkErrorInExpression[SparkDateTimeException](
      expression = new ToTime(Literal("100:50"), Literal("mm:HH")),
      condition = "CANNOT_PARSE_TIME",
      parameters = Map("input" -> "'100:50'", "format" -> "'mm:HH'"))
  }

  test("HourExpressionBuilder") {
    // Empty expressions list
    checkError(
      exception = intercept[AnalysisException] {
        HourExpressionBuilder.build("hour", Seq.empty)
      },
      condition = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
      parameters = Map(
        "functionName" -> "`hour`",
        "expectedNum" -> "> 0",
        "actualNum" -> "0",
        "docroot" -> SPARK_DOC_ROOT)
    )

    // test TIME-typed child should build HoursOfTime
    val timeExpr = Literal(localTime(12, 58, 59), TimeType())
    val builtExprForTime = HourExpressionBuilder.build("hour", Seq(timeExpr))
    assert(builtExprForTime.isInstanceOf[HoursOfTime])
    assert(builtExprForTime.asInstanceOf[HoursOfTime].child eq timeExpr)

    assert(builtExprForTime.checkInputDataTypes().isSuccess)

    // test TIME-typed child should build HoursOfTime for all allowed custom precision values
    (TimeType.MIN_PRECISION to TimeType.MICROS_PRECISION).foreach { precision =>
      val timeExpr = Literal(localTime(12, 58, 59), TimeType(precision))
      val builtExpr = HourExpressionBuilder.build("hour", Seq(timeExpr))

      assert(builtExpr.isInstanceOf[HoursOfTime])
      assert(builtExpr.asInstanceOf[HoursOfTime].child eq timeExpr)
      assert(builtExpr.checkInputDataTypes().isSuccess)
    }

    // test non TIME-typed child should build hour
    val tsExpr = Literal("2007-09-03 10:45:23")
    val builtExprForTs = HourExpressionBuilder.build("hour", Seq(tsExpr))
    assert(builtExprForTs.isInstanceOf[Hour])
    assert(builtExprForTs.asInstanceOf[Hour].child eq tsExpr)
  }

  test("Hour with TIME type") {
    // A few test times in microseconds since midnight:
    //   time in microseconds -> expected hour
    val testTimes = Seq(
      localTime() -> 0,
      localTime(1) -> 1,
      localTime(0, 59) -> 0,
      localTime(14, 30) -> 14,
      localTime(12, 58, 59) -> 12,
      localTime(23, 0, 1) -> 23,
      localTime(23, 59, 59, 999999) -> 23
    )

    // Create a literal with TimeType() for each test microsecond value
    // evaluate HoursOfTime(...), and check that the result matches the expected hour.
    testTimes.foreach { case (micros, expectedHour) =>
      checkEvaluation(
        HoursOfTime(Literal(micros, TimeType())),
        expectedHour)
    }

    // Verify NULL handling
    checkEvaluation(
      HoursOfTime(Literal.create(null, TimeType(TimeType.MICROS_PRECISION))),
      null
    )

    checkConsistencyBetweenInterpretedAndCodegen(
      (child: Expression) => HoursOfTime(child).replacement, TimeType())
  }

  test("MinuteExpressionBuilder") {
    // Empty expressions list
    checkError(
      exception = intercept[AnalysisException] {
        MinuteExpressionBuilder.build("minute", Seq.empty)
      },
      condition = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
      parameters = Map(
        "functionName" -> "`minute`",
        "expectedNum" -> "> 0",
        "actualNum" -> "0",
        "docroot" -> SPARK_DOC_ROOT)
    )

    // test TIME-typed child should build MinutesOfTime for default precision value
    val timeExpr = Literal(localTime(12, 58, 59), TimeType())
    val builtExprForTime = MinuteExpressionBuilder.build("minute", Seq(timeExpr))
    assert(builtExprForTime.isInstanceOf[MinutesOfTime])
    assert(builtExprForTime.asInstanceOf[MinutesOfTime].child eq timeExpr)
    assert(builtExprForTime.checkInputDataTypes().isSuccess)

    // test TIME-typed child should build MinutesOfTime for all allowed custom precision values
    (TimeType.MIN_PRECISION to TimeType.MICROS_PRECISION).foreach { precision =>
      val timeExpr = Literal(localTime(12, 58, 59), TimeType(precision))
      val builtExpr = MinuteExpressionBuilder.build("minute", Seq(timeExpr))

      assert(builtExpr.isInstanceOf[MinutesOfTime])
      assert(builtExpr.asInstanceOf[MinutesOfTime].child eq timeExpr)
      assert(builtExpr.checkInputDataTypes().isSuccess)
    }

    // test non TIME-typed child should build Minute
    val tsExpr = Literal("2009-07-30 12:58:59")
    val builtExprForTs = MinuteExpressionBuilder.build("minute", Seq(tsExpr))
    assert(builtExprForTs.isInstanceOf[Minute])
    assert(builtExprForTs.asInstanceOf[Minute].child eq tsExpr)
  }

  test("Minute with TIME type") {
    // A few test times in microseconds since midnight:
    //   time in microseconds -> expected minute
    val testTimes = Seq(
      localTime() -> 0,
      localTime(1) -> 0,
      localTime(0, 59) -> 59,
      localTime(14, 30) -> 30,
      localTime(12, 58, 59) -> 58,
      localTime(23, 0, 1) -> 0,
      localTime(23, 59, 59, 999999) -> 59
    )

    // Create a literal with TimeType() for each test microsecond value
    // evaluate MinutesOfTime(...), and check that the result matches the expected minute.
    testTimes.foreach { case (micros, expectedMinute) =>
      checkEvaluation(
        MinutesOfTime(Literal(micros, TimeType())),
        expectedMinute)
    }

    // Verify NULL handling
    checkEvaluation(
      MinutesOfTime(Literal.create(null, TimeType(TimeType.MICROS_PRECISION))),
      null
    )

    checkConsistencyBetweenInterpretedAndCodegen(
      (child: Expression) => MinutesOfTime(child).replacement, TimeType())
  }

  test("creating values of TimeType via make_time") {
    // basic case
    checkEvaluation(
      MakeTime(Literal(13), Literal(2), Literal(Decimal(23.5, 16, 6))),
      LocalTime.of(13, 2, 23, 500000000))

    // null cases
    checkEvaluation(
      MakeTime(Literal.create(null, IntegerType), Literal(18), Literal(Decimal(23.5, 16, 6))),
      null)
    checkEvaluation(
      MakeTime(Literal(13), Literal.create(null, IntegerType), Literal(Decimal(23.5, 16, 6))),
      null)
    checkEvaluation(MakeTime(Literal(13), Literal(18), Literal.create(null, DecimalType(16, 6))),
      null)

    // Invalid cases
    val errorCode = "DATETIME_FIELD_OUT_OF_BOUNDS.WITHOUT_SUGGESTION"
    checkErrorInExpression[SparkDateTimeException](
      MakeTime(Literal(25), Literal(2), Literal(Decimal(23.5, 16, 6))),
      errorCode,
      Map("rangeMessage" -> "Invalid value for HourOfDay (valid values 0 - 23): 25")
    )
    checkErrorInExpression[SparkDateTimeException](
      MakeTime(Literal(23), Literal(-1), Literal(Decimal(23.5, 16, 6))),
      errorCode,
      Map("rangeMessage" -> "Invalid value for MinuteOfHour (valid values 0 - 59): -1")
    )
    checkErrorInExpression[SparkDateTimeException](
      MakeTime(Literal(23), Literal(12), Literal(Decimal(100.5, 16, 6))),
      errorCode,
      Map("rangeMessage" -> "Invalid value for SecondOfMinute (valid values 0 - 59): 100")
    )
  }

  test("SecondExpressionBuilder") {
    // Empty expressions list
    checkError(
      exception = intercept[AnalysisException] {
        SecondExpressionBuilder.build("second", Seq.empty)
      },
      condition = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
      parameters = Map(
        "functionName" -> "`second`",
        "expectedNum" -> "> 0",
        "actualNum" -> "0",
        "docroot" -> SPARK_DOC_ROOT)
    )

    // test TIME-typed child should build SecondsOfTime
    val timeExpr = Literal(localTime(12, 58, 59), TimeType())
    val builtExprForTime = SecondExpressionBuilder.build("second", Seq(timeExpr))
    assert(builtExprForTime.isInstanceOf[SecondsOfTime])
    assert(builtExprForTime.asInstanceOf[SecondsOfTime].child eq timeExpr)

    // test non TIME-typed child should build second
    val tsExpr = Literal("2007-09-03 10:45:23")
    val builtExprForTs = SecondExpressionBuilder.build("second", Seq(tsExpr))
    assert(builtExprForTs.isInstanceOf[Second])
    assert(builtExprForTs.asInstanceOf[Second].child eq tsExpr)
  }

  test("Second with TIME type") {
    // A few test times in microseconds since midnight:
    //   time in microseconds -> expected second
    val testTimes = Seq(
      localTime() -> 0,
      localTime(1) -> 0,
      localTime(0, 59) -> 0,
      localTime(14, 30) -> 0,
      localTime(12, 58, 59) -> 59,
      localTime(23, 0, 1) -> 1,
      localTime(23, 59, 59, 999999) -> 59
    )

    // Create a literal with TimeType() for each test microsecond value
    // evaluate SecondsOfTime(...), and check that the result matches the expected second.
    testTimes.foreach { case (micros, expectedSecond) =>
      checkEvaluation(
        SecondsOfTime(Literal(micros, TimeType())),
        expectedSecond)
    }

    // Verify NULL handling
    checkEvaluation(
      SecondsOfTime(Literal.create(null, TimeType(TimeType.MICROS_PRECISION))),
      null
    )

    checkConsistencyBetweenInterpretedAndCodegen(
      (child: Expression) => SecondsOfTime(child).replacement, TimeType())
  }

  test("CurrentTime") {
    // test valid precision
    var expr = CurrentTime(Literal(3))
    assert(expr.dataType == TimeType(3), "Should produce TIME(3) data type")
    assert(expr.checkInputDataTypes() == TypeCheckSuccess)

    // test default constructor => TIME(6)
    expr = CurrentTime()
    assert(expr.precision == 6, "Default precision should be 6")
    assert(expr.dataType == TimeType(6))
    assert(expr.checkInputDataTypes() == TypeCheckSuccess)

    // test no value => TIME()
    expr = CurrentTime()
    assert(expr.precision == 6, "Default precision should be 6")
    assert(expr.dataType == TimeType(6))
    assert(expr.checkInputDataTypes() == TypeCheckSuccess)

    // test foldable value
    expr = CurrentTime(Literal(1 + 1))
    assert(expr.precision == 2, "Precision should be 2")
    assert(expr.dataType == TimeType(2))
    assert(expr.checkInputDataTypes() == TypeCheckSuccess)

    // test out of range precision => checkInputDataTypes fails
    expr = CurrentTime(Literal(2 + 8))
    assert(expr.checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "VALUE_OUT_OF_RANGE",
        messageParameters = Map(
          "exprName" -> toSQLId("precision"),
          "valueRange" -> s"[${TimeType.MIN_PRECISION}, ${TimeType.MICROS_PRECISION}]",
          "currentValue" -> toSQLValue(10, IntegerType)
        )
      )
    )

    // test non number value should fail since we skip analyzer here
    expr = CurrentTime(Literal("2"))
    val failure = intercept[ClassCastException] {
      expr.precision
    }
    assert(failure.getMessage.contains("cannot be cast to class java.lang.Number"))
  }

  test("Second with fraction from  TIME type") {
    val time = "13:11:15.987654321"
    assert(
      SecondsOfTimeWithFraction(
        Cast(Literal(time), TimeType(TimeType.MICROS_PRECISION))).resolved)
    assert(
      SecondsOfTimeWithFraction(
        Cast(Literal.create(time), TimeType(TimeType.MIN_PRECISION + 3))).resolved)
    Seq(
      0 -> 15.0,
      1 -> 15.9,
      2 -> 15.98,
      3 -> 15.987,
      4 -> 15.9876,
      5 -> 15.98765,
      6 -> 15.987654).foreach { case (precision, expected) =>
      checkEvaluation(
        SecondsOfTimeWithFraction(Literal(localTime(13, 11, 15, 987654), TimeType(precision))),
        BigDecimal(expected))
    }
    // Verify NULL handling
    checkEvaluation(
      SecondsOfTimeWithFraction(Literal.create(null, TimeType(TimeType.MICROS_PRECISION))),
      null)
    checkConsistencyBetweenInterpretedAndCodegen(
      (child: Expression) => SecondsOfTimeWithFraction(child).replacement,
      TimeType())
  }
}
