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

import java.time.{Duration, LocalTime}

import org.apache.spark.{SPARK_DOC_ROOT, SparkDateTimeException, SparkFunSuite, SparkIllegalArgumentException}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{DataTypeMismatch, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.Cast.{toSQLId, toSQLValue}
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._
import org.apache.spark.sql.catalyst.util.SparkDateTimeUtils.localTimeToNanos
import org.apache.spark.sql.types.{DayTimeIntervalType, Decimal, DecimalType, IntegerType, LongType, StringType, TimeType}
import org.apache.spark.sql.types.DayTimeIntervalType.{DAY, HOUR, SECOND}

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
    assert(SecondsOfTimeWithFraction(Literal.create(null)).inputTypes.nonEmpty)

    checkConsistencyBetweenInterpretedAndCodegen(
      (child: Expression) => SecondsOfTimeWithFraction(child).replacement,
      TimeType())
  }

  test("Add ANSI day-time intervals to TIME") {
    checkEvaluation(
      TimeAddInterval(Literal.create(null, TimeType()), Literal(Duration.ofHours(1))),
      null)
    checkEvaluation(
      TimeAddInterval(Literal(LocalTime.of(12, 30)), Literal(null, DayTimeIntervalType(SECOND))),
      null)
    checkEvaluation(
      TimeAddInterval(Literal(LocalTime.of(8, 31)), Literal(Duration.ofMinutes(30))),
      LocalTime.of(8, 31).plusMinutes(30))
    // Maximum precision of TIME and DAY-TIME INTERVAL
    assert(TimeAddInterval(
      Literal(0L, TimeType(0)),
      Literal(0L, DayTimeIntervalType(DAY))).dataType == TimeType(0))
    assert(TimeAddInterval(
      Literal(1L, TimeType(TimeType.MAX_PRECISION)),
      Literal(1L, DayTimeIntervalType(HOUR))).dataType == TimeType(TimeType.MAX_PRECISION))
    assert(TimeAddInterval(
      Literal(2L, TimeType(TimeType.MIN_PRECISION)),
      Literal(2L, DayTimeIntervalType(SECOND))).dataType == TimeType(TimeType.MICROS_PRECISION))
    assert(TimeAddInterval(
      Literal(3L, TimeType(TimeType.MAX_PRECISION)),
      Literal(3L, DayTimeIntervalType(SECOND))).dataType == TimeType(TimeType.MAX_PRECISION))
    checkConsistencyBetweenInterpretedAndCodegenAllowingException(
      (time: Expression, interval: Expression) => TimeAddInterval(time, interval).replacement,
      TimeType(), DayTimeIntervalType())
  }

  test("SPARK-51555: Time difference") {
    // Test cases for various difference units - from 09:32:05.359123 until 17:23:49.906152.
    val startTime: Long = localTime(9, 32, 5, 359123)
    val startTimeLit: Expression = Literal(startTime, TimeType())
    val endTime: Long = localTime(17, 23, 49, 906152)
    val endTimeLit: Expression = Literal(endTime, TimeType())

    // Test differences for valid units.
    checkEvaluation(TimeDiff(Literal("HOUR"), startTimeLit, endTimeLit), 7L)
    checkEvaluation(TimeDiff(Literal("MINUTE"), startTimeLit, endTimeLit), 471L)
    checkEvaluation(TimeDiff(Literal("SECOND"), startTimeLit, endTimeLit), 28304L)
    checkEvaluation(TimeDiff(Literal("MILLISECOND"), startTimeLit, endTimeLit), 28304547L)
    checkEvaluation(TimeDiff(Literal("MICROSECOND"), startTimeLit, endTimeLit), 28304547029L)

    // Test case-insensitive units.
    checkEvaluation(TimeDiff(Literal("hour"), startTimeLit, endTimeLit), 7L)
    checkEvaluation(TimeDiff(Literal("Minute"), startTimeLit, endTimeLit), 471L)
    checkEvaluation(TimeDiff(Literal("seconD"), startTimeLit, endTimeLit), 28304L)
    checkEvaluation(TimeDiff(Literal("milliSECOND"), startTimeLit, endTimeLit), 28304547L)
    checkEvaluation(TimeDiff(Literal("mIcRoSeCoNd"), startTimeLit, endTimeLit), 28304547029L)

    // Test invalid units.
    val invalidUnits: Seq[String] = Seq("MS", "INVALID", "ABC", "XYZ", " ", "")
    invalidUnits.foreach { unit =>
      checkErrorInExpression[SparkIllegalArgumentException](
        TimeDiff(Literal(unit), startTimeLit, endTimeLit),
        condition = "INVALID_PARAMETER_VALUE.TIME_UNIT",
        parameters = Map(
          "functionName" -> "`time_diff`",
          "parameter" -> "`unit`",
          "invalidValue" -> s"'$unit'"
        )
      )
    }

    // Test null inputs.
    val nullUnit = Literal.create(null, StringType)
    val nullTime = Literal.create(null, TimeType())
    checkEvaluation(TimeDiff(nullUnit, startTimeLit, endTimeLit), null)
    checkEvaluation(TimeDiff(Literal("hour"), nullTime, endTimeLit), null)
    checkEvaluation(TimeDiff(Literal("hour"), startTimeLit, nullTime), null)
    checkEvaluation(TimeDiff(nullUnit, nullTime, endTimeLit), null)
    checkEvaluation(TimeDiff(nullUnit, startTimeLit, nullTime), null)
    checkEvaluation(TimeDiff(Literal("hour"), nullTime, nullTime), null)
    checkEvaluation(TimeDiff(nullUnit, nullTime, nullTime), null)
  }

  test("Subtract times") {
    checkEvaluation(
      SubtractTimes(Literal.create(null, TimeType()), Literal(LocalTime.MIN)),
      null)
    checkEvaluation(
      SubtractTimes(Literal(LocalTime.MAX), Literal(null, TimeType())),
      null)
    checkEvaluation(
      SubtractTimes(
        Literal(LocalTime.of(8, 31).plusMinutes(30)),
        Literal(LocalTime.of(8, 31))),
      Duration.ofMinutes(30))
    assert(SubtractTimes(
      Literal(0L, TimeType(0)),
      Literal(0L, TimeType(0))).dataType == DayTimeIntervalType(HOUR, SECOND))

    for (i <- TimeType.MIN_PRECISION to TimeType.MAX_PRECISION) {
      for (j <- TimeType.MIN_PRECISION to TimeType.MAX_PRECISION) {
        checkConsistencyBetweenInterpretedAndCodegenAllowingException(
          (end: Expression, start: Expression) => SubtractTimes(end, start).replacement,
          TimeType(i), TimeType(j))
      }
    }
  }

  test("SPARK-51554: TimeTrunc") {
    // Test cases for different truncation units - 09:32:05.359123.
    val testTime = localTime(9, 32, 5, 359123)

    // Test HOUR truncation.
    checkEvaluation(
      TimeTrunc(Literal("HOUR"), Literal(testTime, TimeType())),
      localTime(9, 0, 0, 0)
    )
    // Test MINUTE truncation.
    checkEvaluation(
      TimeTrunc(Literal("MINUTE"), Literal(testTime, TimeType())),
      localTime(9, 32, 0, 0)
    )
    // Test SECOND truncation.
    checkEvaluation(
      TimeTrunc(Literal("SECOND"), Literal(testTime, TimeType())),
      localTime(9, 32, 5, 0)
    )
    // Test MILLISECOND truncation.
    checkEvaluation(
      TimeTrunc(Literal("MILLISECOND"), Literal(testTime, TimeType())),
      localTime(9, 32, 5, 359000)
    )
    // Test MICROSECOND truncation.
    checkEvaluation(
      TimeTrunc(Literal("MICROSECOND"), Literal(testTime, TimeType())),
      testTime
    )

    // Test case-insensitive units.
    checkEvaluation(
      TimeTrunc(Literal("hour"), Literal(testTime, TimeType())),
      localTime(9, 0, 0, 0)
    )
    checkEvaluation(
      TimeTrunc(Literal("Hour"), Literal(testTime, TimeType())),
      localTime(9, 0, 0, 0)
    )
    checkEvaluation(
      TimeTrunc(Literal("hoUR"), Literal(testTime, TimeType())),
      localTime(9, 0, 0, 0)
    )

    // Test invalid units.
    val invalidUnits: Seq[String] = Seq("MS", "INVALID", "ABC", "XYZ", " ", "")
    invalidUnits.foreach { unit =>
      checkError(
        exception = intercept[SparkIllegalArgumentException] {
          TimeTrunc(Literal(unit), Literal(testTime, TimeType())).eval()
        },
        condition = "INVALID_PARAMETER_VALUE.TIME_UNIT",
        parameters = Map(
          "functionName" -> "`time_trunc`",
          "parameter" -> "`unit`",
          "invalidValue" -> s"'$unit'"
        )
      )
    }

    // Test null inputs.
    checkEvaluation(
      TimeTrunc(Literal.create(null, StringType), Literal(testTime, TimeType())),
      null
    )
    checkEvaluation(
      TimeTrunc(Literal("HOUR"), Literal.create(null, TimeType())),
      null
    )
    checkEvaluation(
      TimeTrunc(Literal.create(null, StringType), Literal.create(null, TimeType())),
      null
    )

    // Test edge cases.
    val midnightTime = localTime(0, 0, 0, 0)
    val supportedUnits: Seq[String] = Seq("HOUR", "MINUTE", "SECOND", "MILLISECOND", "MICROSECOND")
    supportedUnits.foreach { unit =>
      checkEvaluation(
        TimeTrunc(Literal(unit), Literal(midnightTime, TimeType())),
        midnightTime
      )
    }

    val maxTime = localTimeToNanos(LocalTime.of(23, 59, 59, 999999999))
    checkEvaluation(
      TimeTrunc(Literal("HOUR"), Literal(maxTime, TimeType())),
      localTime(23, 0, 0, 0)
    )
    checkEvaluation(
      TimeTrunc(Literal("MICROSECOND"), Literal(maxTime, TimeType())),
      localTimeToNanos(LocalTime.of(23, 59, 59, 999999000))
    )

    // Test precision loss.
    val timeWithMicroPrecision = localTime(15, 30, 45, 123456)
    val timeTruncMin = TimeTrunc(Literal("MINUTE"), Literal(timeWithMicroPrecision, TimeType(3)))
    assert(timeTruncMin.dataType == TimeType(3))
    checkEvaluation(timeTruncMin, localTime(15, 30, 0, 0))
    val timeTruncSec = TimeTrunc(Literal("SECOND"), Literal(timeWithMicroPrecision, TimeType(3)))
    assert(timeTruncSec.dataType == TimeType(3))
    checkEvaluation(timeTruncSec, localTime(15, 30, 45, 0))
  }

  test("Numeric to TIME conversions") {
    // time_from_seconds (supports Long and Decimal for fractional seconds)
    checkEvaluation(TimeFromSeconds(Literal(0L)), 0L)
    checkEvaluation(TimeFromSeconds(Literal(43200L)), 43200000000000L)
    checkEvaluation(TimeFromSeconds(Literal(52200L)), 52200000000000L)
    checkEvaluation(TimeFromSeconds(Literal(Decimal(52200.5))), 52200500000000L)
    checkEvaluation(TimeFromSeconds(Literal(Decimal(86399.999999))), 86399999999000L)

    // time_from_millis
    checkEvaluation(TimeFromMillis(Literal(0L)), 0L)
    checkEvaluation(TimeFromMillis(Literal(52200000L)), 52200000000000L)
    checkEvaluation(TimeFromMillis(Literal(52200500L)), 52200500000000L)
    checkEvaluation(TimeFromMillis(Literal(86399999L)), 86399999000000L)

    // time_from_micros
    checkEvaluation(TimeFromMicros(Literal(0L)), 0L)
    checkEvaluation(TimeFromMicros(Literal(52200000000L)), 52200000000000L)
    checkEvaluation(TimeFromMicros(Literal(52200500000L)), 52200500000000L)
    checkEvaluation(TimeFromMicros(Literal(86399999999L)), 86399999999000L)
  }

  test("Numeric to TIME conversions - range validation") {

    // time_from_seconds - out of range [0, 86400)
    checkExceptionInExpression[SparkDateTimeException](
      TimeFromSeconds(Literal(-1L)),
      "Invalid TIME value")
    checkExceptionInExpression[SparkDateTimeException](
      TimeFromSeconds(Literal(86400L)),
      "Invalid TIME value")
    checkExceptionInExpression[SparkDateTimeException](
      TimeFromSeconds(Literal(Decimal(-0.1))),
      "Invalid TIME value")
    checkExceptionInExpression[SparkDateTimeException](
      TimeFromSeconds(Literal(Decimal(86400.0))),
      "Invalid TIME value")

    // time_from_millis - out of range [0, 86400000)
    checkExceptionInExpression[SparkDateTimeException](
      TimeFromMillis(Literal(-1L)),
      "Invalid TIME value")
    checkExceptionInExpression[SparkDateTimeException](
      TimeFromMillis(Literal(86400000L)),
      "Invalid TIME value")

    // time_from_micros - out of range [0, 86400000000)
    checkExceptionInExpression[SparkDateTimeException](
      TimeFromMicros(Literal(-1L)),
      "Invalid TIME value")
    checkExceptionInExpression[SparkDateTimeException](
      TimeFromMicros(Literal(86400000000L)),
      "Invalid TIME value")

    // Test overflow in TIME conversion
    checkExceptionInExpression[SparkDateTimeException](
      TimeFromSeconds(Literal(Long.MaxValue)),
      "Overflow in TIME conversion")

    // Test NaN and Infinite for floating point
    checkExceptionInExpression[SparkDateTimeException](
      TimeFromSeconds(Literal(Float.NaN)),
      "Cannot convert NaN or Infinite value to TIME")
    checkExceptionInExpression[SparkDateTimeException](
      TimeFromSeconds(Literal(Double.PositiveInfinity)),
      "Cannot convert NaN or Infinite value to TIME")
  }

  test("Numeric to TIME conversions - NULL inputs") {
    checkEvaluation(TimeFromSeconds(Literal.create(null, LongType)), null)
    checkEvaluation(TimeFromSeconds(Literal.create(null, DecimalType(14, 6))), null)
    checkEvaluation(TimeFromMillis(Literal.create(null, LongType)), null)
    checkEvaluation(TimeFromMicros(Literal.create(null, LongType)), null)
  }

  test("TIME to numeric extractions") {
    val midnight = Literal.create(0L, TimeType())
    val afternoon = Literal.create(52200000000000L, TimeType())  // 14:30:00
    val fractional = Literal.create(52200500000000L, TimeType()) // 14:30:00.5
    val maxTime = Literal.create(86399999999000L, TimeType())    // 23:59:59.999999

    // time_to_seconds (returns DECIMAL to preserve fractional seconds)
    checkEvaluation(TimeToSeconds(midnight), Decimal(0))
    checkEvaluation(TimeToSeconds(afternoon), Decimal(52200))
    checkEvaluation(TimeToSeconds(fractional), Decimal(52200.5))
    checkEvaluation(TimeToSeconds(maxTime), Decimal(86399.999999))

    // time_to_millis (returns LONG)
    checkEvaluation(TimeToMillis(midnight), 0L)
    checkEvaluation(TimeToMillis(afternoon), 52200000L)
    checkEvaluation(TimeToMillis(fractional), 52200500L)
    checkEvaluation(TimeToMillis(maxTime), 86399999L)

    // time_to_micros (returns LONG)
    checkEvaluation(TimeToMicros(midnight), 0L)
    checkEvaluation(TimeToMicros(afternoon), 52200000000L)
    checkEvaluation(TimeToMicros(fractional), 52200500000L)
    checkEvaluation(TimeToMicros(maxTime), 86399999999L)
  }

  test("TIME to numeric extractions - NULL inputs") {
    val nullTime = Literal.create(null, TimeType())
    checkEvaluation(TimeToSeconds(nullTime), null)
    checkEvaluation(TimeToMillis(nullTime), null)
    checkEvaluation(TimeToMicros(nullTime), null)
  }

  test("Round-trip conversions preserve precision") {
    // Seconds (with fractional precision)
    val seconds = Decimal(52200.123456)
    checkEvaluation(TimeToSeconds(TimeFromSeconds(Literal(seconds))), seconds)

    // Millis
    val millis = 52200500L
    checkEvaluation(TimeToMillis(TimeFromMillis(Literal(millis))), millis)

    // Micros
    val micros = 52200500000L
    checkEvaluation(TimeToMicros(TimeFromMicros(Literal(micros))), micros)

    // Cross-precision: seconds -> TIME -> micros
    val secondsValue = Decimal(14.5)
    val timeVal = TimeFromSeconds(Literal(secondsValue))
    checkEvaluation(TimeToMicros(timeVal), 14500000L)
  }
}
