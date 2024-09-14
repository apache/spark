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

import java.time.{Duration, Period}
import java.time.temporal.ChronoUnit

import scala.language.implicitConversions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.catalyst.util.{DateTimeTestUtils, IntervalUtils}
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.IntervalUtils.{safeStringToInterval, stringToInterval}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataTypeTestUtils, DayTimeIntervalType, Decimal, DecimalType, YearMonthIntervalType}
import org.apache.spark.sql.types.DataTypeTestUtils.{dayTimeIntervalTypes, numericTypes, yearMonthIntervalTypes}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class IntervalExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {
  implicit def stringToUTF8Str(str: String): UTF8String = UTF8String.fromString(str)

  implicit def interval(s: String): Literal = {
    Literal(stringToInterval( "interval " + s))
  }

  test("years") {
    checkEvaluation(ExtractIntervalYears("0 years"), 0)
    checkEvaluation(ExtractIntervalYears("9999 years"), 9999)
    checkEvaluation(ExtractIntervalYears("1000 years"), 1000)
    checkEvaluation(ExtractIntervalYears("-2000 years"), -2000)
    // Microseconds part must not be taken into account
    checkEvaluation(ExtractIntervalYears("9 years 400 days"), 9)
    // Year must be taken from years and months
    checkEvaluation(ExtractIntervalYears("9 years 12 months"), 10)
    checkEvaluation(ExtractIntervalYears("10 years -1 months"), 9)
  }

  test("months") {
    checkEvaluation(ExtractIntervalMonths("0 year"), 0.toByte)
    for (m <- -24 to 24) {
      checkEvaluation(ExtractIntervalMonths(s"$m months"), (m % 12).toByte)
    }
    checkEvaluation(ExtractIntervalMonths("1 year 10 months"), 10.toByte)
    checkEvaluation(ExtractIntervalMonths("-2 year -10 months"), -10.toByte)
    checkEvaluation(ExtractIntervalMonths("9999 years"), 0.toByte)
  }

  private val largeInterval: String = "9999 years 11 months " +
    "31 days 11 hours 59 minutes 59 seconds 999 milliseconds 999 microseconds"

  test("days") {
    checkEvaluation(ExtractIntervalDays("0 days"), 0)
    checkEvaluation(ExtractIntervalDays("1 days 100 seconds"), 1)
    checkEvaluation(ExtractIntervalDays("-1 days -100 seconds"), -1)
    checkEvaluation(ExtractIntervalDays("-365 days"), -365)
    checkEvaluation(ExtractIntervalDays("365 days"), 365)
    // Years and months must not be taken into account
    checkEvaluation(ExtractIntervalDays("100 year 10 months 5 days"), 5)
    checkEvaluation(ExtractIntervalDays(largeInterval), 31)
    checkEvaluation(ExtractIntervalDays("25 hours"), 1)
  }

  test("hours") {
    checkEvaluation(ExtractIntervalHours("0 hours"), 0.toByte)
    checkEvaluation(ExtractIntervalHours("1 hour"), 1.toByte)
    checkEvaluation(ExtractIntervalHours("-1 hour"), -1.toByte)
    checkEvaluation(ExtractIntervalHours("23 hours"), 23.toByte)
    checkEvaluation(ExtractIntervalHours("-23 hours"), -23.toByte)
    // Years, months and days must not be taken into account
    checkEvaluation(ExtractIntervalHours("100 year 10 months 10 days 10 hours"), 10.toByte)
    // Minutes should be taken into account
    checkEvaluation(ExtractIntervalHours("10 hours 100 minutes"), 11.toByte)
    checkEvaluation(ExtractIntervalHours(largeInterval), 11.toByte)
    checkEvaluation(ExtractIntervalHours("25 hours"), 1.toByte)

  }

  test("minutes") {
    checkEvaluation(ExtractIntervalMinutes("0 minute"), 0.toByte)
    checkEvaluation(ExtractIntervalMinutes("1 minute"), 1.toByte)
    checkEvaluation(ExtractIntervalMinutes("-1 minute"), -1.toByte)
    checkEvaluation(ExtractIntervalMinutes("59 minute"), 59.toByte)
    checkEvaluation(ExtractIntervalMinutes("-59 minute"), -59.toByte)
    // Years and months must not be taken into account
    checkEvaluation(ExtractIntervalMinutes("100 year 10 months 10 minutes"), 10.toByte)
    checkEvaluation(ExtractIntervalMinutes(largeInterval), 59.toByte)
  }

  test("seconds") {
    checkEvaluation(ExtractIntervalSeconds("0 second"), Decimal(0, 8, 6))
    checkEvaluation(ExtractIntervalSeconds("1 second"), Decimal(1.0, 8, 6))
    checkEvaluation(ExtractIntervalSeconds("-1 second"), Decimal(-1.0, 8, 6))
    checkEvaluation(ExtractIntervalSeconds("1 minute 59 second"), Decimal(59.0, 8, 6))
    checkEvaluation(ExtractIntervalSeconds("-59 minutes -59 seconds"), Decimal(-59.0, 8, 6))
    // Years and months must not be taken into account
    checkEvaluation(ExtractIntervalSeconds("100 year 10 months 10 seconds"), Decimal(10.0, 8, 6))
    checkEvaluation(ExtractIntervalSeconds(largeInterval), Decimal(59.999999, 8, 6))
    checkEvaluation(
      ExtractIntervalSeconds("10 seconds 1 milliseconds 1 microseconds"),
      Decimal(10001001, 8, 6))
    checkEvaluation(ExtractIntervalSeconds("61 seconds 1 microseconds"), Decimal(1000001, 8, 6))
  }

  test("cast large seconds to decimal") {
    checkEvaluation(
      Cast(
        Cast(Literal(Decimal("9223372036854.775807")), DayTimeIntervalType(3, 3)),
        DecimalType(19, 6)
      ),
      Decimal("9223372036854.775807")
    )
  }

  test("multiply") {
    def check(
        interval: String,
        num: Double,
        expected: String,
        isAnsi: Option[Boolean] = None): Unit = {
      val expectedRes = safeStringToInterval(expected)
      val configs = if (isAnsi.isEmpty) Seq("true", "false") else isAnsi.map(_.toString).toSeq
      configs.foreach { v =>
        withSQLConf(SQLConf.ANSI_ENABLED.key -> v) {
          val expr = MultiplyInterval(Literal(stringToInterval(interval)), Literal(num))
          if (expectedRes == null) {
            checkExceptionInExpression[ArithmeticException](expr, expected)
          } else {
            checkEvaluation(expr, expectedRes)
          }
        }
      }
    }

    check("0 seconds", 10, "0 seconds")
    check("10 hours", 0, "0 hours")
    check("12 months 1 microseconds", 2, "2 years 2 microseconds")
    check("-5 year 3 seconds", 3, "-15 years 9 seconds")
    check("1 year 1 second", 0.5, "6 months 500 milliseconds")
    check("-100 years -1 millisecond", 0.5, "-50 years -500 microseconds")
    check("2 months 4 seconds", -0.5, "-1 months -2 seconds")
    check("1 month 2 microseconds", 1.5, "1 months 3 microseconds")
    check("2 months", Int.MaxValue, "integer overflow", Some(true))
    check("2 months", Int.MaxValue, s"${Int.MaxValue} months", Some(false))
  }

  test("divide") {
    def check(
        interval: String,
        num: Double,
        expected: String,
        isAnsi: Option[Boolean] = None): Unit = {
      val expectedRes = safeStringToInterval(expected)
      val configs = if (isAnsi.isEmpty) Seq("true", "false") else isAnsi.map(_.toString).toSeq
      configs.foreach { v =>
        withSQLConf(SQLConf.ANSI_ENABLED.key -> v) {
          val expr = DivideInterval(Literal(stringToInterval(interval)), Literal(num))
          if (expected != null && expectedRes == null) {
            checkExceptionInExpression[ArithmeticException](expr, expected)
          } else {
            checkEvaluation(expr, expectedRes)
          }
        }
      }
    }

    check("0 seconds", 10, "0 seconds")
    check("12 months 3 milliseconds", 2, "6 months 0.0015 seconds")
    check("-5 year 3 seconds", 3, "-1 years -8 months 1 seconds")
    check("6 years -7 seconds", 3, "2 years -2.333333 seconds")
    check("2 years -8 seconds", 0.5, "4 years -16 seconds")
    check("-1 month 2 microseconds", -0.25, "4 months -8 microseconds")
    check("1 month 3 microsecond", 1.5, "2 microseconds")
    check("1 second", 0, "Division by zero", Some(true))
    check("1 second", 0, null, Some(false))
    check(s"${Int.MaxValue} months", 0.9, "integer overflow", Some(true))
    check(s"${Int.MaxValue} months", 0.9, s"${Int.MaxValue} months", Some(false))
  }

  test("make interval") {
    def check(
        years: Int = 0,
        months: Int = 0,
        weeks: Int = 0,
        days: Int = 0,
        hours: Int = 0,
        minutes: Int = 0,
        seconds: Int = 0,
        millis: Int = 0,
        micros: Int = 0): Unit = {
      val secFrac = DateTimeTestUtils.secFrac(seconds, millis, micros)
      val intervalExpr = MakeInterval(Literal(years), Literal(months), Literal(weeks),
        Literal(days), Literal(hours), Literal(minutes),
        Literal(Decimal(secFrac, Decimal.MAX_LONG_DIGITS, 6)))
      val totalMonths = years * MONTHS_PER_YEAR + months
      val totalDays = weeks * DAYS_PER_WEEK + days
      val totalMicros = secFrac + minutes * MICROS_PER_MINUTE + hours * MICROS_PER_HOUR
      val expected = new CalendarInterval(totalMonths, totalDays, totalMicros)
      checkEvaluation(intervalExpr, expected)
    }

    check(months = 0, days = 0, micros = 0)
    check(years = -123)
    check(weeks = 123)
    check(millis = -123)
    check(9999, 11, 0, 31, 23, 59, 59, 999, 999)
    check(years = 10000, micros = -1)
    check(-9999, -11, 0, -31, -23, -59, -59, -999, -999)
    check(years = -10000, micros = 1)
    check(
      hours = Int.MaxValue,
      minutes = Int.MaxValue,
      seconds = Int.MaxValue,
      millis = Int.MaxValue,
      micros = Int.MaxValue)
  }

  test("ANSI mode: make interval") {
    def check(
        years: Int = 0,
        months: Int = 0,
        weeks: Int = 0,
        days: Int = 0,
        hours: Int = 0,
        minutes: Int = 0,
        seconds: Int = 0,
        millis: Int = 0,
        micros: Int = 0): Unit = {
      val secFrac = DateTimeTestUtils.secFrac(seconds, millis, micros)
      val intervalExpr = MakeInterval(Literal(years), Literal(months), Literal(weeks),
        Literal(days), Literal(hours), Literal(minutes),
        Literal(Decimal(secFrac, Decimal.MAX_LONG_DIGITS, 6)))
      val totalMonths = years * MONTHS_PER_YEAR + months
      val totalDays = weeks * DAYS_PER_WEEK + days
      val totalMicros = secFrac + minutes * MICROS_PER_MINUTE + hours * MICROS_PER_HOUR
      val expected = new CalendarInterval(totalMonths, totalDays, totalMicros)
      checkEvaluation(intervalExpr, expected)
    }

    def checkException(
        years: Int = 0,
        months: Int = 0,
        weeks: Int = 0,
        days: Int = 0,
        hours: Int = 0,
        minutes: Int = 0,
        seconds: Int = 0,
        millis: Int = 0,
        micros: Int = 0): Unit = {
      val secFrac = DateTimeTestUtils.secFrac(seconds, millis, micros)
      val intervalExpr = MakeInterval(Literal(years), Literal(months), Literal(weeks),
        Literal(days), Literal(hours), Literal(minutes),
        Literal(Decimal(secFrac, Decimal.MAX_LONG_DIGITS, 6)))
      checkExceptionInExpression[ArithmeticException](intervalExpr, EmptyRow, "")
    }

    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      check(months = 0, days = 0, micros = 0)
      check(years = -123)
      check(weeks = 123)
      check(millis = -123)
      check(9999, 11, 0, 31, 23, 59, 59, 999, 999)
      check(years = 10000, micros = -1)
      check(-9999, -11, 0, -31, -23, -59, -59, -999, -999)
      check(years = -10000, micros = 1)
      check(
        hours = Int.MaxValue,
        minutes = Int.MaxValue,
        seconds = Int.MaxValue,
        millis = Int.MaxValue,
        micros = Int.MaxValue)

      checkException(years = Int.MaxValue)
      checkException(weeks = Int.MaxValue)
    }
  }

  test("SPARK-35130: make day time interval") {
    def check(
        days: Int = 0,
        hours: Int = 0,
        minutes: Int = 0,
        seconds: Int = 0,
        millis: Int = 0,
        micros: Int = 0): Unit = {
      val secFrac = DateTimeTestUtils.secFrac(seconds, millis, micros)
      val durationExpr = MakeDTInterval(Literal(days), Literal(hours), Literal(minutes),
        Literal(Decimal(secFrac, Decimal.MAX_LONG_DIGITS, 6)))
      val expected = secFrac + minutes * MICROS_PER_MINUTE + hours * MICROS_PER_HOUR +
          days * MICROS_PER_DAY
      checkEvaluation(durationExpr, expected)
    }

    def checkException(
        days: Int = 0,
        hours: Int = 0,
        minutes: Int = 0,
        seconds: Int = 0,
        millis: Int = 0,
        micros: Int = 0): Unit = {
      val secFrac = DateTimeTestUtils.secFrac(seconds, millis, micros)
      val durationExpr = MakeDTInterval(Literal(days), Literal(hours), Literal(minutes),
        Literal(Decimal(secFrac, Decimal.MAX_LONG_DIGITS, 6)))
      checkExceptionInExpression[ArithmeticException](durationExpr, EmptyRow, "")
    }

    check(millis = -123)
    check(31, 23, 59, 59, 999, 999)
    check(31, 123, 159, 159, 1999, 1999)
    check(days = 10000, micros = -1)
    check(-31, -23, -59, -59, -999, -999)
    check(days = -10000, micros = 1)
    check(
      hours = Int.MaxValue,
      minutes = Int.MaxValue,
      seconds = Int.MaxValue,
      millis = Int.MaxValue,
      micros = Int.MaxValue)

    checkException(days = Int.MaxValue)
  }

  test("SPARK-34824: multiply year-month interval by numeric") {
    Seq(
      (Period.ofYears(-123), Literal(null, DecimalType.USER_DEFAULT)) -> null,
      (Period.ofMonths(0), 10) -> Period.ofMonths(0),
      (Period.ofMonths(10), 0L) -> Period.ofMonths(0),
      (Period.ofYears(100), -1.toByte) -> Period.ofYears(-100),
      (Period.ofMonths(12), 0.3f) -> Period.ofMonths(4),
      (Period.ofYears(-1000), 0.3d) -> Period.ofYears(-300),
      (Period.ofYears(9999), 0.0001d) -> Period.ofYears(1),
      (Period.ofYears(9999), BigDecimal(0.0001)) -> Period.ofYears(1)
    ).foreach { case ((period, num), expected) =>
      checkEvaluation(MultiplyYMInterval(Literal(period), Literal(num)), expected)
    }

    Seq(
      (Period.ofMonths(2), Int.MaxValue) -> "overflow",
      (Period.ofMonths(Int.MinValue), 10d) -> "out of range",
      (Period.ofMonths(-100), Float.NaN) -> "input is infinite or NaN",
      (Period.ofMonths(200), Double.PositiveInfinity) -> "input is infinite or NaN",
      (Period.ofMonths(-200), Float.NegativeInfinity) -> "input is infinite or NaN"
    ).foreach { case ((period, num), expectedErrMsg) =>
      checkExceptionInExpression[ArithmeticException](
        MultiplyYMInterval(Literal(period), Literal(num)),
        expectedErrMsg)
    }

    numericTypes.foreach { numType =>
      yearMonthIntervalTypes.foreach { it =>
        checkConsistencyBetweenInterpretedAndCodegenAllowingException(
          (interval: Expression, num: Expression) => MultiplyYMInterval(interval, num),
          it, numType)
      }
    }
  }

  test("SPARK-34850: multiply day-time interval by numeric") {
    Seq(
      (Duration.ofHours(-123), Literal(null, DecimalType.USER_DEFAULT)) -> null,
      (Duration.ofMinutes(0), 10) -> Duration.ofMinutes(0),
      (Duration.ofSeconds(10), 0L) -> Duration.ofSeconds(0),
      (Duration.ofMillis(100), -1.toByte) -> Duration.ofMillis(-100),
      (Duration.ofDays(12), 0.3d) -> Duration.ofDays(12).multipliedBy(3).dividedBy(10),
      (Duration.of(-1000, ChronoUnit.MICROS), 0.3f) -> Duration.of(-300, ChronoUnit.MICROS),
      (Duration.ofDays(9999), 0.0001d) -> Duration.ofDays(9999).dividedBy(10000),
      (Duration.ofDays(9999), BigDecimal(0.0001)) -> Duration.ofDays(9999).dividedBy(10000)
    ).foreach { case ((duration, num), expected) =>
      checkEvaluation(MultiplyDTInterval(Literal(duration), Literal(num)), expected)
    }

    Seq(
      (Duration.ofDays(-100), Float.NaN) -> "input is infinite or NaN",
      (Duration.ofDays(2), Int.MaxValue) -> "overflow",
      (Duration.ofHours(Int.MinValue), Short.MinValue) -> "overflow",
      (Duration.ofDays(10), BigDecimal(Long.MinValue)) -> "Overflow",
      (Duration.ofDays(200), Double.PositiveInfinity) -> "input is infinite or NaN",
      (Duration.ofDays(-200), Float.NegativeInfinity) -> "input is infinite or NaN"
    ).foreach { case ((duration, num), expectedErrMsg) =>
      checkExceptionInExpression[ArithmeticException](
        MultiplyDTInterval(Literal(duration), Literal(num)), expectedErrMsg)
    }

    numericTypes.foreach { numType =>
      dayTimeIntervalTypes.foreach { it =>
        checkConsistencyBetweenInterpretedAndCodegenAllowingException(
          (interval: Expression, num: Expression) => MultiplyDTInterval(interval, num),
          it, numType)
      }
    }
  }

  test("SPARK-34868: divide year-month interval by numeric") {
    Seq(
      (Period.ofYears(-123), Literal(null, DecimalType.USER_DEFAULT)) -> null,
      (Period.ofMonths(0), 10) -> Period.ofMonths(0),
      (Period.ofMonths(200), Double.PositiveInfinity) -> Period.ofMonths(0),
      (Period.ofMonths(-200), Float.NegativeInfinity) -> Period.ofMonths(0),
      (Period.ofYears(100), -1.toByte) -> Period.ofYears(-100),
      (Period.ofYears(1), 2.toShort) -> Period.ofMonths(6),
      (Period.ofYears(-1), -3) -> Period.ofMonths(4),
      (Period.ofMonths(-1000), 0.5f) -> Period.ofMonths(-2000),
      (Period.ofYears(1000), 100d) -> Period.ofYears(10),
      (Period.ofMonths(2), BigDecimal(0.1)) -> Period.ofMonths(20)
    ).foreach { case ((period, num), expected) =>
      checkEvaluation(DivideYMInterval(Literal(period), Literal(num)), expected)
    }

    Seq(
      (Period.ofMonths(1), 0) -> "Division by zero",
      (Period.ofMonths(Int.MinValue), 0d) -> "Division by zero",
      (Period.ofMonths(-100), Float.NaN) -> "input is infinite or NaN"
    ).foreach { case ((period, num), expectedErrMsg) =>
      checkExceptionInExpression[ArithmeticException](
        DivideYMInterval(Literal(period), Literal(num)),
        expectedErrMsg)
    }

    numericTypes.foreach { numType =>
      yearMonthIntervalTypes.foreach { it =>
        checkConsistencyBetweenInterpretedAndCodegenAllowingException(
          (interval: Expression, num: Expression) => DivideYMInterval(interval, num),
          it, numType)
      }
    }
  }

  test("SPARK-34875: divide day-time interval by numeric") {
    Seq(
      (Duration.ofDays(-123), Literal(null, DecimalType.USER_DEFAULT)) -> null,
      (Duration.ZERO, 10) -> Duration.ZERO,
      (Duration.ofMillis(200), Double.PositiveInfinity) -> Duration.ZERO,
      (Duration.ofSeconds(-200), Float.NegativeInfinity) -> Duration.ZERO,
      (Duration.ofMinutes(100), -1.toByte) -> Duration.ofMinutes(-100),
      (Duration.ofHours(1), 2.toShort) -> Duration.ofMinutes(30),
      (Duration.ofDays(-1), -3) -> Duration.ofHours(8),
      (Duration.of(-1000, ChronoUnit.MICROS), 0.5f) ->Duration.of(-2000, ChronoUnit.MICROS),
      (Duration.ofDays(10080), 100d) -> Duration.ofDays(10080).dividedBy(100),
      (Duration.ofMillis(2), BigDecimal(-0.1)) -> Duration.ofMillis(-20)
    ).foreach { case ((period, num), expected) =>
      checkEvaluation(DivideDTInterval(Literal(period), Literal(num)), expected)
    }

    Seq(
      (Duration.ofDays(1), 0) -> "Division by zero",
      (Duration.ofMillis(Int.MinValue), 0d) -> "Division by zero",
      (Duration.ofSeconds(-100), Float.NaN) -> "input is infinite or NaN"
    ).foreach { case ((period, num), expectedErrMsg) =>
      checkExceptionInExpression[ArithmeticException](
        DivideDTInterval(Literal(period), Literal(num)),
        expectedErrMsg)
    }

    numericTypes.foreach { numType =>
      dayTimeIntervalTypes.foreach { it =>
        checkConsistencyBetweenInterpretedAndCodegenAllowingException(
          (interval: Expression, num: Expression) => DivideDTInterval(interval, num),
          it, numType)
      }
    }
  }

  test("ANSI: extract years and months") {
    Seq(Period.ZERO,
      Period.ofMonths(100),
      Period.ofMonths(-100),
      Period.ofYears(100),
      Period.ofYears(-100)).foreach { p =>
      checkEvaluation(ExtractANSIIntervalYears(Literal(p)),
        IntervalUtils.getYears(p.toTotalMonths.toInt))
      checkEvaluation(ExtractANSIIntervalMonths(Literal(p)),
        IntervalUtils.getMonths(p.toTotalMonths.toInt))
    }
    checkEvaluation(ExtractANSIIntervalYears(Literal(null, YearMonthIntervalType())), null)
    checkEvaluation(ExtractANSIIntervalMonths(Literal(null, YearMonthIntervalType())), null)
  }

  test("ANSI: extract days, hours, minutes and seconds") {
    Seq(Duration.ZERO,
      Duration.ofMillis(1L * MILLIS_PER_DAY + 2 * MILLIS_PER_SECOND),
      Duration.ofMillis(-1L * MILLIS_PER_DAY + 2 * MILLIS_PER_SECOND),
      Duration.ofDays(100),
      Duration.ofDays(-100),
      Duration.ofHours(-100)).foreach { d =>

      checkEvaluation(ExtractANSIIntervalDays(Literal(d)), d.toDays.toInt)
      checkEvaluation(ExtractANSIIntervalHours(Literal(d)), (d.toHours % HOURS_PER_DAY).toByte)
      checkEvaluation(ExtractANSIIntervalMinutes(Literal(d)),
        (d.toMinutes % MINUTES_PER_HOUR).toByte)
      checkEvaluation(ExtractANSIIntervalSeconds(Literal(d)),
        IntervalUtils.getSeconds(IntervalUtils.durationToMicros(d)))
    }
    checkEvaluation(ExtractANSIIntervalDays(
      Literal(null, DayTimeIntervalType())), null)
    checkEvaluation(ExtractANSIIntervalHours(
      Literal(null, DayTimeIntervalType())), null)
    checkEvaluation(ExtractANSIIntervalMinutes(
      Literal(null, DayTimeIntervalType())), null)
    checkEvaluation(ExtractANSIIntervalSeconds(
      Literal(null, DayTimeIntervalType())), null)
  }

  test("SPARK-35129: make_ym_interval") {
    checkEvaluation(MakeYMInterval(Literal(0), Literal(10)), 10)
    checkEvaluation(MakeYMInterval(Literal(1), Literal(10)), 22)
    checkEvaluation(MakeYMInterval(Literal(1), Literal(0)), 12)
    checkEvaluation(MakeYMInterval(Literal(1), Literal(-1)), 11)
    checkEvaluation(MakeYMInterval(Literal(-2), Literal(-1)), -25)

    checkEvaluation(MakeYMInterval(Literal(178956970), Literal(7)), Int.MaxValue)
    checkEvaluation(MakeYMInterval(Literal(-178956970), Literal(-8)), Int.MinValue)

    Seq(MakeYMInterval(Literal(178956970), Literal(8)),
      MakeYMInterval(Literal(-178956970), Literal(-9)))
      .foreach { ym =>
        checkExceptionInExpression[ArithmeticException](ym, "integer overflow")
      }

    def checkImplicitEvaluation(expr: Expression, value: Any): Unit = {
      val resolvedExpr = TypeCoercion.ImplicitTypeCasts.transform(expr)
      checkEvaluation(resolvedExpr, value)
    }

    // Check implicit casts.
    checkImplicitEvaluation(MakeYMInterval(Literal(1L), Literal(-1L)), 11)
    checkImplicitEvaluation(MakeYMInterval(Literal(1d), Literal(-1L)), 11)
    checkImplicitEvaluation(MakeYMInterval(Literal(1.1), Literal(-1L)), 11)
  }

  test("SPARK-35728: Check multiply/divide of day-time intervals of any fields by numeric") {
    Seq(
      ((Duration.ofMinutes(0), 10),
        Array(Duration.ofDays(0), Duration.ofHours(0), Duration.ofMinutes(0),
          Duration.ofSeconds(0), Duration.ofHours(0), Duration.ofMinutes(0),
          Duration.ofSeconds(0), Duration.ofMinutes(0), Duration.ofSeconds(0),
          Duration.ofSeconds(0)),
        Array(Duration.ofDays(0), Duration.ofHours(0), Duration.ofMinutes(0),
          Duration.ofSeconds(0), Duration.ofHours(0), Duration.ofMinutes(0),
          Duration.ofSeconds(0), Duration.ofMinutes(0), Duration.ofSeconds(0),
          Duration.ofSeconds(0))),
      ((Duration.ofSeconds(86400 + 3600 + 60 + 1), 1L),
        Array(Duration.ofSeconds(86400), Duration.ofSeconds(90000),
          Duration.ofSeconds(90060), Duration.ofSeconds(90061),
          Duration.ofSeconds(90000), Duration.ofSeconds(90060),
          Duration.ofSeconds(90061), Duration.ofSeconds(90060),
          Duration.ofSeconds(90061), Duration.ofSeconds(90061)),
        Array(Duration.ofSeconds(86400), Duration.ofSeconds(90000),
          Duration.ofSeconds(90060), Duration.ofSeconds(90061),
          Duration.ofSeconds(90000), Duration.ofSeconds(90060),
          Duration.ofSeconds(90061), Duration.ofSeconds(90060),
          Duration.ofSeconds(90061), Duration.ofSeconds(90061))),
      ((Duration.ofSeconds(86400 + 3600 + 60 + 1), -1.toByte),
        Array(Duration.ofSeconds(-86400), Duration.ofSeconds(-90000),
          Duration.ofSeconds(-90060), Duration.ofSeconds(-90061),
          Duration.ofSeconds(-90000), Duration.ofSeconds(-90060),
          Duration.ofSeconds(-90061), Duration.ofSeconds(-90060),
          Duration.ofSeconds(-90061), Duration.ofSeconds(-90061)),
        Array(Duration.ofSeconds(-86400), Duration.ofSeconds(-90000),
          Duration.ofSeconds(-90060), Duration.ofSeconds(-90061),
          Duration.ofSeconds(-90000), Duration.ofSeconds(-90060),
          Duration.ofSeconds(-90061), Duration.ofSeconds(-90060),
          Duration.ofSeconds(-90061), Duration.ofSeconds(-90061))),
      ((Duration.ofSeconds(86400 + 3600 + 60 + 1), 0.3d),
        Array(Duration.ofSeconds(288000), Duration.ofSeconds(300000),
          Duration.ofSeconds(300200), Duration.ofNanos(300203333333000L),
          Duration.ofSeconds(300000), Duration.ofSeconds(300200),
          Duration.ofNanos(300203333333000L), Duration.ofSeconds(300200),
          Duration.ofNanos(300203333333000L), Duration.ofNanos(300203333333000L)),
        Array(Duration.ofSeconds(25920), Duration.ofSeconds(27000),
          Duration.ofSeconds(27018), Duration.ofMillis(27018300),
          Duration.ofSeconds(27000), Duration.ofSeconds(27018),
          Duration.ofMillis(27018300), Duration.ofSeconds(27018),
          Duration.ofMillis(27018300), Duration.ofMillis(27018300))),
      ((Duration.of(-1000, ChronoUnit.MICROS), 0.3f),
        Array(Duration.ofSeconds(0), Duration.ofSeconds(0),
          Duration.ofSeconds(0), Duration.ofNanos(-3333000L),
          Duration.ofSeconds(0), Duration.ofSeconds(0),
          Duration.ofNanos(-3333000L), Duration.ofSeconds(0),
          Duration.ofNanos(-3333000L), Duration.ofNanos(-3333000L)),
        Array(Duration.ofSeconds(0), Duration.ofSeconds(0),
          Duration.ofSeconds(0), Duration.ofNanos(-300000),
          Duration.ofSeconds(0), Duration.ofSeconds(0),
          Duration.ofNanos(-300000), Duration.ofSeconds(0),
          Duration.ofNanos(-300000), Duration.ofNanos(-300000))),
      ((Duration.ofDays(9999), 0.0001d),
        Array(Duration.ofSeconds(8639136000000L), Duration.ofSeconds(8639136000000L),
          Duration.ofSeconds(8639136000000L), Duration.ofSeconds(8639136000000L),
          Duration.ofSeconds(8639136000000L), Duration.ofSeconds(8639136000000L),
          Duration.ofSeconds(8639136000000L), Duration.ofSeconds(8639136000000L),
          Duration.ofSeconds(8639136000000L), Duration.ofSeconds(8639136000000L)),
        Array(Duration.ofMillis(86391360), Duration.ofMillis(86391360),
          Duration.ofMillis(86391360), Duration.ofMillis(86391360),
          Duration.ofMillis(86391360), Duration.ofMillis(86391360),
          Duration.ofMillis(86391360), Duration.ofMillis(86391360),
          Duration.ofMillis(86391360), Duration.ofMillis(86391360))),
      ((Duration.ofDays(9999), BigDecimal(0.0001)),
        Array(Duration.ofSeconds(8639136000000L), Duration.ofSeconds(8639136000000L),
          Duration.ofSeconds(8639136000000L), Duration.ofSeconds(8639136000000L),
          Duration.ofSeconds(8639136000000L), Duration.ofSeconds(8639136000000L),
          Duration.ofSeconds(8639136000000L), Duration.ofSeconds(8639136000000L),
          Duration.ofSeconds(8639136000000L), Duration.ofSeconds(8639136000000L)),
        Array(Duration.ofMillis(86391360), Duration.ofMillis(86391360),
          Duration.ofMillis(86391360), Duration.ofMillis(86391360),
          Duration.ofMillis(86391360), Duration.ofMillis(86391360),
          Duration.ofMillis(86391360), Duration.ofMillis(86391360),
          Duration.ofMillis(86391360), Duration.ofMillis(86391360)))
    ).foreach { case ((duration, num), divideExpected, multiplyExpected) =>
      DataTypeTestUtils.dayTimeIntervalTypes.zip(divideExpected)
        .foreach { case (dt, result) =>
          checkEvaluation(DivideDTInterval(Literal.create(duration, dt), Literal(num)), result)
        }
      DataTypeTestUtils.dayTimeIntervalTypes.zip(multiplyExpected)
        .foreach { case (dt, result) =>
          checkEvaluation(MultiplyDTInterval(Literal.create(duration, dt), Literal(num)), result)
        }
    }
  }

  test("SPARK-35778: Check multiply/divide of year-month intervals of any fields by numeric") {
    Seq(
      ((Period.ofMonths(0), 10),
        Array(Period.ofMonths(0), Period.ofMonths(0), Period.ofMonths(0)),
        Array(Period.ofMonths(0), Period.ofMonths(0), Period.ofMonths(0))),
      ((Period.ofMonths(13), 1),
        Array(Period.ofMonths(13), Period.ofMonths(12), Period.ofMonths(13)),
        Array(Period.ofMonths(13), Period.ofMonths(12), Period.ofMonths(13))),
      ((Period.ofMonths(-200), 1),
        Array(Period.ofMonths(-200), Period.ofMonths(-192), Period.ofMonths(-200)),
        Array(Period.ofMonths(-200), Period.ofMonths(-192), Period.ofMonths(-200))),
      ((Period.ofYears(100), -1.toByte),
        Array(Period.ofMonths(-1200), Period.ofMonths(-1200), Period.ofMonths(-1200)),
        Array(Period.ofMonths(-1200), Period.ofMonths(-1200), Period.ofMonths(-1200))),
      ((Period.ofYears(1), 2.toShort),
        Array(Period.ofMonths(6), Period.ofMonths(6), Period.ofMonths(6)),
        Array(Period.ofMonths(24), Period.ofMonths(24), Period.ofMonths(24))),
      ((Period.ofYears(-1), -3),
        Array(Period.ofMonths(4), Period.ofMonths(4), Period.ofMonths(4)),
        Array(Period.ofMonths(36), Period.ofMonths(36), Period.ofMonths(36))),
      ((Period.ofMonths(-1000), 0.5f),
        Array(Period.ofMonths(-2000), Period.ofMonths(-1992), Period.ofMonths(-2000)),
        Array(Period.ofMonths(-500), Period.ofMonths(-498), Period.ofMonths(-500))),
      ((Period.ofYears(1000), 100d),
        Array(Period.ofYears(10), Period.ofYears(10), Period.ofYears(10)),
        Array(Period.ofMonths(1200000), Period.ofMonths(1200000), Period.ofMonths(1200000))),
      ((Period.ofMonths(2), BigDecimal(0.1)),
        Array(Period.ofMonths(20), Period.ofMonths(0), Period.ofMonths(20)),
        Array(Period.ofMonths(0), Period.ofMonths(0), Period.ofMonths(0)))
    ).foreach { case ((period, num), divideExpected, multiplyExpected) =>
      DataTypeTestUtils.yearMonthIntervalTypes.zip(divideExpected)
        .foreach { case (dt, result) =>
          checkEvaluation(DivideYMInterval(Literal.create(period, dt), Literal(num)), result)
        }
      DataTypeTestUtils.yearMonthIntervalTypes.zip(multiplyExpected)
        .foreach { case (dt, result) =>
          checkEvaluation(MultiplyYMInterval(Literal.create(period, dt), Literal(num)), result)
        }
    }
  }
}
