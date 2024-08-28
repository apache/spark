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

package org.apache.spark.sql.catalyst.util

import java.time.{Duration, Period}
import java.util.concurrent.TimeUnit

import org.apache.spark.{SparkFunSuite, SparkIllegalArgumentException}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeUtils.millisToMicros
import org.apache.spark.sql.catalyst.util.IntervalStringStyles.{ANSI_STYLE, HIVE_STYLE}
import org.apache.spark.sql.catalyst.util.IntervalUtils._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{YearMonthIntervalType => YM}
import org.apache.spark.sql.types.DayTimeIntervalType._
import org.apache.spark.sql.types.YearMonthIntervalType._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class IntervalUtilsSuite extends SparkFunSuite with SQLHelper {

  private def checkFromString(input: String, expected: CalendarInterval): Unit = {
    assert(stringToInterval(UTF8String.fromString(input)) === expected)
    assert(safeStringToInterval(UTF8String.fromString(input)) === expected)
  }

  private def checkFromInvalidStringNull(input: String): Unit = {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        stringToInterval(UTF8String.fromString(input))
      },
      condition = "INVALID_INTERVAL_FORMAT.INPUT_IS_NULL",
      parameters = Map(
        "input" -> Option(input).map(_.toString).getOrElse("null")))
    assert(safeStringToInterval(UTF8String.fromString(input)) === null)
  }

  private def checkFromInvalidStringEmpty(input: String): Unit = {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        stringToInterval(UTF8String.fromString(input))
      },
      condition = "INVALID_INTERVAL_FORMAT.INPUT_IS_EMPTY",
      parameters = Map(
        "input" -> Option(input).map(_.toString).getOrElse("null")))
    assert(safeStringToInterval(UTF8String.fromString(input)) === null)
  }

  private def checkFromInvalidStringInvalidPrefix(input: String, prefix: String): Unit = {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        stringToInterval(UTF8String.fromString(input))
      },
      condition = "INVALID_INTERVAL_FORMAT.INVALID_PREFIX",
      parameters = Map(
        "input" -> Option(input).map(_.toString).getOrElse("null"),
        "prefix" -> prefix))
    assert(safeStringToInterval(UTF8String.fromString(input)) === null)
  }

  private def checkFromInvalidStringUnrecognizedNumber(input: String, number: String): Unit = {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        stringToInterval(UTF8String.fromString(input))
      },
      condition = "INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER",
      parameters = Map(
        "input" -> Option(input).map(_.toString).getOrElse("null"),
        "number" -> number))
    assert(safeStringToInterval(UTF8String.fromString(input)) === null)
  }

  private def checkFromInvalidStringArithmeticException(input: String): Unit = {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        stringToInterval(UTF8String.fromString(input))
      },
      condition = "INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION",
      parameters = Map(
        "input" -> Option(input).map(_.toString).getOrElse("null")))
    assert(safeStringToInterval(UTF8String.fromString(input)) === null)
  }

  private def checkFromInvalidStringInvalidValue(input: String, value: String): Unit = {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        stringToInterval(UTF8String.fromString(input))
      },
      condition = "INVALID_INTERVAL_FORMAT.INVALID_VALUE",
      parameters = Map(
        "input" -> Option(input).map(_.toString).getOrElse("null"),
        "value" -> value))
    assert(safeStringToInterval(UTF8String.fromString(input)) === null)
  }

  private def checkFromInvalidStringInvalidPrecision(input: String, value: String): Unit = {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        stringToInterval(UTF8String.fromString(input))
      },
      condition = "INVALID_INTERVAL_FORMAT.INVALID_PRECISION",
      parameters = Map(
        "input" -> Option(input).map(_.toString).getOrElse("null"),
        "value" -> value))
    assert(safeStringToInterval(UTF8String.fromString(input)) === null)
  }

  private def checkFromInvalidStringInvalidFraction(input: String, unit: String): Unit = {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        stringToInterval(UTF8String.fromString(input))
      },
      condition = "INVALID_INTERVAL_FORMAT.INVALID_FRACTION",
      parameters = Map(
        "input" -> Option(input).map(_.toString).getOrElse("null"),
        "unit" -> unit))
    assert(safeStringToInterval(UTF8String.fromString(input)) === null)
  }

  private def checkFromInvalidStringInvalidUnit(input: String, unit: String): Unit = {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        stringToInterval(UTF8String.fromString(input))
      },
      condition = "INVALID_INTERVAL_FORMAT.INVALID_UNIT",
      parameters = Map(
        "input" -> Option(input).map(_.toString).getOrElse("null"),
        "unit" -> unit))
    assert(safeStringToInterval(UTF8String.fromString(input)) === null)
  }

  private def checkFromInvalidStringMissingNumber(input: String, word: String): Unit = {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        stringToInterval(UTF8String.fromString(input))
      },
      condition = "INVALID_INTERVAL_FORMAT.MISSING_NUMBER",
      parameters = Map(
        "input" -> Option(input).map(_.toString).getOrElse("null"),
        "word" -> word))
    assert(safeStringToInterval(UTF8String.fromString(input)) === null)
  }

  private def checkFromInvalidStringMissingUnit(input: String, word: String): Unit = {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        stringToInterval(UTF8String.fromString(input))
      },
      condition = "INVALID_INTERVAL_FORMAT.MISSING_UNIT",
      parameters = Map(
        "input" -> Option(input).map(_.toString).getOrElse("null"),
        "word" -> word))
    assert(safeStringToInterval(UTF8String.fromString(input)) === null)
  }

  private def checkFromInvalidStringUnknownError(input: String, word: String): Unit = {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        stringToInterval(UTF8String.fromString(input))
      },
      condition = "INVALID_INTERVAL_FORMAT.UNKNOWN_PARSING_ERROR",
      parameters = Map(
        "input" -> Option(input).map(_.toString).getOrElse("null"),
        "word" -> word))
    assert(safeStringToInterval(UTF8String.fromString(input)) === null)
  }

  private def failFuncWithInvalidInput(
      input: String, errorMsg: String, converter: String => CalendarInterval): Unit = {
    withClue("Expected to throw an exception for the invalid input") {
      val e = intercept[IllegalArgumentException](converter(input))
      assert(e.getMessage.contains(errorMsg))
    }
  }

  private def testSingleUnit(
    unit: String, number: Int, months: Int, days: Int, microseconds: Long): Unit = {
    for (prefix <- Seq("interval ", "")) {
      val input1 = prefix + number + " " + unit
      val input2 = prefix + number + " " + unit + "s"
      val result = new CalendarInterval(months, days, microseconds)
      checkFromString(input1, result)
      checkFromString(input2, result)
    }
  }

  test("string to interval: basic") {
    testSingleUnit("YEAR", 3, 36, 0, 0)
    testSingleUnit("Month", 3, 3, 0, 0)
    testSingleUnit("Week", 3, 0, 21, 0)
    testSingleUnit("DAY", 3, 0, 3, 0)
    testSingleUnit("HouR", 3, 0, 0, 3 * MICROS_PER_HOUR)
    testSingleUnit("MiNuTe", 3, 0, 0, 3 * MICROS_PER_MINUTE)
    testSingleUnit("Second", 3, 0, 0, 3 * MICROS_PER_SECOND)
    testSingleUnit("MilliSecond", 3, 0, 0, millisToMicros(3))
    testSingleUnit("MicroSecond", 3, 0, 0, 3)

    checkFromInvalidStringNull(null)
    checkFromInvalidStringEmpty("")
    checkFromInvalidStringEmpty("interval")
    checkFromInvalidStringUnrecognizedNumber("foo", "foo")
    checkFromInvalidStringUnrecognizedNumber("foo 1 day", "foo")
  }

  test("string to interval: interval with dangling parts should not results null") {
    checkFromInvalidStringMissingNumber("+", "+")
    checkFromInvalidStringMissingNumber("-", "-")
    checkFromInvalidStringMissingUnit("+ 2", "2")
    checkFromInvalidStringMissingUnit("- 1", "1")
    checkFromInvalidStringMissingUnit("1", "1")
    checkFromInvalidStringMissingUnit("1.2", "1.2")
    checkFromInvalidStringMissingUnit("1 day 2", "2")
    checkFromInvalidStringMissingUnit("1 day 2.2", "2.2")
    checkFromInvalidStringMissingNumber("1 day -", "-")
    checkFromInvalidStringMissingUnit("-.", "-.")
  }

  test("string to interval: multiple units") {
    Seq(
      "-1 MONTH 1 day -1 microseconds" -> new CalendarInterval(-1, 1, -1),
      " 123 MONTHS        123 DAYS  123 Microsecond    " -> new CalendarInterval(123, 123, 123),
      "interval -1 day +3 Microseconds" -> new CalendarInterval(0, -1, 3),
      "interval -   1 day +     3 Microseconds" -> new CalendarInterval(0, -1, 3),
      "  interval  8  years -11 months 123  weeks   -1 day " +
        "23 hours -22 minutes 1 second  -123  millisecond    567 microseconds " ->
        new CalendarInterval(85, 860, 81480877567L)).foreach { case (input, expected) =>
      checkFromString(input, expected)
    }
  }

  test("string to interval: special cases") {
    // Support any order of interval units
    checkFromString("1 day 1 year", new CalendarInterval(12, 1, 0))
    // Allow duplicated units and summarize their values
    checkFromString("1 day 10 day", new CalendarInterval(0, 11, 0))
    // Only the seconds units can have the fractional part
    checkFromInvalidStringInvalidFraction("1.5 days", "days")
    checkFromInvalidStringInvalidFraction("1. hour", "hour")
    checkFromInvalidStringInvalidUnit("1 hourX", "hourx")
    checkFromInvalidStringUnrecognizedNumber("~1 hour", "~1")
    checkFromInvalidStringInvalidUnit("1 Mour", "mour")
    checkFromInvalidStringInvalidUnit("1 aour", "aour")
    checkFromInvalidStringInvalidValue("1a1 hour", "1a1")
    checkFromInvalidStringInvalidValue("1.1a1 seconds", "1.1a1")
    checkFromInvalidStringArithmeticException("2234567890 days")
    checkFromInvalidStringInvalidValue(". seconds", ".")
  }

  test("string to interval: whitespaces") {
    checkFromInvalidStringEmpty(" ")
    checkFromInvalidStringEmpty("\n")
    checkFromInvalidStringEmpty("\t")
    checkFromString("1 \t day \n 2 \r hour", new CalendarInterval(0, 1, 2 * MICROS_PER_HOUR))
    checkFromInvalidStringInvalidPrefix("interval1 \t day \n 2 \r hour", "interval1")
    checkFromString("interval\r1\tday", new CalendarInterval(0, 1, 0))
    // scalastyle:off nonascii
    checkFromInvalidStringUnrecognizedNumber("中国 interval 1 day", "中国")
    checkFromInvalidStringInvalidPrefix("interval浙江 1 day", "interval浙江")
    checkFromInvalidStringInvalidValue("interval 1杭州 day", "1杭州")
    checkFromInvalidStringInvalidUnit("interval 1 滨江day", "滨江day")
    checkFromInvalidStringInvalidUnit("interval 1 day长河", "day长河")
    checkFromInvalidStringUnrecognizedNumber("interval 1 day 网商路", "网商路")
    // scalastyle:on nonascii
  }

  test("string to interval: seconds with fractional part") {
    checkFromString("0.1 seconds", new CalendarInterval(0, 0, 100000))
    checkFromString("1. seconds", new CalendarInterval(0, 0, 1000000))
    checkFromString("123.001 seconds", new CalendarInterval(0, 0, 123001000))
    checkFromString("1.001001 seconds", new CalendarInterval(0, 0, 1001001))
    checkFromString("1 minute 1.001001 seconds", new CalendarInterval(0, 0, 61001001))
    checkFromString("-1.5 seconds", new CalendarInterval(0, 0, -1500000))
    // truncate nanoseconds to microseconds
    checkFromString("0.999999999 seconds", new CalendarInterval(0, 0, 999999))
    checkFromString(".999999999 seconds", new CalendarInterval(0, 0, 999999))
    checkFromInvalidStringInvalidPrecision("0.123456789123 seconds", "0.123456789123")
  }

  test("from year-month string") {
    assert(fromYearMonthString("99-10") === new CalendarInterval(99 * 12 + 10, 0, 0L))
    assert(fromYearMonthString("+99-10") === new CalendarInterval(99 * 12 + 10, 0, 0L))
    assert(fromYearMonthString("-8-10") === new CalendarInterval(-8 * 12 - 10, 0, 0L))
    failFuncWithInvalidInput("99-15", "month 15 outside range", fromYearMonthString)
    failFuncWithInvalidInput("9a9-15", "Interval string does not match year-month format",
      fromYearMonthString)

    // whitespaces
    assert(fromYearMonthString("99-10 ") === new CalendarInterval(99 * 12 + 10, 0, 0L))
    assert(fromYearMonthString("+99-10\t") === new CalendarInterval(99 * 12 + 10, 0, 0L))
    assert(fromYearMonthString("\t\t-8-10\t") === new CalendarInterval(-8 * 12 - 10, 0, 0L))
    failFuncWithInvalidInput("99\t-15", "Interval string does not match year-month format",
      fromYearMonthString)
    failFuncWithInvalidInput("-\t99-15", "Interval string does not match year-month format",
      fromYearMonthString)

    assert(fromYearMonthString("178956970-6") == new CalendarInterval(Int.MaxValue - 1, 0, 0))
    assert(fromYearMonthString("178956970-7") == new CalendarInterval(Int.MaxValue, 0, 0))

    val e1 = intercept[IllegalArgumentException]{
      assert(fromYearMonthString("178956970-8") == new CalendarInterval(Int.MinValue, 0, 0))
    }.getMessage
    assert(e1.contains("integer overflow"))
    assert(fromYearMonthString("-178956970-8") == new CalendarInterval(Int.MinValue, 0, 0))
    val e2 = intercept[IllegalArgumentException]{
      assert(fromYearMonthString("-178956970-9") == new CalendarInterval(Int.MinValue, 0, 0))
    }.getMessage
    assert(e2.contains("integer overflow"))
  }

  test("from day-time string - legacy") {
    withSQLConf(SQLConf.LEGACY_FROM_DAYTIME_STRING.key -> "true") {
      assert(fromDayTimeString("5 12:40:30.999999999") ===
        new CalendarInterval(
          0,
          5,
          12 * MICROS_PER_HOUR +
            40 * MICROS_PER_MINUTE +
            30 * MICROS_PER_SECOND + 999999L))
      assert(fromDayTimeString("10 0:12:0.888") ===
        new CalendarInterval(
          0,
          10,
          12 * MICROS_PER_MINUTE + millisToMicros(888)))
      assert(fromDayTimeString("-3 0:0:0") === new CalendarInterval(0, -3, 0L))

      checkError(
        exception = intercept[SparkIllegalArgumentException] {
          fromDayTimeString("5 30:12:20")
        },
        parameters = Map(
          "msg" -> "requirement failed: hour 30 outside range [0, 23]",
          "input" -> "5 30:12:20"),
        errorClass = "INVALID_INTERVAL_FORMAT.DAY_TIME_PARSING",
        sqlState = Some("22006")
      )

      failFuncWithInvalidInput("5 30:12:20", "hour 30 outside range", fromDayTimeString)
      failFuncWithInvalidInput("5 30-12", "must match day-time format", fromDayTimeString)
    }
  }

  test("interval duration") {
    def duration(s: String, unit: TimeUnit, daysPerMonth: Int): Long = {
      IntervalUtils.getDuration(stringToInterval(UTF8String.fromString(s)), unit, daysPerMonth)
    }

    assert(duration("0 seconds", TimeUnit.MILLISECONDS, 31) === 0)
    assert(duration("1 month", TimeUnit.DAYS, 31) === 31)
    assert(duration("1 microsecond", TimeUnit.MICROSECONDS, 30) === 1)
    assert(duration("1 month -30 days", TimeUnit.DAYS, 31) === 1)

    val e = intercept[ArithmeticException] {
      duration(s"${Integer.MAX_VALUE} month", TimeUnit.SECONDS, 31)
    }
    assert(e.getMessage.contains("overflow"))
  }

  test("negative interval") {
    def isNegative(s: String, daysPerMonth: Int): Boolean = {
      IntervalUtils.isNegative(stringToInterval(UTF8String.fromString(s)), daysPerMonth)
    }

    assert(isNegative("-1 months", 28))
    assert(isNegative("-1 microsecond", 30))
    assert(isNegative("-1 month 30 days", 31))
    assert(isNegative("2 months -61 days", 30))
    assert(isNegative("-1 year -2 seconds", 30))
    assert(!isNegative("0 months", 28))
    assert(!isNegative("1 year -360 days", 31))
    assert(!isNegative("-1 year 380 days", 31))
  }

  test("negate") {
    assert(negateExact(new CalendarInterval(1, 2, 3)) === new CalendarInterval(-1, -2, -3))
    assert(negate(new CalendarInterval(1, 2, 3)) === new CalendarInterval(-1, -2, -3))
  }

  test("parsing second_nano string") {
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        toDTInterval("12", "33.33.33", 1)
      },
      errorClass = "INTERVAL_ERROR.SECOND_NANO_FORMAT",
      sqlState = Some("22009")
    )
  }

  test("subtract one interval by another") {
    val input1 = new CalendarInterval(3, 1, 1 * MICROS_PER_HOUR)
    val input2 = new CalendarInterval(2, 4, 100 * MICROS_PER_HOUR)
    val input3 = new CalendarInterval(-10, -30, -81 * MICROS_PER_HOUR)
    val input4 = new CalendarInterval(75, 150, 200 * MICROS_PER_HOUR)
    Seq[(CalendarInterval, CalendarInterval) => CalendarInterval](subtractExact, subtract)
      .foreach { func =>
        assert(new CalendarInterval(1, -3, -99 * MICROS_PER_HOUR) === func(input1, input2))
        assert(new CalendarInterval(-85, -180, -281 * MICROS_PER_HOUR) === func(input3, input4))
      }
  }

  test("add two intervals") {
    val input1 = new CalendarInterval(3, 1, 1 * MICROS_PER_HOUR)
    val input2 = new CalendarInterval(2, 4, 100 * MICROS_PER_HOUR)
    val input3 = new CalendarInterval(-10, -30, -81 * MICROS_PER_HOUR)
    val input4 = new CalendarInterval(75, 150, 200 * MICROS_PER_HOUR)
    Seq[(CalendarInterval, CalendarInterval) => CalendarInterval](addExact, add).foreach { func =>
      assert(new CalendarInterval(5, 5, 101 * MICROS_PER_HOUR) === func(input1, input2))
      assert(new CalendarInterval(65, 120, 119 * MICROS_PER_HOUR) === func(input3, input4))
    }
  }

  test("multiply by num") {
    Seq[(CalendarInterval, Double) => CalendarInterval](multiply, multiplyExact).foreach { func =>
      var interval = new CalendarInterval(0, 0, 0)
      assert(interval === func(interval, 0))
      interval = new CalendarInterval(123, 456, 789)
      assert(new CalendarInterval(123 * 42, 456 * 42, 789 * 42) === func(interval, 42))
      interval = new CalendarInterval(-123, -456, -789)
      assert(new CalendarInterval(-123 * 42, -456 * 42, -789 * 42) === func(interval, 42))
      interval = new CalendarInterval(1, 5, 0)
      assert(new CalendarInterval(1, 7, 12 * MICROS_PER_HOUR) === func(interval, 1.5))
      interval = new CalendarInterval(2, 2, 2 * MICROS_PER_HOUR)
      assert(new CalendarInterval(2, 2, 12 * MICROS_PER_HOUR) === func(interval, 1.2))
    }

    val interval = new CalendarInterval(2, 0, 0)
    assert(multiply(interval, Integer.MAX_VALUE) === new CalendarInterval(Int.MaxValue, 0, 0))

    val e = intercept[ArithmeticException](multiplyExact(interval, Integer.MAX_VALUE))
    assert(e.getMessage.contains("overflow"))
  }

  test("divide by num") {
    Seq[(CalendarInterval, Double) => CalendarInterval](divide, divideExact).foreach { func =>
      var interval = new CalendarInterval(0, 0, 0)
      assert(interval === func(interval, 10))
      interval = new CalendarInterval(1, 3, 30 * MICROS_PER_SECOND)
      assert(new CalendarInterval(0, 1, 12 * MICROS_PER_HOUR + 15 * MICROS_PER_SECOND) ===
        func(interval, 2))
      assert(new CalendarInterval(2, 6, MICROS_PER_MINUTE) === func(interval, 0.5))
      interval = new CalendarInterval(-1, 0, -30 * MICROS_PER_SECOND)
      assert(new CalendarInterval(0, 0, -15 * MICROS_PER_SECOND) === func(interval, 2))
      assert(new CalendarInterval(-2, 0, -MICROS_PER_MINUTE) === func(interval, 0.5))
    }

    var interval = new CalendarInterval(Int.MaxValue, Int.MaxValue, 0)
    assert(divide(interval, 0.9) === new CalendarInterval(Int.MaxValue, Int.MaxValue,
      ((Int.MaxValue / 9.0) * MICROS_PER_DAY).round))
    val e1 = intercept[ArithmeticException](divideExact(interval, 0.9))
    assert(e1.getMessage.contains("integer overflow"))

    interval = new CalendarInterval(123, 456, 789)
    assert(divide(interval, 0) === null)
    val e2 = intercept[ArithmeticException](divideExact(interval, 0))
    assert(e2.getMessage.contains("Division by zero"))
  }

  test("from day-time string") {
    def check(input: String, from: Byte, to: Byte, expected: String): Unit = {
      withClue(s"from = $from, to = $to") {
        val expectedUtf8 = UTF8String.fromString(expected)
        assert(fromDayTimeString(input, from, to) === safeStringToInterval(expectedUtf8))
      }
    }
    def checkFail(input: String, from: Byte, to: Byte, errMsg: String): Unit = {
      failFuncWithInvalidInput(input, errMsg, s => fromDayTimeString(s, from, to))
    }

    check("12:40", HOUR, MINUTE, "12 hours 40 minutes")
    check("+12:40", HOUR, MINUTE, "12 hours 40 minutes")
    check("-12:40", HOUR, MINUTE, "-12 hours -40 minutes")
    checkFail("5 12:40", HOUR, MINUTE, "Interval string does not match day-time format")

    check("12:40:30.999999999", HOUR, SECOND, "12 hours 40 minutes 30.999999 seconds")
    check("+12:40:30.123456789", HOUR, SECOND, "12 hours 40 minutes 30.123456 seconds")
    check("-12:40:30.123456789", HOUR, SECOND, "-12 hours -40 minutes -30.123456 seconds")
    checkFail("5 12:40:30", HOUR, SECOND, "Interval string does not match day-time format")
    checkFail("12:40:30.0123456789", HOUR, SECOND,
      "Interval string does not match day-time format")

    check("40:30.123456789", MINUTE, SECOND, "40 minutes 30.123456 seconds")
    check("+40:30.123456789", MINUTE, SECOND, "40 minutes 30.123456 seconds")
    check("-40:30.123456789", MINUTE, SECOND, "-40 minutes -30.123456 seconds")
    checkFail("12:40:30", MINUTE, SECOND, "Interval string does not match day-time format")

    check("5 12", DAY, HOUR, "5 days 12 hours")
    check("+5 12", DAY, HOUR, "5 days 12 hours")
    check("-5 12", DAY, HOUR, "-5 days -12 hours")
    checkFail("5 12:30", DAY, HOUR, "Interval string does not match day-time format")

    check("5 12:40", DAY, MINUTE, "5 days 12 hours 40 minutes")
    check("+5 12:40", DAY, MINUTE, "5 days 12 hours 40 minutes")
    check("-5 12:40", DAY, MINUTE, "-5 days -12 hours -40 minutes")
    checkFail("5 12", DAY, MINUTE, "Interval string does not match day-time format")

    check("5 12:40:30.123", DAY, SECOND, "5 days 12 hours 40 minutes 30.123 seconds")
    check("+5 12:40:30.123456", DAY, SECOND, "5 days 12 hours 40 minutes 30.123456 seconds")
    check("-5 12:40:30.123456789", DAY, SECOND, "-5 days -12 hours -40 minutes -30.123456 seconds")
    checkFail("5 12", DAY, SECOND, "Interval string does not match day-time format")

    checkFail("5 30:12:20", DAY, SECOND, "hour 30 outside range")
    checkFail("5 30-12", DAY, SECOND, "Interval string does not match day-time format")

    // whitespaces
    check("\t +5 12:40\t ", DAY, MINUTE, "5 days 12 hours 40 minutes")
    checkFail("+5\t 12:40", DAY, MINUTE, "Interval string does not match day-time format")

  }

  test("interval overflow check") {
    val maxMonth = new CalendarInterval(Int.MaxValue, 0, 0)
    val minMonth = new CalendarInterval(Int.MinValue, 0, 0)
    val oneMonth = new CalendarInterval(1, 0, 0)
    val maxDay = new CalendarInterval(0, Int.MaxValue, 0)
    val minDay = new CalendarInterval(0, Int.MinValue, 0)
    val oneDay = new CalendarInterval(0, 1, 0)
    val maxMicros = new CalendarInterval(0, 0, Long.MaxValue)
    val minMicros = new CalendarInterval(0, 0, Long.MinValue)
    val oneMicros = new CalendarInterval(0, 0, 1)
    intercept[ArithmeticException](negateExact(minMonth))
    assert(negate(minMonth) === minMonth)

    intercept[ArithmeticException](addExact(maxMonth, oneMonth))
    intercept[ArithmeticException](addExact(maxDay, oneDay))
    intercept[ArithmeticException](addExact(maxMicros, oneMicros))
    assert(add(maxMonth, oneMonth) === minMonth)
    assert(add(maxDay, oneDay) === minDay)
    assert(add(maxMicros, oneMicros) === minMicros)

    intercept[ArithmeticException](subtractExact(minDay, oneDay))
    intercept[ArithmeticException](subtractExact(minMonth, oneMonth))
    intercept[ArithmeticException](subtractExact(minMicros, oneMicros))
    assert(subtract(minMonth, oneMonth) === maxMonth)
    assert(subtract(minDay, oneDay) === maxDay)
    assert(subtract(minMicros, oneMicros) === maxMicros)

    intercept[ArithmeticException](multiplyExact(maxMonth, 2))
    intercept[ArithmeticException](divideExact(maxDay, 0.5))
  }

  test("SPARK-34605: microseconds to duration") {
    assert(microsToDuration(0).isZero)
    assert(microsToDuration(-1).toNanos === -1000)
    assert(microsToDuration(1).toNanos === 1000)
    assert(microsToDuration(Long.MaxValue).toDays === 106751991)
    assert(microsToDuration(Long.MinValue).toDays === -106751991)
  }

  test("SPARK-34605: duration to microseconds") {
    assert(durationToMicros(Duration.ZERO) === 0)
    assert(durationToMicros(Duration.ofSeconds(-1)) === -1000000)
    assert(durationToMicros(Duration.ofNanos(123456)) === 123)
    assert(durationToMicros(Duration.ofDays(106751991)) ===
      (Long.MaxValue / MICROS_PER_DAY) * MICROS_PER_DAY)

    val errMsg = intercept[ArithmeticException] {
      durationToMicros(Duration.ofDays(106751991 + 1))
    }.getMessage
    assert(errMsg.contains("long overflow"))
  }

  test("SPARK-34615: period to months") {
    assert(periodToMonths(Period.ZERO) === 0)
    assert(periodToMonths(Period.of(0, -1, 0)) === -1)
    assert(periodToMonths(Period.of(0, -11, 0)) === -11)
    assert(periodToMonths(Period.of(0, -12, 0)) === -12)
    assert(periodToMonths(Period.of(0, -13, 0)) === -13)
    assert(periodToMonths(Period.of(0, 11, 0), YM.YEAR) === 0)
    assert(periodToMonths(Period.of(0, -11, 0), YM.YEAR) === 0)
    assert(periodToMonths(Period.of(0, 12, 0), YM.YEAR) === 12)
    assert(periodToMonths(Period.of(0, -12, 0), YM.YEAR) === -12)
    assert(periodToMonths(Period.of(0, 13, 0), YM.YEAR) === 12)
    assert(periodToMonths(Period.of(0, -13, 0), YM.YEAR) === -12)
    assert(periodToMonths(Period.of(-1, 0, 10)) === -12) // ignore days
    assert(periodToMonths(Period.of(178956970, 7, 0)) === Int.MaxValue)
    assert(periodToMonths(Period.of(-178956970, -8, 123)) === Int.MinValue)
    assert(periodToMonths(Period.of(0, Int.MaxValue, Int.MaxValue)) === Int.MaxValue)

    val errMsg = intercept[ArithmeticException] {
      periodToMonths(Period.of(Int.MaxValue, 0, 0))
    }.getMessage
    assert(errMsg.contains("integer overflow"))
  }

  test("SPARK-34615: months to period") {
    assert(monthsToPeriod(0) === Period.ZERO)
    assert(monthsToPeriod(-11) === Period.of(0, -11, 0))
    assert(monthsToPeriod(11) === Period.of(0, 11, 0))
    assert(monthsToPeriod(27) === Period.of(2, 3, 0))
    assert(monthsToPeriod(-13) === Period.of(-1, -1, 0))
    assert(monthsToPeriod(Int.MaxValue) === Period.ofYears(178956970).withMonths(7))
    assert(monthsToPeriod(Int.MinValue) === Period.ofYears(-178956970).withMonths(-8))
  }

  test("SPARK-34695: round trip conversion of micros -> duration -> micros") {
    Seq(
      0,
      MICROS_PER_SECOND - 1,
      -MICROS_PER_SECOND + 1,
      MICROS_PER_SECOND,
      -MICROS_PER_SECOND,
      Long.MaxValue - MICROS_PER_SECOND,
      Long.MinValue + MICROS_PER_SECOND,
      Long.MaxValue,
      Long.MinValue).foreach { micros =>
      val duration = microsToDuration(micros)
      assert(durationToMicros(duration) === micros)
    }
  }

  test("SPARK-34715: Add round trip tests for period <-> month and duration <-> micros") {
    // Months -> Period -> Months
    Seq(
      0,
      MONTHS_PER_YEAR - 1,
      MONTHS_PER_YEAR + 1,
      MONTHS_PER_YEAR,
      -MONTHS_PER_YEAR,
      Int.MaxValue - MONTHS_PER_YEAR,
      Int.MinValue + MONTHS_PER_YEAR,
      Int.MaxValue,
      Int.MinValue).foreach { months =>
      val period = monthsToPeriod(months)
      assert(periodToMonths(period) === months)
    }
    // Period -> Months -> Period
    Seq(
      monthsToPeriod(0),
      monthsToPeriod(MONTHS_PER_YEAR - 1),
      monthsToPeriod(MONTHS_PER_YEAR + 1),
      monthsToPeriod(MONTHS_PER_YEAR),
      monthsToPeriod(-MONTHS_PER_YEAR),
      monthsToPeriod(Int.MaxValue - MONTHS_PER_YEAR),
      monthsToPeriod(Int.MinValue + MONTHS_PER_YEAR),
      monthsToPeriod(Int.MaxValue),
      monthsToPeriod(Int.MinValue)).foreach { period =>
      val months = periodToMonths(period)
      assert(monthsToPeriod(months) === period)
    }
    // Duration -> micros -> Duration
    Seq(
      microsToDuration(0),
      microsToDuration(MICROS_PER_SECOND - 1),
      microsToDuration(-MICROS_PER_SECOND + 1),
      microsToDuration(MICROS_PER_SECOND),
      microsToDuration(-MICROS_PER_SECOND),
      microsToDuration(Long.MaxValue - MICROS_PER_SECOND),
      microsToDuration(Long.MinValue + MICROS_PER_SECOND),
      microsToDuration(Long.MaxValue),
      microsToDuration(Long.MinValue)).foreach { duration =>
      val micros = durationToMicros(duration)
      assert(microsToDuration(micros) === duration)
    }
  }

  test("SPARK-35016: format year-month intervals") {
    Seq(
      0 -> ("0-0", "INTERVAL '0-0' YEAR TO MONTH"),
      -11 -> ("-0-11", "INTERVAL '-0-11' YEAR TO MONTH"),
      11 -> ("0-11", "INTERVAL '0-11' YEAR TO MONTH"),
      -12 -> ("-1-0", "INTERVAL '-1-0' YEAR TO MONTH"),
      12 -> ("1-0", "INTERVAL '1-0' YEAR TO MONTH"),
      -13 -> ("-1-1", "INTERVAL '-1-1' YEAR TO MONTH"),
      13 -> ("1-1", "INTERVAL '1-1' YEAR TO MONTH"),
      -24 -> ("-2-0", "INTERVAL '-2-0' YEAR TO MONTH"),
      24 -> ("2-0", "INTERVAL '2-0' YEAR TO MONTH"),
      Int.MinValue -> ("-178956970-8", "INTERVAL '-178956970-8' YEAR TO MONTH"),
      Int.MaxValue -> ("178956970-7", "INTERVAL '178956970-7' YEAR TO MONTH")
    ).foreach { case (months, (hiveIntervalStr, ansiIntervalStr)) =>
      assert(toYearMonthIntervalString(months, ANSI_STYLE, YEAR, MONTH) === ansiIntervalStr)
      assert(toYearMonthIntervalString(months, HIVE_STYLE, YEAR, MONTH) === hiveIntervalStr)
    }
  }

  test("SPARK-49208 format year-month intervals") {
    Seq(
      0 -> ("0-0", "INTERVAL '0' MONTH"),
      -11 -> ("-0-11", "INTERVAL '-11' MONTH"),
      11 -> ("0-11", "INTERVAL '11' MONTH"),
      -12 -> ("-1-0", "INTERVAL '-12' MONTH"),
      12 -> ("1-0", "INTERVAL '12' MONTH"),
      -13 -> ("-1-1", "INTERVAL '-13' MONTH"),
      13 -> ("1-1", "INTERVAL '13' MONTH")
    ).foreach { case (months, (hiveIntervalStr, ansiIntervalStr)) =>
      assert(toYearMonthIntervalString(months, ANSI_STYLE, MONTH, MONTH) === ansiIntervalStr)
      assert(toYearMonthIntervalString(months, HIVE_STYLE, MONTH, MONTH) === hiveIntervalStr)
    }
  }

  test("SPARK-35016: format day-time intervals") {
    Seq(
      0L -> ("0 00:00:00.000000000", "INTERVAL '0 00:00:00' DAY TO SECOND"),
      -1L -> ("-0 00:00:00.000001000", "INTERVAL '-0 00:00:00.000001' DAY TO SECOND"),
      10 * MICROS_PER_MILLIS -> ("0 00:00:00.010000000", "INTERVAL '0 00:00:00.01' DAY TO SECOND"),
      (-123 * MICROS_PER_DAY - 3 * MICROS_PER_SECOND) ->
        ("-123 00:00:03.000000000", "INTERVAL '-123 00:00:03' DAY TO SECOND"),
      Long.MinValue -> ("-106751991 04:00:54.775808000",
        "INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND")
    ).foreach { case (micros, (hiveIntervalStr, ansiIntervalStr)) =>
      assert(toDayTimeIntervalString(micros, ANSI_STYLE, DAY, SECOND) === ansiIntervalStr)
      assert(toDayTimeIntervalString(micros, HIVE_STYLE, DAY, SECOND) === hiveIntervalStr)
    }
  }

  test("SPARK-49208: format negative month intervals") {
    Seq(
      0 -> ("0-0", "INTERVAL '0' MONTH"),
      -11 -> ("-0-11", "INTERVAL '-11' MONTH"),
      11 -> ("0-11", "INTERVAL '11' MONTH"),
      -12 -> ("-1-0", "INTERVAL '-12' MONTH"),
      12 -> ("1-0", "INTERVAL '12' MONTH"),
      -13 -> ("-1-1", "INTERVAL '-13' MONTH"),
      13 -> ("1-1", "INTERVAL '13' MONTH")
    ).foreach { case (months, (hiveIntervalStr, ansiIntervalStr)) =>
      assert(toYearMonthIntervalString(months, ANSI_STYLE, MONTH, MONTH) === ansiIntervalStr)
      assert(toYearMonthIntervalString(months, HIVE_STYLE, MONTH, MONTH) === hiveIntervalStr)
    }
  }

  test("SPARK-35734: Format day-time intervals using type fields") {
    Seq(
      0L ->
        ("INTERVAL '0 00:00:00' DAY TO SECOND",
          "INTERVAL '0 00:00' DAY TO MINUTE",
          "INTERVAL '0 00' DAY TO HOUR",
          "INTERVAL '00:00:00' HOUR TO SECOND",
          "INTERVAL '00:00' HOUR TO MINUTE",
          "INTERVAL '00:00' MINUTE TO SECOND",
          "INTERVAL '0' DAY",
          "INTERVAL '00' HOUR",
          "INTERVAL '00' MINUTE",
          "INTERVAL '00' SECOND"),
      -1L ->
        ("INTERVAL '-0 00:00:00.000001' DAY TO SECOND",
          "INTERVAL '-0 00:00' DAY TO MINUTE",
          "INTERVAL '-0 00' DAY TO HOUR",
          "INTERVAL '-00:00:00.000001' HOUR TO SECOND",
          "INTERVAL '-00:00' HOUR TO MINUTE",
          "INTERVAL '-00:00.000001' MINUTE TO SECOND",
          "INTERVAL '-0' DAY",
          "INTERVAL '-00' HOUR",
          "INTERVAL '-00' MINUTE",
          "INTERVAL '-00.000001' SECOND"),
      10 * MICROS_PER_MILLIS ->
        ("INTERVAL '0 00:00:00.01' DAY TO SECOND",
          "INTERVAL '0 00:00' DAY TO MINUTE",
          "INTERVAL '0 00' DAY TO HOUR",
          "INTERVAL '00:00:00.01' HOUR TO SECOND",
          "INTERVAL '00:00' HOUR TO MINUTE",
          "INTERVAL '00:00.01' MINUTE TO SECOND",
          "INTERVAL '0' DAY",
          "INTERVAL '00' HOUR",
          "INTERVAL '00' MINUTE",
          "INTERVAL '00.01' SECOND"),
      (-123 * MICROS_PER_DAY - 3 * MICROS_PER_SECOND) ->
        ("INTERVAL '-123 00:00:03' DAY TO SECOND",
          "INTERVAL '-123 00:00' DAY TO MINUTE",
          "INTERVAL '-123 00' DAY TO HOUR",
          "INTERVAL '-2952:00:03' HOUR TO SECOND",
          "INTERVAL '-2952:00' HOUR TO MINUTE",
          "INTERVAL '-177120:03' MINUTE TO SECOND",
          "INTERVAL '-123' DAY",
          "INTERVAL '-2952' HOUR",
          "INTERVAL '-177120' MINUTE",
          "INTERVAL '-10627203' SECOND"),
      Long.MinValue ->
        ("INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND",
          "INTERVAL '-106751991 04:00' DAY TO MINUTE",
          "INTERVAL '-106751991 04' DAY TO HOUR",
          "INTERVAL '-2562047788:00:54.775808' HOUR TO SECOND",
          "INTERVAL '-2562047788:00' HOUR TO MINUTE",
          "INTERVAL '-153722867280:54.775808' MINUTE TO SECOND",
          "INTERVAL '-106751991' DAY",
          "INTERVAL '-2562047788' HOUR",
          "INTERVAL '-153722867280' MINUTE",
          "INTERVAL '-9223372036854.775808' SECOND"),
      69159782123456L ->
        ("INTERVAL '800 11:03:02.123456' DAY TO SECOND",
          "INTERVAL '800 11:03' DAY TO MINUTE",
          "INTERVAL '800 11' DAY TO HOUR",
          "INTERVAL '19211:03:02.123456' HOUR TO SECOND",
          "INTERVAL '19211:03' HOUR TO MINUTE",
          "INTERVAL '1152663:02.123456' MINUTE TO SECOND",
          "INTERVAL '800' DAY",
          "INTERVAL '19211' HOUR",
          "INTERVAL '1152663' MINUTE",
          "INTERVAL '69159782.123456' SECOND"),
      -69159782123456L ->
        ("INTERVAL '-800 11:03:02.123456' DAY TO SECOND",
          "INTERVAL '-800 11:03' DAY TO MINUTE",
          "INTERVAL '-800 11' DAY TO HOUR",
          "INTERVAL '-19211:03:02.123456' HOUR TO SECOND",
          "INTERVAL '-19211:03' HOUR TO MINUTE",
          "INTERVAL '-1152663:02.123456' MINUTE TO SECOND",
          "INTERVAL '-800' DAY",
          "INTERVAL '-19211' HOUR",
          "INTERVAL '-1152663' MINUTE",
          "INTERVAL '-69159782.123456' SECOND")
    ).foreach {
      case (
        micros, (
          dayToSec,
          dayToMinute,
          dayToHour,
          hourToSec,
          hourToMinute,
          minuteToSec,
          day,
          hour,
          minute,
          sec)) =>
        assert(toDayTimeIntervalString(micros, ANSI_STYLE, DAY, SECOND) === dayToSec)
        assert(toDayTimeIntervalString(micros, ANSI_STYLE, DAY, MINUTE) === dayToMinute)
        assert(toDayTimeIntervalString(micros, ANSI_STYLE, DAY, HOUR) === dayToHour)
        assert(toDayTimeIntervalString(micros, ANSI_STYLE, HOUR, SECOND) === hourToSec)
        assert(toDayTimeIntervalString(micros, ANSI_STYLE, HOUR, MINUTE) === hourToMinute)
        assert(toDayTimeIntervalString(micros, ANSI_STYLE, MINUTE, SECOND) === minuteToSec)
        assert(toDayTimeIntervalString(micros, ANSI_STYLE, DAY, DAY) === day)
        assert(toDayTimeIntervalString(micros, ANSI_STYLE, HOUR, HOUR) === hour)
        assert(toDayTimeIntervalString(micros, ANSI_STYLE, MINUTE, MINUTE) === minute)
        assert(toDayTimeIntervalString(micros, ANSI_STYLE, SECOND, SECOND) === sec)
    }
  }

  test("SPARK-35771: Format year-month intervals using type fields") {
    Seq(
      0 ->
        ("INTERVAL '0-0' YEAR TO MONTH", "INTERVAL '0' YEAR", "INTERVAL '0' MONTH"),
      -11 -> ("INTERVAL '-0-11' YEAR TO MONTH", "INTERVAL '-0' YEAR", "INTERVAL '-11' MONTH"),
      11 -> ("INTERVAL '0-11' YEAR TO MONTH", "INTERVAL '0' YEAR", "INTERVAL '11' MONTH"),
      -12 -> ("INTERVAL '-1-0' YEAR TO MONTH", "INTERVAL '-1' YEAR", "INTERVAL '-12' MONTH"),
      12 -> ("INTERVAL '1-0' YEAR TO MONTH", "INTERVAL '1' YEAR", "INTERVAL '12' MONTH"),
      -13 -> ("INTERVAL '-1-1' YEAR TO MONTH", "INTERVAL '-1' YEAR", "INTERVAL '-13' MONTH"),
      13 -> ("INTERVAL '1-1' YEAR TO MONTH", "INTERVAL '1' YEAR", "INTERVAL '13' MONTH"),
      -24 -> ("INTERVAL '-2-0' YEAR TO MONTH", "INTERVAL '-2' YEAR", "INTERVAL '-24' MONTH"),
      24 -> ("INTERVAL '2-0' YEAR TO MONTH", "INTERVAL '2' YEAR", "INTERVAL '24' MONTH"),
      Int.MinValue ->
        ("INTERVAL '-178956970-8' YEAR TO MONTH",
          "INTERVAL '-178956970' YEAR",
          "INTERVAL '-2147483648' MONTH"),
      Int.MaxValue ->
        ("INTERVAL '178956970-7' YEAR TO MONTH",
          "INTERVAL '178956970' YEAR",
          "INTERVAL '2147483647' MONTH")
    ).foreach { case (months, (yearToMonth, year, month)) =>
      assert(toYearMonthIntervalString(months, ANSI_STYLE, YEAR, MONTH) === yearToMonth)
      assert(toYearMonthIntervalString(months, ANSI_STYLE, YEAR, YEAR) === year)
      assert(toYearMonthIntervalString(months, ANSI_STYLE, MONTH, MONTH) === month)
    }
  }

  test("SPARK-38324: The second range is not [0, 59] in the day time ANSI interval") {
    Seq(
      ("10 12:40:60", 60, DAY, SECOND),
      ("10 12:40:60.999999999", 60, DAY, SECOND),
      ("10 12:40:99.999999999", 99, DAY, SECOND),
      ("12:40:60", 60, HOUR, SECOND),
      ("12:40:60.999999999", 60, HOUR, SECOND),
      ("12:40:99.999999999", 99, HOUR, SECOND),
      ("40:60", 60, MINUTE, SECOND),
      ("40:60.999999999", 60, MINUTE, SECOND),
      ("40:99.999999999", 99, MINUTE, SECOND)
    ).foreach { case(input, second, from, to) =>
      failFuncWithInvalidInput(
        input, s"second $second outside range [0, 59]", s => fromDayTimeString(s, from, to))
    }
  }
}
