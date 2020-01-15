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

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.IntervalUtils._
import org.apache.spark.sql.catalyst.util.IntervalUtils.IntervalUnit._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class IntervalUtilsSuite extends SparkFunSuite with SQLHelper {

  private def checkFromString(input: String, expected: CalendarInterval): Unit = {
    assert(stringToInterval(UTF8String.fromString(input)) === expected)
    assert(safeStringToInterval(UTF8String.fromString(input)) === expected)
  }

  private def checkFromStringWithFunc(
      input: String,
      months: Int,
      days: Int,
      us: Long,
      func: CalendarInterval => CalendarInterval): Unit = {
    val expected = new CalendarInterval(months, days, us)
    assert(func(stringToInterval(UTF8String.fromString(input))) === expected)
    assert(func(safeStringToInterval(UTF8String.fromString(input))) === expected)
  }

  private def checkFromInvalidString(input: String, errorMsg: String): Unit = {
    try {
      stringToInterval(UTF8String.fromString(input))
      fail("Expected to throw an exception for the invalid input")
    } catch {
      case e: IllegalArgumentException =>
        val msg = e.getMessage
        assert(msg.contains(errorMsg))
    }
    assert(safeStringToInterval(UTF8String.fromString(input)) === null)
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
    testSingleUnit("MilliSecond", 3, 0, 0, 3 * MICROS_PER_MILLIS)
    testSingleUnit("MicroSecond", 3, 0, 0, 3)

    checkFromInvalidString(null, "cannot be null")

    for (input <- Seq("", "interval", "foo", "foo 1 day")) {
      checkFromInvalidString(input, "Error parsing")
    }
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
    checkFromInvalidString("1.5 days", "'days' cannot have fractional part")
    checkFromInvalidString("1. hour", "'hour' cannot have fractional part")
    checkFromInvalidString("1 hourX", "invalid unit 'hourx'")
    checkFromInvalidString("~1 hour", "unrecognized number '~1'")
    checkFromInvalidString("1 Mour", "invalid unit 'mour'")
    checkFromInvalidString("1 aour", "invalid unit 'aour'")
    checkFromInvalidString("1a1 hour", "invalid value '1a1'")
    checkFromInvalidString("1.1a1 seconds", "invalid value '1.1a1'")
    checkFromInvalidString("2234567890 days", "integer overflow")
    checkFromInvalidString(". seconds", "invalid value '.'")
  }

  test("string to interval: whitespaces") {
    checkFromInvalidString(" ", "Error parsing ' ' to interval")
    checkFromInvalidString("\n", "Error parsing '\n' to interval")
    checkFromInvalidString("\t", "Error parsing '\t' to interval")
    checkFromString("1 \t day \n 2 \r hour", new CalendarInterval(0, 1, 2 * MICROS_PER_HOUR))
    checkFromInvalidString("interval1 \t day \n 2 \r hour", "invalid interval prefix interval1")
    checkFromString("interval\r1\tday", new CalendarInterval(0, 1, 0))
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
    checkFromInvalidString("0.123456789123 seconds", "'0.123456789123' is out of range")
  }

  test("from year-month string") {
    assert(fromYearMonthString("99-10") === new CalendarInterval(99 * 12 + 10, 0, 0L))
    assert(fromYearMonthString("+99-10") === new CalendarInterval(99 * 12 + 10, 0, 0L))
    assert(fromYearMonthString("-8-10") === new CalendarInterval(-8 * 12 - 10, 0, 0L))

    try {
      fromYearMonthString("99-15")
      fail("Expected to throw an exception for the invalid input")
    } catch {
      case e: IllegalArgumentException =>
        assert(e.getMessage.contains("month 15 outside range"))
    }

    try {
      fromYearMonthString("9a9-15")
      fail("Expected to throw an exception for the invalid input")
    } catch {
      case e: IllegalArgumentException =>
        assert(e.getMessage.contains("Interval string does not match year-month format"))
    }
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
          12 * MICROS_PER_MINUTE + 888 * MICROS_PER_MILLIS))
      assert(fromDayTimeString("-3 0:0:0") === new CalendarInterval(0, -3, 0L))

      try {
        fromDayTimeString("5 30:12:20")
        fail("Expected to throw an exception for the invalid input")
      } catch {
        case e: IllegalArgumentException =>
          assert(e.getMessage.contains("hour 30 outside range"))
      }

      try {
        fromDayTimeString("5 30-12")
        fail("Expected to throw an exception for the invalid input")
      } catch {
        case e: IllegalArgumentException =>
          assert(e.getMessage.contains("must match day-time format"))
      }

      try {
        fromDayTimeString("5 1:12:20", HOUR, MICROSECOND)
        fail("Expected to throw an exception for the invalid convention type")
      } catch {
        case e: IllegalArgumentException =>
          assert(e.getMessage.contains("Cannot support (interval"))
      }
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

    try {
      duration(Integer.MAX_VALUE + " month", TimeUnit.SECONDS, 31)
      fail("Expected to throw an exception for the invalid input")
    } catch {
      case e: ArithmeticException =>
        assert(e.getMessage.contains("overflow"))
    }
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
    var interval = new CalendarInterval(0, 0, 0)
    assert(interval === multiplyExact(interval, 0))
    interval = new CalendarInterval(123, 456, 789)
    assert(new CalendarInterval(123 * 42, 456 * 42, 789 * 42) === multiplyExact(interval, 42))
    interval = new CalendarInterval(-123, -456, -789)
    assert(new CalendarInterval(-123 * 42, -456 * 42, -789 * 42) === multiplyExact(interval, 42))
    assert(new CalendarInterval(1, 22, 12 * MICROS_PER_HOUR) ===
      multiplyExact(new CalendarInterval(1, 5, 0), 1.5))
    assert(new CalendarInterval(2, 14, 12 * MICROS_PER_HOUR) ===
      multiplyExact(new CalendarInterval(2, 2, 2 * MICROS_PER_HOUR), 1.2))

    try {
      multiplyExact(new CalendarInterval(2, 0, 0), Integer.MAX_VALUE)
      fail("Expected to throw an exception on months overflow")
    } catch {
      case e: ArithmeticException => assert(e.getMessage.contains("overflow"))
    }
  }

  test("divide by num") {
    var interval = new CalendarInterval(0, 0, 0)
    assert(interval === divideExact(interval, 10))
    interval = new CalendarInterval(1, 3, 30 * MICROS_PER_SECOND)
    assert(new CalendarInterval(0, 16, 12 * MICROS_PER_HOUR + 15 * MICROS_PER_SECOND) ===
      divideExact(interval, 2))
    assert(new CalendarInterval(2, 6, MICROS_PER_MINUTE) === divideExact(interval, 0.5))
    interval = new CalendarInterval(-1, 0, -30 * MICROS_PER_SECOND)
    assert(new CalendarInterval(0, -15, -15 * MICROS_PER_SECOND) === divideExact(interval, 2))
    assert(new CalendarInterval(-2, 0, -1 * MICROS_PER_MINUTE) === divideExact(interval, 0.5))
    try {
      divideExact(new CalendarInterval(123, 456, 789), 0)
      fail("Expected to throw an exception on divide by zero")
    } catch {
      case e: ArithmeticException => assert(e.getMessage.contains("divide by zero"))
    }
  }

  test("to ansi sql standard string") {
    val i1 = new CalendarInterval(0, 0, 0)
    assert(IntervalUtils.toSqlStandardString(i1) === "0")
    val i2 = new CalendarInterval(34, 0, 0)
    assert(IntervalUtils.toSqlStandardString(i2) === "+2-10")
    val i3 = new CalendarInterval(-34, 0, 0)
    assert(IntervalUtils.toSqlStandardString(i3) === "-2-10")
    val i4 = new CalendarInterval(0, 31, 0)
    assert(IntervalUtils.toSqlStandardString(i4) === "+31")
    val i5 = new CalendarInterval(0, -31, 0)
    assert(IntervalUtils.toSqlStandardString(i5) === "-31")
    val i6 = new CalendarInterval(0, 0, 3 * MICROS_PER_HOUR + 13 * MICROS_PER_MINUTE + 123)
    assert(IntervalUtils.toSqlStandardString(i6) === "+3:13:00.000123")
    val i7 = new CalendarInterval(0, 0, -3 * MICROS_PER_HOUR - 13 * MICROS_PER_MINUTE - 123)
    assert(IntervalUtils.toSqlStandardString(i7) === "-3:13:00.000123")
    val i8 = new CalendarInterval(-34, 31, 3 * MICROS_PER_HOUR + 13 * MICROS_PER_MINUTE + 123)
    assert(IntervalUtils.toSqlStandardString(i8) === "-2-10 +31 +3:13:00.000123")
    val i9 = new CalendarInterval(0, 0, -3000 * MICROS_PER_HOUR)
    assert(IntervalUtils.toSqlStandardString(i9) === "-3000:00:00")
  }

  test("to iso 8601 string") {
    val i1 = new CalendarInterval(0, 0, 0)
    assert(IntervalUtils.toIso8601String(i1) === "PT0S")
    val i2 = new CalendarInterval(34, 0, 0)
    assert(IntervalUtils.toIso8601String(i2) === "P2Y10M")
    val i3 = new CalendarInterval(-34, 0, 0)
    assert(IntervalUtils.toIso8601String(i3) === "P-2Y-10M")
    val i4 = new CalendarInterval(0, 31, 0)
    assert(IntervalUtils.toIso8601String(i4) === "P31D")
    val i5 = new CalendarInterval(0, -31, 0)
    assert(IntervalUtils.toIso8601String(i5) === "P-31D")
    val i6 = new CalendarInterval(0, 0, 3 * MICROS_PER_HOUR + 13 * MICROS_PER_MINUTE + 123)
    assert(IntervalUtils.toIso8601String(i6) === "PT3H13M0.000123S")
    val i7 = new CalendarInterval(0, 0, -3 * MICROS_PER_HOUR - 13 * MICROS_PER_MINUTE - 123)
    assert(IntervalUtils.toIso8601String(i7) === "PT-3H-13M-0.000123S")
    val i8 = new CalendarInterval(-34, 31, 3 * MICROS_PER_HOUR + 13 * MICROS_PER_MINUTE + 123)
    assert(IntervalUtils.toIso8601String(i8) === "P-2Y-10M31DT3H13M0.000123S")
    val i9 = new CalendarInterval(0, 0, -3000 * MICROS_PER_HOUR)
    assert(IntervalUtils.toIso8601String(i9) === "PT-3000H")
  }

  test("to multi units string") {
    val i1 = new CalendarInterval(0, 0, 0)
    assert(IntervalUtils.toMultiUnitsString(i1) === "0 seconds")
    val i2 = new CalendarInterval(34, 0, 0)
    assert(IntervalUtils.toMultiUnitsString(i2) === "2 years 10 months")
    val i3 = new CalendarInterval(-34, 0, 0)
    assert(IntervalUtils.toMultiUnitsString(i3) === "-2 years -10 months")
    val i4 = new CalendarInterval(0, 31, 0)
    assert(IntervalUtils.toMultiUnitsString(i4) === "31 days")
    val i5 = new CalendarInterval(0, -31, 0)
    assert(IntervalUtils.toMultiUnitsString(i5) === "-31 days")
    val i6 = new CalendarInterval(0, 0, 3 * MICROS_PER_HOUR + 13 * MICROS_PER_MINUTE + 123)
    assert(IntervalUtils.toMultiUnitsString(i6) === "3 hours 13 minutes 0.000123 seconds")
    val i7 = new CalendarInterval(0, 0, -3 * MICROS_PER_HOUR - 13 * MICROS_PER_MINUTE - 123)
    assert(IntervalUtils.toMultiUnitsString(i7) === "-3 hours -13 minutes -0.000123 seconds")
    val i8 = new CalendarInterval(-34, 31, 3 * MICROS_PER_HOUR + 13 * MICROS_PER_MINUTE + 123)
    assert(IntervalUtils.toMultiUnitsString(i8) ===
      "-2 years -10 months 31 days 3 hours 13 minutes 0.000123 seconds")
    val i9 = new CalendarInterval(0, 0, -3000 * MICROS_PER_HOUR)
    assert(IntervalUtils.toMultiUnitsString(i9) === "-3000 hours")
  }

  test("from day-time string") {
    def check(input: String, from: IntervalUnit, to: IntervalUnit, expected: String): Unit = {
      withClue(s"from = $from, to = $to") {
        val expectedUtf8 = UTF8String.fromString(expected)
        assert(fromDayTimeString(input, from, to) === safeStringToInterval(expectedUtf8))
      }
    }
    def checkFail(
        input: String,
        from: IntervalUnit,
        to: IntervalUnit,
        errMsg: String): Unit = {
      try {
        fromDayTimeString(input, from, to)
        fail("Expected to throw an exception for the invalid input")
      } catch {
        case e: IllegalArgumentException =>
          assert(e.getMessage.contains(errMsg))
      }
    }

    check("12:40", HOUR, MINUTE, "12 hours 40 minutes")
    check("+12:40", HOUR, MINUTE, "12 hours 40 minutes")
    check("-12:40", HOUR, MINUTE, "-12 hours -40 minutes")
    checkFail("5 12:40", HOUR, MINUTE, "must match day-time format")

    check("12:40:30.999999999", HOUR, SECOND, "12 hours 40 minutes 30.999999 seconds")
    check("+12:40:30.123456789", HOUR, SECOND, "12 hours 40 minutes 30.123456 seconds")
    check("-12:40:30.123456789", HOUR, SECOND, "-12 hours -40 minutes -30.123456 seconds")
    checkFail("5 12:40:30", HOUR, SECOND, "must match day-time format")
    checkFail("12:40:30.0123456789", HOUR, SECOND, "must match day-time format")

    check("40:30.123456789", MINUTE, SECOND, "40 minutes 30.123456 seconds")
    check("+40:30.123456789", MINUTE, SECOND, "40 minutes 30.123456 seconds")
    check("-40:30.123456789", MINUTE, SECOND, "-40 minutes -30.123456 seconds")
    checkFail("12:40:30", MINUTE, SECOND, "must match day-time format")

    check("5 12", DAY, HOUR, "5 days 12 hours")
    check("+5 12", DAY, HOUR, "5 days 12 hours")
    check("-5 12", DAY, HOUR, "-5 days -12 hours")
    checkFail("5 12:30", DAY, HOUR, "must match day-time format")

    check("5 12:40", DAY, MINUTE, "5 days 12 hours 40 minutes")
    check("+5 12:40", DAY, MINUTE, "5 days 12 hours 40 minutes")
    check("-5 12:40", DAY, MINUTE, "-5 days -12 hours -40 minutes")
    checkFail("5 12", DAY, MINUTE, "must match day-time format")

    check("5 12:40:30.123", DAY, SECOND, "5 days 12 hours 40 minutes 30.123 seconds")
    check("+5 12:40:30.123456", DAY, SECOND, "5 days 12 hours 40 minutes 30.123456 seconds")
    check("-5 12:40:30.123456789", DAY, SECOND, "-5 days -12 hours -40 minutes -30.123456 seconds")
    checkFail("5 12", DAY, SECOND, "must match day-time format")

    checkFail("5 30:12:20", DAY, SECOND, "hour 30 outside range")
    checkFail("5 30-12", DAY, SECOND, "must match day-time format")
    checkFail("5 1:12:20", HOUR, MICROSECOND, "Cannot support (interval")
  }

  test("interval overflow check") {
    intercept[ArithmeticException](negateExact(new CalendarInterval(Int.MinValue, 0, 0)))
    assert(negate(new CalendarInterval(Int.MinValue, 0, 0)) ===
      new CalendarInterval(Int.MinValue, 0, 0))
    intercept[ArithmeticException](negateExact(CalendarInterval.MIN_VALUE))
    assert(negate(CalendarInterval.MIN_VALUE) === CalendarInterval.MIN_VALUE)
    intercept[ArithmeticException](addExact(CalendarInterval.MAX_VALUE,
      new CalendarInterval(0, 0, 1)))
    intercept[ArithmeticException](addExact(CalendarInterval.MAX_VALUE,
      new CalendarInterval(0, 1, 0)))
    intercept[ArithmeticException](addExact(CalendarInterval.MAX_VALUE,
      new CalendarInterval(1, 0, 0)))
    assert(add(CalendarInterval.MAX_VALUE, new CalendarInterval(0, 0, 1)) ===
      new CalendarInterval(Int.MaxValue, Int.MaxValue, Long.MinValue))
    assert(add(CalendarInterval.MAX_VALUE, new CalendarInterval(0, 1, 0)) ===
      new CalendarInterval(Int.MaxValue, Int.MinValue, Long.MaxValue))
    assert(add(CalendarInterval.MAX_VALUE, new CalendarInterval(1, 0, 0)) ===
      new CalendarInterval(Int.MinValue, Int.MaxValue, Long.MaxValue))

    intercept[ArithmeticException](subtractExact(CalendarInterval.MAX_VALUE,
      new CalendarInterval(0, 0, -1)))
    intercept[ArithmeticException](subtractExact(CalendarInterval.MAX_VALUE,
      new CalendarInterval(0, -1, 0)))
    intercept[ArithmeticException](subtractExact(CalendarInterval.MAX_VALUE,
      new CalendarInterval(-1, 0, 0)))
    assert(subtract(CalendarInterval.MAX_VALUE, new CalendarInterval(0, 0, -1)) ===
      new CalendarInterval(Int.MaxValue, Int.MaxValue, Long.MinValue))
    assert(subtract(CalendarInterval.MAX_VALUE, new CalendarInterval(0, -1, 0)) ===
      new CalendarInterval(Int.MaxValue, Int.MinValue, Long.MaxValue))
    assert(subtract(CalendarInterval.MAX_VALUE, new CalendarInterval(-1, 0, 0)) ===
      new CalendarInterval(Int.MinValue, Int.MaxValue, Long.MaxValue))

    intercept[ArithmeticException](multiplyExact(CalendarInterval.MAX_VALUE, 2))
    intercept[ArithmeticException](divideExact(CalendarInterval.MAX_VALUE, 0.5))
  }
}
