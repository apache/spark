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
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.IntervalUtils._
import org.apache.spark.sql.catalyst.util.IntervalUtils.IntervalUnit._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class IntervalUtilsSuite extends SparkFunSuite {

  private def checkFromString(input: String, expected: CalendarInterval): Unit = {
    assert(fromString(input) === expected)
    assert(stringToInterval(UTF8String.fromString(input)) === expected)
  }

  private def checkFromInvalidString(input: String, errorMsg: String): Unit = {
    try {
      fromString(input)
      fail("Expected to throw an exception for the invalid input")
    } catch {
      case e: IllegalArgumentException =>
        val msg = e.getMessage
        assert(msg.contains(errorMsg))
    }
    assert(stringToInterval(UTF8String.fromString(input)) === null)
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

    for (input <- Seq("", " ", "interval", "interval1 day", "foo", "foo 1 day")) {
      checkFromInvalidString(input, "Invalid interval string")
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
    checkFromInvalidString("1.5 days", "Error parsing interval string")
    checkFromInvalidString("1. hour", "Error parsing interval string")
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
    checkFromInvalidString("0.123456789123 seconds", "Error parsing interval string")
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

  test("from day-time string") {
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

  test("interval duration") {
    def duration(s: String, unit: TimeUnit, daysPerMonth: Int): Long = {
      IntervalUtils.getDuration(fromString(s), unit, daysPerMonth)
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
      IntervalUtils.isNegative(fromString(s), daysPerMonth)
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
    assert(negate(new CalendarInterval(1, 2, 3)) === new CalendarInterval(-1, -2, -3))
  }

  test("subtract one interval by another") {
    val input1 = new CalendarInterval(3, 1, 1 * MICROS_PER_HOUR)
    val input2 = new CalendarInterval(2, 4, 100 * MICROS_PER_HOUR)
    assert(new CalendarInterval(1, -3, -99 * MICROS_PER_HOUR) === subtract(input1, input2))
    val input3 = new CalendarInterval(-10, -30, -81 * MICROS_PER_HOUR)
    val input4 = new CalendarInterval(75, 150, 200 * MICROS_PER_HOUR)
    assert(new CalendarInterval(-85, -180, -281 * MICROS_PER_HOUR) === subtract(input3, input4))
  }

  test("add two intervals") {
    val input1 = new CalendarInterval(3, 1, 1 * MICROS_PER_HOUR)
    val input2 = new CalendarInterval(2, 4, 100 * MICROS_PER_HOUR)
    assert(new CalendarInterval(5, 5, 101 * MICROS_PER_HOUR) === add(input1, input2))

    val input3 = new CalendarInterval(-10, -30, -81 * MICROS_PER_HOUR)
    val input4 = new CalendarInterval(75, 150, 200 * MICROS_PER_HOUR)
    assert(new CalendarInterval(65, 120, 119 * MICROS_PER_HOUR) === add(input3, input4))
  }

  test("multiply by num") {
    var interval = new CalendarInterval(0, 0, 0)
    assert(interval === multiply(interval, 0))
    interval = new CalendarInterval(123, 456, 789)
    assert(new CalendarInterval(123 * 42, 456 * 42, 789 * 42) === multiply(interval, 42))
    interval = new CalendarInterval(-123, -456, -789)
    assert(new CalendarInterval(-123 * 42, -456 * 42, -789 * 42) === multiply(interval, 42))
    assert(new CalendarInterval(1, 22, 12 * MICROS_PER_HOUR) ===
      multiply(new CalendarInterval(1, 5, 0), 1.5))
    assert(new CalendarInterval(2, 14, 12 * MICROS_PER_HOUR) ===
      multiply(new CalendarInterval(2, 2, 2 * MICROS_PER_HOUR), 1.2))
    try {
      multiply(new CalendarInterval(2, 0, 0), Integer.MAX_VALUE)
      fail("Expected to throw an exception on months overflow")
    } catch {
      case e: ArithmeticException =>
        assert(e.getMessage.contains("overflow"))
    }
  }

  test("divide by num") {
    var interval = new CalendarInterval(0, 0, 0)
    assert(interval === divide(interval, 10))
    interval = new CalendarInterval(1, 3, 30 * MICROS_PER_SECOND)
    assert(new CalendarInterval(0, 16, 12 * MICROS_PER_HOUR + 15 * MICROS_PER_SECOND) ===
      divide(interval, 2))
    assert(new CalendarInterval(2, 6, MICROS_PER_MINUTE) === divide(interval, 0.5))
    interval = new CalendarInterval(-1, 0, -30 * MICROS_PER_SECOND)
    assert(new CalendarInterval(0, -15, -15 * MICROS_PER_SECOND) === divide(interval, 2))
    assert(new CalendarInterval(-2, 0, -1 * MICROS_PER_MINUTE) === divide(interval, 0.5))
    try {
      divide(new CalendarInterval(123, 456, 789), 0)
      fail("Expected to throw an exception on divide by zero")
    } catch {
      case e: ArithmeticException =>
        assert(e.getMessage.contains("divide by zero"))
    }
  }
}
