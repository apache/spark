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
import org.apache.spark.sql.catalyst.util.IntervalUtils.{fromDayTimeString, fromString, fromYearMonthString}
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.unsafe.types.CalendarInterval._

class IntervalUtilsSuite extends SparkFunSuite {

  test("fromString: basic") {
    testSingleUnit("YEAR", 3, 36, 0, 0)
    testSingleUnit("Month", 3, 3, 0, 0)
    testSingleUnit("Week", 3, 0, 21, 0)
    testSingleUnit("DAY", 3, 0, 3, 0)
    testSingleUnit("HouR", 3, 0, 0, 3 * MICROS_PER_HOUR)
    testSingleUnit("MiNuTe", 3, 0, 0, 3 * MICROS_PER_MINUTE)
    testSingleUnit("Second", 3, 0, 0, 3 * MICROS_PER_SECOND)
    testSingleUnit("MilliSecond", 3, 0, 0, 3 * MICROS_PER_MILLI)
    testSingleUnit("MicroSecond", 3, 0, 0, 3)

    for (input <- Seq(null, "", " ")) {
      try {
        fromString(input)
        fail("Expected to throw an exception for the invalid input")
      } catch {
        case e: IllegalArgumentException =>
          val msg = e.getMessage
          if (input == null) {
            assert(msg.contains("cannot be null"))
          }
      }
    }

    for (input <- Seq("interval", "interval 1 day", "interval1 day", "foo", "foo 1 day")) {
      try {
        fromString(input)
        fail("Expected to throw an exception for the invalid input")
      } catch {
        case e: IllegalArgumentException =>
          val msg = e.getMessage
          assert(msg.contains("Invalid interval string"))
      }
    }
  }

  test("fromString: random order field") {
    val input = "1 day 1 year"
    val result = new CalendarInterval(12, 1, 0)
    assert(fromString(input) == result)
  }

  test("fromString: duplicated fields") {
    val input = "1 day 1 day"
    val result = new CalendarInterval(0, 2, 0)
    assert(fromString(input) == result)
  }

  test("fromString: value with +/-") {
    val input = "+1 year -1 day"
    val result = new CalendarInterval(12, -1, 0)
    assert(fromString(input) == result)
  }

  private def testSingleUnit(
      unit: String, number: Int, months: Int, days: Int, microseconds: Long): Unit = {
    val input1 = number + " " + unit
    val input2 = number + " " + unit + "s"
    val result = new CalendarInterval(months, days, microseconds)
    assert(fromString(input1) == result)
    assert(fromString(input2) == result)
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
        12 * MICROS_PER_MINUTE + 888 * MICROS_PER_MILLI))
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
      fromDayTimeString("5 1:12:20", "hour", "microsecond")
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

  test("legacyCastStringToInterval") {
    for (input <- Seq("inTERval 1 year", "1 year")) {
      val result = new CalendarInterval(12, 0, 0)
      assert(IntervalUtils.legacyCastStringToInterval(input) == result)
    }
  }
}
