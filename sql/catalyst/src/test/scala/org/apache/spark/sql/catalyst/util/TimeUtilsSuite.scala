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

import java.time.{LocalTime, ZoneId}

import org.apache.spark.SparkFunSuite
import org.apache.spark.unsafe.types.UTF8String

class TimeUtilsSuite extends SparkFunSuite {

  test("stringToTime - valid time strings") {
    // Test various valid time formats
    assert(TimeUtils.stringToTime(UTF8String.fromString("00:00:00")).contains(0L))
    assert(TimeUtils.stringToTime(UTF8String.fromString("12:30:45")).contains(45045000000L))
    assert(TimeUtils.stringToTime(UTF8String.fromString("23:59:59")).contains(86399000000L))
    assert(TimeUtils.stringToTime(UTF8String.fromString("14:30:00.123456")).contains(52200123456L))
    assert(TimeUtils.stringToTime(UTF8String.fromString("09:15:30.5")).contains(33330500000L))
  }

  test("stringToTime - invalid time strings") {
    // Test invalid formats
    assert(TimeUtils.stringToTime(UTF8String.fromString("25:00:00")).isEmpty)
    assert(TimeUtils.stringToTime(UTF8String.fromString("12:60:00")).isEmpty)
    assert(TimeUtils.stringToTime(UTF8String.fromString("12:30:60")).isEmpty)
    assert(TimeUtils.stringToTime(UTF8String.fromString("invalid")).isEmpty)
    assert(TimeUtils.stringToTime(UTF8String.fromString("12:30")).isEmpty)
    assert(TimeUtils.stringToTime(null).isEmpty)
  }

  test("timeToString - valid time values") {
    // Test conversion from microseconds to string
    assert(TimeUtils.timeToString(0L) === UTF8String.fromString("00:00:00.000000"))
    assert(TimeUtils.timeToString(45045000000L) === UTF8String.fromString("12:30:45.000000"))
    assert(TimeUtils.timeToString(86399999999L) === UTF8String.fromString("23:59:59.999999"))
    assert(TimeUtils.timeToString(52200123456L) === UTF8String.fromString("14:30:00.123456"))
  }

  test("isValidTime - range validation") {
    // Test valid range
    assert(TimeUtils.isValidTime(0L))
    assert(TimeUtils.isValidTime(43200000000L)) // noon
    assert(TimeUtils.isValidTime(86399999999L)) // max valid time

    // Test invalid range
    assert(!TimeUtils.isValidTime(-1L))
    assert(!TimeUtils.isValidTime(86400000000L)) // 24:00:00
    assert(!TimeUtils.isValidTime(Long.MaxValue))
  }

  test("microsToLocalTime - conversion") {
    val midnight = TimeUtils.microsToLocalTime(0L)
    assert(midnight === LocalTime.of(0, 0, 0, 0))

    val noon = TimeUtils.microsToLocalTime(43200000000L)
    assert(noon === LocalTime.of(12, 0, 0, 0))

    val withMicros = TimeUtils.microsToLocalTime(52200123456L)
    assert(withMicros === LocalTime.of(14, 30, 0, 123456000))
  }

  test("localTimeToMicros - conversion") {
    assert(TimeUtils.localTimeToMicros(LocalTime.of(0, 0, 0, 0)) === 0L)
    assert(TimeUtils.localTimeToMicros(LocalTime.of(12, 0, 0, 0)) === 43200000000L)
    assert(TimeUtils.localTimeToMicros(LocalTime.of(14, 30, 0, 123456000)) === 52200123456L)
    assert(TimeUtils.localTimeToMicros(LocalTime.of(23, 59, 59, 999999000)) === 86399999999L)
  }

  test("extractTimeFromTimestamp - with timezone") {
    val zoneId = ZoneId.of("America/Los_Angeles")

    // Test extracting time from timestamp
    // 2023-01-15 14:30:00 UTC = some time in LA timezone
    val timestampMicros = 1673792400000000L // 2023-01-15 14:30:00 UTC
    val timeMicros = TimeUtils.extractTimeFromTimestamp(timestampMicros, zoneId)

    // Verify it's a valid time value
    assert(TimeUtils.isValidTime(timeMicros))
    assert(timeMicros >= 0L && timeMicros < 86400000000L)
  }

  test("roundtrip conversion - string to time to string") {
    val testCases = Seq(
      "00:00:00.000000",
      "12:30:45.123456",
      "23:59:59.999999",
      "09:15:30.500000"
    )

    testCases.foreach { timeStr =>
      val utf8Str = UTF8String.fromString(timeStr)
      val timeMicros = TimeUtils.stringToTime(utf8Str).get
      val result = TimeUtils.timeToString(timeMicros)
      assert(result === utf8Str, s"Roundtrip failed for $timeStr")
    }
  }

  test("roundtrip conversion - LocalTime to micros to LocalTime") {
    val testCases = Seq(
      LocalTime.of(0, 0, 0, 0),
      LocalTime.of(12, 30, 45, 123456000),
      LocalTime.of(23, 59, 59, 999999000),
      LocalTime.of(9, 15, 30, 500000000)
    )

    testCases.foreach { localTime =>
      val micros = TimeUtils.localTimeToMicros(localTime)
      val result = TimeUtils.microsToLocalTime(micros)
      assert(result === localTime, s"Roundtrip failed for $localTime")
    }
  }

  test("edge cases - midnight and end of day") {
    // Midnight
    assert(TimeUtils.stringToTime(UTF8String.fromString("00:00:00.000000")).contains(0L))
    assert(TimeUtils.timeToString(0L) === UTF8String.fromString("00:00:00.000000"))

    // End of day (one microsecond before midnight)
    val endOfDay = 86399999999L
    assert(TimeUtils.isValidTime(endOfDay))
    assert(TimeUtils.timeToString(endOfDay) === UTF8String.fromString("23:59:59.999999"))

    // Exactly 24:00:00 should be invalid
    assert(!TimeUtils.isValidTime(86400000000L))
  }
}

// Made with Bob
