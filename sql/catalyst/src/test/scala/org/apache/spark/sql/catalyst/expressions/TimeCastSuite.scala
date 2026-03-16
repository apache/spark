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

import java.time.LocalDateTime

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.unsafe.types.UTF8String

class TimeCastSuite extends SparkFunSuite {

  test("Cast TIME to STRING") {
    // Midnight
    assert(TimeCast.timeToString(0L) === UTF8String.fromString("00:00:00"))

    // Morning time
    assert(TimeCast.timeToString(36000000000L) === UTF8String.fromString("10:00:00"))

    // Afternoon time
    assert(TimeCast.timeToString(50400000000L) === UTF8String.fromString("14:00:00"))

    // Time with microseconds
    assert(TimeCast.timeToString(36000123456L) === UTF8String.fromString("10:00:00.123456"))

    // End of day
    assert(TimeCast.timeToString(86399999999L) === UTF8String.fromString("23:59:59.999999"))
  }

  test("Cast STRING to TIME - valid inputs") {
    // Basic format
    assert(TimeCast.stringToTime(UTF8String.fromString("10:30:45")) === Some(37845000000L))

    // With microseconds
    assert(TimeCast.stringToTime(UTF8String.fromString("10:30:45.123456")) === Some(37845123456L))

    // Midnight
    assert(TimeCast.stringToTime(UTF8String.fromString("00:00:00")) === Some(0L))

    // End of day
    assert(TimeCast.stringToTime(UTF8String.fromString("23:59:59.999999")) === Some(86399999999L))
  }

  test("Cast STRING to TIME - invalid inputs") {
    // Invalid format
    assert(TimeCast.stringToTime(UTF8String.fromString("invalid")) === None)

    // Out of range hour
    assert(TimeCast.stringToTime(UTF8String.fromString("24:00:00")) === None)

    // Out of range minute
    assert(TimeCast.stringToTime(UTF8String.fromString("10:60:00")) === None)

    // Out of range second
    assert(TimeCast.stringToTime(UTF8String.fromString("10:30:60")) === None)

    // Empty string
    assert(TimeCast.stringToTime(UTF8String.fromString("")) === None)

    // Format with timezone - should be rejected as TIME type doesn't support timezones
    assert(TimeCast.stringToTime(UTF8String.fromString("12:00:00Z")) === None)
    assert(TimeCast.stringToTime(UTF8String.fromString("12:00:00+08:00")) === None)
  }

  test("Cast TIME to TIMESTAMP") {
    val epochDate = DateTimeUtils.localDateToDays(java.time.LocalDate.ofEpochDay(0))

    // Midnight
    val ts1 = TimeCast.timeToTimestamp(0L)
    assert(ts1 === DateTimeUtils.microsToInstant(0L))

    // 10:00:00
    val ts2 = TimeCast.timeToTimestamp(36000000000L)
    assert(ts2 === DateTimeUtils.microsToInstant(36000000000L))

    // 23:59:59.999999
    val ts3 = TimeCast.timeToTimestamp(86399999999L)
    assert(ts3 === DateTimeUtils.microsToInstant(86399999999L))
  }

  test("Cast TIMESTAMP to TIME") {
    // Timestamp at midnight
    val ts1 = DateTimeUtils.instantToMicros(
      LocalDateTime.of(2024, 1, 15, 0, 0, 0).toInstant(java.time.ZoneOffset.UTC))
    assert(TimeCast.timestampToTime(ts1) === 0L)

    // Timestamp at 10:30:45
    val ts2 = DateTimeUtils.instantToMicros(
      LocalDateTime.of(2024, 1, 15, 10, 30, 45).toInstant(java.time.ZoneOffset.UTC))
    assert(TimeCast.timestampToTime(ts2) === 37845000000L)

    // Timestamp at 23:59:59
    val ts3 = DateTimeUtils.instantToMicros(
      LocalDateTime.of(2024, 1, 15, 23, 59, 59).toInstant(java.time.ZoneOffset.UTC))
    assert(TimeCast.timestampToTime(ts3) === 86399000000L)
  }

  test("Cast TIME to LONG") {
    assert(TimeCast.timeToLong(0L) === 0L)
    assert(TimeCast.timeToLong(36000000000L) === 36000000000L)
    assert(TimeCast.timeToLong(86399999999L) === 86399999999L)
  }

  test("Cast LONG to TIME - valid values") {
    assert(TimeCast.longToTime(0L) === Some(0L))
    assert(TimeCast.longToTime(36000000000L) === Some(36000000000L))
    assert(TimeCast.longToTime(86399999999L) === Some(86399999999L))
  }

  test("Cast LONG to TIME - invalid values") {
    // Negative value
    assert(TimeCast.longToTime(-1L) === None)

    // Value too large (>= 24 hours in microseconds)
    assert(TimeCast.longToTime(86400000000L) === None)
    assert(TimeCast.longToTime(100000000000L) === None)
  }

  test("Cast TIME to INT") {
    // Midnight
    assert(TimeCast.timeToInt(0L) === 0)

    // 10:00:00 (36000 seconds)
    assert(TimeCast.timeToInt(36000000000L) === 36000)

    // 23:59:59 (86399 seconds)
    assert(TimeCast.timeToInt(86399000000L) === 86399)

    // With microseconds (should truncate)
    assert(TimeCast.timeToInt(36000123456L) === 36000)
  }

  test("Cast INT to TIME - valid values") {
    // Midnight
    assert(TimeCast.intToTime(0) === Some(0L))

    // 10:00:00 (36000 seconds)
    assert(TimeCast.intToTime(36000) === Some(36000000000L))

    // 23:59:59 (86399 seconds)
    assert(TimeCast.intToTime(86399) === Some(86399000000L))
  }

  test("Cast INT to TIME - invalid values") {
    // Negative value
    assert(TimeCast.intToTime(-1) === None)

    // Value too large (>= 86400 seconds)
    assert(TimeCast.intToTime(86400) === None)
    assert(TimeCast.intToTime(100000) === None)
  }

  test("Cast TIME to DOUBLE") {
    // Midnight
    assert(TimeCast.timeToDouble(0L) === 0.0)

    // 10:00:00
    assert(TimeCast.timeToDouble(36000000000L) === 36000.0)

    // 10:00:00.5
    assert(TimeCast.timeToDouble(36000500000L) === 36000.5)

    // 23:59:59.999999
    assert(Math.abs(TimeCast.timeToDouble(86399999999L) - 86399.999999) < 0.000001)
  }

  test("Cast DOUBLE to TIME - valid values") {
    // Midnight
    assert(TimeCast.doubleToTime(0.0) === Some(0L))

    // 10:00:00
    assert(TimeCast.doubleToTime(36000.0) === Some(36000000000L))

    // 10:00:00.5
    assert(TimeCast.doubleToTime(36000.5) === Some(36000500000L))

    // 23:59:59.999999
    assert(TimeCast.doubleToTime(86399.999999).isDefined)
  }

  test("Cast DOUBLE to TIME - invalid values") {
    // Negative value
    assert(TimeCast.doubleToTime(-1.0) === None)

    // Value too large
    assert(TimeCast.doubleToTime(86400.0) === None)
    assert(TimeCast.doubleToTime(100000.0) === None)

    // NaN
    assert(TimeCast.doubleToTime(Double.NaN) === None)

    // Infinity
    assert(TimeCast.doubleToTime(Double.PositiveInfinity) === None)
    assert(TimeCast.doubleToTime(Double.NegativeInfinity) === None)
  }

  test("Cast TIME to FLOAT") {
    // Midnight
    assert(TimeCast.timeToFloat(0L) === 0.0f)

    // 10:00:00
    assert(TimeCast.timeToFloat(36000000000L) === 36000.0f)

    // 10:00:00.5
    assert(Math.abs(TimeCast.timeToFloat(36000500000L) - 36000.5f) < 0.1f)
  }

  test("Cast FLOAT to TIME - valid values") {
    // Midnight
    assert(TimeCast.floatToTime(0.0f) === Some(0L))

    // 10:00:00
    assert(TimeCast.floatToTime(36000.0f) === Some(36000000000L))

    // 10:00:00.5
    val result = TimeCast.floatToTime(36000.5f)
    assert(result.isDefined)
    assert(Math.abs(result.get - 36000500000L) < 1000000L) // Within 1 second due to float precision
  }

  test("Cast FLOAT to TIME - invalid values") {
    // Negative value
    assert(TimeCast.floatToTime(-1.0f) === None)

    // Value too large
    assert(TimeCast.floatToTime(86400.0f) === None)

    // NaN
    assert(TimeCast.floatToTime(Float.NaN) === None)

    // Infinity
    assert(TimeCast.floatToTime(Float.PositiveInfinity) === None)
    assert(TimeCast.floatToTime(Float.NegativeInfinity) === None)
  }

  test("Cast TIME to BOOLEAN") {
    // Midnight (0) should be false
    assert(TimeCast.timeToBoolean(0L) === false)

    // Any non-zero time should be true
    assert(TimeCast.timeToBoolean(1L) === true)
    assert(TimeCast.timeToBoolean(36000000000L) === true)
    assert(TimeCast.timeToBoolean(86399999999L) === true)
  }

  test("Cast BOOLEAN to TIME") {
    // false -> midnight
    assert(TimeCast.booleanToTime(false) === 0L)

    // true -> 1 microsecond after midnight
    assert(TimeCast.booleanToTime(true) === 1L)
  }

  test("Cast TIME to DATE") {
    // All TIME values should map to epoch date (1970-01-01)
    val epochDate = DateTimeUtils.localDateToDays(java.time.LocalDate.ofEpochDay(0))

    assert(TimeCast.timeToDate(0L) === epochDate)
    assert(TimeCast.timeToDate(36000000000L) === epochDate)
    assert(TimeCast.timeToDate(86399999999L) === epochDate)
  }

  test("Cast DATE to TIME") {
    // Any date should map to midnight
    val date1 = DateTimeUtils.localDateToDays(java.time.LocalDate.of(2024, 1, 15))
    assert(TimeCast.dateToTime(date1) === 0L)

    val date2 = DateTimeUtils.localDateToDays(java.time.LocalDate.of(2000, 6, 15))
    assert(TimeCast.dateToTime(date2) === 0L)
  }

  test("Cast unsupported types to TIME") {
    // These should return None
    assert(TimeCast.binaryToTime(Array[Byte](1, 2, 3)) === None)
  }

  test("Cast TIME to unsupported types") {
    // These should return None
    assert(TimeCast.timeToBinary(36000000000L) === None)
  }

  test("Round-trip conversions") {
    val times = Seq(0L, 36000000000L, 50400123456L, 86399999999L)

    for (time <- times) {
      // TIME -> STRING -> TIME
      val str = TimeCast.timeToString(time)
      val backToTime = TimeCast.stringToTime(str)
      assert(backToTime === Some(time))

      // TIME -> LONG -> TIME
      val long = TimeCast.timeToLong(time)
      val backFromLong = TimeCast.longToTime(long)
      assert(backFromLong === Some(time))

      // TIME -> TIMESTAMP -> TIME
      val ts = TimeCast.timeToTimestamp(time)
      val tsInMicros = DateTimeUtils.instantToMicros(ts)
      val backFromTs = TimeCast.timestampToTime(tsInMicros)
      assert(backFromTs === time)
    }
  }
}
