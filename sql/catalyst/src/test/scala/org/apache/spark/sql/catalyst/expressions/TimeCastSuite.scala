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
    assert(TimeCast.castTimeToString(0L) === UTF8String.fromString("00:00:00"))

    // Morning time
    assert(TimeCast.castTimeToString(36000000000L) === UTF8String.fromString("10:00:00"))

    // Afternoon time
    assert(TimeCast.castTimeToString(50400000000L) === UTF8String.fromString("14:00:00"))

    // Time with microseconds
    assert(TimeCast.castTimeToString(36000123456L) === UTF8String.fromString("10:00:00.123456"))

    // End of day
    assert(TimeCast.castTimeToString(86399999999L) === UTF8String.fromString("23:59:59.999999"))
  }

  test("Cast STRING to TIME - valid inputs") {
    // Basic format
    assert(TimeCast.castStringToTime(UTF8String.fromString("10:30:45")) === 37845000000L)

    // With microseconds
    assert(TimeCast.castStringToTime(UTF8String.fromString("10:30:45.123456")) === 37845123456L)

    // Midnight
    assert(TimeCast.castStringToTime(UTF8String.fromString("00:00:00")) === 0L)

    // End of day
    assert(TimeCast.castStringToTime(UTF8String.fromString("23:59:59.999999")) === 86399999999L)
  }

  test("Cast STRING to TIME - invalid inputs") {
    // Invalid format
    assert(TimeCast.castStringToTime(UTF8String.fromString("invalid")) === null)

    // Out of range hour
    assert(TimeCast.castStringToTime(UTF8String.fromString("24:00:00")) === null)

    // Empty string
    assert(TimeCast.castStringToTime(UTF8String.fromString("")) === null)
  }

  test("Cast TIME to TIMESTAMP") {
    val zoneId = java.time.ZoneId.of("UTC")

    // Midnight
    val ts1 = TimeCast.castTimeToTimestamp(0L, zoneId)
    assert(ts1 === 0L)

    // 10:00:00
    val ts2 = TimeCast.castTimeToTimestamp(36000000000L, zoneId)
    assert(ts2 === 36000000000L)
  }

  test("Cast TIMESTAMP to TIME") {
    val zoneId = java.time.ZoneId.of("UTC")

    // Timestamp at midnight
    val ts1 = DateTimeUtils.instantToMicros(
      LocalDateTime.of(2024, 1, 15, 0, 0, 0).toInstant(java.time.ZoneOffset.UTC))
    assert(TimeCast.castTimestampToTime(ts1, zoneId) === 0L)

    // Timestamp at 10:30:45
    val ts2 = DateTimeUtils.instantToMicros(
      LocalDateTime.of(2024, 1, 15, 10, 30, 45).toInstant(java.time.ZoneOffset.UTC))
    assert(TimeCast.castTimestampToTime(ts2, zoneId) === 37845000000L)
  }

  test("Cast TIME to LONG/INT") {
    val time = 37845123456L // 10:30:45.123456
    assert(TimeCast.castTimeToLong(time) === 37845L)
    assert(TimeCast.castTimeToInt(time) === 37845)
  }

  test("Cast LONG to TIME - valid values") {
    assert(TimeCast.castLongToTime(0L) === 0L)
    assert(TimeCast.castLongToTime(36000L) === 36000000000L)
  }

  test("Cast LONG to TIME - invalid values") {
    // Negative value
    assert(TimeCast.castLongToTime(-1L) === null)

    // Value too large (>= 24 hours in seconds)
    assert(TimeCast.castLongToTime(86400L) === null)
  }

  test("Cast DATE to TIME") {
    val date1 = DateTimeUtils.localDateToDays(java.time.LocalDate.of(2024, 1, 15))
    assert(TimeCast.castDateToTime(date1) === 0L)
  }
}
