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

import java.time.{ZoneId, ZoneOffset}

import org.apache.spark.{SparkDateTimeException, SparkFunSuite}
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.unsafe.types.{TimestampNanosVal, UTF8String}

/**
 * Tests for string-to-nanosecond timestamp parsing added under SPARK-57032. The parser keeps the
 * microsecond part (fractional digits 1-6) and the sub-microsecond remainder (digits 7-9, in
 * [0, 999]) and applies the target fractional precision `p` in [7, 9] by truncating extra digits.
 */
class TimestampNanosParseSuite extends SparkFunSuite {

  private val losAngeles = getZoneId("America/Los_Angeles")

  private def ntz(
      str: String,
      precision: Int,
      allowTimeZone: Boolean = true): Option[TimestampNanosVal] = {
    stringToTimestampNTZNanos(UTF8String.fromString(str), precision, allowTimeZone)
  }

  private def ltz(str: String, precision: Int, zoneId: ZoneId): Option[TimestampNanosVal] = {
    stringToTimestampLTZNanos(UTF8String.fromString(str), precision, zoneId)
  }

  test("NTZ: fractional digits 7-9 are preserved as nanosWithinMicro") {
    assert(ntz("2015-01-02 00:00:00.123456789", 9).get ===
      TimestampNanosVal.fromParts(date(2015, 1, 2, 0, 0, 0, 123456, ZoneOffset.UTC), 789.toShort))
    assert(ntz("2015-01-02 00:00:00.1234567", 9).get ===
      TimestampNanosVal.fromParts(date(2015, 1, 2, 0, 0, 0, 123456, ZoneOffset.UTC), 700.toShort))
    assert(ntz("2015-01-02 00:00:00.12345678", 9).get ===
      TimestampNanosVal.fromParts(date(2015, 1, 2, 0, 0, 0, 123456, ZoneOffset.UTC), 780.toShort))
  }

  test("NTZ: precision truncates excess sub-microsecond digits toward zero") {
    val micros = date(2020, 12, 31, 23, 59, 59, 123456, ZoneOffset.UTC)
    assert(ntz("2020-12-31 23:59:59.123456789", 9).get ===
      TimestampNanosVal.fromParts(micros, 789.toShort))
    assert(ntz("2020-12-31 23:59:59.123456789", 8).get ===
      TimestampNanosVal.fromParts(micros, 780.toShort))
    assert(ntz("2020-12-31 23:59:59.123456789", 7).get ===
      TimestampNanosVal.fromParts(micros, 700.toShort))
  }

  test("NTZ: digits beyond the 9th are dropped") {
    val expected = TimestampNanosVal.fromParts(
      date(2020, 12, 31, 23, 59, 59, 123456, ZoneOffset.UTC), 789.toShort)
    assert(ntz("2020-12-31 23:59:59.1234567890", 9).get === expected)
    assert(ntz("2020-12-31 23:59:59.123456789999", 9).get === expected)
  }

  test("NTZ: fewer than 6 fractional digits yield zero nanosWithinMicro") {
    assert(ntz("2020-01-01 00:00:00.0", 9).get ===
      TimestampNanosVal.fromParts(date(2020, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC), 0.toShort))
    assert(ntz("2020-01-01 00:00:00.1", 9).get ===
      TimestampNanosVal.fromParts(date(2020, 1, 1, 0, 0, 0, 100000, ZoneOffset.UTC), 0.toShort))
    assert(ntz("2020-01-01 00:00:00.123456", 9).get ===
      TimestampNanosVal.fromParts(date(2020, 1, 1, 0, 0, 0, 123456, ZoneOffset.UTC), 0.toShort))
  }

  test("NTZ: trailing zeros in the sub-microsecond part") {
    assert(ntz("2015-01-02 00:00:00.000050000", 9).get ===
      TimestampNanosVal.fromParts(date(2015, 1, 2, 0, 0, 0, 50, ZoneOffset.UTC), 0.toShort))
    assert(ntz("2015-01-02 00:00:00.100000009", 9).get ===
      TimestampNanosVal.fromParts(date(2015, 1, 2, 0, 0, 0, 100000, ZoneOffset.UTC), 9.toShort))
  }

  test("NTZ: maximum and minimum sub-microsecond fractions") {
    assert(ntz("2020-06-15 12:00:00.999999999", 9).get ===
      TimestampNanosVal.fromParts(date(2020, 6, 15, 12, 0, 0, 999999, ZoneOffset.UTC), 999.toShort))
    assert(ntz("2020-06-15 12:00:00.000000001", 9).get ===
      TimestampNanosVal.fromParts(date(2020, 6, 15, 12, 0, 0, 0, ZoneOffset.UTC), 1.toShort))
    // ".000000001" loses its only sub-micro digit at precision 8 and 7.
    assert(ntz("2020-06-15 12:00:00.000000001", 8).get.nanosWithinMicro === 0.toShort)
    assert(ntz("2020-06-15 12:00:00.000000001", 7).get.nanosWithinMicro === 0.toShort)
  }

  test("NTZ: time zone component is discarded or rejected based on allowTimeZone") {
    // With allowTimeZone = true (default) the zone suffix is discarded.
    assert(ntz("2015-03-18T12:03:17.123456789Z", 9).get ===
      TimestampNanosVal.fromParts(
        date(2015, 3, 18, 12, 3, 17, 123456, ZoneOffset.UTC), 789.toShort))
    // With allowTimeZone = false a zone suffix makes the input invalid.
    assert(ntz("2015-03-18T12:03:17.123456789Z", 9, allowTimeZone = false).isEmpty)
    // A time-only input cannot be parsed as TIMESTAMP_NTZ.
    assert(ntz("12:03:17.123456789", 9).isEmpty)
  }

  test("LTZ: explicit zone offset in the string") {
    val expected = TimestampNanosVal.fromParts(
      date(2015, 3, 18, 12, 3, 17, 123456, getZoneId("+07:00")), 789.toShort)
    assert(ltz("2015-03-18T12:03:17.123456789+07:00", 9, ZoneOffset.UTC).get === expected)
  }

  test("LTZ: region-based zone in the string") {
    val expected = TimestampNanosVal.fromParts(
      date(2015, 3, 18, 12, 3, 17, 123456, getZoneId("Europe/Moscow")), 789.toShort)
    assert(ltz("2015-03-18T12:03:17.123456789 Europe/Moscow", 9, ZoneOffset.UTC).get === expected)
  }

  test("LTZ: falls back to the session zone when the string has no zone") {
    val expected = TimestampNanosVal.fromParts(
      date(2015, 3, 18, 12, 3, 17, 123456, losAngeles), 789.toShort)
    assert(ltz("2015-03-18 12:03:17.123456789", 9, losAngeles).get === expected)
  }

  test("LTZ: precision truncation matches the NTZ path") {
    val micros = date(2015, 3, 18, 12, 3, 17, 123456, ZoneOffset.UTC)
    assert(ltz("2015-03-18T12:03:17.123456789Z", 7, ZoneOffset.UTC).get ===
      TimestampNanosVal.fromParts(micros, 700.toShort))
    assert(ltz("2015-03-18T12:03:17.123456789Z", 8, ZoneOffset.UTC).get ===
      TimestampNanosVal.fromParts(micros, 780.toShort))
  }

  test("range edge cases with sub-microsecond fractions") {
    // Unix epoch.
    assert(ntz("1970-01-01 00:00:00.000000001", 9).get ===
      TimestampNanosVal.fromParts(0L, 1.toShort))
    // Julian/Gregorian cutover.
    assert(ntz("1582-10-15 00:00:00.123456789", 9).get ===
      TimestampNanosVal.fromParts(date(1582, 10, 15, 0, 0, 0, 123456, ZoneOffset.UTC), 789.toShort))
    // End of the supported range.
    assert(ntz("9999-12-31 23:59:59.999999999", 9).get ===
      TimestampNanosVal.fromParts(
        date(9999, 12, 31, 23, 59, 59, 999999, ZoneOffset.UTC), 999.toShort))
  }

  test("invalid inputs return None") {
    assert(ntz("not a timestamp", 9).isEmpty)
    assert(ntz("", 9).isEmpty)
    assert(ltz("2015-13-40 99:99:99.123456789", 9, ZoneOffset.UTC).isEmpty)
  }

  test("ANSI variants throw on invalid input") {
    val ntzValid = stringToTimestampNTZNanosAnsi(
      UTF8String.fromString("2015-01-02 00:00:00.123456789"), 9)
    assert(ntzValid ===
      TimestampNanosVal.fromParts(date(2015, 1, 2, 0, 0, 0, 123456, ZoneOffset.UTC), 789.toShort))

    val ltzValid = stringToTimestampLTZNanosAnsi(
      UTF8String.fromString("2015-01-02 00:00:00.123456789Z"), 9, ZoneOffset.UTC)
    assert(ltzValid ===
      TimestampNanosVal.fromParts(date(2015, 1, 2, 0, 0, 0, 123456, ZoneOffset.UTC), 789.toShort))

    intercept[SparkDateTimeException] {
      stringToTimestampNTZNanosAnsi(UTF8String.fromString("invalid"), 9)
    }
    intercept[SparkDateTimeException] {
      stringToTimestampLTZNanosAnsi(UTF8String.fromString("invalid"), 9, ZoneOffset.UTC)
    }
  }
}
