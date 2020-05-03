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

package org.apache.spark.sql.util

import java.time.{DateTimeException, Instant, LocalDateTime, LocalTime, ZoneOffset}
import java.util.concurrent.TimeUnit

import org.scalatest.Matchers

import org.apache.spark.{SparkFunSuite, SparkUpgradeException}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.{DateTimeTestUtils, DateTimeUtils, LegacyDateFormats, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.{CET, PST, UTC}
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.unsafe.types.UTF8String

class TimestampFormatterSuite extends SparkFunSuite with SQLHelper with Matchers {

  test("parsing timestamps using time zones") {
    val localDate = "2018-12-02T10:11:12.001234"
    val expectedMicros = Map(
      "UTC" -> 1543745472001234L,
      PST.getId -> 1543774272001234L,
      CET.getId -> 1543741872001234L,
      "Africa/Dakar" -> 1543745472001234L,
      "America/Los_Angeles" -> 1543774272001234L,
      "Antarctica/Vostok" -> 1543723872001234L,
      "Asia/Hong_Kong" -> 1543716672001234L,
      "Europe/Amsterdam" -> 1543741872001234L)
    DateTimeTestUtils.outstandingTimezonesIds.foreach { zoneId =>
      val formatter = TimestampFormatter(
        "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
        DateTimeUtils.getZoneId(zoneId),
        needVarLengthSecondFraction = true)
      val microsSinceEpoch = formatter.parse(localDate)
      assert(microsSinceEpoch === expectedMicros(zoneId))
    }
  }

  test("format timestamps using time zones") {
    val microsSinceEpoch = 1543745472001234L
    val expectedTimestamp = Map(
      "UTC" -> "2018-12-02T10:11:12.001234",
      PST.getId -> "2018-12-02T02:11:12.001234",
      CET.getId -> "2018-12-02T11:11:12.001234",
      "Africa/Dakar" -> "2018-12-02T10:11:12.001234",
      "America/Los_Angeles" -> "2018-12-02T02:11:12.001234",
      "Antarctica/Vostok" -> "2018-12-02T16:11:12.001234",
      "Asia/Hong_Kong" -> "2018-12-02T18:11:12.001234",
      "Europe/Amsterdam" -> "2018-12-02T11:11:12.001234")
    DateTimeTestUtils.outstandingTimezonesIds.foreach { zoneId =>
      val formatter = TimestampFormatter(
        "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
        DateTimeUtils.getZoneId(zoneId))
      val timestamp = formatter.format(microsSinceEpoch)
      assert(timestamp === expectedTimestamp(zoneId))
    }
  }

  test("roundtrip micros -> timestamp -> micros using timezones") {
    Seq("yyyy-MM-dd'T'HH:mm:ss.SSSSSS", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXXXX").foreach { pattern =>
      Seq(
        -58710115316212000L,
        -18926315945345679L,
        -9463427405253013L,
        -244000001L,
        0L,
        99628200102030L,
        1543749753123456L,
        2177456523456789L,
        11858049903010203L).foreach { micros =>
        DateTimeTestUtils.outstandingZoneIds.foreach { zoneId =>
          val timestamp = TimestampFormatter(pattern, zoneId).format(micros)
          val parsed = TimestampFormatter(
            pattern, zoneId, needVarLengthSecondFraction = true).parse(timestamp)
          assert(micros === parsed)
        }
      }
    }
  }

  test("roundtrip timestamp -> micros -> timestamp using timezones") {
    Seq(
      "0109-07-20T18:38:03.788000",
      "1370-04-01T10:00:54.654321",
      "1670-02-11T14:09:54.746987",
      "1969-12-31T23:55:55.999999",
      "1970-01-01T00:00:00.000000",
      "1973-02-27T02:30:00.102030",
      "2018-12-02T11:22:33.123456",
      "2039-01-01T01:02:03.456789",
      "2345-10-07T22:45:03.010203").foreach { timestamp =>
      DateTimeTestUtils.outstandingZoneIds.foreach { zoneId =>
        val pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
        val micros = TimestampFormatter(
          pattern, zoneId, needVarLengthSecondFraction = true).parse(timestamp)
        val formatted = TimestampFormatter(pattern, zoneId).format(micros)
        assert(timestamp === formatted)
      }
    }
  }

  test("case insensitive parsing of am and pm") {
    val formatter = TimestampFormatter("yyyy MMM dd hh:mm:ss a", ZoneOffset.UTC)
    val micros = formatter.parse("2009 Mar 20 11:30:01 am")
    assert(micros === TimeUnit.SECONDS.toMicros(
      LocalDateTime.of(2009, 3, 20, 11, 30, 1).toEpochSecond(ZoneOffset.UTC)))
  }

  test("format fraction of second") {
    val formatter = TimestampFormatter.getFractionFormatter(ZoneOffset.UTC)
    assert(formatter.format(0) === "1970-01-01 00:00:00")
    assert(formatter.format(1) === "1970-01-01 00:00:00.000001")
    assert(formatter.format(1000) === "1970-01-01 00:00:00.001")
    assert(formatter.format(900000) === "1970-01-01 00:00:00.9")
    assert(formatter.format(1000000) === "1970-01-01 00:00:01")
  }

  test("formatting negative years with default pattern") {
    val instant = LocalDateTime.of(-99, 1, 1, 0, 0, 0)
      .atZone(ZoneOffset.UTC)
      .toInstant
    val micros = DateTimeUtils.instantToMicros(instant)
    assert(TimestampFormatter(ZoneOffset.UTC).format(micros) === "-0099-01-01 00:00:00")
  }

  test("special timestamp values") {
    testSpecialDatetimeValues { zoneId =>
      val formatter = TimestampFormatter(zoneId)
      val tolerance = TimeUnit.SECONDS.toMicros(30)

      assert(formatter.parse("EPOCH") === 0)
      val now = instantToMicros(Instant.now())
      formatter.parse("now") should be(now +- tolerance)
      val localToday = LocalDateTime.now(zoneId)
        .`with`(LocalTime.MIDNIGHT)
        .atZone(zoneId)
      val yesterday = instantToMicros(localToday.minusDays(1).toInstant)
      formatter.parse("yesterday CET") should be(yesterday +- tolerance)
      val today = instantToMicros(localToday.toInstant)
      formatter.parse(" TODAY ") should be(today +- tolerance)
      val tomorrow = instantToMicros(localToday.plusDays(1).toInstant)
      formatter.parse("Tomorrow ") should be(tomorrow +- tolerance)
    }
  }

  test("parsing timestamp strings with various seconds fractions") {
    DateTimeTestUtils.outstandingZoneIds.foreach { zoneId =>
      def check(pattern: String, input: String, reference: String): Unit = {
        val formatter = TimestampFormatter(pattern, zoneId, needVarLengthSecondFraction = true)
        val expected = DateTimeUtils.stringToTimestamp(
          UTF8String.fromString(reference), zoneId).get
        val actual = formatter.parse(input)
        assert(actual === expected)
      }

      check("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSXXX",
        "2019-10-14T09:39:07.3220000Z", "2019-10-14T09:39:07.322Z")
      check("yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
        "2019-10-14T09:39:07.322000", "2019-10-14T09:39:07.322")
      check("yyyy-MM-dd'T'HH:mm:ss.SSSSSSX",
        "2019-10-14T09:39:07.123456Z", "2019-10-14T09:39:07.123456Z")
      check("yyyy-MM-dd'T'HH:mm:ss.SSSSSSX",
        "2019-10-14T09:39:07.000010Z", "2019-10-14T09:39:07.00001Z")
      check("yyyy HH:mm:ss.SSSSS", "1970 01:02:03.00004", "1970-01-01 01:02:03.00004")
      check("yyyy HH:mm:ss.SSSS", "2019 00:00:07.0100", "2019-01-01 00:00:07.0100")
      check("yyyy-MM-dd'T'HH:mm:ss.SSSX",
        "2019-10-14T09:39:07.322Z", "2019-10-14T09:39:07.322Z")
      check("yyyy-MM-dd'T'HH:mm:ss.SS",
        "2019-10-14T09:39:07.10", "2019-10-14T09:39:07.1")
      check("yyyy-MM-dd'T'HH:mm:ss.S",
        "2019-10-14T09:39:07.1", "2019-10-14T09:39:07.1")

      try {
        TimestampFormatter("yyyy/MM/dd HH_mm_ss.SSSSSS", zoneId, true)
          .parse("2019/11/14 20#25#30.123456")
        fail("Expected to throw an exception for the invalid input")
      } catch {
        case e: java.time.format.DateTimeParseException =>
          assert(e.getMessage.contains("could not be parsed"))
      }
    }
  }

  test("formatting timestamp strings up to microsecond precision") {
    DateTimeTestUtils.outstandingZoneIds.foreach { zoneId =>
      def check(pattern: String, input: String, expected: String): Unit = {
        val formatter = TimestampFormatter(pattern, zoneId)
        val timestamp = DateTimeUtils.stringToTimestamp(
          UTF8String.fromString(input), zoneId).get
        val actual = formatter.format(timestamp)
        assert(actual === expected)
      }

      check(
        "yyyy-MM-dd HH:mm:ss.SSSSSSS", "2019-10-14T09:39:07.123456",
        "2019-10-14 09:39:07.1234560")
      check(
        "yyyy-MM-dd HH:mm:ss.SSSSSS", "1960-01-01T09:39:07.123456",
        "1960-01-01 09:39:07.123456")
      check(
        "yyyy-MM-dd HH:mm:ss.SSSSS", "0001-10-14T09:39:07.1",
        "0001-10-14 09:39:07.10000")
      check(
        "yyyy-MM-dd HH:mm:ss.SSSS", "9999-12-31T23:59:59.999",
        "9999-12-31 23:59:59.9990")
      check(
        "yyyy-MM-dd HH:mm:ss.SSS", "1970-01-01T00:00:00.0101",
        "1970-01-01 00:00:00.010")
      check(
        "yyyy-MM-dd HH:mm:ss.SS", "2019-10-14T09:39:07.09",
        "2019-10-14 09:39:07.09")
      check(
        "yyyy-MM-dd HH:mm:ss.S", "2019-10-14T09:39:07.2",
        "2019-10-14 09:39:07.2")
      check(
        "yyyy-MM-dd HH:mm:ss.S", "2019-10-14T09:39:07",
        "2019-10-14 09:39:07.0")
      check(
        "yyyy-MM-dd HH:mm:ss", "2019-10-14T09:39:07.123456",
        "2019-10-14 09:39:07")
    }
  }

  test("SPARK-30958: parse timestamp with negative year") {
    val formatter1 = TimestampFormatter("yyyy-MM-dd HH:mm:ss", ZoneOffset.UTC, true)
    assert(formatter1.parse("-1234-02-22 02:22:22") === instantToMicros(
      LocalDateTime.of(-1234, 2, 22, 2, 22, 22).toInstant(ZoneOffset.UTC)))

    def assertParsingError(f: => Unit): Unit = {
      intercept[Exception](f) match {
        case e: SparkUpgradeException =>
          assert(e.getCause.isInstanceOf[DateTimeException])
        case e =>
          assert(e.isInstanceOf[DateTimeException])
      }
    }

    // "yyyy" with "G" can't parse negative year or year 0000.
    val formatter2 = TimestampFormatter("G yyyy-MM-dd HH:mm:ss", UTC, true)
    assertParsingError(formatter2.parse("BC -1234-02-22 02:22:22"))
    assertParsingError(formatter2.parse("AC 0000-02-22 02:22:22"))

    assert(formatter2.parse("BC 1234-02-22 02:22:22") === instantToMicros(
      LocalDateTime.of(-1233, 2, 22, 2, 22, 22).toInstant(ZoneOffset.UTC)))
    assert(formatter2.parse("AD 1234-02-22 02:22:22") === instantToMicros(
      LocalDateTime.of(1234, 2, 22, 2, 22, 22).toInstant(ZoneOffset.UTC)))
  }

  test("SPARK-31557: rebasing in legacy formatters/parsers") {
    withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> LegacyBehaviorPolicy.LEGACY.toString) {
      LegacyDateFormats.values.foreach { legacyFormat =>
        DateTimeTestUtils.outstandingZoneIds.foreach { zoneId =>
          withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> zoneId.getId) {
            DateTimeTestUtils.withDefaultTimeZone(zoneId) {
              withClue(s"${zoneId.getId} legacyFormat = $legacyFormat") {
                val formatter = TimestampFormatter(
                  TimestampFormatter.defaultPattern,
                  zoneId,
                  TimestampFormatter.defaultLocale,
                  legacyFormat,
                  needVarLengthSecondFraction = false)
                assert(microsToInstant(formatter.parse("1000-01-01 01:02:03"))
                  .atZone(zoneId)
                  .toLocalDateTime === LocalDateTime.of(1000, 1, 1, 1, 2, 3))

                assert(formatter.format(instantToMicros(
                  LocalDateTime.of(1000, 1, 1, 1, 2, 3)
                    .atZone(zoneId).toInstant)) === "1000-01-01 01:02:03")
              }
            }
          }
        }
      }
    }
  }
}
