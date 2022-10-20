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

import java.time.{DateTimeException, LocalDateTime}

import org.apache.commons.lang3.{JavaVersion, SystemUtils}

import org.apache.spark.SparkUpgradeException
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.unsafe.types.UTF8String

class TimestampFormatterSuite extends DatetimeFormatterSuite {

  override def checkFormatterCreation(pattern: String, isParsing: Boolean): Unit = {
    TimestampFormatter(pattern, UTC, isParsing)
  }

  override protected def useDateFormatter: Boolean = false

  test("parsing timestamps using time zones") {
    val localDate = "2018-12-02T10:11:12.001234"
    val expectedMicros = Map(
      "UTC" -> 1543745472001234L,
      PST.getId -> 1543774272001234L,
      CET.getId -> 1543741872001234L,
      "Africa/Dakar" -> 1543745472001234L,
      "America/Los_Angeles" -> 1543774272001234L,
      "Asia/Urumqi" -> 1543723872001234L,
      "Asia/Hong_Kong" -> 1543716672001234L,
      "Europe/Brussels" -> 1543741872001234L)
    outstandingTimezonesIds.foreach { zoneId =>
      val formatter = TimestampFormatter(
        "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
        getZoneId(zoneId),
        isParsing = true)
      val microsSinceEpoch = formatter.parse(localDate)
      assert(microsSinceEpoch === expectedMicros(zoneId))
    }
  }

  test("format timestamps using time zones") {
    val microsSinceEpoch = 1543745472001234L
    val expectedTimestamp = Map(
      "UTC" -> "2018-12-02 10:11:12.001234",
      PST.getId -> "2018-12-02 02:11:12.001234",
      CET.getId -> "2018-12-02 11:11:12.001234",
      "Africa/Dakar" -> "2018-12-02 10:11:12.001234",
      "America/Los_Angeles" -> "2018-12-02 02:11:12.001234",
      "Asia/Urumqi" -> "2018-12-02 16:11:12.001234",
      "Asia/Hong_Kong" -> "2018-12-02 18:11:12.001234",
      "Europe/Brussels" -> "2018-12-02 11:11:12.001234")
    outstandingTimezonesIds.foreach { zoneId =>
      Seq(
        TimestampFormatter(
          "yyyy-MM-dd HH:mm:ss.SSSSSS",
          getZoneId(zoneId),
          // Test only FAST_DATE_FORMAT because other legacy formats don't support formatting
          // in microsecond precision.
          LegacyDateFormats.FAST_DATE_FORMAT,
          isParsing = false),
        TimestampFormatter.getFractionFormatter(getZoneId(zoneId))).foreach { formatter =>
        val timestamp = formatter.format(microsSinceEpoch)
        assert(timestamp === expectedTimestamp(zoneId))
        assert(formatter.format(microsToInstant(microsSinceEpoch)) === expectedTimestamp(zoneId))
        assert(formatter.format(toJavaTimestamp(microsSinceEpoch)) === expectedTimestamp(zoneId))
      }
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
        outstandingZoneIds.foreach { zoneId =>
          val timestamp = TimestampFormatter(pattern, zoneId, isParsing = false).format(micros)
          val parsed = TimestampFormatter(
            pattern, zoneId, isParsing = true).parse(timestamp)
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
      outstandingZoneIds.foreach { zoneId =>
        val pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
        val micros = TimestampFormatter(
          pattern, zoneId, isParsing = true).parse(timestamp)
        val formatted = TimestampFormatter(pattern, zoneId, isParsing = false).format(micros)
        assert(timestamp === formatted)
      }
    }
  }

  test("case insensitive parsing of am and pm") {
    val formatter = TimestampFormatter("yyyy MMM dd hh:mm:ss a", UTC, isParsing = false)
    val micros = formatter.parse("2009 Mar 20 11:30:01 am")
    assert(micros === date(2009, 3, 20, 11, 30, 1))
  }

  test("format fraction of second") {
    val formatter = TimestampFormatter.getFractionFormatter(UTC)
    Seq(
      -999999 -> "1969-12-31 23:59:59.000001",
      -999900 -> "1969-12-31 23:59:59.0001",
      -1 -> "1969-12-31 23:59:59.999999",
      0 -> "1970-01-01 00:00:00",
      1 -> "1970-01-01 00:00:00.000001",
      1000 -> "1970-01-01 00:00:00.001",
      900000 -> "1970-01-01 00:00:00.9",
      1000000 -> "1970-01-01 00:00:01").foreach { case (micros, tsStr) =>
      assert(formatter.format(micros) === tsStr)
      assert(formatter.format(microsToInstant(micros)) === tsStr)
      withDefaultTimeZone(UTC) {
        assert(formatter.format(toJavaTimestamp(micros)) === tsStr)
      }
    }
  }

  test("formatting negative years with default pattern") {
    val instant = LocalDateTime.of(-99, 1, 1, 0, 0, 0).atZone(UTC).toInstant
    val micros = instantToMicros(instant)
    assert(TimestampFormatter(UTC).format(micros) === "-0099-01-01 00:00:00")
    assert(TimestampFormatter(UTC).format(instant) === "-0099-01-01 00:00:00")
    withDefaultTimeZone(UTC) { // toJavaTimestamp depends on the default time zone
      assert(TimestampFormatter("yyyy-MM-dd HH:mm:SS G", UTC, isParsing = false)
        .format(toJavaTimestamp(micros)) === "0100-01-01 00:00:00 BC")
    }
  }

  test("parsing timestamp strings with various seconds fractions") {
    outstandingZoneIds.foreach { zoneId =>
      def check(pattern: String, input: String, reference: String): Unit = {
        val formatter = TimestampFormatter(pattern, zoneId, isParsing = true)
        val expected = stringToTimestamp(UTF8String.fromString(reference), zoneId).get
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
        TimestampFormatter("yyyy/MM/dd HH_mm_ss.SSSSSS", zoneId, isParsing = true)
          .parse("2019/11/14 20#25#30.123456")
        fail("Expected to throw an exception for the invalid input")
      } catch {
        case e: java.time.format.DateTimeParseException =>
          assert(e.getMessage.contains("could not be parsed"))
      }
    }
  }

  test("formatting timestamp strings up to microsecond precision") {
    outstandingZoneIds.foreach { zoneId =>
      def check(pattern: String, input: String, expected: String): Unit = {
        val formatter = TimestampFormatter(pattern, zoneId, isParsing = false)
        val timestamp = stringToTimestamp(UTF8String.fromString(input), zoneId).get
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
    val formatter1 = TimestampFormatter("yyyy-MM-dd HH:mm:ss", UTC, isParsing = true)
    assert(formatter1.parse("-1234-02-22 02:22:22") === date(-1234, 2, 22, 2, 22, 22))

    def assertParsingError(f: => Unit): Unit = {
      intercept[Exception](f) match {
        case e: SparkUpgradeException =>
          assert(e.getCause.isInstanceOf[DateTimeException])
        case e =>
          assert(e.isInstanceOf[DateTimeException])
      }
    }

    // "yyyy" with "G" can't parse negative year or year 0000.
    val formatter2 = TimestampFormatter("G yyyy-MM-dd HH:mm:ss", UTC, isParsing = true)
    assertParsingError(formatter2.parse("BC -1234-02-22 02:22:22"))
    assertParsingError(formatter2.parse("AC 0000-02-22 02:22:22"))

    assert(formatter2.parse("BC 1234-02-22 02:22:22") === date(-1233, 2, 22, 2, 22, 22))
    assert(formatter2.parse("AD 1234-02-22 02:22:22") === date(1234, 2, 22, 2, 22, 22))
  }

  test("SPARK-31557: rebasing in legacy formatters/parsers") {
    withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> LegacyBehaviorPolicy.LEGACY.toString) {
      outstandingZoneIds.foreach { zoneId =>
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> zoneId.getId) {
          withDefaultTimeZone(zoneId) {
            withClue(s"zoneId = ${zoneId.getId}") {
              val formatters = LegacyDateFormats.values.toSeq.map { legacyFormat =>
                TimestampFormatter(
                  TimestampFormatter.defaultPattern,
                  zoneId,
                  TimestampFormatter.defaultLocale,
                  legacyFormat,
                  isParsing = false)
              } :+ TimestampFormatter.getFractionFormatter(zoneId)
              formatters.foreach { formatter =>
                assert(microsToInstant(formatter.parse("1000-01-01 01:02:03"))
                  .atZone(zoneId)
                  .toLocalDateTime === LocalDateTime.of(1000, 1, 1, 1, 2, 3))

                assert(formatter.format(
                  LocalDateTime.of(1000, 1, 1, 1, 2, 3).atZone(zoneId).toInstant) ===
                  "1000-01-01 01:02:03")
                assert(formatter.format(instantToMicros(
                  LocalDateTime.of(1000, 1, 1, 1, 2, 3)
                    .atZone(zoneId).toInstant)) === "1000-01-01 01:02:03")
                assert(formatter.format(java.sql.Timestamp.valueOf("1000-01-01 01:02:03")) ===
                  "1000-01-01 01:02:03")
              }
            }
          }
        }
      }
    }
  }

  test("parsing hour with various patterns") {
    def createFormatter(pattern: String): TimestampFormatter = {
      // Use `SIMPLE_DATE_FORMAT`, so that the legacy parser also fails with invalid value range.
      TimestampFormatter(pattern, UTC, LegacyDateFormats.SIMPLE_DATE_FORMAT, isParsing = true)
    }

    withClue("HH") {
      val formatter = createFormatter("yyyy-MM-dd HH")

      val micros1 = formatter.parse("2009-12-12 00")
      assert(micros1 === date(2009, 12, 12))

      val micros2 = formatter.parse("2009-12-12 15")
      assert(micros2 === date(2009, 12, 12, 15))

      intercept[DateTimeException](formatter.parse("2009-12-12 24"))
    }

    withClue("kk") {
      val formatter = createFormatter("yyyy-MM-dd kk")

      intercept[DateTimeException](formatter.parse("2009-12-12 00"))

      val micros1 = formatter.parse("2009-12-12 15")
      assert(micros1 === date(2009, 12, 12, 15))

      val micros2 = formatter.parse("2009-12-12 24")
      assert(micros2 === date(2009, 12, 12))
    }

    withClue("KK") {
      val formatter = createFormatter("yyyy-MM-dd KK a")

      val micros1 = formatter.parse("2009-12-12 00 am")
      assert(micros1 === date(2009, 12, 12))

      // JDK-8223773: DateTimeFormatter Fails to throw an Exception on Invalid HOUR_OF_AMPM
      // For `KK`, "12:00:00 am" is the same as "00:00:00 pm".
      if (SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_13)) {
        intercept[DateTimeException](formatter.parse("2009-12-12 12 am"))
      } else {
        val micros2 = formatter.parse("2009-12-12 12 am")
        assert(micros2 === date(2009, 12, 12, 12))
      }

      val micros3 = formatter.parse("2009-12-12 00 pm")
      assert(micros3 === date(2009, 12, 12, 12))

      intercept[DateTimeException](formatter.parse("2009-12-12 12 pm"))
    }

    withClue("hh") {
      val formatter = createFormatter("yyyy-MM-dd hh a")

      intercept[DateTimeException](formatter.parse("2009-12-12 00 am"))

      val micros1 = formatter.parse("2009-12-12 12 am")
      assert(micros1 === date(2009, 12, 12))

      intercept[DateTimeException](formatter.parse("2009-12-12 00 pm"))

      val micros2 = formatter.parse("2009-12-12 12 pm")
      assert(micros2 === date(2009, 12, 12, 12))
    }
  }

  test("missing date fields") {
    val formatter = TimestampFormatter("HH:mm:ss", UTC, isParsing = true)
    val micros = formatter.parse("11:30:01")
    assert(micros === date(1970, 1, 1, 11, 30, 1))
  }

  test("missing year field with invalid date") {
    // Use `SIMPLE_DATE_FORMAT`, so that the legacy parser also fails with invalid date.
    val formatter =
      TimestampFormatter("MM-dd", UTC, LegacyDateFormats.SIMPLE_DATE_FORMAT, isParsing = true)
    withDefaultTimeZone(UTC)(intercept[DateTimeException](formatter.parse("02-29")))
  }

  test("missing am/pm field") {
    Seq("HH", "hh", "KK", "kk").foreach { hour =>
      val formatter = TimestampFormatter(s"yyyy $hour:mm:ss", UTC, isParsing = true)
      val micros = formatter.parse("2009 11:30:01")
      assert(micros === date(2009, 1, 1, 11, 30, 1))
    }
  }

  test("missing time fields") {
    val formatter = TimestampFormatter("yyyy HH", UTC, isParsing = true)
    val micros = formatter.parse("2009 11")
    assert(micros === date(2009, 1, 1, 11))
  }

  test("missing hour field") {
    val f1 = TimestampFormatter("mm:ss a", UTC, isParsing = true)
    val t1 = f1.parse("30:01 PM")
    assert(t1 === date(1970, 1, 1, 12, 30, 1))
    val t2 = f1.parse("30:01 AM")
    assert(t2 === date(1970, 1, 1, 0, 30, 1))
    val f2 = TimestampFormatter("mm:ss", UTC, isParsing = true)
    val t3 = f2.parse("30:01")
    assert(t3 === date(1970, 1, 1, 0, 30, 1))
    val f3 = TimestampFormatter("a", UTC, isParsing = true)
    val t4 = f3.parse("PM")
    assert(t4 === date(1970, 1, 1, 12))
    val t5 = f3.parse("AM")
    assert(t5 === date(1970))
  }

  test("check result differences for datetime formatting") {
    val formatter = TimestampFormatter("DD", UTC, isParsing = false)
    assert(formatter.format(date(1970, 1, 3)) == "03")
    assert(formatter.format(date(1970, 4, 9)) == "99")

    if (System.getProperty("java.version").split("\\D+")(0).toInt < 9) {
      // https://bugs.openjdk.java.net/browse/JDK-8079628
      intercept[SparkUpgradeException] {
        formatter.format(date(1970, 4, 10))
      }
    } else {
      assert(formatter.format(date(1970, 4, 10)) == "100")
    }
  }

  test("SPARK-32424: avoid silent data change when timestamp overflows") {
    val formatter = TimestampFormatter("y", UTC, isParsing = true)
    assert(formatter.parse("294247") === date(294247))
    assert(formatter.parse("-290307") === date(-290307))
    val e1 = intercept[ArithmeticException](formatter.parse("294248"))
    assert(e1.getMessage === "long overflow")
    val e2 = intercept[ArithmeticException](formatter.parse("-290308"))
    assert(e2.getMessage === "long overflow")
  }

  test("SPARK-36418: default parsing w/o pattern") {
    outstandingZoneIds.foreach { zoneId =>
      val formatter = new DefaultTimestampFormatter(
        zoneId,
        locale = DateFormatter.defaultLocale,
        legacyFormat = LegacyDateFormats.SIMPLE_DATE_FORMAT,
        isParsing = true)
      Seq(
        "-0042-3-4" -> LocalDateTime.of(-42, 3, 4, 0, 0, 0),
        "1000" -> LocalDateTime.of(1000, 1, 1, 0, 0, 0),
        "1582-10-4" -> LocalDateTime.of(1582, 10, 4, 0, 0, 0),
        "1583-1-1 " -> LocalDateTime.of(1583, 1, 1, 0, 0, 0),
        "1970-01-1 01:02:3" -> LocalDateTime.of(1970, 1, 1, 1, 2, 3),
        "2021-8-12T18:31:50" -> LocalDateTime.of(2021, 8, 12, 18, 31, 50)
      ).foreach { case (inputStr, ldt) =>
        assert(formatter.parse(inputStr) === DateTimeTestUtils.localDateTimeToMicros(ldt, zoneId))
      }

      val errMsg = intercept[DateTimeException] {
        formatter.parse("x123")
      }.getMessage
      assert(errMsg.contains(
        """The value 'x123' of the type "STRING" cannot be cast to "TIMESTAMP""""))
    }
  }

  test("SPARK-39193: support returning optional parse results in the default formatter") {
    val formatter = new DefaultTimestampFormatter(
      DateTimeTestUtils.LA,
      locale = DateFormatter.defaultLocale,
      legacyFormat = LegacyDateFormats.SIMPLE_DATE_FORMAT,
      isParsing = true)
    assert(formatter.parseOptional("2021-01-01T00:00:00").contains(1609488000000000L))
    assert(
      formatter.parseWithoutTimeZoneOptional("2021-01-01T00:00:00", false)
        .contains(1609459200000000L))
    assert(formatter.parseOptional("abc").isEmpty)
    assert(
      formatter.parseWithoutTimeZoneOptional("abc", false).isEmpty)
  }
}
