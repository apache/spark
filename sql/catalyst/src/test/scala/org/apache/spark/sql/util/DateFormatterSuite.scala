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

import java.time.{DateTimeException, LocalDate, ZoneOffset}

import org.apache.spark.{SparkFunSuite, SparkUpgradeException}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{getZoneId, localDateToDays}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy

class DateFormatterSuite extends SparkFunSuite with SQLHelper {
  test("parsing dates") {
    DateTimeTestUtils.outstandingTimezonesIds.foreach { timeZone =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> timeZone) {
        val formatter = DateFormatter(getZoneId(timeZone))
        val daysSinceEpoch = formatter.parse("2018-12-02")
        assert(daysSinceEpoch === 17867)
      }
    }
  }

  test("format dates") {
    DateTimeTestUtils.outstandingTimezonesIds.foreach { timeZone =>
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> timeZone) {
        val formatter = DateFormatter(getZoneId(timeZone))
        val date = formatter.format(17867)
        assert(date === "2018-12-02")
      }
    }
  }

  test("roundtrip date -> days -> date") {
    LegacyBehaviorPolicy.values.foreach { parserPolicy =>
      withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> parserPolicy.toString) {
        LegacyDateFormats.values.foreach { legacyFormat =>
          Seq(
            "0050-01-01",
            "0953-02-02",
            "1423-03-08",
            "1582-10-15",
            "1969-12-31",
            "1972-08-25",
            "1975-09-26",
            "2018-12-12",
            "2038-01-01",
            "5010-11-17").foreach { date =>
            DateTimeTestUtils.outstandingTimezonesIds.foreach { timeZone =>
              withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> timeZone) {
                val formatter = DateFormatter(
                  DateFormatter.defaultPattern,
                  getZoneId(timeZone),
                  DateFormatter.defaultLocale,
                  legacyFormat)
                val days = formatter.parse(date)
                val formatted = formatter.format(days)
                assert(date === formatted)
              }
            }
          }
        }
      }
    }
  }

  test("roundtrip days -> date -> days") {
    LegacyBehaviorPolicy.values.foreach { parserPolicy =>
      withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> parserPolicy.toString) {
        LegacyDateFormats.values.foreach { legacyFormat =>
          Seq(
            -701265,
            -371419,
            -199722,
            -1,
            0,
            967,
            2094,
            17877,
            24837,
            1110657).foreach { days =>
            DateTimeTestUtils.outstandingTimezonesIds.foreach { timeZone =>
              withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> timeZone) {
                val formatter = DateFormatter(
                  DateFormatter.defaultPattern,
                  getZoneId(timeZone),
                  DateFormatter.defaultLocale,
                  legacyFormat)
                val date = formatter.format(days)
                val parsed = formatter.parse(date)
                assert(days === parsed)
              }
            }
          }
        }
      }
    }
  }

  test("parsing date without explicit day") {
    val formatter = DateFormatter("yyyy MMM", ZoneOffset.UTC)
    val daysSinceEpoch = formatter.parse("2018 Dec")
    assert(daysSinceEpoch === LocalDate.of(2018, 12, 1).toEpochDay)
  }

  test("formatting negative years with default pattern") {
    val epochDays = LocalDate.of(-99, 1, 1).toEpochDay.toInt
    assert(DateFormatter(ZoneOffset.UTC).format(epochDays) === "-0099-01-01")
  }

  test("special date values") {
    testSpecialDatetimeValues { zoneId =>
      val formatter = DateFormatter(zoneId)

      assert(formatter.parse("EPOCH") === 0)
      val today = localDateToDays(LocalDate.now(zoneId))
      assert(formatter.parse("Yesterday") === today - 1)
      assert(formatter.parse("now") === today)
      assert(formatter.parse("today ") === today)
      assert(formatter.parse("tomorrow UTC") === today + 1)
    }
  }

  test("SPARK-30958: parse date with negative year") {
    val formatter1 = DateFormatter("yyyy-MM-dd", ZoneOffset.UTC)
    assert(formatter1.parse("-1234-02-22") === localDateToDays(LocalDate.of(-1234, 2, 22)))

    def assertParsingError(f: => Unit): Unit = {
      intercept[Exception](f) match {
        case e: SparkUpgradeException =>
          assert(e.getCause.isInstanceOf[DateTimeException])
        case e =>
          assert(e.isInstanceOf[DateTimeException])
      }
    }

    // "yyyy" with "G" can't parse negative year or year 0000.
    val formatter2 = DateFormatter("G yyyy-MM-dd", ZoneOffset.UTC)
    assertParsingError(formatter2.parse("BC -1234-02-22"))
    assertParsingError(formatter2.parse("AD 0000-02-22"))

    assert(formatter2.parse("BC 1234-02-22") === localDateToDays(LocalDate.of(-1233, 2, 22)))
    assert(formatter2.parse("AD 1234-02-22") === localDateToDays(LocalDate.of(1234, 2, 22)))
  }

  test("SPARK-31557: rebasing in legacy formatters/parsers") {
    withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> LegacyBehaviorPolicy.LEGACY.toString) {
      LegacyDateFormats.values.foreach { legacyFormat =>
        DateTimeTestUtils.outstandingTimezonesIds.foreach { timeZone =>
          withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> timeZone) {
            val formatter = DateFormatter(
              DateFormatter.defaultPattern,
              getZoneId(timeZone),
              DateFormatter.defaultLocale,
              legacyFormat)
            assert(LocalDate.ofEpochDay(formatter.parse("1000-01-01")) === LocalDate.of(1000, 1, 1))
            assert(formatter.format(localDateToDays(LocalDate.of(1000, 1, 1))) === "1000-01-01")
          }
        }
      }
    }
  }
}
