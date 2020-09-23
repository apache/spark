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

import java.time.DateTimeException

import org.scalatest.matchers.must.Matchers

import org.apache.spark.{SparkFunSuite, SparkUpgradeException}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.{date, UTC}

trait DatetimeFormatterSuite extends SparkFunSuite with SQLHelper with Matchers {
  import DateTimeFormatterHelper._
  import LegacyDateFormats._
  def checkFormatterCreation(pattern: String, isParsing: Boolean): Unit

  private def dateFormatter(
      pattern: String, ldf: LegacyDateFormat = FAST_DATE_FORMAT): DateFormatter = {
    DateFormatter(pattern, UTC, DateFormatter.defaultLocale, ldf, isParsing = true)
  }

  private def timestampFormatter(
      pattern: String, ldf: LegacyDateFormat = SIMPLE_DATE_FORMAT): TimestampFormatter = {
    TimestampFormatter(pattern, UTC, legacyFormat = ldf, isParsing = true)
  }

  protected def useDateFormatter: Boolean

  private def assertEqual(pattern: String, datetimeStr: String, expected: Long): Unit = {
    if (useDateFormatter) {
      assert(dateFormatter(pattern).parse(datetimeStr) ===
        DateTimeUtils.microsToDays(expected, UTC))
    } else {
      assert(timestampFormatter(pattern).parse(datetimeStr) === expected)
    }
  }

  private def assertError(pattern: String, datetimeStr: String, expectedMsg: String): Unit = {
    if (useDateFormatter) {
      LegacyDateFormats.values.foreach { ldf =>
        // The legacy DateFormatter is always lenient by default
        val e = intercept[SparkUpgradeException](dateFormatter(pattern, ldf).parse(datetimeStr))
        assert(e.getCause.getMessage.contains(expectedMsg))
      }
    } else {
      // In strict mode, the legacy TimestampFormatter fails too
      val e = intercept[DateTimeException](timestampFormatter(pattern).parse(datetimeStr))
      assert(e.getMessage.contains(expectedMsg))
      // In lenient mode, the legacy TimestampFormatter does not fail
      Seq(FAST_DATE_FORMAT, LENIENT_SIMPLE_DATE_FORMAT).foreach { ldf =>
        val e = intercept[SparkUpgradeException] {
          timestampFormatter(pattern, ldf).parse(datetimeStr)
        }
        assert(e.getCause.getMessage.contains(expectedMsg))
      }
    }
  }

  test("explicitly forbidden datetime patterns") {

    Seq(true, false).foreach { isParsing =>
      // not support by the legacy one too
      val unsupportedBoth = Seq("QQQQQ", "qqqqq", "eeeee", "A", "c", "n", "N", "p", "e")
      unsupportedBoth.foreach { pattern =>
        intercept[IllegalArgumentException](checkFormatterCreation(pattern, isParsing))
      }
      // supported by the legacy one, then we will suggest users with SparkUpgradeException
      ((weekBasedLetters ++ unsupportedLetters).map(_.toString)
        ++ unsupportedPatternLengths -- unsupportedBoth).foreach {
        pattern => intercept[SparkUpgradeException](checkFormatterCreation(pattern, isParsing))
      }
    }

    // not support by the legacy one too
    val unsupportedBoth = Seq("q", "Q")
    unsupportedBoth.foreach { pattern =>
      intercept[IllegalArgumentException](checkFormatterCreation(pattern, true))
    }
    // supported by the legacy one, then we will suggest users with SparkUpgradeException
    (unsupportedLettersForParsing.map(_.toString) -- unsupportedBoth).foreach {
      pattern => intercept[SparkUpgradeException](checkFormatterCreation(pattern, true))
    }
  }

  test("SPARK-31939: Fix Parsing day of year when year field pattern is missing") {
    // resolved to queryable LocaleDate or fail directly
    assertEqual("yyyy-dd-DD", "2020-29-60", date(2020, 2, 29))
    assertError("yyyy-dd-DD", "2020-02-60",
      "Field DayOfMonth 29 differs from DayOfMonth 2 derived from 2020-02-29")
    assertEqual("yyyy-MM-DD", "2020-02-60", date(2020, 2, 29))
    assertError("yyyy-MM-DD", "2020-03-60",
      "Field MonthOfYear 2 differs from MonthOfYear 3 derived from 2020-02-29")
    assertEqual("yyyy-MM-dd-DD", "2020-02-29-60", date(2020, 2, 29))
    assertError("yyyy-MM-dd-DD", "2020-03-01-60",
      "Field DayOfYear 61 differs from DayOfYear 60 derived from 2020-03-01")
    assertEqual("yyyy-DDD", "2020-366", date(2020, 12, 31))
    assertError("yyyy-DDD", "2019-366",
      "Invalid date 'DayOfYear 366' as '2019' is not a leap year")

    // unresolved and need to check manually(SPARK-31939 fixed)
    assertEqual("DDD", "365", date(1970, 12, 31))
    assertError("DDD", "366",
      "Invalid date 'DayOfYear 366' as '1970' is not a leap year")
    assertEqual("MM-DD", "03-60", date(1970, 3))
    assertError("MM-DD", "02-60",
      "Field MonthOfYear 2 differs from MonthOfYear 3 derived from 1970-03-01")
    assertEqual("MM-dd-DD", "02-28-59", date(1970, 2, 28))
    assertError("MM-dd-DD", "02-28-60",
      "Field MonthOfYear 2 differs from MonthOfYear 3 derived from 1970-03-01")
    assertError("MM-dd-DD", "02-28-58",
      "Field DayOfMonth 28 differs from DayOfMonth 27 derived from 1970-02-27")
    assertEqual("dd-DD", "28-59", date(1970, 2, 28))
    assertError("dd-DD", "27-59",
      "Field DayOfMonth 27 differs from DayOfMonth 28 derived from 1970-02-28")
  }
}
