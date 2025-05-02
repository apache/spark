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

import scala.util.Random

import org.apache.spark.{SPARK_DOC_ROOT, SparkDateTimeException, SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._
import org.apache.spark.sql.catalyst.util.DateTimeUtils.microsToLocalTime

class TimeFormatterSuite extends SparkFunSuite with SQLHelper {

  test("time parsing") {
    Seq(
      ("12", "HH") -> 12 * 3600 * 1000000L,
      ("01:02", "HH:mm") -> (1 * 3600 + 2 * 60) * 1000000L,
      ("10:20", "HH:mm") -> (10 * 3600 + 20 * 60) * 1000000L,
      ("00:00:00", "HH:mm:ss") -> 0L,
      ("01:02:03", "HH:mm:ss") -> (1 * 3600 + 2 * 60 + 3) * 1000000L,
      ("23:59:59", "HH:mm:ss") -> (23 * 3600 + 59 * 60 + 59) * 1000000L,
      ("00:00:00.000000", "HH:mm:ss.SSSSSS") -> 0L,
      ("12:34:56.789012", "HH:mm:ss.SSSSSS") -> ((12 * 3600 + 34 * 60 + 56) * 1000000L + 789012),
      ("23:59:59.000000", "HH:mm:ss.SSSSSS") -> (23 * 3600 + 59 * 60 + 59) * 1000000L,
      ("23:59:59.999999", "HH:mm:ss.SSSSSS") -> ((23 * 3600 + 59 * 60 + 59) * 1000000L + 999999)
    ).foreach { case ((inputStr, pattern), expectedMicros) =>
      val formatter = TimeFormatter(format = pattern, isParsing = true)
      assert(formatter.parse(inputStr) === expectedMicros)
    }
  }

  test("time strings do not match to the pattern") {
    def assertError(str: String, expectedMsg: String): Unit = {
      val e = intercept[DateTimeException] {
        TimeFormatter(format = "HH:mm:ss", isParsing = true).parse(str)
      }
      assert(e.getMessage.contains(expectedMsg))
    }

    assertError("11.12.13", "Text '11.12.13' could not be parsed")
    assertError("25:00:00", "Text '25:00:00' could not be parsed: Invalid value")
  }

  test("time formatting") {
    Seq(
      (12 * 3600 * 1000000L, "HH") -> "12",
      ((1 * 3600 + 2 * 60) * 1000000L, "HH:mm") -> "01:02",
      ((10 * 3600 + 20 * 60) * 1000000L, "HH:mm") -> "10:20",
      (0L, "HH:mm:ss") -> "00:00:00",
      ((1 * 3600 + 2 * 60 + 3) * 1000000L, "HH:mm:ss") -> "01:02:03",
      ((23 * 3600 + 59 * 60 + 59) * 1000000L, "HH:mm:ss") -> "23:59:59",
      (0L, "HH:mm:ss.SSSSSS") -> "00:00:00.000000",
      ((12 * 3600 + 34 * 60 + 56) * 1000000L + 789012, "HH:mm:ss.SSSSSS") -> "12:34:56.789012",
      ((23 * 3600 + 59 * 60 + 59) * 1000000L, "HH:mm:ss.SSSSSS") -> "23:59:59.000000",
      ((23 * 3600 + 59 * 60 + 59) * 1000000L + 999999, "HH:mm:ss.SSSSSS") -> "23:59:59.999999"
    ).foreach { case ((micros, pattern), expectedStr) =>
      val formatter = TimeFormatter(format = pattern)
      assert(formatter.format(micros) === expectedStr)
    }
  }

  test("micros are out of supported range") {
    def assertError(micros: Long, expectedMsg: String): Unit = {
      val e = intercept[DateTimeException](TimeFormatter(isParsing = false).format(micros))
      assert(e.getMessage.contains(expectedMsg))
    }

    assertError(-1, "Invalid value for NanoOfDay (valid values 0 - 86399999999999): -1000")
    assertError(25L * 3600 * 1000 * 1000,
      "Invalid value for NanoOfDay (valid values 0 - 86399999999999): 90000000000000")
  }

  test("invalid pattern") {
    Seq("hHH:mmm:s", "kkk", "GGGGGG").foreach { invalidPattern =>
      checkError(
        exception = intercept[SparkRuntimeException] {
          TimeFormatter(invalidPattern)
        },
        condition = "INVALID_DATETIME_PATTERN.WITH_SUGGESTION",
        parameters = Map(
          "pattern" -> s"'$invalidPattern'",
          "docroot" -> SPARK_DOC_ROOT))
    }
  }

  test("round trip with the default pattern: format -> parse") {
    val data = Seq.tabulate(10) { _ => Random.between(0, 24 * 60 * 60 * 1000000L) }
    val pattern = "HH:mm:ss.SSSSSS"
    val (formatter, parser) =
      (TimeFormatter(pattern, isParsing = false), TimeFormatter(pattern, isParsing = true))
    data.foreach { micros =>
      val str = formatter.format(micros)
      assert(parser.parse(str) === micros, s"micros = $micros")
      assert(formatter.format(microsToLocalTime(micros)) === str)
    }
  }

  test("format fraction of second") {
    val formatter = new FractionTimeFormatter()
    Seq(
      0 -> "00:00:00",
      1 -> "00:00:00.000001",
      1000 -> "00:00:00.001",
      900000 -> "00:00:00.9",
      1000000 -> "00:00:01").foreach { case (micros, tsStr) =>
      assert(formatter.format(micros) === tsStr)
      assert(formatter.format(microsToLocalTime(micros)) === tsStr)
    }
  }

  test("missing am/pm field") {
    Seq("HH", "hh", "KK", "kk").foreach { hour =>
      val formatter = TimeFormatter(format = s"$hour:mm:ss", isParsing = true)
      val micros = formatter.parse("11:30:01")
      assert(micros === localTime(11, 30, 1))
    }
  }

  test("missing hour field") {
    val f1 = TimeFormatter(format = "mm:ss a", isParsing = true)
    val t1 = f1.parse("30:01 PM")
    assert(t1 === localTime(12, 30, 1))
    val t2 = f1.parse("30:01 AM")
    assert(t2 === localTime(0, 30, 1))
    val f2 = TimeFormatter(format = "mm:ss", isParsing = true)
    val t3 = f2.parse("30:01")
    assert(t3 === localTime(0, 30, 1))
    val f3 = TimeFormatter(format = "a", isParsing = true)
    val t4 = f3.parse("PM")
    assert(t4 === localTime(12))
    val t5 = f3.parse("AM")
    assert(t5 === localTime())
  }

  test("default parsing w/o pattern") {
    val formatter = new DefaultTimeFormatter(
      locale = DateFormatter.defaultLocale,
      isParsing = true)
    Seq(
      "00:00:00" -> localTime(),
      "00:00:00.000001" -> localTime(micros = 1),
      "01:02:03" -> localTime(1, 2, 3),
      "1:2:3.999999" -> localTime(1, 2, 3, 999999),
      "23:59:59.1" -> localTime(23, 59, 59, 100000)
    ).foreach { case (inputStr, micros) =>
      assert(formatter.parse(inputStr) === micros)
    }

    checkError(
      exception = intercept[SparkDateTimeException] {
        formatter.parse("x123")
      },
      condition = "CAST_INVALID_INPUT",
      parameters = Map(
        "expression" -> "'x123'",
        "sourceType" -> "\"STRING\"",
        "targetType" -> "\"TIME(6)\"",
        "ansiConfig" -> "\"spark.sql.ansi.enabled\""))
  }
}
