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

import java.util.{Locale, TimeZone}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeFormatter, DateTimeTestUtils}

class DateTimeFormatterSuite  extends SparkFunSuite {
  test("parsing dates using time zones") {
    val localDate = "2018-12-02"
    val expectedDays = Map(
      "UTC" -> 17867,
      "PST" -> 17867,
      "CET" -> 17866,
      "Africa/Dakar" -> 17867,
      "America/Los_Angeles" -> 17867,
      "Antarctica/Vostok" -> 17866,
      "Asia/Hong_Kong" -> 17866,
      "Europe/Amsterdam" -> 17866)
    DateTimeTestUtils.outstandingTimezonesIds.foreach { timeZone =>
      val formatter = DateFormatter("yyyy-MM-dd", TimeZone.getTimeZone(timeZone), Locale.US)
      val daysSinceEpoch = formatter.parse(localDate)
      assert(daysSinceEpoch === expectedDays(timeZone))
    }
  }

  test("parsing timestamps using time zones") {
    val localDate = "2018-12-02T10:11:12.001234"
    val expectedMicros = Map(
      "UTC" -> 1543745472001234L,
      "PST" -> 1543774272001234L,
      "CET" -> 1543741872001234L,
      "Africa/Dakar" -> 1543745472001234L,
      "America/Los_Angeles" -> 1543774272001234L,
      "Antarctica/Vostok" -> 1543723872001234L,
      "Asia/Hong_Kong" -> 1543716672001234L,
      "Europe/Amsterdam" -> 1543741872001234L)
    DateTimeTestUtils.outstandingTimezonesIds.foreach { timeZone =>
      val formatter = DateTimeFormatter(
        "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
        TimeZone.getTimeZone(timeZone),
        Locale.US)
      val microsSinceEpoch = formatter.parse(localDate)
      assert(microsSinceEpoch === expectedMicros(timeZone))
    }
  }

  test("format dates using time zones") {
    val daysSinceEpoch = 17867
    val expectedDate = Map(
      "UTC" -> "2018-12-02",
      "PST" -> "2018-12-01",
      "CET" -> "2018-12-02",
      "Africa/Dakar" -> "2018-12-02",
      "America/Los_Angeles" -> "2018-12-01",
      "Antarctica/Vostok" -> "2018-12-02",
      "Asia/Hong_Kong" -> "2018-12-02",
      "Europe/Amsterdam" -> "2018-12-02")
    DateTimeTestUtils.outstandingTimezonesIds.foreach { timeZone =>
      val formatter = DateFormatter("yyyy-MM-dd", TimeZone.getTimeZone(timeZone), Locale.US)
      val date = formatter.format(daysSinceEpoch)
      assert(date === expectedDate(timeZone))
    }
  }

  test("format timestamps using time zones") {
    val microsSinceEpoch = 1543745472001234L
    val expectedTimestamp = Map(
      "UTC" -> "2018-12-02T10:11:12.001234",
      "PST" -> "2018-12-02T02:11:12.001234",
      "CET" -> "2018-12-02T11:11:12.001234",
      "Africa/Dakar" -> "2018-12-02T10:11:12.001234",
      "America/Los_Angeles" -> "2018-12-02T02:11:12.001234",
      "Antarctica/Vostok" -> "2018-12-02T16:11:12.001234",
      "Asia/Hong_Kong" -> "2018-12-02T18:11:12.001234",
      "Europe/Amsterdam" -> "2018-12-02T11:11:12.001234")
    DateTimeTestUtils.outstandingTimezonesIds.foreach { timeZone =>
      val formatter = DateTimeFormatter(
        "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
        TimeZone.getTimeZone(timeZone),
        Locale.US)
      val timestamp = formatter.format(microsSinceEpoch)
      assert(timestamp === expectedTimestamp(timeZone))
    }
  }

  test("roundtrip parsing timestamps using timezones") {
    DateTimeTestUtils.outstandingTimezones.foreach { timeZone =>
      val timestamp = "2018-12-02T11:22:33.123456"
      val formatter = DateTimeFormatter("yyyy-MM-dd'T'HH:mm:ss.SSSSSS", timeZone, Locale.US)
      val micros = formatter.parse(timestamp)
      val formatted = formatter.format(micros)
      assert(timestamp === formatted)
    }
  }
}
