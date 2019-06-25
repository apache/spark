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

import java.time.{LocalDateTime, ZoneId, ZoneOffset}
import java.util.concurrent.TimeUnit

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.{DateTimeTestUtils, DateTimeUtils, TimestampFormatter}

class TimestampFormatterSuite extends SparkFunSuite with SQLHelper {

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
    DateTimeTestUtils.outstandingTimezonesIds.foreach { zoneId =>
      val formatter = TimestampFormatter(
        "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
        DateTimeUtils.getZoneId(zoneId))
      val microsSinceEpoch = formatter.parse(localDate)
      assert(microsSinceEpoch === expectedMicros(zoneId))
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
          val formatter = TimestampFormatter(pattern, zoneId)
          val timestamp = formatter.format(micros)
          val parsed = formatter.parse(timestamp)
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
        val formatter = TimestampFormatter("yyyy-MM-dd'T'HH:mm:ss.SSSSSS", zoneId)
        val micros = formatter.parse(timestamp)
        val formatted = formatter.format(micros)
        assert(timestamp === formatted)
      }
    }
  }

  test(" case insensitive parsing of am and pm") {
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
}
