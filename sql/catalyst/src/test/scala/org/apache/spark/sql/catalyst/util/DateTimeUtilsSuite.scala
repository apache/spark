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

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.time.ZoneId
import java.util.{Locale, TimeZone}
import java.util.concurrent.TimeUnit

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.unsafe.types.UTF8String

class DateTimeUtilsSuite extends SparkFunSuite {

  val TimeZonePST = TimeZone.getTimeZone("PST")
  private def defaultTz = DateTimeUtils.defaultTimeZone()
  private def defaultZoneId = ZoneId.systemDefault()

  test("nanoseconds truncation") {
    val tf = TimestampFormatter.getFractionFormatter(DateTimeUtils.defaultTimeZone.toZoneId)
    def checkStringToTimestamp(originalTime: String, expectedParsedTime: String) {
      val parsedTimestampOp = DateTimeUtils.stringToTimestamp(
        UTF8String.fromString(originalTime), defaultZoneId)
      assert(parsedTimestampOp.isDefined, "timestamp with nanoseconds was not parsed correctly")
      assert(DateTimeUtils.timestampToString(tf, parsedTimestampOp.get) === expectedParsedTime)
    }

    checkStringToTimestamp("2015-01-02 00:00:00.123456789", "2015-01-02 00:00:00.123456")
    checkStringToTimestamp("2015-01-02 00:00:00.100000009", "2015-01-02 00:00:00.1")
    checkStringToTimestamp("2015-01-02 00:00:00.000050000", "2015-01-02 00:00:00.00005")
    checkStringToTimestamp("2015-01-02 00:00:00.12005", "2015-01-02 00:00:00.12005")
    checkStringToTimestamp("2015-01-02 00:00:00.100", "2015-01-02 00:00:00.1")
    checkStringToTimestamp("2015-01-02 00:00:00.000456789", "2015-01-02 00:00:00.000456")
    checkStringToTimestamp("1950-01-02 00:00:00.000456789", "1950-01-02 00:00:00.000456")
  }

  test("timestamp and us") {
    val now = new Timestamp(System.currentTimeMillis())
    now.setNanos(1000)
    val ns = fromJavaTimestamp(now)
    assert(ns % 1000000L === 1)
    assert(toJavaTimestamp(ns) === now)

    List(-111111111111L, -1L, 0, 1L, 111111111111L).foreach { t =>
      val ts = toJavaTimestamp(t)
      assert(fromJavaTimestamp(ts) === t)
      assert(toJavaTimestamp(fromJavaTimestamp(ts)) === ts)
    }
  }

  test("us and julian day") {
    val (d, ns) = toJulianDay(0)
    assert(d === JULIAN_DAY_OF_EPOCH)
    assert(ns === 0)
    assert(fromJulianDay(d, ns) == 0L)

    Seq(Timestamp.valueOf("2015-06-11 10:10:10.100"),
      Timestamp.valueOf("2015-06-11 20:10:10.100"),
      Timestamp.valueOf("1900-06-11 20:10:10.100")).foreach { t =>
      val (d, ns) = toJulianDay(fromJavaTimestamp(t))
      assert(ns > 0)
      val t1 = toJavaTimestamp(fromJulianDay(d, ns))
      assert(t.equals(t1))
    }
  }

  test("SPARK-6785: java date conversion before and after epoch") {
    def checkFromToJavaDate(d1: Date): Unit = {
      val d2 = toJavaDate(fromJavaDate(d1))
      assert(d2.toString === d1.toString)
    }

    val df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
    val df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z", Locale.US)

    checkFromToJavaDate(new Date(100))

    checkFromToJavaDate(Date.valueOf("1970-01-01"))

    checkFromToJavaDate(new Date(df1.parse("1970-01-01 00:00:00").getTime))
    checkFromToJavaDate(new Date(df2.parse("1970-01-01 00:00:00 UTC").getTime))

    checkFromToJavaDate(new Date(df1.parse("1970-01-01 00:00:01").getTime))
    checkFromToJavaDate(new Date(df2.parse("1970-01-01 00:00:01 UTC").getTime))

    checkFromToJavaDate(new Date(df1.parse("1969-12-31 23:59:59").getTime))
    checkFromToJavaDate(new Date(df2.parse("1969-12-31 23:59:59 UTC").getTime))

    checkFromToJavaDate(Date.valueOf("1969-01-01"))

    checkFromToJavaDate(new Date(df1.parse("1969-01-01 00:00:00").getTime))
    checkFromToJavaDate(new Date(df2.parse("1969-01-01 00:00:00 UTC").getTime))

    checkFromToJavaDate(new Date(df1.parse("1969-01-01 00:00:01").getTime))
    checkFromToJavaDate(new Date(df2.parse("1969-01-01 00:00:01 UTC").getTime))

    checkFromToJavaDate(new Date(df1.parse("1989-11-09 11:59:59").getTime))
    checkFromToJavaDate(new Date(df2.parse("1989-11-09 19:59:59 UTC").getTime))

    checkFromToJavaDate(new Date(df1.parse("1776-07-04 10:30:00").getTime))
    checkFromToJavaDate(new Date(df2.parse("1776-07-04 18:30:00 UTC").getTime))
  }

  test("string to date") {
    assert(stringToDate(UTF8String.fromString("2015-01-28")).get === days(2015, 1, 28))
    assert(stringToDate(UTF8String.fromString("2015")).get === days(2015, 1, 1))
    assert(stringToDate(UTF8String.fromString("0001")).get === days(1, 1, 1))
    assert(stringToDate(UTF8String.fromString("2015-03")).get === days(2015, 3, 1))
    Seq("2015-03-18", "2015-03-18 ", " 2015-03-18", " 2015-03-18 ", "2015-03-18 123142",
      "2015-03-18T123123", "2015-03-18T").foreach { s =>
      assert(stringToDate(UTF8String.fromString(s)).get === days(2015, 3, 18))
    }

    assert(stringToDate(UTF8String.fromString("2015-03-18X")).isEmpty)
    assert(stringToDate(UTF8String.fromString("2015/03/18")).isEmpty)
    assert(stringToDate(UTF8String.fromString("2015.03.18")).isEmpty)
    assert(stringToDate(UTF8String.fromString("20150318")).isEmpty)
    assert(stringToDate(UTF8String.fromString("2015-031-8")).isEmpty)
    assert(stringToDate(UTF8String.fromString("02015-03-18")).isEmpty)
    assert(stringToDate(UTF8String.fromString("015-03-18")).isEmpty)
    assert(stringToDate(UTF8String.fromString("015")).isEmpty)
    assert(stringToDate(UTF8String.fromString("02015")).isEmpty)
  }

  test("string to timestamp") {
    for (tz <- ALL_TIMEZONES) {
      def checkStringToTimestamp(str: String, expected: Option[Long]): Unit = {
        assert(stringToTimestamp(UTF8String.fromString(str), tz.toZoneId) === expected)
      }

      checkStringToTimestamp("1969-12-31 16:00:00", Option(date(1969, 12, 31, 16, tz = tz)))
      checkStringToTimestamp("0001", Option(date(1, 1, 1, 0, tz = tz)))
      checkStringToTimestamp("2015-03", Option(date(2015, 3, 1, tz = tz)))
      Seq("2015-03-18", "2015-03-18 ", " 2015-03-18", " 2015-03-18 ", "2015-03-18T").foreach { s =>
        checkStringToTimestamp(s, Option(date(2015, 3, 18, tz = tz)))
      }

      var expected = Option(date(2015, 3, 18, 12, 3, 17, tz = tz))
      checkStringToTimestamp("2015-03-18 12:03:17", expected)
      checkStringToTimestamp("2015-03-18T12:03:17", expected)

      // If the string value includes timezone string, it represents the timestamp string
      // in the timezone regardless of the tz parameter.
      var timeZone = TimeZone.getTimeZone("GMT-13:53")
      expected = Option(date(2015, 3, 18, 12, 3, 17, tz = timeZone))
      checkStringToTimestamp("2015-03-18T12:03:17-13:53", expected)

      timeZone = TimeZone.getTimeZone("UTC")
      expected = Option(date(2015, 3, 18, 12, 3, 17, tz = timeZone))
      checkStringToTimestamp("2015-03-18T12:03:17Z", expected)
      checkStringToTimestamp("2015-03-18 12:03:17Z", expected)

      timeZone = TimeZone.getTimeZone("GMT-01:00")
      expected = Option(date(2015, 3, 18, 12, 3, 17, tz = timeZone))
      checkStringToTimestamp("2015-03-18T12:03:17-1:0", expected)
      checkStringToTimestamp("2015-03-18T12:03:17-01:00", expected)

      timeZone = TimeZone.getTimeZone("GMT+07:30")
      expected = Option(date(2015, 3, 18, 12, 3, 17, tz = timeZone))
      checkStringToTimestamp("2015-03-18T12:03:17+07:30", expected)

      timeZone = TimeZone.getTimeZone("GMT+07:03")
      expected = Option(date(2015, 3, 18, 12, 3, 17, tz = timeZone))
      checkStringToTimestamp("2015-03-18T12:03:17+07:03", expected)

      // tests for the string including milliseconds.
      expected = Option(date(2015, 3, 18, 12, 3, 17, 123000, tz = tz))
      checkStringToTimestamp("2015-03-18 12:03:17.123", expected)
      checkStringToTimestamp("2015-03-18T12:03:17.123", expected)

      // If the string value includes timezone string, it represents the timestamp string
      // in the timezone regardless of the tz parameter.
      timeZone = TimeZone.getTimeZone("UTC")
      expected = Option(date(2015, 3, 18, 12, 3, 17, 456000, tz = timeZone))
      checkStringToTimestamp("2015-03-18T12:03:17.456Z", expected)
      checkStringToTimestamp("2015-03-18 12:03:17.456Z", expected)

      timeZone = TimeZone.getTimeZone("GMT-01:00")
      expected = Option(date(2015, 3, 18, 12, 3, 17, 123000, tz = timeZone))
      checkStringToTimestamp("2015-03-18T12:03:17.123-1:0", expected)
      checkStringToTimestamp("2015-03-18T12:03:17.123-01:00", expected)

      timeZone = TimeZone.getTimeZone("GMT+07:30")
      expected = Option(date(2015, 3, 18, 12, 3, 17, 123000, tz = timeZone))
      checkStringToTimestamp("2015-03-18T12:03:17.123+07:30", expected)

      timeZone = TimeZone.getTimeZone("GMT+07:30")
      expected = Option(date(2015, 3, 18, 12, 3, 17, 123000, tz = timeZone))
      checkStringToTimestamp("2015-03-18T12:03:17.123+07:30", expected)

      timeZone = TimeZone.getTimeZone("GMT+07:30")
      expected = Option(date(2015, 3, 18, 12, 3, 17, 123121, tz = timeZone))
      checkStringToTimestamp("2015-03-18T12:03:17.123121+7:30", expected)

      timeZone = TimeZone.getTimeZone("GMT+07:30")
      expected = Option(date(2015, 3, 18, 12, 3, 17, 123120, tz = timeZone))
      checkStringToTimestamp("2015-03-18T12:03:17.12312+7:30", expected)

      expected = Option(time(18, 12, 15, tz = tz))
      checkStringToTimestamp("18:12:15", expected)

      timeZone = TimeZone.getTimeZone("GMT+07:30")
      expected = Option(time(18, 12, 15, 123120, tz = timeZone))
      checkStringToTimestamp("T18:12:15.12312+7:30", expected)

      timeZone = TimeZone.getTimeZone("GMT+07:30")
      expected = Option(time(18, 12, 15, 123120, tz = timeZone))
      checkStringToTimestamp("18:12:15.12312+7:30", expected)

      expected = Option(date(2011, 5, 6, 7, 8, 9, 100000, tz = tz))
      checkStringToTimestamp("2011-05-06 07:08:09.1000", expected)

      checkStringToTimestamp("238", None)
      checkStringToTimestamp("00238", None)
      checkStringToTimestamp("2015-03-18 123142", None)
      checkStringToTimestamp("2015-03-18T123123", None)
      checkStringToTimestamp("2015-03-18X", None)
      checkStringToTimestamp("2015/03/18", None)
      checkStringToTimestamp("2015.03.18", None)
      checkStringToTimestamp("20150318", None)
      checkStringToTimestamp("2015-031-8", None)
      checkStringToTimestamp("02015-01-18", None)
      checkStringToTimestamp("015-01-18", None)
      checkStringToTimestamp("2015-03-18T12:03.17-20:0", None)
      checkStringToTimestamp("2015-03-18T12:03.17-0:70", None)
      checkStringToTimestamp("2015-03-18T12:03.17-1:0:0", None)

      // Truncating the fractional seconds
      timeZone = TimeZone.getTimeZone("GMT+00:00")
      expected = Option(date(2015, 3, 18, 12, 3, 17, 123456, tz = timeZone))
      checkStringToTimestamp(
        "2015-03-18T12:03:17.123456789+0:00", expected)
    }
  }

  test("SPARK-15379: special invalid date string") {
    // Test stringToDate
    assert(stringToDate(
      UTF8String.fromString("2015-02-29 00:00:00")).isEmpty)
    assert(stringToDate(
      UTF8String.fromString("2015-04-31 00:00:00")).isEmpty)
    assert(stringToDate(UTF8String.fromString("2015-02-29")).isEmpty)
    assert(stringToDate(UTF8String.fromString("2015-04-31")).isEmpty)


    // Test stringToTimestamp
    assert(stringToTimestamp(
      UTF8String.fromString("2015-02-29 00:00:00"), defaultZoneId).isEmpty)
    assert(stringToTimestamp(
      UTF8String.fromString("2015-04-31 00:00:00"), defaultZoneId).isEmpty)
    assert(stringToTimestamp(UTF8String.fromString("2015-02-29"), defaultZoneId).isEmpty)
    assert(stringToTimestamp(UTF8String.fromString("2015-04-31"), defaultZoneId).isEmpty)
  }

  test("hours") {
    var input = date(2015, 3, 18, 13, 2, 11, 0, TimeZonePST)
    assert(getHours(input, TimeZonePST) === 13)
    assert(getHours(input, TimeZoneGMT) === 20)
    input = date(2015, 12, 8, 2, 7, 9, 0, TimeZonePST)
    assert(getHours(input, TimeZonePST) === 2)
    assert(getHours(input, TimeZoneGMT) === 10)
  }

  test("minutes") {
    var input = date(2015, 3, 18, 13, 2, 11, 0, TimeZonePST)
    assert(getMinutes(input, TimeZonePST) === 2)
    assert(getMinutes(input, TimeZoneGMT) === 2)
    assert(getMinutes(input, TimeZone.getTimeZone("Australia/North")) === 32)
    input = date(2015, 3, 8, 2, 7, 9, 0, TimeZonePST)
    assert(getMinutes(input, TimeZonePST) === 7)
    assert(getMinutes(input, TimeZoneGMT) === 7)
    assert(getMinutes(input, TimeZone.getTimeZone("Australia/North")) === 37)
  }

  test("seconds") {
    var input = date(2015, 3, 18, 13, 2, 11, 0, TimeZonePST)
    assert(getSeconds(input, TimeZonePST) === 11)
    assert(getSeconds(input, TimeZoneGMT) === 11)
    input = date(2015, 3, 8, 2, 7, 9, 0, TimeZonePST)
    assert(getSeconds(input, TimeZonePST) === 9)
    assert(getSeconds(input, TimeZoneGMT) === 9)
  }

  test("hours / minutes / seconds") {
    Seq(Timestamp.valueOf("2015-06-11 10:12:35.789"),
      Timestamp.valueOf("2015-06-11 20:13:40.789"),
      Timestamp.valueOf("1900-06-11 12:14:50.789"),
      Timestamp.valueOf("1700-02-28 12:14:50.123456")).foreach { t =>
      val us = fromJavaTimestamp(t)
      assert(toJavaTimestamp(us) === t)
    }
  }

  test("get day in year") {
    assert(getDayInYear(days(2015, 3, 18)) === 77)
    assert(getDayInYear(days(2012, 3, 18)) === 78)
  }

  test("day of year calculations for old years") {
    assert(getDayInYear(days(1582, 3)) === 60)

    (1000 to 1600 by 10).foreach { year =>
      // January 1 is the 1st day of year.
      assert(getYear(days(year)) === year)
      assert(getMonth(days(year, 1)) === 1)
      assert(getDayInYear(days(year, 1, 1)) === 1)

      // December 31 is the 1st day of year.
      val date = days(year, 12, 31)
      assert(getYear(date) === year)
      assert(getMonth(date) === 12)
      assert(getDayOfMonth(date) === 31)
    }
  }

  test("get year") {
    assert(getYear(days(2015, 2, 18)) === 2015)
    assert(getYear(days(2012, 2, 18)) === 2012)
  }

  test("get quarter") {
    assert(getQuarter(days(2015, 2, 18)) === 1)
    assert(getQuarter(days(2012, 11, 18)) === 4)
  }

  test("get month") {
    assert(getMonth(days(2015, 3, 18)) === 3)
    assert(getMonth(days(2012, 12, 18)) === 12)
  }

  test("get day of month") {
    assert(getDayOfMonth(days(2015, 3, 18)) === 18)
    assert(getDayOfMonth(days(2012, 12, 24)) === 24)
  }

  test("date add months") {
    val input = days(1997, 2, 28, 10, 30)
    assert(dateAddMonths(input, 36) === days(2000, 2, 29))
    assert(dateAddMonths(input, -13) === days(1996, 1, 31))
  }

  test("timestamp add months") {
    val ts1 = date(1997, 2, 28, 10, 30, 0)
    val ts2 = date(2000, 2, 29, 10, 30, 0, 123000)
    assert(timestampAddInterval(ts1, 36, 123000, defaultTz) === ts2)

    val ts3 = date(1997, 2, 27, 16, 0, 0, 0, TimeZonePST)
    val ts4 = date(2000, 2, 27, 16, 0, 0, 123000, TimeZonePST)
    val ts5 = date(2000, 2, 29, 0, 0, 0, 123000, TimeZoneGMT)
    assert(timestampAddInterval(ts3, 36, 123000, TimeZonePST) === ts4)
    assert(timestampAddInterval(ts3, 36, 123000, TimeZoneGMT) === ts5)
  }

  test("monthsBetween") {
    val date1 = date(1997, 2, 28, 10, 30, 0)
    var date2 = date(1996, 10, 30)
    assert(monthsBetween(date1, date2, true, TimeZoneUTC) === 3.94959677)
    assert(monthsBetween(date1, date2, false, TimeZoneUTC) === 3.9495967741935485)
    Seq(true, false).foreach { roundOff =>
      date2 = date(2000, 2, 28)
      assert(monthsBetween(date1, date2, roundOff, TimeZoneUTC) === -36)
      date2 = date(2000, 2, 29)
      assert(monthsBetween(date1, date2, roundOff, TimeZoneUTC) === -36)
      date2 = date(1996, 3, 31)
      assert(monthsBetween(date1, date2, roundOff, TimeZoneUTC) === 11)
    }

    val date3 = date(2000, 2, 28, 16, tz = TimeZonePST)
    val date4 = date(1997, 2, 28, 16, tz = TimeZonePST)
    assert(monthsBetween(date3, date4, true, TimeZonePST) === 36.0)
    assert(monthsBetween(date3, date4, true, TimeZoneGMT) === 35.90322581)
    assert(monthsBetween(date3, date4, false, TimeZoneGMT) === 35.903225806451616)
  }

  test("from UTC timestamp") {
    def test(utc: String, tz: String, expected: String): Unit = {
      assert(toJavaTimestamp(fromUTCTime(fromJavaTimestamp(Timestamp.valueOf(utc)), tz)).toString
        === expected)
    }
    for (tz <- ALL_TIMEZONES) {
      withDefaultTimeZone(tz) {
        test("2011-12-25 09:00:00.123456", "UTC", "2011-12-25 09:00:00.123456")
        test("2011-12-25 09:00:00.123456", "JST", "2011-12-25 18:00:00.123456")
        test("2011-12-25 09:00:00.123456", "PST", "2011-12-25 01:00:00.123456")
        test("2011-12-25 09:00:00.123456", "Asia/Shanghai", "2011-12-25 17:00:00.123456")
      }
    }

    withDefaultTimeZone(TimeZone.getTimeZone("PST")) {
      // Daylight Saving Time
      test("2016-03-13 09:59:59.0", "PST", "2016-03-13 01:59:59.0")
      test("2016-03-13 10:00:00.0", "PST", "2016-03-13 03:00:00.0")
      test("2016-11-06 08:59:59.0", "PST", "2016-11-06 01:59:59.0")
      test("2016-11-06 09:00:00.0", "PST", "2016-11-06 01:00:00.0")
      test("2016-11-06 10:00:00.0", "PST", "2016-11-06 02:00:00.0")
    }
  }

  test("to UTC timestamp") {
    def test(utc: String, tz: String, expected: String): Unit = {
      assert(toJavaTimestamp(toUTCTime(fromJavaTimestamp(Timestamp.valueOf(utc)), tz)).toString
        === expected)
    }

    for (tz <- ALL_TIMEZONES) {
      withDefaultTimeZone(tz) {
        test("2011-12-25 09:00:00.123456", "UTC", "2011-12-25 09:00:00.123456")
        test("2011-12-25 18:00:00.123456", "JST", "2011-12-25 09:00:00.123456")
        test("2011-12-25 01:00:00.123456", "PST", "2011-12-25 09:00:00.123456")
        test("2011-12-25 17:00:00.123456", "Asia/Shanghai", "2011-12-25 09:00:00.123456")
      }
    }

    withDefaultTimeZone(TimeZone.getTimeZone("PST")) {
      // Daylight Saving Time
      test("2016-03-13 01:59:59", "PST", "2016-03-13 09:59:59.0")
      // 2016-03-13 02:00:00 PST does not exists
      test("2016-03-13 02:00:00", "PST", "2016-03-13 10:00:00.0")
      test("2016-03-13 03:00:00", "PST", "2016-03-13 10:00:00.0")
      test("2016-11-06 00:59:59", "PST", "2016-11-06 07:59:59.0")
      // 2016-11-06 01:00:00 PST could be 2016-11-06 08:00:00 UTC or 2016-11-06 09:00:00 UTC
      test("2016-11-06 01:00:00", "PST", "2016-11-06 09:00:00.0")
      test("2016-11-06 01:59:59", "PST", "2016-11-06 09:59:59.0")
      test("2016-11-06 02:00:00", "PST", "2016-11-06 10:00:00.0")
    }
  }

  test("truncTimestamp") {
    def testTrunc(
        level: Int,
        expected: String,
        inputTS: SQLTimestamp,
        timezone: TimeZone = DateTimeUtils.defaultTimeZone()): Unit = {
      val truncated =
        DateTimeUtils.truncTimestamp(inputTS, level, timezone)
      val expectedTS =
        DateTimeUtils.stringToTimestamp(UTF8String.fromString(expected), defaultZoneId)
      assert(truncated === expectedTS.get)
    }

    val defaultInputTS = DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-03-05T09:32:05.359"), defaultZoneId)
    val defaultInputTS1 = DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-03-31T20:32:05.359"), defaultZoneId)
    val defaultInputTS2 = DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-04-01T02:32:05.359"), defaultZoneId)
    val defaultInputTS3 = DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-03-30T02:32:05.359"), defaultZoneId)
    val defaultInputTS4 = DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-03-29T02:32:05.359"), defaultZoneId)

    testTrunc(DateTimeUtils.TRUNC_TO_YEAR, "2015-01-01T00:00:00", defaultInputTS.get)
    testTrunc(DateTimeUtils.TRUNC_TO_MONTH, "2015-03-01T00:00:00", defaultInputTS.get)
    testTrunc(DateTimeUtils.TRUNC_TO_DAY, "2015-03-05T00:00:00", defaultInputTS.get)
    testTrunc(DateTimeUtils.TRUNC_TO_HOUR, "2015-03-05T09:00:00", defaultInputTS.get)
    testTrunc(DateTimeUtils.TRUNC_TO_MINUTE, "2015-03-05T09:32:00", defaultInputTS.get)
    testTrunc(DateTimeUtils.TRUNC_TO_SECOND, "2015-03-05T09:32:05", defaultInputTS.get)
    testTrunc(DateTimeUtils.TRUNC_TO_WEEK, "2015-03-02T00:00:00", defaultInputTS.get)
    testTrunc(DateTimeUtils.TRUNC_TO_WEEK, "2015-03-30T00:00:00", defaultInputTS1.get)
    testTrunc(DateTimeUtils.TRUNC_TO_WEEK, "2015-03-30T00:00:00", defaultInputTS2.get)
    testTrunc(DateTimeUtils.TRUNC_TO_WEEK, "2015-03-30T00:00:00", defaultInputTS3.get)
    testTrunc(DateTimeUtils.TRUNC_TO_WEEK, "2015-03-23T00:00:00", defaultInputTS4.get)
    testTrunc(DateTimeUtils.TRUNC_TO_QUARTER, "2015-01-01T00:00:00", defaultInputTS.get)
    testTrunc(DateTimeUtils.TRUNC_TO_QUARTER, "2015-01-01T00:00:00", defaultInputTS1.get)
    testTrunc(DateTimeUtils.TRUNC_TO_QUARTER, "2015-04-01T00:00:00", defaultInputTS2.get)

    for (tz <- ALL_TIMEZONES) {
      withDefaultTimeZone(tz) {
        val inputTS = DateTimeUtils.stringToTimestamp(
          UTF8String.fromString("2015-03-05T09:32:05.359"), defaultZoneId)
        val inputTS1 = DateTimeUtils.stringToTimestamp(
          UTF8String.fromString("2015-03-31T20:32:05.359"), defaultZoneId)
        val inputTS2 = DateTimeUtils.stringToTimestamp(
          UTF8String.fromString("2015-04-01T02:32:05.359"), defaultZoneId)
        val inputTS3 = DateTimeUtils.stringToTimestamp(
          UTF8String.fromString("2015-03-30T02:32:05.359"), defaultZoneId)
        val inputTS4 = DateTimeUtils.stringToTimestamp(
          UTF8String.fromString("2015-03-29T02:32:05.359"), defaultZoneId)

        testTrunc(DateTimeUtils.TRUNC_TO_YEAR, "2015-01-01T00:00:00", inputTS.get, tz)
        testTrunc(DateTimeUtils.TRUNC_TO_MONTH, "2015-03-01T00:00:00", inputTS.get, tz)
        testTrunc(DateTimeUtils.TRUNC_TO_DAY, "2015-03-05T00:00:00", inputTS.get, tz)
        testTrunc(DateTimeUtils.TRUNC_TO_HOUR, "2015-03-05T09:00:00", inputTS.get, tz)
        testTrunc(DateTimeUtils.TRUNC_TO_MINUTE, "2015-03-05T09:32:00", inputTS.get, tz)
        testTrunc(DateTimeUtils.TRUNC_TO_SECOND, "2015-03-05T09:32:05", inputTS.get, tz)
        testTrunc(DateTimeUtils.TRUNC_TO_WEEK, "2015-03-02T00:00:00", inputTS.get, tz)
        testTrunc(DateTimeUtils.TRUNC_TO_WEEK, "2015-03-30T00:00:00", inputTS1.get, tz)
        testTrunc(DateTimeUtils.TRUNC_TO_WEEK, "2015-03-30T00:00:00", inputTS2.get, tz)
        testTrunc(DateTimeUtils.TRUNC_TO_WEEK, "2015-03-30T00:00:00", inputTS3.get, tz)
        testTrunc(DateTimeUtils.TRUNC_TO_WEEK, "2015-03-23T00:00:00", inputTS4.get, tz)
        testTrunc(DateTimeUtils.TRUNC_TO_QUARTER, "2015-01-01T00:00:00", inputTS.get, tz)
        testTrunc(DateTimeUtils.TRUNC_TO_QUARTER, "2015-01-01T00:00:00", inputTS1.get, tz)
        testTrunc(DateTimeUtils.TRUNC_TO_QUARTER, "2015-04-01T00:00:00", inputTS2.get, tz)
      }
    }
  }

  test("daysToMillis and millisToDays") {
    val input = TimeUnit.MICROSECONDS.toMillis(date(2015, 12, 31, 16, tz = TimeZonePST))
    assert(millisToDays(input, TimeZonePST) === 16800)
    assert(millisToDays(input, TimeZoneGMT) === 16801)
    assert(millisToDays(-1 * MILLIS_PER_DAY + 1, TimeZoneGMT) == -1)

    var expected = TimeUnit.MICROSECONDS.toMillis(date(2015, 12, 31, tz = TimeZonePST))
    assert(daysToMillis(16800, TimeZonePST) === expected)

    expected = TimeUnit.MICROSECONDS.toMillis(date(2015, 12, 31, tz = TimeZoneGMT))
    assert(daysToMillis(16800, TimeZoneGMT) === expected)

    // There are some days are skipped entirely in some timezone, skip them here.
    val skipped_days = Map[String, Set[Int]](
      "Kwajalein" -> Set(8632),
      "Pacific/Apia" -> Set(15338),
      "Pacific/Enderbury" -> Set(9130, 9131),
      "Pacific/Fakaofo" -> Set(15338),
      "Pacific/Kiritimati" -> Set(9130, 9131),
      "Pacific/Kwajalein" -> Set(8632),
      "MIT" -> Set(15338))
    for (tz <- ALL_TIMEZONES) {
      val skipped = skipped_days.getOrElse(tz.getID, Set.empty)
      (-20000 to 20000).foreach { d =>
        if (!skipped.contains(d)) {
          assert(millisToDays(daysToMillis(d, tz), tz) === d,
            s"Round trip of ${d} did not work in tz ${tz}")
        }
      }
    }
  }

  test("toMillis") {
    assert(DateTimeUtils.toMillis(-9223372036844776001L) === -9223372036844777L)
    assert(DateTimeUtils.toMillis(-157700927876544L) === -157700927877L)
  }
}
