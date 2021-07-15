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
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneId}
import java.util.Locale
import java.util.concurrent.TimeUnit

import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.catalyst.util.RebaseDateTime.rebaseJulianToGregorianMicros
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class DateTimeUtilsSuite extends SparkFunSuite with Matchers with SQLHelper {

  private def defaultZoneId = ZoneId.systemDefault()

  test("nanoseconds truncation") {
    val tf = TimestampFormatter.getFractionFormatter(ZoneId.systemDefault())
    def checkStringToTimestamp(originalTime: String, expectedParsedTime: String): Unit = {
      val parsedTimestampOp = DateTimeUtils.stringToTimestamp(
        UTF8String.fromString(originalTime), defaultZoneId)
      assert(parsedTimestampOp.isDefined, "timestamp with nanoseconds was not parsed correctly")
      assert(tf.format(parsedTimestampOp.get) === expectedParsedTime)
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
    val (d, ns) = toJulianDay(RebaseDateTime.rebaseGregorianToJulianMicros(0))
    assert(d === JULIAN_DAY_OF_EPOCH)
    assert(ns === 0)
    assert(rebaseJulianToGregorianMicros(fromJulianDay(d, ns)) == 0L)

    Seq(Timestamp.valueOf("2015-06-11 10:10:10.100"),
      Timestamp.valueOf("2015-06-11 20:10:10.100"),
      Timestamp.valueOf("1900-06-11 20:10:10.100")).foreach { t =>
      val (d, ns) = toJulianDay(RebaseDateTime.rebaseGregorianToJulianMicros(fromJavaTimestamp(t)))
      assert(ns > 0)
      val t1 = toJavaTimestamp(rebaseJulianToGregorianMicros(fromJulianDay(d, ns)))
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

  private def toDate(s: String): Option[Int] = {
    stringToDate(UTF8String.fromString(s))
  }

  test("string to date") {
    assert(toDate("2015-01-28").get === days(2015, 1, 28))
    assert(toDate("2015").get === days(2015, 1, 1))
    assert(toDate("0001").get === days(1, 1, 1))
    assert(toDate("2015-03").get === days(2015, 3, 1))
    Seq("2015-03-18", "2015-03-18 ", " 2015-03-18", " 2015-03-18 ", "2015-03-18 123142",
      "2015-03-18T123123", "2015-03-18T").foreach { s =>
      assert(toDate(s).get === days(2015, 3, 18))
    }

    assert(toDate("2015-03-18X").isEmpty)
    assert(toDate("2015/03/18").isEmpty)
    assert(toDate("2015.03.18").isEmpty)
    assert(toDate("20150318").isEmpty)
    assert(toDate("2015-031-8").isEmpty)
    assert(toDate("015-03-18").isEmpty)
    assert(toDate("015").isEmpty)
    assert(toDate("1999 08 01").isEmpty)
    assert(toDate("1999-08 01").isEmpty)
    assert(toDate("1999 08").isEmpty)
    assert(toDate("").isEmpty)
    assert(toDate("   ").isEmpty)
  }

  test("SPARK-35780: support full range of date string") {
    assert(toDate("02015-03-18").get === days(2015, 3, 18))
    assert(toDate("02015").get === days(2015, 1, 1))
    assert(toDate("-02015").get === days(-2015, 1, 1))
    assert(toDate("999999-1-28").get === days(999999, 1, 28))
    assert(toDate("-999999-1-28").get === days(-999999, 1, 28))
    assert(toDate("0001-1-28").get === days(1, 1, 28))
    // Int.MaxValue and Int.MaxValue + 1 day
    assert(toDate("5881580-7-11").get === days(5881580, 7, 11))
    assert(toDate("5881580-7-12").isEmpty)
    // Int.MinValue and Int.MinValue - 1 day
    assert(toDate("-5877641-6-23").get === days(-5877641, 6, 23))
    assert(toDate("-5877641-6-22").isEmpty)
    // Check overflow of single segment in date format
    assert(toDate("4294967297").isEmpty)
    assert(toDate("2021-4294967297-11").isEmpty)
  }

  private def toTimestamp(str: String, zoneId: ZoneId): Option[Long] = {
    stringToTimestamp(UTF8String.fromString(str), zoneId)
  }

  test("string to timestamp") {
    for (zid <- ALL_TIMEZONES) {
      def checkStringToTimestamp(str: String, expected: Option[Long]): Unit = {
        assert(toTimestamp(str, zid) === expected)
      }

      checkStringToTimestamp("1969-12-31 16:00:00", Option(date(1969, 12, 31, 16, zid = zid)))
      checkStringToTimestamp("0001", Option(date(1, 1, 1, 0, zid = zid)))
      checkStringToTimestamp("2015-03", Option(date(2015, 3, 1, zid = zid)))
      Seq("2015-03-18", "2015-03-18 ", " 2015-03-18", " 2015-03-18 ", "2015-03-18T").foreach { s =>
        checkStringToTimestamp(s, Option(date(2015, 3, 18, zid = zid)))
      }

      var expected = Option(date(2015, 3, 18, 12, 3, 17, zid = zid))
      checkStringToTimestamp("2015-03-18 12:03:17", expected)
      checkStringToTimestamp("2015-03-18T12:03:17", expected)

      // If the string value includes timezone string, it represents the timestamp string
      // in the timezone regardless of the tz parameter.
      var zoneId = getZoneId("-13:53")
      expected = Option(date(2015, 3, 18, 12, 3, 17, zid = zoneId))
      checkStringToTimestamp("2015-03-18T12:03:17-13:53", expected)
      checkStringToTimestamp("2015-03-18T12:03:17GMT-13:53", expected)

      expected = Option(date(2015, 3, 18, 12, 3, 17, zid = UTC))
      checkStringToTimestamp("2015-03-18T12:03:17Z", expected)
      checkStringToTimestamp("2015-03-18 12:03:17Z", expected)
      checkStringToTimestamp("2015-03-18 12:03:17UTC", expected)

      zoneId = getZoneId("-01:00")
      expected = Option(date(2015, 3, 18, 12, 3, 17, zid = zoneId))
      checkStringToTimestamp("2015-03-18T12:03:17-1:0", expected)
      checkStringToTimestamp("2015-03-18T12:03:17-01:00", expected)
      checkStringToTimestamp("2015-03-18T12:03:17GMT-01:00", expected)

      zoneId = getZoneId("+07:30")
      expected = Option(date(2015, 3, 18, 12, 3, 17, zid = zoneId))
      checkStringToTimestamp("2015-03-18T12:03:17+07:30", expected)
      checkStringToTimestamp("2015-03-18T12:03:17 GMT+07:30", expected)

      zoneId = getZoneId("+07:03")
      expected = Option(date(2015, 3, 18, 12, 3, 17, zid = zoneId))
      checkStringToTimestamp("2015-03-18T12:03:17+07:03", expected)
      checkStringToTimestamp("2015-03-18T12:03:17GMT+07:03", expected)

      // tests for the string including milliseconds.
      expected = Option(date(2015, 3, 18, 12, 3, 17, 123000, zid = zid))
      checkStringToTimestamp("2015-03-18 12:03:17.123", expected)
      checkStringToTimestamp("2015-03-18T12:03:17.123", expected)

      // If the string value includes timezone string, it represents the timestamp string
      // in the timezone regardless of the tz parameter.
      expected = Option(date(2015, 3, 18, 12, 3, 17, 456000, zid = UTC))
      checkStringToTimestamp("2015-03-18T12:03:17.456Z", expected)
      checkStringToTimestamp("2015-03-18 12:03:17.456Z", expected)
      checkStringToTimestamp("2015-03-18 12:03:17.456 UTC", expected)

      zoneId = getZoneId("-01:00")
      expected = Option(date(2015, 3, 18, 12, 3, 17, 123000, zid = zoneId))
      checkStringToTimestamp("2015-03-18T12:03:17.123-1:0", expected)
      checkStringToTimestamp("2015-03-18T12:03:17.123-01:00", expected)
      checkStringToTimestamp("2015-03-18T12:03:17.123 GMT-01:00", expected)

      zoneId = getZoneId("+07:30")
      expected = Option(date(2015, 3, 18, 12, 3, 17, 123000, zid = zoneId))
      checkStringToTimestamp("2015-03-18T12:03:17.123+07:30", expected)
      checkStringToTimestamp("2015-03-18T12:03:17.123 GMT+07:30", expected)

      zoneId = getZoneId("+07:30")
      expected = Option(date(2015, 3, 18, 12, 3, 17, 123000, zid = zoneId))
      checkStringToTimestamp("2015-03-18T12:03:17.123+07:30", expected)
      checkStringToTimestamp("2015-03-18T12:03:17.123GMT+07:30", expected)

      expected = Option(date(2015, 3, 18, 12, 3, 17, 123121, zid = zoneId))
      checkStringToTimestamp("2015-03-18T12:03:17.123121+7:30", expected)
      checkStringToTimestamp("2015-03-18T12:03:17.123121 GMT+0730", expected)

      zoneId = getZoneId("+07:30")
      expected = Option(date(2015, 3, 18, 12, 3, 17, 123120, zid = zoneId))
      checkStringToTimestamp("2015-03-18T12:03:17.12312+7:30", expected)
      checkStringToTimestamp("2015-03-18T12:03:17.12312 UT+07:30", expected)

      expected = Option(time(18, 12, 15, zid = zid))
      checkStringToTimestamp("18:12:15", expected)

      zoneId = getZoneId("+07:30")
      expected = Option(time(18, 12, 15, 123120, zid = zoneId))
      checkStringToTimestamp("T18:12:15.12312+7:30", expected)
      checkStringToTimestamp("T18:12:15.12312 UTC+07:30", expected)

      zoneId = getZoneId("+07:30")
      expected = Option(time(18, 12, 15, 123120, zid = zoneId))
      checkStringToTimestamp("18:12:15.12312+7:30", expected)
      checkStringToTimestamp("18:12:15.12312 GMT+07:30", expected)

      expected = Option(date(2011, 5, 6, 7, 8, 9, 100000, zid = zid))
      checkStringToTimestamp("2011-05-06 07:08:09.1000", expected)

      checkStringToTimestamp("238", None)
      checkStringToTimestamp("2015-03-18 123142", None)
      checkStringToTimestamp("2015-03-18T123123", None)
      checkStringToTimestamp("2015-03-18X", None)
      checkStringToTimestamp("2015/03/18", None)
      checkStringToTimestamp("2015.03.18", None)
      checkStringToTimestamp("20150318", None)
      checkStringToTimestamp("2015-031-8", None)
      checkStringToTimestamp("015-01-18", None)
      checkStringToTimestamp("2015-03-18T12:03.17-20:0", None)
      checkStringToTimestamp("2015-03-18T12:03.17-0:70", None)
      checkStringToTimestamp("2015-03-18T12:03.17-1:0:0", None)
      checkStringToTimestamp("1999 08 01", None)
      checkStringToTimestamp("1999-08 01", None)
      checkStringToTimestamp("1999 08", None)
      checkStringToTimestamp("", None)
      checkStringToTimestamp("    ", None)
      checkStringToTimestamp("+", None)

      // Truncating the fractional seconds
      expected = Option(date(2015, 3, 18, 12, 3, 17, 123456, zid = UTC))
      checkStringToTimestamp("2015-03-18T12:03:17.123456789+0:00", expected)
      checkStringToTimestamp("2015-03-18T12:03:17.123456789 UTC+0", expected)
      checkStringToTimestamp("2015-03-18T12:03:17.123456789GMT+00:00", expected)

      zoneId = getZoneId("Europe/Moscow")
      expected = Option(date(2015, 3, 18, 12, 3, 17, 123456, zid = zoneId))
      checkStringToTimestamp("2015-03-18T12:03:17.123456 Europe/Moscow", expected)
    }
  }

  test("SPARK-35780: support full range of timestamp string") {
    def checkStringToTimestamp(str: String, expected: Option[Long]): Unit = {
      assert(toTimestamp(str, UTC) === expected)
    }

    checkStringToTimestamp("-1969-12-31 16:00:00", Option(date(-1969, 12, 31, 16, zid = UTC)))
    checkStringToTimestamp("02015-03-18 16:00:00", Option(date(2015, 3, 18, 16, zid = UTC)))
    checkStringToTimestamp("000001", Option(date(1, 1, 1, 0, zid = UTC)))
    checkStringToTimestamp("-000001", Option(date(-1, 1, 1, 0, zid = UTC)))
    checkStringToTimestamp("00238", Option(date(238, 1, 1, 0, zid = UTC)))
    checkStringToTimestamp("99999-03-01T12:03:17", Option(date(99999, 3, 1, 12, 3, 17, zid = UTC)))
    checkStringToTimestamp("+12:12:12", None)
    checkStringToTimestamp("-12:12:12", None)
    checkStringToTimestamp("", None)
    checkStringToTimestamp("    ", None)
    checkStringToTimestamp("+", None)
    // Long.MaxValue and Long.MaxValue + 1 micro seconds
    checkStringToTimestamp(
      "294247-01-10T04:00:54.775807Z",
      Option(date(294247, 1, 10, 4, 0, 54, 775807, zid = UTC)))
    checkStringToTimestamp("294247-01-10T04:00:54.775808Z", None)
    // Long.MinValue and Long.MinValue - 1 micro seconds
    checkStringToTimestamp(
      "-290308-12-21T19:59:05.224192Z",
      Option(date(-290308, 12, 21, 19, 59, 5, 224192, zid = UTC)))
    // Check overflow of single segment in timestamp format
    checkStringToTimestamp("-290308-12-21T19:59:05.224191Z", None)
    checkStringToTimestamp("4294967297", None)
    checkStringToTimestamp("2021-4294967297-11", None)
    checkStringToTimestamp("4294967297:30:00", None)
    checkStringToTimestamp("2021-11-4294967297T12:30:00", None)
    checkStringToTimestamp("2021-01-01T12:4294967297:00", None)
    checkStringToTimestamp("2021-01-01T12:30:4294967297", None)
    checkStringToTimestamp("2021-01-01T12:30:4294967297.123456", None)
    checkStringToTimestamp("2021-01-01T12:30:4294967297+07:30", None)
    checkStringToTimestamp("2021-01-01T12:30:4294967297UTC", None)
    checkStringToTimestamp("2021-01-01T12:30:4294967297+4294967297:30", None)
  }

  test("SPARK-15379: special invalid date string") {
    // Test stringToDate
    assert(toDate("2015-02-29 00:00:00").isEmpty)
    assert(toDate("2015-04-31 00:00:00").isEmpty)
    assert(toDate("2015-02-29").isEmpty)
    assert(toDate("2015-04-31").isEmpty)


    // Test stringToTimestamp
    assert(stringToTimestamp(
      UTF8String.fromString("2015-02-29 00:00:00"), defaultZoneId).isEmpty)
    assert(stringToTimestamp(
      UTF8String.fromString("2015-04-31 00:00:00"), defaultZoneId).isEmpty)
    assert(toTimestamp("2015-02-29", defaultZoneId).isEmpty)
    assert(toTimestamp("2015-04-31", defaultZoneId).isEmpty)
  }

  test("hours") {
    var input = date(2015, 3, 18, 13, 2, 11, 0, LA)
    assert(getHours(input, LA) === 13)
    assert(getHours(input, UTC) === 20)
    input = date(2015, 12, 8, 2, 7, 9, 0, LA)
    assert(getHours(input, LA) === 2)
    assert(getHours(input, UTC) === 10)
    input = date(10, 1, 1, 0, 0, 0, 0, LA)
    assert(getHours(input, LA) === 0)
  }

  test("minutes") {
    var input = date(2015, 3, 18, 13, 2, 11, 0, LA)
    assert(getMinutes(input, LA) === 2)
    assert(getMinutes(input, UTC) === 2)
    assert(getMinutes(input, getZoneId("Australia/North")) === 32)
    input = date(2015, 3, 8, 2, 7, 9, 0, LA)
    assert(getMinutes(input, LA) === 7)
    assert(getMinutes(input, UTC) === 7)
    assert(getMinutes(input, getZoneId("Australia/North")) === 37)
    input = date(10, 1, 1, 0, 0, 0, 0, LA)
    assert(getMinutes(input, LA) === 0)
  }

  test("seconds") {
    var input = date(2015, 3, 18, 13, 2, 11, 0, LA)
    assert(getSeconds(input, LA) === 11)
    assert(getSeconds(input, UTC) === 11)
    input = date(2015, 3, 8, 2, 7, 9, 0, LA)
    assert(getSeconds(input, LA) === 9)
    assert(getSeconds(input, UTC) === 9)
    input = date(10, 1, 1, 0, 0, 0, 0, LA)
    assert(getSeconds(input, LA) === 0)
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
    val input = days(1997, 2, 28)
    assert(dateAddMonths(input, 36) === days(2000, 2, 28))
    assert(dateAddMonths(input, -13) === days(1996, 1, 28))
  }

  test("SPARK-34739: timestamp add months") {
    outstandingZoneIds.foreach { zid =>
      Seq(
        (date(2021, 3, 13, 21, 28, 13, 123456, zid), 0, date(2021, 3, 13, 21, 28, 13, 123456, zid)),
        (date(2021, 3, 31, 0, 0, 0, 123, zid), -1, date(2021, 2, 28, 0, 0, 0, 123, zid)),
        (date(2020, 2, 29, 1, 2, 3, 4, zid), 12, date(2021, 2, 28, 1, 2, 3, 4, zid)),
        (date(1, 1, 1, 0, 0, 0, 1, zid), 2020 * 12, date(2021, 1, 1, 0, 0, 0, 1, zid)),
        (date(1581, 10, 7, 23, 59, 59, 999, zid), 12, date(1582, 10, 7, 23, 59, 59, 999, zid)),
        (date(9999, 12, 31, 23, 59, 59, 999999, zid), -11,
          date(9999, 1, 31, 23, 59, 59, 999999, zid))
      ).foreach { case (timestamp, months, expected) =>
        assert(timestampAddMonths(timestamp, months, zid) === expected)
      }
    }
  }

  test("date add interval with day precision") {
    val input = days(1997, 2, 28)
    assert(dateAddInterval(input, new CalendarInterval(36, 0, 0)) === days(2000, 2, 28))
    assert(dateAddInterval(input, new CalendarInterval(36, 47, 0)) === days(2000, 4, 15))
    assert(dateAddInterval(input, new CalendarInterval(-13, 0, 0)) === days(1996, 1, 28))
    intercept[IllegalArgumentException](dateAddInterval(input, new CalendarInterval(36, 47, 1)))
  }

  test("timestamp add interval") {
    val ts1 = date(1997, 2, 28, 10, 30, 0)
    val ts2 = date(2000, 2, 28, 10, 30, 0, 123000)
    assert(timestampAddInterval(ts1, 36, 0, 123000, defaultZoneId) === ts2)

    val ts3 = date(1997, 2, 27, 16, 0, 0, 0, LA)
    val ts4 = date(2000, 2, 27, 16, 0, 0, 123000, LA)
    val ts5 = date(2000, 2, 28, 0, 0, 0, 123000, UTC)
    assert(timestampAddInterval(ts3, 36, 0, 123000, LA) === ts4)
    assert(timestampAddInterval(ts3, 36, 0, 123000, UTC) === ts5)
  }

  test("timestamp add days") {
    // 2019-3-9 is the end of Pacific Standard Time
    val ts1 = date(2019, 3, 9, 12, 0, 0, 123000, LA)
    // 2019-3-10 is the start of Pacific Daylight Time
    val ts2 = date(2019, 3, 10, 12, 0, 0, 123000, LA)
    val ts3 = date(2019, 5, 9, 12, 0, 0, 123000, LA)
    val ts4 = date(2019, 5, 10, 12, 0, 0, 123000, LA)
    // 2019-11-2 is the end of Pacific Daylight Time
    val ts5 = date(2019, 11, 2, 12, 0, 0, 123000, LA)
    // 2019-11-3 is the start of Pacific Standard Time
    val ts6 = date(2019, 11, 3, 12, 0, 0, 123000, LA)

    // transit from Pacific Standard Time to Pacific Daylight Time
    assert(timestampAddInterval(
      ts1, 0, 0, 23 * MICROS_PER_HOUR, LA) === ts2)
    assert(timestampAddInterval(ts1, 0, 1, 0, LA) === ts2)
    // just a normal day
    assert(timestampAddInterval(
      ts3, 0, 0, 24 * MICROS_PER_HOUR, LA) === ts4)
    assert(timestampAddInterval(ts3, 0, 1, 0, LA) === ts4)
    // transit from Pacific Daylight Time to Pacific Standard Time
    assert(timestampAddInterval(
      ts5, 0, 0, 25 * MICROS_PER_HOUR, LA) === ts6)
    assert(timestampAddInterval(ts5, 0, 1, 0, LA) === ts6)
  }

  test("monthsBetween") {
    val date1 = date(1997, 2, 28, 10, 30, 0)
    var date2 = date(1996, 10, 30)
    assert(monthsBetween(date1, date2, true, UTC) === 3.94959677)
    assert(monthsBetween(date1, date2, false, UTC) === 3.9495967741935485)
    Seq(true, false).foreach { roundOff =>
      date2 = date(2000, 2, 28)
      assert(monthsBetween(date1, date2, roundOff, UTC) === -36)
      date2 = date(2000, 2, 29)
      assert(monthsBetween(date1, date2, roundOff, UTC) === -36)
      date2 = date(1996, 3, 31)
      assert(monthsBetween(date1, date2, roundOff, UTC) === 11)
    }

    val date3 = date(2000, 2, 28, 16, zid = LA)
    val date4 = date(1997, 2, 28, 16, zid = LA)
    assert(monthsBetween(date3, date4, true, LA) === 36.0)
    assert(monthsBetween(date3, date4, true, UTC) === 35.90322581)
    assert(monthsBetween(date3, date4, false, UTC) === 35.903225806451616)
  }

  test("from UTC timestamp") {
    def test(utc: String, tz: String, expected: String): Unit = {
      assert(toJavaTimestamp(fromUTCTime(fromJavaTimestamp(Timestamp.valueOf(utc)), tz)).toString
        === expected)
    }
    for (tz <- ALL_TIMEZONES) {
      withDefaultTimeZone(tz) {
        test("2011-12-25 09:00:00.123456", "UTC", "2011-12-25 09:00:00.123456")
        test("2011-12-25 09:00:00.123456", JST.getId, "2011-12-25 18:00:00.123456")
        test("2011-12-25 09:00:00.123456", LA.getId, "2011-12-25 01:00:00.123456")
        test("2011-12-25 09:00:00.123456", "Asia/Shanghai", "2011-12-25 17:00:00.123456")
        test("2011-12-25 09:00:00.123456", "-7", "2011-12-25 02:00:00.123456")
        test("2011-12-25 09:00:00.123456", "+8:00", "2011-12-25 17:00:00.123456")
        test("2011-12-25 09:00:00.123456", "+8:00:00", "2011-12-25 17:00:00.123456")
        test("2011-12-25 09:00:00.123456", "+0800", "2011-12-25 17:00:00.123456")
        test("2011-12-25 09:00:00.123456", "-071020", "2011-12-25 01:49:40.123456")
        test("2011-12-25 09:00:00.123456", "-07:10:20", "2011-12-25 01:49:40.123456")

      }
    }

    withDefaultTimeZone(LA) {
      // Daylight Saving Time
      test("2016-03-13 09:59:59.0", LA.getId, "2016-03-13 01:59:59.0")
      test("2016-03-13 10:00:00.0", LA.getId, "2016-03-13 03:00:00.0")
      test("2016-11-06 08:59:59.0", LA.getId, "2016-11-06 01:59:59.0")
      test("2016-11-06 09:00:00.0", LA.getId, "2016-11-06 01:00:00.0")
      test("2016-11-06 10:00:00.0", LA.getId, "2016-11-06 02:00:00.0")
    }
  }

  test("to UTC timestamp") {
    def test(utc: String, tz: String, expected: String): Unit = {
      assert(toJavaTimestamp(toUTCTime(fromJavaTimestamp(Timestamp.valueOf(utc)), tz)).toString
        === expected)
    }

    for (zid <- ALL_TIMEZONES) {
      withDefaultTimeZone(zid) {
        test("2011-12-25 09:00:00.123456", "UTC", "2011-12-25 09:00:00.123456")
        test("2011-12-25 18:00:00.123456", JST.getId, "2011-12-25 09:00:00.123456")
        test("2011-12-25 01:00:00.123456", LA.getId, "2011-12-25 09:00:00.123456")
        test("2011-12-25 17:00:00.123456", "Asia/Shanghai", "2011-12-25 09:00:00.123456")
        test("2011-12-25 02:00:00.123456", "-7", "2011-12-25 09:00:00.123456")
        test("2011-12-25 17:00:00.123456", "+8:00", "2011-12-25 09:00:00.123456")
        test("2011-12-25 17:00:00.123456", "+8:00:00", "2011-12-25 09:00:00.123456")
        test("2011-12-25 17:00:00.123456", "+0800", "2011-12-25 09:00:00.123456")
        test("2011-12-25 01:49:40.123456", "-071020", "2011-12-25 09:00:00.123456")
        test("2011-12-25 01:49:40.123456", "-07:10:20", "2011-12-25 09:00:00.123456")
      }
    }

    withDefaultTimeZone(LA) {
      val tz = LA.getId
      // Daylight Saving Time
      test("2016-03-13 01:59:59", tz, "2016-03-13 09:59:59.0")
      test("2016-03-13 02:00:00", tz, "2016-03-13 10:00:00.0")
      test("2016-03-13 03:00:00", tz, "2016-03-13 10:00:00.0")
      test("2016-11-06 00:59:59", tz, "2016-11-06 07:59:59.0")
      test("2016-11-06 01:00:00", tz, "2016-11-06 09:00:00.0")
      test("2016-11-06 01:59:59", tz, "2016-11-06 09:59:59.0")
      test("2016-11-06 02:00:00", tz, "2016-11-06 10:00:00.0")
    }
  }

  test("trailing characters while converting string to timestamp") {
    val s = UTF8String.fromString("2019-10-31T10:59:23Z:::")
    val time = DateTimeUtils.stringToTimestamp(s, defaultZoneId)
    assert(time == None)
  }

  def testTrunc(
      level: Int,
      expected: String,
      inputTS: Long,
      zoneId: ZoneId = defaultZoneId): Unit = {
    val truncated = DateTimeUtils.truncTimestamp(inputTS, level, zoneId)
    val expectedTS = toTimestamp(expected, defaultZoneId)
    assert(truncated === expectedTS.get)
  }

  test("SPARK-33404: test truncTimestamp when time zone offset from UTC has a " +
    "granularity of seconds") {
    for (zid <- ALL_TIMEZONES) {
      withDefaultTimeZone(zid) {
        val inputTS = DateTimeUtils.stringToTimestamp(
          UTF8String.fromString("1769-10-17T17:10:02.123456"), defaultZoneId)
        testTrunc(DateTimeUtils.TRUNC_TO_MINUTE, "1769-10-17T17:10:00", inputTS.get, zid)
        testTrunc(DateTimeUtils.TRUNC_TO_SECOND, "1769-10-17T17:10:02", inputTS.get, zid)
        testTrunc(DateTimeUtils.TRUNC_TO_MILLISECOND, "1769-10-17T17:10:02.123", inputTS.get, zid)
        testTrunc(DateTimeUtils.TRUNC_TO_MICROSECOND, "1769-10-17T17:10:02.123456",
          inputTS.get, zid)
      }
    }
  }

  test("truncTimestamp") {
    val defaultInputTS = DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-03-05T09:32:05.359123"), defaultZoneId)
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
    testTrunc(DateTimeUtils.TRUNC_TO_MICROSECOND, "2015-03-05T09:32:05.359123", defaultInputTS.get)
    testTrunc(DateTimeUtils.TRUNC_TO_MILLISECOND, "2015-03-05T09:32:05.359", defaultInputTS.get)

    for (zid <- ALL_TIMEZONES) {
      withDefaultTimeZone(zid) {
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
        val inputTS5 = DateTimeUtils.stringToTimestamp(
          UTF8String.fromString("1999-03-29T01:02:03.456789"), defaultZoneId)

        testTrunc(DateTimeUtils.TRUNC_TO_YEAR, "2015-01-01T00:00:00", inputTS.get, zid)
        testTrunc(DateTimeUtils.TRUNC_TO_MONTH, "2015-03-01T00:00:00", inputTS.get, zid)
        testTrunc(DateTimeUtils.TRUNC_TO_DAY, "2015-03-05T00:00:00", inputTS.get, zid)
        testTrunc(DateTimeUtils.TRUNC_TO_HOUR, "2015-03-05T09:00:00", inputTS.get, zid)
        testTrunc(DateTimeUtils.TRUNC_TO_MINUTE, "2015-03-05T09:32:00", inputTS.get, zid)
        testTrunc(DateTimeUtils.TRUNC_TO_SECOND, "2015-03-05T09:32:05", inputTS.get, zid)
        testTrunc(DateTimeUtils.TRUNC_TO_WEEK, "2015-03-02T00:00:00", inputTS.get, zid)
        testTrunc(DateTimeUtils.TRUNC_TO_WEEK, "2015-03-30T00:00:00", inputTS1.get, zid)
        testTrunc(DateTimeUtils.TRUNC_TO_WEEK, "2015-03-30T00:00:00", inputTS2.get, zid)
        testTrunc(DateTimeUtils.TRUNC_TO_WEEK, "2015-03-30T00:00:00", inputTS3.get, zid)
        testTrunc(DateTimeUtils.TRUNC_TO_WEEK, "2015-03-23T00:00:00", inputTS4.get, zid)
        testTrunc(DateTimeUtils.TRUNC_TO_QUARTER, "2015-01-01T00:00:00", inputTS.get, zid)
        testTrunc(DateTimeUtils.TRUNC_TO_QUARTER, "2015-01-01T00:00:00", inputTS1.get, zid)
        testTrunc(DateTimeUtils.TRUNC_TO_QUARTER, "2015-04-01T00:00:00", inputTS2.get, zid)
      }
    }
  }

  test("SPARK-35664: microseconds to LocalDateTime") {
    assert(microsToLocalDateTime(0) ==  LocalDateTime.parse("1970-01-01T00:00:00"))
    assert(microsToLocalDateTime(100) ==  LocalDateTime.parse("1970-01-01T00:00:00.0001"))
    assert(microsToLocalDateTime(100000000) ==  LocalDateTime.parse("1970-01-01T00:01:40"))
    assert(microsToLocalDateTime(100000000000L) ==  LocalDateTime.parse("1970-01-02T03:46:40"))
    assert(microsToLocalDateTime(253402300799999999L) ==
      LocalDateTime.parse("9999-12-31T23:59:59.999999"))
    assert(microsToLocalDateTime(Long.MinValue) ==
      LocalDateTime.parse("-290308-12-21T19:59:05.224192"))
    assert(microsToLocalDateTime(Long.MaxValue) ==
      LocalDateTime.parse("+294247-01-10T04:00:54.775807"))
  }

  test("SPARK-35664: LocalDateTime to microseconds") {
    assert(DateTimeUtils.localDateTimeToMicros(LocalDateTime.parse("1970-01-01T00:00:00")) == 0)
    assert(
      DateTimeUtils.localDateTimeToMicros(LocalDateTime.parse("1970-01-01T00:00:00.0001")) == 100)
    assert(
      DateTimeUtils.localDateTimeToMicros(LocalDateTime.parse("1970-01-01T00:01:40")) == 100000000)
    assert(DateTimeUtils.localDateTimeToMicros(LocalDateTime.parse("1970-01-02T03:46:40")) ==
      100000000000L)
    assert(DateTimeUtils.localDateTimeToMicros(LocalDateTime.parse("9999-12-31T23:59:59.999999"))
      == 253402300799999999L)
    assert(DateTimeUtils.localDateTimeToMicros(LocalDateTime.parse("-1000-12-31T23:59:59.999999"))
      == -93692592000000001L)
    Seq(LocalDateTime.MIN, LocalDateTime.MAX).foreach { dt =>
      val msg = intercept[ArithmeticException] {
        DateTimeUtils.localDateTimeToMicros(dt)
      }.getMessage
      assert(msg == "long overflow")
    }
  }

  test("daysToMicros and microsToDays") {
    val input = date(2015, 12, 31, 16, zid = LA)
    assert(microsToDays(input, LA) === 16800)
    assert(microsToDays(input, UTC) === 16801)
    assert(microsToDays(-1 * MILLIS_PER_DAY + 1, UTC) == -1)

    var expected = date(2015, 12, 31, zid = LA)
    assert(daysToMicros(16800, LA) === expected)

    expected = date(2015, 12, 31, zid = UTC)
    assert(daysToMicros(16800, UTC) === expected)

    // There are some days are skipped entirely in some timezone, skip them here.
    val skipped_days = Map[String, Set[Int]](
      "Kwajalein" -> Set(8632, 8633, 8634),
      "Pacific/Apia" -> Set(15338),
      "Pacific/Enderbury" -> Set(9130, 9131),
      "Pacific/Fakaofo" -> Set(15338),
      "Pacific/Kiritimati" -> Set(9130, 9131),
      "Pacific/Kwajalein" -> Set(8632, 8633, 8634),
      MIT.getId -> Set(15338))
    for (zid <- ALL_TIMEZONES) {
      val skipped = skipped_days.getOrElse(zid.getId, Set.empty)
      val testingData = Seq(-20000, 20000) ++
        (1 to 1000).map(_ => (math.random() * 40000 - 20000).toInt)
      testingData.foreach { d =>
        if (!skipped.contains(d)) {
          assert(microsToDays(daysToMicros(d, zid), zid) === d,
            s"Round trip of $d did not work in tz ${zid.getId}")
        }
      }
    }
  }

  test("microsToMillis") {
    assert(DateTimeUtils.microsToMillis(-9223372036844776001L) === -9223372036844777L)
    assert(DateTimeUtils.microsToMillis(-157700927876544L) === -157700927877L)
  }

  test("SPARK-29012: special timestamp values") {
    testSpecialDatetimeValues { zoneId =>
      val tolerance = TimeUnit.SECONDS.toMicros(30)

      assert(convertSpecialTimestamp("Epoch", zoneId).get === 0)
      val now = instantToMicros(Instant.now())
      convertSpecialTimestamp("NOW", zoneId).get should be(now +- tolerance)
      assert(convertSpecialTimestamp("now UTC", zoneId) === None)
      val localToday = LocalDateTime.now(zoneId)
        .`with`(LocalTime.MIDNIGHT)
        .atZone(zoneId)
      val yesterday = instantToMicros(localToday.minusDays(1).toInstant)
      convertSpecialTimestamp(" Yesterday", zoneId).get should be(yesterday +- tolerance)
      val today = instantToMicros(localToday.toInstant)
      convertSpecialTimestamp("Today ", zoneId).get should be(today +- tolerance)
      val tomorrow = instantToMicros(localToday.plusDays(1).toInstant)
      convertSpecialTimestamp(" tomorrow CET ", zoneId).get should be(tomorrow +- tolerance)
    }
  }

  test("SPARK-35979: special timestamp without time zone values") {
    val tolerance = TimeUnit.SECONDS.toMicros(30)

    assert(convertSpecialTimestampNTZ("Epoch").get === 0)
    val now = DateTimeUtils.localDateTimeToMicros(LocalDateTime.now())
    convertSpecialTimestampNTZ("NOW").get should be(now +- tolerance)
    val localToday = LocalDateTime.now().`with`(LocalTime.MIDNIGHT)
    val yesterday = DateTimeUtils.localDateTimeToMicros(localToday.minusDays(1))
    convertSpecialTimestampNTZ(" Yesterday").get should be(yesterday)
    val today = DateTimeUtils.localDateTimeToMicros(localToday)
    convertSpecialTimestampNTZ("Today ").get should be(today)
    val tomorrow = DateTimeUtils.localDateTimeToMicros(localToday.plusDays(1))
    convertSpecialTimestampNTZ(" tomorrow ").get should be(tomorrow)
  }

  test("SPARK-28141: special date values") {
    testSpecialDatetimeValues { zoneId =>
      assert(convertSpecialDate("epoch", zoneId).get === 0)
      val today = localDateToDays(LocalDate.now(zoneId))
      assert(convertSpecialDate("YESTERDAY", zoneId).get === today - 1)
      assert(convertSpecialDate(" Now ", zoneId).get === today)
      assert(convertSpecialDate("now UTC", zoneId) === None) // "now" does not accept time zones
      assert(convertSpecialDate("today", zoneId).get === today)
      assert(convertSpecialDate("tomorrow CET ", zoneId).get === today + 1)
    }
  }

  test("parsing day of week") {
    assert(getDayOfWeekFromString(UTF8String.fromString("THU")) == 0)
    assert(getDayOfWeekFromString(UTF8String.fromString("MONDAY")) == 4)
    intercept[IllegalArgumentException](getDayOfWeekFromString(UTF8String.fromString("xx")))
    intercept[IllegalArgumentException](getDayOfWeekFromString(UTF8String.fromString("\"quote")))
  }

  test("SPARK-34761: timestamp add day-time interval") {
    // transit from Pacific Standard Time to Pacific Daylight Time
    assert(timestampAddDayTime(
      // 2019-3-9 is the end of Pacific Standard Time
      date(2019, 3, 9, 12, 0, 0, 123000, LA),
      MICROS_PER_DAY, LA) ===
      // 2019-3-10 is the start of Pacific Daylight Time
      date(2019, 3, 10, 12, 0, 0, 123000, LA))
    // just normal days
    outstandingZoneIds.foreach { zid =>
      assert(timestampAddDayTime(
        date(2021, 3, 18, 19, 44, 1, 100000, zid), 0, zid) ===
        date(2021, 3, 18, 19, 44, 1, 100000, zid))
      assert(timestampAddDayTime(
        date(2021, 1, 19, 0, 0, 0, 0, zid), -18 * MICROS_PER_DAY, zid) ===
        date(2021, 1, 1, 0, 0, 0, 0, zid))
      assert(timestampAddDayTime(
        date(2021, 3, 18, 19, 44, 1, 999999, zid), 10 * MICROS_PER_MINUTE, zid) ===
        date(2021, 3, 18, 19, 54, 1, 999999, zid))
      assert(timestampAddDayTime(
        date(2021, 3, 18, 19, 44, 1, 1, zid), -MICROS_PER_DAY - 1, zid) ===
        date(2021, 3, 17, 19, 44, 1, 0, zid))
      assert(timestampAddDayTime(
        date(2019, 5, 9, 12, 0, 0, 123456, zid), 2 * MICROS_PER_DAY + 1, zid) ===
        date(2019, 5, 11, 12, 0, 0, 123457, zid))
    }
    // transit from Pacific Daylight Time to Pacific Standard Time
    assert(timestampAddDayTime(
      // 2019-11-2 is the end of Pacific Daylight Time
      date(2019, 11, 2, 12, 0, 0, 123000, LA),
      MICROS_PER_DAY, LA) ===
      // 2019-11-3 is the start of Pacific Standard Time
      date(2019, 11, 3, 12, 0, 0, 123000, LA))
  }

  test("SPARK-34903: subtract timestamps") {
    DateTimeTestUtils.outstandingZoneIds.foreach { zid =>
      Seq(
        // 1000-02-29 exists in Julian calendar because 1000 is a leap year
        (LocalDateTime.of(1000, 2, 28, 1, 2, 3, 456789000),
          LocalDateTime.of(1000, 3, 1, 1, 2, 3, 456789000)) -> TimeUnit.DAYS.toMicros(1),
        // The range 1582-10-04 .. 1582-10-15 doesn't exist in Julian calendar
        (LocalDateTime.of(1582, 10, 4, 23, 59, 59, 999999000),
          LocalDateTime.of(1582, 10, 15, 23, 59, 59, 999999000)) -> TimeUnit.DAYS.toMicros(11),
        // America/Los_Angeles -08:00 zone offset
        (LocalDateTime.of(1883, 11, 20, 0, 0, 0, 123456000),
          // America/Los_Angeles -08:00 zone offset
          LocalDateTime.of(1883, 11, 10, 0, 0, 0)) -> (TimeUnit.DAYS.toMicros(-10) - 123456),
        // No difference between Proleptic Gregorian and Julian calendars after 1900-01-01
        (LocalDateTime.of(1900, 1, 1, 0, 0, 0, 1000),
          LocalDateTime.of(1899, 12, 31, 23, 59, 59, 999999000)) -> -2,
        // The 'Asia/Hong_Kong' time zone switched from 'Japan Standard Time' (JST = UTC+9)
        // to 'Hong Kong Time' (HKT = UTC+8). After Sunday, 18 November, 1945 01:59:59 AM,
        // clocks were moved backward to become Sunday, 18 November, 1945 01:00:00 AM.
        // In this way, the overlap happened w/o Daylight Saving Time.
        (LocalDateTime.of(1945, 11, 18, 0, 30, 30),
          LocalDateTime.of(1945, 11, 18, 1, 30, 30)) -> TimeUnit.HOURS.toMicros(1),
        (LocalDateTime.of(1945, 11, 18, 2, 0, 0),
          LocalDateTime.of(1945, 11, 18, 1, 0, 0)) -> TimeUnit.HOURS.toMicros(-1),
        // The epoch has zero offset in microseconds
        (LocalDateTime.of(1970, 1, 1, 0, 0, 0), LocalDateTime.of(1970, 1, 1, 0, 0, 0)) -> 0,
        // 2020 is a leap year
        (LocalDateTime.of(2020, 2, 29, 0, 0, 0),
          LocalDateTime.of(2021, 3, 1, 0, 0, 0)) -> TimeUnit.DAYS.toMicros(366),
        // Daylight saving in America/Los_Angeles: from winter to summer time
        (LocalDateTime.of(2021, 3, 14, 1, 0, 0), LocalDateTime.of(2021, 3, 14, 3, 0, 0)) ->
          TimeUnit.HOURS.toMicros(2)
      ).foreach { case ((start, end), expected) =>
        val startMicros = DateTimeTestUtils.localDateTimeToMicros(start, zid)
        val endMicros = DateTimeTestUtils.localDateTimeToMicros(end, zid)
        val result = subtractTimestamps(endMicros, startMicros, zid)
        assert(result === expected)
      }
    }
  }

  test("SPARK-35679: instantToMicros should be able to return microseconds of Long.MinValue") {
    assert(instantToMicros(microsToInstant(Long.MaxValue)) === Long.MaxValue)
    assert(instantToMicros(microsToInstant(Long.MinValue)) === Long.MinValue)
  }
}
