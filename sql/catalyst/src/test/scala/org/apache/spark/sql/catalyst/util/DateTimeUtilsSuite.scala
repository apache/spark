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
import java.util.{Calendar, Locale, TimeZone}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.unsafe.types.UTF8String

class DateTimeUtilsSuite extends SparkFunSuite {

  val TimeZonePST = TimeZone.getTimeZone("PST")

  private[this] def getInUTCDays(timestamp: Long): Int = {
    val tz = TimeZone.getDefault
    ((timestamp + tz.getOffset(timestamp)) / MILLIS_PER_DAY).toInt
  }

  test("nanoseconds truncation") {
    def checkStringToTimestamp(originalTime: String, expectedParsedTime: String) {
      val parsedTimestampOp = DateTimeUtils.stringToTimestamp(UTF8String.fromString(originalTime))
      assert(parsedTimestampOp.isDefined, "timestamp with nanoseconds was not parsed correctly")
      assert(DateTimeUtils.timestampToString(parsedTimestampOp.get) === expectedParsedTime)
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

    var c = Calendar.getInstance()
    c.set(2015, 0, 28, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 0)
    assert(stringToDate(UTF8String.fromString("2015-01-28")).get ===
      millisToDays(c.getTimeInMillis))
    c.set(2015, 0, 1, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 0)
    assert(stringToDate(UTF8String.fromString("2015")).get ===
      millisToDays(c.getTimeInMillis))
    c.set(1, 0, 1, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 0)
    assert(stringToDate(UTF8String.fromString("0001")).get ===
      millisToDays(c.getTimeInMillis))
    c = Calendar.getInstance()
    c.set(2015, 2, 1, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 0)
    assert(stringToDate(UTF8String.fromString("2015-03")).get ===
      millisToDays(c.getTimeInMillis))
    c = Calendar.getInstance()
    c.set(2015, 2, 18, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 0)
    assert(stringToDate(UTF8String.fromString("2015-03-18")).get ===
      millisToDays(c.getTimeInMillis))
    assert(stringToDate(UTF8String.fromString("2015-03-18 ")).get ===
      millisToDays(c.getTimeInMillis))
    assert(stringToDate(UTF8String.fromString("2015-03-18 123142")).get ===
      millisToDays(c.getTimeInMillis))
    assert(stringToDate(UTF8String.fromString("2015-03-18T123123")).get ===
      millisToDays(c.getTimeInMillis))
    assert(stringToDate(UTF8String.fromString("2015-03-18T")).get ===
      millisToDays(c.getTimeInMillis))

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

  test("string to time") {
    // Tests with UTC.
    val c = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    c.set(Calendar.MILLISECOND, 0)

    c.set(1900, 0, 1, 0, 0, 0)
    assert(stringToTime("1900-01-01T00:00:00GMT-00:00") === c.getTime())

    c.set(2000, 11, 30, 10, 0, 0)
    assert(stringToTime("2000-12-30T10:00:00Z") === c.getTime())

    // Tests with set time zone.
    c.setTimeZone(TimeZone.getTimeZone("GMT-04:00"))
    c.set(Calendar.MILLISECOND, 0)

    c.set(1900, 0, 1, 0, 0, 0)
    assert(stringToTime("1900-01-01T00:00:00-04:00") === c.getTime())

    c.set(1900, 0, 1, 0, 0, 0)
    assert(stringToTime("1900-01-01T00:00:00GMT-04:00") === c.getTime())

    // Tests with local time zone.
    c.setTimeZone(TimeZone.getDefault())
    c.set(Calendar.MILLISECOND, 0)

    c.set(2000, 11, 30, 0, 0, 0)
    assert(stringToTime("2000-12-30") === new Date(c.getTimeInMillis()))

    c.set(2000, 11, 30, 10, 0, 0)
    assert(stringToTime("2000-12-30 10:00:00") === new Timestamp(c.getTimeInMillis()))
  }

  test("string to timestamp") {
    for (tz <- DateTimeTestUtils.ALL_TIMEZONES) {
      def checkStringToTimestamp(str: String, expected: Option[Long]): Unit = {
        assert(stringToTimestamp(UTF8String.fromString(str), tz) === expected)
      }

      var c = Calendar.getInstance(tz)
      c.set(1969, 11, 31, 16, 0, 0)
      c.set(Calendar.MILLISECOND, 0)
      checkStringToTimestamp("1969-12-31 16:00:00", Option(c.getTimeInMillis * 1000))
      c.set(1, 0, 1, 0, 0, 0)
      c.set(Calendar.MILLISECOND, 0)
      checkStringToTimestamp("0001", Option(c.getTimeInMillis * 1000))
      c = Calendar.getInstance(tz)
      c.set(2015, 2, 1, 0, 0, 0)
      c.set(Calendar.MILLISECOND, 0)
      checkStringToTimestamp("2015-03", Option(c.getTimeInMillis * 1000))
      c = Calendar.getInstance(tz)
      c.set(2015, 2, 18, 0, 0, 0)
      c.set(Calendar.MILLISECOND, 0)
      checkStringToTimestamp("2015-03-18", Option(c.getTimeInMillis * 1000))
      checkStringToTimestamp("2015-03-18 ", Option(c.getTimeInMillis * 1000))
      checkStringToTimestamp("2015-03-18T", Option(c.getTimeInMillis * 1000))

      c = Calendar.getInstance(tz)
      c.set(2015, 2, 18, 12, 3, 17)
      c.set(Calendar.MILLISECOND, 0)
      checkStringToTimestamp("2015-03-18 12:03:17", Option(c.getTimeInMillis * 1000))
      checkStringToTimestamp("2015-03-18T12:03:17", Option(c.getTimeInMillis * 1000))

      // If the string value includes timezone string, it represents the timestamp string
      // in the timezone regardless of the tz parameter.
      c = Calendar.getInstance(TimeZone.getTimeZone("GMT-13:53"))
      c.set(2015, 2, 18, 12, 3, 17)
      c.set(Calendar.MILLISECOND, 0)
      checkStringToTimestamp("2015-03-18T12:03:17-13:53", Option(c.getTimeInMillis * 1000))

      c = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
      c.set(2015, 2, 18, 12, 3, 17)
      c.set(Calendar.MILLISECOND, 0)
      checkStringToTimestamp("2015-03-18T12:03:17Z", Option(c.getTimeInMillis * 1000))
      checkStringToTimestamp("2015-03-18 12:03:17Z", Option(c.getTimeInMillis * 1000))

      c = Calendar.getInstance(TimeZone.getTimeZone("GMT-01:00"))
      c.set(2015, 2, 18, 12, 3, 17)
      c.set(Calendar.MILLISECOND, 0)
      checkStringToTimestamp("2015-03-18T12:03:17-1:0", Option(c.getTimeInMillis * 1000))
      checkStringToTimestamp("2015-03-18T12:03:17-01:00", Option(c.getTimeInMillis * 1000))

      c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:30"))
      c.set(2015, 2, 18, 12, 3, 17)
      c.set(Calendar.MILLISECOND, 0)
      checkStringToTimestamp("2015-03-18T12:03:17+07:30", Option(c.getTimeInMillis * 1000))

      c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:03"))
      c.set(2015, 2, 18, 12, 3, 17)
      c.set(Calendar.MILLISECOND, 0)
      checkStringToTimestamp("2015-03-18T12:03:17+07:03", Option(c.getTimeInMillis * 1000))

      // tests for the string including milliseconds.
      c = Calendar.getInstance(tz)
      c.set(2015, 2, 18, 12, 3, 17)
      c.set(Calendar.MILLISECOND, 123)
      checkStringToTimestamp("2015-03-18 12:03:17.123", Option(c.getTimeInMillis * 1000))
      checkStringToTimestamp("2015-03-18T12:03:17.123", Option(c.getTimeInMillis * 1000))

      // If the string value includes timezone string, it represents the timestamp string
      // in the timezone regardless of the tz parameter.
      c = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
      c.set(2015, 2, 18, 12, 3, 17)
      c.set(Calendar.MILLISECOND, 456)
      checkStringToTimestamp("2015-03-18T12:03:17.456Z", Option(c.getTimeInMillis * 1000))
      checkStringToTimestamp("2015-03-18 12:03:17.456Z", Option(c.getTimeInMillis * 1000))

      c = Calendar.getInstance(TimeZone.getTimeZone("GMT-01:00"))
      c.set(2015, 2, 18, 12, 3, 17)
      c.set(Calendar.MILLISECOND, 123)
      checkStringToTimestamp("2015-03-18T12:03:17.123-1:0", Option(c.getTimeInMillis * 1000))
      checkStringToTimestamp("2015-03-18T12:03:17.123-01:00", Option(c.getTimeInMillis * 1000))

      c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:30"))
      c.set(2015, 2, 18, 12, 3, 17)
      c.set(Calendar.MILLISECOND, 123)
      checkStringToTimestamp("2015-03-18T12:03:17.123+07:30", Option(c.getTimeInMillis * 1000))

      c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:30"))
      c.set(2015, 2, 18, 12, 3, 17)
      c.set(Calendar.MILLISECOND, 123)
      checkStringToTimestamp("2015-03-18T12:03:17.123+07:30", Option(c.getTimeInMillis * 1000))

      c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:30"))
      c.set(2015, 2, 18, 12, 3, 17)
      c.set(Calendar.MILLISECOND, 123)
      checkStringToTimestamp(
        "2015-03-18T12:03:17.123121+7:30", Option(c.getTimeInMillis * 1000 + 121))

      c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:30"))
      c.set(2015, 2, 18, 12, 3, 17)
      c.set(Calendar.MILLISECOND, 123)
      checkStringToTimestamp(
        "2015-03-18T12:03:17.12312+7:30", Option(c.getTimeInMillis * 1000 + 120))

      c = Calendar.getInstance(tz)
      c.set(Calendar.HOUR_OF_DAY, 18)
      c.set(Calendar.MINUTE, 12)
      c.set(Calendar.SECOND, 15)
      c.set(Calendar.MILLISECOND, 0)
      checkStringToTimestamp("18:12:15", Option(c.getTimeInMillis * 1000))

      c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:30"))
      c.set(Calendar.HOUR_OF_DAY, 18)
      c.set(Calendar.MINUTE, 12)
      c.set(Calendar.SECOND, 15)
      c.set(Calendar.MILLISECOND, 123)
      checkStringToTimestamp("T18:12:15.12312+7:30", Option(c.getTimeInMillis * 1000 + 120))

      c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:30"))
      c.set(Calendar.HOUR_OF_DAY, 18)
      c.set(Calendar.MINUTE, 12)
      c.set(Calendar.SECOND, 15)
      c.set(Calendar.MILLISECOND, 123)
      checkStringToTimestamp("18:12:15.12312+7:30", Option(c.getTimeInMillis * 1000 + 120))

      c = Calendar.getInstance(tz)
      c.set(2011, 4, 6, 7, 8, 9)
      c.set(Calendar.MILLISECOND, 100)
      checkStringToTimestamp("2011-05-06 07:08:09.1000", Option(c.getTimeInMillis * 1000))

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
      c = Calendar.getInstance(TimeZone.getTimeZone("GMT+00:00"))
      c.set(2015, 2, 18, 12, 3, 17)
      c.set(Calendar.MILLISECOND, 0)
      checkStringToTimestamp(
        "2015-03-18T12:03:17.123456789+0:00", Option(c.getTimeInMillis * 1000 + 123456))
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
      UTF8String.fromString("2015-02-29 00:00:00")).isEmpty)
    assert(stringToTimestamp(
      UTF8String.fromString("2015-04-31 00:00:00")).isEmpty)
    assert(stringToTimestamp(UTF8String.fromString("2015-02-29")).isEmpty)
    assert(stringToTimestamp(UTF8String.fromString("2015-04-31")).isEmpty)
  }

  test("hours") {
    val c = Calendar.getInstance(TimeZonePST)
    c.set(2015, 2, 18, 13, 2, 11)
    assert(getHours(c.getTimeInMillis * 1000, TimeZonePST) === 13)
    assert(getHours(c.getTimeInMillis * 1000, TimeZoneGMT) === 20)
    c.set(2015, 12, 8, 2, 7, 9)
    assert(getHours(c.getTimeInMillis * 1000, TimeZonePST) === 2)
    assert(getHours(c.getTimeInMillis * 1000, TimeZoneGMT) === 10)
  }

  test("minutes") {
    val c = Calendar.getInstance(TimeZonePST)
    c.set(2015, 2, 18, 13, 2, 11)
    assert(getMinutes(c.getTimeInMillis * 1000, TimeZonePST) === 2)
    assert(getMinutes(c.getTimeInMillis * 1000, TimeZoneGMT) === 2)
    assert(getMinutes(c.getTimeInMillis * 1000, TimeZone.getTimeZone("Australia/North")) === 32)
    c.set(2015, 2, 8, 2, 7, 9)
    assert(getMinutes(c.getTimeInMillis * 1000, TimeZonePST) === 7)
    assert(getMinutes(c.getTimeInMillis * 1000, TimeZoneGMT) === 7)
    assert(getMinutes(c.getTimeInMillis * 1000, TimeZone.getTimeZone("Australia/North")) === 37)
  }

  test("seconds") {
    val c = Calendar.getInstance(TimeZonePST)
    c.set(2015, 2, 18, 13, 2, 11)
    assert(getSeconds(c.getTimeInMillis * 1000, TimeZonePST) === 11)
    assert(getSeconds(c.getTimeInMillis * 1000, TimeZoneGMT) === 11)
    c.set(2015, 2, 8, 2, 7, 9)
    assert(getSeconds(c.getTimeInMillis * 1000, TimeZonePST) === 9)
    assert(getSeconds(c.getTimeInMillis * 1000, TimeZoneGMT) === 9)
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
    val c = Calendar.getInstance()
    c.set(2015, 2, 18, 0, 0, 0)
    assert(getDayInYear(getInUTCDays(c.getTimeInMillis)) === 77)
    c.set(2012, 2, 18, 0, 0, 0)
    assert(getDayInYear(getInUTCDays(c.getTimeInMillis)) === 78)
  }

  test("get year") {
    val c = Calendar.getInstance()
    c.set(2015, 2, 18, 0, 0, 0)
    assert(getYear(getInUTCDays(c.getTimeInMillis)) === 2015)
    c.set(2012, 2, 18, 0, 0, 0)
    assert(getYear(getInUTCDays(c.getTimeInMillis)) === 2012)
  }

  test("get quarter") {
    val c = Calendar.getInstance()
    c.set(2015, 2, 18, 0, 0, 0)
    assert(getQuarter(getInUTCDays(c.getTimeInMillis)) === 1)
    c.set(2012, 11, 18, 0, 0, 0)
    assert(getQuarter(getInUTCDays(c.getTimeInMillis)) === 4)
  }

  test("get month") {
    val c = Calendar.getInstance()
    c.set(2015, 2, 18, 0, 0, 0)
    assert(getMonth(getInUTCDays(c.getTimeInMillis)) === 3)
    c.set(2012, 11, 18, 0, 0, 0)
    assert(getMonth(getInUTCDays(c.getTimeInMillis)) === 12)
  }

  test("get day of month") {
    val c = Calendar.getInstance()
    c.set(2015, 2, 18, 0, 0, 0)
    assert(getDayOfMonth(getInUTCDays(c.getTimeInMillis)) === 18)
    c.set(2012, 11, 24, 0, 0, 0)
    assert(getDayOfMonth(getInUTCDays(c.getTimeInMillis)) === 24)
  }

  test("date add months") {
    val c1 = Calendar.getInstance()
    c1.set(1997, 1, 28, 10, 30, 0)
    val days1 = millisToDays(c1.getTimeInMillis)
    val c2 = Calendar.getInstance()
    c2.set(2000, 1, 29)
    assert(dateAddMonths(days1, 36) === millisToDays(c2.getTimeInMillis))
    c2.set(1996, 0, 31)
    assert(dateAddMonths(days1, -13) === millisToDays(c2.getTimeInMillis))
  }

  test("timestamp add months") {
    val c1 = Calendar.getInstance()
    c1.set(1997, 1, 28, 10, 30, 0)
    c1.set(Calendar.MILLISECOND, 0)
    val ts1 = c1.getTimeInMillis * 1000L
    val c2 = Calendar.getInstance()
    c2.set(2000, 1, 29, 10, 30, 0)
    c2.set(Calendar.MILLISECOND, 123)
    val ts2 = c2.getTimeInMillis * 1000L
    assert(timestampAddInterval(ts1, 36, 123000) === ts2)

    val c3 = Calendar.getInstance(TimeZonePST)
    c3.set(1997, 1, 27, 16, 0, 0)
    c3.set(Calendar.MILLISECOND, 0)
    val ts3 = c3.getTimeInMillis * 1000L
    val c4 = Calendar.getInstance(TimeZonePST)
    c4.set(2000, 1, 27, 16, 0, 0)
    c4.set(Calendar.MILLISECOND, 123)
    val ts4 = c4.getTimeInMillis * 1000L
    val c5 = Calendar.getInstance(TimeZoneGMT)
    c5.set(2000, 1, 29, 0, 0, 0)
    c5.set(Calendar.MILLISECOND, 123)
    val ts5 = c5.getTimeInMillis * 1000L
    assert(timestampAddInterval(ts3, 36, 123000, TimeZonePST) === ts4)
    assert(timestampAddInterval(ts3, 36, 123000, TimeZoneGMT) === ts5)
  }

  test("monthsBetween") {
    val c1 = Calendar.getInstance()
    c1.set(1997, 1, 28, 10, 30, 0)
    val c2 = Calendar.getInstance()
    c2.set(1996, 9, 30, 0, 0, 0)
    assert(monthsBetween(c1.getTimeInMillis * 1000L, c2.getTimeInMillis * 1000L) === 3.94959677)
    c2.set(2000, 1, 28, 0, 0, 0)
    assert(monthsBetween(c1.getTimeInMillis * 1000L, c2.getTimeInMillis * 1000L) === -36)
    c2.set(2000, 1, 29, 0, 0, 0)
    assert(monthsBetween(c1.getTimeInMillis * 1000L, c2.getTimeInMillis * 1000L) === -36)
    c2.set(1996, 2, 31, 0, 0, 0)
    assert(monthsBetween(c1.getTimeInMillis * 1000L, c2.getTimeInMillis * 1000L) === 11)

    val c3 = Calendar.getInstance(TimeZonePST)
    c3.set(2000, 1, 28, 16, 0, 0)
    val c4 = Calendar.getInstance(TimeZonePST)
    c4.set(1997, 1, 28, 16, 0, 0)
    assert(
      monthsBetween(c3.getTimeInMillis * 1000L, c4.getTimeInMillis * 1000L, TimeZonePST)
      === 36.0)
    assert(
      monthsBetween(c3.getTimeInMillis * 1000L, c4.getTimeInMillis * 1000L, TimeZoneGMT)
      === 35.90322581)
  }

  test("from UTC timestamp") {
    def test(utc: String, tz: String, expected: String): Unit = {
      assert(toJavaTimestamp(fromUTCTime(fromJavaTimestamp(Timestamp.valueOf(utc)), tz)).toString
        === expected)
    }
    for (tz <- DateTimeTestUtils.ALL_TIMEZONES) {
      DateTimeTestUtils.withDefaultTimeZone(tz) {
        test("2011-12-25 09:00:00.123456", "UTC", "2011-12-25 09:00:00.123456")
        test("2011-12-25 09:00:00.123456", "JST", "2011-12-25 18:00:00.123456")
        test("2011-12-25 09:00:00.123456", "PST", "2011-12-25 01:00:00.123456")
        test("2011-12-25 09:00:00.123456", "Asia/Shanghai", "2011-12-25 17:00:00.123456")
      }
    }

    DateTimeTestUtils.withDefaultTimeZone(TimeZone.getTimeZone("PST")) {
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

    for (tz <- DateTimeTestUtils.ALL_TIMEZONES) {
      DateTimeTestUtils.withDefaultTimeZone(tz) {
        test("2011-12-25 09:00:00.123456", "UTC", "2011-12-25 09:00:00.123456")
        test("2011-12-25 18:00:00.123456", "JST", "2011-12-25 09:00:00.123456")
        test("2011-12-25 01:00:00.123456", "PST", "2011-12-25 09:00:00.123456")
        test("2011-12-25 17:00:00.123456", "Asia/Shanghai", "2011-12-25 09:00:00.123456")
      }
    }

    DateTimeTestUtils.withDefaultTimeZone(TimeZone.getTimeZone("PST")) {
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

  test("daysToMillis and millisToDays") {
    val c = Calendar.getInstance(TimeZonePST)

    c.set(2015, 11, 31, 16, 0, 0)
    assert(millisToDays(c.getTimeInMillis, TimeZonePST) === 16800)
    assert(millisToDays(c.getTimeInMillis, TimeZoneGMT) === 16801)

    c.set(2015, 11, 31, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 0)
    assert(daysToMillis(16800, TimeZonePST) === c.getTimeInMillis)

    c.setTimeZone(TimeZoneGMT)
    c.set(2015, 11, 31, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 0)
    assert(daysToMillis(16800, TimeZoneGMT) === c.getTimeInMillis)

    // There are some days are skipped entirely in some timezone, skip them here.
    val skipped_days = Map[String, Set[Int]](
      "Kwajalein" -> Set(8632),
      "Pacific/Apia" -> Set(15338),
      "Pacific/Enderbury" -> Set(9130, 9131),
      "Pacific/Fakaofo" -> Set(15338),
      "Pacific/Kiritimati" -> Set(9130, 9131),
      "Pacific/Kwajalein" -> Set(8632),
      "MIT" -> Set(15338))
    for (tz <- DateTimeTestUtils.ALL_TIMEZONES) {
      val skipped = skipped_days.getOrElse(tz.getID, Set.empty)
      (-20000 to 20000).foreach { d =>
        if (!skipped.contains(d)) {
          assert(millisToDays(daysToMillis(d, tz), tz) === d,
            s"Round trip of ${d} did not work in tz ${tz}")
        }
      }
    }
  }
}
