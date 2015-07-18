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
import java.util.{TimeZone, Calendar}

import org.apache.spark.SparkFunSuite
import org.apache.spark.unsafe.types.UTF8String

class DateTimeUtilsSuite extends SparkFunSuite {

  test("timestamp and us") {
    val now = new Timestamp(System.currentTimeMillis())
    now.setNanos(1000)
    val ns = DateTimeUtils.fromJavaTimestamp(now)
    assert(ns % 1000000L === 1)
    assert(DateTimeUtils.toJavaTimestamp(ns) === now)

    List(-111111111111L, -1L, 0, 1L, 111111111111L).foreach { t =>
      val ts = DateTimeUtils.toJavaTimestamp(t)
      assert(DateTimeUtils.fromJavaTimestamp(ts) === t)
      assert(DateTimeUtils.toJavaTimestamp(DateTimeUtils.fromJavaTimestamp(ts)) === ts)
    }
  }

  test("us and julian day") {
    val (d, ns) = DateTimeUtils.toJulianDay(0)
    assert(d === DateTimeUtils.JULIAN_DAY_OF_EPOCH)
    assert(ns === DateTimeUtils.SECONDS_PER_DAY / 2 * DateTimeUtils.NANOS_PER_SECOND)
    assert(DateTimeUtils.fromJulianDay(d, ns) == 0L)

    val t = new Timestamp(61394778610000L) // (2015, 6, 11, 10, 10, 10, 100)
    val (d1, ns1) = DateTimeUtils.toJulianDay(DateTimeUtils.fromJavaTimestamp(t))
    val t2 = DateTimeUtils.toJavaTimestamp(DateTimeUtils.fromJulianDay(d1, ns1))
    assert(t.equals(t2))
  }

  test("SPARK-6785: java date conversion before and after epoch") {
    def checkFromToJavaDate(d1: Date): Unit = {
      val d2 = DateTimeUtils.toJavaDate(DateTimeUtils.fromJavaDate(d1))
      assert(d2.toString === d1.toString)
    }

    val df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z")

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
    import DateTimeUtils.millisToDays

    var c = Calendar.getInstance()
    c.set(2015, 0, 28, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 0)
    assert(DateTimeUtils.stringToDate(UTF8String.fromString("2015-01-28")).get ===
      millisToDays(c.getTimeInMillis))
    c.set(2015, 0, 1, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 0)
    assert(DateTimeUtils.stringToDate(UTF8String.fromString("2015")).get ===
      millisToDays(c.getTimeInMillis))
    c = Calendar.getInstance()
    c.set(2015, 2, 1, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 0)
    assert(DateTimeUtils.stringToDate(UTF8String.fromString("2015-03")).get ===
      millisToDays(c.getTimeInMillis))
    c = Calendar.getInstance()
    c.set(2015, 2, 18, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 0)
    assert(DateTimeUtils.stringToDate(UTF8String.fromString("2015-03-18")).get ===
      millisToDays(c.getTimeInMillis))
    assert(DateTimeUtils.stringToDate(UTF8String.fromString("2015-03-18 ")).get ===
      millisToDays(c.getTimeInMillis))
    assert(DateTimeUtils.stringToDate(UTF8String.fromString("2015-03-18 123142")).get ===
      millisToDays(c.getTimeInMillis))
    assert(DateTimeUtils.stringToDate(UTF8String.fromString("2015-03-18T123123")).get ===
      millisToDays(c.getTimeInMillis))
    assert(DateTimeUtils.stringToDate(UTF8String.fromString("2015-03-18T")).get ===
      millisToDays(c.getTimeInMillis))

    assert(DateTimeUtils.stringToDate(UTF8String.fromString("2015-03-18X")).isEmpty)
    assert(DateTimeUtils.stringToDate(UTF8String.fromString("2015/03/18")).isEmpty)
    assert(DateTimeUtils.stringToDate(UTF8String.fromString("2015.03.18")).isEmpty)
    assert(DateTimeUtils.stringToDate(UTF8String.fromString("20150318")).isEmpty)
    assert(DateTimeUtils.stringToDate(UTF8String.fromString("2015-031-8")).isEmpty)
  }

  test("string to timestamp") {
    var c = Calendar.getInstance()
    c.set(1969, 11, 31, 16, 0, 0)
    c.set(Calendar.MILLISECOND, 0)
    assert(DateTimeUtils.stringToTimestamp(UTF8String.fromString("1969-12-31 16:00:00")).get ===
      c.getTimeInMillis * 1000)
    c.set(2015, 0, 1, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 0)
    assert(DateTimeUtils.stringToTimestamp(UTF8String.fromString("2015")).get ===
      c.getTimeInMillis * 1000)
    c = Calendar.getInstance()
    c.set(2015, 2, 1, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 0)
    assert(DateTimeUtils.stringToTimestamp(UTF8String.fromString("2015-03")).get ===
      c.getTimeInMillis * 1000)
    c = Calendar.getInstance()
    c.set(2015, 2, 18, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 0)
    assert(DateTimeUtils.stringToTimestamp(UTF8String.fromString("2015-03-18")).get ===
      c.getTimeInMillis * 1000)
    assert(DateTimeUtils.stringToTimestamp(UTF8String.fromString("2015-03-18 ")).get ===
      c.getTimeInMillis * 1000)
    assert(DateTimeUtils.stringToTimestamp(UTF8String.fromString("2015-03-18T")).get ===
      c.getTimeInMillis * 1000)

    c = Calendar.getInstance()
    c.set(2015, 2, 18, 12, 3, 17)
    c.set(Calendar.MILLISECOND, 0)
    assert(DateTimeUtils.stringToTimestamp(UTF8String.fromString("2015-03-18 12:03:17")).get ===
      c.getTimeInMillis * 1000)
    assert(DateTimeUtils.stringToTimestamp(UTF8String.fromString("2015-03-18T12:03:17")).get ===
      c.getTimeInMillis * 1000)

    c = Calendar.getInstance(TimeZone.getTimeZone("GMT-13:53"))
    c.set(2015, 2, 18, 12, 3, 17)
    c.set(Calendar.MILLISECOND, 0)
    assert(DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-03-18T12:03:17-13:53")).get === c.getTimeInMillis * 1000)

    c = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    c.set(2015, 2, 18, 12, 3, 17)
    c.set(Calendar.MILLISECOND, 0)
    assert(DateTimeUtils.stringToTimestamp(UTF8String.fromString("2015-03-18T12:03:17Z")).get ===
      c.getTimeInMillis * 1000)
    assert(DateTimeUtils.stringToTimestamp(UTF8String.fromString("2015-03-18 12:03:17Z")).get ===
      c.getTimeInMillis * 1000)

    c = Calendar.getInstance(TimeZone.getTimeZone("GMT-01:00"))
    c.set(2015, 2, 18, 12, 3, 17)
    c.set(Calendar.MILLISECOND, 0)
    assert(DateTimeUtils.stringToTimestamp(UTF8String.fromString("2015-03-18T12:03:17-1:0")).get ===
      c.getTimeInMillis * 1000)
    assert(DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-03-18T12:03:17-01:00")).get === c.getTimeInMillis * 1000)

    c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:30"))
    c.set(2015, 2, 18, 12, 3, 17)
    c.set(Calendar.MILLISECOND, 0)
    assert(DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-03-18T12:03:17+07:30")).get === c.getTimeInMillis * 1000)

    c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:03"))
    c.set(2015, 2, 18, 12, 3, 17)
    c.set(Calendar.MILLISECOND, 0)
    assert(DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-03-18T12:03:17+07:03")).get === c.getTimeInMillis * 1000)

    c = Calendar.getInstance()
    c.set(2015, 2, 18, 12, 3, 17)
    c.set(Calendar.MILLISECOND, 123)
    assert(DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-03-18 12:03:17.123")).get === c.getTimeInMillis * 1000)
    assert(DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-03-18T12:03:17.123")).get === c.getTimeInMillis * 1000)

    c = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    c.set(2015, 2, 18, 12, 3, 17)
    c.set(Calendar.MILLISECOND, 456)
    assert(DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-03-18T12:03:17.456Z")).get  === c.getTimeInMillis * 1000)
    assert(DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-03-18 12:03:17.456Z")).get  === c.getTimeInMillis * 1000)

    c = Calendar.getInstance(TimeZone.getTimeZone("GMT-01:00"))
    c.set(2015, 2, 18, 12, 3, 17)
    c.set(Calendar.MILLISECOND, 123)
    assert(DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-03-18T12:03:17.123-1:0")).get  === c.getTimeInMillis * 1000)
    assert(DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-03-18T12:03:17.123-01:00")).get ===  c.getTimeInMillis * 1000)

    c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:30"))
    c.set(2015, 2, 18, 12, 3, 17)
    c.set(Calendar.MILLISECOND, 123)
    assert(DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-03-18T12:03:17.123+07:30")).get ===  c.getTimeInMillis * 1000)

    c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:30"))
    c.set(2015, 2, 18, 12, 3, 17)
    c.set(Calendar.MILLISECOND, 123)
    assert(DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-03-18T12:03:17.123+07:30")).get === c.getTimeInMillis * 1000)

    c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:30"))
    c.set(2015, 2, 18, 12, 3, 17)
    c.set(Calendar.MILLISECOND, 123)
    assert(DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-03-18T12:03:17.123121+7:30")).get ===
        c.getTimeInMillis * 1000 + 121)

    c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:30"))
    c.set(2015, 2, 18, 12, 3, 17)
    c.set(Calendar.MILLISECOND, 123)
    assert(DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-03-18T12:03:17.12312+7:30")).get ===
        c.getTimeInMillis * 1000 + 120)

    c = Calendar.getInstance()
    c.set(Calendar.HOUR_OF_DAY, 18)
    c.set(Calendar.MINUTE, 12)
    c.set(Calendar.SECOND, 15)
    c.set(Calendar.MILLISECOND, 0)
    assert(DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("18:12:15")).get ===
      c.getTimeInMillis * 1000)

    c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:30"))
    c.set(Calendar.HOUR_OF_DAY, 18)
    c.set(Calendar.MINUTE, 12)
    c.set(Calendar.SECOND, 15)
    c.set(Calendar.MILLISECOND, 123)
    assert(DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("T18:12:15.12312+7:30")).get ===
      c.getTimeInMillis * 1000 + 120)

    c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:30"))
    c.set(Calendar.HOUR_OF_DAY, 18)
    c.set(Calendar.MINUTE, 12)
    c.set(Calendar.SECOND, 15)
    c.set(Calendar.MILLISECOND, 123)
    assert(DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("18:12:15.12312+7:30")).get ===
      c.getTimeInMillis * 1000 + 120)

    c = Calendar.getInstance()
    c.set(2011, 4, 6, 7, 8, 9)
    c.set(Calendar.MILLISECOND, 100)
    assert(DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2011-05-06 07:08:09.1000")).get === c.getTimeInMillis * 1000)

    val defaultTimeZone = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))

    c = Calendar.getInstance()
    c.set(2015, 2, 8, 2, 0, 0)
    c.set(Calendar.MILLISECOND, 0)
    assert(DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-3-8 2:0:0")).get === c.getTimeInMillis * 1000)
    c.add(Calendar.MINUTE, 30)
    assert(DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-3-8 3:30:0")).get === c.getTimeInMillis * 1000)
    assert(DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-3-8 2:30:0")).get === c.getTimeInMillis * 1000)

    c = Calendar.getInstance()
    c.set(2015, 10, 1, 1, 59, 0)
    c.set(Calendar.MILLISECOND, 0)
    c.add(Calendar.MINUTE, 31)
    assert(DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-11-1 2:30:0")).get === c.getTimeInMillis * 1000)
    TimeZone.setDefault(defaultTimeZone)

    assert(DateTimeUtils.stringToTimestamp(UTF8String.fromString("238")).isEmpty)
    assert(DateTimeUtils.stringToTimestamp(UTF8String.fromString("2015-03-18 123142")).isEmpty)
    assert(DateTimeUtils.stringToTimestamp(UTF8String.fromString("2015-03-18T123123")).isEmpty)
    assert(DateTimeUtils.stringToTimestamp(UTF8String.fromString("2015-03-18X")).isEmpty)
    assert(DateTimeUtils.stringToTimestamp(UTF8String.fromString("2015/03/18")).isEmpty)
    assert(DateTimeUtils.stringToTimestamp(UTF8String.fromString("2015.03.18")).isEmpty)
    assert(DateTimeUtils.stringToTimestamp(UTF8String.fromString("20150318")).isEmpty)
    assert(DateTimeUtils.stringToTimestamp(UTF8String.fromString("2015-031-8")).isEmpty)
    assert(DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-03-18T12:03.17-20:0")).isEmpty)
    assert(DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-03-18T12:03.17-0:70")).isEmpty)
    assert(DateTimeUtils.stringToTimestamp(
      UTF8String.fromString("2015-03-18T12:03.17-1:0:0")).isEmpty)
  }
}
