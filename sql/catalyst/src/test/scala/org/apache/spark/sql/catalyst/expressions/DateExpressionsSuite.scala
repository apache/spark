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

package org.apache.spark.sql.catalyst.expressions

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Calendar, Locale, TimeZone}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.TimeZoneGMT
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

class DateExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  import IntegralLiteralTestUtils._

  val TimeZonePST = TimeZone.getTimeZone("PST")
  val TimeZoneJST = TimeZone.getTimeZone("JST")

  val gmtId = Option(TimeZoneGMT.getID)
  val pstId = Option(TimeZonePST.getID)
  val jstId = Option(TimeZoneJST.getID)

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
  sdf.setTimeZone(TimeZoneGMT)
  val sdfDate = new SimpleDateFormat("yyyy-MM-dd", Locale.US)
  sdfDate.setTimeZone(TimeZoneGMT)
  val d = new Date(sdf.parse("2015-04-08 13:10:15").getTime)
  val ts = new Timestamp(sdf.parse("2013-11-08 13:10:15").getTime)

  test("datetime function current_date") {
    val d0 = DateTimeUtils.millisToDays(System.currentTimeMillis(), TimeZoneGMT)
    val cd = CurrentDate(gmtId).eval(EmptyRow).asInstanceOf[Int]
    val d1 = DateTimeUtils.millisToDays(System.currentTimeMillis(), TimeZoneGMT)
    assert(d0 <= cd && cd <= d1 && d1 - d0 <= 1)

    val cdjst = CurrentDate(jstId).eval(EmptyRow).asInstanceOf[Int]
    val cdpst = CurrentDate(pstId).eval(EmptyRow).asInstanceOf[Int]
    assert(cdpst <= cd && cd <= cdjst)
  }

  test("datetime function current_timestamp") {
    val ct = DateTimeUtils.toJavaTimestamp(CurrentTimestamp().eval(EmptyRow).asInstanceOf[Long])
    val t1 = System.currentTimeMillis()
    assert(math.abs(t1 - ct.getTime) < 5000)
  }

  test("DayOfYear") {
    val sdfDay = new SimpleDateFormat("D", Locale.US)

    val c = Calendar.getInstance()
    (0 to 3).foreach { m =>
      (0 to 5).foreach { i =>
        c.set(2000, m, 28, 0, 0, 0)
        c.add(Calendar.DATE, i)
        checkEvaluation(DayOfYear(Literal(new Date(c.getTimeInMillis))),
          sdfDay.format(c.getTime).toInt)
      }
    }
    checkEvaluation(DayOfYear(Literal.create(null, DateType)), null)

    checkEvaluation(DayOfYear(Literal(new Date(sdf.parse("1582-10-15 13:10:15").getTime))), 288)
    checkEvaluation(DayOfYear(Literal(new Date(sdf.parse("1582-10-04 13:10:15").getTime))), 277)
    checkConsistencyBetweenInterpretedAndCodegen(DayOfYear, DateType)
  }

  test("Year") {
    checkEvaluation(Year(Literal.create(null, DateType)), null)
    checkEvaluation(Year(Literal(d)), 2015)
    checkEvaluation(Year(Cast(Literal(sdfDate.format(d)), DateType, gmtId)), 2015)
    checkEvaluation(Year(Cast(Literal(ts), DateType, gmtId)), 2013)

    val c = Calendar.getInstance()
    (2000 to 2002).foreach { y =>
      (0 to 11 by 11).foreach { m =>
        c.set(y, m, 28)
        (0 to 5 * 24).foreach { i =>
          c.add(Calendar.HOUR_OF_DAY, 1)
          checkEvaluation(Year(Literal(new Date(c.getTimeInMillis))),
            c.get(Calendar.YEAR))
        }
      }
    }
    checkEvaluation(Year(Literal(new Date(sdf.parse("1582-01-01 13:10:15").getTime))), 1582)
    checkEvaluation(Year(Literal(new Date(sdf.parse("1581-12-31 13:10:15").getTime))), 1581)
    checkConsistencyBetweenInterpretedAndCodegen(Year, DateType)
  }

  test("Quarter") {
    checkEvaluation(Quarter(Literal.create(null, DateType)), null)
    checkEvaluation(Quarter(Literal(d)), 2)
    checkEvaluation(Quarter(Cast(Literal(sdfDate.format(d)), DateType, gmtId)), 2)
    checkEvaluation(Quarter(Cast(Literal(ts), DateType, gmtId)), 4)

    val c = Calendar.getInstance()
    (2003 to 2004).foreach { y =>
      (0 to 11 by 3).foreach { m =>
        c.set(y, m, 28, 0, 0, 0)
        (0 to 5 * 24).foreach { i =>
          c.add(Calendar.HOUR_OF_DAY, 1)
          checkEvaluation(Quarter(Literal(new Date(c.getTimeInMillis))),
            c.get(Calendar.MONTH) / 3 + 1)
        }
      }
    }

    checkEvaluation(Quarter(Literal(new Date(sdf.parse("1582-10-01 13:10:15").getTime))), 4)
    checkEvaluation(Quarter(Literal(new Date(sdf.parse("1582-09-30 13:10:15").getTime))), 3)
    checkConsistencyBetweenInterpretedAndCodegen(Quarter, DateType)
  }

  test("Month") {
    checkEvaluation(Month(Literal.create(null, DateType)), null)
    checkEvaluation(Month(Literal(d)), 4)
    checkEvaluation(Month(Cast(Literal(sdfDate.format(d)), DateType, gmtId)), 4)
    checkEvaluation(Month(Cast(Literal(ts), DateType, gmtId)), 11)

    checkEvaluation(Month(Literal(new Date(sdf.parse("1582-04-28 13:10:15").getTime))), 4)
    checkEvaluation(Month(Literal(new Date(sdf.parse("1582-10-04 13:10:15").getTime))), 10)
    checkEvaluation(Month(Literal(new Date(sdf.parse("1582-10-15 13:10:15").getTime))), 10)

    val c = Calendar.getInstance()
    (2003 to 2004).foreach { y =>
      (0 to 3).foreach { m =>
        (0 to 2 * 24).foreach { i =>
          c.set(y, m, 28, 0, 0, 0)
          c.add(Calendar.HOUR_OF_DAY, i)
          checkEvaluation(Month(Literal(new Date(c.getTimeInMillis))),
            c.get(Calendar.MONTH) + 1)
        }
      }
    }
    checkConsistencyBetweenInterpretedAndCodegen(Month, DateType)
  }

  test("Day / DayOfMonth") {
    checkEvaluation(DayOfMonth(Cast(Literal("2000-02-29"), DateType)), 29)
    checkEvaluation(DayOfMonth(Literal.create(null, DateType)), null)
    checkEvaluation(DayOfMonth(Literal(d)), 8)
    checkEvaluation(DayOfMonth(Cast(Literal(sdfDate.format(d)), DateType, gmtId)), 8)
    checkEvaluation(DayOfMonth(Cast(Literal(ts), DateType, gmtId)), 8)

    checkEvaluation(DayOfMonth(Literal(new Date(sdf.parse("1582-04-28 13:10:15").getTime))), 28)
    checkEvaluation(DayOfMonth(Literal(new Date(sdf.parse("1582-10-15 13:10:15").getTime))), 15)
    checkEvaluation(DayOfMonth(Literal(new Date(sdf.parse("1582-10-04 13:10:15").getTime))), 4)

    val c = Calendar.getInstance()
    (1999 to 2000).foreach { y =>
      c.set(y, 0, 1, 0, 0, 0)
      (0 to 365).foreach { d =>
        c.add(Calendar.DATE, 1)
        checkEvaluation(DayOfMonth(Literal(new Date(c.getTimeInMillis))),
          c.get(Calendar.DAY_OF_MONTH))
      }
    }
    checkConsistencyBetweenInterpretedAndCodegen(DayOfMonth, DateType)
  }

  test("Seconds") {
    assert(Second(Literal.create(null, DateType), gmtId).resolved === false)
    assert(Second(Cast(Literal(d), TimestampType, gmtId), gmtId).resolved === true)
    checkEvaluation(Second(Cast(Literal(d), TimestampType, gmtId), gmtId), 0)
    checkEvaluation(Second(Cast(Literal(sdf.format(d)), TimestampType, gmtId), gmtId), 15)
    checkEvaluation(Second(Literal(ts), gmtId), 15)

    val c = Calendar.getInstance()
    for (tz <- Seq(TimeZoneGMT, TimeZonePST, TimeZoneJST)) {
      val timeZoneId = Option(tz.getID)
      c.setTimeZone(tz)
      (0 to 60 by 5).foreach { s =>
        c.set(2015, 18, 3, 3, 5, s)
        checkEvaluation(
          Second(Literal(new Timestamp(c.getTimeInMillis)), timeZoneId),
          c.get(Calendar.SECOND))
      }
      checkConsistencyBetweenInterpretedAndCodegen(
        (child: Expression) => Second(child, timeZoneId), TimestampType)
    }
  }

  test("WeekOfYear") {
    checkEvaluation(WeekOfYear(Literal.create(null, DateType)), null)
    checkEvaluation(WeekOfYear(Literal(d)), 15)
    checkEvaluation(WeekOfYear(Cast(Literal(sdfDate.format(d)), DateType, gmtId)), 15)
    checkEvaluation(WeekOfYear(Cast(Literal(ts), DateType, gmtId)), 45)
    checkEvaluation(WeekOfYear(Cast(Literal("2011-05-06"), DateType, gmtId)), 18)
    checkEvaluation(WeekOfYear(Literal(new Date(sdf.parse("1582-10-15 13:10:15").getTime))), 40)
    checkEvaluation(WeekOfYear(Literal(new Date(sdf.parse("1582-10-04 13:10:15").getTime))), 40)
    checkConsistencyBetweenInterpretedAndCodegen(WeekOfYear, DateType)
  }

  test("DateFormat") {
    checkEvaluation(
      DateFormatClass(Literal.create(null, TimestampType), Literal("y"), gmtId),
      null)
    checkEvaluation(DateFormatClass(Cast(Literal(d), TimestampType, gmtId),
      Literal.create(null, StringType), gmtId), null)

    checkEvaluation(DateFormatClass(Cast(Literal(d), TimestampType, gmtId),
      Literal("y"), gmtId), "2015")
    checkEvaluation(DateFormatClass(Literal(ts), Literal("y"), gmtId), "2013")
    checkEvaluation(DateFormatClass(Cast(Literal(d), TimestampType, gmtId),
      Literal("H"), gmtId), "0")
    checkEvaluation(DateFormatClass(Literal(ts), Literal("H"), gmtId), "13")

    checkEvaluation(DateFormatClass(Cast(Literal(d), TimestampType, pstId),
      Literal("y"), pstId), "2015")
    checkEvaluation(DateFormatClass(Literal(ts), Literal("y"), pstId), "2013")
    checkEvaluation(DateFormatClass(Cast(Literal(d), TimestampType, pstId),
      Literal("H"), pstId), "0")
    checkEvaluation(DateFormatClass(Literal(ts), Literal("H"), pstId), "5")

    checkEvaluation(DateFormatClass(Cast(Literal(d), TimestampType, jstId),
      Literal("y"), jstId), "2015")
    checkEvaluation(DateFormatClass(Literal(ts), Literal("y"), jstId), "2013")
    checkEvaluation(DateFormatClass(Cast(Literal(d), TimestampType, jstId),
      Literal("H"), jstId), "0")
    checkEvaluation(DateFormatClass(Literal(ts), Literal("H"), jstId), "22")
  }

  test("Hour") {
    assert(Hour(Literal.create(null, DateType), gmtId).resolved === false)
    assert(Hour(Literal(ts), gmtId).resolved === true)
    checkEvaluation(Hour(Cast(Literal(d), TimestampType, gmtId), gmtId), 0)
    checkEvaluation(Hour(Cast(Literal(sdf.format(d)), TimestampType, gmtId), gmtId), 13)
    checkEvaluation(Hour(Literal(ts), gmtId), 13)

    val c = Calendar.getInstance()
    for (tz <- Seq(TimeZoneGMT, TimeZonePST, TimeZoneJST)) {
      val timeZoneId = Option(tz.getID)
      c.setTimeZone(tz)
      (0 to 24).foreach { h =>
        (0 to 60 by 15).foreach { m =>
          (0 to 60 by 15).foreach { s =>
            c.set(2015, 18, 3, h, m, s)
            checkEvaluation(
              Hour(Literal(new Timestamp(c.getTimeInMillis)), timeZoneId),
              c.get(Calendar.HOUR_OF_DAY))
          }
        }
      }
      checkConsistencyBetweenInterpretedAndCodegen(
        (child: Expression) => Hour(child, timeZoneId), TimestampType)
    }
  }

  test("Minute") {
    assert(Minute(Literal.create(null, DateType), gmtId).resolved === false)
    assert(Minute(Literal(ts), gmtId).resolved === true)
    checkEvaluation(Minute(Cast(Literal(d), TimestampType, gmtId), gmtId), 0)
    checkEvaluation(
      Minute(Cast(Literal(sdf.format(d)), TimestampType, gmtId), gmtId), 10)
    checkEvaluation(Minute(Literal(ts), gmtId), 10)

    val c = Calendar.getInstance()
    for (tz <- Seq(TimeZoneGMT, TimeZonePST, TimeZoneJST)) {
      val timeZoneId = Option(tz.getID)
      c.setTimeZone(tz)
      (0 to 60 by 5).foreach { m =>
        (0 to 60 by 15).foreach { s =>
          c.set(2015, 18, 3, 3, m, s)
          checkEvaluation(
            Minute(Literal(new Timestamp(c.getTimeInMillis)), timeZoneId),
            c.get(Calendar.MINUTE))
        }
      }
      checkConsistencyBetweenInterpretedAndCodegen(
        (child: Expression) => Minute(child, timeZoneId), TimestampType)
    }
  }

  test("date_add") {
    checkEvaluation(
      DateAdd(Literal(Date.valueOf("2016-02-28")), Literal(1)),
      DateTimeUtils.fromJavaDate(Date.valueOf("2016-02-29")))
    checkEvaluation(
      DateAdd(Literal(Date.valueOf("2016-02-28")), Literal(-365)),
      DateTimeUtils.fromJavaDate(Date.valueOf("2015-02-28")))
    checkEvaluation(DateAdd(Literal.create(null, DateType), Literal(1)), null)
    checkEvaluation(DateAdd(Literal(Date.valueOf("2016-02-28")), Literal.create(null, IntegerType)),
      null)
    checkEvaluation(DateAdd(Literal.create(null, DateType), Literal.create(null, IntegerType)),
      null)
    checkEvaluation(
      DateAdd(Literal(Date.valueOf("2016-02-28")), positiveIntLit), 49627)
    checkEvaluation(
      DateAdd(Literal(Date.valueOf("2016-02-28")), negativeIntLit), -15910)
    checkConsistencyBetweenInterpretedAndCodegen(DateAdd, DateType, IntegerType)
  }

  test("date_sub") {
    checkEvaluation(
      DateSub(Literal(Date.valueOf("2015-01-01")), Literal(1)),
      DateTimeUtils.fromJavaDate(Date.valueOf("2014-12-31")))
    checkEvaluation(
      DateSub(Literal(Date.valueOf("2015-01-01")), Literal(-1)),
      DateTimeUtils.fromJavaDate(Date.valueOf("2015-01-02")))
    checkEvaluation(DateSub(Literal.create(null, DateType), Literal(1)), null)
    checkEvaluation(DateSub(Literal(Date.valueOf("2016-02-28")), Literal.create(null, IntegerType)),
      null)
    checkEvaluation(DateSub(Literal.create(null, DateType), Literal.create(null, IntegerType)),
      null)
    checkEvaluation(
      DateSub(Literal(Date.valueOf("2016-02-28")), positiveIntLit), -15909)
    checkEvaluation(
      DateSub(Literal(Date.valueOf("2016-02-28")), negativeIntLit), 49628)
    checkConsistencyBetweenInterpretedAndCodegen(DateSub, DateType, IntegerType)
  }

  test("time_add") {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.US)
    for (tz <- Seq(TimeZoneGMT, TimeZonePST, TimeZoneJST)) {
      val timeZoneId = Option(tz.getID)
      sdf.setTimeZone(tz)

      checkEvaluation(
        TimeAdd(
          Literal(new Timestamp(sdf.parse("2016-01-29 10:00:00.000").getTime)),
          Literal(new CalendarInterval(1, 123000L)),
          timeZoneId),
        DateTimeUtils.fromJavaTimestamp(
          new Timestamp(sdf.parse("2016-02-29 10:00:00.123").getTime)))

      checkEvaluation(
        TimeAdd(
          Literal.create(null, TimestampType),
          Literal(new CalendarInterval(1, 123000L)),
          timeZoneId),
        null)
      checkEvaluation(
        TimeAdd(
          Literal(new Timestamp(sdf.parse("2016-01-29 10:00:00.000").getTime)),
          Literal.create(null, CalendarIntervalType),
          timeZoneId),
        null)
      checkEvaluation(
        TimeAdd(
          Literal.create(null, TimestampType),
          Literal.create(null, CalendarIntervalType),
          timeZoneId),
        null)
      checkConsistencyBetweenInterpretedAndCodegen(
        (start: Expression, interval: Expression) => TimeAdd(start, interval, timeZoneId),
        TimestampType, CalendarIntervalType)
    }
  }

  test("time_sub") {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.US)
    for (tz <- Seq(TimeZoneGMT, TimeZonePST, TimeZoneJST)) {
      val timeZoneId = Option(tz.getID)
      sdf.setTimeZone(tz)

      checkEvaluation(
        TimeSub(
          Literal(new Timestamp(sdf.parse("2016-03-31 10:00:00.000").getTime)),
          Literal(new CalendarInterval(1, 0)),
          timeZoneId),
        DateTimeUtils.fromJavaTimestamp(
          new Timestamp(sdf.parse("2016-02-29 10:00:00.000").getTime)))
      checkEvaluation(
        TimeSub(
          Literal(new Timestamp(sdf.parse("2016-03-30 00:00:01.000").getTime)),
          Literal(new CalendarInterval(1, 2000000.toLong)),
          timeZoneId),
        DateTimeUtils.fromJavaTimestamp(
          new Timestamp(sdf.parse("2016-02-28 23:59:59.000").getTime)))

      checkEvaluation(
        TimeSub(
          Literal.create(null, TimestampType),
          Literal(new CalendarInterval(1, 123000L)),
          timeZoneId),
        null)
      checkEvaluation(
        TimeSub(
          Literal(new Timestamp(sdf.parse("2016-01-29 10:00:00.000").getTime)),
          Literal.create(null, CalendarIntervalType),
          timeZoneId),
        null)
      checkEvaluation(
        TimeSub(
          Literal.create(null, TimestampType),
          Literal.create(null, CalendarIntervalType),
          timeZoneId),
        null)
      checkConsistencyBetweenInterpretedAndCodegen(
        (start: Expression, interval: Expression) => TimeSub(start, interval, timeZoneId),
        TimestampType, CalendarIntervalType)
    }
  }

  test("add_months") {
    checkEvaluation(AddMonths(Literal(Date.valueOf("2015-01-30")), Literal(1)),
      DateTimeUtils.fromJavaDate(Date.valueOf("2015-02-28")))
    checkEvaluation(AddMonths(Literal(Date.valueOf("2016-03-30")), Literal(-1)),
      DateTimeUtils.fromJavaDate(Date.valueOf("2016-02-29")))
    checkEvaluation(
      AddMonths(Literal(Date.valueOf("2015-01-30")), Literal.create(null, IntegerType)),
      null)
    checkEvaluation(AddMonths(Literal.create(null, DateType), Literal(1)), null)
    checkEvaluation(AddMonths(Literal.create(null, DateType), Literal.create(null, IntegerType)),
      null)
    checkEvaluation(
      AddMonths(Literal(Date.valueOf("2015-01-30")), Literal(Int.MinValue)), -7293498)
    checkEvaluation(
      AddMonths(Literal(Date.valueOf("2016-02-28")), positiveIntLit), 1014213)
    checkEvaluation(
      AddMonths(Literal(Date.valueOf("2016-02-28")), negativeIntLit), -980528)
    checkConsistencyBetweenInterpretedAndCodegen(AddMonths, DateType, IntegerType)
  }

  test("months_between") {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
    for (tz <- Seq(TimeZoneGMT, TimeZonePST, TimeZoneJST)) {
      val timeZoneId = Option(tz.getID)
      sdf.setTimeZone(tz)

      checkEvaluation(
        MonthsBetween(
          Literal(new Timestamp(sdf.parse("1997-02-28 10:30:00").getTime)),
          Literal(new Timestamp(sdf.parse("1996-10-30 00:00:00").getTime)),
          timeZoneId),
        3.94959677)
      checkEvaluation(
        MonthsBetween(
          Literal(new Timestamp(sdf.parse("2015-01-30 11:52:00").getTime)),
          Literal(new Timestamp(sdf.parse("2015-01-30 11:50:00").getTime)),
          timeZoneId),
        0.0)
      checkEvaluation(
        MonthsBetween(
          Literal(new Timestamp(sdf.parse("2015-01-31 00:00:00").getTime)),
          Literal(new Timestamp(sdf.parse("2015-03-31 22:00:00").getTime)),
          timeZoneId),
        -2.0)
      checkEvaluation(
        MonthsBetween(
          Literal(new Timestamp(sdf.parse("2015-03-31 22:00:00").getTime)),
          Literal(new Timestamp(sdf.parse("2015-02-28 00:00:00").getTime)),
          timeZoneId),
        1.0)
      val t = Literal(Timestamp.valueOf("2015-03-31 22:00:00"))
      val tnull = Literal.create(null, TimestampType)
      checkEvaluation(MonthsBetween(t, tnull, timeZoneId), null)
      checkEvaluation(MonthsBetween(tnull, t, timeZoneId), null)
      checkEvaluation(MonthsBetween(tnull, tnull, timeZoneId), null)
      checkConsistencyBetweenInterpretedAndCodegen(
        (time1: Expression, time2: Expression) => MonthsBetween(time1, time2, timeZoneId),
        TimestampType, TimestampType)
    }
  }

  test("last_day") {
    checkEvaluation(LastDay(Literal(Date.valueOf("2015-02-28"))), Date.valueOf("2015-02-28"))
    checkEvaluation(LastDay(Literal(Date.valueOf("2015-03-27"))), Date.valueOf("2015-03-31"))
    checkEvaluation(LastDay(Literal(Date.valueOf("2015-04-26"))), Date.valueOf("2015-04-30"))
    checkEvaluation(LastDay(Literal(Date.valueOf("2015-05-25"))), Date.valueOf("2015-05-31"))
    checkEvaluation(LastDay(Literal(Date.valueOf("2015-06-24"))), Date.valueOf("2015-06-30"))
    checkEvaluation(LastDay(Literal(Date.valueOf("2015-07-23"))), Date.valueOf("2015-07-31"))
    checkEvaluation(LastDay(Literal(Date.valueOf("2015-08-01"))), Date.valueOf("2015-08-31"))
    checkEvaluation(LastDay(Literal(Date.valueOf("2015-09-02"))), Date.valueOf("2015-09-30"))
    checkEvaluation(LastDay(Literal(Date.valueOf("2015-10-03"))), Date.valueOf("2015-10-31"))
    checkEvaluation(LastDay(Literal(Date.valueOf("2015-11-04"))), Date.valueOf("2015-11-30"))
    checkEvaluation(LastDay(Literal(Date.valueOf("2015-12-05"))), Date.valueOf("2015-12-31"))
    checkEvaluation(LastDay(Literal(Date.valueOf("2016-01-06"))), Date.valueOf("2016-01-31"))
    checkEvaluation(LastDay(Literal(Date.valueOf("2016-02-07"))), Date.valueOf("2016-02-29"))
    checkEvaluation(LastDay(Literal.create(null, DateType)), null)
    checkConsistencyBetweenInterpretedAndCodegen(LastDay, DateType)
  }

  test("next_day") {
    def testNextDay(input: String, dayOfWeek: String, output: String): Unit = {
      checkEvaluation(
        NextDay(Literal(Date.valueOf(input)), NonFoldableLiteral(dayOfWeek)),
        DateTimeUtils.fromJavaDate(Date.valueOf(output)))
      checkEvaluation(
        NextDay(Literal(Date.valueOf(input)), Literal(dayOfWeek)),
        DateTimeUtils.fromJavaDate(Date.valueOf(output)))
    }
    testNextDay("2015-07-23", "Mon", "2015-07-27")
    testNextDay("2015-07-23", "mo", "2015-07-27")
    testNextDay("2015-07-23", "Tue", "2015-07-28")
    testNextDay("2015-07-23", "tu", "2015-07-28")
    testNextDay("2015-07-23", "we", "2015-07-29")
    testNextDay("2015-07-23", "wed", "2015-07-29")
    testNextDay("2015-07-23", "Thu", "2015-07-30")
    testNextDay("2015-07-23", "TH", "2015-07-30")
    testNextDay("2015-07-23", "Fri", "2015-07-24")
    testNextDay("2015-07-23", "fr", "2015-07-24")

    checkEvaluation(NextDay(Literal(Date.valueOf("2015-07-23")), Literal("xx")), null)
    checkEvaluation(NextDay(Literal.create(null, DateType), Literal("xx")), null)
    checkEvaluation(
      NextDay(Literal(Date.valueOf("2015-07-23")), Literal.create(null, StringType)), null)
  }

  test("function to_date") {
    checkEvaluation(
      ToDate(Literal(Date.valueOf("2015-07-22"))),
      DateTimeUtils.fromJavaDate(Date.valueOf("2015-07-22")))
    checkEvaluation(ToDate(Literal.create(null, DateType)), null)
    checkConsistencyBetweenInterpretedAndCodegen(ToDate, DateType)
  }

  test("function trunc") {
    def testTrunc(input: Date, fmt: String, expected: Date): Unit = {
      checkEvaluation(TruncDate(Literal.create(input, DateType), Literal.create(fmt, StringType)),
        expected)
      checkEvaluation(
        TruncDate(Literal.create(input, DateType), NonFoldableLiteral.create(fmt, StringType)),
        expected)
    }
    val date = Date.valueOf("2015-07-22")
    Seq("yyyy", "YYYY", "year", "YEAR", "yy", "YY").foreach { fmt =>
      testTrunc(date, fmt, Date.valueOf("2015-01-01"))
    }
    Seq("month", "MONTH", "mon", "MON", "mm", "MM").foreach { fmt =>
      testTrunc(date, fmt, Date.valueOf("2015-07-01"))
    }
    testTrunc(date, "DD", null)
    testTrunc(date, null, null)
    testTrunc(null, "MON", null)
    testTrunc(null, null, null)
  }

  test("from_unixtime") {
    val sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
    val fmt2 = "yyyy-MM-dd HH:mm:ss.SSS"
    val sdf2 = new SimpleDateFormat(fmt2, Locale.US)
    for (tz <- Seq(TimeZoneGMT, TimeZonePST, TimeZoneJST)) {
      val timeZoneId = Option(tz.getID)
      sdf1.setTimeZone(tz)
      sdf2.setTimeZone(tz)

      checkEvaluation(
        FromUnixTime(Literal(0L), Literal("yyyy-MM-dd HH:mm:ss"), timeZoneId),
        sdf1.format(new Timestamp(0)))
      checkEvaluation(FromUnixTime(
        Literal(1000L), Literal("yyyy-MM-dd HH:mm:ss"), timeZoneId),
        sdf1.format(new Timestamp(1000000)))
      checkEvaluation(
        FromUnixTime(Literal(-1000L), Literal(fmt2), timeZoneId),
        sdf2.format(new Timestamp(-1000000)))
      checkEvaluation(
        FromUnixTime(Literal.create(null, LongType), Literal.create(null, StringType), timeZoneId),
        null)
      checkEvaluation(
        FromUnixTime(Literal.create(null, LongType), Literal("yyyy-MM-dd HH:mm:ss"), timeZoneId),
        null)
      checkEvaluation(
        FromUnixTime(Literal(1000L), Literal.create(null, StringType), timeZoneId),
        null)
      checkEvaluation(
        FromUnixTime(Literal(0L), Literal("not a valid format"), timeZoneId), null)
    }
  }

  test("unix_timestamp") {
    val sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
    val fmt2 = "yyyy-MM-dd HH:mm:ss.SSS"
    val sdf2 = new SimpleDateFormat(fmt2, Locale.US)
    val fmt3 = "yy-MM-dd"
    val sdf3 = new SimpleDateFormat(fmt3, Locale.US)
    sdf3.setTimeZone(TimeZoneGMT)

    withDefaultTimeZone(TimeZoneGMT) {
      for (tz <- Seq(TimeZoneGMT, TimeZonePST, TimeZoneJST)) {
        val timeZoneId = Option(tz.getID)
        sdf1.setTimeZone(tz)
        sdf2.setTimeZone(tz)

        val date1 = Date.valueOf("2015-07-24")
        checkEvaluation(UnixTimestamp(
          Literal(sdf1.format(new Timestamp(0))), Literal("yyyy-MM-dd HH:mm:ss"), timeZoneId), 0L)
        checkEvaluation(UnixTimestamp(
          Literal(sdf1.format(new Timestamp(1000000))), Literal("yyyy-MM-dd HH:mm:ss"), timeZoneId),
          1000L)
        checkEvaluation(
          UnixTimestamp(
            Literal(new Timestamp(1000000)), Literal("yyyy-MM-dd HH:mm:ss"), timeZoneId),
          1000L)
        checkEvaluation(
          UnixTimestamp(Literal(date1), Literal("yyyy-MM-dd HH:mm:ss"), timeZoneId),
          DateTimeUtils.daysToMillis(DateTimeUtils.fromJavaDate(date1), tz) / 1000L)
        checkEvaluation(
          UnixTimestamp(Literal(sdf2.format(new Timestamp(-1000000))), Literal(fmt2), timeZoneId),
          -1000L)
        checkEvaluation(UnixTimestamp(
          Literal(sdf3.format(Date.valueOf("2015-07-24"))), Literal(fmt3), timeZoneId),
          DateTimeUtils.daysToMillis(
            DateTimeUtils.fromJavaDate(Date.valueOf("2015-07-24")), tz) / 1000L)
        val t1 = UnixTimestamp(
          CurrentTimestamp(), Literal("yyyy-MM-dd HH:mm:ss")).eval().asInstanceOf[Long]
        val t2 = UnixTimestamp(
          CurrentTimestamp(), Literal("yyyy-MM-dd HH:mm:ss")).eval().asInstanceOf[Long]
        assert(t2 - t1 <= 1)
        checkEvaluation(
          UnixTimestamp(
            Literal.create(null, DateType), Literal.create(null, StringType), timeZoneId),
          null)
        checkEvaluation(
          UnixTimestamp(Literal.create(null, DateType), Literal("yyyy-MM-dd HH:mm:ss"), timeZoneId),
          null)
        checkEvaluation(
          UnixTimestamp(Literal(date1), Literal.create(null, StringType), timeZoneId),
          DateTimeUtils.daysToMillis(DateTimeUtils.fromJavaDate(date1), tz) / 1000L)
        checkEvaluation(
          UnixTimestamp(Literal("2015-07-24"), Literal("not a valid format"), timeZoneId), null)
      }
    }
  }

  test("to_unix_timestamp") {
    val sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
    val fmt2 = "yyyy-MM-dd HH:mm:ss.SSS"
    val sdf2 = new SimpleDateFormat(fmt2, Locale.US)
    val fmt3 = "yy-MM-dd"
    val sdf3 = new SimpleDateFormat(fmt3, Locale.US)
    sdf3.setTimeZone(TimeZoneGMT)

    withDefaultTimeZone(TimeZoneGMT) {
      for (tz <- Seq(TimeZoneGMT, TimeZonePST, TimeZoneJST)) {
        val timeZoneId = Option(tz.getID)
        sdf1.setTimeZone(tz)
        sdf2.setTimeZone(tz)

        val date1 = Date.valueOf("2015-07-24")
        checkEvaluation(ToUnixTimestamp(
          Literal(sdf1.format(new Timestamp(0))), Literal("yyyy-MM-dd HH:mm:ss"), timeZoneId), 0L)
        checkEvaluation(ToUnixTimestamp(
          Literal(sdf1.format(new Timestamp(1000000))), Literal("yyyy-MM-dd HH:mm:ss"), timeZoneId),
          1000L)
        checkEvaluation(ToUnixTimestamp(
          Literal(new Timestamp(1000000)), Literal("yyyy-MM-dd HH:mm:ss")),
          1000L)
        checkEvaluation(
          ToUnixTimestamp(Literal(date1), Literal("yyyy-MM-dd HH:mm:ss"), timeZoneId),
          DateTimeUtils.daysToMillis(DateTimeUtils.fromJavaDate(date1), tz) / 1000L)
        checkEvaluation(
          ToUnixTimestamp(Literal(sdf2.format(new Timestamp(-1000000))), Literal(fmt2), timeZoneId),
          -1000L)
        checkEvaluation(ToUnixTimestamp(
          Literal(sdf3.format(Date.valueOf("2015-07-24"))), Literal(fmt3), timeZoneId),
          DateTimeUtils.daysToMillis(
            DateTimeUtils.fromJavaDate(Date.valueOf("2015-07-24")), tz) / 1000L)
        val t1 = ToUnixTimestamp(
          CurrentTimestamp(), Literal("yyyy-MM-dd HH:mm:ss")).eval().asInstanceOf[Long]
        val t2 = ToUnixTimestamp(
          CurrentTimestamp(), Literal("yyyy-MM-dd HH:mm:ss")).eval().asInstanceOf[Long]
        assert(t2 - t1 <= 1)
        checkEvaluation(ToUnixTimestamp(
          Literal.create(null, DateType), Literal.create(null, StringType), timeZoneId), null)
        checkEvaluation(
          ToUnixTimestamp(
            Literal.create(null, DateType), Literal("yyyy-MM-dd HH:mm:ss"), timeZoneId),
          null)
        checkEvaluation(ToUnixTimestamp(
          Literal(date1), Literal.create(null, StringType), timeZoneId),
          DateTimeUtils.daysToMillis(DateTimeUtils.fromJavaDate(date1), tz) / 1000L)
        checkEvaluation(
          ToUnixTimestamp(Literal("2015-07-24"), Literal("not a valid format"), timeZoneId), null)
      }
    }
  }

  test("datediff") {
    checkEvaluation(
      DateDiff(Literal(Date.valueOf("2015-07-24")), Literal(Date.valueOf("2015-07-21"))), 3)
    checkEvaluation(
      DateDiff(Literal(Date.valueOf("2015-07-21")), Literal(Date.valueOf("2015-07-24"))), -3)
    checkEvaluation(DateDiff(Literal.create(null, DateType), Literal(Date.valueOf("2015-07-24"))),
      null)
    checkEvaluation(DateDiff(Literal(Date.valueOf("2015-07-24")), Literal.create(null, DateType)),
      null)
    checkEvaluation(
      DateDiff(Literal.create(null, DateType), Literal.create(null, DateType)),
      null)
  }

  test("to_utc_timestamp") {
    def test(t: String, tz: String, expected: String): Unit = {
      checkEvaluation(
        ToUTCTimestamp(
          Literal.create(if (t != null) Timestamp.valueOf(t) else null, TimestampType),
          Literal.create(tz, StringType)),
        if (expected != null) Timestamp.valueOf(expected) else null)
      checkEvaluation(
        ToUTCTimestamp(
          Literal.create(if (t != null) Timestamp.valueOf(t) else null, TimestampType),
          NonFoldableLiteral.create(tz, StringType)),
        if (expected != null) Timestamp.valueOf(expected) else null)
    }
    test("2015-07-24 00:00:00", "PST", "2015-07-24 07:00:00")
    test("2015-01-24 00:00:00", "PST", "2015-01-24 08:00:00")
    test(null, "UTC", null)
    test("2015-07-24 00:00:00", null, null)
    test(null, null, null)
  }

  test("from_utc_timestamp") {
    def test(t: String, tz: String, expected: String): Unit = {
      checkEvaluation(
        FromUTCTimestamp(
          Literal.create(if (t != null) Timestamp.valueOf(t) else null, TimestampType),
          Literal.create(tz, StringType)),
        if (expected != null) Timestamp.valueOf(expected) else null)
      checkEvaluation(
        FromUTCTimestamp(
          Literal.create(if (t != null) Timestamp.valueOf(t) else null, TimestampType),
          NonFoldableLiteral.create(tz, StringType)),
        if (expected != null) Timestamp.valueOf(expected) else null)
    }
    test("2015-07-24 00:00:00", "PST", "2015-07-23 17:00:00")
    test("2015-01-24 00:00:00", "PST", "2015-01-23 16:00:00")
    test(null, "UTC", null)
    test("2015-07-24 00:00:00", null, null)
    test(null, null, null)
  }
}
