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
import java.time.ZoneOffset
import java.util.{Calendar, Locale, TimeZone}
import java.util.concurrent.TimeUnit

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.DateTimeUtils.TimeZoneGMT
import org.apache.spark.sql.types._

class DateExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  import IntegralLiteralTestUtils._

  val TimeZonePST = TimeZone.getTimeZone("PST")
  val TimeZoneJST = TimeZone.getTimeZone("JST")

  val gmtId = Option(TimeZoneGMT.getID)
  val pstId = Option(TimeZonePST.getID)
  val jstId = Option(TimeZoneJST.getID)

  def toMillis(timestamp: String): Long = {
    val tf = TimestampFormatter("yyyy-MM-dd HH:mm:ss", ZoneOffset.UTC)
    TimeUnit.MICROSECONDS.toMillis(tf.parse(timestamp))
  }
  val date = "2015-04-08 13:10:15"
  val d = new Date(toMillis(date))
  val time = "2013-11-08 13:10:15"
  val ts = new Timestamp(toMillis(time))

  test("datetime function current_date") {
    val d0 = DateTimeUtils.millisToDays(System.currentTimeMillis(), TimeZoneGMT)
    val cd = CurrentDate(gmtId).eval(EmptyRow).asInstanceOf[Int]
    val d1 = DateTimeUtils.millisToDays(System.currentTimeMillis(), TimeZoneGMT)
    assert(d0 <= cd && cd <= d1 && d1 - d0 <= 1)

    val cdjst = CurrentDate(jstId).eval(EmptyRow).asInstanceOf[Int]
    val cdpst = CurrentDate(pstId).eval(EmptyRow).asInstanceOf[Int]
    assert(cdpst <= cd && cd <= cdjst)
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

    checkEvaluation(DayOfYear(Cast(Literal("1582-10-15 13:10:15"), DateType)), 288)
    checkEvaluation(DayOfYear(Cast(Literal("1582-10-04 13:10:15"), DateType)), 277)
    checkConsistencyBetweenInterpretedAndCodegen(DayOfYear, DateType)
  }

  test("Year") {
    checkEvaluation(Year(Literal.create(null, DateType)), null)
    checkEvaluation(Year(Literal(d)), 2015)
    checkEvaluation(Year(Cast(Literal(date), DateType, gmtId)), 2015)
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
    checkEvaluation(Year(Cast(Literal("1582-01-01 13:10:15"), DateType)), 1582)
    checkEvaluation(Year(Cast(Literal("1581-12-31 13:10:15"), DateType)), 1581)
    checkConsistencyBetweenInterpretedAndCodegen(Year, DateType)
  }

  test("Quarter") {
    checkEvaluation(Quarter(Literal.create(null, DateType)), null)
    checkEvaluation(Quarter(Literal(d)), 2)
    checkEvaluation(Quarter(Cast(Literal(date), DateType, gmtId)), 2)
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

    checkEvaluation(Quarter(Cast(Literal("1582-10-01 13:10:15"), DateType)), 4)
    checkEvaluation(Quarter(Cast(Literal("1582-09-30 13:10:15"), DateType)), 3)
    checkConsistencyBetweenInterpretedAndCodegen(Quarter, DateType)
  }

  test("Month") {
    checkEvaluation(Month(Literal.create(null, DateType)), null)
    checkEvaluation(Month(Literal(d)), 4)
    checkEvaluation(Month(Cast(Literal(date), DateType, gmtId)), 4)
    checkEvaluation(Month(Cast(Literal(ts), DateType, gmtId)), 11)

    checkEvaluation(Month(Cast(Literal("1582-04-28 13:10:15"), DateType)), 4)
    checkEvaluation(Month(Cast(Literal("1582-10-04 13:10:15"), DateType)), 10)
    checkEvaluation(Month(Cast(Literal("1582-10-15 13:10:15"), DateType)), 10)

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
    checkEvaluation(DayOfMonth(Cast(Literal(date), DateType, gmtId)), 8)
    checkEvaluation(DayOfMonth(Cast(Literal(ts), DateType, gmtId)), 8)

    checkEvaluation(DayOfMonth(Cast(Literal("1582-04-28 13:10:15"), DateType)), 28)
    checkEvaluation(DayOfMonth(Cast(Literal("1582-10-15 13:10:15"), DateType)), 15)
    checkEvaluation(DayOfMonth(Cast(Literal("1582-10-04 13:10:15"), DateType)), 4)

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

  test("DayOfWeek") {
    checkEvaluation(DayOfWeek(Literal.create(null, DateType)), null)
    checkEvaluation(DayOfWeek(Literal(d)), Calendar.WEDNESDAY)
    checkEvaluation(DayOfWeek(Cast(Literal(date), DateType, gmtId)),
      Calendar.WEDNESDAY)
    checkEvaluation(DayOfWeek(Cast(Literal(ts), DateType, gmtId)), Calendar.FRIDAY)
    checkEvaluation(DayOfWeek(Cast(Literal("2011-05-06"), DateType, gmtId)), Calendar.FRIDAY)
    checkEvaluation(DayOfWeek(Literal(new Date(toMillis("2017-05-27 13:10:15")))),
      Calendar.SATURDAY)
    checkEvaluation(DayOfWeek(Literal(new Date(toMillis("1582-10-15 13:10:15")))),
      Calendar.FRIDAY)
    checkConsistencyBetweenInterpretedAndCodegen(DayOfWeek, DateType)
  }

  test("WeekDay") {
    checkEvaluation(WeekDay(Literal.create(null, DateType)), null)
    checkEvaluation(WeekDay(Literal(d)), 2)
    checkEvaluation(WeekDay(Cast(Literal(date), DateType, gmtId)), 2)
    checkEvaluation(WeekDay(Cast(Literal(ts), DateType, gmtId)), 4)
    checkEvaluation(WeekDay(Cast(Literal("2011-05-06"), DateType, gmtId)), 4)
    checkEvaluation(WeekDay(Literal(new Date(toMillis("2017-05-27 13:10:15")))), 5)
    checkEvaluation(WeekDay(Literal(new Date(toMillis("1582-10-15 13:10:15")))), 4)
    checkConsistencyBetweenInterpretedAndCodegen(WeekDay, DateType)
  }

  test("WeekOfYear") {
    checkEvaluation(WeekOfYear(Literal.create(null, DateType)), null)
    checkEvaluation(WeekOfYear(Literal(d)), 15)
    checkEvaluation(WeekOfYear(Cast(Literal(date), DateType, gmtId)), 15)
    checkEvaluation(WeekOfYear(Cast(Literal(ts), DateType, gmtId)), 45)
    checkEvaluation(WeekOfYear(Cast(Literal("2011-05-06"), DateType, gmtId)), 18)
    checkEvaluation(WeekOfYear(Cast(Literal("1582-10-15 13:10:15"), DateType, gmtId)), 41)
    checkEvaluation(WeekOfYear(Cast(Literal("1582-10-04 13:10:15"), DateType, gmtId)), 40)
    checkConsistencyBetweenInterpretedAndCodegen(WeekOfYear, DateType)
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
    // Valid range of DateType is [0001-01-01, 9999-12-31]
    val maxMonthInterval = 10000 * 12
    checkEvaluation(
      AddMonths(Literal(Date.valueOf("0001-01-01")), Literal(maxMonthInterval)), 2933261)
    checkEvaluation(
      AddMonths(Literal(Date.valueOf("9999-12-31")), Literal(-1 * maxMonthInterval)), -719529)
    // Test evaluation results between Interpreted mode and Codegen mode
    forAll (
      LiteralGenerator.randomGen(DateType),
      LiteralGenerator.monthIntervalLiterGen
    ) { (l1: Literal, l2: Literal) =>
      cmpInterpretWithCodegen(EmptyRow, AddMonths(l1, l2))
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

  test("TruncDate") {
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
    testTrunc(date, "SECOND", null)
    testTrunc(date, "HOUR", null)
    testTrunc(date, null, null)
    testTrunc(null, "MON", null)
    testTrunc(null, null, null)

    testTrunc(Date.valueOf("2000-03-08"), "decade", Date.valueOf("2000-01-01"))
    testTrunc(Date.valueOf("2000-03-08"), "century", Date.valueOf("1901-01-01"))
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

  test("creating values of DateType via make_date") {
    checkEvaluation(MakeDate(Literal(2013), Literal(7), Literal(15)), Date.valueOf("2013-7-15"))
    checkEvaluation(MakeDate(Literal.create(null, IntegerType), Literal(7), Literal(15)), null)
    checkEvaluation(MakeDate(Literal(2019), Literal.create(null, IntegerType), Literal(19)), null)
    checkEvaluation(MakeDate(Literal(2019), Literal(7), Literal.create(null, IntegerType)), null)
    checkEvaluation(MakeDate(Literal(Int.MaxValue), Literal(13), Literal(19)), null)
    checkEvaluation(MakeDate(Literal(2019), Literal(13), Literal(19)), null)
    checkEvaluation(MakeDate(Literal(2019), Literal(7), Literal(32)), null)
  }

  test("millennium") {
    val date = MakeDate(Literal(2019), Literal(1), Literal(1))
    checkEvaluation(Millennium(date), 3)
    checkEvaluation(Millennium(date.copy(year = Literal(2001))), 3)
    checkEvaluation(Millennium(date.copy(year = Literal(2000))), 2)
    checkEvaluation(Millennium(date.copy(year = Literal(1001), day = Literal(28))), 2)
    checkEvaluation(Millennium(date.copy(year = Literal(1))), 1)
    checkEvaluation(Millennium(date.copy(year = Literal(-1))), -1)
    checkEvaluation(Millennium(date.copy(year = Literal(-100), month = Literal(12))), -1)
    checkEvaluation(Millennium(date.copy(year = Literal(-2019))), -3)
  }

  test("century") {
    val date = MakeDate(Literal(2019), Literal(1), Literal(1))
    checkEvaluation(Century(date), 21)
    checkEvaluation(Century(date.copy(year = Literal(2001))), 21)
    checkEvaluation(Century(date.copy(year = Literal(2000))), 20)
    checkEvaluation(Century(date.copy(year = Literal(1001), day = Literal(28))), 11)
    checkEvaluation(Century(date.copy(year = Literal(1))), 1)
    checkEvaluation(Century(date.copy(year = Literal(-1))), -1)
    checkEvaluation(Century(date.copy(year = Literal(-100), month = Literal(12))), -2)
    checkEvaluation(Century(date.copy(year = Literal(-2019))), -21)
  }

  test("decade") {
    val date = MakeDate(Literal(2019), Literal(8), Literal(8))
    checkEvaluation(Decade(date), 201)
    checkEvaluation(Decade(date.copy(year = Literal(2011))), 201)
    checkEvaluation(Decade(date.copy(year = Literal(2010))), 201)
    checkEvaluation(Decade(date.copy(year = Literal(2009))), 200)
    checkEvaluation(Decade(date.copy(year = Literal(10))), 1)
    checkEvaluation(Decade(date.copy(year = Literal(1))), 0)
    checkEvaluation(Decade(date.copy(year = Literal(-1))), -1)
    checkEvaluation(Decade(date.copy(year = Literal(-10))), -1)
    checkEvaluation(Decade(date.copy(year = Literal(-11))), -2)
    checkEvaluation(Decade(date.copy(year = Literal(-2019))), -202)
  }

  test("ISO 8601 week-numbering year") {
    checkEvaluation(IsoYear(MakeDate(Literal(2006), Literal(1), Literal(1))), 2005)
    checkEvaluation(IsoYear(MakeDate(Literal(2006), Literal(1), Literal(2))), 2006)
  }
}
