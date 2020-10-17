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

package org.apache.spark.sql

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.unsafe.types.CalendarInterval

class DateFunctionsSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("function current_date") {
    val df1 = Seq((1, 2), (3, 1)).toDF("a", "b")
    val d0 = DateTimeUtils.millisToDays(System.currentTimeMillis())
    val d1 = DateTimeUtils.fromJavaDate(df1.select(current_date()).collect().head.getDate(0))
    val d2 = DateTimeUtils.fromJavaDate(
      sql("""SELECT CURRENT_DATE()""").collect().head.getDate(0))
    val d3 = DateTimeUtils.millisToDays(System.currentTimeMillis())
    assert(d0 <= d1 && d1 <= d2 && d2 <= d3 && d3 - d0 <= 1)
  }

  test("function current_timestamp and now") {
    val df1 = Seq((1, 2), (3, 1)).toDF("a", "b")
    checkAnswer(df1.select(countDistinct(current_timestamp())), Row(1))

    // Execution in one query should return the same value
    checkAnswer(sql("""SELECT CURRENT_TIMESTAMP() = CURRENT_TIMESTAMP()"""), Row(true))

    // Current timestamp should return the current timestamp ...
    val before = System.currentTimeMillis
    val got = sql("SELECT CURRENT_TIMESTAMP()").collect().head.getTimestamp(0).getTime
    val after = System.currentTimeMillis
    assert(got >= before && got <= after)

    // Now alias
    checkAnswer(sql("""SELECT CURRENT_TIMESTAMP() = NOW()"""), Row(true))
  }

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
  val sdfDate = new SimpleDateFormat("yyyy-MM-dd", Locale.US)
  val d = new Date(sdf.parse("2015-04-08 13:10:15").getTime)
  val ts = new Timestamp(sdf.parse("2013-04-08 13:10:15").getTime)

  test("timestamp comparison with date strings") {
    val df = Seq(
      (1, Timestamp.valueOf("2015-01-01 00:00:00")),
      (2, Timestamp.valueOf("2014-01-01 00:00:00"))).toDF("i", "t")

    checkAnswer(
      df.select("t").filter($"t" <= "2014-06-01"),
      Row(Timestamp.valueOf("2014-01-01 00:00:00")) :: Nil)


    checkAnswer(
      df.select("t").filter($"t" >= "2014-06-01"),
      Row(Timestamp.valueOf("2015-01-01 00:00:00")) :: Nil)
  }

  test("date comparison with date strings") {
    val df = Seq(
      (1, Date.valueOf("2015-01-01")),
      (2, Date.valueOf("2014-01-01"))).toDF("i", "t")

    checkAnswer(
      df.select("t").filter($"t" <= "2014-06-01"),
      Row(Date.valueOf("2014-01-01")) :: Nil)


    checkAnswer(
      df.select("t").filter($"t" >= "2015"),
      Row(Date.valueOf("2015-01-01")) :: Nil)
  }

  test("date format") {
    val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(date_format($"a", "y"), date_format($"b", "y"), date_format($"c", "y")),
      Row("2015", "2015", "2013"))

    checkAnswer(
      df.selectExpr("date_format(a, 'y')", "date_format(b, 'y')", "date_format(c, 'y')"),
      Row("2015", "2015", "2013"))
  }

  test("year") {
    val df = Seq((d, sdfDate.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(year($"a"), year($"b"), year($"c")),
      Row(2015, 2015, 2013))

    checkAnswer(
      df.selectExpr("year(a)", "year(b)", "year(c)"),
      Row(2015, 2015, 2013))
  }

  test("quarter") {
    val ts = new Timestamp(sdf.parse("2013-11-08 13:10:15").getTime)

    val df = Seq((d, sdfDate.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(quarter($"a"), quarter($"b"), quarter($"c")),
      Row(2, 2, 4))

    checkAnswer(
      df.selectExpr("quarter(a)", "quarter(b)", "quarter(c)"),
      Row(2, 2, 4))
  }

  test("month") {
    val df = Seq((d, sdfDate.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(month($"a"), month($"b"), month($"c")),
      Row(4, 4, 4))

    checkAnswer(
      df.selectExpr("month(a)", "month(b)", "month(c)"),
      Row(4, 4, 4))
  }

  test("dayofmonth") {
    val df = Seq((d, sdfDate.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(dayofmonth($"a"), dayofmonth($"b"), dayofmonth($"c")),
      Row(8, 8, 8))

    checkAnswer(
      df.selectExpr("day(a)", "day(b)", "dayofmonth(c)"),
      Row(8, 8, 8))
  }

  test("dayofyear") {
    val df = Seq((d, sdfDate.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(dayofyear($"a"), dayofyear($"b"), dayofyear($"c")),
      Row(98, 98, 98))

    checkAnswer(
      df.selectExpr("dayofyear(a)", "dayofyear(b)", "dayofyear(c)"),
      Row(98, 98, 98))
  }

  test("hour") {
    val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(hour($"a"), hour($"b"), hour($"c")),
      Row(0, 13, 13))

    checkAnswer(
      df.selectExpr("hour(a)", "hour(b)", "hour(c)"),
      Row(0, 13, 13))
  }

  test("minute") {
    val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(minute($"a"), minute($"b"), minute($"c")),
      Row(0, 10, 10))

    checkAnswer(
      df.selectExpr("minute(a)", "minute(b)", "minute(c)"),
      Row(0, 10, 10))
  }

  test("second") {
    val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(second($"a"), second($"b"), second($"c")),
      Row(0, 15, 15))

    checkAnswer(
      df.selectExpr("second(a)", "second(b)", "second(c)"),
      Row(0, 15, 15))
  }

  test("weekofyear") {
    val df = Seq((d, sdfDate.format(d), ts)).toDF("a", "b", "c")

    checkAnswer(
      df.select(weekofyear($"a"), weekofyear($"b"), weekofyear($"c")),
      Row(15, 15, 15))

    checkAnswer(
      df.selectExpr("weekofyear(a)", "weekofyear(b)", "weekofyear(c)"),
      Row(15, 15, 15))
  }

  test("function date_add") {
    val st1 = "2015-06-01 12:34:56"
    val st2 = "2015-06-02 12:34:56"
    val t1 = Timestamp.valueOf(st1)
    val t2 = Timestamp.valueOf(st2)
    val s1 = "2015-06-01"
    val s2 = "2015-06-02"
    val d1 = Date.valueOf(s1)
    val d2 = Date.valueOf(s2)
    val df = Seq((t1, d1, s1, st1), (t2, d2, s2, st2)).toDF("t", "d", "s", "ss")
    checkAnswer(
      df.select(date_add(col("d"), 1)),
      Seq(Row(Date.valueOf("2015-06-02")), Row(Date.valueOf("2015-06-03"))))
    checkAnswer(
      df.select(date_add(col("t"), 3)),
      Seq(Row(Date.valueOf("2015-06-04")), Row(Date.valueOf("2015-06-05"))))
    checkAnswer(
      df.select(date_add(col("s"), 5)),
      Seq(Row(Date.valueOf("2015-06-06")), Row(Date.valueOf("2015-06-07"))))
    checkAnswer(
      df.select(date_add(col("ss"), 7)),
      Seq(Row(Date.valueOf("2015-06-08")), Row(Date.valueOf("2015-06-09"))))

    checkAnswer(df.selectExpr("DATE_ADD(null, 1)"), Seq(Row(null), Row(null)))
    checkAnswer(
      df.selectExpr("""DATE_ADD(d, 1)"""),
      Seq(Row(Date.valueOf("2015-06-02")), Row(Date.valueOf("2015-06-03"))))
  }

  test("function date_sub") {
    val st1 = "2015-06-01 12:34:56"
    val st2 = "2015-06-02 12:34:56"
    val t1 = Timestamp.valueOf(st1)
    val t2 = Timestamp.valueOf(st2)
    val s1 = "2015-06-01"
    val s2 = "2015-06-02"
    val d1 = Date.valueOf(s1)
    val d2 = Date.valueOf(s2)
    val df = Seq((t1, d1, s1, st1), (t2, d2, s2, st2)).toDF("t", "d", "s", "ss")
    checkAnswer(
      df.select(date_sub(col("d"), 1)),
      Seq(Row(Date.valueOf("2015-05-31")), Row(Date.valueOf("2015-06-01"))))
    checkAnswer(
      df.select(date_sub(col("t"), 1)),
      Seq(Row(Date.valueOf("2015-05-31")), Row(Date.valueOf("2015-06-01"))))
    checkAnswer(
      df.select(date_sub(col("s"), 1)),
      Seq(Row(Date.valueOf("2015-05-31")), Row(Date.valueOf("2015-06-01"))))
    checkAnswer(
      df.select(date_sub(col("ss"), 1)),
      Seq(Row(Date.valueOf("2015-05-31")), Row(Date.valueOf("2015-06-01"))))
    checkAnswer(
      df.select(date_sub(lit(null), 1)).limit(1), Row(null))

    checkAnswer(df.selectExpr("""DATE_SUB(d, null)"""), Seq(Row(null), Row(null)))
    checkAnswer(
      df.selectExpr("""DATE_SUB(d, 1)"""),
      Seq(Row(Date.valueOf("2015-05-31")), Row(Date.valueOf("2015-06-01"))))
  }

  test("time_add") {
    val t1 = Timestamp.valueOf("2015-07-31 23:59:59")
    val t2 = Timestamp.valueOf("2015-12-31 00:00:00")
    val d1 = Date.valueOf("2015-07-31")
    val d2 = Date.valueOf("2015-12-31")
    val i = new CalendarInterval(2, 2000000L)
    val df = Seq((1, t1, d1), (3, t2, d2)).toDF("n", "t", "d")
    checkAnswer(
      df.selectExpr(s"d + $i"),
      Seq(Row(Date.valueOf("2015-09-30")), Row(Date.valueOf("2016-02-29"))))
    checkAnswer(
      df.selectExpr(s"t + $i"),
      Seq(Row(Timestamp.valueOf("2015-10-01 00:00:01")),
        Row(Timestamp.valueOf("2016-02-29 00:00:02"))))
  }

  test("time_sub") {
    val t1 = Timestamp.valueOf("2015-10-01 00:00:01")
    val t2 = Timestamp.valueOf("2016-02-29 00:00:02")
    val d1 = Date.valueOf("2015-09-30")
    val d2 = Date.valueOf("2016-02-29")
    val i = new CalendarInterval(2, 2000000L)
    val df = Seq((1, t1, d1), (3, t2, d2)).toDF("n", "t", "d")
    checkAnswer(
      df.selectExpr(s"d - $i"),
      Seq(Row(Date.valueOf("2015-07-30")), Row(Date.valueOf("2015-12-30"))))
    checkAnswer(
      df.selectExpr(s"t - $i"),
      Seq(Row(Timestamp.valueOf("2015-07-31 23:59:59")),
        Row(Timestamp.valueOf("2015-12-31 00:00:00"))))
  }

  test("function add_months") {
    val d1 = Date.valueOf("2015-08-31")
    val d2 = Date.valueOf("2015-02-28")
    val df = Seq((1, d1), (2, d2)).toDF("n", "d")
    checkAnswer(
      df.select(add_months(col("d"), 1)),
      Seq(Row(Date.valueOf("2015-09-30")), Row(Date.valueOf("2015-03-31"))))
    checkAnswer(
      df.selectExpr("add_months(d, -1)"),
      Seq(Row(Date.valueOf("2015-07-31")), Row(Date.valueOf("2015-01-31"))))
  }

  test("function months_between") {
    val d1 = Date.valueOf("2015-07-31")
    val d2 = Date.valueOf("2015-02-16")
    val t1 = Timestamp.valueOf("2014-09-30 23:30:00")
    val t2 = Timestamp.valueOf("2015-09-16 12:00:00")
    val s1 = "2014-09-15 11:30:00"
    val s2 = "2015-10-01 00:00:00"
    val df = Seq((t1, d1, s1), (t2, d2, s2)).toDF("t", "d", "s")
    checkAnswer(df.select(months_between(col("t"), col("d"))), Seq(Row(-10.0), Row(7.0)))
    checkAnswer(df.selectExpr("months_between(t, s)"), Seq(Row(0.5), Row(-0.5)))
    checkAnswer(df.selectExpr("months_between(t, s, true)"), Seq(Row(0.5), Row(-0.5)))
    Seq(true, false).foreach { roundOff =>
      checkAnswer(df.select(months_between(col("t"), col("d"), roundOff)),
        Seq(Row(-10.0), Row(7.0)))
      checkAnswer(df.withColumn("r", lit(false)).selectExpr("months_between(t, s, r)"),
        Seq(Row(0.5), Row(-0.5)))
    }
  }

  test("function last_day") {
    val df1 = Seq((1, "2015-07-23"), (2, "2015-07-24")).toDF("i", "d")
    val df2 = Seq((1, "2015-07-23 00:11:22"), (2, "2015-07-24 11:22:33")).toDF("i", "t")
    checkAnswer(
      df1.select(last_day(col("d"))),
      Seq(Row(Date.valueOf("2015-07-31")), Row(Date.valueOf("2015-07-31"))))
    checkAnswer(
      df2.select(last_day(col("t"))),
      Seq(Row(Date.valueOf("2015-07-31")), Row(Date.valueOf("2015-07-31"))))
  }

  test("function next_day") {
    val df1 = Seq(("mon", "2015-07-23"), ("tuesday", "2015-07-20")).toDF("dow", "d")
    val df2 = Seq(("th", "2015-07-23 00:11:22"), ("xx", "2015-07-24 11:22:33")).toDF("dow", "t")
    checkAnswer(
      df1.select(next_day(col("d"), "MONDAY")),
      Seq(Row(Date.valueOf("2015-07-27")), Row(Date.valueOf("2015-07-27"))))
    checkAnswer(
      df2.select(next_day(col("t"), "th")),
      Seq(Row(Date.valueOf("2015-07-30")), Row(Date.valueOf("2015-07-30"))))
  }

  test("function to_date") {
    val d1 = Date.valueOf("2015-07-22")
    val d2 = Date.valueOf("2015-07-01")
    val d3 = Date.valueOf("2014-12-31")
    val t1 = Timestamp.valueOf("2015-07-22 10:00:00")
    val t2 = Timestamp.valueOf("2014-12-31 23:59:59")
    val t3 = Timestamp.valueOf("2014-12-31 23:59:59")
    val s1 = "2015-07-22 10:00:00"
    val s2 = "2014-12-31"
    val s3 = "2014-31-12"
    val df = Seq((d1, t1, s1), (d2, t2, s2), (d3, t3, s3)).toDF("d", "t", "s")

    checkAnswer(
      df.select(to_date(col("t"))),
      Seq(Row(Date.valueOf("2015-07-22")), Row(Date.valueOf("2014-12-31")),
        Row(Date.valueOf("2014-12-31"))))
    checkAnswer(
      df.select(to_date(col("d"))),
      Seq(Row(Date.valueOf("2015-07-22")), Row(Date.valueOf("2015-07-01")),
        Row(Date.valueOf("2014-12-31"))))
    checkAnswer(
      df.select(to_date(col("s"))),
      Seq(Row(Date.valueOf("2015-07-22")), Row(Date.valueOf("2014-12-31")), Row(null)))

    checkAnswer(
      df.selectExpr("to_date(t)"),
      Seq(Row(Date.valueOf("2015-07-22")), Row(Date.valueOf("2014-12-31")),
        Row(Date.valueOf("2014-12-31"))))
    checkAnswer(
      df.selectExpr("to_date(d)"),
      Seq(Row(Date.valueOf("2015-07-22")), Row(Date.valueOf("2015-07-01")),
        Row(Date.valueOf("2014-12-31"))))
    checkAnswer(
      df.selectExpr("to_date(s)"),
      Seq(Row(Date.valueOf("2015-07-22")), Row(Date.valueOf("2014-12-31")), Row(null)))

    // now with format
    checkAnswer(
      df.select(to_date(col("t"), "yyyy-MM-dd")),
      Seq(Row(Date.valueOf("2015-07-22")), Row(Date.valueOf("2014-12-31")),
        Row(Date.valueOf("2014-12-31"))))
    checkAnswer(
      df.select(to_date(col("d"), "yyyy-MM-dd")),
      Seq(Row(Date.valueOf("2015-07-22")), Row(Date.valueOf("2015-07-01")),
        Row(Date.valueOf("2014-12-31"))))
    checkAnswer(
      df.select(to_date(col("s"), "yyyy-MM-dd")),
      Seq(Row(Date.valueOf("2015-07-22")), Row(Date.valueOf("2014-12-31")), Row(null)))

    // now switch format
    checkAnswer(
      df.select(to_date(col("s"), "yyyy-dd-MM")),
      Seq(Row(null), Row(null), Row(Date.valueOf("2014-12-31"))))

    // invalid format
    checkAnswer(
      df.select(to_date(col("s"), "yyyy-hh-MM")),
      Seq(Row(null), Row(null), Row(null)))
    checkAnswer(
      df.select(to_date(col("s"), "yyyy-dd-aa")),
      Seq(Row(null), Row(null), Row(null)))

    // february
    val x1 = "2016-02-29"
    val x2 = "2017-02-29"
    val df1 = Seq(x1, x2).toDF("x")
    checkAnswer(
      df1.select(to_date(col("x"))), Row(Date.valueOf("2016-02-29")) :: Row(null) :: Nil)
  }

  test("function trunc") {
    val df = Seq(
      (1, Timestamp.valueOf("2015-07-22 10:00:00")),
      (2, Timestamp.valueOf("2014-12-31 00:00:00"))).toDF("i", "t")

    checkAnswer(
      df.select(trunc(col("t"), "YY")),
      Seq(Row(Date.valueOf("2015-01-01")), Row(Date.valueOf("2014-01-01"))))

    checkAnswer(
      df.selectExpr("trunc(t, 'Month')"),
      Seq(Row(Date.valueOf("2015-07-01")), Row(Date.valueOf("2014-12-01"))))
  }

  test("function date_trunc") {
    val df = Seq(
      (1, Timestamp.valueOf("2015-07-22 10:01:40.523")),
      (2, Timestamp.valueOf("2014-12-31 05:29:06.876"))).toDF("i", "t")

    checkAnswer(
      df.select(date_trunc("YY", col("t"))),
      Seq(Row(Timestamp.valueOf("2015-01-01 00:00:00")),
        Row(Timestamp.valueOf("2014-01-01 00:00:00"))))

    checkAnswer(
      df.selectExpr("date_trunc('MONTH', t)"),
      Seq(Row(Timestamp.valueOf("2015-07-01 00:00:00")),
        Row(Timestamp.valueOf("2014-12-01 00:00:00"))))

    checkAnswer(
      df.selectExpr("date_trunc('DAY', t)"),
      Seq(Row(Timestamp.valueOf("2015-07-22 00:00:00")),
        Row(Timestamp.valueOf("2014-12-31 00:00:00"))))

    checkAnswer(
      df.selectExpr("date_trunc('HOUR', t)"),
      Seq(Row(Timestamp.valueOf("2015-07-22 10:00:00")),
        Row(Timestamp.valueOf("2014-12-31 05:00:00"))))

    checkAnswer(
      df.selectExpr("date_trunc('MINUTE', t)"),
      Seq(Row(Timestamp.valueOf("2015-07-22 10:01:00")),
        Row(Timestamp.valueOf("2014-12-31 05:29:00"))))

    checkAnswer(
      df.selectExpr("date_trunc('SECOND', t)"),
      Seq(Row(Timestamp.valueOf("2015-07-22 10:01:40")),
        Row(Timestamp.valueOf("2014-12-31 05:29:06"))))

    checkAnswer(
      df.selectExpr("date_trunc('WEEK', t)"),
      Seq(Row(Timestamp.valueOf("2015-07-20 00:00:00")),
        Row(Timestamp.valueOf("2014-12-29 00:00:00"))))

    checkAnswer(
      df.selectExpr("date_trunc('QUARTER', t)"),
      Seq(Row(Timestamp.valueOf("2015-07-01 00:00:00")),
        Row(Timestamp.valueOf("2014-10-01 00:00:00"))))
  }

  test("from_unixtime") {
    val sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
    val fmt2 = "yyyy-MM-dd HH:mm:ss.SSS"
    val sdf2 = new SimpleDateFormat(fmt2, Locale.US)
    val fmt3 = "yy-MM-dd HH-mm-ss"
    val sdf3 = new SimpleDateFormat(fmt3, Locale.US)
    val df = Seq((1000, "yyyy-MM-dd HH:mm:ss.SSS"), (-1000, "yy-MM-dd HH-mm-ss")).toDF("a", "b")
    checkAnswer(
      df.select(from_unixtime(col("a"))),
      Seq(Row(sdf1.format(new Timestamp(1000000))), Row(sdf1.format(new Timestamp(-1000000)))))
    checkAnswer(
      df.select(from_unixtime(col("a"), fmt2)),
      Seq(Row(sdf2.format(new Timestamp(1000000))), Row(sdf2.format(new Timestamp(-1000000)))))
    checkAnswer(
      df.select(from_unixtime(col("a"), fmt3)),
      Seq(Row(sdf3.format(new Timestamp(1000000))), Row(sdf3.format(new Timestamp(-1000000)))))
    checkAnswer(
      df.selectExpr("from_unixtime(a)"),
      Seq(Row(sdf1.format(new Timestamp(1000000))), Row(sdf1.format(new Timestamp(-1000000)))))
    checkAnswer(
      df.selectExpr(s"from_unixtime(a, '$fmt2')"),
      Seq(Row(sdf2.format(new Timestamp(1000000))), Row(sdf2.format(new Timestamp(-1000000)))))
    checkAnswer(
      df.selectExpr(s"from_unixtime(a, '$fmt3')"),
      Seq(Row(sdf3.format(new Timestamp(1000000))), Row(sdf3.format(new Timestamp(-1000000)))))
  }

  test("unix_timestamp") {
    val date1 = Date.valueOf("2015-07-24")
    val date2 = Date.valueOf("2015-07-25")
    val ts1 = Timestamp.valueOf("2015-07-24 10:00:00.3")
    val ts2 = Timestamp.valueOf("2015-07-25 02:02:02.2")
    val s1 = "2015/07/24 10:00:00.5"
    val s2 = "2015/07/25 02:02:02.6"
    val ss1 = "2015-07-24 10:00:00"
    val ss2 = "2015-07-25 02:02:02"
    val fmt = "yyyy/MM/dd HH:mm:ss.S"
    val df = Seq((date1, ts1, s1, ss1), (date2, ts2, s2, ss2)).toDF("d", "ts", "s", "ss")
    checkAnswer(df.select(unix_timestamp(col("ts"))), Seq(
      Row(ts1.getTime / 1000L), Row(ts2.getTime / 1000L)))
    checkAnswer(df.select(unix_timestamp(col("ss"))), Seq(
      Row(ts1.getTime / 1000L), Row(ts2.getTime / 1000L)))
    checkAnswer(df.select(unix_timestamp(col("d"), fmt)), Seq(
      Row(date1.getTime / 1000L), Row(date2.getTime / 1000L)))
    checkAnswer(df.select(unix_timestamp(col("s"), fmt)), Seq(
      Row(ts1.getTime / 1000L), Row(ts2.getTime / 1000L)))
    checkAnswer(df.selectExpr("unix_timestamp(ts)"), Seq(
      Row(ts1.getTime / 1000L), Row(ts2.getTime / 1000L)))
    checkAnswer(df.selectExpr("unix_timestamp(ss)"), Seq(
      Row(ts1.getTime / 1000L), Row(ts2.getTime / 1000L)))
    checkAnswer(df.selectExpr(s"unix_timestamp(d, '$fmt')"), Seq(
      Row(date1.getTime / 1000L), Row(date2.getTime / 1000L)))
    checkAnswer(df.selectExpr(s"unix_timestamp(s, '$fmt')"), Seq(
      Row(ts1.getTime / 1000L), Row(ts2.getTime / 1000L)))

    val x1 = "2015-07-24 10:00:00"
    val x2 = "2015-25-07 02:02:02"
    val x3 = "2015-07-24 25:02:02"
    val x4 = "2015-24-07 26:02:02"
    val ts3 = Timestamp.valueOf("2015-07-24 02:25:02")
    val ts4 = Timestamp.valueOf("2015-07-24 00:10:00")

    val df1 = Seq(x1, x2, x3, x4).toDF("x")
    checkAnswer(df1.select(unix_timestamp(col("x"))), Seq(
      Row(ts1.getTime / 1000L), Row(null), Row(null), Row(null)))
    checkAnswer(df1.selectExpr("unix_timestamp(x)"), Seq(
      Row(ts1.getTime / 1000L), Row(null), Row(null), Row(null)))
    checkAnswer(df1.select(unix_timestamp(col("x"), "yyyy-dd-MM HH:mm:ss")), Seq(
      Row(null), Row(ts2.getTime / 1000L), Row(null), Row(null)))
    checkAnswer(df1.selectExpr(s"unix_timestamp(x, 'yyyy-MM-dd mm:HH:ss')"), Seq(
      Row(ts4.getTime / 1000L), Row(null), Row(ts3.getTime / 1000L), Row(null)))

    // invalid format
    checkAnswer(df1.selectExpr(s"unix_timestamp(x, 'yyyy-MM-dd aa:HH:ss')"), Seq(
      Row(null), Row(null), Row(null), Row(null)))

    // february
    val y1 = "2016-02-29"
    val y2 = "2017-02-29"
    val ts5 = Timestamp.valueOf("2016-02-29 00:00:00")
    val df2 = Seq(y1, y2).toDF("y")
    checkAnswer(df2.select(unix_timestamp(col("y"), "yyyy-MM-dd")), Seq(
      Row(ts5.getTime / 1000L), Row(null)))

    val now = sql("select unix_timestamp()").collect().head.getLong(0)
    checkAnswer(sql(s"select cast ($now as timestamp)"), Row(new java.util.Date(now * 1000)))
  }

  test("to_unix_timestamp") {
    val date1 = Date.valueOf("2015-07-24")
    val date2 = Date.valueOf("2015-07-25")
    val ts1 = Timestamp.valueOf("2015-07-24 10:00:00.3")
    val ts2 = Timestamp.valueOf("2015-07-25 02:02:02.2")
    val s1 = "2015/07/24 10:00:00.5"
    val s2 = "2015/07/25 02:02:02.6"
    val ss1 = "2015-07-24 10:00:00"
    val ss2 = "2015-07-25 02:02:02"
    val fmt = "yyyy/MM/dd HH:mm:ss.S"
    val df = Seq((date1, ts1, s1, ss1), (date2, ts2, s2, ss2)).toDF("d", "ts", "s", "ss")
    checkAnswer(df.selectExpr("to_unix_timestamp(ts)"), Seq(
      Row(ts1.getTime / 1000L), Row(ts2.getTime / 1000L)))
    checkAnswer(df.selectExpr("to_unix_timestamp(ss)"), Seq(
      Row(ts1.getTime / 1000L), Row(ts2.getTime / 1000L)))
    checkAnswer(df.selectExpr(s"to_unix_timestamp(d, '$fmt')"), Seq(
      Row(date1.getTime / 1000L), Row(date2.getTime / 1000L)))
    checkAnswer(df.selectExpr(s"to_unix_timestamp(s, '$fmt')"), Seq(
      Row(ts1.getTime / 1000L), Row(ts2.getTime / 1000L)))

    val x1 = "2015-07-24 10:00:00"
    val x2 = "2015-25-07 02:02:02"
    val x3 = "2015-07-24 25:02:02"
    val x4 = "2015-24-07 26:02:02"
    val ts3 = Timestamp.valueOf("2015-07-24 02:25:02")
    val ts4 = Timestamp.valueOf("2015-07-24 00:10:00")

    val df1 = Seq(x1, x2, x3, x4).toDF("x")
    checkAnswer(df1.selectExpr("to_unix_timestamp(x)"), Seq(
      Row(ts1.getTime / 1000L), Row(null), Row(null), Row(null)))
    checkAnswer(df1.selectExpr(s"to_unix_timestamp(x, 'yyyy-MM-dd mm:HH:ss')"), Seq(
      Row(ts4.getTime / 1000L), Row(null), Row(ts3.getTime / 1000L), Row(null)))

    // february
    val y1 = "2016-02-29"
    val y2 = "2017-02-29"
    val ts5 = Timestamp.valueOf("2016-02-29 00:00:00")
    val df2 = Seq(y1, y2).toDF("y")
    checkAnswer(df2.select(unix_timestamp(col("y"), "yyyy-MM-dd")), Seq(
      Row(ts5.getTime / 1000L), Row(null)))

    // invalid format
    checkAnswer(df1.selectExpr(s"to_unix_timestamp(x, 'yyyy-MM-dd bb:HH:ss')"), Seq(
      Row(null), Row(null), Row(null), Row(null)))
  }


  test("to_timestamp") {
    val date1 = Date.valueOf("2015-07-24")
    val date2 = Date.valueOf("2015-07-25")
    val ts_date1 = Timestamp.valueOf("2015-07-24 00:00:00")
    val ts_date2 = Timestamp.valueOf("2015-07-25 00:00:00")
    val ts1 = Timestamp.valueOf("2015-07-24 10:00:00")
    val ts2 = Timestamp.valueOf("2015-07-25 02:02:02")
    val s1 = "2015/07/24 10:00:00.5"
    val s2 = "2015/07/25 02:02:02.6"
    val ss1 = "2015-07-24 10:00:00"
    val ss2 = "2015-07-25 02:02:02"
    val fmt = "yyyy/MM/dd HH:mm:ss.S"
    val df = Seq((date1, ts1, s1, ss1), (date2, ts2, s2, ss2)).toDF("d", "ts", "s", "ss")

    checkAnswer(df.select(to_timestamp(col("ss"))),
      df.select(unix_timestamp(col("ss")).cast("timestamp")))
    checkAnswer(df.select(to_timestamp(col("ss"))), Seq(
      Row(ts1), Row(ts2)))
    checkAnswer(df.select(to_timestamp(col("s"), fmt)), Seq(
      Row(ts1), Row(ts2)))
    checkAnswer(df.select(to_timestamp(col("ts"), fmt)), Seq(
      Row(ts1), Row(ts2)))
    checkAnswer(df.select(to_timestamp(col("d"), "yyyy-MM-dd")), Seq(
      Row(ts_date1), Row(ts_date2)))
  }

  test("datediff") {
    val df = Seq(
      (Date.valueOf("2015-07-24"), Timestamp.valueOf("2015-07-24 01:00:00"),
        "2015-07-23", "2015-07-23 03:00:00"),
      (Date.valueOf("2015-07-25"), Timestamp.valueOf("2015-07-25 02:00:00"),
        "2015-07-24", "2015-07-24 04:00:00")
    ).toDF("a", "b", "c", "d")
    checkAnswer(df.select(datediff(col("a"), col("b"))), Seq(Row(0), Row(0)))
    checkAnswer(df.select(datediff(col("a"), col("c"))), Seq(Row(1), Row(1)))
    checkAnswer(df.select(datediff(col("d"), col("b"))), Seq(Row(-1), Row(-1)))
    checkAnswer(df.selectExpr("datediff(a, d)"), Seq(Row(1), Row(1)))
  }

  test("from_utc_timestamp with literal zone") {
    val df = Seq(
      (Timestamp.valueOf("2015-07-24 00:00:00"), "2015-07-24 00:00:00"),
      (Timestamp.valueOf("2015-07-25 00:00:00"), "2015-07-25 00:00:00")
    ).toDF("a", "b")
    checkAnswer(
      df.select(from_utc_timestamp(col("a"), "PST")),
      Seq(
        Row(Timestamp.valueOf("2015-07-23 17:00:00")),
        Row(Timestamp.valueOf("2015-07-24 17:00:00"))))
    checkAnswer(
      df.select(from_utc_timestamp(col("b"), "PST")),
      Seq(
        Row(Timestamp.valueOf("2015-07-23 17:00:00")),
        Row(Timestamp.valueOf("2015-07-24 17:00:00"))))
  }

  test("from_utc_timestamp with column zone") {
    val df = Seq(
      (Timestamp.valueOf("2015-07-24 00:00:00"), "2015-07-24 00:00:00", "CET"),
      (Timestamp.valueOf("2015-07-25 00:00:00"), "2015-07-25 00:00:00", "PST")
    ).toDF("a", "b", "c")
    checkAnswer(
      df.select(from_utc_timestamp(col("a"), col("c"))),
      Seq(
        Row(Timestamp.valueOf("2015-07-24 02:00:00")),
        Row(Timestamp.valueOf("2015-07-24 17:00:00"))))
    checkAnswer(
      df.select(from_utc_timestamp(col("b"), col("c"))),
      Seq(
        Row(Timestamp.valueOf("2015-07-24 02:00:00")),
        Row(Timestamp.valueOf("2015-07-24 17:00:00"))))
  }

  test("to_utc_timestamp with literal zone") {
    val df = Seq(
      (Timestamp.valueOf("2015-07-24 00:00:00"), "2015-07-24 00:00:00"),
      (Timestamp.valueOf("2015-07-25 00:00:00"), "2015-07-25 00:00:00")
    ).toDF("a", "b")
    checkAnswer(
      df.select(to_utc_timestamp(col("a"), "PST")),
      Seq(
        Row(Timestamp.valueOf("2015-07-24 07:00:00")),
        Row(Timestamp.valueOf("2015-07-25 07:00:00"))))
    checkAnswer(
      df.select(to_utc_timestamp(col("b"), "PST")),
      Seq(
        Row(Timestamp.valueOf("2015-07-24 07:00:00")),
        Row(Timestamp.valueOf("2015-07-25 07:00:00"))))
  }

  test("to_utc_timestamp with column zone") {
    val df = Seq(
      (Timestamp.valueOf("2015-07-24 00:00:00"), "2015-07-24 00:00:00", "PST"),
      (Timestamp.valueOf("2015-07-25 00:00:00"), "2015-07-25 00:00:00", "CET")
    ).toDF("a", "b", "c")
    checkAnswer(
      df.select(to_utc_timestamp(col("a"), col("c"))),
      Seq(
        Row(Timestamp.valueOf("2015-07-24 07:00:00")),
        Row(Timestamp.valueOf("2015-07-24 22:00:00"))))
    checkAnswer(
      df.select(to_utc_timestamp(col("b"), col("c"))),
      Seq(
        Row(Timestamp.valueOf("2015-07-24 07:00:00")),
        Row(Timestamp.valueOf("2015-07-24 22:00:00"))))
  }

  test("SPARK-30793, SPARK-30857: truncate timestamps before the epoch") {
    def checkTrunc(level: String, expected: String): Unit = {
      val df = Seq("1961-04-12 00:01:02.345")
        .toDF()
        .select($"value".cast("timestamp").as("ts"))
        .select(date_trunc(level, $"ts").cast("string"))
      checkAnswer(df, Row(expected))
    }

    checkTrunc("SECOND", "1961-04-12 00:01:02")
    checkTrunc("MINUTE", "1961-04-12 00:01:00")
    checkTrunc("HOUR", "1961-04-12 00:00:00")
    checkTrunc("DAY", "1961-04-12 00:00:00")
  }
}
