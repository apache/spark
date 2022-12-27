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
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.{Locale, TimeZone}
import java.util.concurrent.TimeUnit

import org.apache.spark.{SparkConf, SparkException, SparkUpgradeException}
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.{CEST, LA}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.unsafe.types.CalendarInterval

class DateFunctionsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  // The test cases which throw exceptions under ANSI mode are covered by date.sql and
  // datetime-parsing-invalid.sql in org.apache.spark.sql.SQLQueryTestSuite.
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "false")

  test("function current_date") {
    val df1 = Seq((1, 2), (3, 1)).toDF("a", "b")
    val d0 = DateTimeUtils.currentDate(ZoneId.systemDefault())
    val d1 = DateTimeUtils.fromJavaDate(df1.select(current_date()).collect().head.getDate(0))
    val d2 = DateTimeUtils.fromJavaDate(
      sql("""SELECT CURRENT_DATE()""").collect().head.getDate(0))
    val d3 = DateTimeUtils.fromJavaDate(
      sql("""SELECT CURDATE()""").collect().head.getDate(0))
    val d4 = DateTimeUtils.currentDate(ZoneId.systemDefault())
    assert(d0 <= d1 && d1 <= d2 && d2 <= d3 && d3 <= d4 && d4 - d0 <= 1)

    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT CURDATE(1)")
      },
      errorClass = "WRONG_NUM_ARGS.WITH_SUGGESTION",
      parameters = Map(
        "functionName" -> "`curdate`",
        "expectedNum" -> "0",
        "actualNum" -> "1"
      ),
      context = ExpectedContext("", "", 7, 16, "CURDATE(1)")
    )
  }

  test("function current_timestamp and now") {
    val df1 = Seq((1, 2), (3, 1)).toDF("a", "b")
    checkAnswer(df1.select(count_distinct(current_timestamp())), Row(1))

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
    Seq("legacy", "corrected").foreach { legacyParserPolicy =>
      withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy) {
        val df = Seq((d, sdf.format(d), ts)).toDF("a", "b", "c")

        checkAnswer(
          df.select(date_format($"a", "y"), date_format($"b", "y"), date_format($"c", "y")),
          Row("2015", "2015", "2013"))

        checkAnswer(
          df.selectExpr("date_format(a, 'y')", "date_format(b, 'y')", "date_format(c, 'y')"),
          Row("2015", "2015", "2013"))
      }
    }
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

    checkAnswer(
      df.withColumn("x", lit(1)).select(date_add(col("d"), col("x"))),
      Seq(Row(Date.valueOf("2015-06-02")), Row(Date.valueOf("2015-06-03"))))

    checkAnswer(df.selectExpr("DATE_ADD(null, 1)"), Seq(Row(null), Row(null)))
    checkAnswer(
      df.selectExpr("""DATE_ADD(d, 1)"""),
      Seq(Row(Date.valueOf("2015-06-02")), Row(Date.valueOf("2015-06-03"))))
    checkAnswer(
      df.selectExpr("""DATEADD(d, 1)"""),
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

    checkAnswer(
      df.withColumn("x", lit(1)).select(date_sub(col("d"), col("x"))),
      Seq(Row(Date.valueOf("2015-05-31")), Row(Date.valueOf("2015-06-01"))))

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
    val i = new CalendarInterval(2, 2, 2000000L)
    val day = "1 day"
    val ym = "1 year 2 month"
    val dt = "1 day 2 hour 3 minute 4 second 5 millisecond 6 microsecond"
    val df = Seq((1, t1, d1), (3, t2, d2)).toDF("n", "t", "d")
    checkAnswer(
      df.selectExpr(s"d + INTERVAL'$ym'"),
      Seq(Row(Date.valueOf("2016-09-30")),
        Row(Date.valueOf("2017-02-28"))))
    checkAnswer(
      df.selectExpr(s"t + INTERVAL'$ym'"),
      Seq(Row(Timestamp.valueOf("2016-09-30 23:59:59")),
        Row(Timestamp.valueOf("2017-02-28 00:00:00"))))
    checkAnswer(
      df.selectExpr(s"d + INTERVAL'$dt'"),
      Seq(Row(Timestamp.valueOf("2015-08-01 02:03:04.005006")),
        Row(Timestamp.valueOf("2016-01-01 02:03:04.005006"))))
    checkAnswer(
      df.selectExpr(s"d + INTERVAL '$day'"),
      Seq(Row(Date.valueOf("2015-08-01")),
        Row(Date.valueOf("2016-01-01"))))
    checkAnswer(
      df.selectExpr(s"t + INTERVAL'$dt'"),
      Seq(Row(Timestamp.valueOf("2015-08-02 02:03:03.005006")),
        Row(Timestamp.valueOf("2016-01-01 02:03:04.005006"))))
    withSQLConf(SQLConf.LEGACY_INTERVAL_ENABLED.key -> "true") {
      checkAnswer(
        df.selectExpr(s"d + INTERVAL'${i.toString}'"),
        Seq(Row(Date.valueOf("2015-10-02")), Row(Date.valueOf("2016-03-02"))))
      checkAnswer(
        df.selectExpr(s"t + INTERVAL'${i.toString}'"),
        Seq(Row(Timestamp.valueOf("2015-10-03 00:00:01")),
          Row(Timestamp.valueOf("2016-03-02 00:00:02"))))
    }
  }

  test("time_sub") {
    val t1 = Timestamp.valueOf("2015-10-01 00:00:01")
    val t2 = Timestamp.valueOf("2016-02-29 00:00:02")
    val d1 = Date.valueOf("2015-09-30")
    val d2 = Date.valueOf("2016-02-29")
    val i = new CalendarInterval(2, 2, 2000000L)
    val day = "1 day"
    val ym = "1 year 2 month"
    val dt = "1 day 2 hour 3 minute 4 second 5 millisecond 6 microsecond"
    val df = Seq((1, t1, d1), (3, t2, d2)).toDF("n", "t", "d")
    checkAnswer(
      df.selectExpr(s"d - INTERVAL'$ym'"),
      Seq(Row(Date.valueOf("2014-07-30")),
        Row(Date.valueOf("2014-12-29"))))
    checkAnswer(
      df.selectExpr(s"t - INTERVAL'$ym'"),
      Seq(Row(Timestamp.valueOf("2014-08-01 00:00:01")),
        Row(Timestamp.valueOf("2014-12-29 00:00:02"))))
    checkAnswer(
      df.selectExpr(s"d - INTERVAL'$dt'"),
      Seq(Row(Timestamp.valueOf("2015-09-28 21:56:55.994994")),
        Row(Timestamp.valueOf("2016-02-27 21:56:55.994994"))))
    checkAnswer(
      df.selectExpr(s"d - INTERVAL '$day'"),
      Seq(Row(Date.valueOf("2015-09-29")),
        Row(Date.valueOf("2016-02-28"))))
    checkAnswer(
      df.selectExpr(s"t - INTERVAL'$dt'"),
      Seq(Row(Timestamp.valueOf("2015-09-29 21:56:56.994994")),
        Row(Timestamp.valueOf("2016-02-27 21:56:57.994994"))))
    withSQLConf(SQLConf.LEGACY_INTERVAL_ENABLED.key -> "true") {
      checkAnswer(
        df.selectExpr(s"d - INTERVAL'${i.toString}'"),
        Seq(Row(Date.valueOf("2015-07-27")), Row(Date.valueOf("2015-12-26"))))
      checkAnswer(
        df.selectExpr(s"t - INTERVAL'${i.toString}'"),
        Seq(Row(Timestamp.valueOf("2015-07-29 23:59:59")),
          Row(Timestamp.valueOf("2015-12-27 00:00:00"))))
    }
  }

  test("function add_months") {
    val d1 = Date.valueOf("2015-08-31")
    val d2 = Date.valueOf("2015-02-28")
    val df = Seq((1, d1), (2, d2)).toDF("n", "d")
    checkAnswer(
      df.select(add_months(col("d"), 1)),
      Seq(Row(Date.valueOf("2015-09-30")), Row(Date.valueOf("2015-03-28"))))
    checkAnswer(
      df.selectExpr("add_months(d, -1)"),
      Seq(Row(Date.valueOf("2015-07-31")), Row(Date.valueOf("2015-01-28"))))
    checkAnswer(
      df.withColumn("x", lit(1)).select(add_months(col("d"), col("x"))),
      Seq(Row(Date.valueOf("2015-09-30")), Row(Date.valueOf("2015-03-28"))))
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
      df1.select(
        next_day(col("d"), "MONDAY"),
        next_day(col("d"), col("dow")),
        next_day(col("d"), "NonValidDay")),
      Seq(
        Row(Date.valueOf("2015-07-27"), Date.valueOf("2015-07-27"), null),
        Row(Date.valueOf("2015-07-27"), Date.valueOf("2015-07-21"), null)))
    checkAnswer(
      df2.select(
        next_day(col("t"), "th"),
        next_day(col("t"), col("dow")),
        next_day(col("t"), "NonValidDay")),
      Seq(
        Row(Date.valueOf("2015-07-30"), Date.valueOf("2015-07-30"), null),
        Row(Date.valueOf("2015-07-30"), null, null)))
  }

  def checkExceptionMessage(df: DataFrame): Unit = {
    val message = intercept[SparkException] {
      df.collect()
    }.getCause.getMessage
    assert(message.contains("Fail to parse"))
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
    val confKey = SQLConf.LEGACY_TIME_PARSER_POLICY.key
    withSQLConf(confKey -> "corrected") {
      checkAnswer(
        df.select(to_date(col("s"), "yyyy-MM-dd")),
        Seq(Row(null), Row(Date.valueOf("2014-12-31")), Row(null)))
    }
    withSQLConf(confKey -> "exception") {
      checkExceptionMessage(df.select(to_date(col("s"), "yyyy-MM-dd")))
    }

    // now switch format
    checkAnswer(
      df.select(to_date(col("s"), "yyyy-dd-MM")),
      Seq(Row(null), Row(null), Row(Date.valueOf("2014-12-31"))))

    // invalid format
    checkAnswer(
      df.select(to_date(col("s"), "yyyy-hh-MM")),
      Seq(Row(null), Row(null), Row(null)))
    val e = intercept[SparkUpgradeException](df.select(to_date(col("s"), "yyyy-dd-aa")).collect())
    assert(e.getCause.isInstanceOf[IllegalArgumentException])
    assert(e.getMessage.contains("You may get a different result due to the upgrading to Spark"))

    // February
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
      (1, Timestamp.valueOf("2015-07-22 10:01:40.123456")),
      (2, Timestamp.valueOf("2014-12-31 05:29:06.123456"))).toDF("i", "t")

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

    checkAnswer(
      df.selectExpr("date_trunc('MILLISECOND', t)"),
      Seq(Row(Timestamp.valueOf("2015-07-22 10:01:40.123")),
        Row(Timestamp.valueOf("2014-12-31 05:29:06.123"))))
  }

  test("unsupported fmt fields for trunc/date_trunc results null") {
    Seq("INVALID", "decade", "century", "millennium", "whatever", null).foreach { f =>
    checkAnswer(
      Seq(Date.valueOf("2014-12-31"))
        .toDF("dt")
        .selectExpr(s"date_trunc('$f', dt)", "trunc(dt, '$f')"),
      Row(null, null))
    }
  }

  test("from_unixtime") {
    Seq("corrected", "legacy").foreach { legacyParserPolicy =>
      withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy) {
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
    }
  }

  private def secs(millis: Long): Long = TimeUnit.MILLISECONDS.toSeconds(millis)

  test("unix_timestamp") {
    Seq("corrected", "legacy").foreach { legacyParserPolicy =>
      withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy) {
        val date1 = Date.valueOf("2015-07-24")
        val date2 = Date.valueOf("2015-07-25")
        val ts1 = Timestamp.valueOf("2015-07-24 10:00:00.3")
        val ts2 = Timestamp.valueOf("2015-07-25 02:02:02.2")
        val ntzTs1 = LocalDateTime.parse("2015-07-24T10:00:00.3")
        val ntzTs2 = LocalDateTime.parse("2015-07-25T02:02:02.2")
        val s1 = "2015/07/24 10:00:00.5"
        val s2 = "2015/07/25 02:02:02.6"
        val ss1 = "2015-07-24 10:00:00"
        val ss2 = "2015-07-25 02:02:02"
        val fmt = "yyyy/MM/dd HH:mm:ss.S"
        val df = Seq((date1, ts1, ntzTs1, s1, ss1), (date2, ts2, ntzTs2, s2, ss2)).toDF(
          "d", "ts", "ntzTs", "s", "ss")
        checkAnswer(df.select(unix_timestamp(col("ts"))), Seq(
          Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
        checkAnswer(df.select(unix_timestamp(col("ss"))), Seq(
          Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
        checkAnswer(df.select(unix_timestamp(col("ntzTs"))), Seq(
          Row(secs(DateTimeUtils.microsToMillis(DateTimeUtils.localDateTimeToMicros(ntzTs1)))),
          Row(secs(DateTimeUtils.microsToMillis(DateTimeUtils.localDateTimeToMicros(ntzTs2))))))
        checkAnswer(df.select(unix_timestamp(col("d"), fmt)), Seq(
          Row(secs(date1.getTime)), Row(secs(date2.getTime))))
        checkAnswer(df.select(unix_timestamp(col("s"), fmt)), Seq(
          Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
        checkAnswer(df.selectExpr("unix_timestamp(ts)"), Seq(
          Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
        checkAnswer(df.selectExpr("unix_timestamp(ss)"), Seq(
          Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
        checkAnswer(df.selectExpr("unix_timestamp(ntzTs)"), Seq(
          Row(secs(DateTimeUtils.microsToMillis(DateTimeUtils.localDateTimeToMicros(ntzTs1)))),
          Row(secs(DateTimeUtils.microsToMillis(DateTimeUtils.localDateTimeToMicros(ntzTs2))))))
        checkAnswer(df.selectExpr(s"unix_timestamp(d, '$fmt')"), Seq(
          Row(secs(date1.getTime)), Row(secs(date2.getTime))))
        checkAnswer(df.selectExpr(s"unix_timestamp(s, '$fmt')"), Seq(
          Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))

        val x1 = "2015-07-24 10:00:00"
        val x2 = "2015-25-07 02:02:02"
        val x3 = "2015-07-24 25:02:02"
        val x4 = "2015-24-07 26:02:02"
        val ts3 = Timestamp.valueOf("2015-07-24 02:25:02")
        val ts4 = Timestamp.valueOf("2015-07-24 00:10:00")

        val df1 = Seq(x1, x2, x3, x4).toDF("x")
        checkAnswer(df1.select(unix_timestamp(col("x"))), Seq(
          Row(secs(ts1.getTime)), Row(null), Row(null), Row(null)))
        checkAnswer(df1.selectExpr("unix_timestamp(x)"), Seq(
          Row(secs(ts1.getTime)), Row(null), Row(null), Row(null)))
        checkAnswer(df1.select(unix_timestamp(col("x"), "yyyy-dd-MM HH:mm:ss")), Seq(
          Row(null), Row(secs(ts2.getTime)), Row(null), Row(null)))
        checkAnswer(df1.selectExpr(s"unix_timestamp(x, 'yyyy-MM-dd mm:HH:ss')"), Seq(
          Row(secs(ts4.getTime)), Row(null), Row(secs(ts3.getTime)), Row(null)))

        // invalid format
        val invalid = df1.selectExpr(s"unix_timestamp(x, 'yyyy-MM-dd aa:HH:ss')")
        if (legacyParserPolicy == "legacy") {
          checkAnswer(invalid,
            Seq(Row(null), Row(null), Row(null), Row(null)))
        } else {
          val e = intercept[SparkUpgradeException](invalid.collect())
          assert(e.getCause.isInstanceOf[IllegalArgumentException])
          assert(
            e.getMessage.contains("You may get a different result due to the upgrading to Spark"))
        }

        // February
        val y1 = "2016-02-29"
        val y2 = "2017-02-29"
        val ts5 = Timestamp.valueOf("2016-02-29 00:00:00")
        val df2 = Seq(y1, y2).toDF("y")
        checkAnswer(df2.select(unix_timestamp(col("y"), "yyyy-MM-dd")), Seq(
          Row(secs(ts5.getTime)), Row(null)))

        val now = sql("select unix_timestamp()").collect().head.getLong(0)
        checkAnswer(
          sql(s"select timestamp_seconds($now)"),
          Row(new java.util.Date(TimeUnit.SECONDS.toMillis(now))))
      }
    }
  }

  test("to_unix_timestamp") {
    Seq("corrected", "legacy").foreach { legacyParserPolicy =>
      withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy) {
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
          Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
        checkAnswer(df.selectExpr("to_unix_timestamp(ss)"), Seq(
          Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
        checkAnswer(df.selectExpr(s"to_unix_timestamp(d, '$fmt')"), Seq(
          Row(secs(date1.getTime)), Row(secs(date2.getTime))))
        checkAnswer(df.selectExpr(s"to_unix_timestamp(s, '$fmt')"), Seq(
          Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))

        val x1 = "2015-07-24 10:00:00"
        val x2 = "2015-25-07 02:02:02"
        val x3 = "2015-07-24 25:02:02"
        val x4 = "2015-24-07 26:02:02"
        val ts3 = Timestamp.valueOf("2015-07-24 02:25:02")
        val ts4 = Timestamp.valueOf("2015-07-24 00:10:00")

        val df1 = Seq(x1, x2, x3, x4).toDF("x")
        checkAnswer(df1.selectExpr("to_unix_timestamp(x)"), Seq(
          Row(secs(ts1.getTime)), Row(null), Row(null), Row(null)))
        checkAnswer(df1.selectExpr(s"to_unix_timestamp(x, 'yyyy-MM-dd mm:HH:ss')"), Seq(
          Row(secs(ts4.getTime)), Row(null), Row(secs(ts3.getTime)), Row(null)))

        // February
        val y1 = "2016-02-29"
        val y2 = "2017-02-29"
        val ts5 = Timestamp.valueOf("2016-02-29 00:00:00")
        val df2 = Seq(y1, y2).toDF("y")
        checkAnswer(df2.select(unix_timestamp(col("y"), "yyyy-MM-dd")), Seq(
          Row(secs(ts5.getTime)), Row(null)))

        // invalid format
        val invalid = df1.selectExpr(s"to_unix_timestamp(x, 'yyyy-MM-dd bb:HH:ss')")
        val e = intercept[IllegalArgumentException](invalid.collect())
        assert(e.getMessage.contains('b'))
      }
    }
  }


  test("to_timestamp") {
    Seq("legacy", "corrected").foreach { legacyParserPolicy =>
      withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> legacyParserPolicy) {
        val date1 = Date.valueOf("2015-07-24")
        val date2 = Date.valueOf("2015-07-25")
        val ts_date1 = Timestamp.valueOf("2015-07-24 00:00:00")
        val ts_date2 = Timestamp.valueOf("2015-07-25 00:00:00")
        val ts1 = Timestamp.valueOf("2015-07-24 10:00:00")
        val ts2 = Timestamp.valueOf("2015-07-25 02:02:02")
        val s1 = "2015/07/24 10:00:00.5"
        val s2 = "2015/07/25 02:02:02.6"
        val ts1m = Timestamp.valueOf("2015-07-24 10:00:00.5")
        val ts2m = Timestamp.valueOf("2015-07-25 02:02:02.6")
        val ss1 = "2015-07-24 10:00:00"
        val ss2 = "2015-07-25 02:02:02"
        val fmt = "yyyy/MM/dd HH:mm:ss.S"
        val df = Seq((date1, ts1, s1, ss1), (date2, ts2, s2, ss2)).toDF("d", "ts", "s", "ss")

        checkAnswer(df.select(to_timestamp(col("ss"))),
          df.select(timestamp_seconds(unix_timestamp(col("ss")))))
        checkAnswer(df.select(to_timestamp(col("ss"))), Seq(
          Row(ts1), Row(ts2)))
        if (legacyParserPolicy == "legacy") {
          // In Spark 2.4 and earlier, to_timestamp() parses in seconds precision and cuts off
          // the fractional part of seconds. The behavior was changed by SPARK-27438.
          val legacyFmt = "yyyy/MM/dd HH:mm:ss"
          checkAnswer(df.select(to_timestamp(col("s"), legacyFmt)), Seq(
            Row(ts1), Row(ts2)))
        } else {
          checkAnswer(df.select(to_timestamp(col("s"), fmt)), Seq(
            Row(ts1m), Row(ts2m)))
        }
        checkAnswer(df.select(to_timestamp(col("ts"), fmt)), Seq(
          Row(ts1), Row(ts2)))
        checkAnswer(df.select(to_timestamp(col("d"), "yyyy-MM-dd")), Seq(
          Row(ts_date1), Row(ts_date2)))
      }
    }
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
    checkAnswer(df.selectExpr("date_diff(a, d)"), Seq(Row(1), Row(1)))
  }

  test("to_timestamp with microseconds precision") {
    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      val timestamp = "1970-01-01T00:00:00.123456Z"
      val df = Seq(timestamp).toDF("t")
      checkAnswer(df.select(to_timestamp($"t", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSX")),
        Seq(Row(Instant.parse(timestamp))))
    }
  }

  test("from_utc_timestamp with literal zone") {
    val df = Seq(
      (Timestamp.valueOf("2015-07-24 00:00:00"), "2015-07-24 00:00:00"),
      (Timestamp.valueOf("2015-07-25 00:00:00"), "2015-07-25 00:00:00")
    ).toDF("a", "b")
    checkAnswer(
      df.select(from_utc_timestamp(col("a"), LA.getId)),
      Seq(
        Row(Timestamp.valueOf("2015-07-23 17:00:00")),
        Row(Timestamp.valueOf("2015-07-24 17:00:00"))))
    checkAnswer(
      df.select(from_utc_timestamp(col("b"), LA.getId)),
      Seq(
        Row(Timestamp.valueOf("2015-07-23 17:00:00")),
        Row(Timestamp.valueOf("2015-07-24 17:00:00"))))
  }

  test("from_utc_timestamp with column zone") {
    val df = Seq(
      (Timestamp.valueOf("2015-07-24 00:00:00"), "2015-07-24 00:00:00", CEST.getId),
      (Timestamp.valueOf("2015-07-25 00:00:00"), "2015-07-25 00:00:00", LA.getId)
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

  test("handling null field by date_part") {
    val input = Seq(Date.valueOf("2019-09-20")).toDF("d")
    Seq("date_part(null, d)", "date_part(null, date'2019-09-20')").foreach { expr =>
      val df = input.selectExpr(expr)
      assert(df.schema.headOption.get.dataType == DoubleType)
      checkAnswer(df, Row(null))
    }
  }

  test("to_utc_timestamp with literal zone") {
    val df = Seq(
      (Timestamp.valueOf("2015-07-24 00:00:00"), "2015-07-24 00:00:00"),
      (Timestamp.valueOf("2015-07-25 00:00:00"), "2015-07-25 00:00:00")
    ).toDF("a", "b")
    checkAnswer(
      df.select(to_utc_timestamp(col("a"), LA.getId)),
      Seq(
        Row(Timestamp.valueOf("2015-07-24 07:00:00")),
        Row(Timestamp.valueOf("2015-07-25 07:00:00"))))
    checkAnswer(
      df.select(to_utc_timestamp(col("b"), LA.getId)),
      Seq(
        Row(Timestamp.valueOf("2015-07-24 07:00:00")),
        Row(Timestamp.valueOf("2015-07-25 07:00:00"))))
  }

  test("to_utc_timestamp with column zone") {
    val df = Seq(
      (Timestamp.valueOf("2015-07-24 00:00:00"), "2015-07-24 00:00:00", LA.getId),
      (Timestamp.valueOf("2015-07-25 00:00:00"), "2015-07-25 00:00:00", CEST.getId)
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

  test("SPARK-30668: use legacy timestamp parser in to_timestamp") {
    val confKey = SQLConf.LEGACY_TIME_PARSER_POLICY.key
    val df = Seq("2020-01-27T20:06:11.847-0800").toDF("ts")
    withSQLConf(confKey -> "legacy") {
      val expected = Timestamp.valueOf("2020-01-27 20:06:11.847")
      checkAnswer(df.select(to_timestamp(col("ts"), "yyyy-MM-dd'T'HH:mm:ss.SSSz")),
        Row(expected))
    }
    withSQLConf(confKey -> "corrected") {
      checkAnswer(df.select(to_timestamp(col("ts"), "yyyy-MM-dd'T'HH:mm:ss.SSSz")), Row(null))
    }
    withSQLConf(confKey -> "exception") {
      checkExceptionMessage(df.select(to_timestamp(col("ts"), "yyyy-MM-dd'T'HH:mm:ss.SSSz")))
    }
  }

  test("SPARK-30752: convert time zones on a daylight saving day") {
    val systemTz = LA.getId
    val sessionTz = "UTC"
    val fromTz = "Asia/Hong_Kong"
    val fromTs = "2019-11-03T12:00:00" // daylight saving date in America/Los_Angeles
    val utsTs = "2019-11-03T04:00:00"
    val defaultTz = TimeZone.getDefault
    try {
      TimeZone.setDefault(DateTimeUtils.getTimeZone(systemTz))
      withSQLConf(
        SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> sessionTz) {
        val expected = LocalDateTime.parse(utsTs)
          .atZone(DateTimeUtils.getZoneId(sessionTz))
          .toInstant
        val df = Seq(fromTs).toDF("localTs")
        checkAnswer(
          df.select(to_utc_timestamp(col("localTs"), fromTz)),
          Row(expected))
      }
    } finally {
      TimeZone.setDefault(defaultTz)
    }
  }

  test("SPARK-30766: date_trunc of old timestamps to hours and days") {
    def checkTrunc(level: String, expected: String): Unit = {
      val df = Seq("0010-01-01 01:02:03.123456")
        .toDF()
        .select($"value".cast("timestamp").as("ts"))
        .select(date_trunc(level, $"ts").cast("string"))
      checkAnswer(df, Row(expected))
    }

    checkTrunc("HOUR", "0010-01-01 01:00:00")
    checkTrunc("DAY", "0010-01-01 00:00:00")
  }

  test("SPARK-30793: truncate timestamps before the epoch to seconds and minutes") {
    def checkTrunc(level: String, expected: String): Unit = {
      val df = Seq("1961-04-12 00:01:02.345")
        .toDF()
        .select($"value".cast("timestamp").as("ts"))
        .select(date_trunc(level, $"ts").cast("string"))
      checkAnswer(df, Row(expected))
    }

    checkTrunc("SECOND", "1961-04-12 00:01:02")
    checkTrunc("MINUTE", "1961-04-12 00:01:00")
  }
}
