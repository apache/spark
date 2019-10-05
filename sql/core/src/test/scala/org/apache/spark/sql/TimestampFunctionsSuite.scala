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
import java.time.Instant
import java.util.Locale
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.unsafe.types.CalendarInterval

class TimestampFunctionsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

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
      Seq(Row(Date.valueOf("2015-07-29")), Row(Date.valueOf("2015-12-28"))))
    checkAnswer(
      df.selectExpr(s"t - $i"),
      Seq(Row(Timestamp.valueOf("2015-07-31 23:59:59")),
        Row(Timestamp.valueOf("2015-12-29 00:00:00"))))
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

    checkAnswer(
      df.selectExpr("date_trunc('DECADE', t)"),
      Seq(Row(Timestamp.valueOf("2010-01-01 00:00:00")),
        Row(Timestamp.valueOf("2010-01-01 00:00:00"))))

    Seq("century", "millennium").foreach { level =>
      checkAnswer(
        df.selectExpr(s"date_trunc('$level', t)"),
        Seq(Row(Timestamp.valueOf("2001-01-01 00:00:00")),
          Row(Timestamp.valueOf("2001-01-01 00:00:00"))))
    }
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

  private def secs(millis: Long): Long = TimeUnit.MILLISECONDS.toSeconds(millis)

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
      Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
    checkAnswer(df.select(unix_timestamp(col("ss"))), Seq(
      Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
    checkAnswer(df.select(unix_timestamp(col("d"), fmt)), Seq(
      Row(secs(date1.getTime)), Row(secs(date2.getTime))))
    checkAnswer(df.select(unix_timestamp(col("s"), fmt)), Seq(
      Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
    checkAnswer(df.selectExpr("unix_timestamp(ts)"), Seq(
      Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
    checkAnswer(df.selectExpr("unix_timestamp(ss)"), Seq(
      Row(secs(ts1.getTime)), Row(secs(ts2.getTime))))
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
    checkAnswer(df1.selectExpr(s"unix_timestamp(x, 'yyyy-MM-dd aa:HH:ss')"), Seq(
      Row(null), Row(null), Row(null), Row(null)))

    // february
    val y1 = "2016-02-29"
    val y2 = "2017-02-29"
    val ts5 = Timestamp.valueOf("2016-02-29 00:00:00")
    val df2 = Seq(y1, y2).toDF("y")
    checkAnswer(df2.select(unix_timestamp(col("y"), "yyyy-MM-dd")), Seq(
      Row(secs(ts5.getTime)), Row(null)))

    val now = sql("select unix_timestamp()").collect().head.getLong(0)
    checkAnswer(
      sql(s"select cast ($now as timestamp)"),
      Row(new java.util.Date(TimeUnit.SECONDS.toMillis(now))))
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

    // february
    val y1 = "2016-02-29"
    val y2 = "2017-02-29"
    val ts5 = Timestamp.valueOf("2016-02-29 00:00:00")
    val df2 = Seq(y1, y2).toDF("y")
    checkAnswer(df2.select(unix_timestamp(col("y"), "yyyy-MM-dd")), Seq(
      Row(secs(ts5.getTime)), Row(null)))

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
    val ts1m = Timestamp.valueOf("2015-07-24 10:00:00.5")
    val ts2m = Timestamp.valueOf("2015-07-25 02:02:02.6")
    val ss1 = "2015-07-24 10:00:00"
    val ss2 = "2015-07-25 02:02:02"
    val fmt = "yyyy/MM/dd HH:mm:ss.S"
    val df = Seq((date1, ts1, s1, ss1), (date2, ts2, s2, ss2)).toDF("d", "ts", "s", "ss")

    checkAnswer(df.select(to_timestamp(col("ss"))),
      df.select(unix_timestamp(col("ss")).cast("timestamp")))
    checkAnswer(df.select(to_timestamp(col("ss"))), Seq(
      Row(ts1), Row(ts2)))
    checkAnswer(df.select(to_timestamp(col("s"), fmt)), Seq(
      Row(ts1m), Row(ts2m)))
    checkAnswer(df.select(to_timestamp(col("ts"), fmt)), Seq(
      Row(ts1), Row(ts2)))
    checkAnswer(df.select(to_timestamp(col("d"), "yyyy-MM-dd")), Seq(
      Row(ts_date1), Row(ts_date2)))
  }

  test("from_utc_timestamp with literal zone") {
    val df = Seq(
      (Timestamp.valueOf("2015-07-24 00:00:00"), "2015-07-24 00:00:00"),
      (Timestamp.valueOf("2015-07-25 00:00:00"), "2015-07-25 00:00:00")
    ).toDF("a", "b")
    withSQLConf(SQLConf.UTC_TIMESTAMP_FUNC_ENABLED.key -> "true") {
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
    val msg = intercept[AnalysisException] {
      df.select(from_utc_timestamp(col("a"), "PST")).collect()
    }.getMessage
    assert(msg.contains(SQLConf.UTC_TIMESTAMP_FUNC_ENABLED.key))
  }

  test("from_utc_timestamp with column zone") {
    withSQLConf(SQLConf.UTC_TIMESTAMP_FUNC_ENABLED.key -> "true") {
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
  }

  test("to_utc_timestamp with literal zone") {
    val df = Seq(
      (Timestamp.valueOf("2015-07-24 00:00:00"), "2015-07-24 00:00:00"),
      (Timestamp.valueOf("2015-07-25 00:00:00"), "2015-07-25 00:00:00")
    ).toDF("a", "b")
    withSQLConf(SQLConf.UTC_TIMESTAMP_FUNC_ENABLED.key -> "true") {
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
    val msg = intercept[AnalysisException] {
      df.select(to_utc_timestamp(col("a"), "PST")).collect()
    }.getMessage
    assert(msg.contains(SQLConf.UTC_TIMESTAMP_FUNC_ENABLED.key))
  }

  test("to_utc_timestamp with column zone") {
    withSQLConf(SQLConf.UTC_TIMESTAMP_FUNC_ENABLED.key -> "true") {
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
  }

  test("to_timestamp with microseconds precision") {
    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      val timestamp = "1970-01-01T00:00:00.123456Z"
      val df = Seq(timestamp).toDF("t")
      checkAnswer(df.select(to_timestamp($"t", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSX")),
        Seq(Row(Instant.parse(timestamp))))
    }
  }
}
