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

import java.time.{Instant, LocalDateTime}

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * End-to-end tests for the `hour`, `minute` and `second` functions over the nanosecond-precision
 * timestamp types `TIMESTAMP_NTZ(p)` / `TIMESTAMP_LTZ(p)` (`p` in `[7, 9]`), part of the
 * nanosecond timestamp preview (SPARK-56822). Each test exercises both the SQL path
 * (`selectExpr`) and the Scala `Column` API (`functions.hour` / `minute` / `second`).
 */
abstract class TimestampNanosFunctionsSuiteBase extends SharedSparkSession {

  // The nanosecond timestamp types are gated behind a preview flag that is enabled by default
  // under tests (Utils.isTesting), so it is not set here. The session time zone is fixed so that
  // the TIMESTAMP_LTZ extraction below is deterministic.
  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "America/Los_Angeles")

  // A DataFrame with one TIMESTAMP_NTZ(p) and one TIMESTAMP_LTZ(p) nanosecond column plus a NULL
  // row. The wall-clock time of both non-null values is 13:24:35 in the session time zone, so the
  // hour/minute/second fields match regardless of `p`; the sub-microsecond digits never affect the
  // integer result.
  private def nanosDF(precision: Int): DataFrame = {
    val schema = new StructType()
      .add("ntz", TimestampNTZNanosType(precision))
      .add("ltz", TimestampLTZNanosType(precision))
    val data = Seq(
      Row(
        // TIMESTAMP_NTZ is zone-independent: 13:24:35 wall-clock.
        LocalDateTime.parse("2020-01-01T13:24:35.123456789"),
        // TIMESTAMP_LTZ: 21:24:35 UTC -> 13:24:35 in America/Los_Angeles (UTC-8 in January).
        Instant.parse("2020-01-01T21:24:35.987654321Z")),
      Row(null, null))
    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }

  test("SPARK-57315: hour over nanosecond-precision timestamps") {
    Seq(7, 8, 9).foreach { p =>
      val df = nanosDF(p)
      val result1 = df.selectExpr("hour(ntz)", "hour(ltz)")
      val result2 = df.select(hour(col("ntz")), hour(col("ltz")))
      checkAnswer(result1, result2)
      checkAnswer(result1, Seq(Row(13, 13), Row(null, null)))
    }
  }

  test("SPARK-57315: minute over nanosecond-precision timestamps") {
    Seq(7, 8, 9).foreach { p =>
      val df = nanosDF(p)
      val result1 = df.selectExpr("minute(ntz)", "minute(ltz)")
      val result2 = df.select(minute(col("ntz")), minute(col("ltz")))
      checkAnswer(result1, result2)
      checkAnswer(result1, Seq(Row(24, 24), Row(null, null)))
    }
  }

  test("SPARK-57315: second over nanosecond-precision timestamps") {
    Seq(7, 8, 9).foreach { p =>
      val df = nanosDF(p)
      val result1 = df.selectExpr("second(ntz)", "second(ltz)")
      val result2 = df.select(second(col("ntz")), second(col("ltz")))
      checkAnswer(result1, result2)
      checkAnswer(result1, Seq(Row(35, 35), Row(null, null)))
    }
  }

  test("SPARK-57315: hour/minute/second over pre-epoch nanosecond TIMESTAMP_NTZ") {
    val schema = new StructType().add("ntz", TimestampNTZNanosType(9))
    val data = Seq(Row(LocalDateTime.parse("1960-01-01T05:06:07.123456789")))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    checkAnswer(
      df.select(hour(col("ntz")), minute(col("ntz")), second(col("ntz"))),
      Row(5, 6, 7))
    checkAnswer(
      df.selectExpr("hour(ntz)", "minute(ntz)", "second(ntz)"),
      Row(5, 6, 7))
  }

  test("SPARK-57340: extract/date_part HOUR and MINUTE over nanosecond-precision timestamps") {
    Seq(7, 8, 9).foreach { p =>
      val df = nanosDF(p)
      val result1 = df.selectExpr(
        "extract(HOUR FROM ntz)", "extract(MINUTE FROM ntz)",
        "extract(HOUR FROM ltz)", "extract(MINUTE FROM ltz)")
      val result2 = df.selectExpr(
        "date_part('HOUR', ntz)", "date_part('MINUTE', ntz)",
        "date_part('HOUR', ltz)", "date_part('MINUTE', ltz)")
      val result3 = df.select(
        extract(lit("HOUR"), col("ntz")), extract(lit("MINUTE"), col("ntz")),
        extract(lit("HOUR"), col("ltz")), extract(lit("MINUTE"), col("ltz")))
      checkAnswer(result1, result2)
      checkAnswer(result1, result3)
      checkAnswer(result1, Seq(Row(13, 24, 13, 24), Row(null, null, null, null)))
    }
  }

  test("SPARK-57340: extract/date_part SECOND keeps the nanosecond fraction") {
    // EXTRACT(SECOND) widens the result to DECIMAL(11, 9); digits below the type's precision
    // `p` were floored when the values were created, so they read back as zeros.
    Seq(
      7 -> ("35.123456700", "35.987654300"),
      8 -> ("35.123456780", "35.987654320"),
      9 -> ("35.123456789", "35.987654321")
    ).foreach { case (p, (ntzSec, ltzSec)) =>
      val df = nanosDF(p)
      val result1 = df.selectExpr("extract(SECOND FROM ntz)", "extract(SECOND FROM ltz)")
      val result2 = df.selectExpr("date_part('SECOND', ntz)", "date_part('SECOND', ltz)")
      val result3 = df.select(
        extract(lit("SECOND"), col("ntz")), extract(lit("SECOND"), col("ltz")))
      checkAnswer(result1, result2)
      checkAnswer(result1, result3)
      checkAnswer(result1, Seq(
        Row(new java.math.BigDecimal(ntzSec), new java.math.BigDecimal(ltzSec)),
        Row(null, null)))
    }
  }

  test("SPARK-57340: extract SECOND over pre-epoch nanosecond TIMESTAMP_NTZ") {
    val schema = new StructType().add("ntz", TimestampNTZNanosType(9))
    val data = Seq(Row(LocalDateTime.parse("1960-01-01T05:06:07.123456789")))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    checkAnswer(
      df.selectExpr("extract(SECOND FROM ntz)"),
      Row(new java.math.BigDecimal("7.123456789")))
  }

  // Date field extraction functions, plus the EXTRACT / date_part date components that rewrite
  // to them. The fields depend only on the calendar date, so the precision, time-of-day and
  // sub-microsecond digits never change the result.
  private val dateFieldExprs = Seq(
    "year(c)", "quarter(c)", "month(c)", "day(c)", "dayofmonth(c)", "dayofyear(c)",
    "dayofweek(c)", "weekday(c)", "weekofyear(c)",
    "extract(YEAR FROM c)", "extract(MONTH FROM c)", "extract(DAY FROM c)",
    "extract(DOY FROM c)", "extract(WEEK FROM c)", "extract(DOW FROM c)",
    "extract(YEAROFWEEK FROM c)", "date_part('QUARTER', c)", "date_part('DOY', c)")

  private def ntzNanos(ldt: String, precision: Int): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(LocalDateTime.parse(ldt)))),
      new StructType().add("c", TimestampNTZNanosType(precision)))

  private def ltzNanos(instant: String, precision: Int): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(Instant.parse(instant)))),
      new StructType().add("c", TimestampLTZNanosType(precision)))

  test("SPARK-57469: date field functions over nanosecond TIMESTAMP_NTZ match the micro path") {
    // Leap day, an ISO-week boundary (week 53 of 2020), a quarter boundary, and a pre-epoch date.
    val ldts = Seq(
      "2020-02-29T12:34:56.123456789",
      "2021-01-01T00:00:00.000000001",
      "2020-04-01T23:59:59.999999999",
      "1960-07-15T06:07:08.123456789")
    Seq(7, 8, 9).foreach { p =>
      ldts.foreach { s =>
        val nanos = ntzNanos(s, p)
        val micro = spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(Row(LocalDateTime.parse(s)))),
          new StructType().add("c", TimestampNTZType))
        checkAnswer(nanos.selectExpr(dateFieldExprs: _*), micro.selectExpr(dateFieldExprs: _*))
      }
    }
  }

  test("SPARK-57469: date field functions over nanosecond TIMESTAMP_LTZ match the micro path") {
    // The third and fourth instants roll back to the previous calendar day in the session zone
    // (America/Los_Angeles), crossing a year and a quarter boundary respectively.
    val instants = Seq(
      "2020-02-29T12:34:56.123456789Z",
      "2020-01-01T04:00:00.123456789Z",
      "2020-04-01T06:00:00.123456789Z",
      "1960-07-15T06:07:08.123456789Z")
    Seq(7, 8, 9).foreach { p =>
      instants.foreach { s =>
        val nanos = ltzNanos(s, p)
        val micro = spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(Row(Instant.parse(s)))),
          new StructType().add("c", TimestampType))
        checkAnswer(nanos.selectExpr(dateFieldExprs: _*), micro.selectExpr(dateFieldExprs: _*))
      }
    }
  }

  test("SPARK-57469: date field corner cases over nanosecond TIMESTAMP_NTZ") {
    // year, quarter, month, day, dayofyear, dayofweek (1=Sun..7=Sat), weekday (0=Mon..6=Sun),
    // weekofyear (ISO), yearofweek (ISO week-based year).
    val fields = Seq("year(c)", "quarter(c)", "month(c)", "day(c)", "dayofyear(c)",
      "dayofweek(c)", "weekday(c)", "weekofyear(c)", "extract(YEAROFWEEK FROM c)")
    Seq(7, 8, 9).foreach { p =>
      // 2020-02-29 is a Saturday in the leap year 2020: day 60, ISO week 9.
      checkAnswer(
        ntzNanos("2020-02-29T23:59:59.999999999", p).selectExpr(fields: _*),
        Row(2020, 1, 2, 29, 60, 7, 5, 9, 2020))
      // 2021-01-01 is a Friday that belongs to ISO week 53 of 2020.
      checkAnswer(
        ntzNanos("2021-01-01T00:00:00.000000001", p).selectExpr(fields: _*),
        Row(2021, 1, 1, 1, 1, 6, 4, 53, 2020))
    }
  }

  test("SPARK-57469: date field functions match the functions.* Column API over nanos") {
    val df = ntzNanos("2020-02-29T12:34:56.123456789", 9)
    checkAnswer(
      df.selectExpr("year(c)", "month(c)", "dayofmonth(c)", "dayofweek(c)", "weekofyear(c)"),
      df.select(year(col("c")), month(col("c")), dayofmonth(col("c")),
        dayofweek(col("c")), weekofyear(col("c"))))
  }

  test("SPARK-57469: date field functions over NULL nanosecond timestamps") {
    Seq(7, 8, 9).foreach { p =>
      val ntz = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(null))),
        new StructType().add("c", TimestampNTZNanosType(p)))
      checkAnswer(
        ntz.selectExpr("year(c)", "month(c)", "day(c)", "extract(DOY FROM c)"),
        Row(null, null, null, null))
    }
  }

  test("SPARK-57528: unix_timestamp / to_unix_timestamp over nanosecond-precision timestamps") {
    // unix_timestamp returns whole-second BIGINT and applies no zone shift to a timestamp
    // argument, so the sub-second digits are dropped and the nanos result equals the
    // microsecond-timestamp result.
    val ntzStr = "2020-01-01T13:24:35.123456789"
    val ltzStr = "2020-01-01T21:24:35.987654321Z"
    Seq(7, 8, 9).foreach { p =>
      val ntzNano = ntzNanos(ntzStr, p)
      val ntzMicro = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(LocalDateTime.parse(ntzStr)))),
        new StructType().add("c", TimestampNTZType))
      checkAnswer(
        ntzNano.select(unix_timestamp(col("c")), to_unix_timestamp(col("c"))),
        ntzMicro.select(unix_timestamp(col("c")), to_unix_timestamp(col("c"))))
      // 2020-01-01 13:24:35 read as the wall-clock instant -> 1577885075 epoch seconds.
      checkAnswer(
        ntzNano.select(unix_timestamp(col("c")), to_unix_timestamp(col("c"))),
        Row(1577885075L, 1577885075L))

      val ltzNano = ltzNanos(ltzStr, p)
      val ltzMicro = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(Instant.parse(ltzStr)))),
        new StructType().add("c", TimestampType))
      checkAnswer(
        ltzNano.select(unix_timestamp(col("c")), to_unix_timestamp(col("c"))),
        ltzMicro.select(unix_timestamp(col("c")), to_unix_timestamp(col("c"))))
      // 2020-01-01 21:24:35 UTC -> 1577913875 epoch seconds.
      checkAnswer(
        ltzNano.select(unix_timestamp(col("c")), to_unix_timestamp(col("c"))),
        Row(1577913875L, 1577913875L))
    }
  }

  test("SPARK-57528: unix_timestamp / to_unix_timestamp over NULL nanosecond timestamps") {
    Seq(7, 8, 9).foreach { p =>
      val ntz = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(null))),
        new StructType().add("c", TimestampNTZNanosType(p)))
      val ltz = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(null))),
        new StructType().add("c", TimestampLTZNanosType(p)))
      checkAnswer(
        ntz.select(unix_timestamp(col("c")), to_unix_timestamp(col("c"))), Row(null, null))
      checkAnswer(
        ltz.select(unix_timestamp(col("c")), to_unix_timestamp(col("c"))), Row(null, null))
    }
  }
}

// Runs the nanosecond timestamp function tests with ANSI mode enabled explicitly.
class TimestampNanosFunctionsAnsiOnSuite extends TimestampNanosFunctionsSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")
}

// Runs the nanosecond timestamp function tests with ANSI mode disabled explicitly.
class TimestampNanosFunctionsAnsiOffSuite extends TimestampNanosFunctionsSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "false")
}
