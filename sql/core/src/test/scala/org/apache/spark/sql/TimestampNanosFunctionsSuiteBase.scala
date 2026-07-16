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

import org.apache.spark.{SparkConf, SparkRuntimeException, SparkUnsupportedOperationException}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * End-to-end tests over the nanosecond-precision timestamp types `TIMESTAMP_NTZ(p)` /
 * `TIMESTAMP_LTZ(p)` (`p` in `[7, 9]`), part of the nanosecond timestamp preview (SPARK-56822).
 * Covers the datetime functions (`hour`/`minute`/`second`, `EXTRACT`/`date_part`, the date-field
 * functions) and the `MIN`/`MAX` aggregates (plus `min_by`/`max_by`/`greatest`/`least`). Most
 * tests use the SQL path (`selectExpr`); several also cross-check the Scala `Column` API. The two
 * subclasses run every test with ANSI mode on and off.
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

  // ===== MIN / MAX aggregates over nanosecond-precision timestamps (SPARK-56822) =====
  // `Min`/`Max` are type-agnostic `DeclarativeAggregate`s gated only on orderability
  // (`TypeUtils.checkForOrderingExpr`); the nanosecond timestamp types became orderable in
  // SPARK-57103, so MIN/MAX (and `min_by`/`max_by`/`greatest`/`least`, which ride the same gate)
  // work without any change to the aggregates themselves. These end-to-end tests lock that in,
  // mirroring the TimeType precedent (SPARK-52626 group-by, SPARK-52660 codegen split). The result
  // type preserves the input precision (`dataType = child.dataType`). Mixed-precision inputs route
  // through `findWiderDateTimeType`, which has no nanos arm yet, so they are out of scope here
  // (SPARK-57454); every column below is strictly same-precision.

  test("SPARK-57103: max/min over nanosecond-precision timestamps preserve the input type") {
    Seq(7, 8, 9).foreach { p =>
      val schema = new StructType()
        .add("ntz", TimestampNTZNanosType(p))
        .add("ltz", TimestampLTZNanosType(p))
      val data = Seq(
        Row(LocalDateTime.parse("2020-01-01T00:00:01.100000000"),
          Instant.parse("2020-01-01T00:00:01.100000000Z")),
        Row(LocalDateTime.parse("2020-01-01T00:00:02.200000000"),
          Instant.parse("2020-01-01T00:00:02.200000000Z")),
        Row(LocalDateTime.parse("2020-01-01T00:00:00.300000000"),
          Instant.parse("2020-01-01T00:00:00.300000000Z")),
        Row(null, null))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      val sqlRes = df.selectExpr("max(ntz)", "min(ntz)", "max(ltz)", "min(ltz)")
      val colRes = df.select(
        max(col("ntz")), min(col("ntz")), max(col("ltz")), min(col("ltz")))
      // The SQL and the Scala Column API agree.
      checkAnswer(sqlRes, colRes)
      // Absolute values (NTZ collects to LocalDateTime, LTZ to Instant; SPARK-57033).
      checkAnswer(sqlRes, Row(
        LocalDateTime.parse("2020-01-01T00:00:02.200000000"),
        LocalDateTime.parse("2020-01-01T00:00:00.300000000"),
        Instant.parse("2020-01-01T00:00:02.200000000Z"),
        Instant.parse("2020-01-01T00:00:00.300000000Z")))
      // The result keeps both the family (NTZ/LTZ) and the precision of the input.
      assert(sqlRes.schema.map(_.dataType) === Seq(
        TimestampNTZNanosType(p), TimestampNTZNanosType(p),
        TimestampLTZNanosType(p), TimestampLTZNanosType(p)))
    }
  }

  test("SPARK-57103: max/min over nanos order by the sub-microsecond remainder") {
    // Two values share the same epochMicros and differ only within the microsecond, so a correct
    // result must use the full `TimestampNanosVal` comparison and never truncate to micros.
    // Run on both the codegen (`CodeGenerator.genComp` AnyTimestampNanoType arm) and the
    // interpreted (`Ordering[TimestampNanosVal]`) paths.
    Seq(
      Seq(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
        SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY"),
      Seq(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN")
    ).foreach { conf =>
      withSQLConf(conf: _*) {
        val ntz = spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(
            Row(LocalDateTime.parse("2020-01-01T00:00:00.000000001")),
            Row(LocalDateTime.parse("2020-01-01T00:00:00.000000999")),
            Row(null))),
          new StructType().add("c", TimestampNTZNanosType(9)))
        checkAnswer(ntz.selectExpr("max(c)", "min(c)"),
          Row(LocalDateTime.parse("2020-01-01T00:00:00.000000999"),
            LocalDateTime.parse("2020-01-01T00:00:00.000000001")))

        val ltz = spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(
            Row(Instant.parse("2020-01-01T00:00:00.000000001Z")),
            Row(Instant.parse("2020-01-01T00:00:00.000000999Z")),
            Row(null))),
          new StructType().add("c", TimestampLTZNanosType(9)))
        checkAnswer(ltz.selectExpr("max(c)", "min(c)"),
          Row(Instant.parse("2020-01-01T00:00:00.000000999Z"),
            Instant.parse("2020-01-01T00:00:00.000000001Z")))
      }
    }
  }

  test("SPARK-57103: max/min over all-NULL or empty nanos input return NULL") {
    Seq(7, 8, 9).foreach { p =>
      val ntz = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(null), Row(null))),
        new StructType().add("c", TimestampNTZNanosType(p)))
      checkAnswer(ntz.selectExpr("max(c)", "min(c)"), Row(null, null))
      // Global aggregate over zero rows still produces one all-NULL row.
      checkAnswer(ntz.filter(lit(false)).selectExpr("max(c)", "min(c)"), Row(null, null))

      val ltz = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(null), Row(null))),
        new StructType().add("c", TimestampLTZNanosType(p)))
      checkAnswer(ltz.selectExpr("max(c)", "min(c)"), Row(null, null))
    }
  }

  test("SPARK-57103: group by a nanosecond key with per-group max/min") {
    // The grouping keys k1/k2 share their epochMicros but differ within the microsecond, so
    // hashing/grouping (SPARK-57103) must distinguish sub-microsecond keys; the per-group max/min
    // then order by the remainder.
    val schema = new StructType()
      .add("k", TimestampNTZNanosType(9))
      .add("v", TimestampLTZNanosType(9))
    val k1 = "2020-01-01T00:00:00.000000001"
    val k2 = "2020-01-01T00:00:00.000000002"
    val data = Seq(
      Row(LocalDateTime.parse(k1), Instant.parse("2020-01-01T10:00:00.000000111Z")),
      Row(LocalDateTime.parse(k1), Instant.parse("2020-01-01T10:00:00.000000999Z")),
      Row(LocalDateTime.parse(k2), Instant.parse("2020-01-01T10:00:00.000000500Z")))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    val res = df.groupBy("k").agg(max("v").as("mx"), min("v").as("mn")).orderBy("k")
    checkAnswer(res, Seq(
      Row(LocalDateTime.parse(k1),
        Instant.parse("2020-01-01T10:00:00.000000999Z"),
        Instant.parse("2020-01-01T10:00:00.000000111Z")),
      Row(LocalDateTime.parse(k2),
        Instant.parse("2020-01-01T10:00:00.000000500Z"),
        Instant.parse("2020-01-01T10:00:00.000000500Z"))))
    // The two sub-microsecond-distinct keys do not collapse into one group.
    assert(res.count() === 2)
    assert(res.schema("k").dataType === TimestampNTZNanosType(9))
  }

  test("SPARK-57103: min_by/max_by and greatest/least over same-precision nanos") {
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("early", LocalDateTime.parse("2020-01-01T00:00:00.000000001")),
        Row("late", LocalDateTime.parse("2020-01-01T00:00:00.000000999")))),
      new StructType().add("label", StringType).add("ts", TimestampNTZNanosType(9)))
    checkAnswer(df.selectExpr("max_by(label, ts)", "min_by(label, ts)"), Row("late", "early"))

    val df2 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(
        LocalDateTime.parse("2020-01-01T00:00:00.000000001"),
        LocalDateTime.parse("2020-01-01T00:00:00.000000999")))),
      new StructType()
        .add("a", TimestampNTZNanosType(9))
        .add("b", TimestampNTZNanosType(9)))
    checkAnswer(df2.selectExpr("greatest(a, b)", "least(a, b)"),
      Row(LocalDateTime.parse("2020-01-01T00:00:00.000000999"),
        LocalDateTime.parse("2020-01-01T00:00:00.000000001")))
  }

  test("SPARK-57103: max/min over nanos agree with the micros path when sub-micro digits are 0") {
    Seq(7, 8, 9).foreach { p =>
      val ldts = Seq(
        "2020-01-01T00:00:01.100000000",
        "2020-01-01T00:00:02.200000000",
        "2020-01-01T00:00:00.300000000")
      val nanos = spark.createDataFrame(
        spark.sparkContext.parallelize(ldts.map(s => Row(LocalDateTime.parse(s)))),
        new StructType().add("c", TimestampNTZNanosType(p)))
      val micro = spark.createDataFrame(
        spark.sparkContext.parallelize(ldts.map(s => Row(LocalDateTime.parse(s)))),
        new StructType().add("c", TimestampNTZType))
      // Compare via the string rendering so the differing result types (nanos vs micros) do not
      // matter; the sub-microsecond digits are all zero, so the values agree.
      checkAnswer(
        nanos.selectExpr("cast(max(c) as string)", "cast(min(c) as string)"),
        micro.selectExpr("cast(max(c) as string)", "cast(min(c) as string)"))
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

  // ===== max_by / min_by over nanosecond-precision timestamps (SPARK-56822) =====
  // `MaxBy`/`MinBy` gate only on the ordering expression's orderability
  // (`MaxMinBy.checkInputDataTypes` -> `TypeUtils.checkForOrderingExpr`), which the nanosecond
  // types pass (SPARK-57103); the value expression is unrestricted and `dataType = valueExpr
  // .dataType`, so a nanosecond *value* is returned with its precision preserved. No change to the
  // aggregates is needed -- these tests lock in both the nanos-as-value and nanos-as-ordering
  // paths.

  test("SPARK-57103: max_by/min_by return a nanosecond value and preserve its precision") {
    Seq(7, 8, 9).foreach { p =>
      // Value columns are nanos; the ordering column is a plain int key (max at k=3, min at k=1).
      // The sub-microsecond parts are multiples of 100ns, so they are exact at every p in [7, 9]
      // (no flooring) yet still non-zero -- proving the nanos value survives, not truncated to
      // micros.
      val schema = new StructType()
        .add("ntz", TimestampNTZNanosType(p))
        .add("ltz", TimestampLTZNanosType(p))
        .add("k", IntegerType)
      val data = Seq(
        Row(LocalDateTime.parse("2020-01-01T00:00:00.000000100"),
          Instant.parse("2020-01-01T00:00:00.000000100Z"), 1),
        Row(LocalDateTime.parse("2020-01-01T00:00:00.000000900"),
          Instant.parse("2020-01-01T00:00:00.000000900Z"), 3),
        Row(LocalDateTime.parse("2020-01-01T00:00:00.000000500"),
          Instant.parse("2020-01-01T00:00:00.000000500Z"), 2))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      val res = df.select(
        max_by(col("ntz"), col("k")), min_by(col("ntz"), col("k")),
        max_by(col("ltz"), col("k")), min_by(col("ltz"), col("k")))
      checkAnswer(res, Row(
        LocalDateTime.parse("2020-01-01T00:00:00.000000900"),
        LocalDateTime.parse("2020-01-01T00:00:00.000000100"),
        Instant.parse("2020-01-01T00:00:00.000000900Z"),
        Instant.parse("2020-01-01T00:00:00.000000100Z")))
      // The returned value keeps the family (NTZ/LTZ) and precision of the value column.
      assert(res.schema.map(_.dataType) === Seq(
        TimestampNTZNanosType(p), TimestampNTZNanosType(p),
        TimestampLTZNanosType(p), TimestampLTZNanosType(p)))
    }
  }

  test("SPARK-57103: max_by/min_by order by a nanosecond key down to the sub-microsecond") {
    // The ordering values share epochMicros and differ only within the microsecond, so picking the
    // extreme requires the full TimestampNanosVal comparison; a NULL-ordering row is ignored.
    // Run on both the codegen and the interpreted paths.
    Seq(
      Seq(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
        SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY"),
      Seq(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN")
    ).foreach { conf =>
      withSQLConf(conf: _*) {
        val df = spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(
            Row("lo", Instant.parse("2020-01-01T00:00:00.000000001Z")),
            Row("hi", Instant.parse("2020-01-01T00:00:00.000000999Z")),
            Row("skip", null))),
          new StructType().add("label", StringType).add("ts", TimestampLTZNanosType(9)))
        checkAnswer(
          df.select(max_by(col("label"), col("ts")), min_by(col("label"), col("ts"))),
          Row("hi", "lo"))
      }
    }
  }

  test("SPARK-57103: max_by/min_by over nanos handle all-NULL ordering and GROUP BY") {
    Seq(7, 8, 9).foreach { p =>
      // All ordering values NULL -> result NULL.
      val allNull = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row("a", null), Row("b", null))),
        new StructType().add("label", StringType).add("ts", TimestampNTZNanosType(p)))
      checkAnswer(
        allNull.select(max_by(col("label"), col("ts")), min_by(col("label"), col("ts"))),
        Row(null, null))

      // GROUP BY: per group, pick the label at the extreme nanosecond ordering key.
      val grouped = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row("g1", "g1-lo", LocalDateTime.parse("2020-01-01T00:00:00.000000001")),
          Row("g1", "g1-hi", LocalDateTime.parse("2020-01-01T00:00:00.000000999")),
          Row("g2", "g2-only", LocalDateTime.parse("2020-01-01T00:00:00.000000005")))),
        new StructType().add("g", StringType).add("label", StringType)
          .add("ts", TimestampNTZNanosType(p)))
      checkAnswer(
        grouped.groupBy("g").agg(
          max_by(col("label"), col("ts")).as("mx"),
          min_by(col("label"), col("ts")).as("mn")).orderBy("g"),
        Seq(Row("g1", "g1-hi", "g1-lo"), Row("g2", "g2-only", "g2-only")))
    }
  }

  test("SPARK-57527: unix_nanos over nanosecond-precision timestamps") {
    // unix_nanos returns DECIMAL(21, 0) nanoseconds since the epoch and applies no zone shift to a
    // timestamp argument. The chosen fractions have zeros beyond the 7th digit, so truncating to
    // precision p in {7, 8, 9} leaves the sub-microsecond part unchanged and the result is the same
    // for every p. Both the Scala Column API and the SQL path are exercised.
    val ntzStr = "2020-01-01T13:24:35.123456700"
    val ltzStr = "2020-01-01T21:24:35.987654300Z"
    // 2020-01-01 13:24:35.123456 -> 1577885075123456 micros, + 700 ns = 1577885075123456700.
    val ntzExpected = Row(new java.math.BigDecimal("1577885075123456700"))
    // 2020-01-01 21:24:35.987654 UTC -> 1577913875987654 micros, + 300 ns = 1577913875987654300.
    val ltzExpected = Row(new java.math.BigDecimal("1577913875987654300"))
    Seq(7, 8, 9).foreach { p =>
      checkAnswer(ntzNanos(ntzStr, p).select(unix_nanos(col("c"))), ntzExpected)
      checkAnswer(ntzNanos(ntzStr, p).selectExpr("unix_nanos(c)"), ntzExpected)
      checkAnswer(ltzNanos(ltzStr, p).select(unix_nanos(col("c"))), ltzExpected)
      checkAnswer(ltzNanos(ltzStr, p).selectExpr("unix_nanos(c)"), ltzExpected)
    }
  }

  test("SPARK-57527: unix_nanos over NULL nanosecond timestamps") {
    Seq(7, 8, 9).foreach { p =>
      val ntz = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(null))),
        new StructType().add("c", TimestampNTZNanosType(p)))
      val ltz = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(null))),
        new StructType().add("c", TimestampLTZNanosType(p)))
      checkAnswer(ntz.select(unix_nanos(col("c"))), Row(null))
      checkAnswer(ltz.select(unix_nanos(col("c"))), Row(null))
    }
  }

  test("SPARK-57526: timestamp_nanos builds nanosecond-precision TIMESTAMP_LTZ values") {
    // 1230219000123456789 ns since the epoch -> 2008-12-25 15:30:00.123456789 UTC. The result is a
    // TIMESTAMP_LTZ(9); collecting it yields the absolute Instant regardless of the session zone.
    val nanos = 1230219000123456789L
    val instant = Instant.parse("2008-12-25T15:30:00.123456789Z")
    val sqlRes = spark.sql(s"SELECT timestamp_nanos($nanos)")
    val colRes = spark.range(1).select(timestamp_nanos(lit(nanos)))
    // The SQL and Scala Column API agree, return the expected instant, and keep the LTZ(9) type.
    checkAnswer(sqlRes, colRes)
    checkAnswer(sqlRes, Row(instant))
    assert(sqlRes.schema.head.dataType === TimestampLTZNanosType(9))

    // A BIGINT argument is accepted directly through the dedicated IntegralType path (widened to
    // BigInteger, no DECIMAL coercion), so the integral literal works without a cast.
    checkAnswer(spark.sql(s"SELECT timestamp_nanos(${nanos}L)"), Row(instant))

    // DECIMAL input reaches the full [0001, 9999] calendar range, beyond a 64-bit BIGINT of nanos.
    Seq(
      Instant.parse("9999-12-31T23:59:59.999999999Z"),
      Instant.parse("0001-01-01T00:00:00.000000001Z")
    ).foreach { i =>
      val n = BigInt(i.getEpochSecond) * 1000000000L + i.getNano
      checkAnswer(
        spark.range(1).select(timestamp_nanos(lit(BigDecimal(n).bigDecimal))),
        Row(i))
    }
  }

  test("SPARK-57526: timestamp_nanos over NULL input") {
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(null))),
      new StructType().add("n", LongType))
    checkAnswer(df.select(timestamp_nanos(col("n"))), Row(null))
    checkAnswer(df.selectExpr("timestamp_nanos(n)"), Row(null))
  }

  test("SPARK-57809: listagg(distinct cast(ts as string)) within group (order by ts) " +
    "over nanosecond-precision timestamps") {
    // isCastEqualityPreserving: NTZ nanos is safe (UTC, no DST ambiguity), LTZ nanos is unsafe
    // (same DST fall-back risk as micro TIMESTAMP_LTZ). This mirrors the micro-precision behavior:
    // TimestampNTZType -> true, TimestampType -> false.
    Seq(7, 8, 9).foreach { p =>
      val ntzDF = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row(LocalDateTime.parse("2020-01-01T12:00:00.100000000")),
          Row(LocalDateTime.parse("2020-01-02T12:00:00.200000000")))),
        new StructType().add("ts", TimestampNTZNanosType(p)))

      // NTZ nanos: cast to string is equality-preserving, so LISTAGG(DISTINCT ...) is allowed.
      withSQLConf(SQLConf.LISTAGG_ALLOW_DISTINCT_CAST_WITH_ORDER.key -> "true") {
        val result = ntzDF.selectExpr(
          "listagg(distinct cast(ts as string), ', ') within group (order by ts)").collect()
        assert(result.length == 1 && result.head.getString(0) != null,
          s"NTZ nanos p=$p: listagg should succeed with a non-null result")
      }

      val ltzDF = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row(Instant.parse("2020-01-01T20:00:00.100000000Z")),
          Row(Instant.parse("2020-01-02T20:00:00.200000000Z")))),
        new StructType().add("ts", TimestampLTZNanosType(p)))

      withSQLConf(SQLConf.LISTAGG_ALLOW_DISTINCT_CAST_WITH_ORDER.key -> "true") {
        checkError(
          exception = intercept[AnalysisException] {
            ltzDF.selectExpr(
              "listagg(distinct cast(ts as string)) within group (order by ts)")
          },
          condition =
            "INVALID_WITHIN_GROUP_EXPRESSION.MISMATCH_WITH_DISTINCT_INPUT_UNSAFE_CAST",
          parameters = Map(
            "funcName" -> "`listagg`",
            "inputType" -> s""""TIMESTAMP_LTZ($p)"""",
            "castType" -> "\"STRING\""
          )
        )
      }
      withSQLConf(SQLConf.LISTAGG_ALLOW_DISTINCT_CAST_WITH_ORDER.key -> "false") {
        checkError(
          exception = intercept[AnalysisException] {
            ltzDF.selectExpr(
              "listagg(distinct cast(ts as string)) within group (order by ts)")
          },
          condition = "INVALID_WITHIN_GROUP_EXPRESSION.MISMATCH_WITH_DISTINCT_INPUT",
          parameters = Map(
            "funcName" -> "`listagg`",
            "funcArg" -> "\"CAST(ts AS STRING)\"",
            "orderingExpr" -> "\"ts\""
          )
        )
      }
    }
  }

  test("SPARK-57816: date_format / to_char / to_varchar over nanosecond-precision timestamps") {
    // The 9-`S` pattern is a fixed-width fraction field, so it always emits 9 digits; truncating to
    // precision `p` zeros the low digits (floor); it does not drop them. The session zone is
    // America/Los_Angeles: TIMESTAMP_NTZ renders its wall clock zone-independently, while the
    // TIMESTAMP_LTZ instant (21:24:35 UTC) shifts to 13:24:35 in the session zone (UTC-8 in Jan).
    val fmt = "yyyy-MM-dd HH:mm:ss.SSSSSSSSS"
    val ntzStr = "2020-01-01T13:24:35.123456789"
    val ltzStr = "2020-01-01T21:24:35.987654321Z"
    Seq(
      9 -> ("2020-01-01 13:24:35.123456789", "2020-01-01 13:24:35.987654321"),
      8 -> ("2020-01-01 13:24:35.123456780", "2020-01-01 13:24:35.987654320"),
      7 -> ("2020-01-01 13:24:35.123456700", "2020-01-01 13:24:35.987654300")
    ).foreach { case (p, (ntzExpected, ltzExpected)) =>
      val ntz = ntzNanos(ntzStr, p)
      // SQL path: date_format, to_char, to_varchar all render sub-microsecond digits.
      checkAnswer(
        ntz.selectExpr(
          s"date_format(c, '$fmt')", s"to_char(c, '$fmt')", s"to_varchar(c, '$fmt')"),
        Row(ntzExpected, ntzExpected, ntzExpected))
      // Column API agrees with the SQL path.
      checkAnswer(
        ntz.select(date_format(col("c"), fmt), to_char(col("c"), lit(fmt)),
          to_varchar(col("c"), lit(fmt))),
        Row(ntzExpected, ntzExpected, ntzExpected))

      val ltz = ltzNanos(ltzStr, p)
      checkAnswer(
        ltz.selectExpr(
          s"date_format(c, '$fmt')", s"to_char(c, '$fmt')", s"to_varchar(c, '$fmt')"),
        Row(ltzExpected, ltzExpected, ltzExpected))
      checkAnswer(
        ltz.select(date_format(col("c"), fmt), to_char(col("c"), lit(fmt)),
          to_varchar(col("c"), lit(fmt))),
        Row(ltzExpected, ltzExpected, ltzExpected))
    }

    // NULL nanosecond input renders NULL for all three functions.
    Seq(7, 8, 9).foreach { p =>
      val ntzNull = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(null))),
        new StructType().add("c", TimestampNTZNanosType(p)))
      val ltzNull = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(null))),
        new StructType().add("c", TimestampLTZNanosType(p)))
      checkAnswer(
        ntzNull.selectExpr(
          s"date_format(c, '$fmt')", s"to_char(c, '$fmt')", s"to_varchar(c, '$fmt')"),
        Row(null, null, null))
      checkAnswer(
        ltzNull.selectExpr(
          s"date_format(c, '$fmt')", s"to_char(c, '$fmt')", s"to_varchar(c, '$fmt')"),
        Row(null, null, null))
    }
  }

  test("SPARK-57816: date_format / to_char / to_varchar over nanos reject the LEGACY " +
    "time parser policy") {
    // date_format never sets forTimestampNTZ, so under LEGACY it builds a legacy formatter for
    // both NTZ and LTZ nanos, whose nanos format methods raise
    // TIMESTAMP_NANOS_WITH_LEGACY_TIME_PARSER (a micros value would format fine). Mirrors the
    // JSON-path LEGACY coverage (SPARK-57456) at the expression level.
    def rootNanosError(e: Throwable): SparkUnsupportedOperationException = {
      var cause = e
      while (cause != null && !cause.isInstanceOf[SparkUnsupportedOperationException]) {
        cause = cause.getCause
      }
      assert(cause != null, s"Expected TIMESTAMP_NANOS_WITH_LEGACY_TIME_PARSER, but got: $e")
      cause.asInstanceOf[SparkUnsupportedOperationException]
    }
    withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> "LEGACY") {
      val fmt = "yyyy-MM-dd HH:mm:ss.SSSSSSSSS"
      val expectedParameters =
        Map("config" -> ("\"" + SQLConf.LEGACY_TIME_PARSER_POLICY.key + "\""))
      val dfs = Seq(
        ntzNanos("2020-01-01T13:24:35.123456789", 9),
        ltzNanos("2020-01-01T21:24:35.987654321Z", 9))
      dfs.foreach { df =>
        Seq(s"date_format(c, '$fmt')", s"to_char(c, '$fmt')", s"to_varchar(c, '$fmt')")
          .foreach { expr =>
            checkError(
              exception = rootNanosError(intercept[Exception] {
                df.selectExpr(expr).collect()
              }),
              condition = "UNSUPPORTED_FEATURE.TIMESTAMP_NANOS_WITH_LEGACY_TIME_PARSER",
              parameters = expectedParameters)
          }
      }
    }
  }

  test("SPARK-57816: date_format / to_char / to_varchar over NTZ nanos reject a zone-token " +
    "pattern with a clean error") {
    // A zone token (`z`, `Z`, `X`, `O`, `VV`) has no meaning for the zone-less TIMESTAMP_NTZ wall
    // clock, so rendering raises java.time.DateTimeException underneath. date_format maps it to a
    // clean INVALID_PARAMETER_VALUE.PATTERN Spark error rather than leaking the raw java.time
    // exception. LTZ is zone-aware, so the same pattern renders the session zone instead of
    // erroring - hence this is NTZ-only.
    val ntz = ntzNanos("2020-01-01T13:24:35.123456789", 9)
    Seq("z", "Z", "X", "O", "VV").foreach { zoneToken =>
      val fmt = s"yyyy-MM-dd HH:mm:ss $zoneToken"
      Seq(s"date_format(c, '$fmt')", s"to_char(c, '$fmt')", s"to_varchar(c, '$fmt')")
        .foreach { expr =>
          checkError(
            exception = intercept[SparkRuntimeException] {
              ntz.selectExpr(expr).collect()
            },
            condition = "INVALID_PARAMETER_VALUE.PATTERN",
            parameters = Map(
              "parameter" -> "`format`",
              "functionName" -> "`date_format`",
              "value" -> s"'$fmt'"))
        }
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
