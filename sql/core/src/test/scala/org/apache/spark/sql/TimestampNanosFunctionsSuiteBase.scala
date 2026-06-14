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
}

// Runs the nanosecond timestamp function tests with ANSI mode enabled explicitly.
class TimestampNanosFunctionsAnsiOnSuite extends TimestampNanosFunctionsSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")
}

// Runs the nanosecond timestamp function tests with ANSI mode disabled explicitly.
class TimestampNanosFunctionsAnsiOffSuite extends TimestampNanosFunctionsSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "false")
}
