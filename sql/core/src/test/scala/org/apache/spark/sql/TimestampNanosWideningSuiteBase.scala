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

import java.time.{Instant, LocalDate, LocalDateTime}

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * End-to-end tests for implicit type coercion / widening over the nanosecond-precision timestamp
 * types `TIMESTAMP_NTZ(p)` / `TIMESTAMP_LTZ(p)` (`p` in `[7, 9]`), part of the nanosecond timestamp
 * preview (SPARK-56822). Exercises the operators that rely on `findWiderDateTimeType`
 * (SPARK-57454): `UNION ALL`, `coalesce`, `IN`, `CASE WHEN`, and binary comparisons, mixing the
 * microsecond and nanosecond timestamp types across both time-zone families. The two subclasses run
 * every test with ANSI mode on and off.
 *
 * The nanosecond timestamp types are gated behind a preview flag that is enabled by default under
 * tests (`Utils.isTesting`), so it is not set here. The session time zone is fixed so the
 * `TIMESTAMP_LTZ` values are deterministic. The Java 8 datetime API is enabled so the microsecond
 * `TIMESTAMP` / `TIMESTAMP_NTZ` / `DATE` columns accept `Instant` / `LocalDateTime` / `LocalDate`,
 * matching the external types of the nanosecond timestamp types (see `RowEncoder`).
 */
abstract class TimestampNanosWideningSuiteBase extends SharedSparkSession {

  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "America/Los_Angeles")
    .set(SQLConf.DATETIME_JAVA8API_ENABLED.key, "true")

  // Microsecond-aligned instants/local-date-times, so they are representable exactly at every
  // precision in [6, 9] and the widening never changes the stored value.
  private val instantA = Instant.parse("2020-01-01T00:00:00Z")
  private val instantB = Instant.parse("2021-07-15T12:34:56.000001Z")
  private val ldtA = LocalDateTime.parse("2020-01-01T00:00:00")
  private val ldtB = LocalDateTime.parse("2021-07-15T12:34:56.000001")

  private def single(dt: DataType, value: Any): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(value))),
      new StructType().add("c", dt))

  private def twoCols(dt1: DataType, v1: Any, dt2: DataType, v2: Any): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(v1, v2))),
      new StructType().add("a", dt1).add("b", dt2))

  test("SPARK-57454: UNION ALL widens nanosecond timestamps") {
    // micro <-> nanos within the same family widen to the nanos type.
    val ltz = single(TimestampType, instantA).union(single(TimestampLTZNanosType(9), instantB))
    assert(ltz.schema("c").dataType === TimestampLTZNanosType(9))
    assert(ltz.count() === 2)

    val ntz = single(TimestampNTZType, ldtA).union(single(TimestampNTZNanosType(8), ldtB))
    assert(ntz.schema("c").dataType === TimestampNTZNanosType(8))
    assert(ntz.count() === 2)

    // nanos(p1) <-> nanos(p2) within the same family widen to the max precision.
    val ltzNanos =
      single(TimestampLTZNanosType(7), instantA).union(single(TimestampLTZNanosType(9), instantB))
    assert(ltzNanos.schema("c").dataType === TimestampLTZNanosType(9))
    assert(ltzNanos.count() === 2)

    // Mixed time-zone families widen to the LTZ family.
    val mixed = single(TimestampType, instantA).union(single(TimestampNTZNanosType(9), ldtB))
    assert(mixed.schema("c").dataType === TimestampLTZNanosType(9))
    assert(mixed.count() === 2)

    // nanos <-> date widen to the nanos type.
    val withDate = single(DateType, LocalDate.parse("2020-01-01"))
      .union(single(TimestampNTZNanosType(7), ldtB))
    assert(withDate.schema("c").dataType === TimestampNTZNanosType(7))
    assert(withDate.count() === 2)
  }

  test("SPARK-57454: coalesce widens nanosecond timestamps") {
    val ltz = twoCols(TimestampLTZNanosType(7), instantA, TimestampLTZNanosType(9), instantB)
    val ltzRes = ltz.select(coalesce(col("a"), col("b")).as("c"))
    assert(ltzRes.schema("c").dataType === TimestampLTZNanosType(9))

    val ntz = twoCols(TimestampNTZType, ldtA, TimestampNTZNanosType(8), ldtB)
    val ntzRes = ntz.select(coalesce(col("a"), col("b")).as("c"))
    assert(ntzRes.schema("c").dataType === TimestampNTZNanosType(8))
  }

  test("SPARK-57454: CASE WHEN widens nanosecond timestamps") {
    val ltz = twoCols(TimestampType, instantA, TimestampLTZNanosType(9), instantB)
    val ltzRes = ltz.selectExpr("CASE WHEN a < b THEN a ELSE b END AS c")
    assert(ltzRes.schema("c").dataType === TimestampLTZNanosType(9))

    // Mixed time-zone families widen to the LTZ family.
    val mixed = twoCols(TimestampNTZType, ldtA, TimestampLTZNanosType(9), instantB)
    val mixedRes = mixed.selectExpr("CASE WHEN true THEN a ELSE b END AS c")
    assert(mixedRes.schema("c").dataType === TimestampLTZNanosType(9))
  }

  test("SPARK-57454: IN widens nanosecond timestamps") {
    // a (p=7) and b (p=9) hold different instants, so `a IN (b)` is false but still type-checks.
    val ltz = twoCols(TimestampLTZNanosType(7), instantA, TimestampLTZNanosType(9), instantB)
    checkAnswer(ltz.selectExpr("a IN (b)"), Row(false))

    val ntz = twoCols(TimestampNTZType, ldtA, TimestampNTZNanosType(9), ldtB)
    checkAnswer(ntz.selectExpr("a IN (b)"), Row(false))
  }

  test("SPARK-57454: binary comparison widens nanosecond timestamps") {
    // Equal absolute instants stored at different precisions compare equal.
    val ltzEq = twoCols(TimestampType, instantA, TimestampLTZNanosType(9), instantA)
    checkAnswer(ltzEq.selectExpr("a = b", "a < b"), Row(true, false))

    // b is one microsecond after a.
    val ltzLt = twoCols(TimestampLTZNanosType(7), instantA, TimestampLTZNanosType(9), instantB)
    checkAnswer(ltzLt.selectExpr("a = b", "a < b"), Row(false, true))

    val ntzEq = twoCols(TimestampNTZType, ldtA, TimestampNTZNanosType(9), ldtA)
    checkAnswer(ntzEq.selectExpr("a = b", "a < b"), Row(true, false))
  }
}

// Runs the nanosecond timestamp widening tests with ANSI mode enabled explicitly.
class TimestampNanosWideningAnsiOnSuite extends TimestampNanosWideningSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")
}

// Runs the nanosecond timestamp widening tests with ANSI mode disabled explicitly.
class TimestampNanosWideningAnsiOffSuite extends TimestampNanosWideningSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "false")
}
