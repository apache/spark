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
// castToImpl: `spark.createDataFrame` is typed as the public sql.DataFrame, but `showString` is a
// classic-only method; this implicit narrows the receiver to classic.Dataset for that call.
import org.apache.spark.sql.classic.ClassicConversions.castToImpl
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * End-to-end `show()` / `collect()` rendering tests over the nanosecond-precision timestamp types
 * `TIMESTAMP_NTZ(p)` / `TIMESTAMP_LTZ(p)` (`p` in `[7, 9]`). These ride on the nanos cast-to-string
 * (`ToPrettyString` -> `ToStringBase`, which routes nanos through the Types Framework fraction
 * formatter) and the nanos external encoders, so no production change is required.
 *
 * Two distinct surfaces are pinned, neither of which the golden SQL files can express:
 *   - `Dataset.show()` string rendering: the fractional second is rendered to the type's precision
 *     with sub-precision digits floored to zero and an all-zero fraction trimmed entirely. A value
 *     that only differs below the type's precision therefore renders identically -- the display is
 *     lossy at `p`, exactly as the stored value is. (The golden `.out` files render via
 *     `hiveResultString`, a different entry point onto the same formatter; `HiveResultSuite` covers
 *     that one. `show()` is what a user actually sees at the console.)
 *   - `collect()` external round-trip: NTZ comes back as `java.time.LocalDateTime` and LTZ as
 *     `java.time.Instant` (see `RowEncoder`), and `.getNano` preserves the sub-microsecond
 *     remainder floored to the type's precision -- so two values that share `epochMicros` but
 *     differ in `nanosWithinMicro` are distinguishable after a full driver round-trip.
 *
 * The nanosecond timestamp types are gated behind a preview flag enabled by default under tests
 * (`Utils.isTesting`), so it is not set here. The session time zone is fixed to America/Los_Angeles
 * (UTC-08:00, no DST on 2020-01-01) so the `TIMESTAMP_LTZ` (`Instant`) values render
 * deterministically in wall-clock time. The two subclasses run every test with ANSI mode on/off.
 */
abstract class TimestampNanosRenderingSuiteBase extends QueryTest with SharedSparkSession {

  override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "America/Los_Angeles")

  // Single nanosecond TIMESTAMP_NTZ(p) column "c"; a null element becomes a NULL row.
  private def ntzDF(values: Seq[String], precision: Int): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(
        values.map(s => Row(if (s == null) null else LocalDateTime.parse(s)))),
      new StructType().add("c", TimestampNTZNanosType(precision)))

  // Single nanosecond TIMESTAMP_LTZ(p) column "c"; a null element becomes a NULL row.
  private def ltzDF(values: Seq[String], precision: Int): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(
        values.map(s => Row(if (s == null) null else Instant.parse(s)))),
      new StructType().add("c", TimestampLTZNanosType(precision)))

  /**
   * The data cells of `Dataset.show(truncate = 0)`, one string per (single-column) row, in row
   * order with the alignment padding stripped. `showString` frames the table as a top border,
   * a header row, a separator, one line per data row, then a bottom border; dropping the first
   * three lines and the last isolates the data rows, and `.trim` removes the right-alignment
   * padding so the exact rendered value can be compared (a prefix `contains` check could not tell
   * a precision-floored fraction from a longer one).
   */
  private def shownCells(df: DataFrame): Seq[String] = {
    val lines = df.showString(100, truncate = 0, vertical = false).split("\n").toSeq
    lines.drop(3).dropRight(1).map(_.stripPrefix("|").stripSuffix("|").trim)
  }

  // The per-precision floored rendering of ".123456789": p=7 -> 7 digits, p=8 -> 8, p=9 -> 9.
  private val frac: Map[Int, String] = Map(
    7 -> ".1234567", 8 -> ".12345678", 9 -> ".123456789")

  // ==========================================================================================
  // show() renders the fraction to the type's precision, flooring sub-precision digits and
  // trimming an all-zero fraction. NTZ is zone-independent.
  // ==========================================================================================
  test("show() renders nanosecond TIMESTAMP_NTZ to the type precision") {
    Seq(7, 8, 9).foreach { p =>
      val df = ntzDF(Seq(
        "2020-01-01T00:00:00.123456789", // floored to p digits
        "2020-01-01T00:00:00.000000001", // non-zero only at digit 9 -> survives only at p=9
        "2020-01-01T00:00:00",           // all-zero fraction -> no fraction
        null), p)
      assert(shownCells(df) === Seq(
        "2020-01-01 00:00:00" + frac(p),
        if (p == 9) "2020-01-01 00:00:00.000000001" else "2020-01-01 00:00:00",
        "2020-01-01 00:00:00",
        "NULL"))
    }
  }

  test("show() renders nanosecond TIMESTAMP_LTZ in the session zone to the type precision") {
    Seq(7, 8, 9).foreach { p =>
      // UTC instants; the session zone is UTC-08:00, so 08:00:00Z renders as 00:00:00 wall-clock.
      val df = ltzDF(Seq(
        "2020-01-01T08:00:00.123456789Z",
        "2020-01-01T08:00:00.000000001Z",
        "2020-01-01T08:00:00Z",
        null), p)
      assert(shownCells(df) === Seq(
        "2020-01-01 00:00:00" + frac(p),
        if (p == 9) "2020-01-01 00:00:00.000000001" else "2020-01-01 00:00:00",
        "2020-01-01 00:00:00",
        "NULL"))
    }
  }

  test("show() renders nanosecond timestamps nested in array / struct") {
    // A 9-digit fraction inside a complex type still renders to the element precision (p=9 here).
    val ntz = ntzDF(Seq("2020-01-01T00:00:00.123456789"), 9)
    assert(shownCells(ntz.selectExpr("array(c)")) ===
      Seq("[2020-01-01 00:00:00.123456789]"))
    assert(shownCells(ntz.selectExpr("named_struct('f', c)")) ===
      Seq("{2020-01-01 00:00:00.123456789}"))
  }

  // ==========================================================================================
  // collect() returns the external LocalDateTime / Instant, preserving the sub-microsecond
  // remainder floored to the type precision. Two values sharing epochMicros are distinguishable.
  // ==========================================================================================
  // .000000123 floors to 100ns at p=7, 120ns at p=8, 123ns at p=9; .000000999 -> 900 / 990 / 999.
  private def flooredNano(base: Int, p: Int): Int = p match {
    case 7 => base / 100 * 100
    case 8 => base / 10 * 10
    case 9 => base
  }

  test("collect() over nanosecond TIMESTAMP_NTZ preserves the precision-floored remainder") {
    Seq(7, 8, 9).foreach { p =>
      val df = ntzDF(Seq(
        "2020-01-01T00:00:00.000000123",
        "2020-01-01T00:00:00.000000999",
        null), p)
      val got = df.collect().map(r => Option(r.getAs[LocalDateTime]("c")).map(_.getNano)).toSet
      assert(got === Set(Some(flooredNano(123, p)), Some(flooredNano(999, p)), None))
      // The two non-null values share epochMicros yet stay distinct at every supported precision.
      assert(flooredNano(123, p) != flooredNano(999, p))
    }
  }

  test("collect() over nanosecond TIMESTAMP_LTZ preserves the precision-floored remainder") {
    Seq(7, 8, 9).foreach { p =>
      val df = ltzDF(Seq(
        "2020-01-01T00:00:00.000000123Z",
        "2020-01-01T00:00:00.000000999Z",
        null), p)
      val got = df.collect().map(r => Option(r.getAs[Instant]("c")).map(_.getNano)).toSet
      assert(got === Set(Some(flooredNano(123, p)), Some(flooredNano(999, p)), None))
    }
  }
}

// Runs the nanosecond timestamp rendering tests with ANSI mode enabled explicitly.
class TimestampNanosRenderingAnsiOnSuite extends TimestampNanosRenderingSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")
}

// Runs the nanosecond timestamp rendering tests with ANSI mode disabled explicitly.
class TimestampNanosRenderingAnsiOffSuite extends TimestampNanosRenderingSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "false")
}
