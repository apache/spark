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

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId, ZoneOffset}

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class BinBySuite extends QueryTest with SharedSparkSession {

  // The BIN BY tests pin the session zone to UTC, so expected timestamps must be built at UTC too
  // (Timestamp.valueOf would parse in the JVM default zone and mismatch the UTC session output).
  private def ts(s: String): Timestamp =
    Timestamp.from(LocalDateTime.parse(s.replace(' ', 'T')).toInstant(ZoneOffset.UTC))

  // Builds the instant of a civil time in `zone`, for tests that pin a non-UTC session zone.
  private def tsAt(s: String, zone: ZoneId): Timestamp =
    Timestamp.from(LocalDateTime.parse(s.replace(' ', 'T')).atZone(zone).toInstant)

  // Reproduces the operator's ratio arithmetic (`overlapMicros / totalMicros` in double) so an
  // expected `bin_distribute_ratio` matches the kernel's bits (checkAnswer compares doubles raw).
  private def ratio(overlapMicros: Long, totalMicros: Long): Double =
    overlapMicros.toDouble / totalMicros.toDouble

  private def createMetricsView(): Unit = {
    spark.sql(
      """SELECT TIMESTAMP '2024-01-01 00:00:00' AS ts_start,
        |       TIMESTAMP '2024-01-01 01:00:00' AS ts_end,
        |       CAST(1 AS DOUBLE) AS value""".stripMargin).createOrReplaceTempView("metrics")
  }

  private val binByQuery =
    """SELECT * FROM metrics BIN BY (
      |  RANGE ts_start TO ts_end
      |  BIN WIDTH INTERVAL '5' MINUTE
      |  DISTRIBUTE UNIFORM (value)
      |)""".stripMargin

  test("BIN BY splits a range into proportional sub-rows") {
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      val df = spark.sql(
        """SELECT bin_start, bin_end, bin_distribute_ratio, value
          |FROM VALUES
          |  (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:10:00', 100.0D)
          |  AS metrics(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00' DISTRIBUTE UNIFORM (value))
          |ORDER BY bin_start""".stripMargin)
      checkAnswer(df, Seq(
        Row(ts("2024-01-01 00:00:00"), ts("2024-01-01 00:05:00"), 0.5, 50.0),
        Row(ts("2024-01-01 00:05:00"), ts("2024-01-01 00:10:00"), 0.5, 50.0)))
    }
  }

  test("BIN BY scales FLOAT and DOUBLE DISTRIBUTE columns in one clause") {
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      val df = spark.sql(
        """SELECT bin_start, f, d
          |FROM VALUES
          |  (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:10:00',
          |   CAST(100.0 AS FLOAT), 200.0D)
          |  AS metrics(ts_start, ts_end, f, d)
          |BIN BY (
          |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00' DISTRIBUTE UNIFORM (f, d))
          |ORDER BY bin_start""".stripMargin)
      checkAnswer(df, Seq(
        Row(ts("2024-01-01 00:00:00"), 50.0f, 100.0),
        Row(ts("2024-01-01 00:05:00"), 50.0f, 100.0)))
    }
  }

  test("BIN BY passes a single-bin range through with ratio 1.0") {
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      val df = spark.sql(
        """SELECT bin_start, bin_end, bin_distribute_ratio, value
          |FROM VALUES
          |  (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:05:00', 100.0D)
          |  AS metrics(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00' DISTRIBUTE UNIFORM (value))""".stripMargin)
      checkAnswer(df, Seq(
        Row(ts("2024-01-01 00:00:00"), ts("2024-01-01 00:05:00"), 1.0, 100.0)))
    }
  }

  test("BIN BY composes with a downstream GROUP BY") {
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      val df = spark.sql(
        """SELECT bin_start, SUM(value) AS total
          |FROM VALUES
          |  (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:10:00', 100.0D),
          |  (TIMESTAMP '2024-01-01 00:05:00', TIMESTAMP '2024-01-01 00:10:00', 60.0D)
          |  AS metrics(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00' DISTRIBUTE UNIFORM (value))
          |GROUP BY bin_start
          |ORDER BY bin_start""".stripMargin)
      // Row 1 splits 100 into 50/50; row 2 is single-bin 60 in [00:05,00:10).
      checkAnswer(df, Seq(
        Row(ts("2024-01-01 00:00:00"), 50.0),
        Row(ts("2024-01-01 00:05:00"), 110.0)))
    }
  }

  test("BIN BY emits a NULL-range row with all computed columns NULL") {
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      // A NULL in either range column triggers the null-range path, so cover both a NULL rangeStart
      // and a NULL rangeEnd. The `id` passthrough column is non-NULL and must survive unchanged.
      val df = spark.sql(
        """SELECT id, bin_start, bin_end, bin_distribute_ratio, value
          |FROM VALUES
          |  (1, CAST(NULL AS TIMESTAMP), TIMESTAMP '2024-01-01 00:10:00', 100.0D),
          |  (2, TIMESTAMP '2024-01-01 00:00:00', CAST(NULL AS TIMESTAMP), 200.0D)
          |  AS metrics(id, ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00' DISTRIBUTE UNIFORM (value))
          |ORDER BY id""".stripMargin)
      // All computed columns are NULL: no valid bin exists so neither scaled values nor bin
      // boundaries can be computed. The non-DISTRIBUTE passthrough column `id` is unaffected.
      checkAnswer(df, Seq(
        Row(1, null, null, null, null),
        Row(2, null, null, null, null)))
    }
  }

  test("BIN BY raises BIN_BY_INVALID_RANGE for an inverted range") {
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      val df = spark.sql(
        """SELECT * FROM VALUES
          |  (TIMESTAMP '2024-01-01 00:10:00', TIMESTAMP '2024-01-01 00:00:00', 100.0D)
          |  AS metrics(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00' DISTRIBUTE UNIFORM (value))""".stripMargin)
      checkError(
        exception = intercept[SparkThrowable] {
          df.collect()
        },
        condition = "BIN_BY_INVALID_RANGE",
        parameters = Map(
          "rangeStart" -> "2024-01-01 00:10:00",
          "rangeEnd" -> "2024-01-01 00:00:00"))
    }
  }

  test("BIN BY renames the appended output columns") {
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      val df = spark.sql(
        """SELECT w_start, w_end, frac, value
          |FROM VALUES
          |  (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:05:00', 100.0D)
          |  AS metrics(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00' DISTRIBUTE UNIFORM (value)
          |  BIN_START AS w_start BIN_END AS w_end BIN_DISTRIBUTE_RATIO AS frac)""".stripMargin)
      checkAnswer(df, Seq(
        Row(ts("2024-01-01 00:00:00"), ts("2024-01-01 00:05:00"), 1.0, 100.0)))
    }
  }

  test("BIN BY emits a single ratio-1.0 row for a zero-length range") {
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      val df = spark.sql(
        """SELECT bin_start, bin_end, bin_distribute_ratio, value
          |FROM VALUES
          |  (TIMESTAMP '2024-01-01 00:02:00', TIMESTAMP '2024-01-01 00:02:00', 100.0D)
          |  AS metrics(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00' DISTRIBUTE UNIFORM (value))""".stripMargin)
      // rangeStart == rangeEnd: one row in the bin containing the instant, ratio 1.0, value kept.
      checkAnswer(df, Seq(
        Row(ts("2024-01-01 00:00:00"), ts("2024-01-01 00:05:00"), 1.0, 100.0)))
    }
  }

  test("BIN BY uses civil-time bin boundaries across a DST spring-forward") {
    val la = ZoneId.of("America/Los_Angeles")
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      // A 1-DAY bin spanning the 2024-03-10 spring-forward. Multi-day widths use civil-time
      // arithmetic in the session zone, so boundaries land on civil midnight and the 2024-03-10
      // bin is 23h wide (02:00 PST -> 03:00 PDT) while 2024-03-11 is 24h. The ratios split the
      // range by real elapsed microseconds, so the 23h bin gets a smaller share than the 24h bin.
      val df = spark.sql(
        """SELECT bin_start, bin_end, bin_distribute_ratio, value
          |FROM VALUES
          |  (TIMESTAMP '2024-03-10 00:00:00', TIMESTAMP '2024-03-12 00:00:00', 100.0D)
          |  AS metrics(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '1' DAY
          |  ALIGN TO TIMESTAMP '2024-03-10 00:00:00' DISTRIBUTE UNIFORM (value))
          |ORDER BY bin_start""".stripMargin)
      val h = 3600L * 1000000L // one hour in micros
      val total = 47 * h       // 23h (DST day) + 24h
      checkAnswer(df, Seq(
        Row(tsAt("2024-03-10 00:00:00", la), tsAt("2024-03-11 00:00:00", la),
          ratio(23 * h, total), 100.0 * ratio(23 * h, total)),
        Row(tsAt("2024-03-11 00:00:00", la), tsAt("2024-03-12 00:00:00", la),
          ratio(24 * h, total), 100.0 * ratio(24 * h, total))))
    }
  }

  test("BIN BY replicates a nested struct passthrough column across a multi-bin split") {
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      // The struct is passed through by reference on the JoinedRow left side and shared across the
      // sub-rows of one input row; the per-partition UnsafeProjection copies it at each emit, so it
      // must appear identically on every split row.
      val df = spark.sql(
        """SELECT s, bin_start, value
          |FROM VALUES
          |  (named_struct('a', 1, 'b', 'x'),
          |   TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:10:00', 100.0D)
          |  AS metrics(s, ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00' DISTRIBUTE UNIFORM (value))
          |ORDER BY bin_start""".stripMargin)
      checkAnswer(df, Seq(
        Row(Row(1, "x"), ts("2024-01-01 00:00:00"), 50.0),
        Row(Row(1, "x"), ts("2024-01-01 00:05:00"), 50.0)))
    }
  }

  test("BIN BY is rejected when the operator is disabled") {
    withSQLConf(SQLConf.BIN_BY_ENABLED.key -> "false") {
      withTempView("metrics") {
        createMetricsView()
        // Disabled, the operator is rejected at analysis with the same UNSUPPORTED_FEATURE.BIN_BY
        // condition the execution stub raises when enabled.
        checkError(
          exception = intercept[SparkThrowable] {
            spark.sql(binByQuery).queryExecution.assertAnalyzed()
          },
          condition = "UNSUPPORTED_FEATURE.BIN_BY",
          parameters = Map.empty[String, String])
      }
    }
  }

  test("BIN BY analyzes NTZ inputs and a custom ALIGN TO with renamed outputs") {
    withSQLConf(SQLConf.BIN_BY_ENABLED.key -> "true") {
      // NTZ inputs default the origin to epoch (LTZ defaults to the session-zone epoch).
      spark.sql(
        """SELECT * FROM VALUES
          |  (TIMESTAMP_NTZ'2024-01-01 00:00:00', TIMESTAMP_NTZ'2024-01-01 01:00:00', 1.0D)
          |  AS t(ts_start, ts_end, value)
          |BIN BY (RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE
          |  DISTRIBUTE UNIFORM (value))
          |""".stripMargin).queryExecution.assertAnalyzed()

      // Custom ALIGN TO origin with renamed output columns.
      spark.sql(
        """SELECT * FROM VALUES
          |  (TIMESTAMP'2024-01-01 00:00:00', TIMESTAMP'2024-01-01 02:00:00', 10.0D, 5.0D)
          |  AS t(ts_start, ts_end, a, b)
          |BIN BY (RANGE ts_start TO ts_end BIN WIDTH INTERVAL '1' HOUR
          |  ALIGN TO TIMESTAMP'2024-01-01 00:30:00'
          |  DISTRIBUTE UNIFORM (a, b)
          |  BIN_START AS w_start BIN_END AS w_end
          |  BIN_DISTRIBUTE_RATIO AS frac)
          |""".stripMargin).queryExecution.assertAnalyzed()
    }
  }
}
