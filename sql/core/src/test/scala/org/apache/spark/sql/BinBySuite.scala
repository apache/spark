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

  private def tsAt(s: String, zone: ZoneId = ZoneOffset.UTC): Timestamp =
    Timestamp.from(LocalDateTime.parse(s.replace(' ', 'T')).atZone(zone).toInstant)

  private def ntz(s: String): LocalDateTime = LocalDateTime.parse(s.replace(' ', 'T'))

  private def ratio(overlapMicros: Long, totalMicros: Long): Double =
    overlapMicros.toDouble / totalMicros.toDouble

  test("BIN BY is rejected when the operator is disabled") {
    withSQLConf(SQLConf.BIN_BY_ENABLED.key -> "false") {
      // Disabled, BIN BY is rejected at analysis with UNSUPPORTED_FEATURE.BIN_BY. spark.sql eagerly
      // analyzes, so the query itself is inside the intercept.
      checkError(
        exception = intercept[SparkThrowable] {
          spark.sql(
            """SELECT * FROM VALUES
              |  (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:10:00', 100.0D)
              |  AS metrics(ts_start, ts_end, value)
              |BIN BY (
              |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE
              |  DISTRIBUTE UNIFORM (value))""".stripMargin)
        },
        condition = "UNSUPPORTED_FEATURE.BIN_BY",
        parameters = Map.empty[String, String])
    }
  }

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
        Row(tsAt("2024-01-01 00:00:00"), tsAt("2024-01-01 00:05:00"), 0.5, 50.0),
        Row(tsAt("2024-01-01 00:05:00"), tsAt("2024-01-01 00:10:00"), 0.5, 50.0)))
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
        Row(tsAt("2024-01-01 00:00:00"), 50.0f, 100.0),
        Row(tsAt("2024-01-01 00:05:00"), 50.0f, 100.0)))
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
        Row(tsAt("2024-01-01 00:00:00"), tsAt("2024-01-01 00:05:00"), 1.0, 100.0)))
    }
  }

  test("BIN BY aligns to an origin later than the range") {
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      // ALIGN TO after the range exercises a negative bucket index (origin later than the range).
      // The grid still lands on :00/:05/:10, so [00:02, 00:12) splits into 3, 5, 2 minutes.
      val df = spark.sql(
        """SELECT bin_start, bin_end, bin_distribute_ratio, value
          |FROM VALUES
          |  (TIMESTAMP '2024-01-01 00:02:00', TIMESTAMP '2024-01-01 00:12:00', 100.0D)
          |  AS metrics(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP '2024-01-01 00:20:00' DISTRIBUTE UNIFORM (value))
          |ORDER BY bin_start""".stripMargin)
      val m = 60L * 1000000L // one minute in micros
      val total = 10 * m
      checkAnswer(df, Seq(
        Row(tsAt("2024-01-01 00:00:00"), tsAt("2024-01-01 00:05:00"),
          ratio(3 * m, total), 100.0 * ratio(3 * m, total)),
        Row(tsAt("2024-01-01 00:05:00"), tsAt("2024-01-01 00:10:00"),
          ratio(5 * m, total), 100.0 * ratio(5 * m, total)),
        Row(tsAt("2024-01-01 00:10:00"), tsAt("2024-01-01 00:15:00"),
          ratio(2 * m, total), 100.0 * ratio(2 * m, total))))
    }
  }

  test("BIN BY replicates a nested struct passthrough column across a multi-bin split") {
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      // A nested struct passthrough must appear identically on every split sub-row.
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
        Row(Row(1, "x"), tsAt("2024-01-01 00:00:00"), 50.0),
        Row(Row(1, "x"), tsAt("2024-01-01 00:05:00"), 50.0)))
    }
  }

  test("BIN BY uses UTC arithmetic for a sub-day bin in a non-UTC session") {
    val la = ZoneId.of("America/Los_Angeles")
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      // Sub-day widths use UTC microsecond arithmetic, not the civil-time path the multi-day tests
      // exercise; the 5-minute grid lands on clean boundaries even in a non-UTC session.
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
        Row(tsAt("2024-01-01 00:00:00", la), tsAt("2024-01-01 00:05:00", la), 0.5, 50.0),
        Row(tsAt("2024-01-01 00:05:00", la), tsAt("2024-01-01 00:10:00", la), 0.5, 50.0)))
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

  test("BIN BY uses civil-time bin boundaries across a DST fall-back") {
    val la = ZoneId.of("America/Los_Angeles")
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      // A 1-DAY bin spanning the 2024-11-03 fall-back: the 2024-11-03 bin is 25h wide (01:00 PDT
      // repeats as 01:00 PST) while 2024-11-04 is 24h, so it gets a larger share of the range.
      val df = spark.sql(
        """SELECT bin_start, bin_end, bin_distribute_ratio, value
          |FROM VALUES
          |  (TIMESTAMP '2024-11-03 00:00:00', TIMESTAMP '2024-11-05 00:00:00', 100.0D)
          |  AS metrics(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '1' DAY
          |  ALIGN TO TIMESTAMP '2024-11-03 00:00:00' DISTRIBUTE UNIFORM (value))
          |ORDER BY bin_start""".stripMargin)
      val h = 3600L * 1000000L // one hour in micros
      val total = 49 * h       // 25h (DST day) + 24h
      checkAnswer(df, Seq(
        Row(tsAt("2024-11-03 00:00:00", la), tsAt("2024-11-04 00:00:00", la),
          ratio(25 * h, total), 100.0 * ratio(25 * h, total)),
        Row(tsAt("2024-11-04 00:00:00", la), tsAt("2024-11-05 00:00:00", la),
          ratio(24 * h, total), 100.0 * ratio(24 * h, total))))
    }
  }

  test("BIN BY uses civil-time boundaries for a compound multi-day width across DST") {
    val la = ZoneId.of("America/Los_Angeles")
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      // A 2-DAY bin across the 2024-11-03 fall-back: boundaries land two civil days apart, so the
      // first bin (Nov 2 + Nov 3) is 24h + 25h = 49h and the second (Nov 4 + Nov 5) is 48h.
      val df = spark.sql(
        """SELECT bin_start, bin_end, bin_distribute_ratio, value
          |FROM VALUES
          |  (TIMESTAMP '2024-11-02 00:00:00', TIMESTAMP '2024-11-06 00:00:00', 100.0D)
          |  AS metrics(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '2' DAY
          |  ALIGN TO TIMESTAMP '2024-11-02 00:00:00' DISTRIBUTE UNIFORM (value))
          |ORDER BY bin_start""".stripMargin)
      val h = 3600L * 1000000L // one hour in micros
      val total = 97 * h       // 49h (Nov 2 + fall-back Nov 3) + 48h (Nov 4 + Nov 5)
      checkAnswer(df, Seq(
        Row(tsAt("2024-11-02 00:00:00", la), tsAt("2024-11-04 00:00:00", la),
          ratio(49 * h, total), 100.0 * ratio(49 * h, total)),
        Row(tsAt("2024-11-04 00:00:00", la), tsAt("2024-11-06 00:00:00", la),
          ratio(48 * h, total), 100.0 * ratio(48 * h, total))))
    }
  }

  test("BIN BY executes on NTZ inputs with the epoch default origin") {
    withSQLConf(SQLConf.BIN_BY_ENABLED.key -> "true") {
      val df = spark.sql(
        """SELECT bin_start, bin_end, bin_distribute_ratio, value
          |FROM VALUES
          |  (TIMESTAMP_NTZ'2024-01-01 00:00:00', TIMESTAMP_NTZ'2024-01-01 00:10:00', 100.0D)
          |  AS t(ts_start, ts_end, value)
          |BIN BY (RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE
          |  DISTRIBUTE UNIFORM (value))
          |ORDER BY bin_start""".stripMargin)
      checkAnswer(df, Seq(
        Row(ntz("2024-01-01 00:00:00"), ntz("2024-01-01 00:05:00"), 0.5, 50.0),
        Row(ntz("2024-01-01 00:05:00"), ntz("2024-01-01 00:10:00"), 0.5, 50.0)))
    }
  }

  test("BIN BY on NTZ inputs uses UTC arithmetic and ignores the session zone across DST") {
    // A multi-day NTZ bin over the LA spring-forward: NTZ ignores the session zone, so every day is
    // a full 24h (no DST shortening), unlike the LTZ civil-time path. Two 1-day bins split evenly.
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      val df = spark.sql(
        """SELECT bin_start, bin_end, bin_distribute_ratio, value
          |FROM VALUES
          |  (TIMESTAMP_NTZ'2024-03-10 00:00:00', TIMESTAMP_NTZ'2024-03-12 00:00:00', 100.0D)
          |  AS t(ts_start, ts_end, value)
          |BIN BY (RANGE ts_start TO ts_end BIN WIDTH INTERVAL '1' DAY
          |  ALIGN TO TIMESTAMP_NTZ'2024-03-10 00:00:00' DISTRIBUTE UNIFORM (value))
          |ORDER BY bin_start""".stripMargin)
      checkAnswer(df, Seq(
        Row(ntz("2024-03-10 00:00:00"), ntz("2024-03-11 00:00:00"), 0.5, 50.0),
        Row(ntz("2024-03-11 00:00:00"), ntz("2024-03-12 00:00:00"), 0.5, 50.0)))
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
      // rangeStart == rangeEnd: one row, ratio 1.0, value kept.
      checkAnswer(df, Seq(
        Row(tsAt("2024-01-01 00:00:00"), tsAt("2024-01-01 00:05:00"), 1.0, 100.0)))
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

  test("BIN BY emits a NULL-range row with all computed columns NULL") {
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
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
      // A NULL range nulls every computed column; only the `id` passthrough survives.
      checkAnswer(df, Seq(
        Row(1, null, null, null, null),
        Row(2, null, null, null, null)))
    }
  }
}
