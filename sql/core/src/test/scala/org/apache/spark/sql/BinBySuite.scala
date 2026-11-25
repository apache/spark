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

import scala.concurrent.duration._

import org.apache.spark.{SparkArithmeticException, SparkConf, SparkException, SparkThrowable}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskStart}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.{BinByExec, FilterExec, InputAdapter, ProjectExec, RangeExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.functions.{col, expr, lit}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class BinBySuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
      .set(SQLConf.ENABLE_BIN_BY_CODEGEN.key, "true")
  }

  private def tsAt(s: String, zone: ZoneId = ZoneOffset.UTC): Timestamp =
    Timestamp.from(LocalDateTime.parse(s.replace(' ', 'T')).atZone(zone).toInstant)

  private def ntz(s: String): LocalDateTime = LocalDateTime.parse(s.replace(' ', 'T'))

  private def ratio(overlapMicros: Long, totalMicros: Long): Double =
    overlapMicros.toDouble / totalMicros.toDouble

  private def checkBinByDatetimeOverflow(
      rangeStart: LocalDateTime,
      rangeEnd: LocalDateTime,
      binWidthSql: String,
      originSql: String): Unit = {
    withSQLConf(SQLConf.BIN_BY_ENABLED.key -> "true") {
      withTempView("bin_by_overflow") {
        Seq((rangeStart, rangeEnd, 1.0d))
          .toDF("ts_start", "ts_end", "value")
          .createOrReplaceTempView("bin_by_overflow")

        checkError(
          exception = intercept[SparkArithmeticException] {
            spark.sql(
              s"""SELECT * FROM bin_by_overflow
                 |BIN BY (
                 |  RANGE ts_start TO ts_end BIN WIDTH $binWidthSql
                 |  ALIGN TO $originSql
                 |  DISTRIBUTE UNIFORM (value))""".stripMargin).collect()
          },
          condition = "DATETIME_OVERFLOW",
          parameters = Map("operation" -> "compute BIN BY bucket boundaries"))
      }
    }
  }

  private def apiInput: DataFrame = spark.sql(
    """SELECT TIMESTAMP '2024-01-01 00:00:00' AS start,
      |  TIMESTAMP '2024-01-01 00:10:00' AS end, 100.0D AS value""".stripMargin)

  private def binByExecArgs(sqlText: String): String = {
    val plan = spark.sql(sqlText).queryExecution.executedPlan
    val binByExec = plan.collectFirst {
      case b: BinByExec => b
    }.getOrElse(fail(s"No BinByExec in plan:\n$plan"))
    // exprIds are non-deterministic; normalize them to #x as the golden files do.
    binByExec.simpleString(SQLConf.get.maxToStringFields).replaceAll("#\\d+", "#x")
  }

  // Nodes that run in the generated code of `stage`. An InputAdapter is the stage boundary.
  private def stageNodes(stage: WholeStageCodegenExec): Seq[SparkPlan] = {
    def collect(node: SparkPlan): Seq[SparkPlan] = node match {
      case _: InputAdapter => Nil
      case _ => node +: node.children.flatMap(collect)
    }
    collect(stage.child)
  }

  // The generated stage that runs BIN BY, if any.
  private def binByCodegenStage(plan: SparkPlan): Option[WholeStageCodegenExec] =
    plan.collectFirst {
      case w: WholeStageCodegenExec if stageNodes(w).exists(_.isInstanceOf[BinByExec]) => w
    }

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

  test("BIN BY scales every copy of a DISTRIBUTE column") {
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      val df = spark.sql(
        """SELECT * FROM (SELECT *, value, value AS raw_value FROM VALUES
          |  (1, TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:10:00', 100.0D),
          |  (2, CAST(NULL AS TIMESTAMP), TIMESTAMP '2024-01-01 00:10:00', 10.0D)
          |  AS metrics(id, ts_start, ts_end, value))
          |BIN BY (
          |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00' DISTRIBUTE UNIFORM (value))""".stripMargin)
      val plan = df.queryExecution.executedPlan
      // A projection above BIN BY would read both copies from the first one.
      assert(plan.collectFirst {
        case p: ProjectExec if p.exists(_.isInstanceOf[BinByExec]) => p
      }.isEmpty, plan)
      val codegen = spark.sessionState.conf.getConf(SQLConf.ENABLE_BIN_BY_CODEGEN)
      assert(binByCodegenStage(plan).isDefined == codegen, plan)
      // Read the rows by position, because `collect` reads both copies from the first one.
      val rows = plan.executeCollectPublic().toSeq
        .map(row => Seq(row.get(0), row.get(3), row.get(4), row.get(5)))
        .sortBy(_.head.asInstanceOf[Int])
      // Both copies of `value` are scaled, or NULL for a NULL range. `raw_value` is not scaled.
      assert(rows == Seq(
        Seq[Any](1, 50.0, 50.0, 100.0),
        Seq[Any](1, 50.0, 50.0, 100.0),
        Seq[Any](2, null, null, 10.0)))
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

  test("BIN BY places multi-bin civil-time boundaries on the absolute grid across DST") {
    val la = ZoneId.of("America/Los_Angeles")
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      // Boundaries of a 36h width must lie on the grid bin_start(k) = ALIGN_TO + k * 36h. Across
      // the 2024-03-10 spring-forward, a forward walk would drift 1h off the grid from bin 1 on.
      // This range spans four bins.
      val df = spark.sql(
        """SELECT bin_start, bin_end, bin_distribute_ratio, value
          |FROM VALUES
          |  (TIMESTAMP '2024-03-09 06:00:00', TIMESTAMP '2024-03-13 18:00:00', 100.0D)
          |  AS metrics(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '36' HOUR
          |  ALIGN TO TIMESTAMP '2024-03-09 00:00:00' DISTRIBUTE UNIFORM (value))
          |ORDER BY bin_start""".stripMargin)
      val h = 3600L * 1000000L
      val total = 107 * h      // 30h + 35h + 36h + 6h
      // Bin 2 starts at 2024-03-12 00:00 LA (07:00 UTC); a forward walk would report 08:00 UTC.
      checkAnswer(df, Seq(
        Row(tsAt("2024-03-09 00:00:00", la), tsAt("2024-03-10 13:00:00", la),
          ratio(30 * h, total), 100.0 * ratio(30 * h, total)),
        Row(tsAt("2024-03-10 13:00:00", la), tsAt("2024-03-12 00:00:00", la),
          ratio(35 * h, total), 100.0 * ratio(35 * h, total)),
        Row(tsAt("2024-03-12 00:00:00", la), tsAt("2024-03-13 12:00:00", la),
          ratio(36 * h, total), 100.0 * ratio(36 * h, total)),
        Row(tsAt("2024-03-13 12:00:00", la), tsAt("2024-03-15 00:00:00", la),
          ratio(6 * h, total), 100.0 * ratio(6 * h, total))))
    }
  }

  test("BIN BY reports the same civil-time bin boundary regardless of where a row starts") {
    val la = ZoneId.of("America/Los_Angeles")
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      // Both rows span the bin starting 2024-03-12 00:00 LA: one walks into it from bin 0, the
      // other starts inside it. Both must report the same bin_start; a forward walk would report it
      // an hour apart, splitting GROUP BY bin_start.
      val df = spark.sql(
        """SELECT ts_start, bin_start, bin_end
          |FROM VALUES
          |  (TIMESTAMP '2024-03-09 06:00:00', TIMESTAMP '2024-03-13 06:00:00', 1.0D),
          |  (TIMESTAMP '2024-03-12 03:00:00', TIMESTAMP '2024-03-13 06:00:00', 1.0D)
          |  AS metrics(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '36' HOUR
          |  ALIGN TO TIMESTAMP '2024-03-09 00:00:00' DISTRIBUTE UNIFORM (value))
          |WHERE bin_start = TIMESTAMP '2024-03-12 00:00:00'
          |ORDER BY ts_start""".stripMargin)
      checkAnswer(df, Seq(
        Row(tsAt("2024-03-09 06:00:00", la), tsAt("2024-03-12 00:00:00", la),
          tsAt("2024-03-13 12:00:00", la)),
        Row(tsAt("2024-03-12 03:00:00", la), tsAt("2024-03-12 00:00:00", la),
          tsAt("2024-03-13 12:00:00", la))))
    }
  }

  test("BIN BY emits a zero-width bin at a whole-day zone skip") {
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "Pacific/Apia") {
      // Apia skipped all of 2011-12-30 (UTC-11 -> UTC+13), so two grid boundaries land on the same
      // instant, emitting a zero-width ratio-0 bin (bin_start == bin_end). The real bins still tile
      // the range, so ratios sum to 1.0. Compared as UTC instants: a civil label on the skipped day
      // is ambiguous.
      val utc = ZoneOffset.UTC
      val df = spark.sql(
        """SELECT bin_start, bin_end, bin_distribute_ratio, value
          |FROM VALUES
          |  (TIMESTAMP '2011-12-29 12:00:00', TIMESTAMP '2011-12-31 12:00:00', 100.0D)
          |  AS metrics(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '1' DAY
          |  ALIGN TO TIMESTAMP '2011-12-25 00:00:00' DISTRIBUTE UNIFORM (value))
          |ORDER BY bin_start, bin_end""".stripMargin)
      checkAnswer(df, Seq(
        Row(tsAt("2011-12-29 10:00:00", utc), tsAt("2011-12-30 10:00:00", utc), 0.5, 50.0),
        Row(tsAt("2011-12-30 10:00:00", utc), tsAt("2011-12-30 10:00:00", utc), 0.0, 0.0),
        Row(tsAt("2011-12-30 10:00:00", utc), tsAt("2011-12-31 10:00:00", utc), 0.5, 50.0)))
    }
  }

  test("BIN BY places the zero-length range bin on the grid for a non-whole-day width") {
    val la = ZoneId.of("America/Los_Angeles")
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      // A zero-length range emits one ratio-1.0 row for its bin. Its bin's 36h step crosses the
      // spring-forward, so bin_end must be the grid boundary (2024-03-12 00:00), not the walked
      // bin_start + 36h (2024-03-12 01:00).
      val df = spark.sql(
        """SELECT bin_start, bin_end, bin_distribute_ratio, value
          |FROM VALUES
          |  (TIMESTAMP '2024-03-11 12:00:00', TIMESTAMP '2024-03-11 12:00:00', 100.0D)
          |  AS metrics(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '36' HOUR
          |  ALIGN TO TIMESTAMP '2024-03-09 00:00:00' DISTRIBUTE UNIFORM (value))""".stripMargin)
      checkAnswer(df, Seq(
        Row(tsAt("2024-03-10 13:00:00", la), tsAt("2024-03-12 00:00:00", la), 1.0, 100.0)))
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

  test("BIN BY raises DATETIME_OVERFLOW when the next bin index is not representable") {
    val maxTimestamp = DateTimeUtils.microsToLocalDateTime(Long.MaxValue)
    checkBinByDatetimeOverflow(
      rangeStart = maxTimestamp,
      rangeEnd = maxTimestamp,
      binWidthSql = "INTERVAL '0.000001' SECOND",
      originSql = "TIMESTAMP_NTZ '1970-01-01 00:00:00'")
  }

  test("BIN BY raises DATETIME_OVERFLOW when the range duration is not representable") {
    checkBinByDatetimeOverflow(
      rangeStart = DateTimeUtils.microsToLocalDateTime(Long.MinValue),
      rangeEnd = DateTimeUtils.microsToLocalDateTime(Long.MaxValue),
      binWidthSql = "INTERVAL '1' SECOND",
      originSql = "TIMESTAMP_NTZ '1970-01-01 00:00:00'")
  }

  test("BIN BY raises DATETIME_OVERFLOW when locating a zero-length range bin overflows") {
    val maxTimestamp = DateTimeUtils.microsToLocalDateTime(Long.MaxValue)
    checkBinByDatetimeOverflow(
      rangeStart = maxTimestamp,
      rangeEnd = maxTimestamp,
      binWidthSql = "INTERVAL '1' DAY",
      originSql = "TIMESTAMP_NTZ '1969-12-31 23:59:59.999999'")
  }

  test("BIN BY raises DATETIME_OVERFLOW when locating a non-empty range bin overflows") {
    checkBinByDatetimeOverflow(
      rangeStart = DateTimeUtils.microsToLocalDateTime(Long.MaxValue - 1L),
      rangeEnd = DateTimeUtils.microsToLocalDateTime(Long.MaxValue),
      binWidthSql = "INTERVAL '1' DAY",
      originSql = "TIMESTAMP_NTZ '1969-12-31 23:59:59.999998'")
  }

  test("BIN BY raises DATETIME_OVERFLOW when advancing a bin index overflows") {
    checkBinByDatetimeOverflow(
      rangeStart = DateTimeUtils.microsToLocalDateTime(Long.MaxValue - 1L),
      rangeEnd = DateTimeUtils.microsToLocalDateTime(Long.MaxValue),
      binWidthSql = "INTERVAL '0.000001' SECOND",
      originSql = "TIMESTAMP_NTZ '1969-12-31 23:59:59.999999'")
  }

  test("BIN BY raises DATETIME_OVERFLOW when a multi-day bin boundary overflows") {
    checkBinByDatetimeOverflow(
      rangeStart = ntz("2020-01-01 00:00:00"),
      rangeEnd = ntz("2020-01-02 00:00:00"),
      binWidthSql = "INTERVAL '106751990' DAY",
      originSql = "TIMESTAMP_NTZ '2000-01-01 00:00:00'")
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

  test("BIN BY codegen fuses wide output and bin filtering") {
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.ENABLE_BIN_BY_CODEGEN.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      withTempView("bin_by_codegen") {
        spark.range(start = 0L, end = 1000L, step = 1L, numPartitions = 1)
          .selectExpr(
            "id",
            "concat('payload-', id) AS payload",
            "named_struct('ids', array(id, id + 1), 'label', concat('row-', id)) AS nested",
            "TIMESTAMP '2024-01-01 00:02:00' AS ts_start",
            "TIMESTAMP '2024-01-01 00:17:00' AS ts_end",
            "CAST(id + 1 AS FLOAT) AS f",
            "CAST(id * 2 + 1 AS DOUBLE) AS d")
          .createOrReplaceTempView("bin_by_codegen")
        val query =
          """SELECT id, payload, nested, f, d, bin_start, bin_end, bin_distribute_ratio
            |FROM bin_by_codegen BIN BY (
            |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE
            |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00' DISTRIBUTE UNIFORM (f, d))
            |WHERE bin_start >= TIMESTAMP '2024-01-01 00:05:00'""".stripMargin
        val expected = withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
          sql(query).collect().toSeq
        }
        assert(expected.length == 3000)
        val generated = sql(query)
        val plan = generated.queryExecution.executedPlan
        val stage = binByCodegenStage(plan)
          .getOrElse(fail(s"BIN BY was not code generated:\n$plan"))
        assert(stageNodes(stage).exists(_.isInstanceOf[FilterExec]))
        assert(stageNodes(stage).exists(_.isInstanceOf[ProjectExec]))
        val binBy = stageNodes(stage).collectFirst { case b: BinByExec => b }.get
        // BIN BY's child runs in its own generated stage.
        binBy.child match {
          case InputAdapter(childStage: WholeStageCodegenExec) =>
            assert(stageNodes(childStage).exists(_.isInstanceOf[RangeExec]), plan)
          case other => fail(s"The child of BIN BY is not a separate generated stage:\n$other")
        }
        assert(!stageNodes(stage).exists(_.isInstanceOf[RangeExec]), plan)
        checkAnswer(generated, expected)
        assert(binBy.metrics("numOutputRows").value == 4000L)
      }
    }
  }

  gridTest("BIN BY codegen splits its consume for a parent that reads every column")(
      Seq(false, true)) { split =>
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.ENABLE_BIN_BY_CODEGEN.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
        SQLConf.WHOLESTAGE_SPLIT_CONSUME_FUNC_BY_OPERATOR.key -> split.toString) {
      // DISTINCT reads every BIN BY column, so the consume code can be split.
      val query =
        """SELECT DISTINCT * FROM VALUES
          |  (1, TIMESTAMP '2024-01-01 00:02:00', TIMESTAMP '2024-01-01 00:17:00', 150.0D),
          |  (2, CAST(NULL AS TIMESTAMP), TIMESTAMP '2024-01-01 00:10:00', 100.0D),
          |  (3, TIMESTAMP '2024-01-01 00:04:00', TIMESTAMP '2024-01-01 00:06:00', 20.0D)
          |  AS metrics(id, ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00' DISTRIBUTE UNIFORM (value))""".stripMargin
      val expected = withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
        sql(query).collect().toSeq
      }
      assert(expected.length == 7)
      val df = sql(query)
      val plan = df.queryExecution.executedPlan
      val stage = binByCodegenStage(plan)
        .getOrElse(fail(s"BIN BY was not code generated:\n$plan"))
      assert(stageNodes(stage).exists(_.isInstanceOf[HashAggregateExec]), plan)
      assert(stage.doCodeGen()._2.body.contains("hashAgg_doConsume") == split)
      checkAnswer(df, expected)
    }
  }

  gridTest("BIN BY codegen handles mixed null and zero-length ranges")(Seq(false, true)) {
    codegen =>
      withSQLConf(
          SQLConf.BIN_BY_ENABLED.key -> "true",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString) {
        val df = sql(
          """SELECT id, bin_start, bin_end, bin_distribute_ratio, f, d
            |FROM VALUES
            |  (1, NULL, TIMESTAMP '2024-01-01 00:10:00', 100.0F, 200.0D),
            |  (2, TIMESTAMP '2024-01-01 00:00:00', NULL, 100.0F, 200.0D),
            |  (3, TIMESTAMP '2024-01-01 00:02:00', TIMESTAMP '2024-01-01 00:02:00',
            |    100.0F, 200.0D),
            |  (4, TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:10:00',
            |    NULL, 200.0D),
            |  (5, TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:10:00',
            |    100.0F, NULL),
            |  (6, NULL, NULL, 100.0F, 200.0D)
            |  AS t(id, ts_start, ts_end, f, d)
            |BIN BY (
            |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE
            |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00' DISTRIBUTE UNIFORM (f, d))
            |""".stripMargin)
        checkAnswer(df, Seq(
          Row(1, null, null, null, null, null),
          Row(2, null, null, null, null, null),
          Row(3, tsAt("2024-01-01 00:00:00"), tsAt("2024-01-01 00:05:00"),
            1.0, 100.0f, 200.0),
          Row(4, tsAt("2024-01-01 00:00:00"), tsAt("2024-01-01 00:05:00"),
            0.5, null, 100.0),
          Row(4, tsAt("2024-01-01 00:05:00"), tsAt("2024-01-01 00:10:00"),
            0.5, null, 100.0),
          Row(5, tsAt("2024-01-01 00:00:00"), tsAt("2024-01-01 00:05:00"),
            0.5, 50.0f, null),
          Row(5, tsAt("2024-01-01 00:05:00"), tsAt("2024-01-01 00:10:00"),
            0.5, 50.0f, null),
          Row(6, null, null, null, null, null)))
      }
  }

  test("BIN BY codegen preserves nondeterministic payloads across bins and under LIMIT") {
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      withTempView("bin_by_codegen") {
        spark.range(start = 0L, end = 100L, step = 1L, numPartitions = 1)
          .selectExpr(
            "id",
            "rand(42) AS payload",
            "TIMESTAMP '2024-01-01 00:00:00' AS ts_start",
            "TIMESTAMP '2024-01-01 00:10:00' AS ts_end",
            "100.0D AS value")
          .createOrReplaceTempView("bin_by_codegen")
        val query =
          """SELECT id, payload, value, bin_start FROM bin_by_codegen BIN BY (
            |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE
            |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00' DISTRIBUTE UNIFORM (value))
            |""".stripMargin
        val expected = withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
          sql(query).collect().toSeq
        }
        checkAnswer(sql(query), expected)
        assert(expected.groupBy(_.getLong(0)).values.forall { bins =>
          bins.length == 2 && bins.map(_.getDouble(1)).distinct.length == 1
        })
        checkAnswer(sql(query).limit(5), expected.take(5))
      }
    }
  }

  gridTest("BIN BY stops expanding a row at a LIMIT in the same stage")(Seq(false, true)) {
    codegen =>
      withSQLConf(
          SQLConf.BIN_BY_ENABLED.key -> "true",
          SQLConf.ENABLE_BIN_BY_CODEGEN.key -> codegen.toString,
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        // One input row spans 1000 one-minute bins. Both paths stop after the 5 bins that the
        // LIMIT takes.
        val df = sql(
          """SELECT count(*) FROM (
            |  SELECT bin_start FROM VALUES
            |    (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 16:40:00', 100.0D)
            |    AS t(ts_start, ts_end, value)
            |  BIN BY (
            |    RANGE ts_start TO ts_end BIN WIDTH INTERVAL '1' MINUTE
            |    ALIGN TO TIMESTAMP '2024-01-01 00:00:00' DISTRIBUTE UNIFORM (value))
            |  LIMIT 5)""".stripMargin)
        checkAnswer(df, Row(5L))
        val plan = df.queryExecution.executedPlan
        assert(binByCodegenStage(plan).isDefined == codegen, plan)
        val binBy = plan.collectFirst { case b: BinByExec => b }
          .getOrElse(fail(s"No BinByExec in plan:\n$plan"))
        assert(binBy.metrics("numOutputRows").value == 5L)
      }
  }

  gridTest("BIN BY stops at a top-level LIMIT after the bins it returns")(Seq(false, true)) {
    codegen =>
      withSQLConf(
          SQLConf.BIN_BY_ENABLED.key -> "true",
          SQLConf.ENABLE_BIN_BY_CODEGEN.key -> codegen.toString,
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        // One input row spans 1000 one-minute bins. The LIMIT runs outside the generated stage,
        // so the bins go to the stage output.
        val df = sql(
          """SELECT bin_start FROM VALUES
            |  (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 16:40:00', 100.0D)
            |  AS t(ts_start, ts_end, value)
            |BIN BY (
            |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '1' MINUTE
            |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00' DISTRIBUTE UNIFORM (value))
            |LIMIT 20""".stripMargin)
        assert(df.collect().length == 20)
        val plan = df.queryExecution.executedPlan
        assert(binByCodegenStage(plan).isDefined == codegen, plan)
        val binBy = plan.collectFirst { case b: BinByExec => b }
          .getOrElse(fail(s"No BinByExec in plan:\n$plan"))
        assert(binBy.metrics("numOutputRows").value == 20L)
      }
  }

  test("BIN BY returns the bins a LIMIT takes before a later bin boundary overflows") {
    withSQLConf(SQLConf.BIN_BY_ENABLED.key -> "true") {
      withTempView("bin_by_overflow") {
        val rangeStart = DateTimeUtils.microsToLocalDateTime(Long.MaxValue - 3L)
        Seq((rangeStart, DateTimeUtils.microsToLocalDateTime(Long.MaxValue), 1.0d))
          .toDF("ts_start", "ts_end", "value")
          .createOrReplaceTempView("bin_by_overflow")
        // The third bin overflows the bin index. LIMIT 1 reads only the first bin.
        val df = sql(
          """SELECT bin_start FROM bin_by_overflow BIN BY (
            |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '0.000001' SECOND
            |  ALIGN TO TIMESTAMP_NTZ '1969-12-31 23:59:59.999999'
            |  DISTRIBUTE UNIFORM (value))
            |LIMIT 1""".stripMargin)
        checkAnswer(df, Row(rangeStart))
      }
    }
  }

  test("BIN BY codegen checks for a task kill while it expands one row") {
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.ENABLE_BIN_BY_CODEGEN.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      // Cancel the stage after its task starts to expand the row. Then only the kill check in the
      // bin loop can stop the task.
      val listener = new SparkListener {
        override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
          val stageId = taskStart.stageId
          new Thread(() => {
            Thread.sleep(2000)
            sparkContext.cancelStage(stageId)
          }).start()
        }
      }
      sparkContext.addSparkListener(listener)
      try {
        // One input row spans about 10^15 one-microsecond bins. The aggregate in the same stage
        // takes every bin, so the loop does not return between bins.
        val ex = intercept[SparkException] {
          sql(
            """SELECT count(*) FROM VALUES
              |  (TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2030-01-01 00:00:00', 1.0D)
              |  AS t(ts_start, ts_end, value)
              |BIN BY (
              |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '0.000001' SECOND
              |  DISTRIBUTE UNIFORM (value))""".stripMargin).collect()
        }
        assert(Option(ex.getCause).getOrElse(ex).getMessage.contains("cancelled"), ex)
        sparkContext.listenerBus.waitUntilEmpty(20.seconds.toMillis)
        eventually(timeout(20.seconds)) {
          assert(sparkContext.statusTracker.getExecutorInfos.map(_.numRunningTasks()).sum == 0)
        }
      } finally {
        sparkContext.removeSparkListener(listener)
      }
    }
  }

  test("ORDER BY on a column BIN BY re-outputs without its qualifier is rejected") {
    // BIN BY re-outputs its input columns without the original qualifier, so `ORDER BY t.value`
    // cannot resolve against them; hidden-output insertion appends the column to an operator whose
    // child does not produce it, and analysis fails with MISSING_ATTRIBUTES.
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false") {
      checkError(
        exception = intercept[AnalysisException] {
          spark.sql(
            """SELECT * FROM VALUES
              |  (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:05:00', 100.0D)
              |  AS t(ts_start, ts_end, value)
              |BIN BY (
              |  RANGE ts_start TO ts_end
              |  BIN WIDTH INTERVAL '5' MINUTE
              |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00'
              |  DISTRIBUTE UNIFORM (value))
              |ORDER BY t.value""".stripMargin)
        },
        condition = "MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION",
        parameters = Map(
          "missingAttributes" -> "\"value\"",
          "input" -> ("\"ts_start\", \"ts_end\", \"value\", \"bin_start\", \"bin_end\", " +
            "\"bin_distribute_ratio\""),
          "operator" -> "!Sort \\[value#\\d+ ASC NULLS FIRST\\], true",
          "operation" -> "\"value\""),
        matchPVals = true,
        queryContext =
          Array(ExpectedContext(fragment = "ORDER BY t.value", start = 271, stop = 286)))
    }
  }

  test("BIN BY resolves a bare ORDER BY on a re-output DISTRIBUTE column that is not projected") {
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      // A bare ORDER BY on the re-output DISTRIBUTE column resolves even when it is not projected.
      val df = spark.sql(
        """SELECT bin_start
          |FROM VALUES
          |  (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:10:00', 100.0D)
          |  AS metrics(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00' DISTRIBUTE UNIFORM (value))
          |ORDER BY value""".stripMargin)
      checkAnswer(df, Seq(
        Row(tsAt("2024-01-01 00:00:00")),
        Row(tsAt("2024-01-01 00:05:00"))))
    }
  }

  test("BIN BY rejects a source-qualified reference to a re-output DISTRIBUTE column") {
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      // The re-output DISTRIBUTE column drops its source qualifier, so `metrics.value` fails.
      checkError(
        exception = intercept[AnalysisException] {
          spark.sql(
            """SELECT metrics.value
              |FROM VALUES
              |  (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:10:00', 100.0D)
              |  AS metrics(ts_start, ts_end, value)
              |BIN BY (
              |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE
              |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00'
              |  DISTRIBUTE UNIFORM (value))""".stripMargin)
        },
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        parameters = Map(
          "objectName" -> "`metrics`.`value`",
          "proposal" -> ("`metrics`.`ts_end`, `value`, `bin_start`, " +
            "`metrics`.`ts_start`, `bin_end`")),
        queryContext =
          Array(ExpectedContext(fragment = "metrics.value", start = 7, stop = 19)))
    }
  }

  test("BIN BY keeps a file-source metadata column reachable in ORDER BY and SELECT") {
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      withTempDir { dir =>
        val path = new java.io.File(dir, "metrics").getCanonicalPath
        spark.sql(
          """SELECT TIMESTAMP '2024-01-01 00:00:00' AS ts_start,
            |       TIMESTAMP '2024-01-01 00:10:00' AS ts_end, 100.0D AS value""".stripMargin)
          .write.mode("overwrite").parquet(path)
        val binBy =
          s"""parquet.`$path` BIN BY (RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE
             |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00' DISTRIBUTE UNIFORM (value))""".stripMargin

        // `_metadata` is a hidden metadata column; BIN BY carries it through for a bare ORDER BY.
        checkAnswer(
          spark.sql(s"SELECT bin_start FROM $binBy ORDER BY _metadata.file_size"),
          Seq(Row(tsAt("2024-01-01 00:00:00")), Row(tsAt("2024-01-01 00:05:00"))))

        // Also selectable through BIN BY.
        checkAnswer(
          spark.sql(s"SELECT bin_start, _metadata.file_size > 0 FROM $binBy"),
          Seq(Row(tsAt("2024-01-01 00:00:00"), true), Row(tsAt("2024-01-01 00:05:00"), true)))
      }
    }
  }

  test("EXPLAIN for LTZ BIN BY") {
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val rendered = binByExecArgs(
        """SELECT * FROM VALUES
          |  (TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:05:00', 100.0D)
          |  AS t(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP '2024-01-01 00:00:00' DISTRIBUTE UNIFORM (value))""".stripMargin)
      assert(rendered ==
        "BinBy range=[ts_start#x: timestamp, ts_end#x: timestamp], " +
          "binWidth=INTERVAL '0 00:05:00' DAY TO SECOND, alignTo=2024-01-01 00:00:00, " +
          "distribute=[value#x: double], scaledDistribute=[value#x: double], " +
          "appends=[bin_start#x: timestamp, bin_end#x: timestamp, " +
          "bin_distribute_ratio#x: double], zone=America/Los_Angeles")
    }
  }

  test("EXPLAIN for TIMESTAMP_NTZ BIN BY") {
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val rendered = binByExecArgs(
        """SELECT * FROM VALUES
          |  (TIMESTAMP_NTZ '2024-01-01 00:00:00', TIMESTAMP_NTZ '2024-01-01 00:05:00', 100.0D)
          |  AS t(ts_start, ts_end, value)
          |BIN BY (
          |  RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE
          |  ALIGN TO TIMESTAMP_NTZ '2024-01-01 00:00:00'
          |  DISTRIBUTE UNIFORM (value))""".stripMargin)
      assert(rendered ==
        "BinBy range=[ts_start#x: timestamp_ntz, ts_end#x: timestamp_ntz], " +
          "binWidth=INTERVAL '0 00:05:00' DAY TO SECOND, alignTo=2024-01-01 00:00:00, " +
          "distribute=[value#x: double], scaledDistribute=[value#x: double], " +
          "appends=[bin_start#x: timestamp_ntz, bin_end#x: timestamp_ntz, " +
          "bin_distribute_ratio#x: double]")
    }
  }

  test("BIN BY DataFrame API supports optional alignment and output names") {
    withSQLConf(
        SQLConf.BIN_BY_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      val input = apiInput
      val width = expr("INTERVAL 5 MINUTES")
      val renamed = input.binBy(
        rangeStart = input("start"),
        rangeEnd = input("end"),
        binWidth = width,
        distributeUniform = Array(input("value")),
        alignTo = null,
        binStartName = "bin.start",
        binEndName = "bin`end",
        binDistributeRatioName = "ratio")
      assert(renamed.columns.toSeq ==
        Seq("start", "end", "value", "bin.start", "bin`end", "ratio"))

      val aligned = input.binBy(
        rangeStart = col("start"),
        rangeEnd = col("end"),
        binWidth = width,
        distributeUniform = Array(col("value")),
        alignTo = expr("TIMESTAMP '2024-01-01 00:02:00'"))
      checkAnswer(aligned.select("value", "bin_start", "bin_distribute_ratio"), Seq(
        Row(20.0, tsAt("2023-12-31 23:57:00"), 0.2),
        Row(50.0, tsAt("2024-01-01 00:02:00"), 0.5),
        Row(30.0, tsAt("2024-01-01 00:07:00"), 0.3)))
      val result = input.binBy(
        rangeStart = input("start"),
        rangeEnd = input("end"),
        binWidth = width,
        distributeUniform = Array(input("value")))
      assert(result.columns.toSeq ==
        Seq("start", "end", "value", "bin_start", "bin_end", "bin_distribute_ratio"))
      checkAnswer(result.select("value", "bin_start", "bin_end", "bin_distribute_ratio"), Seq(
        Row(50.0, tsAt("2024-01-01 00:00:00"), tsAt("2024-01-01 00:05:00"), 0.5),
        Row(50.0, tsAt("2024-01-01 00:05:00"), tsAt("2024-01-01 00:10:00"), 0.5)))
      checkAnswer(renamed, result)
    }
  }

  test("BIN BY DataFrame API validates column inputs and an explicit NULL origin") {
    withSQLConf(SQLConf.BIN_BY_ENABLED.key -> "true") {
      val input = apiInput
      val width = expr("INTERVAL 5 MINUTES")
      checkError(
        exception = intercept[AnalysisException] {
          input.binBy(
            rangeStart = col("start"),
            rangeEnd = col("end"),
            binWidth = width,
            distributeUniform = Array.empty[Column])
        },
        condition = "BIN_BY_MISSING_DISTRIBUTE",
        parameters = Map.empty)
      checkError(
        exception = intercept[AnalysisException] {
          input.binBy(
            rangeStart = col("start"),
            rangeEnd = col("end"),
            binWidth = width,
            distributeUniform = Array(col("value"), input("value")))
        },
        condition = "BIN_BY_DUPLICATE_DISTRIBUTE_COLUMN",
        parameters = Map("columnName" -> "`value`"))
      checkError(
        exception = intercept[AnalysisException] {
          input.binBy(
            rangeStart = col("start"),
            rangeEnd = col("end"),
            binWidth = width,
            distributeUniform = Array(col("value") + 1))
        },
        condition = "BIN_BY_REQUIRES_TOP_LEVEL_COLUMN",
        parameters = Map("columnName" -> "\"(value + 1)\""))
      checkError(
        exception = intercept[AnalysisException] {
          input.binBy(
            rangeStart = col("start"),
            rangeEnd = col("end"),
            binWidth = width,
            distributeUniform = Array(col("value").as("v")))
        },
        condition = "BIN_BY_REQUIRES_TOP_LEVEL_COLUMN",
        parameters = Map("columnName" -> "\"value AS v\""))
      checkError(
        exception = intercept[AnalysisException] {
          input.binBy(
            rangeStart = col("start").as("s"),
            rangeEnd = col("end"),
            binWidth = width,
            distributeUniform = Array(col("value")))
        },
        condition = "BIN_BY_REQUIRES_TOP_LEVEL_COLUMN",
        parameters = Map("columnName" -> "\"start AS s\""))
      checkError(
        exception = intercept[AnalysisException] {
          input.binBy(
            rangeStart = col("start").cast("timestamp_ntz"),
            rangeEnd = col("end"),
            binWidth = width,
            distributeUniform = Array(col("value")))
        },
        condition = "BIN_BY_REQUIRES_TOP_LEVEL_COLUMN",
        parameters = Map("columnName" -> "\"CAST(start AS TIMESTAMP_NTZ)\""))
      checkError(
        exception = intercept[AnalysisException] {
          input.binBy(
            rangeStart = col("start"),
            rangeEnd = col("end"),
            binWidth = width,
            distributeUniform = Array(col("value")),
            alignTo = lit(null).cast("timestamp"))
        },
        condition = "BIN_BY_NULL_ARGUMENT",
        parameters = Map("clause" -> "ALIGN TO"))
    }
  }

  test("BIN BY DataFrame API respects the feature gate") {
    withSQLConf(SQLConf.BIN_BY_ENABLED.key -> "false") {
      checkError(
        exception = intercept[AnalysisException] {
          apiInput.binBy(
            rangeStart = col("start"),
            rangeEnd = col("end"),
            binWidth = expr("INTERVAL 5 MINUTES"),
            distributeUniform = Array(col("value")))
        },
        condition = "UNSUPPORTED_FEATURE.BIN_BY",
        parameters = Map.empty)
    }
  }
}

/**
 * Runs [[BinBySuite]] with BIN BY code-gen disabled. It skips the tests that set the BIN BY
 * code-gen config themselves.
 */
class BinByWithoutCodegenSuite extends BinBySuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ENABLE_BIN_BY_CODEGEN.key, "false")

  override protected def excluded: Seq[String] = Seq(
    "BIN BY codegen fuses wide output and bin filtering",
    "BIN BY codegen splits its consume for a parent that reads every column (false)",
    "BIN BY codegen splits its consume for a parent that reads every column (true)",
    "BIN BY stops expanding a row at a LIMIT in the same stage (false)",
    "BIN BY stops expanding a row at a LIMIT in the same stage (true)",
    "BIN BY stops at a top-level LIMIT after the bins it returns (false)",
    "BIN BY stops at a top-level LIMIT after the bins it returns (true)",
    "BIN BY codegen checks for a task kill while it expands one row")
}
