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
import java.time.{Instant, LocalDateTime}

import org.apache.spark.sql.api.java.UDF0
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast}
import org.apache.spark.sql.catalyst.plans.{AsOfJoinDirection, Backward, Forward}
import org.apache.spark.sql.catalyst.plans.logical.AsOfJoin
import org.apache.spark.sql.execution.{FileSourceScanExec, ProjectExec, SortExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.SortMergeAsOfJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType

/**
 * SQL ASOF JOIN tests that exercise the sort-merge physical operator.
 * SQL ASOF JOIN uses sort-merge implicitly; only `spark.sql.join.asofJoin.enabled`
 * needs to be set (unlike the DataFrame API, which still requires
 * `spark.sql.join.sortMergeAsOfJoin.enabled`).
 *
 * Parser and analysis negative paths live in `AsOfJoinSQLSuite`.
 */
class AsOfJoinSortMergeSQLSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.SQL_ASOF_JOIN_ENABLED.key, "true")
  }

  override def afterAll(): Unit = {
    spark.conf.unset(SQLConf.SQL_ASOF_JOIN_ENABLED.key)
    super.afterAll()
  }

  private def assertUsesSortMergeAsOfJoin(query: DataFrame): Unit = {
    val plan = query.queryExecution.executedPlan
    assert(collectWithSubqueries(plan) {
      case _: SortMergeAsOfJoinExec => true
    }.nonEmpty, s"Expected SortMergeAsOfJoinExec in plan:\n$plan")
  }

  private def assertPlannedDirection(query: DataFrame, direction: AsOfJoinDirection): Unit = {
    val plan = query.queryExecution.executedPlan
    val directions = collectWithSubqueries(plan) { case j: SortMergeAsOfJoinExec => j.direction }
    assert(directions === Seq(direction),
      s"Expected one $direction SortMergeAsOfJoinExec in plan:\n$plan")
  }

  private def checkSortMergeAsOf(query: => DataFrame, expected: Seq[Row]): Unit = {
    val df = query
    assertUsesSortMergeAsOfJoin(df)
    checkAnswer(df, expected)
  }

  private def setupTradeQuoteViews(): Unit = {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW trades(trade_time, symbol, quantity) AS
        |VALUES (TIMESTAMP '2026-06-29 10:00:05', 'AAPL', 100),
        |       (TIMESTAMP '2026-06-29 10:00:11', 'AAPL', 200),
        |       (TIMESTAMP '2026-06-29 10:00:12', 'MSFT',  50),
        |       (TIMESTAMP '2026-06-29 09:59:59', 'GOOG',  30)
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW quotes(quote_time, symbol, bid_price) AS
        |VALUES (TIMESTAMP '2026-06-29 10:00:00', 'AAPL', 180.10),
        |       (TIMESTAMP '2026-06-29 10:00:07', 'AAPL', 180.15),
        |       (TIMESTAMP '2026-06-29 10:00:10', 'AAPL', 180.20),
        |       (TIMESTAMP '2026-06-29 10:00:08', 'MSFT', 420.50)
        |""".stripMargin)
  }

  /** Copies the trade and quote views into parquet tables, so a pushed filter reaches a scan. */
  private def withTradeQuoteTables(f: => Unit): Unit = {
    setupTradeQuoteViews()
    withTable("trades_t", "quotes_t") {
      sql("CREATE TABLE trades_t USING parquet AS SELECT * FROM trades")
      sql("CREATE TABLE quotes_t USING parquet AS SELECT * FROM quotes")
      f
    }
  }

  /** Names of the columns that the scan of `table` filters on. */
  private def scanFilterColumns(df: DataFrame, table: String): Set[String] = {
    val plan = df.queryExecution.executedPlan
    val scans = collect(plan) {
      case scan: FileSourceScanExec if scan.tableIdentifier.exists(_.table == table) => scan
    }
    assert(scans.length == 1, s"Expected one scan of $table in plan:\n$plan")
    scans.head.dataFilters.flatMap(_.references.map(_.name)).toSet
  }

  /** Runs `f` with temp view `r` over `rows`, kept in input order so ties keep that order. */
  private def withOrderedRightView(rows: String, columns: String)(f: => Unit): Unit = {
    withTempView("r") {
      sql(s"SELECT * FROM VALUES $rows AS r($columns)").coalesce(1).createOrReplaceTempView("r")
      f
    }
  }

  test("INNER ASOF JOIN with TIMESTAMP MATCH_CONDITION") {
    setupTradeQuoteViews()
    checkSortMergeAsOf(
      sql(
        """
          |SELECT t.trade_time, t.symbol, t.quantity, q.bid_price
          |FROM trades t
          |ASOF JOIN quotes q
          |  MATCH_CONDITION (t.trade_time >= q.quote_time)
          |  ON t.symbol = q.symbol
          |""".stripMargin),
      Seq(
        Row(Timestamp.valueOf("2026-06-29 10:00:05"), "AAPL", 100, 180.10),
        Row(Timestamp.valueOf("2026-06-29 10:00:11"), "AAPL", 200, 180.20),
        Row(Timestamp.valueOf("2026-06-29 10:00:12"), "MSFT", 50, 420.50)))
  }

  gridTest("SQL ASOF JOIN uses sort-merge for mixed timestamp keys")(
    for {
      singlePass <- Seq(false, true)
      sortMergeEnabled <- Seq(false, true)
    } yield (singlePass, sortMergeEnabled)
  ) { case (singlePass, sortMergeEnabled) =>
    withSQLConf(
        SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> sortMergeEnabled.toString,
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
        SQLConf.TIMESTAMP_TYPE.key -> "TIMESTAMP_LTZ",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
        SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_ASOF_JOIN_RESOLUTION.key -> "true"
    ) {
      val query = sql(
        """
          |SELECT r.id
          |FROM VALUES (TIMESTAMP '2026-06-29 10:00:05') AS l(ts)
          |ASOF JOIN VALUES
          |  (TIMESTAMP_NTZ '2026-06-29 10:00:00', 1),
          |  (TIMESTAMP_NTZ '2026-06-29 10:00:04', 2),
          |  (TIMESTAMP_NTZ '2026-06-29 10:00:06', 3) AS r(ts, id)
          |  MATCH_CONDITION (l.ts >= r.ts)
          |""".stripMargin
      )
      assertUsesSortMergeAsOfJoin(query)
      // Single-pass analysis does not support the deserializer used by checkToRDD.
      QueryTest.checkAnswer(df = query, expectedAnswer = Seq(Row(2)), checkToRDD = false)
    }
  }

  gridTest("SQL ASOF JOIN projects computed keys and preserves its output")(Seq(false, true)) {
    singlePass =>
      withSQLConf(
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles",
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_ASOF_JOIN_RESOLUTION.key -> "true") {
        for {
          ltzOnLeft <- Seq(false, true)
          tuple <- Seq(false, true)
        } {
          val (leftType, rightType) = if (ltzOnLeft) {
            ("TIMESTAMP_LTZ", "TIMESTAMP_NTZ")
          } else {
            ("TIMESTAMP_NTZ", "TIMESTAMP_LTZ")
          }
          val leftKey = if (tuple) "(l.ts, l.ts)" else "l.ts"
          val rightKey = if (tuple) "(r.ts, r.ts)" else "r.ts"
          val query = sql(
            s"""SELECT *
               |FROM VALUES
               |  ($leftType '2024-03-10 01:00:00', 1),
               |  ($leftType '2024-03-10 03:00:00', 2) l(ts, _asof_key)
               |LEFT ASOF JOIN VALUES
               |  ($rightType '2024-03-10 01:30:00', 3) r(ts, _asof_key)
               |MATCH_CONDITION ($leftKey >= $rightKey)
               |""".stripMargin)
          val logicalJoin = query.queryExecution.optimizedPlan.collectFirst {
            case j: AsOfJoin => j
          }.get
          val plan = query.queryExecution.sparkPlan
          val outputProject = plan.collectFirst {
            case p: ProjectExec if p.child.isInstanceOf[SortMergeAsOfJoinExec] => p
          }.get
          val join = outputProject.child.asInstanceOf[SortMergeAsOfJoinExec]
          assert(outputProject.output == logicalJoin.output)
          assert(join.leftSortExprs.forall(_.isInstanceOf[Attribute]))
          assert(join.rightSortExprs.forall(_.isInstanceOf[Attribute]))
          assert(!join.asOfCondition.exists(_.isInstanceOf[Cast]))
          val projectedChild = if (ltzOnLeft) join.right else join.left
          val aliases = projectedChild.asInstanceOf[ProjectExec].projectList.collect {
            case a: Alias => a
          }
          // Repeated tuple fields share one conversion, even if an input has the helper's name.
          assert(aliases.size == 1)
          assert(join.asOfCondition.references.contains(aliases.head.toAttribute))
          val leftExtra = if (ltzOnLeft) 0 else 1
          assert(join.left.output.size == logicalJoin.left.output.size + leftExtra)
          assert(join.right.output.size == logicalJoin.right.output.size + 1 - leftExtra)

          def timestamp(value: String, ltz: Boolean): Any = {
            if (ltz) Timestamp.from(Instant.parse(value + "Z")) else LocalDateTime.parse(value)
          }
          QueryTest.checkAnswer(
            df = query,
            expectedAnswer = Seq(
              Row(timestamp(if (ltzOnLeft) "2024-03-10T09:00:00" else "2024-03-10T01:00:00",
                ltzOnLeft), 1, null, null),
              Row(timestamp(if (ltzOnLeft) "2024-03-10T10:00:00" else "2024-03-10T03:00:00",
                ltzOnLeft), 2,
                timestamp(if (ltzOnLeft) "2024-03-10T01:30:00" else "2024-03-10T09:30:00",
                  !ltzOnLeft), 3)),
            checkToRDD = false)
        }
      }
  }

  gridTest("SQL ASOF JOIN keeps reference-free tuple keys on their own side")(Seq(false, true)) {
    singlePass =>
      withUserDefinedFunction("constant_key" -> true) {
        spark.udf.register(
          name = "constant_key",
          f = new UDF0[Long] {
            override def call(): Long = 5L
          },
          returnType = LongType)
        withSQLConf(
            SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
            SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
            SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
            SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_ASOF_JOIN_RESOLUTION.key -> "true") {
          val query = sql(
            """SELECT l.k, r.k
              |FROM VALUES (1), (2) l(k) ASOF JOIN VALUES (0), (1) r(k)
              |MATCH_CONDITION ((l.k, constant_key()) >= (r.k, constant_key()))
              |""".stripMargin)
          assertUsesSortMergeAsOfJoin(query)
          QueryTest.checkAnswer(
            df = query,
            expectedAnswer = Seq(Row(1, 1), Row(2, 1)),
            checkToRDD = false)
        }
      }
  }

  gridTest("SQL ASOF JOIN preserves projected key ordering")(Seq(false, true)) { singlePass =>
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.REMOVE_REDUNDANT_SORTS_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
        SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_ASOF_JOIN_RESOLUTION.key -> "true") {
      val inGap = LocalDateTime.parse("2024-03-10T02:50:00")
      val afterGap = LocalDateTime.parse("2024-03-10T03:00:00")
      for ((ordering, sortCount, expected) <- Seq(
          ("CAST(l.ts AS TIMESTAMP_LTZ)", 2, Seq(afterGap, inGap)),
          ("CAST(l.ts AS TIMESTAMP_LTZ) DESC", 3, Seq(inGap, afterGap)),
          ("l.ts", 3, Seq(inGap, afterGap)))) {
        val query = sql(
          s"""SELECT l.ts, r.rts
             |FROM VALUES
             |  (TIMESTAMP_NTZ '2024-03-10 02:50:00'),
             |  (TIMESTAMP_NTZ '2024-03-10 03:00:00') l(ts)
             |ASOF JOIN VALUES
             |  (TIMESTAMP_LTZ '2024-03-10 09:00:00Z'),
             |  (TIMESTAMP_LTZ '2024-03-10 10:00:00Z') r(rts)
             |MATCH_CONDITION (CAST(l.ts AS TIMESTAMP_LTZ) >= r.rts)
             |ORDER BY $ordering
             |""".stripMargin)
        val plan = query.queryExecution.executedPlan
        assert(plan.collect { case s: SortExec => s }.size == sortCount, plan.treeString)
        assert(query.collect().map(_.getAs[LocalDateTime](0)).toSeq == expected)
      }
    }
  }

  gridTest("SQL ASOF JOIN preserves projected key partitioning")(Seq(false, true)) { singlePass =>
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "2",
        SQLConf.SINGLE_TASK_EXECUTION_ENABLED.key -> "false",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
        SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_ASOF_JOIN_RESOLUTION.key -> "true") {
      for ((joinType, leftType, rightType, groupedSide) <- Seq(
          ("LEFT", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "l"),
          ("INNER", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "l"),
          ("INNER", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "r"))) {
        val groupKey = s"CAST($groupedSide.ts AS TIMESTAMP_LTZ)"
        val query = sql(
          s"""SELECT $groupKey, count(r.id)
             |FROM VALUES ($leftType '2024-03-10 01:00:00'),
             |  ($leftType '2024-03-10 01:01:00'),
             |  ($leftType '2024-03-10 01:02:00'),
             |  ($leftType '2024-03-10 01:03:00') l(ts)
             |$joinType ASOF JOIN VALUES ($rightType '2024-03-10 01:00:00', 0),
             |  ($rightType '2024-03-10 01:01:00', 1),
             |  ($rightType '2024-03-10 01:02:00', 2) r(ts, id)
             |MATCH_CONDITION (l.ts >= r.ts)
             |ON l.ts = r.ts
             |GROUP BY $groupKey
             |""".stripMargin)
        val plan = query.queryExecution.executedPlan
        assertUsesSortMergeAsOfJoin(query)
        // Only the join inputs shuffle; the aggregation reuses the join's partitioning.
        assert(plan.collect { case s: ShuffleExchangeExec => s }.size == 2, plan.treeString)
        val rowCount = if (joinType == "LEFT") 4 else 3
        val start = Instant.parse("2024-03-10T09:00:00Z")
        val expected = (0 until rowCount).map { i =>
          Row(Timestamp.from(start.plusSeconds(i * 60L)), if (i < 3) 1L else 0L)
        }
        QueryTest.checkAnswer(df = query, expectedAnswer = expected, checkToRDD = false)
      }
    }
  }

  gridTest("SQL ASOF JOIN projects keys from hidden USING columns")(Seq(false, true)) {
    singlePass =>
      withSQLConf(
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles",
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_ASOF_JOIN_RESOLUTION.key -> "true") {
        val query = sql(
          """SELECT *, h.ts AS hidden_ts
            |FROM (
            |  VALUES (TIMESTAMP_NTZ '2024-03-10 02:50:00', 1),
            |    (TIMESTAMP_NTZ '2024-03-10 03:00:00', 2) b(ts, id)
            |  LEFT JOIN VALUES (TIMESTAMP_NTZ '2024-03-10 03:00:00') h(ts) USING (ts))
            |LEFT ASOF JOIN VALUES (TIMESTAMP_LTZ '2024-03-10 10:00:00Z') r(lts)
            |MATCH_CONDITION (h.ts >= r.lts)
            |""".stripMargin)
        assert(query.columns.toSeq == Seq("ts", "id", "lts", "hidden_ts"))
        assertUsesSortMergeAsOfJoin(query)
        QueryTest.checkAnswer(
          df = query,
          expectedAnswer = Seq(
            Row(LocalDateTime.parse("2024-03-10T02:50:00"), 1, null, null),
            Row(LocalDateTime.parse("2024-03-10T03:00:00"), 2,
              Timestamp.from(Instant.parse("2024-03-10T10:00:00Z")),
              LocalDateTime.parse("2024-03-10T03:00:00"))),
          checkToRDD = false)
      }
  }

  gridTest("SQL ASOF JOIN sorts mixed timestamp keys across DST gaps")(
    Seq(false, true)
  ) { singlePass =>
    withSQLConf(
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles",
        SQLConf.TIMESTAMP_NANOS_TYPES_ENABLED.key -> "true",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
        SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_ASOF_JOIN_RESOLUTION.key -> "true"
    ) {
      val ltzRows =
        """(TIMESTAMP_LTZ '2024-03-10 10:05:00Z', 1),
          |(TIMESTAMP_LTZ '2024-03-10 10:55:00Z', 2)""".stripMargin
      // 02:50 becomes 10:50Z, after both 03:00 (10:00Z) and 03:15 (10:15Z).
      val ntzRows =
        """(TIMESTAMP_NTZ '2024-03-10 02:50:00', 1),
          |(TIMESTAMP_NTZ '2024-03-10 03:00:00', 2),
          |(TIMESTAMP_NTZ '2024-03-10 03:15:00', 3)""".stripMargin
      val keyExpressions = Seq(
        "scalar" -> "ts",
        "tuple" -> "ts",
        "struct" -> "named_struct('ts', ts, 'n', 0)",
        "array" -> "array(ts)",
        "nested" -> "named_struct('items', array(named_struct('ts', ts)))")
      for {
        (shape, key) <- keyExpressions
        // Top-level ARRAY operands require lambda resolution in the fixed-point analyzer.
        if !singlePass || shape != "array"
        ltzOnLeft <- Seq(false, true)
        op <- Seq(">=", "<=")
      } {
        withClue(s"shape=$shape, ltzOnLeft=$ltzOnLeft, op=$op: ") {
          val (leftRows, rightRows) = if (ltzOnLeft) (ltzRows, ntzRows) else (ntzRows, ltzRows)
          val leftKey = if (shape == "tuple") "(l.k, 0)" else "l.k"
          val rightKey = if (shape == "tuple") "(r.k, 0)" else "r.k"
          val query = sql(
            s"""SELECT l.id, r.id
               |FROM (SELECT $key AS k, id FROM VALUES $leftRows AS v(ts, id)) l
               |LEFT ASOF JOIN
               |  (SELECT $key AS k, id FROM VALUES $rightRows AS v(ts, id)) r
               |MATCH_CONDITION ($leftKey $op $rightKey)
               |""".stripMargin
          )
          val expected = (ltzOnLeft, op == ">=") match {
            case (true, true) => Seq(Row(1, 2), Row(2, 1))
            case (true, false) => Seq(Row(1, 3), Row(2, null))
            case (false, true) => Seq(Row(1, 1), Row(2, null), Row(3, 1))
            case (false, false) => Seq(Row(1, 2), Row(2, 1), Row(3, 2))
          }
          assertUsesSortMergeAsOfJoin(query)
          QueryTest.checkAnswer(df = query, expectedAnswer = expected, checkToRDD = false)
        }
      }

      for (op <- Seq(">=", "<=")) {
        withClue(s"nanosecond keys, op=$op: ") {
          val query = sql(
            s"""SELECT r.id
               |FROM VALUES (CAST('2024-03-10 10:15:00.000000150Z' AS TIMESTAMP_LTZ(9))) l(ts)
               |LEFT ASOF JOIN VALUES
               |  (CAST('2024-03-10 02:50:00.0000001' AS TIMESTAMP_NTZ(7)), 1),
               |  (CAST('2024-03-10 03:15:00.0000001' AS TIMESTAMP_NTZ(7)), 2),
               |  (CAST('2024-03-10 03:15:00.0000002' AS TIMESTAMP_NTZ(7)), 3) r(ts, id)
               |MATCH_CONDITION (l.ts $op r.ts)
               |""".stripMargin
          )
          assertUsesSortMergeAsOfJoin(query)
          QueryTest.checkAnswer(
            df = query,
            expectedAnswer = Seq(Row(if (op == ">=") 2 else 3)),
            checkToRDD = false
          )
        }

        withClue(s"mixed timestamp and integral struct fields, op=$op: ") {
          val query = sql(
            s"""SELECT r.id
               |FROM (SELECT named_struct('ts', ts, 'n', n) AS k
               |  FROM VALUES (TIMESTAMP_LTZ '2024-03-10 10:15:00Z', 2L) v(ts, n)) l
               |LEFT ASOF JOIN (SELECT named_struct('ts', ts, 'n', n) AS k, id FROM VALUES
               |  (TIMESTAMP_NTZ '2024-03-10 02:50:00', 1, 1),
               |  (TIMESTAMP_NTZ '2024-03-10 03:15:00', 1, 2),
               |  (TIMESTAMP_NTZ '2024-03-10 03:15:00', 3, 3) v(ts, n, id)) r
               |MATCH_CONDITION (l.k $op r.k)
               |""".stripMargin
          )
          assertUsesSortMergeAsOfJoin(query)
          QueryTest.checkAnswer(
            df = query,
            expectedAnswer = Seq(Row(if (op == ">=") 2 else 3)),
            checkToRDD = false
          )
        }
      }

      def nullStructRows(timestampType: String): Seq[String] = Seq(
        s"(CAST(NULL AS STRUCT<ts: $timestampType>), 1)",
        s"(named_struct('ts', CAST(NULL AS $timestampType)), 2)")

      for {
        (leftType, rightType) <- Seq(
          ("TIMESTAMP_LTZ", "TIMESTAMP_NTZ"), ("TIMESTAMP_NTZ", "TIMESTAMP_LTZ"))
        op <- Seq(">=", "<=")
        reverseRightRows <- Seq(false, true)
      } {
        withClue(s"leftType=$leftType, op=$op, reverseRightRows=$reverseRightRows: ") {
          val leftRows = nullStructRows(leftType).mkString(", ")
          val rightRows = nullStructRows(rightType)
          val orderedRightRows = if (reverseRightRows) rightRows.reverse else rightRows
          val query = sql(
            s"""SELECT l.id, r.id
               |FROM VALUES $leftRows AS l(k, id)
               |LEFT ASOF JOIN VALUES ${orderedRightRows.mkString(", ")} AS r(k, id)
               |MATCH_CONDITION (l.k $op r.k)
               |""".stripMargin
          )
          assertUsesSortMergeAsOfJoin(query)
          QueryTest.checkAnswer(
            df = query,
            expectedAnswer = Seq(Row(1, null), Row(2, 2)),
            checkToRDD = false
          )
        }
      }
    }
  }

  test("LEFT ASOF JOIN preserves unmatched left rows") {
    setupTradeQuoteViews()
    checkSortMergeAsOf(
      sql(
        """
          |SELECT t.trade_time, t.symbol, q.bid_price
          |FROM trades t
          |LEFT ASOF JOIN quotes q
          |  MATCH_CONDITION (t.trade_time >= q.quote_time)
          |  ON t.symbol = q.symbol
          |ORDER BY t.trade_time
          |""".stripMargin),
      Seq(
        Row(Timestamp.valueOf("2026-06-29 09:59:59"), "GOOG", null),
        Row(Timestamp.valueOf("2026-06-29 10:00:05"), "AAPL", 180.10),
        Row(Timestamp.valueOf("2026-06-29 10:00:11"), "AAPL", 180.20),
        Row(Timestamp.valueOf("2026-06-29 10:00:12"), "MSFT", 420.50)))
  }

  test("USING is equivalent to ON symbol equality") {
    setupTradeQuoteViews()
    checkSortMergeAsOf(
      sql(
        """
          |SELECT trade_time, symbol, bid_price
          |FROM trades
          |ASOF JOIN quotes
          |  MATCH_CONDITION (trades.trade_time >= quotes.quote_time)
          |  USING (symbol)
          |ORDER BY trade_time
          |""".stripMargin),
      Seq(
        Row(Timestamp.valueOf("2026-06-29 10:00:05"), "AAPL", 180.10),
        Row(Timestamp.valueOf("2026-06-29 10:00:11"), "AAPL", 180.20),
        Row(Timestamp.valueOf("2026-06-29 10:00:12"), "MSFT", 420.50)))
  }

  test("forward match with <=") {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW alerts(alert_time, host) AS
        |VALUES (TIMESTAMP '2026-06-29 10:00:00', 'db-01')
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW maintenance(window_start, host) AS
        |VALUES (TIMESTAMP '2026-06-29 08:00:00', 'db-01'),
        |       (TIMESTAMP '2026-06-29 12:00:00', 'db-01')
        |""".stripMargin)
    checkSortMergeAsOf(
      sql(
        """
          |SELECT a.alert_time, a.host, m.window_start
          |FROM alerts a
          |ASOF JOIN maintenance m
          |  MATCH_CONDITION (a.alert_time <= m.window_start)
          |  ON a.host = m.host
          |""".stripMargin),
      Row(
        Timestamp.valueOf("2026-06-29 10:00:00"),
        "db-01",
        Timestamp.valueOf("2026-06-29 12:00:00")) :: Nil)
  }

  test("MATCH_CONDITION with the right operand written first (<=) is a backward match") {
    setupTradeQuoteViews()
    // q.quote_time <= t.trade_time normalizes to t.trade_time >= q.quote_time.
    val query = sql(
      """
        |SELECT t.trade_time, t.symbol, q.bid_price
        |FROM trades t ASOF JOIN quotes q
        |  MATCH_CONDITION (q.quote_time <= t.trade_time)
        |  ON t.symbol = q.symbol
        |""".stripMargin)
    checkSortMergeAsOf(
      query,
      Seq(
        Row(Timestamp.valueOf("2026-06-29 10:00:05"), "AAPL", 180.10),
        Row(Timestamp.valueOf("2026-06-29 10:00:11"), "AAPL", 180.20),
        Row(Timestamp.valueOf("2026-06-29 10:00:12"), "MSFT", 420.50)))
    assertPlannedDirection(query, Backward)
  }

  test("MATCH_CONDITION with the right operand written first (>=) is a forward match") {
    setupTradeQuoteViews()
    // q.quote_time >= t.trade_time normalizes to t.trade_time <= q.quote_time.
    val query = sql(
      """
        |SELECT t.trade_time, t.symbol, q.bid_price
        |FROM trades t ASOF JOIN quotes q
        |  MATCH_CONDITION (q.quote_time >= t.trade_time)
        |  ON t.symbol = q.symbol
        |""".stripMargin)
    checkSortMergeAsOf(
      query,
      Row(Timestamp.valueOf("2026-06-29 10:00:05"), "AAPL", 180.15) :: Nil)
    assertPlannedDirection(query, Forward)
  }

  test("MATCH_CONDITION right operand first (>=) forward match is inclusive at the boundary") {
    setupTradeQuoteViews()
    checkSortMergeAsOf(
      sql(
        """
          |SELECT q.bid_price
          |FROM VALUES (TIMESTAMP '2026-06-29 10:00:07', 'AAPL') AS t(trade_time, symbol)
          |ASOF JOIN quotes q
          |  MATCH_CONDITION (q.quote_time >= t.trade_time)
          |  ON t.symbol = q.symbol
          |""".stripMargin),
      Row(180.15) :: Nil)
  }

  test("TIMESTAMP MATCH_CONDITION with legacy intervals") {
    // The distance is then a CalendarInterval, which has no ordering. Only Nearest reads it.
    withSQLConf(SQLConf.LEGACY_INTERVAL_ENABLED.key -> "true") {
      for ((op, tag) <- Seq(">=" -> "r10", ">" -> "r9", "<=" -> "r10", "<" -> "r11")) {
        checkSortMergeAsOf(
          sql(
            s"""
               |SELECT r.tag
               |FROM VALUES (TIMESTAMP '2026-06-29 10:00:00') AS t(ts)
               |ASOF JOIN VALUES
               |  (TIMESTAMP '2026-06-29 08:00:00', 'r8'),
               |  (TIMESTAMP '2026-06-29 09:00:00', 'r9'),
               |  (TIMESTAMP '2026-06-29 10:00:00', 'r10'),
               |  (TIMESTAMP '2026-06-29 11:00:00', 'r11'),
               |  (TIMESTAMP '2026-06-29 12:00:00', 'r12') AS r(ts, tag)
               |  MATCH_CONDITION (t.ts $op r.ts)
               |""".stripMargin),
          Row(tag) :: Nil)
      }
    }
  }

  test("DATE scalar MATCH_CONDITION") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT t.d, r.d AS matched_d
          |FROM VALUES (DATE '2026-06-29') AS t(d) ASOF JOIN
          |     VALUES (DATE '2026-06-28'), (DATE '2026-06-29') AS r(d)
          |  MATCH_CONDITION (t.d >= r.d)
          |""".stripMargin),
      Row(Date.valueOf("2026-06-29"), Date.valueOf("2026-06-29")) :: Nil)
  }

  test("INT scalar MATCH_CONDITION") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT t.k, r.k AS matched_k
          |FROM VALUES (10) AS t(k) ASOF JOIN VALUES (5), (7) AS r(k)
          |  MATCH_CONDITION (t.k >= r.k)
          |""".stripMargin),
      Row(10, 7) :: Nil)
  }

  test("STRING scalar MATCH_CONDITION") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT t.k, r.k AS matched_k
          |FROM VALUES ('c') AS t(k) ASOF JOIN VALUES ('a'), ('b') AS r(k)
          |  MATCH_CONDITION (t.k >= r.k)
          |""".stripMargin),
      Row("c", "b") :: Nil)
  }

  test("STRING scalar forward MATCH_CONDITION with <=") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT t.k, r.k AS matched_k
          |FROM VALUES ('b') AS t(k) ASOF JOIN VALUES ('c'), ('d'), ('z') AS r(k)
          |  MATCH_CONDITION (t.k <= r.k)
          |""".stripMargin),
      Row("b", "c") :: Nil)
  }

  test("DECIMAL scalar MATCH_CONDITION") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT t.k, r.k AS matched_k
          |FROM VALUES (CAST(10.50 AS DECIMAL(10, 2))) AS t(k) ASOF JOIN
          |     VALUES (CAST(5.00 AS DECIMAL(10, 2))),
          |            (CAST(7.25 AS DECIMAL(10, 2))),
          |            (CAST(9.99 AS DECIMAL(10, 2))) AS r(k)
          |  MATCH_CONDITION (t.k >= r.k)
          |""".stripMargin),
      Row(
        new java.math.BigDecimal("10.50"),
        new java.math.BigDecimal("9.99")) :: Nil)
  }

  test("DECIMAL scalar forward MATCH_CONDITION with <=") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT t.k, r.k AS matched_k
          |FROM VALUES (CAST(10.50 AS DECIMAL(10, 2))) AS t(k) ASOF JOIN
          |     VALUES (CAST(11.00 AS DECIMAL(10, 2))),
          |            (CAST(12.50 AS DECIMAL(10, 2))),
          |            (CAST(15.00 AS DECIMAL(10, 2))) AS r(k)
          |  MATCH_CONDITION (t.k <= r.k)
          |""".stripMargin),
      Row(
        new java.math.BigDecimal("10.50"),
        new java.math.BigDecimal("11.00")) :: Nil)
  }

  test("DOUBLE scalar MATCH_CONDITION") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT t.k, r.k AS matched_k
          |FROM VALUES (CAST(10.5 AS DOUBLE)) AS t(k) ASOF JOIN
          |     VALUES (CAST(5.0 AS DOUBLE)),
          |            (CAST(7.25 AS DOUBLE)),
          |            (CAST(9.99 AS DOUBLE)) AS r(k)
          |  MATCH_CONDITION (t.k >= r.k)
          |""".stripMargin),
      Row(10.5, 9.99) :: Nil)
  }

  test("DOUBLE scalar forward MATCH_CONDITION with <=") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT t.k, r.k AS matched_k
          |FROM VALUES (CAST(10.5 AS DOUBLE)) AS t(k) ASOF JOIN
          |     VALUES (CAST(11.0 AS DOUBLE)),
          |            (CAST(12.5 AS DOUBLE)),
          |            (CAST(15.0 AS DOUBLE)) AS r(k)
          |  MATCH_CONDITION (t.k <= r.k)
          |""".stripMargin),
      Row(10.5, 11.0) :: Nil)
  }

  test("STRUCT tuple MATCH_CONDITION") {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW requests(req_ts, seq, service) AS
        |VALUES (TIMESTAMP '2026-06-29 10:00:00', 5, 'api'),
        |       (TIMESTAMP '2026-06-29 10:03:00', 1, 'api')
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW deploys(deploy_ts, seq, service, version) AS
        |VALUES (TIMESTAMP '2026-06-29 10:00:00', 1, 'api', 'v1.0'),
        |       (TIMESTAMP '2026-06-29 10:00:00', 3, 'api', 'v1.1')
        |""".stripMargin)
    checkSortMergeAsOf(
      sql(
        """
          |SELECT r.req_ts, r.seq, d.version
          |FROM requests r ASOF JOIN deploys d
          |  MATCH_CONDITION ((r.req_ts, r.seq) >= (d.deploy_ts, d.seq))
          |  ON r.service = d.service
          |ORDER BY r.req_ts, r.seq
          |""".stripMargin),
      Seq(
        Row(Timestamp.valueOf("2026-06-29 10:00:00"), 5, "v1.1"),
        Row(Timestamp.valueOf("2026-06-29 10:03:00"), 1, "v1.1")))
  }

  test("scalar leaf STRUCT tuple MATCH_CONDITION") {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW left_nested(inner_ts, inner_seq, tag) AS
        |VALUES (TIMESTAMP '2026-06-29 10:00:00', 2, 'a')
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW right_nested(inner_ts, inner_seq, tag) AS
        |VALUES (TIMESTAMP '2026-06-29 09:00:00', 1, 'a'),
        |       (TIMESTAMP '2026-06-29 10:00:00', 1, 'a')
        |""".stripMargin)
    checkSortMergeAsOf(
      sql(
        """
          |SELECT r.tag, r.inner_seq
          |FROM left_nested t
          |ASOF JOIN right_nested r
          |  MATCH_CONDITION ((t.inner_ts, t.inner_seq, t.tag) >= (r.inner_ts, r.inner_seq, r.tag))
          |""".stripMargin),
      Row("a", 1) :: Nil)
  }

  test("whole STRUCT column MATCH_CONDITION") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT r.k.tag, r.k.inner_seq
          |FROM VALUES (named_struct(
          |  'inner_ts', TIMESTAMP '2026-06-29 10:00:00',
          |  'inner_seq', 2,
          |  'tag', 'a')) AS t(k)
          |ASOF JOIN (
          |  SELECT * FROM VALUES
          |    (named_struct(
          |      'inner_ts', TIMESTAMP '2026-06-29 09:00:00',
          |      'inner_seq', 1,
          |      'tag', 'a')),
          |    (named_struct(
          |      'inner_ts', TIMESTAMP '2026-06-29 10:00:00',
          |      'inner_seq', 1,
          |      'tag', 'a')) AS r(k)
          |) r
          |  MATCH_CONDITION (t.k >= r.k)
          |""".stripMargin),
      Row("a", 1) :: Nil)
  }

  test("nested whole STRUCT column MATCH_CONDITION") {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW requests(req_ts, seq, service) AS
        |VALUES (TIMESTAMP '2026-06-29 10:00:00', 5, 'api'),
        |       (TIMESTAMP '2026-06-29 10:03:00', 1, 'api')
        |""".stripMargin)
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW deploys(deploy_ts, seq, service, version) AS
        |VALUES (TIMESTAMP '2026-06-29 10:00:00', 1, 'api', 'v1.0'),
        |       (TIMESTAMP '2026-06-29 10:00:00', 3, 'api', 'v1.1')
        |""".stripMargin)
    checkSortMergeAsOf(
      sql(
        """
          |SELECT r.req_ts, r.seq, d.version
          |FROM (
          |  SELECT named_struct('ts', req_ts, 'seq', seq) AS k, service, req_ts, seq
          |  FROM requests
          |) r
          |ASOF JOIN (
          |  SELECT named_struct('ts', deploy_ts, 'seq', seq) AS k, service, version, deploy_ts, seq
          |  FROM deploys
          |) d
          |  MATCH_CONDITION (r.k >= d.k)
          |  ON r.service = d.service
          |ORDER BY r.req_ts, r.seq
          |""".stripMargin),
      Seq(
        Row(Timestamp.valueOf("2026-06-29 10:00:00"), 5, "v1.1"),
        Row(Timestamp.valueOf("2026-06-29 10:03:00"), 1, "v1.1")))
  }

  gridTest("NULL whole STRUCT column MATCH_CONDITION never matches")(Seq(
      ("CAST(NULL AS STRUCT<a: INT>)", "named_struct('a', 1)", "named_struct('a', 5)"),
      ("CAST(NULL AS STRUCT<a: INT, b: INT>)",
        "named_struct('a', 1, 'b', 1)", "named_struct('a', 5, 'b', 5)"),
      ("CAST(NULL AS STRUCT<e: STRUCT<a: INT>>)",
        "named_struct('e', named_struct('a', 1))", "named_struct('e', named_struct('a', 5))"),
      // Non-NULL values of this type are all equal.
      ("CAST(NULL AS STRUCT<e: STRUCT<>>)",
        "named_struct('e', named_struct())", "named_struct('e', named_struct())"))) {
    case (nullStruct, one, five) =>
      // A NULL struct must never match, the same as a NULL scalar operand.
      Seq(true, false).foreach { ansiEnabled =>
        withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
          def asOfTags(joinType: String, left: String, op: String, rights: String): DataFrame =
            sql(
              s"""
                 |SELECT r.tag
                 |FROM VALUES ($left, 'l') AS t(k, tag)
                 |$joinType ASOF JOIN VALUES $rights AS r(k, tag)
                 |  MATCH_CONDITION (t.k $op r.k)
                 |""".stripMargin)
          checkSortMergeAsOf(
            asOfTags("", nullStruct, ">=", s"($nullStruct, 'rnull'), ($one, 'r1')"), Nil)
          checkSortMergeAsOf(asOfTags("", nullStruct, "<=", s"($one, 'r1')"), Nil)
          checkSortMergeAsOf(asOfTags("", five, ">=", s"($nullStruct, 'rnull')"), Nil)
          // LEFT ASOF JOIN keeps the NULL left row with NULL right columns.
          checkSortMergeAsOf(
            asOfTags("LEFT", nullStruct, ">=", s"($nullStruct, 'rnull'), ($one, 'r1')"),
            Row(null) :: Nil)
        }
      }
  }

  gridTest("whole STRUCT column MATCH_CONDITION matches a struct whose fields are all NULL")(Seq(
      ">=" -> Seq(Row("allnull")), "<=" -> Seq(Row("allnull")), ">" -> Nil, "<" -> Nil)) {
    case (op, expected) =>
      // Two structs of NULL fields are equal, and a NULL struct never matches.
      val allNull = "named_struct('a', CAST(NULL AS INT), 'b', CAST(NULL AS INT))"
      val nullStruct = "CAST(NULL AS STRUCT<a: INT, b: INT>)"
      Seq(true, false).foreach { ansiEnabled =>
        withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
          checkSortMergeAsOf(
            sql(
              s"""
                 |SELECT r.tag
                 |FROM VALUES ($allNull, 'l') AS t(k, tag)
                 |ASOF JOIN VALUES ($nullStruct, 'rnull'), ($allNull, 'allnull') AS r(k, tag)
                 |  MATCH_CONDITION (t.k $op r.k)
                 |""".stripMargin),
            expected)
        }
      }
  }

  test("nested STRUCT column MATCH_CONDITION keeps a NULL inner struct apart from NULL fields") {
    // {e: NULL} sorts before {e: {a: NULL}}, as in a plain comparison.
    val eNull = "named_struct('e', CAST(NULL AS STRUCT<a: INT>))"
    val aNull = "named_struct('e', named_struct('a', CAST(NULL AS INT)))"
    for {
      (left, op, right) <- Seq((aNull, ">", eNull), (eNull, "<", aNull))
      ansiEnabled <- Seq(true, false)
    } {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
        checkSortMergeAsOf(
          sql(
            s"""
               |SELECT r.tag
               |FROM VALUES ($left, 'l') AS t(k, tag)
               |ASOF JOIN VALUES ($right, 'r') AS r(k, tag)
               |  MATCH_CONDITION (t.k $op r.k)
               |""".stripMargin),
          Row("r") :: Nil)
      }
    }
  }

  test("tuple vs nullable STRUCT column MATCH_CONDITION: a NULL struct does not end the scan") {
    // r.s sorts as one value, so NULL comes before {NULL, NULL}. Sorting by its fields would tie
    // them, and in one of these input orders the NULL row would end the scan before r11.
    val allNull = "(named_struct('a', CAST(NULL AS INT), 'b', CAST(NULL AS INT)), 'allnull')"
    val nullStruct = "(CAST(NULL AS STRUCT<a: INT, b: INT>), 'rnull')"
    val r11 = "(named_struct('a', 1, 'b', 1), 'r11')"
    for {
      rights <- Seq(s"$allNull, $nullStruct, $r11", s"$nullStruct, $allNull, $r11")
      ansiEnabled <- Seq(true, false)
    } {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
        withOrderedRightView(rights, "s, tag") {
          checkSortMergeAsOf(
            sql(
              """
                |SELECT r.tag
                |FROM VALUES (1, 2) AS t(a, b)
                |ASOF JOIN r
                |  MATCH_CONDITION ((t.a, t.b) >= r.s)
                |""".stripMargin),
            Row("r11") :: Nil)
        }
      }
    }
  }

  test("tuple with a nullable inner STRUCT MATCH_CONDITION picks the closest row") {
    // NULL and {a: NULL} have the same field, but the comparison ranks NULL lower. Both input
    // orders must pick the row equal to the left one, in both directions.
    val aNull = "named_struct('a', CAST(NULL AS INT))"
    val nullA = "CAST(NULL AS STRUCT<a: INT>)"
    val rAnull = s"(1, $aNull, 'r_anull')"
    val rNull = s"(1, $nullA, 'r_null')"
    for {
      rights <- Seq(s"$rAnull, $rNull", s"$rNull, $rAnull")
      ansiEnabled <- Seq(true, false)
    } {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
        withOrderedRightView(rights, "x, s, tag") {
          def asOfTag(left: String, op: String): DataFrame =
            sql(
              s"""
                 |SELECT r.tag
                 |FROM VALUES (1, $left) AS t(x, s)
                 |ASOF JOIN r
                 |  MATCH_CONDITION ((t.x, t.s) $op (r.x, r.s))
                 |""".stripMargin)
          checkSortMergeAsOf(asOfTag(aNull, ">="), Row("r_anull") :: Nil)
          checkSortMergeAsOf(asOfTag(nullA, "<="), Row("r_null") :: Nil)
        }
      }
    }
  }

  test("whole STRUCT column with an ARRAY field MATCH_CONDITION") {
    // The array field is compared as a whole; field 'a' ties, so the array field decides.
    checkSortMergeAsOf(
      sql(
        """
          |SELECT r.k
          |FROM VALUES (named_struct('a', 5, 'arr', ARRAY(2, 2))) AS t(k)
          |ASOF JOIN (
          |  SELECT * FROM VALUES
          |    (named_struct('a', 5, 'arr', ARRAY(1, 1))),
          |    (named_struct('a', 5, 'arr', ARRAY(9, 9))) AS r(k)
          |) r
          |  MATCH_CONDITION (t.k >= r.k)
          |""".stripMargin),
      Row(Row(5, Seq(1, 1))) :: Nil)
  }

  test("empty STRUCT column MATCH_CONDITION") {
    // Empty structs are equal, also when nested, so inclusive operators match and strict do not.
    for {
      emptyStruct <- Seq("named_struct()", "named_struct('e', named_struct())")
      (op, tag) <- Seq(">=" -> "hit", "<=" -> "hit", ">" -> null, "<" -> null)
    } {
      checkSortMergeAsOf(
        sql(
          s"""
             |SELECT r.tag
             |FROM VALUES ($emptyStruct) AS t(s)
             |LEFT ASOF JOIN VALUES ($emptyStruct, 'hit') AS r(s, tag)
             |  MATCH_CONDITION (t.s $op r.s)
             |""".stripMargin),
        Row(tag) :: Nil)
    }
  }

  test("NULL empty STRUCT operand never matches") {
    // A NULL operand fails every comparison, though any two non-NULL empty structs are equal.
    for {
      (leftValue, rightValue) <- Seq(
        "CAST(NULL AS STRUCT<>)" -> "named_struct()",
        "named_struct()" -> "CAST(NULL AS STRUCT<>)")
      op <- Seq(">=", ">", "<=", "<")
    } {
      checkSortMergeAsOf(
        sql(
          s"""
             |SELECT r.tag
             |FROM VALUES ($leftValue) AS t(s)
             |LEFT ASOF JOIN VALUES ($rightValue, 'hit') AS r(s, tag)
             |  MATCH_CONDITION (t.s $op r.s)
             |""".stripMargin),
        Row(null) :: Nil)
    }
  }

  test("STRUCT with an empty STRUCT field MATCH_CONDITION") {
    // The empty field always ties, so field 'seq' alone picks the nearest match to 5.
    for {
      (left, right) <- Seq("t.k" -> "r.k", "(t.k.e, t.k.seq)" -> "(r.k.e, r.k.seq)")
      (op, seq) <- Seq(">=" -> 5, ">" -> 4, "<=" -> 5, "<" -> 6)
    } {
      checkSortMergeAsOf(
        sql(
          s"""
             |SELECT r.k.seq
             |FROM VALUES (named_struct('e', named_struct(), 'seq', 5)) AS t(k)
             |ASOF JOIN VALUES
             |  (named_struct('e', named_struct(), 'seq', 3)),
             |  (named_struct('e', named_struct(), 'seq', 4)),
             |  (named_struct('e', named_struct(), 'seq', 5)),
             |  (named_struct('e', named_struct(), 'seq', 6)),
             |  (named_struct('e', named_struct(), 'seq', 7)) AS r(k)
             |  MATCH_CONDITION ($left $op $right)
             |""".stripMargin),
        Row(seq) :: Nil)
    }
  }

  test("ARRAY<INT> MATCH_CONDITION") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT r.a
          |FROM VALUES (ARRAY(1, 3)) AS t(a)
          |ASOF JOIN VALUES (ARRAY(1, 2)), (ARRAY(1, 4)) AS r(a)
          |MATCH_CONDITION (t.a >= r.a)
          |""".stripMargin),
      Row(Seq(1, 2)) :: Nil)
  }

  test("ARRAY<INT> vs ARRAY<BIGINT> coercible MATCH_CONDITION") {
    // SPARK-59528: elements coerce to BIGINT; closest match is [1, 2]. ANSI on and off.
    Seq(true, false).foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
        checkSortMergeAsOf(
          sql(
            """
              |SELECT r.a
              |FROM VALUES (ARRAY(1, 3)) AS t(a)
              |ASOF JOIN VALUES (ARRAY(CAST(1 AS BIGINT), CAST(2 AS BIGINT))),
              |                 (ARRAY(CAST(1 AS BIGINT), CAST(4 AS BIGINT))) AS r(a)
              |MATCH_CONDITION (t.a >= r.a)
              |""".stripMargin),
          Row(Seq(1L, 2L)) :: Nil)
      }
    }
  }

  test("ARRAY<INT> vs ARRAY<FLOAT> coercible MATCH_CONDITION") {
    // SPARK-59528: INT vs FLOAT coercion under both ANSI modes (FLOAT non-ANSI, DOUBLE ANSI).
    Seq(true, false).foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
        checkSortMergeAsOf(
          sql(
            """
              |SELECT r.a
              |FROM VALUES (ARRAY(1, 3)) AS t(a)
              |ASOF JOIN VALUES (ARRAY(CAST(1 AS FLOAT), CAST(2 AS FLOAT))),
              |                 (ARRAY(CAST(1 AS FLOAT), CAST(4 AS FLOAT))) AS r(a)
              |MATCH_CONDITION (t.a >= r.a)
              |""".stripMargin),
          Row(Seq(1.0f, 2.0f)) :: Nil)
      }
    }
  }

  test("nested ARRAY<ARRAY<INT>> vs ARRAY<ARRAY<BIGINT>> coercible MATCH_CONDITION") {
    // SPARK-59528: array elements coerce element-wise; closest match is [[1, 2]].
    checkSortMergeAsOf(
      sql(
        """
          |SELECT r.a
          |FROM VALUES (ARRAY(ARRAY(1, 3))) AS t(a)
          |ASOF JOIN VALUES (ARRAY(ARRAY(CAST(1 AS BIGINT), CAST(2 AS BIGINT)))),
          |                 (ARRAY(ARRAY(CAST(1 AS BIGINT), CAST(4 AS BIGINT)))) AS r(a)
          |MATCH_CONDITION (t.a >= r.a)
          |""".stripMargin),
      Row(Seq(Seq(1L, 2L))) :: Nil)
  }

  test("ARRAY<INT> operands of different lengths MATCH_CONDITION") {
    // All elements equal, so length alone orders the arrays: the nearest right array <= [5, 5, 5]
    // is [5, 5], not the longer [5, 5, 5, 5].
    checkSortMergeAsOf(
      sql(
        """
          |SELECT r.a
          |FROM VALUES (ARRAY(5, 5, 5)) AS t(a)
          |ASOF JOIN VALUES (ARRAY(5)), (ARRAY(5, 5)), (ARRAY(5, 5, 5, 5)) AS r(a)
          |  MATCH_CONDITION (t.a >= r.a)
          |""".stripMargin),
      Row(Seq(5, 5)) :: Nil)
  }

  test("ARRAY<STRUCT> whole column MATCH_CONDITION") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT r.a
          |FROM VALUES (ARRAY(named_struct('seq', 1, 'val', 3))) AS t(a)
          |ASOF JOIN (
          |  SELECT * FROM VALUES
          |    (ARRAY(named_struct('seq', 1, 'val', 2))),
          |    (ARRAY(named_struct('seq', 1, 'val', 4))) AS r(a)
          |) r
          |  MATCH_CONDITION (t.a >= r.a)
          |""".stripMargin),
      Row(Seq(Row(1, 2))) :: Nil)
  }

  test("ARRAY<STRUCT> MATCH_CONDITION with compatible but not identical element types") {
    // Left element fields are BIGINT, right element fields are INT. Per-field coercion lets the
    // element-wise comparison type-check; the backward match returns the nearest qualifying row.
    checkSortMergeAsOf(
      sql(
        """
          |SELECT r.a
          |FROM VALUES
          |  (ARRAY(named_struct('seq', CAST(1 AS BIGINT), 'val', CAST(5 AS BIGINT)))) AS t(a)
          |ASOF JOIN (
          |  SELECT * FROM VALUES
          |    (ARRAY(named_struct('seq', 1, 'val', 2))),
          |    (ARRAY(named_struct('seq', 1, 'val', 4))),
          |    (ARRAY(named_struct('seq', 1, 'val', 7))) AS r(a)
          |) r
          |  MATCH_CONDITION (t.a >= r.a)
          |""".stripMargin),
      Row(Seq(Row(1, 4))) :: Nil)
  }

  test("ARRAY of empty STRUCT MATCH_CONDITION") {
    // Empty struct elements always tie, so the arrays order by length alone.
    Seq(">=" -> 2, ">" -> 1, "<=" -> 2, "<" -> 3).foreach { case (op, size) =>
      checkSortMergeAsOf(
        sql(
          s"""
             |SELECT size(r.a)
             |FROM VALUES (ARRAY(named_struct(), named_struct())) AS t(a)
             |ASOF JOIN VALUES
             |  (ARRAY(named_struct())),
             |  (ARRAY(named_struct(), named_struct())),
             |  (ARRAY(named_struct(), named_struct(), named_struct())) AS r(a)
             |  MATCH_CONDITION (t.a $op r.a)
             |""".stripMargin),
        Row(size) :: Nil)
    }
  }

  test("STRUCT tuple from scalar columns MATCH_CONDITION") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT r.c1, r.c2
          |FROM VALUES (1, 2) AS t(c1, c2)
          |ASOF JOIN VALUES (1, 1) AS r(c1, c2)
          |MATCH_CONDITION ((t.c1, t.c2) >= (r.c1, r.c2))
          |""".stripMargin),
      Row(1, 1) :: Nil)
  }

  test("MATCH_CONDITION operand arithmetic with ON") {
    setupTradeQuoteViews()
    checkSortMergeAsOf(
      sql(
        """
          |SELECT count(*) AS cnt
          |FROM trades t ASOF JOIN quotes q
          |  MATCH_CONDITION (t.trade_time >= date_trunc('hour', q.quote_time))
          |  ON t.symbol = q.symbol
          |""".stripMargin),
      Row(3L) :: Nil)
  }

  test("non-equi ON filters right rows before closest match") {
    setupTradeQuoteViews()
    checkSortMergeAsOf(
      sql(
        """
          |SELECT t.symbol, q.bid_price
          |FROM trades t ASOF JOIN quotes q
          |  MATCH_CONDITION (t.trade_time >= q.quote_time)
          |  ON t.symbol = q.symbol AND q.bid_price > 200
          |ORDER BY t.symbol
          |""".stripMargin),
      Row("MSFT", 420.50) :: Nil)
  }

  test("non-equi range predicate in ON") {
    setupTradeQuoteViews()
    checkSortMergeAsOf(
      sql(
        """
          |SELECT t.symbol, q.bid_price
          |FROM trades t ASOF JOIN quotes q
          |  MATCH_CONDITION (t.trade_time >= q.quote_time)
          |  ON abs(unix_timestamp(t.trade_time) - unix_timestamp(q.quote_time)) < 30
          |ORDER BY t.symbol, t.trade_time
          |""".stripMargin),
      Seq(
        Row("AAPL", 180.10),
        Row("AAPL", 180.20),
        Row("MSFT", 180.20)))
  }

  test("disjunction in ON") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT count(*) AS cnt
          |FROM VALUES
          |  (CAST(NULL AS STRING), TIMESTAMP '2026-06-29 10:00:00') AS t(symbol, trade_time)
          |ASOF JOIN VALUES
          |  (CAST(NULL AS STRING), TIMESTAMP '2026-06-29 09:00:00') AS q(symbol, quote_time)
          |  MATCH_CONDITION (t.trade_time >= q.quote_time)
          |  ON t.symbol = q.symbol OR (t.symbol IS NULL AND q.symbol IS NULL)
          |""".stripMargin),
      Row(1L) :: Nil)
  }

  test("ON predicate with expression over both tables") {
    setupTradeQuoteViews()
    checkSortMergeAsOf(
      sql(
        """
          |SELECT t.symbol, q.bid_price
          |FROM trades t ASOF JOIN quotes q
          |  MATCH_CONDITION (t.trade_time >= q.quote_time)
          |  ON substr(t.symbol, 1, 1) = substr(q.symbol, 1, 1)
          |ORDER BY t.symbol, t.trade_time
          |""".stripMargin),
      Seq(
        Row("AAPL", 180.10),
        Row("AAPL", 180.20),
        Row("MSFT", 420.50)))
  }

  test("ON TRUE matches whole right side") {
    setupTradeQuoteViews()
    checkSortMergeAsOf(
      sql(
        """
          |SELECT count(*) AS cnt
          |FROM trades t ASOF JOIN quotes q
          |  MATCH_CONDITION (t.trade_time >= q.quote_time)
          |  ON TRUE
          |""".stripMargin),
      Row(3L) :: Nil)
  }

  test("ON FALSE empties right search space") {
    setupTradeQuoteViews()
    checkSortMergeAsOf(
      sql(
        """
          |SELECT count(*) AS inner_cnt
          |FROM trades t ASOF JOIN quotes q
          |  MATCH_CONDITION (t.trade_time >= q.quote_time)
          |  ON FALSE
          |""".stripMargin),
      Row(0L) :: Nil)
    checkSortMergeAsOf(
      sql(
        """
          |SELECT count(*) AS left_cnt
          |FROM trades t LEFT ASOF JOIN quotes q
          |  MATCH_CONDITION (t.trade_time >= q.quote_time)
          |  ON FALSE
          |""".stripMargin),
      Row(4L) :: Nil)
  }

  test("WHERE on the left input is pushed into the left scan") {
    withTradeQuoteTables {
      val query = sql(
        """
          |SELECT t.trade_time, t.symbol, q.bid_price
          |FROM trades_t t LEFT ASOF JOIN quotes_t q
          |  MATCH_CONDITION (t.trade_time >= q.quote_time)
          |  ON t.symbol = q.symbol
          |WHERE t.quantity > 100
          |""".stripMargin)
      checkSortMergeAsOf(
        query,
        Row(Timestamp.valueOf("2026-06-29 10:00:11"), "AAPL", 180.20) :: Nil)
      assert(scanFilterColumns(query, "trades_t").contains("quantity"))
    }
  }

  test("WHERE on the right input runs after the closest match, unlike the same ON predicate") {
    withTradeQuoteTables {
      // The 10:00:11 trade's closest quote is 180.20. WHERE drops the trade, while ON skips
      // that quote and matches the older 180.15 one.
      val whereQuery = sql(
        """
          |SELECT t.trade_time, q.bid_price
          |FROM trades_t t ASOF JOIN quotes_t q
          |  MATCH_CONDITION (t.trade_time >= q.quote_time)
          |  ON t.symbol = q.symbol
          |WHERE q.bid_price < 180.18
          |""".stripMargin)
      checkSortMergeAsOf(
        whereQuery,
        Row(Timestamp.valueOf("2026-06-29 10:00:05"), 180.10) :: Nil)
      assert(!scanFilterColumns(whereQuery, "quotes_t").contains("bid_price"))

      val onQuery = sql(
        """
          |SELECT t.trade_time, q.bid_price
          |FROM trades_t t ASOF JOIN quotes_t q
          |  MATCH_CONDITION (t.trade_time >= q.quote_time)
          |  ON t.symbol = q.symbol AND q.bid_price < 180.18
          |""".stripMargin)
      checkSortMergeAsOf(
        onQuery,
        Seq(
          Row(Timestamp.valueOf("2026-06-29 10:00:05"), 180.10),
          Row(Timestamp.valueOf("2026-06-29 10:00:11"), 180.15)))
      assert(scanFilterColumns(onQuery, "quotes_t").contains("bid_price"))
    }
  }

  test("ON predicate on the left input keeps every row of a LEFT ASOF JOIN") {
    setupTradeQuoteViews()
    checkSortMergeAsOf(
      sql(
        """
          |SELECT t.trade_time, q.bid_price
          |FROM trades t LEFT ASOF JOIN quotes q
          |  MATCH_CONDITION (t.trade_time >= q.quote_time)
          |  ON t.symbol = q.symbol AND t.quantity > 100
          |""".stripMargin),
      Seq(
        Row(Timestamp.valueOf("2026-06-29 09:59:59"), null),
        Row(Timestamp.valueOf("2026-06-29 10:00:05"), null),
        Row(Timestamp.valueOf("2026-06-29 10:00:11"), 180.20),
        Row(Timestamp.valueOf("2026-06-29 10:00:12"), null)))
  }

  test("LEFT ASOF JOIN null-pads NOT NULL right catalog columns") {
    withTable("right_nn", "left_nn") {
      sql("CREATE TABLE right_nn (k INT NOT NULL, v STRING NOT NULL) USING parquet")
      sql("INSERT INTO right_nn VALUES (3, 'a')")
      sql("CREATE TABLE left_nn (k INT, label STRING) USING parquet")
      sql("INSERT INTO left_nn VALUES (2, 'unmatched')")
      checkSortMergeAsOf(
        sql(
          """
            |SELECT l.k, l.label, r.v
            |FROM left_nn l LEFT ASOF JOIN right_nn r
            |  MATCH_CONDITION (l.k >= r.k)
            |""".stripMargin),
        Row(2, "unmatched", null) :: Nil)
    }
  }

  test("strict > MATCH_CONDITION excludes equal right rows") {
    // Two rows are strictly earlier, so a forward scan would pick 08:00 instead.
    checkSortMergeAsOf(
      sql(
        """
          |SELECT q.quote_time
          |FROM VALUES (TIMESTAMP '2026-06-29 10:00:00', 'X') AS t(trade_time, symbol)
          |ASOF JOIN VALUES
          |  (TIMESTAMP '2026-06-29 10:00:00', 'X'),
          |  (TIMESTAMP '2026-06-29 09:00:00', 'X'),
          |  (TIMESTAMP '2026-06-29 08:00:00', 'X') AS q(quote_time, symbol)
          |  MATCH_CONDITION (t.trade_time > q.quote_time)
          |  ON t.symbol = q.symbol
          |""".stripMargin),
      Row(Timestamp.valueOf("2026-06-29 09:00:00")) :: Nil)
  }

  test("strict < forward MATCH_CONDITION") {
    // Two rows are strictly later, so a backward scan would pick 12:00 instead.
    checkSortMergeAsOf(
      sql(
        """
          |SELECT r.ts
          |FROM VALUES (TIMESTAMP '2026-06-29 10:00:00') AS t(ts)
          |ASOF JOIN VALUES
          |  (TIMESTAMP '2026-06-29 10:00:00'),
          |  (TIMESTAMP '2026-06-29 11:00:00'),
          |  (TIMESTAMP '2026-06-29 12:00:00') AS r(ts)
          |  MATCH_CONDITION (t.ts < r.ts)
          |""".stripMargin),
      Row(Timestamp.valueOf("2026-06-29 11:00:00")) :: Nil)
  }

  test("forward ARRAY<INT> <= MATCH_CONDITION") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT r.a
          |FROM VALUES (ARRAY(1, 5)) AS t(a)
          |ASOF JOIN VALUES (ARRAY(1, 6)), (ARRAY(1, 8)) AS r(a)
          |  MATCH_CONDITION (t.a <= r.a)
          |""".stripMargin),
      Row(Seq(1, 6)) :: Nil)
  }

  test("forward MATCH_CONDITION with NULL array elements picks the smallest right row") {
    // NULL elements sort first: [null] < [null, null] < [5]. [null] equals the left row.
    val nullInt = "CAST(NULL AS INT)"
    Seq("<=" -> "n", "<" -> "nn").foreach { case (op, tag) =>
      checkSortMergeAsOf(
        sql(
          s"""
             |SELECT r.tag
             |FROM VALUES (ARRAY($nullInt)) AS t(a)
             |ASOF JOIN VALUES
             |  (ARRAY($nullInt), 'n'),
             |  (ARRAY($nullInt, $nullInt), 'nn'),
             |  (ARRAY(5), 'five') AS r(a, tag)
             |  MATCH_CONDITION (t.a $op r.a)
             |""".stripMargin),
        Row(tag) :: Nil)
    }
  }

  test("forward MATCH_CONDITION with a NULL struct field picks the smallest right row") {
    // A NULL field sorts first, so {null, 5} < {null, 7} < {0, 1}. {null, 5} equals the left row.
    val nullInt = "CAST(NULL AS INT)"
    for {
      (left, right) <- Seq("t.k" -> "r.k", "(t.k.f, t.k.seq)" -> "(r.k.f, r.k.seq)")
      (op, tag) <- Seq("<=" -> "s5", "<" -> "s7")
    } {
      checkSortMergeAsOf(
        sql(
          s"""
             |SELECT r.tag
             |FROM VALUES (named_struct('f', $nullInt, 'seq', 5)) AS t(k)
             |ASOF JOIN VALUES
             |  (named_struct('f', $nullInt, 'seq', 5), 's5'),
             |  (named_struct('f', $nullInt, 'seq', 7), 's7'),
             |  (named_struct('f', 0, 'seq', 1), 's1') AS r(k, tag)
             |  MATCH_CONDITION ($left $op $right)
             |""".stripMargin),
        Row(tag) :: Nil)
    }
  }

  test("forward MATCH_CONDITION with a leading STRING field picks the smallest right row") {
    // {b, 5} equals the left row. {c, 1} < {d, 0}, but the STRING field distance is 1 for both,
    // so a distance picks {d, 0}.
    for {
      (left, right) <- Seq("t.k" -> "r.k", "(t.k.s, t.k.n)" -> "(r.k.s, r.k.n)")
      (op, tag) <- Seq("<=" -> "b5", "<" -> "c1")
    } {
      checkSortMergeAsOf(
        sql(
          s"""
             |SELECT r.tag
             |FROM VALUES (named_struct('s', 'b', 'n', 5)) AS t(k)
             |ASOF JOIN VALUES
             |  (named_struct('s', 'b', 'n', 5), 'b5'),
             |  (named_struct('s', 'c', 'n', 1), 'c1'),
             |  (named_struct('s', 'd', 'n', 0), 'd0') AS r(k, tag)
             |  MATCH_CONDITION ($left $op $right)
             |""".stripMargin),
        Row(tag) :: Nil)
    }
  }

  test("forward MATCH_CONDITION skips a closer row that fails a non-equi ON predicate") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT r.k
          |FROM VALUES (10, 5) AS t(k, lim)
          |ASOF JOIN VALUES (12, 9), (15, 1), (20, 0) AS r(k, v)
          |  MATCH_CONDITION (t.k <= r.k)
          |  ON r.v < t.lim
          |""".stripMargin),
      Row(15) :: Nil)
  }

  // (right - left) overflows the type for the far row, and for DECIMAL for both rows. So a
  // distance-based scan throws or gives a wrong answer.
  Seq(
    ("INT", "-100", "-50", "2147483647"),
    ("BIGINT", "-100", "-50", "9223372036854775807"),
    ("DECIMAL(38, 0)", "-99999999999999999999999999999999999999", "1", "2"),
    ("INTERVAL YEAR TO MONTH",
      "INTERVAL '-100' MONTH", "INTERVAL '-50' MONTH", "INTERVAL '178956970-7' YEAR TO MONTH"),
    ("INTERVAL DAY TO SECOND",
      "INTERVAL '-100' DAY", "INTERVAL '-50' DAY", "INTERVAL '106751991' DAY")
  ).foreach { case (dataType, left, near, far) =>
    test(s"forward $dataType MATCH_CONDITION picks the closest row when right - left overflows") {
      Seq(true, false).foreach { ansiEnabled =>
        withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
          Seq("<=", "<").foreach { op =>
            checkSortMergeAsOf(
              sql(
                s"""
                   |SELECT r.tag
                   |FROM VALUES (CAST($left AS $dataType)) AS t(k)
                   |ASOF JOIN VALUES
                   |  (CAST($near AS $dataType), 'near'),
                   |  (CAST($far AS $dataType), 'far') AS r(k, tag)
                   |  MATCH_CONDITION (t.k $op r.k)
                   |""".stripMargin),
              Row("near") :: Nil)
          }
        }
      }
    }
  }

  test("ARRAY<STRUCT> MATCH_CONDITION with fields of different types") {
    // Each element has an INT distance and an INTERVAL distance.
    def element(first: String, time: String): String =
      s"ARRAY(named_struct('f', $first, 'ts', TIMESTAMP '2026-06-29 $time'))"
    for {
      first <- Seq("1", "named_struct()")
      (op, tag) <- Seq(">=" -> "r9", ">" -> "r9", "<=" -> "r11", "<" -> "r11")
    } {
      checkSortMergeAsOf(
        sql(
          s"""
             |SELECT r.tag
             |FROM VALUES (${element(first, "10:00:00")}) AS t(a)
             |ASOF JOIN VALUES
             |  (${element(first, "09:00:00")}, 'r9'),
             |  (${element(first, "11:00:00")}, 'r11') AS r(a, tag)
             |  MATCH_CONDITION (t.a $op r.a)
             |""".stripMargin),
        Row(tag) :: Nil)
    }
  }

  test("ARRAY<STRUCT> operands with different field names") {
    checkSortMergeAsOf(
      sql(
        """
          |SELECT r.a
          |FROM VALUES (ARRAY(named_struct('x', 1, 'y', 3))) AS t(a)
          |ASOF JOIN VALUES (ARRAY(named_struct('p', 1, 'q', 2))) AS r(a)
          |  MATCH_CONDITION (t.a >= r.a)
          |""".stripMargin),
      Row(Seq(Row(1, 2))) :: Nil)
  }
}
