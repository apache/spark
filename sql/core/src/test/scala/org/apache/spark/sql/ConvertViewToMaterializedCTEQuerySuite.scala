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

import org.apache.spark.sql.catalyst.plans.logical.{
  BROADCAST, CommandResult, Join, JoinHint, LogicalPlan, RepartitionByExpression}
import org.apache.spark.sql.catalyst.plans.physical.RoundRobinPartitioning
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Integration tests for the `ConvertViewToMaterializedCTE` optimizer rule: repeated
 * references to the same view are rewritten into one CTE definition with multiple
 * references when `spark.sql.optimizer.convertViewToMaterializedCTE.enabled` is enabled.
 * After the final `Replace CTE with Repartition` batch, a converted view shows up as one
 * repartition node per reference site; exchange reuse deduplicates them at execution
 * time.
 */
class ConvertViewToMaterializedCTEQuerySuite extends QueryTest with SharedSparkSession
    with AdaptiveSparkPlanHelper {
  import testImplicits._

  private val selfJoinQuery =
    "SELECT t1.id, t2.k FROM v t1 JOIN v t2 ON t1.id = t2.id WHERE t1.id < 10"

  private def withSelfJoinedView(f: => Unit): Unit = {
    withTempView("v") {
      spark.range(0, 100).select($"id", ($"id" % 10).as("k")).createOrReplaceTempView("v")
      f
    }
  }

  private def countRepartitions(query: String): Int =
    countRepartitions(spark.sql(query))

  private def countRepartitions(df: DataFrame): Int =
    countRepartitions(df.queryExecution.optimizedPlan)

  private def countRepartitions(plan: LogicalPlan): Int =
    plan.collectWithSubqueries {
      case _: RepartitionByExpression => true
    }.length

  test("self-joined view returns identical results with conversion enabled") {
    withSelfJoinedView {
      val expected = spark.sql(selfJoinQuery).collect()
      withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
        checkAnswer(spark.sql(selfJoinQuery), expected)
      }
    }
  }

  test("conversion adds one repartition per reference site") {
    withSelfJoinedView {
      assert(countRepartitions(selfJoinQuery) == 0)
      withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
        // One shuffle boundary per reference site; identical shuffles are then reused.
        assert(countRepartitions(selfJoinQuery) == 2)
      }
    }
  }

  test("conversion is disabled when exchange reuse is disabled") {
    withSelfJoinedView {
      withSQLConf(SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
        assert(countRepartitions(selfJoinQuery) == 0)
        withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
          assert(countRepartitions(selfJoinQuery) == 0)
        }
      }
    }
  }

  test("the converted view body is computed once") {
    // Assert on the executed plan. With AQE on, the reused exchange sits behind
    // AdaptiveSparkPlanExec query stages, which `collect` descends through. Count
    // only the conversion's round-robin exchanges, excluding any join shuffles.
    withSelfJoinedView {
      Seq(true, false).foreach { aqeEnabled =>
        def viewExchanges: Int = {
          val df = spark.sql(selfJoinQuery)
          df.collect()
          collect(df.queryExecution.executedPlan) {
            case e: ShuffleExchangeExec
              if e.outputPartitioning.isInstanceOf[RoundRobinPartitioning] => 1
            case e: ReusedExchangeExec
              if e.outputPartitioning.isInstanceOf[RoundRobinPartitioning] => 1
          }.length
        }
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqeEnabled.toString) {
          // Without conversion there is no round-robin repartition to reuse.
          assert(viewExchanges == 0)
          withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
            // One live exchange plus one reuse of it: the body is computed once.
            assert(viewExchanges == 2)
          }
        }
      }
    }
  }

  test("non-deterministic views are not converted") {
    withTempView("rand_view") {
      spark.range(0, 10).select($"id", rand(0).as("r"))
        .createOrReplaceTempView("rand_view")
      val query = "SELECT t1.r FROM rand_view t1 JOIN rand_view t2 ON t1.id = t2.id"
      withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
        assert(countRepartitions(query) == 0)
      }
    }
  }

  test("input file expressions above a view prevent conversion") {
    withTable("t") {
      sql("CREATE TABLE t USING parquet AS SELECT id, id % 10 AS k FROM range(4)")
      withTempView("v") {
        sql("CREATE TEMP VIEW v AS SELECT id, k FROM t")
        val query =
          """
            |SELECT input_file_name() <> '' AS has_file,
            |  input_file_block_start() >= 0 AS has_start,
            |  input_file_block_length() >= 0 AS has_length, id
            |FROM v WHERE id < 2
            |UNION ALL
            |SELECT input_file_name() <> '', input_file_block_start() >= 0,
            |  input_file_block_length() >= 0, id
            |FROM v WHERE id < 2
            |""".stripMargin
        val expected = sql(query).collect()
        assert(expected.nonEmpty && expected.forall(row =>
          row.getBoolean(0) && row.getBoolean(1) && row.getBoolean(2)))
        withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
          checkAnswer(sql(query), expected)
          assert(countRepartitions(query) == 0)
        }
      }
    }
  }

  test("a divergent view occurrence is not rebound to a converted definition") {
    // `held` captures v's first body; the third reference is analyzed after v is
    // replaced. The two bodies diverge, so the name must not convert: rebinding the
    // stale occurrence to the new definition would return the wrong rows.
    withTempView("v") {
      sql("CREATE OR REPLACE TEMP VIEW v AS SELECT id, id AS k FROM range(4)")
      val held = spark.table("v")
      val pair = held.join(held.as("b"), "id").select($"id")
      sql("CREATE OR REPLACE TEMP VIEW v AS SELECT id, id + 100 AS k FROM range(4)")
      // A fresh Dataset per setting so each check optimizes anew; reusing one Dataset
      // would reuse its cached optimized plan.
      def mkQuery(): DataFrame = pair.join(spark.table("v"), "id").select($"id", $"k")
      assert(countRepartitions(mkQuery()) == 0)
      val expected = mkQuery().collect()
      withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
        // Rows alone cannot tell conversion off from a conversion that happens to
        // match them; assert the plan adds no repartition either.
        assert(countRepartitions(mkQuery()) == 0)
        checkAnswer(mkQuery(), expected)
      }
    }
  }

  test("stale view pair inside a scalar subquery does not crash") {
    // The same divergence as above, but the occurrences sit inside scalar subqueries.
    // Converting there would wrap the Subquery root in WithCTE and break FinishAnalysis,
    // which destructures that root.
    withTempView("v", "w") {
      sql("CREATE OR REPLACE TEMP VIEW v AS SELECT id, id AS k FROM range(4)")
      val held = spark.table("v")
      val pair = held.join(held.as("b"), "id").select($"id")
      pair.createOrReplaceTempView("w")
      sql("CREATE OR REPLACE TEMP VIEW v AS SELECT id, id + 100 AS k FROM range(4)")
      val query = "SELECT (SELECT count(*) FROM w) AS cw, (SELECT count(*) FROM v) AS cv"
      val expected = spark.sql(query).collect()
      withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
        assert(countRepartitions(query) == 0)
        checkAnswer(spark.sql(query), expected)
      }
    }
  }

  test("converted definition picked from a subquery occurrence is cleaned") {
    // The definition body is taken from a first-visited subquery occurrence, which can
    // still hold analysis-only nodes (the table's SubqueryAlias). They must not reach
    // the planner.
    withTable("t") {
      sql("CREATE TABLE t USING parquet AS SELECT id, id % 10 AS k FROM range(0, 20)")
      withTempView("v") {
        sql("CREATE TEMP VIEW v AS SELECT id, k FROM t WHERE k < 4")
        val query = "SELECT (SELECT count(*) FROM v) AS c, (SELECT max(id) FROM v) AS m"
        val expected = spark.sql(query).collect()
        withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
          checkAnswer(spark.sql(query), expected)
        }
      }
    }
  }

  test("converted definition picked from a subquery occurrence, with view hints") {
    // Same as above, with a hint in the body. A plan assertion on surviving
    // ResolvedHint would be vacuous - EliminateResolvedHint runs whether or not
    // conversion fires - so this is the no-crash and result-correctness variant
    // for a hinted body whose definition is lifted from a subquery occurrence.
    withTable("t") {
      sql("CREATE TABLE t USING parquet AS SELECT id, id % 10 AS k FROM range(0, 20)")
      withTempView("v") {
        sql("CREATE TEMP VIEW v AS SELECT /*+ BROADCAST(t) */ id, k FROM t WHERE k < 4")
        val query = "SELECT (SELECT count(*) FROM v) AS c, (SELECT max(id) FROM v) AS m"
        val expected = spark.sql(query).collect()
        withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
          checkAnswer(spark.sql(query), expected)
        }
      }
    }
  }

  test("join hints in the view body are applied to the consuming joins") {
    // A hint in the view body applies to the join the view takes part in (SPARK-40999).
    // Left in the definition, the leaf CTERelationRef would strand it and downgrade the
    // self-join to SortMergeJoin.
    withTable("t", "big") {
      sql("CREATE TABLE t USING parquet AS SELECT id, id % 10 AS k FROM range(0, 100)")
      sql("CREATE TABLE big USING parquet AS SELECT id FROM range(0, 1000)")
      withTempView("v") {
        sql("CREATE TEMP VIEW v AS SELECT /*+ BROADCAST(t) */ id, k FROM t WHERE k < 4")

        // The self join rewrites both occurrences as new references; the chained joins
        // reuse the definition the first occurrence created.
        val queries = Seq(
          "SELECT t1.id FROM v t1 JOIN v t2 ON t1.id = t2.id",
          "SELECT b.id FROM big b JOIN v t1 ON b.id = t1.id JOIN v t2 ON b.id = t2.id")

        queries.foreach { query =>
          val expected = countBroadcastHintedJoins(query)
          assert(expected > 0, s"precondition broken, hint not resolved: $query")
          withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
            assert(countBroadcastHintedJoins(query) == expected,
              s"conversion changed the join hints of: $query")
          }
        }

        // With the broadcast threshold disabled, the hint is what keeps the self join
        // a BroadcastHashJoin instead of a SortMergeJoin.
        val query = queries.head
        val expected = countBroadcastHashJoins(query, aqe = false)
        assert(expected == 1, s"precondition broken, no broadcast join: $expected")
        withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
          assert(countBroadcastHashJoins(query, aqe = false) == expected,
            "broadcast hint was lost under conversion")
        }
      }
    }
  }

  test("occurrences with divergent view hints are not converted") {
    // Canonicalization ignores ResolvedHint, so `held` and its unhinted replacement look
    // alike; converting would copy the first occurrence's hint onto the second join.
    withTable("t", "big") {
      sql("CREATE TABLE t USING parquet AS SELECT id, id % 10 AS k FROM range(0, 100)")
      sql("CREATE TABLE big USING parquet AS SELECT id FROM range(0, 1000)")
      withTempView("v") {
        sql("CREATE TEMP VIEW v AS SELECT /*+ BROADCAST(t) */ id, k FROM t WHERE k < 4")
        val held = spark.table("v")
        sql("CREATE OR REPLACE TEMP VIEW v AS SELECT id, k FROM t WHERE k < 4")
        val big = spark.table("big")
        def mkQuery(): DataFrame = big.join(held, "id").join(spark.table("v"), "id")
        def joinHints(df: DataFrame): Seq[JoinHint] =
          df.queryExecution.optimizedPlan.collectWithSubqueries { case j: Join => j.hint }

        val expectedHints = joinHints(mkQuery())
        assert(expectedHints.count(h =>
          h.leftHint.exists(_.strategy.contains(BROADCAST)) ||
            h.rightHint.exists(_.strategy.contains(BROADCAST))) == 1)
        withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
          assert(countRepartitions(mkQuery()) == 0)
          assert(joinHints(mkQuery()) == expectedHints)
        }
      }
    }
  }

  private def countBroadcastHintedJoins(query: String): Int =
    spark.sql(query).queryExecution.optimizedPlan.collect {
      case j: Join => j.hint
    }.count { h =>
      h.leftHint.exists(_.strategy.contains(BROADCAST)) ||
        h.rightHint.exists(_.strategy.contains(BROADCAST))
    }

  private def countBroadcastHashJoins(query: String, aqe: Boolean): Int = {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString) {
      val df = spark.sql(query)
      df.collect()
      // `collect` from AdaptiveSparkPlanHelper also descends through AQE wrappers.
      collect(df.queryExecution.executedPlan) {
        case _: BroadcastHashJoinExec => 1
      }.length
    }
  }

  test("view with top-level ORDER BY is not converted") {
    // The added shuffle would drop the ORDER BY an outer LIMIT observes. OFFSET wraps
    // the Sort in Offset/limit nodes, which must be rejected too.
    val bodies = Seq(
      "SELECT id FROM range(100) ORDER BY id",
      "SELECT id FROM range(100) ORDER BY id OFFSET 1",
      "SELECT id FROM range(100) ORDER BY id LIMIT 50 OFFSET 1")
    bodies.foreach { body =>
      withTempView("v") {
        sql(s"CREATE OR REPLACE TEMP VIEW v AS $body")
        val query = "SELECT t1.id FROM v t1 JOIN v t2 ON t1.id = t2.id LIMIT 3"
        val expected = spark.sql(query).collect()
        assert(countRepartitions(query) == 0)
        withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
          assert(countRepartitions(query) == 0)
          checkAnswer(spark.sql(query), expected)
        }
      }
    }
  }

  test("a Sort buried in the body's main plan region is not converted") {
    // The shuffle drops order the body's main plan exposes, not only a top-level ORDER
    // BY. The subquery case is the opposite and is covered by "a Sort confined to a
    // subquery expression does not block conversion".
    withTable("t") {
      sql("CREATE TABLE t USING parquet AS SELECT id, id % 10 AS k FROM range(0, 100)")
      withTempView("v1") {
        sql("CREATE TEMP VIEW v1 AS SELECT id FROM t ORDER BY id DESC")
        val bodies = Seq(
          "SELECT id FROM v1",
          "SELECT DISTINCT id FROM v1",
          "SELECT id FROM v1 LIMIT 3",
          "(SELECT id FROM t ORDER BY id DESC) UNION ALL (SELECT id FROM t ORDER BY id DESC)",
          "WITH c AS (SELECT id FROM t) SELECT id FROM c ORDER BY id DESC",
          "WITH c AS (SELECT id FROM t ORDER BY id DESC) SELECT id FROM c",
          "SELECT v1.id FROM v1 JOIN t ON v1.id = t.id")
        bodies.foreach { body =>
          withTempView("v") {
            sql(s"CREATE TEMP VIEW v AS $body")
            val query = "SELECT t1.id FROM v t1 JOIN v t2 ON t1.id = t2.id LIMIT 3"
            val expected = spark.sql(query).collect()
            withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
              assert(countRepartitions(query) == 0, s"should not convert: $body")
              checkAnswer(spark.sql(query), expected)
            }
          }
        }
      }
    }
  }

  test("a TABLESAMPLE over a sorted inner view is not converted") {
    // REPEATABLE keeps the seedless-sample guard from rejecting the view before
    // hasMainRegionSort sees the inner ORDER BY.
    withTable("t") {
      sql("CREATE TABLE t USING parquet AS SELECT id, id % 10 AS k FROM range(0, 100)")
      withTempView("v1", "v") {
        sql("CREATE TEMP VIEW v1 AS SELECT id FROM t ORDER BY id DESC")
        sql("CREATE TEMP VIEW v AS SELECT id FROM v1 TABLESAMPLE(50 PERCENT) REPEATABLE(7)")
        val query = "SELECT t1.id FROM v t1 JOIN v t2 ON t1.id = t2.id LIMIT 3"
        withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
          assert(countRepartitions(query) == 0)
        }
      }
    }
  }

  test("an unseeded TABLESAMPLE view is not converted") {
    // Sample has no expressions, so `deterministic` is true even though SampleExec draws
    // a fresh seed. References must keep their independent samples.
    withTable("t") {
      sql("CREATE TABLE t USING parquet AS SELECT id, id % 10 AS k FROM range(0, 100)")
      withTempView("v") {
        sql("CREATE TEMP VIEW v AS SELECT id, k FROM t TABLESAMPLE(50 PERCENT)")
        val query = "SELECT count(*) FROM (SELECT id FROM v UNION ALL SELECT id FROM v)"
        withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
          assert(countRepartitions(query) == 0)
        }
      }
    }
  }

  test("a repeatable TABLESAMPLE view is converted") {
    // An explicit seed makes the body deterministic, so both references can share it.
    withTable("t") {
      sql("CREATE TABLE t USING parquet AS SELECT id, id % 10 AS k FROM range(0, 100)")
      withTempView("v") {
        sql("CREATE TEMP VIEW v AS " +
          "SELECT id, k FROM t TABLESAMPLE(50 PERCENT) REPEATABLE(7)")
        val query = "SELECT count(*) FROM (SELECT id FROM v UNION ALL SELECT id FROM v)"
        val expected = spark.sql(query).collect()
        withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
          assert(countRepartitions(query) == 2)
          checkAnswer(spark.sql(query), expected)
        }
      }
    }
  }

  test("a Sort confined to a subquery expression does not block conversion") {
    // The IN-subquery's ORDER BY picks which rows the body produces but does not order
    // the body's output, so v still converts.
    withTable("t") {
      sql("CREATE TABLE t USING parquet AS SELECT id, id % 10 AS k FROM range(0, 100)")
      withTempView("v") {
        sql("CREATE TEMP VIEW v AS " +
          "SELECT id, k FROM t WHERE id IN (SELECT id FROM t ORDER BY id DESC LIMIT 5)")
        val query = "SELECT t1.id FROM v t1 JOIN v t2 ON t1.id = t2.id LIMIT 5"
        val expected = spark.sql(query).collect()
        withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
          assert(countRepartitions(query) == 2)
          checkAnswer(spark.sql(query), expected)
        }
      }
    }
  }

  test("insert into a table selecting from a repeatedly referenced view") {
    withTable("dest") {
      withSelfJoinedView {
        sql("CREATE TABLE dest (id BIGINT, k BIGINT) USING parquet")
        withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
          val insert = sql(s"INSERT INTO dest $selfJoinQuery")
          // The command's logical child is analyzed rather than optimized. Inspect the
          // physical plan and count only the conversion's round-robin exchanges, excluding
          // any join shuffles.
          val insertPlan = insert.queryExecution.optimizedPlan.collectFirst {
            case c: CommandResult => c.commandPhysicalPlan
          }.getOrElse(fail("expected an INSERT command plan"))
          val viewShuffles = collect(insertPlan) {
            case e: ShuffleExchangeExec
              if e.outputPartitioning.isInstanceOf[RoundRobinPartitioning] => 1
            case e: ReusedExchangeExec
              if e.outputPartitioning.isInstanceOf[RoundRobinPartitioning] => 1
          }
          assert(viewShuffles.sum == 2, s"INSERT plan: $insertPlan")
        }
        checkAnswer(spark.table("dest"), spark.range(0, 10).select($"id", ($"id" % 10)))
      }
    }
  }

  test("grouping by a correlated scalar subquery over a converted view") {
    // The select list and GROUP BY contain equivalent scalar subqueries. Every converted
    // view occurrence must have the same reference shape so PullOutGroupingExpressions
    // still recognizes those copies as one grouping expression.
    withTable("l", "tr") {
      withTempView("vr") {
        sql("CREATE TABLE tr (c BIGINT, d DOUBLE) USING parquet")
        sql("INSERT INTO tr VALUES (1, 1.0), (2, 2.0), (2, 4.0), (3, 2.0)")
        sql("CREATE TABLE l (a BIGINT) USING parquet")
        sql("INSERT INTO l VALUES (1), (2), (3), (4)")
        sql("CREATE TEMP VIEW vr AS SELECT * FROM tr")
        val queries = Seq(
          "SELECT a, (SELECT sum(d) FROM vr WHERE a = c) sum_d " +
            "FROM l l1 GROUP BY 1, 2 ORDER BY a",
          "SELECT a, (SELECT sum(d) FROM vr WHERE a = c) sum_d FROM l l1 " +
            "GROUP BY a, (SELECT sum(d) FROM vr WHERE a = c) ORDER BY a")
        val expected = Seq(
          Row(1L, 1.0), Row(2L, 6.0), Row(3L, 2.0), Row(4L, null))
        queries.foreach { query =>
          withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
            assert(countRepartitions(query) > 0)
            checkAnswer(sql(query), expected)
          }
        }
      }
    }
  }

  test("internally correlated view executes correctly with conversion enabled") {
    withTempView("t", "s", "v") {
      spark.range(5).selectExpr("id AS k", "id AS x").createOrReplaceTempView("t")
      spark.range(5).selectExpr("id AS k", "id AS y").createOrReplaceTempView("s")
      sql("CREATE OR REPLACE TEMP VIEW v AS " +
        "SELECT * FROM t WHERE x IN (SELECT y FROM s WHERE s.k = t.k)")
      val query = "SELECT * FROM v, v"
      val expected = spark.sql(query).collect()
      assert(countRepartitions(query) == 0)
      withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
        checkAnswer(spark.sql(query), expected)
        // Only v converts. t and s each keep one reference inside v's definition, so
        // InlineCTE flattens them rather than materializing a shuffle nothing reuses.
        assert(countRepartitions(query) == 2)
      }
    }
  }

  test("nested view is converted together with the view it references") {
    withTempView("v1", "v2") {
      spark.range(0, 100).select($"id", ($"id" % 10).as("k")).createOrReplaceTempView("v1")
      sql("CREATE OR REPLACE TEMP VIEW v2 AS SELECT id, k FROM v1 WHERE k < 5")
      val query = "SELECT t1.id FROM v2 t1 JOIN v2 t2 ON t1.id = t2.id"
      val expected = spark.sql(query).collect()
      assert(countRepartitions(query) == 0)
      withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
        checkAnswer(spark.sql(query), expected)
        // Only v2 converts. v1 keeps one reference inside v2's definition, so InlineCTE
        // flattens it rather than materializing a shuffle nothing reuses.
        assert(countRepartitions(query) == 2)
      }
    }
  }

  test("a reference inside an unreferenced CTE does not trigger materialization") {
    // `c` is never referenced, so it does not count: v keeps one live reference and its
    // definition must stay inlinable.
    withTempView("v") {
      spark.range(0, 10).select($"id", ($"id" % 10).as("k")).createOrReplaceTempView("v")
      val query = "WITH c AS (SELECT id FROM v) SELECT id FROM v WHERE id < 5"
      val expected = spark.sql(query).collect()
      withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
        assert(countRepartitions(query) == 0)
        checkAnswer(spark.sql(query), expected)
      }
    }
  }

  test("converts a temp view whose body has a WITH clause over a persistent table") {
    // Re-analysis re-substitutes the body's inner CTEs with fresh definition ids. The
    // rule normalizes those ids before grouping, so the occurrences still convert.
    withTable("t") {
      sql("CREATE TABLE t USING parquet AS SELECT id, id % 10 AS k FROM range(0, 20)")
      withTempView("v") {
        sql("CREATE OR REPLACE TEMP VIEW v AS " +
          "WITH c AS (SELECT id, k FROM t WHERE k < 4) SELECT id, k FROM c")
        val query = "SELECT t1.id FROM v t1 JOIN v t2 ON t1.id = t2.id"
        val expected = spark.sql(query).collect()
        assert(countRepartitions(query) == 0)
        withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
          checkAnswer(spark.sql(query), expected)
          assert(countRepartitions(query) == 2)
        }
      }
    }
  }

  test("converts a persistent view whose body has a WITH clause") {
    withTable("t") {
      sql("CREATE TABLE t USING parquet AS SELECT id, id % 10 AS k FROM range(0, 20)")
      withView("pers_v") {
        sql("CREATE VIEW pers_v AS " +
          "WITH c AS (SELECT id, k FROM t WHERE k < 4) SELECT id, k FROM c")
        val query = "SELECT t1.id FROM pers_v t1 JOIN pers_v t2 ON t1.id = t2.id"
        val expected = spark.sql(query).collect()
        assert(countRepartitions(query) == 0)
        withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
          checkAnswer(spark.sql(query), expected)
          assert(countRepartitions(query) == 2)
        }
      }
    }
  }
}
