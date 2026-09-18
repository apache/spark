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



import org.apache.spark.sql.catalyst.MetricKey
import org.apache.spark.sql.catalyst.plans.logical.CTEReuseRelation
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper}
import org.apache.spark.sql.execution.adaptive.AQEShuffleReadExec
import org.apache.spark.sql.execution.adaptive.CTEReuseQueryStageExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end tests for CTE reuse through AQE: verifies that
 * CTEReuseQueryStageExec instances share the same inner AQE,
 * metrics are recorded correctly, and AQE re-optimization
 * interacts properly with CTE reuse stages.
 */
class CTEReuseWithAQESuite
    extends QueryTest with SharedSparkSession
    with AdaptiveSparkPlanHelper {

  private val cteReuseConf =
    "spark.sql.optimizer.replaceCTERefWithCTEReuse.enabled"

  private def withCTEReuseEnabled(f: => Unit): Unit = {
    withSQLConf(
      cteReuseConf.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true"
    )(f)
  }

  private def getAQETracker(
      df: DataFrame): org.apache.spark.sql.catalyst.QueryPlanningTracker = {
    df.queryExecution.executedPlan match {
      case aqe: AdaptiveSparkPlanExec => aqe.tracker
      case other =>
        fail(s"Expected AdaptiveSparkPlanExec, " +
          s"got ${other.getClass.getSimpleName}")
    }
  }

  /**
   * Verify that all [[CTEReuseQueryStageExec]] with the same cteId
   * share the same inner AQE by identity. This is the core
   * single-materialization invariant: N refs create N stage wrappers
   * but all point to the same [[AdaptiveSparkPlanExec]], so the CTE
   * shuffle is materialized exactly once.
   */
  /**
   * Verify single-materialization: each distinct CTE has exactly one
   * inner AQE shared by all its refs. `expectedDistinctCTEs` is the
   * number of CTE definitions (default 1).
   */
  private def assertInnerAQEShared(
      df: DataFrame,
      expectedDistinctCTEs: Int = 1): Unit = {
    val executedPlan = df.queryExecution.executedPlan
    val stages = collectWithSubqueries(executedPlan) {
      case s: CTEReuseQueryStageExec => s
    }
    if (stages.isEmpty) return
    val distinctAQEs = stages.map(s =>
      System.identityHashCode(s.innerAQE)).toSet
    assert(distinctAQEs.size == expectedDistinctCTEs,
      s"Expected $expectedDistinctCTEs distinct inner AQE(s) " +
        s"for ${stages.size} CTEReuseQueryStageExec, " +
        s"but found ${distinctAQEs.size}.\n" +
        s"Plan:\n${executedPlan.treeString}")
  }

  private def collectCTEReuseRelations(
      df: DataFrame): Seq[CTEReuseRelation] = {
    df.queryExecution.optimizedPlan.collectWithSubqueries {
      case r: CTEReuseRelation => r
    }
  }

  // -----------------------------------------------------------------
  // Metrics tests
  // -----------------------------------------------------------------

  test("CTE reuse metrics: 2 reuse for 2 refs") {
    withCTEReuseEnabled {
      withTable("metric_src") {
        sql("CREATE TABLE metric_src (id INT) USING parquet")
        sql(
          "INSERT INTO metric_src VALUES (1), (2), (3), (4), (5)")
        val df = sql(
          """WITH cte AS (
            |  SELECT id, rand() as r FROM metric_src
            |)
            |SELECT * FROM cte c1 JOIN cte c2 ON c1.id = c2.id
            |""".stripMargin)
        df.collect()
        val tracker = getAQETracker(df)
        val reuseMetric = tracker.getMetric(
          MetricKey.AQE_CTE_REUSE_INNER_AQE_REUSED)
        assert(reuseMetric != null && reuseMetric.count == 2,
          s"Expected 2 CTE inner AQE reuse (one per ref), " +
            s"got: ${Option(reuseMetric).map(_.count)}")
        assertInnerAQEShared(df)
      }
    }
  }

  test("CTE reuse metrics when subplan produces empty result") {
    withCTEReuseEnabled {
      withTable("metric_empty_src") {
        sql("CREATE TABLE metric_empty_src (id INT) USING parquet")
        sql("INSERT INTO metric_empty_src VALUES (1), (2), (3)")
        val df = sql(
          """WITH cte AS (
            |  SELECT id, rand() as r
            |  FROM metric_empty_src WHERE id > 100
            |)
            |SELECT * FROM cte c1 JOIN cte c2 ON c1.id = c2.id
            |""".stripMargin)
        checkAnswer(df, Seq.empty)
        val tracker = getAQETracker(df)
        val reuseMetric = tracker.getMetric(
          MetricKey.AQE_CTE_REUSE_INNER_AQE_REUSED)
        assert(reuseMetric != null && reuseMetric.count == 2,
          s"Expected 2 CTE inner AQE reuse for empty subplan, " +
            s"got: ${Option(reuseMetric).map(_.count)}")
        assertInnerAQEShared(df)
      }
    }
  }

  // -----------------------------------------------------------------
  // AQE empty relation propagation
  // -----------------------------------------------------------------

  test("AQE empty relation propagation eliminates CTE consumers") {
    withCTEReuseEnabled {
      withSQLConf(
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
          ("org.apache.spark.sql.catalyst.optimizer" +
            ".PropagateEmptyRelation")
      ) {
        withTable("cte_nonempty", "join_side") {
          sql(
            "CREATE TABLE cte_nonempty (id INT) USING parquet")
          sql(
            "INSERT INTO cte_nonempty VALUES (1), (2), (3), (4), (5)")
          sql("CREATE TABLE join_side (id INT) USING parquet")
          sql("INSERT INTO join_side VALUES (1), (2), (3)")
          val df = sql(
            """WITH cte AS (
              |  SELECT id, rand() as r FROM cte_nonempty
              |)
              |SELECT c1.id FROM cte c1
              |INNER JOIN (
              |  SELECT id FROM join_side WHERE id > 100
              |) e1 ON c1.id = e1.id
              |UNION ALL
              |SELECT c2.id FROM cte c2
              |INNER JOIN (
              |  SELECT id FROM join_side WHERE id > 100
              |) e2 ON c2.id = e2.id
              |""".stripMargin)
          checkAnswer(df, Seq.empty)
          val aqe = df.queryExecution.executedPlan
            .asInstanceOf[AdaptiveSparkPlanExec]
          val finalPlan = aqe.executedPlan

          // 1. CTEReuseQueryStageExec was resolved.
          val tracker = aqe.tracker
          val reuseMetric = tracker.getMetric(
            MetricKey.AQE_CTE_REUSE_INNER_AQE_REUSED)
          assert(reuseMetric != null && reuseMetric.count >= 1,
            s"Expected CTE inner AQE reuse metric, " +
              s"got: ${Option(reuseMetric).map(_.count)}" +
              s"\nFinal plan:\n${finalPlan.treeString}")

          // 2. All CTEReuseQueryStageExec removed from final plan.
          val remaining = finalPlan.collect {
            case s: CTEReuseQueryStageExec => s
          }
          assert(remaining.isEmpty,
            "Expected all CTEReuseQueryStageExec removed " +
              "after empty relation propagation, " +
              s"but found ${remaining.size}." +
              s"\nFinal plan:\n${finalPlan.treeString}")

          // 3. Inner AQE was created (registry entry exists).
          val registry = aqe.context.cteAQERegistry
          assert(registry.nonEmpty,
            "Expected cteAQERegistry to contain inner AQE")
        }
      }
    }
  }

  // -----------------------------------------------------------------
  // q24-style: CTE in main query + subquery
  // -----------------------------------------------------------------

  test("q24-style: main query + HAVING subquery") {
    withCTEReuseEnabled {
      withTable("sales", "items") {
        sql(
          "CREATE TABLE sales (item_id INT, amount INT) USING parquet")
        sql(
          """INSERT INTO sales VALUES
            |(1, 10), (1, 20), (2, 30), (2, 40),
            |(3, 50), (3, 60)""".stripMargin)
        sql(
          "CREATE TABLE items (id INT, color STRING) USING parquet")
        sql(
          """INSERT INTO items VALUES
            |(1, 'red'), (2, 'blue'), (3, 'red')""".stripMargin)

        val df = sql(
          """WITH ssales AS (
            |  SELECT s.item_id, i.color,
            |    sum(s.amount) as total, rand() as r
            |  FROM sales s JOIN items i ON s.item_id = i.id
            |  GROUP BY s.item_id, i.color
            |)
            |SELECT item_id, sum(total) as paid
            |FROM ssales
            |WHERE color = 'red'
            |GROUP BY item_id
            |HAVING sum(total) > (
            |  SELECT 0.05 * avg(total) FROM ssales
            |)
            |""".stripMargin)

        val reuses = collectCTEReuseRelations(df)
        assert(reuses.size >= 2,
          s"Expected >= 2 CTEReuseRelation (main + subquery), " +
            s"got ${reuses.size}.\nPlan:\n" +
            df.queryExecution.optimizedPlan.treeString)
        df.collect()
        assertInnerAQEShared(df)
      }
    }
  }

  test("q24-style: main query + scalar subquery") {
    withCTEReuseEnabled {
      withTable("orders", "products") {
        sql(
          "CREATE TABLE orders (pid INT, qty INT) USING parquet")
        sql(
          """INSERT INTO orders VALUES
            |(1, 5), (2, 10), (3, 15),
            |(1, 20), (2, 25)""".stripMargin)
        sql(
          "CREATE TABLE products (id INT, cat STRING) USING parquet")
        sql(
          """INSERT INTO products VALUES
            |(1, 'A'), (2, 'B'), (3, 'A')""".stripMargin)

        val df = sql(
          """WITH agg AS (
            |  SELECT o.pid, p.cat,
            |    sum(o.qty) as total, rand() as r
            |  FROM orders o JOIN products p ON o.pid = p.id
            |  GROUP BY o.pid, p.cat
            |)
            |SELECT pid, total FROM agg a1
            |WHERE cat = 'A'
            |AND total > (SELECT avg(total) FROM agg)
            |""".stripMargin)

        val reuses = collectCTEReuseRelations(df)
        assert(reuses.size >= 2,
          s"Expected >= 2 CTEReuseRelation (main + subquery), " +
            s"got ${reuses.size}.\nPlan:\n" +
            df.queryExecution.optimizedPlan.treeString)
        df.collect()
        assertInnerAQEShared(df)
      }
    }
  }

  // -----------------------------------------------------------------
  // AQE multi-iteration replan
  // -----------------------------------------------------------------

  test("AQE multi-iteration replan preserves CTE reuse") {
    // Multiple CTE refs across joins force multiple AQE iterations.
    // Disable broadcast to force shuffle joins. Verifies that
    // re-optimization across iterations does not break sharing.
    //
    // Iter 0 logical plan -- all leaves are CTEReuseRelation:
    //   Project [a, b, a2, b2]
    //   +- Join Inner, (a = b2)
    //      :- Join Inner, (a = a2)
    //      :  :- Join Inner, (a = b)
    //      :  :  :- CTEReuseRelation cteId=1  (cte_a ref 1)
    //      :  :  +- CTEReuseRelation cteId=2  (cte_b ref 1)
    //      :  +- CTEReuseRelation cteId=1     (cte_a ref 2)
    //      +- CTEReuseRelation cteId=2        (cte_b ref 2)
    //
    // Iter 0 physical plan -- all CTEReuseRelation converted to
    // CTEReuseQueryStageExec in createQueryStages:
    //   SortMergeJoin [a], [b2], Inner
    //   :- SortMergeJoin [a], [a2], Inner
    //   :  :- SortMergeJoin [a], [b], Inner
    //   :  :  :- Exchange ENSURE_REQUIREMENTS
    //   :  :  :  +- CTEReuseQueryStage 0 (innerAQE-1)
    //   :  :  +- Exchange ENSURE_REQUIREMENTS
    //   :  :     +- CTEReuseQueryStage 1 (innerAQE-2)
    //   :  +- Exchange ENSURE_REQUIREMENTS
    //   :     +- CTEReuseQueryStage 2 (innerAQE-1)
    //   +- Exchange ENSURE_REQUIREMENTS
    //      +- CTEReuseQueryStage 3 (innerAQE-2)
    //
    // Iter 0 re-optimize input -- all CTEReuseRelation wrapped in
    // LogicalQueryStage with runtime stats visible:
    //   Project [a, b, a2, b2]
    //   +- Join Inner, (a = b2)
    //      :- Join Inner, (a = a2)
    //      :  :- Join Inner, (a = b)
    //      :  :  :- LogicalQueryStage CTEReuseRelation cteId=1,
    //      :  :  :    CTEReuseQueryStage 0
    //      :  :  +- LogicalQueryStage CTEReuseRelation cteId=2,
    //      :  :       CTEReuseQueryStage 1
    //      :  +- LogicalQueryStage CTEReuseRelation cteId=1,
    //      :       CTEReuseQueryStage 2
    //      +- LogicalQueryStage CTEReuseRelation cteId=2,
    //           CTEReuseQueryStage 3
    withCTEReuseEnabled {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_LOG_LEVEL.key -> "INFO",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "5"
      ) {
        withTable("replan_a", "replan_b") {
          sql(
            "CREATE TABLE replan_a (a INT, ka INT) USING parquet")
          sql(
            "CREATE TABLE replan_b (b INT, kb INT) USING parquet")
          (1 to 50).foreach { i =>
            sql(s"INSERT INTO replan_a VALUES ($i, $i)")
          }
          (1 to 30).foreach { i =>
            sql(s"INSERT INTO replan_b VALUES ($i, $i)")
          }

          // CTE A referenced twice (c1 + c3), CTE B twice (c2 + c4).
          // Join c1-c2 on a=b, then join with c3 on a=a, then join
          // with c4 on a=b. Forces 2+ AQE iterations.
          val df = sql(
            """WITH
              |  cte_a AS (
              |    SELECT a, ka, rand() as r FROM replan_a
              |  ),
              |  cte_b AS (
              |    SELECT b, kb, rand() as r FROM replan_b
              |  )
              |SELECT c1.a, c2.b, c3.a as a2, c4.b as b2
              |FROM cte_a c1
              |JOIN cte_b c2 ON c1.a = c2.b
              |JOIN cte_a c3 ON c1.a = c3.a
              |JOIN cte_b c4 ON c1.a = c4.b
              |""".stripMargin)

          val reuses = collectCTEReuseRelations(df)
          assert(reuses.size >= 4,
            s"Expected >= 4 CTEReuseRelation, " +
              s"got ${reuses.size}.\nPlan:\n" +
              df.queryExecution.optimizedPlan.treeString)

          df.collect()
          assertInnerAQEShared(df, expectedDistinctCTEs = 2)
        }
      }
    }
  }

  // -----------------------------------------------------------------
  // Nested CTEs
  // -----------------------------------------------------------------

  test("nested CTEs: outer CTE body references inner CTE") {
    // Inner CTE (cte_inner) is defined inside outer CTE (cte_outer)'s
    // body. Both have rand() to prevent inlining. The outer CTE is
    // referenced twice in the main query; the inner CTE is referenced
    // twice inside the outer CTE's definition. This produces 2
    // distinct CTE materializations in cteAQERegistry.
    withCTEReuseEnabled {
      withTable("nested_src") {
        sql(
          "CREATE TABLE nested_src (id INT, v INT) USING parquet")
        sql(
          """INSERT INTO nested_src VALUES
            |(1, 10), (2, 20), (3, 30),
            |(4, 40), (5, 50)""".stripMargin)

        val df = sql(
          """WITH
            |  cte_inner AS (
            |    SELECT id, v, rand() as ri FROM nested_src
            |  ),
            |  cte_outer AS (
            |    SELECT a.id, a.v + b.v as total, rand() as ro
            |    FROM cte_inner a JOIN cte_inner b ON a.id = b.id
            |  )
            |SELECT c1.id, c2.id as id2
            |FROM cte_outer c1
            |JOIN cte_outer c2 ON c1.id = c2.id
            |""".stripMargin)

        val reuses = collectCTEReuseRelations(df)
        // Only the outer CTE's 2 refs are visible at the top level.
        // The inner CTE's refs live inside the outer CTE's
        // sharedSubplan and materialize inside the outer CTE's
        // inner AQE.
        assert(reuses.size >= 2,
          s"Expected >= 2 CTEReuseRelation (outer CTE refs)" +
            s", got ${reuses.size}.\nPlan:\n" +
            df.queryExecution.optimizedPlan.treeString)

        df.collect()
        // collectWithSubqueries descends into QueryStageExec.plan
        // and AdaptiveSparkPlanExec.executedPlan, so it finds both
        // outer CTE stages (2) and inner CTE stages inside the
        // outer's inner AQE. 2 distinct inner AQEs total.
        assertInnerAQEShared(df, expectedDistinctCTEs = 2)
        // Both outer and inner CTE are in the shared registry.
        val aqe = df.queryExecution.executedPlan
          .asInstanceOf[AdaptiveSparkPlanExec]
        val registry = aqe.context.cteAQERegistry
        assert(registry.size == 2,
          s"Expected 2 entries in cteAQERegistry " +
            s"(outer + inner CTE), got ${registry.size}")
      }
    }
  }

  // -----------------------------------------------------------------
  // Deeply nested CTEs (6 levels)
  // -----------------------------------------------------------------

  // -----------------------------------------------------------------
  // LocalPartition CTE feeding a global aggregate
  // -----------------------------------------------------------------

  test("global aggregate directly over a LocalPartition CTE") {
    // `LocalPartition.numPartitions` is 1 and it inherits the default `satisfies0`,
    // under which `AllTuples` is satisfied when `numPartitions == 1`. So a
    // LocalPartition CTE claims to satisfy the `AllTuples` requirement of a global
    // aggregate, and the aggregate is planned directly on the CTE stage with no
    // shuffle in between.
    //
    // That claim is truthful for the guaranteed-reuse path, because no local read is
    // ever injected over the CTE shuffle: `CTEReuseQueryStageExec` is a
    // `LeafExecNode`, so the outer AQE's `OptimizeShuffleWithLocalRead` does not
    // descend into it, and the inner AQE is driven by `materialize()` with
    // `skipResultStage = true`, so it never reaches `newResultQueryStage` -- the only
    // caller of `optimizeQueryStage(isFinalStage = true)` on the root. The
    // `RepartitionForCTEStage` hook in `OptimizeShuffleWithLocalRead` therefore never
    // fires and the CTE shuffle keeps its single partition.
    //
    // TODO(SC-TBD): AQEShuffleRead local is not applied to the guaranteed shuffle
    // reuse infra. A LocalPartition shuffle without a local read is equivalent to a
    // single-partition shuffle, which is bad for performance. Once a local read IS
    // injected over the CTE shuffle, the read produces one partition per mapper while
    // `CTEReuseQueryStageExec.outputPartitioning` still reports
    // `innerAQE.inputPlan.outputPartitioning` (the pre-optimization plan, i.e.
    // LocalPartition) -- the `AllTuples` claim then becomes false and this global
    // aggregate would emit one row per partition. Whoever enables the local read must
    // stop `LocalPartition` from claiming `AllTuples` (and re-check this test).
    withCTEReuseEnabled {
      withTable("all_tuples_src") {
        sql("CREATE TABLE all_tuples_src (id INT, v INT) USING parquet")
        // Separate inserts -> several files -> several mappers, so that a local read
        // would produce more than one partition if one were injected.
        (1 to 8).foreach { i =>
          sql(s"INSERT INTO all_tuples_src VALUES ($i, ${i * 10})")
        }
        // rand() keeps the CTE from being inlined; two refs trigger reuse. Each global
        // aggregate (no GROUP BY -> AllTuples) sits DIRECTLY on a CTE ref: a join in
        // between would mask the behavior, since the join does not accept
        // LocalPartition either and would insert its own ENSURE_REQUIREMENTS shuffles.
        val df = sql(
          """WITH cte AS (
            |  SELECT id, v, rand() as r FROM all_tuples_src
            |)
            |SELECT
            |  (SELECT count(*) FROM cte) as cnt,
            |  (SELECT sum(v) FROM cte) as total
            |""".stripMargin)
        checkAnswer(df, Row(8, 360))

        // The single-partition claim must be truthful: no local read over the CTE
        // shuffle, and the shuffle really has one partition.
        val plan = df.queryExecution.executedPlan
        val localReads = collectWithSubqueries(plan) {
          case r: AQEShuffleReadExec if r.isLocalRead => r
        }
        assert(localReads.isEmpty,
          "A local read over the CTE shuffle invalidates the LocalPartition " +
            "AllTuples claim; see the TODO above.\n" +
            s"Plan:\n${plan.treeString}")
      }
    }
  }

  test("deeply nested CTEs: 6 levels of non-deterministic CTE nesting") {
    // Each level defines a non-deterministic CTE (rand()) that references
    // the CTE from the level below, with two refs per level to trigger
    // CTE reuse. This exercises the CTE thread pool with concurrent
    // inner AQEs and validates that deeply nested CTE materialization
    // does not deadlock or produce incorrect results.
    //
    // With the default QueryStageCreator pool (16 threads), deeply nested
    // CTEs would deadlock if CTE inner AQEs shared that pool. The
    // separate CTE thread pool (1024 threads) prevents this.
    withCTEReuseEnabled {
      withTable("deep_src") {
        sql("CREATE TABLE deep_src (id INT) USING parquet")
        sql("INSERT INTO deep_src VALUES (1), (2), (3)")

        val numLevels = 6
        val cteDefinitions = (1 to numLevels).map { i =>
          if (i == 1) {
            s"cte_$i AS (SELECT id, rand() as r$i FROM deep_src)"
          } else {
            val prev = i - 1
            s"cte_$i AS (" +
              s"SELECT a.id, rand() as r$i " +
              s"FROM cte_$prev a JOIN cte_$prev b ON a.id = b.id)"
          }
        }.mkString(",\n")

        val query =
          s"""WITH
             |$cteDefinitions
             |SELECT c1.id, c2.id as id2
             |FROM cte_$numLevels c1
             |JOIN cte_$numLevels c2 ON c1.id = c2.id
             |""".stripMargin

        val df = sql(query)
        df.collect()

        val aqe = df.queryExecution.executedPlan
          .asInstanceOf[AdaptiveSparkPlanExec]

        assert(aqe.context.cteAQERegistry.nonEmpty,
          "Expected cteAQERegistry to contain inner AQE(s)")

        // Verify correct result: 3 ids self-joined = 3 rows
        val result = df.collect()
        assert(result.length == 3,
          s"Expected 3 rows from $numLevels-level nested CTE join, " +
            s"got ${result.length}")
      }
    }
  }
}
