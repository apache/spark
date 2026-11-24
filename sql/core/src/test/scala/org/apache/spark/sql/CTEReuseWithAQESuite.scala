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

import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import org.apache.spark.{SparkContext, SparkThrowable}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap}
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, CTERelationRef, CTEReuseRelation, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.{ApplyColumnarRulesAndInsertTransitions, ColumnarToRowExec, EmptyRelationExec, FileSourceScanExec, LeafExecNode, SQLExecution}
import org.apache.spark.sql.execution.adaptive.{AdaptiveExecutionContext, AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, CTEReuseQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.execution.joins.BaseJoinExec
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.ThreadUtils

/**
 * End-to-end tests for CTE reuse through AQE: verifies that CTEReuseQueryStageExec instances
 * reusing one CTE share the same inner AdaptiveSparkPlanExec, and that AQE re-optimization
 * interacts properly with CTE reuse stages.
 */
class CTEReuseWithAQESuite
    extends QueryTest with SharedSparkSession
    with AdaptiveSparkPlanHelper {

  private val cteReuseConf = SQLConf.REPLACE_CTE_REF_WITH_CTE_REUSE.key

  private def withCTEReuseEnabled(f: => Unit): Unit = {
    withSQLConf(
      cteReuseConf -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true"
    )(f)
  }

  /**
   * Verify the single-materialization invariant: all [[CTEReuseQueryStageExec]] stages reusing one
   * CTE point to the same inner AdaptiveSparkPlanExec by identity, so a CTE referenced N times
   * creates N stage wrappers but is materialized exactly once. `expectedDistinctCTEs` is the number
   * of distinct shared inner plans, one per plan-reuse id (default 1); forced CTEs with identical
   * deterministic bodies share one.
   */
  private def assertInnerAQEShared(
      df: DataFrame,
      expectedDistinctCTEs: Int = 1,
      allowEmpty: Boolean = false): Unit = {
    val executedPlan = df.queryExecution.executedPlan
    val stages = collectWithSubqueries(executedPlan) {
      case s: CTEReuseQueryStageExec => s
    }
    if (stages.isEmpty) {
      // Only callers that legitimately prune all CTE stages (e.g. empty-relation propagation)
      // may pass here; otherwise a missing-stages regression must fail, not pass vacuously.
      assert(allowEmpty,
        s"Expected CTEReuseQueryStageExec stages but found none:\n${executedPlan.treeString}")
      return
    }
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

  /**
   * Runs `text` with every CTE forced to materialize (`forceSkipInline = true`) so it opts into
   * guaranteed CTE shuffle reuse. `forceSkipInline` has no SQL syntax, so tag the analyzed plan's
   * CTE definitions and re-run the tagged plan.
   */
  private def sqlWithForcedCTEReuse(text: String): DataFrame = {
    val analyzed = spark.sql(text).queryExecution.analyzed
    val tagged = analyzed.transformWithSubqueries {
      case d: CTERelationDef => d.copy(forceSkipInline = true)
    }
    classic.Dataset.ofRows(spark, tagged)
  }

  /** Like [[sqlWithForcedCTEReuse]], but forces only the CTEs named in `names`. */
  private def sqlWithForcedCTEs(text: String, names: Set[String]): DataFrame = {
    val analyzed = spark.sql(text).queryExecution.analyzed
    val tagged = analyzed.transformWithSubqueries {
      case d: CTERelationDef if (d.child match {
        case a: SubqueryAlias => names.contains(a.alias)
        case _ => false
      }) => d.copy(forceSkipInline = true)
    }
    classic.Dataset.ofRows(spark, tagged)
  }

  /**
   * Builds `text`, which must reference its one CTE exactly twice, with every CTE forced to
   * materialize and both references sharing the first reference's attributes, and skips
   * re-analysis, as `CTEReuseWithoutAQESuite` does for a dynamic-pruning subquery in a CTE body.
   */
  private def forcedCTEWithSharedReferences(text: String): DataFrame = {
    val analyzed = spark.sql(text).queryExecution.analyzed
    val refs = analyzed.collect { case r: CTERelationRef => r }
    assert(refs.size == 2, analyzed.treeString)
    val mapping = AttributeMap(refs(1).output.zip(refs(0).output))
    val shared: LogicalPlan = analyzed.transformUp {
      case r: CTERelationRef if r.output == refs(1).output => r.copy(output = refs(0).output)
    }.transformAllExpressions {
      case a: Attribute if mapping.contains(a) => mapping(a)
    }.transformWithSubqueries {
      case d: CTERelationDef => d.copy(forceSkipInline = true)
    }
    shared.foreach(_.setAnalyzed())
    classic.Dataset.ofRows(spark, shared)
  }

  /** The rows `text` returns with guaranteed CTE reuse off, computed independently of it. */
  private def rowsWithoutCTEReuse(text: String): Seq[Row] =
    withSQLConf(cteReuseConf -> "false") {
      sqlWithForcedCTEReuse(text).collect().toSeq
    }

  // -----------------------------------------------------------------
  // Inner-AQE sharing tests
  // -----------------------------------------------------------------

  test("CTE reuse: 2 refs share one inner AQE") {
    withCTEReuseEnabled {
      withTable("metric_src") {
        sql("CREATE TABLE metric_src (id INT) USING parquet")
        sql(
          "INSERT INTO metric_src VALUES (1), (2), (3), (4), (5)")
        val df = sqlWithForcedCTEReuse(
          """WITH cte AS (
            |  SELECT id, rand() as r FROM metric_src
            |)
            |SELECT * FROM cte c1 JOIN cte c2 ON c1.id = c2.id
            |""".stripMargin)
        val rows = df.collect().map(r => (r.getInt(0), r.getDouble(1), r.getInt(2), r.getDouble(3)))
        assert(rows.map(_._1).sorted.toSeq == (1 to 5), rows.mkString(", "))
        // One materialization: both references return the same random value for each id.
        assert(rows.forall { case (id1, r1, id2, r2) => id1 == id2 && r1 == r2 },
          rows.mkString(", "))
        // Two refs create two CTEReuseQueryStageExec wrappers, both sharing one inner AQE.
        assert(collectWithSubqueries(df.queryExecution.executedPlan) {
          case s: CTEReuseQueryStageExec => s
        }.size == 2)
        assertInnerAQEShared(df)
      }
    }
  }

  test("CTE reuse when subplan produces empty result") {
    withCTEReuseEnabled {
      withTable("metric_empty_src") {
        sql("CREATE TABLE metric_empty_src (id INT) USING parquet")
        sql("INSERT INTO metric_empty_src VALUES (1), (2), (3)")
        val df = sqlWithForcedCTEReuse(
          """WITH cte AS (
            |  SELECT id, rand() as r
            |  FROM metric_empty_src WHERE id > 100
            |)
            |SELECT * FROM cte c1 JOIN cte c2 ON c1.id = c2.id
            |""".stripMargin)
        checkAnswer(df, Seq.empty)
        // Any surviving CTE stages (empty-relation propagation may prune some) share one inner AQE.
        assertInnerAQEShared(df, allowEmpty = true)
      }
    }
  }

  test("forced CTE keeps its root reuse shuffle when the body satisfies the repartitioning") {
    withCTEReuseEnabled {
      withTable("cte_repart_src") {
        sql("CREATE TABLE cte_repart_src (k INT, v INT) USING parquet")
        sql("INSERT INTO cte_repart_src VALUES (1, 10), (1, 20), (2, 30), (3, 40)")
        // The body's root REPARTITION(k) is already satisfied by the GROUP BY k below it, so on an
        // AQE replan EnsureRequirements would elide it unless its LOCAL_SHUFFLE_FOR_CTE origin
        // survives re-planning -- otherwise the inner AQE has no root shuffle stage and crashes.
        val df = sqlWithForcedCTEReuse(
          """WITH c AS (
            |  SELECT /*+ REPARTITION(k) */ k, count(*) AS cnt FROM cte_repart_src GROUP BY k
            |)
            |SELECT c1.k, c1.cnt FROM c c1 JOIN c c2 ON c1.k = c2.k
            |""".stripMargin)
        checkAnswer(df, Seq(Row(1, 2), Row(2, 1), Row(3, 1)))
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
          val df = sqlWithForcedCTEReuse(
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

          // 1. All CTEReuseQueryStageExec removed from final plan.
          val remaining = finalPlan.collect {
            case s: CTEReuseQueryStageExec => s
          }
          assert(remaining.isEmpty,
            "Expected all CTEReuseQueryStageExec removed " +
              "after empty relation propagation, " +
              s"but found ${remaining.size}." +
              s"\nFinal plan:\n${finalPlan.treeString}")

          // 2. Inner AQE was created (registry entry exists).
          val registry = aqe.context.cteAQERegistry
          assert(registry.nonEmpty,
            "Expected cteAQERegistry to contain inner AQE")
        }
      }
    }
  }

  // A CTE stage whose inner AQE collapsed to an EmptyRelationExec must still execute as the
  // number of partitions it advertises. With one shuffle partition the CTE seals as
  // SinglePartition, which satisfies AllTuples, so each group-less count(*) reads its stage
  // directly and no shuffle sits in between to absorb a 0-partition RDD.
  test("count(*) over an empty forced CTE read directly returns 0 instead of no row") {
    withCTEReuseEnabled {
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
        withTable("cte_empty_count_src") {
          sql("CREATE TABLE cte_empty_count_src (id INT, batch_id INT) USING parquet")
          val df = sqlWithForcedCTEReuse(
            """WITH flagged AS (
              |  SELECT batch_id, count(id) AS c FROM cte_empty_count_src GROUP BY batch_id
              |)
              |SELECT count(*) FROM flagged
              |UNION ALL
              |SELECT count(*) FROM flagged
              |""".stripMargin)
          checkAnswer(df, Seq(Row(0L), Row(0L)))
          val plan = df.queryExecution.executedPlan
          val stages = collectWithSubqueries(plan) { case s: CTEReuseQueryStageExec => s }
          assert(stages.size == 2 &&
              stages.forall(_.innerAQE.executedPlan.isInstanceOf[EmptyRelationExec]),
            s"Expected both count(*) to read a collapsed-empty CTE stage.\n${plan.treeString}")
          val shuffles = collectWithSubqueries(plan) {
            case s: ShuffleQueryStageExec => s
            case e: ShuffleExchangeLike => e
          }
          assert(shuffles.isEmpty,
            s"Expected count(*) to read the CTE stages with no shuffle.\n${plan.treeString}")
          assertInnerAQEShared(df)
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

        val text =
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
            |""".stripMargin
        val df = sqlWithForcedCTEReuse(text)

        val reuses = collectCTEReuseRelations(df)
        assert(reuses.size >= 2,
          s"Expected >= 2 CTEReuseRelation (main + subquery), " +
            s"got ${reuses.size}.\nPlan:\n" +
            df.queryExecution.optimizedPlan.treeString)
        checkAnswer(df, rowsWithoutCTEReuse(text))
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

        val text =
          """WITH agg AS (
            |  SELECT o.pid, p.cat,
            |    sum(o.qty) as total, rand() as r
            |  FROM orders o JOIN products p ON o.pid = p.id
            |  GROUP BY o.pid, p.cat
            |)
            |SELECT pid, total FROM agg a1
            |WHERE cat = 'A'
            |AND total > (SELECT avg(total) FROM agg)
            |""".stripMargin
        val df = sqlWithForcedCTEReuse(text)

        val reuses = collectCTEReuseRelations(df)
        assert(reuses.size >= 2,
          s"Expected >= 2 CTEReuseRelation (main + subquery), " +
            s"got ${reuses.size}.\nPlan:\n" +
            df.queryExecution.optimizedPlan.treeString)
        checkAnswer(df, rowsWithoutCTEReuse(text))
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
          val text =
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
              |""".stripMargin
          val df = sqlWithForcedCTEReuse(text)

          val reuses = collectCTEReuseRelations(df)
          assert(reuses.size >= 4,
            s"Expected >= 4 CTEReuseRelation, " +
              s"got ${reuses.size}.\nPlan:\n" +
              df.queryExecution.optimizedPlan.treeString)

          checkAnswer(df, rowsWithoutCTEReuse(text))
          assertInnerAQEShared(df, expectedDistinctCTEs = 2)
        }
      }
    }
  }

  // -----------------------------------------------------------------
  // Nested CTEs
  // -----------------------------------------------------------------

  test("nested CTEs: outer CTE body references inner CTE") {
    // Both CTEs are defined at one WITH level, and cte_outer's body
    // references cte_inner. Both have rand() to prevent inlining. The outer CTE is
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

        val text =
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
            |""".stripMargin
        val df = sqlWithForcedCTEReuse(text)

        val reuses = collectCTEReuseRelations(df)
        // Only the outer CTE's 2 refs are visible at the top level.
        // The inner CTE's refs live inside the outer CTE's
        // sharedSubplan and materialize inside the outer CTE's
        // inner AQE.
        assert(reuses.size >= 2,
          s"Expected >= 2 CTEReuseRelation (outer CTE refs)" +
            s", got ${reuses.size}.\nPlan:\n" +
            df.queryExecution.optimizedPlan.treeString)

        checkAnswer(df, rowsWithoutCTEReuse(text))
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
  // Global aggregates over a forced CTE
  // -----------------------------------------------------------------

  test("global aggregates over a forced CTE share one inner AQE") {
    withCTEReuseEnabled {
      withTable("all_tuples_src") {
        sql("CREATE TABLE all_tuples_src (id INT, v INT) USING parquet")
        (1 to 8).foreach { i =>
          sql(s"INSERT INTO all_tuples_src VALUES ($i, ${i * 10})")
        }
        // Two global aggregates (no GROUP BY) each reference the same forced CTE. rand() keeps the
        // CTE from inlining; both references must resolve to one shared inner AQE and return
        // consistent results.
        val df = sqlWithForcedCTEReuse(
          """WITH cte AS (
            |  SELECT id, v, rand() as r FROM all_tuples_src
            |)
            |SELECT
            |  (SELECT count(*) FROM cte) as cnt,
            |  (SELECT sum(v) FROM cte) as total
            |""".stripMargin)
        checkAnswer(df, Row(8, 360))
        assertInnerAQEShared(df)
      }
    }
  }

  // -----------------------------------------------------------------
  // Deeply nested CTEs (6 levels)
  // -----------------------------------------------------------------

  test("deeply nested CTEs: 6 levels of non-deterministic CTE nesting") {
    // Each level defines a non-deterministic CTE (rand()) that references
    // the CTE from the level below, with two refs per level to trigger
    // CTE reuse. This exercises nested inner AQEs materializing on the
    // CTE thread pool and checks that six levels of nesting complete with
    // correct results. At most six materializations block at once, so the
    // test does not show that either pool avoids exhaustion.
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

        val df = sqlWithForcedCTEReuse(query)
        // 3 ids self-joined at every level = 3 rows, (1, 1), (2, 2) and (3, 3).
        checkAnswer(df, rowsWithoutCTEReuse(query))
        checkAnswer(df, (1 to 3).map(i => Row(i, i)))

        val aqe = df.queryExecution.executedPlan
          .asInstanceOf[AdaptiveSparkPlanExec]

        assert(aqe.context.cteAQERegistry.nonEmpty,
          "Expected cteAQERegistry to contain inner AQE(s)")
      }
    }
  }

  // -----------------------------------------------------------------
  // AQE applicability, the CTE stage's collect paths and job context
  // -----------------------------------------------------------------

  test("a union of two forced CTE references with no other exchange runs under AQE") {
    withCTEReuseEnabled {
      withTable("union_only_src") {
        sql("CREATE TABLE union_only_src (id INT) USING parquet")
        sql("INSERT INTO union_only_src VALUES (1), (2), (3)")
        // The only shuffles are inside the forced CTE, so AQE applies only because
        // InsertAdaptiveSparkPlan counts the CTE reuse exchanges.
        val df = sqlWithForcedCTEReuse(
          """WITH cte AS (SELECT id, rand() AS r FROM union_only_src)
            |SELECT id FROM cte UNION ALL SELECT id FROM cte
            |""".stripMargin)
        checkAnswer(df, Seq(1, 2, 3, 1, 2, 3).map(Row(_)))
        assert(df.queryExecution.executedPlan.isInstanceOf[AdaptiveSparkPlanExec],
          s"Expected an adaptive plan:\n${df.queryExecution.executedPlan}")
        assertInnerAQEShared(df)
      }
    }
  }

  test("a result stage over CTE reuse stages returns the same rows when collected twice") {
    withCTEReuseEnabled {
      withTable("result_stage_src") {
        sql("CREATE TABLE result_stage_src (id INT) USING parquet")
        sql("INSERT INTO result_stage_src VALUES (1), (2), (3)")
        // The result stage reads both CTE reuse stages directly (no exchange above them).
        val df = sqlWithForcedCTEReuse(
          """WITH cte AS (SELECT id, rand() AS r FROM result_stage_src)
            |SELECT id, r FROM cte UNION ALL SELECT id, r FROM cte
            |""".stripMargin)
        val first = df.collect().toSeq.map(r => (r.getInt(0), r.getDouble(1))).sorted
        val second = df.collect().toSeq.map(r => (r.getInt(0), r.getDouble(1))).sorted
        assert(first == second)
        // One materialization: both references see the same rows.
        assert(first.grouped(2).forall(pair => pair.distinct.size == 1), first)
        val stages = collectWithSubqueries(df.queryExecution.executedPlan) {
          case s: CTEReuseQueryStageExec => s
        }
        assert(stages.nonEmpty)
        stages.foreach { s =>
          val root = s.innerAQE.executedPlan match {
            case r: AQEShuffleReadExec => r.child
            case other => other
          }
          assert(root.isInstanceOf[ShuffleQueryStageExec],
            s"Expected the shared inner plan to end in a shuffle stage:\n${s.innerAQE}")
          // Spark's own shuffles are row-based, so the stage reports row support.
          assert(s.supportsRowBased && !s.supportsColumnar)
        }
      }
    }
  }

  test("jobs that materialize a forced CTE carry the query's execution id and job tags") {
    withCTEReuseEnabled {
      withTable("job_ctx_src") {
        sql("CREATE TABLE job_ctx_src (id INT) USING parquet")
        sql("INSERT INTO job_ctx_src VALUES (1), (2), (3)")
        val df = sqlWithForcedCTEReuse(
          """WITH cte AS (SELECT id, rand() AS r FROM job_ctx_src)
            |SELECT c1.id FROM cte c1 JOIN cte c2 ON c1.id = c2.id
            |""".stripMargin)
        val jobs = mutable.ArrayBuffer.empty[java.util.Properties]
        val listener = new SparkListener {
          override def onJobStart(jobStart: SparkListenerJobStart): Unit = jobs.synchronized {
            jobs += jobStart.properties
          }
        }
        val tag = "cte-reuse-job-context"
        spark.sparkContext.addSparkListener(listener)
        spark.sparkContext.addJobTag(tag)
        try {
          df.collect()
          spark.sparkContext.listenerBus.waitUntilEmpty()
        } finally {
          spark.sparkContext.removeJobTag(tag)
          spark.sparkContext.removeSparkListener(listener)
        }
        val props = jobs.synchronized(jobs.toSeq)
        // The CTE's map-stage job runs from the CTE pool, and the result job from this thread.
        assert(props.size >= 2, s"Expected the CTE materialization and result jobs, got $props")
        val executionIds = props.map(p => Option(p.getProperty(SQLExecution.EXECUTION_ID_KEY)))
        assert(executionIds.forall(_.isDefined) && executionIds.distinct.size == 1,
          s"Expected every job to carry the query's execution id, got $executionIds")
        assert(props.forall(p => Option(p.getProperty(SparkContext.SPARK_JOB_TAGS))
          .exists(_.split(SparkContext.SPARK_JOB_TAGS_SEP).contains(tag))),
          s"Expected every job to carry the job tag $tag")
      }
    }
  }

  // -----------------------------------------------------------------
  // Pins, root shapes and identical bodies
  // -----------------------------------------------------------------

  // The root shuffle stage of a CTE reuse stage's shared inner plan.
  private def innerRootStage(s: CTEReuseQueryStageExec): ShuffleQueryStageExec =
    s.innerAQE.executedPlan match {
      case r: AQEShuffleReadExec => r.child.asInstanceOf[ShuffleQueryStageExec]
      case q: ShuffleQueryStageExec => q
      case other => fail(s"Expected a shuffle stage at the root, got:\n$other")
    }

  test("a pinned forced CTE materializes on the pinned keys and partition count") {
    withCTEReuseEnabled {
      withTable("pinned_src") {
        sql("CREATE TABLE pinned_src (id INT, v INT) USING parquet")
        sql("INSERT INTO pinned_src VALUES (1, 10), (2, 20), (3, 30)")
        val analyzed = spark.sql(
          """WITH cte AS (SELECT id, v, rand() AS r FROM pinned_src)
            |SELECT c1.id, c2.v FROM cte c1 JOIN cte c2 ON c1.id = c2.id
            |""".stripMargin).queryExecution.analyzed
        // Build the pin from the body's own output (below the analyzer's SubqueryAlias), the
        // attributes the optimized definition keeps.
        val df = classic.Dataset.ofRows(spark, analyzed.transformWithSubqueries {
          case d: CTERelationDef =>
            val keys = EliminateSubqueryAliases(d.child).output.filter(_.name == "id")
            d.copy(forceSkipInline = true, forcePartitioning = Some(HashPartitioning(keys, 3)))
        })
        checkAnswer(df, Seq(Row(1, 10), Row(2, 20), Row(3, 30)))
        assertInnerAQEShared(df)
        val stages = collectWithSubqueries(df.queryExecution.executedPlan) {
          case s: CTEReuseQueryStageExec => s
        }
        assert(stages.size == 2)
        stages.foreach { s =>
          innerRootStage(s).shuffle.outputPartitioning match {
            case h: HashPartitioning =>
              assert(h.numPartitions == 3 &&
                h.expressions.map(_.references.head.name) == Seq("id"), h)
            case other => fail(s"Expected the pinned hash partitioning, got $other")
          }
          // Each reference reports the partitioning and statistics in its own attributes.
          s.outputPartitioning match {
            case h: HashPartitioning =>
              assert(h.references.subsetOf(s.outputSet), s"Expected $h in terms of ${s.output}")
            case other => fail(s"Expected a hash partitioning on the stage, got $other")
          }
          s.computeStats().foreach { stats =>
            assert(stats.attributeStats.keys.forall(s.outputSet.contains), stats)
          }
        }
        // The two references are aliased copies with distinct attributes.
        assert(stages.map(_.output.map(_.exprId)).distinct.size == 2,
          stages.map(_.output).mkString("\n"))
      }
    }
  }

  test("a forced CTE rooted at a rebalance materializes a single shuffle") {
    withCTEReuseEnabled {
      withTable("rebalance_src") {
        sql("CREATE TABLE rebalance_src (k INT, v INT) USING parquet")
        sql("INSERT INTO rebalance_src VALUES (1, 10), (2, 20), (3, 30)")
        // REBALANCE(4, k) also sets the partition count, which the CTE shuffle keeps; the session
        // default (5 in tests) differs from it.
        Seq("REBALANCE(k)" -> None, "REBALANCE(4, k)" -> Some(4)).foreach {
          case (hint, numPartitions) =>
            val query =
              s"""WITH cte AS (SELECT /*+ $hint */ k, v, rand() AS r FROM rebalance_src)
                 |SELECT c1.k, c2.v FROM cte c1 JOIN cte c2 ON c1.k = c2.k
                 |""".stripMargin
            val df = sqlWithForcedCTEReuse(query)
            val reference = withSQLConf(cteReuseConf -> "false") {
              sqlWithForcedCTEReuse(query).collect()
            }
            checkAnswer(df, reference.toSeq)
            assertInnerAQEShared(df)
            val stages = collectWithSubqueries(df.queryExecution.executedPlan) {
              case s: CTEReuseQueryStageExec => s
            }
            stages.foreach { s =>
              val inner = s.innerAQE.executedPlan
              val shuffles = collectWithSubqueries(inner) { case q: ShuffleQueryStageExec => q }
              assert(shuffles.size == 1,
                s"$hint: expected one shuffle stage in the inner plan:\n$inner")
              shuffles.head.shuffle.outputPartitioning match {
                case h: HashPartitioning =>
                  assert(h.expressions.map(_.references.head.name) == Seq("k"), s"$hint: $h")
                  numPartitions.foreach { n =>
                    assert(h.numPartitions == n, s"$hint: expected $n partitions, got $h")
                  }
                case other => fail(s"$hint: expected the rebalance's hash partitioning, got $other")
              }
            }
        }
      }
    }
  }

  test("a forced CTE rooted at REPARTITION(n) materializes one n-partition shuffle") {
    withCTEReuseEnabled {
      withTable("repart_n_src") {
        sql("CREATE TABLE repart_n_src (id INT) USING parquet")
        sql("INSERT INTO repart_n_src VALUES (1), (2), (3), (4)")
        val df = sqlWithForcedCTEReuse(
          """WITH cte AS (SELECT /*+ REPARTITION(3) */ id, rand() AS r FROM repart_n_src)
            |SELECT c1.r = c2.r AS same FROM cte c1 JOIN cte c2 ON c1.id = c2.id
            |""".stripMargin)
        checkAnswer(df, (1 to 4).map(_ => Row(true)))
        assertInnerAQEShared(df)
        val stages = collectWithSubqueries(df.queryExecution.executedPlan) {
          case s: CTEReuseQueryStageExec => s
        }
        stages.foreach { s =>
          val inner = s.innerAQE.executedPlan
          val shuffles = collectWithSubqueries(inner) { case q: ShuffleQueryStageExec => q }
          assert(shuffles.size == 1 &&
            shuffles.head.shuffle.outputPartitioning.numPartitions == 3,
            s"Expected one 3-partition shuffle stage in the inner plan:\n$inner")
        }
      }
    }
  }

  test("forced CTEs with identical deterministic bodies share one inner AQE") {
    withCTEReuseEnabled {
      withTable("same_body_src") {
        sql("CREATE TABLE same_body_src (id INT, v INT) USING parquet")
        sql("INSERT INTO same_body_src VALUES (1, 10), (2, 20)")
        val df = sqlWithForcedCTEReuse(
          """WITH a AS (SELECT id, v * 2 AS w FROM same_body_src),
            |  b AS (SELECT id, v * 2 AS w FROM same_body_src)
            |SELECT * FROM a UNION ALL SELECT * FROM a
            |UNION ALL SELECT * FROM b UNION ALL SELECT * FROM b
            |""".stripMargin)
        checkAnswer(df, Seq.fill(4)(Seq(Row(1, 20), Row(2, 40))).flatten)
        assertInnerAQEShared(df, expectedDistinctCTEs = 1)
      }
    }
  }

  // -----------------------------------------------------------------
  // A forced CTE copied through a retained CTE
  // -----------------------------------------------------------------

  test("a forced CTE read once inside a MATERIALIZED CTE read twice is materialized once") {
    withCTEReuseEnabled {
      // Without exchange reuse, each reference of `m` runs its own copy of `m`'s body, so only
      // sealing `f` keeps the two copies of its random values the same.
      withSQLConf(SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
        withTable("copied_src") {
          sql("CREATE TABLE copied_src (id INT) USING parquet")
          sql("INSERT INTO copied_src VALUES (1), (2), (3), (4), (5)")
          val df = sqlWithForcedCTEs(
            """WITH f AS (SELECT /*+ REPARTITION(3) */ id, rand() AS r FROM copied_src),
              |  m AS MATERIALIZED (SELECT id, r FROM f)
              |SELECT m1.id, m1.r = m2.r AS same FROM m m1 JOIN m m2 ON m1.id = m2.id
              |""".stripMargin, names = Set("f"))
          assert(collectCTEReuseRelations(df).size == 2,
            s"Expected the forced CTE to be sealed:\n${df.queryExecution.optimizedPlan}")
          checkAnswer(df, (1 to 5).map(Row(_, true)))
          assertInnerAQEShared(df)
        }
      }
    }
  }

  // -----------------------------------------------------------------
  // Dynamic partition pruning with a sealed CTE as the broadcast build side
  // -----------------------------------------------------------------

  private def withDPPTables(f: => Unit): Unit = {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true") {
      withTable("cte_dpp_fact", "cte_dpp_dim") {
        sql("CREATE TABLE cte_dpp_fact (v INT, store_id INT) USING parquet " +
          "PARTITIONED BY (store_id)")
        sql("INSERT INTO cte_dpp_fact SELECT CAST(id AS INT), CAST(id % 10 AS INT) FROM range(100)")
        sql("CREATE TABLE cte_dpp_dim (store_id INT, country STRING) USING parquet")
        sql("INSERT INTO cte_dpp_dim VALUES (1, 'NL'), (2, 'NL'), (3, 'DE'), (4, 'US')")
        f
      }
    }
  }

  // The number of partitions the scan of the partitioned fact table read.
  private def factPartitionsRead(df: DataFrame): Long = {
    val scans = collectWithSubqueries(df.queryExecution.executedPlan) {
      case s: FileSourceScanExec if s.tableIdentifier.exists(_.table == "cte_dpp_fact") => s
    }
    assert(scans.size == 1, df.queryExecution.executedPlan.treeString)
    scans.head.metrics("numPartitions").value
  }

  Seq(
    "one SQL reference" -> "",
    "two SQL references" -> "UNION ALL SELECT store_id, store_id FROM d WHERE country = 'NL'"
  ).foreach {
    case (name, extra) =>
      test(s"dynamic partition pruning reuses the broadcast of a sealed CTE ($name)") {
        withCTEReuseEnabled {
          withDPPTables {
            // The selective predicate sits above the reference, where PartitionPruning sees it,
            // and the dynamic-pruning subquery then holds a second copy of the CTE.
            Seq(
              "a plain body" -> "SELECT store_id, country FROM cte_dpp_dim",
              "a body with a subquery" ->
                ("SELECT store_id, country FROM cte_dpp_dim " +
                  "WHERE store_id <= (SELECT max(store_id) FROM cte_dpp_dim)")).foreach {
              case (bodyName, body) =>
                val df = sqlWithForcedCTEReuse(
                  s"""WITH d AS ($body)
                     |SELECT f.v, f.store_id FROM cte_dpp_fact f
                     |JOIN d ON f.store_id = d.store_id AND d.country = 'NL'
                     |$extra
                     |""".stripMargin)
                val expected = (0 until 100).filter(i => i % 10 == 1 || i % 10 == 2)
                  .map(i => Row(i, i % 10)) ++
                  (if (extra.isEmpty) Nil else Seq(Row(1, 1), Row(2, 2)))
                checkAnswer(df, expected)
                assert(collectCTEReuseRelations(df).size >= 2,
                  s"$bodyName: expected the CTE to be sealed:\n${df.queryExecution.optimizedPlan}")
                assert(factPartitionsRead(df) == 2,
                  s"$bodyName: expected the scan to read only the two NL partitions:\n" +
                    df.queryExecution.executedPlan.treeString)
                // The dynamic-pruning build side and the SQL references read one inner plan.
                assertInnerAQEShared(df)
            }
          }
        }
      }
  }

  test("references to identical non-deterministic forced CTEs keep distinct canonical forms") {
    withCTEReuseEnabled {
      withTable("nondet_canon_src") {
        sql("CREATE TABLE nondet_canon_src (id INT) USING parquet")
        sql("INSERT INTO nondet_canon_src VALUES (1), (2)")
        val df = sqlWithForcedCTEReuse(
          """WITH a AS (SELECT id, rand(42) AS r FROM nondet_canon_src),
            |  b AS (SELECT id, rand(42) AS r FROM nondet_canon_src)
            |SELECT * FROM a UNION ALL SELECT * FROM a
            |UNION ALL SELECT * FROM b UNION ALL SELECT * FROM b
            |""".stripMargin)
        df.collect()
        assertInnerAQEShared(df, expectedDistinctCTEs = 2)
        val stages = collectWithSubqueries(df.queryExecution.executedPlan) {
          case s: CTEReuseQueryStageExec => s
        }
        assert(stages.size == 4)
        val byInner = stages.groupBy(s => System.identityHashCode(s.innerAQE)).values.toSeq
        assert(byInner.forall(_.map(_.canonicalized).distinct.size == 1),
          "Expected the references of one CTE to share a canonical form")
        assert(byInner.map(_.head.canonicalized).distinct.size == 2,
          "Expected the two CTEs to keep distinct canonical forms")
      }
    }
  }

  // -----------------------------------------------------------------
  // Runtime statistics of an empty CTE stage
  // -----------------------------------------------------------------

  test("an empty CTE stage reports zero rows so a join over it is propagated as empty") {
    withCTEReuseEnabled {
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        withTable("empty_stats_src", "empty_stats_other") {
          sql("CREATE TABLE empty_stats_src (id INT, batch_id INT) USING parquet")
          sql("CREATE TABLE empty_stats_other (id INT) USING parquet")
          sql("INSERT INTO empty_stats_other VALUES (1), (2), (3)")
          // The CTE is empty only at runtime: its inner plan collapses to an empty relation once
          // the aggregate's input stage turns out empty.
          val df = sqlWithForcedCTEReuse(
            """WITH flagged AS (
              |  SELECT batch_id, count(id) AS c FROM empty_stats_src GROUP BY batch_id
              |)
              |SELECT f.batch_id FROM flagged f JOIN empty_stats_other o ON f.batch_id = o.id
              |UNION ALL
              |SELECT CAST(count(*) AS INT) FROM flagged
              |""".stripMargin)
          checkAnswer(df, Seq(Row(0)))
          val aqe = df.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
          val finalPlan = aqe.executedPlan
          assert(collect(finalPlan) { case j: BaseJoinExec => j }.isEmpty,
            s"Expected the join over the empty CTE to be propagated as empty:\n$finalPlan")
          // Empty-relation propagation removes both references' stages from the final plan, so
          // check the statistics a stage over the shared inner plan reports.
          val inners = aqe.context.cteAQERegistry.values.toSeq
          assert(inners.size == 1 && inners.head.executedPlan.isInstanceOf[EmptyRelationExec],
            inners.mkString("\n"))
          val inner = inners.head
          val stats =
            CTEReuseQueryStageExec(0, inner, inner.output, inner.canonicalized).getRuntimeStatistics
          assert(stats.rowCount.contains(BigInt(0)) && stats.sizeInBytes == 0, stats)
        }
      }
    }
  }

  // -----------------------------------------------------------------
  // The CTE stage's collect paths, a failing body, and subqueries in a body
  // -----------------------------------------------------------------

  // Whether the shared inner plan of `s` still ends in its root shuffle stage, rather than in a
  // result stage that a collect through the inner adaptive plan would have put on top of it.
  private def endsInShuffleStage(s: CTEReuseQueryStageExec): Boolean =
    (s.innerAQE.executedPlan match {
      case r: AQEShuffleReadExec => r.child
      case other => other
    }).isInstanceOf[ShuffleQueryStageExec]

  test("a CTE reuse stage collects, takes, tails and iterates over the shared inner plan") {
    withCTEReuseEnabled {
      withTable("collect_paths_src") {
        sql("CREATE TABLE collect_paths_src (id INT) USING parquet")
        sql("INSERT INTO collect_paths_src VALUES (1), (2), (3), (4), (5)")
        val df = sqlWithForcedCTEReuse(
          """WITH cte AS (SELECT id, rand() AS r FROM collect_paths_src)
            |SELECT id, r FROM cte UNION ALL SELECT id, r FROM cte
            |""".stripMargin)
        val expected = df.collect().map(r => (r.getInt(0), r.getDouble(1))).toSet
        assert(expected.size == 5, expected)
        val stages = collectWithSubqueries(df.queryExecution.executedPlan) {
          case s: CTEReuseQueryStageExec => s
        }
        assert(stages.size == 2 && stages.forall(_.isMaterialized))
        def decode(rows: Iterable[InternalRow]): Seq[(Int, Double)] =
          rows.map(r => (r.getInt(0), r.getDouble(1))).toSeq
        stages.foreach { s =>
          assert(decode(s.executeCollect()).toSet == expected)
          val taken = decode(s.executeTake(2))
          assert(taken.size == 2 && taken.toSet.subsetOf(expected), taken)
          val tail = decode(s.executeTail(2))
          assert(tail.size == 2 && tail.toSet.subsetOf(expected), tail)
          assert(decode(s.executeToIterator().map(_.copy()).toSeq).toSet == expected)
          stages.foreach { other =>
            assert(endsInShuffleStage(other),
              s"Expected the shared inner plan to keep its root shuffle stage:\n${other.innerAQE}")
          }
        }
        // The references still read the same materialization.
        assert(df.collect().map(r => (r.getInt(0), r.getDouble(1))).toSet == expected)
      }
    }
  }

  test("a forced CTE body that fails at runtime fails the query with its own error") {
    withCTEReuseEnabled {
      withTable("failing_body_src") {
        sql("CREATE TABLE failing_body_src (id INT) USING parquet")
        sql("INSERT INTO failing_body_src VALUES (1), (2), (3)")
        val df = sqlWithForcedCTEReuse(
          """WITH cte AS (
            |  SELECT id FROM failing_body_src WHERE if(id > 2, raise_error('cte boom'), true)
            |)
            |SELECT c1.id FROM cte c1 JOIN cte c2 ON c1.id = c2.id
            |""".stripMargin)
        val e = intercept[Exception](df.collect())
        val chain = Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).toSeq
        assert(!chain.exists(_.isInstanceOf[java.util.concurrent.CompletionException]),
          chain.mkString("\n"))
        assert(chain.exists {
          case t: SparkThrowable => t.getCondition == "USER_RAISED_EXCEPTION"
          case _ => false
        }, chain.mkString("\n"))
        assert(chain.exists(t => Option(t.getMessage).exists(_.contains("cte boom"))),
          chain.mkString("\n"))
      }
    }
  }

  // The replan that drops every reference to the CTE has to be adopted before the CTE body fails,
  // and the outer plan has to see that failure while another branch is still running. Latches
  // order these events: the empty join sides wait until the test has seen the CTE stages, the CTE
  // body fails only once the adopted plan no longer holds them, and the other branch finishes only
  // once the outer plan has recorded the failure.
  test("a CTE stage that a replan dropped does not fail the query when its body fails later") {
    withCTEReuseEnabled {
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
          "org.apache.spark.sql.catalyst.optimizer.PropagateEmptyRelation") {
        DroppedCTEStageSync.reset()
        spark.udf.register("dropped_cte_wait_for_stages", udf { (id: Long) =>
          DroppedCTEStageSync.await(DroppedCTEStageSync.stagesSeen, "the CTE stages")
          id
        }.asNondeterministic())
        spark.udf.register("dropped_cte_fail_after_drop", udf { (id: Long) =>
          DroppedCTEStageSync.await(DroppedCTEStageSync.stagesDropped, "the replan")
          if (id >= 0) throw new IllegalStateException("dropped CTE body failed")
          true
        }.asNondeterministic())
        spark.udf.register("dropped_cte_wait_for_failure", udf { (k: Long) =>
          DroppedCTEStageSync.await(DroppedCTEStageSync.failureSeen, "the CTE failure")
          k
        }.asNondeterministic())
        val df = sqlWithForcedCTEReuse(
          """WITH c AS (
            |  SELECT id FROM range(0, 4, 1, 1) WHERE dropped_cte_fail_after_drop(id)
            |)
            |SELECT c1.id, 0L AS n FROM c c1 JOIN (
            |  SELECT id FROM range(0, 10, 1, 1) WHERE dropped_cte_wait_for_stages(id) > 100
            |) e1 ON c1.id = e1.id
            |UNION ALL
            |SELECT c2.id, 0L AS n FROM c c2 JOIN (
            |  SELECT id FROM range(0, 10, 1, 1) WHERE dropped_cte_wait_for_stages(id) > 100
            |) e2 ON c2.id = e2.id
            |UNION ALL
            |SELECT k2, count(*) AS n FROM (
            |  SELECT dropped_cte_wait_for_failure(k) AS k2 FROM (
            |    SELECT id % 3 AS k FROM range(0, 4, 1, 1) GROUP BY id % 3
            |  )
            |) GROUP BY k2
            |""".stripMargin)
        val aqe = df.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
        def cteStages: Seq[CTEReuseQueryStageExec] =
          aqe.executedPlan.collect { case s: CTEReuseQueryStageExec => s }
        val result = Future(df.collect())(ExecutionContext.global)
        try {
          DroppedCTEStageSync.waitUntil("the outer plan creates the CTE stages") {
            cteStages.nonEmpty
          }
          val dropped = cteStages
          DroppedCTEStageSync.stagesSeen.countDown()
          DroppedCTEStageSync.waitUntil("a replan drops every CTE stage") {
            cteStages.isEmpty
          }
          DroppedCTEStageSync.stagesDropped.countDown()
          // The outer plan records the error on the stage right before it decides whether the
          // failure fails the query; without the fix, that decision fails it at once.
          DroppedCTEStageSync.waitUntil("the outer plan records the CTE failure") {
            dropped.exists(_.hasFailed) || result.isCompleted
          }
          DroppedCTEStageSync.failureSeen.countDown()
          val rows = ThreadUtils.awaitResult(result, 2.minutes)
          assert(rows.toSeq.map(r => (r.getLong(0), r.getLong(1))).sorted ==
            Seq((0L, 1L), (1L, 1L), (2L, 1L)))
        } finally {
          DroppedCTEStageSync.releaseAll()
        }
      }
    }
  }

  test("forced CTE bodies with subqueries share one inner AQE") {
    withCTEReuseEnabled {
      withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true") {
        withTable("body_sub_src", "body_sub_a", "body_sub_b", "body_sub_fact", "body_sub_dim") {
          sql("CREATE TABLE body_sub_src (id INT, v INT) USING parquet")
          sql("INSERT INTO body_sub_src VALUES (1, 10), (2, 20), (3, 30)")
          sql("CREATE TABLE body_sub_a (a INT) USING parquet")
          sql("INSERT INTO body_sub_a VALUES (2), (3), (7)")
          sql("CREATE TABLE body_sub_b (b INT) USING parquet")
          sql("INSERT INTO body_sub_b VALUES (100), (200)")
          sql("CREATE TABLE body_sub_fact (v INT, store_id INT) USING parquet " +
            "PARTITIONED BY (store_id)")
          sql("INSERT INTO body_sub_fact VALUES (10, 1), (20, 2), (30, 2), (40, 3)")
          sql("CREATE TABLE body_sub_dim (store_id INT, country STRING) USING parquet")
          sql("INSERT INTO body_sub_dim VALUES (1, 'NL'), (2, 'NL'), (3, 'DE')")
          Seq(
            "a scalar subquery" -> (
              "SELECT id, (SELECT max(a) FROM body_sub_a) AS x, rand() AS r FROM body_sub_src",
              Seq(Row(1, 7), Row(2, 7), Row(3, 7))),
            "an IN subquery" -> (
              "SELECT id, v AS x, rand() AS r FROM body_sub_src " +
                "WHERE id IN (SELECT a FROM body_sub_a)",
              Seq(Row(2, 20), Row(3, 30))),
            "a subquery nested in a subquery" -> (
              "SELECT id, (SELECT max(a) + (SELECT max(b) FROM body_sub_b) FROM body_sub_a) " +
                "AS x, rand() AS r FROM body_sub_src",
              Seq(Row(1, 207), Row(2, 207), Row(3, 207)))).foreach {
            case (name, (body, expected)) =>
              val df = sqlWithForcedCTEReuse(
                s"""WITH cte AS ($body)
                   |SELECT c1.id, c1.x FROM cte c1 JOIN cte c2 ON c1.id = c2.id AND c1.x = c2.x
                   |AND c1.r = c2.r
                   |""".stripMargin)
              checkAnswer(df, expected)
              assert(collectCTEReuseRelations(df).size == 2,
                s"$name: expected the CTE to be sealed:\n${df.queryExecution.optimizedPlan}")
              assertInnerAQEShared(df)
          }

          // A dynamic-pruning join inside the body. Both references share attributes, as in the
          // dynamic-pruning test of CTEReuseWithoutAQESuite: copying a body that holds a
          // dynamic-pruning subquery for a reference with fresh attributes leaves the plan
          // unresolved, which is outside this test.
          val dpp = forcedCTEWithSharedReferences(
            """WITH cte AS (
              |  SELECT f.store_id AS id, f.v AS x, rand() AS r FROM body_sub_fact f
              |  JOIN body_sub_dim d ON f.store_id = d.store_id AND d.country = 'NL'
              |)
              |SELECT id, x, r FROM cte UNION ALL SELECT id, x, r FROM cte
              |""".stripMargin)
          val byKey = dpp.collect().toSeq.groupBy(r => (r.getInt(0), r.getInt(1)))
          assert(byKey.keySet == Set((1, 10), (2, 20), (2, 30)), byKey)
          // One materialization: both references return the same random value for each row.
          assert(byKey.values.forall(rows => rows.size == 2 && rows.map(_.getDouble(2)).distinct
            .size == 1), byKey)
          assert(collectCTEReuseRelations(dpp).size == 2,
            s"Expected the CTE to be sealed:\n${dpp.queryExecution.optimizedPlan}")
          assertInnerAQEShared(dpp)
          // The dynamic-pruning filter stays in the shared inner plan, so the fact scan reads only
          // the two NL partitions. Both references reach the same scan through the inner plan.
          val factScans = collectWithSubqueries(dpp.queryExecution.executedPlan) {
            case s: FileSourceScanExec if s.tableIdentifier.exists(_.table == "body_sub_fact") => s
          }.distinct
          assert(factScans.size == 1 && factScans.head.metrics("numPartitions").value == 2,
            s"Expected one fact scan that reads only the two NL partitions:\n" +
              dpp.queryExecution.executedPlan.treeString)
        }
      }
    }
  }

  // -----------------------------------------------------------------
  // Row support of a CTE reuse stage
  // -----------------------------------------------------------------

  test("a CTE reuse stage over a columnar-only inner root reports no row support") {
    val leaf = ColumnarOnlyLeafExec(spark.range(1).queryExecution.analyzed.output)
    val context = AdaptiveExecutionContext(spark, spark.range(1).queryExecution)
    val inner = AdaptiveSparkPlanExec(leaf, context, Nil, isSubquery = true)
    val stage = CTEReuseQueryStageExec(0, inner, leaf.output, leaf.canonicalized)
    assert(stage.supportsColumnar && !stage.supportsRowBased)
    // A row-based consumer therefore gets a columnar-to-row transition above the stage.
    val withTransitions =
      ApplyColumnarRulesAndInsertTransitions(Nil, outputsColumnar = false).apply(stage)
    assert(withTransitions.isInstanceOf[ColumnarToRowExec], withTransitions.treeString)
  }
}

/** Orders the events of the dropped-CTE-stage test across its query and task threads. */
object DroppedCTEStageSync {
  private val timeoutSeconds = 60L
  @volatile var stagesSeen = new CountDownLatch(1)
  @volatile var stagesDropped = new CountDownLatch(1)
  @volatile var failureSeen = new CountDownLatch(1)

  def reset(): Unit = {
    stagesSeen = new CountDownLatch(1)
    stagesDropped = new CountDownLatch(1)
    failureSeen = new CountDownLatch(1)
  }

  def releaseAll(): Unit = {
    stagesSeen.countDown()
    stagesDropped.countDown()
    failureSeen.countDown()
  }

  def await(latch: CountDownLatch, what: String): Unit = {
    if (!latch.await(timeoutSeconds, TimeUnit.SECONDS)) {
      throw new IllegalStateException(s"Timed out waiting for $what")
    }
  }

  def waitUntil(what: String)(condition: => Boolean): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds)
    while (!condition) {
      if (System.nanoTime() > deadline) {
        throw new IllegalStateException(s"Timed out waiting until $what")
      }
      Thread.sleep(10)
    }
  }
}

/** A leaf that only produces columnar batches, like the root of a columnar plugin's plan. */
case class ColumnarOnlyLeafExec(output: Seq[Attribute]) extends LeafExecNode {
  override def supportsColumnar: Boolean = true
  override def supportsRowBased: Boolean = false
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("columnar only")
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
    throw new UnsupportedOperationException("not executed in this test")
}
