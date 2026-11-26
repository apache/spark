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

import org.apache.logging.log4j.Level

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, DynamicPruningExpression, DynamicPruningSubquery}
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, CTERelationRef, CTEReuseRelation, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{BaseSubqueryExec, FileSourceScanExec, QueryExecution, ReusedSubqueryExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.exchange.{CTEReuseExchange, Exchange, LOCAL_SHUFFLE_FOR_CTE, REBALANCE_PARTITIONS_BY_NONE, ReusedExchangeExec, ShuffleExchangeExec, VerifyCTEReuse}
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for CTE reuse when AQE is off, using [[exchange.UnwrapCTEReuseExchange]] and
 * [[exchange.VerifyCTEReuse]]. Each CTEReuseExchange is unwrapped into its shuffle, whose
 * LOCAL_SHUFFLE_FOR_CTE origin carries the cteId; the stock ReuseExchangeAndSubquery then reuses
 * the canonically-equal copies into ReusedExchangeExec, and VerifyCTEReuse confirms reuse held.
 */
class CTEReuseWithoutAQESuite
    extends QueryTest with SharedSparkSession {

  private val cteReuseConf = SQLConf.REPLACE_CTE_REF_WITH_CTE_REUSE.key

  private def withCTEReuseNoAQE(f: => Unit): Unit = {
    withSQLConf(
      cteReuseConf -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      // Fail fast if guaranteed reuse does not hold, so these tests actually exercise the
      // fail-on-reuse-failure path (the individual reuse-disabled test overrides this to false).
      SQLConf.FAIL_ON_CTE_REUSE_WITHOUT_AQE.key -> "true"
    )(f)
  }

  /**
   * Asserts guaranteed CTE shuffle reuse held (AQE off): the executed plan has at least one
   * [[ReusedExchangeExec]], no [[CTEReuseExchange]] left, and live `LOCAL_SHUFFLE_FOR_CTE`
   * shuffles for exactly `expectedDistinctCTEs` distinct plan-reuse ids, one per sealed forced
   * CTE, so the check fails when any CTE the test expects to share is not sealed and only
   * canonical exchange reuse deduplicates its copies. (`withCTEReuseNoAQE` turns
   * FAIL_ON_CTE_REUSE_WITHOUT_AQE on, so a reuse failure would already have thrown during
   * preparation; the flag itself defaults to false.)
   */
  private def assertCTEReuseApplied(df: DataFrame, expectedDistinctCTEs: Int = 1): Unit = {
    val executedPlan = df.queryExecution.executedPlan
    assert(executedPlan.collectWithSubqueries { case c: CTEReuseExchange => c }.isEmpty,
      s"Expected no CTEReuseExchange in executedPlan:\n${executedPlan.treeString}")
    assert(executedPlan.collectWithSubqueries { case r: ReusedExchangeExec => r }.nonEmpty,
      s"Expected >= 1 ReusedExchangeExec for CTE reuse:\n${executedPlan.treeString}")
    val cteIds = executedPlan.collectWithSubqueries {
      case s: ShuffleExchangeExec if s.shuffleOrigin.isInstanceOf[LOCAL_SHUFFLE_FOR_CTE] =>
        s.shuffleOrigin.asInstanceOf[LOCAL_SHUFFLE_FOR_CTE].cteId
    }.distinct
    assert(cteIds.size == expectedDistinctCTEs,
      s"Expected live CTE reuse shuffles for $expectedDistinctCTEs sealed CTE(s), got " +
        s"${cteIds.size} (cteIds ${cteIds.mkString(", ")}):\n${executedPlan.treeString}")
  }

  /**
   * Guards against dangling references: every [[ReusedExchangeExec]] must point (by identity) at
   * an [[Exchange]] instance that actually lives in the executed plan (main tree or a subquery).
   * If reuse were built before other preparation rules copied/rewrote the primary, the reference
   * would point at a stale instance no longer in the tree -- this catches that.
   */
  private def assertNoDanglingReusedExchange(df: DataFrame): Unit = {
    val executedPlan = df.queryExecution.executedPlan
    // Identity set of every live Exchange in the plan and all its subqueries.
    val liveExchanges = java.util.Collections.newSetFromMap(
      new java.util.IdentityHashMap[Exchange, java.lang.Boolean]())
    executedPlan.foreachWithSubqueries {
      case e: Exchange => liveExchanges.add(e)
      case _ =>
    }
    val reused = executedPlan.collectWithSubqueries { case r: ReusedExchangeExec => r }
    reused.foreach { r =>
      assert(liveExchanges.contains(r.child),
        s"Dangling ReusedExchangeExec: its child ${r.child.nodeName}@" +
          s"${System.identityHashCode(r.child)} is not a live Exchange in the plan.\n" +
          s"${executedPlan.treeString}")
    }
  }

  private def numReusedSubqueries(df: DataFrame): Int =
    df.queryExecution.executedPlan.collectWithSubqueries {
      case r: ReusedSubqueryExec => r
    }.size

  private def numSubqueries(df: DataFrame): Int =
    df.queryExecution.executedPlan.collectWithSubqueries {
      case s: BaseSubqueryExec => s
    }.size

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
   * Like [[sqlWithForcedCTEReuse]], and also pins every CTE to a hash partitioning on its `id`
   * column into `numPartitions` partitions. The pin is built from the body's own output (below the
   * analyzer's `SubqueryAlias`), the attributes the optimized definition keeps.
   */
  private def sqlWithPinnedCTEReuse(text: String, numPartitions: Int): DataFrame = {
    val analyzed = spark.sql(text).queryExecution.analyzed
    val tagged = analyzed.transformWithSubqueries {
      case d: CTERelationDef =>
        val keys = EliminateSubqueryAliases(d.child).output.filter(_.name == "id")
        d.copy(forceSkipInline = true,
          forcePartitioning = Some(HashPartitioning(keys, numPartitions)))
    }
    classic.Dataset.ofRows(spark, tagged)
  }

  test("basic: 2-ref CTE join") {
    withCTEReuseNoAQE {
      withTable("cte_noaqe_src") {
        sql("CREATE TABLE cte_noaqe_src (id INT, v INT) USING parquet")
        sql("INSERT INTO cte_noaqe_src VALUES (1, 10), (2, 20), (3, 30)")

        val df = sqlWithForcedCTEReuse(
          """WITH cte AS (
            |  SELECT id, v, rand() as r FROM cte_noaqe_src
            |)
            |SELECT c1.id, c2.v
            |FROM cte c1 JOIN cte c2 ON c1.id = c2.id
            |""".stripMargin)

        // sparkPlan should have CTEReuseExchange (before preparation)
        val sparkPlan = df.queryExecution.sparkPlan
        val cteExchanges = sparkPlan.collect {
          case c: CTEReuseExchange => c
        }
        assert(cteExchanges.size == 2,
          s"Expected 2 CTEReuseExchange in sparkPlan, " +
            s"got ${cteExchanges.size}")

        // executedPlan should have no CTEReuseExchange, and reuse should be applied.
        assertCTEReuseApplied(df)

        // Correct results
        checkAnswer(df.select("id"), Seq(Row(1), Row(2), Row(3)))
      }
    }
  }

  test("nested CTEs") {
    withCTEReuseNoAQE {
      withTable("nested_noaqe") {
        sql("CREATE TABLE nested_noaqe (id INT, v INT) USING parquet")
        sql("INSERT INTO nested_noaqe VALUES (1, 10), (2, 20), (3, 30)")

        val df = sqlWithForcedCTEReuse(
          """WITH
            |  cte_inner AS (
            |    SELECT id, v, rand() as ri FROM nested_noaqe
            |  ),
            |  cte_outer AS (
            |    SELECT a.id, a.v + b.v as total, rand() as ro
            |    FROM cte_inner a JOIN cte_inner b ON a.id = b.id
            |  )
            |SELECT c1.id, c2.total
            |FROM cte_outer c1 JOIN cte_outer c2 ON c1.id = c2.id
            |""".stripMargin)

        // Both the inner and outer CTE shuffles must be reused (nested CTEs), and no
        // CTEReuseExchange may survive.
        assertCTEReuseApplied(df, expectedDistinctCTEs = 2)

        val result = df.collect()
        assert(result.length == 3)
      }
    }
  }

  test("q24-style: CTE in main query + HAVING subquery") {
    withCTEReuseNoAQE {
      withTable("sales_noaqe", "items_noaqe") {
        sql("CREATE TABLE sales_noaqe (item_id INT, amount INT) USING parquet")
        sql(
          """INSERT INTO sales_noaqe VALUES
            |(1, 10), (1, 20), (2, 30), (2, 40),
            |(3, 50), (3, 60)""".stripMargin)
        sql("CREATE TABLE items_noaqe (id INT, color STRING) USING parquet")
        sql(
          """INSERT INTO items_noaqe VALUES
            |(1, 'red'), (2, 'blue'), (3, 'red')""".stripMargin)

        val df = sqlWithForcedCTEReuse(
          """WITH ssales AS (
            |  SELECT s.item_id, i.color,
            |    sum(s.amount) as total, rand() as r
            |  FROM sales_noaqe s JOIN items_noaqe i ON s.item_id = i.id
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

        // CTE is shared across main query and subquery via ReusedExchangeExec, and no
        // CTEReuseExchange survives.
        assertCTEReuseApplied(df)

        df.collect()
      }
    }
  }

  // scalastyle:off line.size.limit
  test("nested CTEs with scalar subquery referencing CTE") {
  // scalastyle:on line.size.limit
    withCTEReuseNoAQE {
      withTable("mix_src") {
        sql("CREATE TABLE mix_src (id INT, v INT) USING parquet")
        sql(
          """INSERT INTO mix_src VALUES
            |(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)""".stripMargin)

        // Structure:
        // - cte_base: non-deterministic CTE (2 refs in cte_agg body)
        //   -> nested CTEReuseExchange (cte_base inside cte_agg)
        // - cte_agg: built on cte_base via self-join + aggregation
        //   (2 refs: one in main query, one in scalar subquery)
        //   -> CTEReuseExchange in main query and scalar subquery scope
        // This tests nested CTEReuseExchange: cte_base inside cte_agg,
        // and cte_agg referenced both in the main query and a scalar
        // subquery.
        val df = sqlWithForcedCTEReuse(
          """WITH
            |  cte_base AS (
            |    SELECT id, v, rand() as r FROM mix_src
            |  ),
            |  cte_agg AS (
            |    SELECT a.id, sum(a.v + b.v) as total, rand() as r2
            |    FROM cte_base a JOIN cte_base b ON a.id = b.id
            |    GROUP BY a.id
            |  )
            |SELECT id, total
            |FROM cte_agg
            |WHERE total > (SELECT avg(total) FROM cte_agg)
            |ORDER BY id
            |""".stripMargin)

        // All CTEReuseExchange should be unwrapped and reuse should be applied.
        assertCTEReuseApplied(df, expectedDistinctCTEs = 2)

        // Verify correctness: total = 2*v for self-join on same id
        // avg(total) = avg(20+40+60+80+100) = 60
        // total > 60 -> ids 4 (80) and 5 (100)
        checkAnswer(df, Seq(Row(4, 80), Row(5, 100)))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Dangling-reference / subquery-interaction tests.
  //
  // The CTE shuffle is deduplicated into ReusedExchangeExec. Subquery preparation in PlanSubqueries
  // does not run ReuseExchangeAndSubquery; it runs once on the main plan, reaching into the
  // subquery plans and re-pointing reused nodes across scopes. These tests make sure the
  // CTE-shuffle reuse survives that interaction with no dangling ReusedExchangeExec references.
  // ---------------------------------------------------------------------------

  private def withReuseSrc(f: => Unit): Unit = withTable("reuse_src") {
    sql("CREATE TABLE reuse_src (id INT, v INT) USING parquet")
    sql("INSERT INTO reuse_src VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
    f
  }

  test("CTE referenced in main query and a subquery; subquery reused") {
    withCTEReuseNoAQE {
      withReuseSrc {
        // cte referenced in the main query and inside a scalar subquery. The scalar subquery
        // (SELECT avg(v) FROM cte) appears twice with identical text, so subquery reuse
        // (ReusedSubqueryExec) also fires -- exercising both CTE-shuffle reuse and subquery reuse.
        val df = sqlWithForcedCTEReuse(
          """WITH cte AS (SELECT id, v, rand() as r FROM reuse_src)
            |SELECT id, v FROM cte
            |WHERE v > (SELECT avg(v) FROM cte)
            |  AND v < (SELECT avg(v) FROM cte) + 100
            |""".stripMargin)
        assertCTEReuseApplied(df)
        assertNoDanglingReusedExchange(df)
        assert(numReusedSubqueries(df) >= 1,
          s"Expected the identical scalar subquery to be reused:\n" +
            df.queryExecution.executedPlan.treeString)
        df.collect()
      }
    }
  }

  test("CTE referenced in two distinct subqueries; subqueries not reused") {
    withCTEReuseNoAQE {
      withReuseSrc {
        // Two different scalar subqueries (avg vs max), each referencing the CTE. The subqueries
        // are NOT reused (different plans), but the CTE shuffle under them must still be reused.
        val df = sqlWithForcedCTEReuse(
          """WITH cte AS (SELECT id, v, rand() as r FROM reuse_src)
            |SELECT id FROM reuse_src o
            |WHERE o.v > (SELECT avg(v) FROM cte)
            |  AND o.v < (SELECT max(v) FROM cte)
            |""".stripMargin)
        assertCTEReuseApplied(df)
        assertNoDanglingReusedExchange(df)
        df.collect()
      }
    }
  }

  test("CTE referenced in two identical subqueries; subqueries reused") {
    withCTEReuseNoAQE {
      withReuseSrc {
        // Two identical scalar subqueries referencing the CTE -> subquery reuse fires, and the
        // CTE shuffle is reused across them. This is the trickiest re-point case: a reused
        // subquery whose plan itself contains a reused CTE exchange.
        val df = sqlWithForcedCTEReuse(
          """WITH cte AS (SELECT id, v, rand() as r FROM reuse_src)
            |SELECT id FROM reuse_src o
            |WHERE o.v > (SELECT avg(v) FROM cte)
            |  AND o.id > (SELECT avg(v) FROM cte) - 100
            |""".stripMargin)
        assertCTEReuseApplied(df)
        assertNoDanglingReusedExchange(df)
        assert(numReusedSubqueries(df) >= 1,
          s"Expected identical subqueries to be reused:\n" +
            df.queryExecution.executedPlan.treeString)
        df.collect()
      }
    }
  }

  test("nested CTE referenced across main query and subqueries") {
    withCTEReuseNoAQE {
      withReuseSrc {
        // Nested CTEs (cte_outer references cte_inner) referenced in the main query and in
        // subqueries, combining the nested-CTE and subquery-scope cases.
        val df = sqlWithForcedCTEReuse(
          """WITH
            |  cte_inner AS (SELECT id, v, rand() as ri FROM reuse_src),
            |  cte_outer AS (
            |    SELECT a.id, a.v + b.v as total, rand() as ro
            |    FROM cte_inner a JOIN cte_inner b ON a.id = b.id
            |  )
            |SELECT id, total FROM cte_outer
            |WHERE total > (SELECT avg(total) FROM cte_outer)
            |  AND total < (SELECT avg(total) FROM cte_outer) + 1000
            |""".stripMargin)
        assertCTEReuseApplied(df, expectedDistinctCTEs = 2)
        assertNoDanglingReusedExchange(df)
        df.collect()
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Consumer-partitioning divergence: the protected LOCAL_SHUFFLE_FOR_CTE shuffle stays reusable
  // even when consumers require different partitioning above the boundary.
  // ---------------------------------------------------------------------------

  test("CTE feeding a broadcast join on one side and a shuffle on the other") {
    withCTEReuseNoAQE {
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1048576") {
        withReuseSrc {
          withTable("big_side") {
            sql("CREATE TABLE big_side (id INT, w INT) USING parquet")
            sql("INSERT INTO big_side VALUES (1, 100), (2, 200), (3, 300)")
            // cte is broadcast into one join and shuffle-joined in another; the two consumers
            // impose different requirements above the shared shuffle, which must still be reused.
            val df = sqlWithForcedCTEReuse(
              """WITH cte AS (SELECT id, v, rand() as r FROM reuse_src)
                |SELECT c1.id
                |FROM cte c1 JOIN big_side b ON c1.id = b.id
                |JOIN cte c2 ON c1.id = c2.id
                |""".stripMargin)
            assertCTEReuseApplied(df)
            assertNoDanglingReusedExchange(df)
            df.collect()
          }
        }
      }
    }
  }

  test("CTE with hash-partitioned repartition in body") {
    withCTEReuseNoAQE {
      withReuseSrc {
        // CTE body carries a REPARTITION(id) hint -> HashPartitioning partitioning on the
        // CTEReuseRelation, exercising the partitioning path (not just round-robin).
        val df = sqlWithForcedCTEReuse(
          """WITH cte AS (
            |  SELECT /*+ REPARTITION(4, id) */ id, v, rand() as r FROM reuse_src
            |)
            |SELECT c1.id, c2.v FROM cte c1 JOIN cte c2 ON c1.id = c2.id
            |""".stripMargin)
        assertCTEReuseApplied(df)
        assertNoDanglingReusedExchange(df)
        df.collect()
      }
    }
  }

  // ---------------------------------------------------------------------------
  // N-way reuse and reuse-disabled behavior.
  // ---------------------------------------------------------------------------

  test("CTE referenced three times") {
    withCTEReuseNoAQE {
      withReuseSrc {
        val df = sqlWithForcedCTEReuse(
          """WITH cte AS (SELECT id, v, rand() as r FROM reuse_src)
            |SELECT c1.id FROM cte c1
            |JOIN cte c2 ON c1.id = c2.id
            |JOIN cte c3 ON c1.id = c3.id
            |""".stripMargin)
        assertCTEReuseApplied(df)
        assertNoDanglingReusedExchange(df)
        // 3 refs -> 1 primary + 2 reused shuffles.
        val reused = df.queryExecution.executedPlan.collectWithSubqueries {
          case r: ReusedExchangeExec => r
        }
        assert(reused.size >= 2,
          s"Expected >= 2 ReusedExchangeExec for a 3-ref CTE, got ${reused.size}:\n" +
            df.queryExecution.executedPlan.treeString)
        df.collect()
      }
    }
  }

  test("exchange reuse disabled: no reuse, verification does not throw") {
    withCTEReuseNoAQE {
      withSQLConf(SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
        withReuseSrc {
          // With exchange reuse off, the CTE shuffles are not deduplicated. VerifyCTEReuse sees
          // >= 2 live shuffles sharing a cteId; FAIL_ON_CTE_REUSE_WITHOUT_AQE governs whether
          // that throws. Turn the flag off here so VerifyCTEReuse only logs, and check that no
          // shuffle is reused, that the miss is logged and that the query still runs.
          withSQLConf(SQLConf.FAIL_ON_CTE_REUSE_WITHOUT_AQE.key -> "false") {
            val df = sqlWithForcedCTEReuse(
              """WITH cte AS (SELECT id, v, rand() as r FROM reuse_src)
                |SELECT c1.id, c2.v FROM cte c1 JOIN cte c2 ON c1.id = c2.id
                |""".stripMargin)
            val logAppender = new LogAppender("CTE reuse verification warning")
            withLogAppender(logAppender) {
              df.queryExecution.executedPlan
            }
            val executedPlan = df.queryExecution.executedPlan
            assert(executedPlan.collectWithSubqueries { case r: ReusedExchangeExec => r }.isEmpty,
              s"Expected no ReusedExchangeExec when exchange reuse is disabled:\n" +
                executedPlan.treeString)
            // The warning is the only signal of the miss in this mode: it names the cteId of the
            // two live shuffles and their number.
            val cteIds = liveCTEShuffles(df).map(_.shuffleOrigin).collect {
              case LOCAL_SHUFFLE_FOR_CTE(cteId) => cteId
            }
            assert(cteIds.size == 2 && cteIds.distinct.size == 1,
              s"Expected two live shuffles of one CTE:\n${executedPlan.treeString}")
            val expectedWarning = "Guaranteed CTE shuffle reuse (AQE off) not applied for " +
              s"cteId=${cteIds.head} (2 shuffles)."
            val warnings = logAppender.loggingEvents.filter(_.getLevel == Level.WARN)
              .map(_.getMessage.getFormattedMessage)
            assert(warnings.contains(expectedWarning),
              s"Expected the warning '$expectedWarning', got:\n${warnings.mkString("\n")}")
            // VerifyCTEReuse ran with the fail flag off, so the query runs without throwing.
            df.collect()
          }
        }
      }
    }
  }

  test("exchange reuse disabled: fail-on policy raises the documented internal error") {
    withCTEReuseNoAQE {
      // withCTEReuseNoAQE leaves FAIL_ON_CTE_REUSE_WITHOUT_AQE enabled. With exchange reuse off the
      // two CTE shuffles sharing one cteId stay un-deduplicated, so VerifyCTEReuse must raise the
      // documented internal error instead of silently running with multiple live CTE shuffles.
      withSQLConf(SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
        withReuseSrc {
          val df = sqlWithForcedCTEReuse(
            """WITH cte AS (SELECT id, v, rand() as r FROM reuse_src)
              |SELECT c1.id, c2.v FROM cte c1 JOIN cte c2 ON c1.id = c2.id
              |""".stripMargin)
          val e = intercept[SparkException] {
            df.queryExecution.executedPlan
          }
          assert(e.getMessage.contains("Guaranteed CTE shuffle reuse (AQE off) failed"),
            s"Expected the fail-on-reuse internal error, got:\n${e.getMessage}")
        }
      }
    }
  }

  /**
   * Builds `text`, which must reference its one CTE exactly twice, with every CTE forced to
   * materialize and both references sharing the first reference's attributes, and skips
   * re-analysis. A second reference normally gets fresh attributes, and replacing it with the
   * CTE body then goes through `DeduplicateRelations.deduplicateRight`, which leaves the body's
   * dynamic-pruning subquery keyed on the old attributes and the plan unresolved. Sharing the
   * attributes keeps that path out of a test about the AQE-off rule order.
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

  test("dynamic partition pruning inside a force-materialized CTE body is planned (AQE off)") {
    withCTEReuseNoAQE {
      withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true") {
        withTable("dpp_fact", "dpp_dim") {
          sql("CREATE TABLE dpp_fact (date_id INT, store_id INT) " +
            "USING parquet PARTITIONED BY (store_id)")
          sql("INSERT INTO dpp_fact VALUES (1000, 1), (1010, 2), (1020, 2), (1100, 3), (1200, 4)")
          sql("CREATE TABLE dpp_dim (store_id INT, country STRING) USING parquet")
          sql("INSERT INTO dpp_dim VALUES (1, 'NL'), (2, 'NL'), (3, 'DE'), (4, 'US')")

          // The join -- which produces the dynamic partition-pruning filter over the partitioned
          // fact scan -- lives inside the force-materialized CTE body, which is referenced twice
          // so it is sealed. In AQE-off mode the body must be unwrapped before
          // PlanDynamicPruningFilters; otherwise the DynamicPruningSubquery is left unplanned and
          // collecting results fails at execution.
          val df = forcedCTEWithSharedReferences(
            """WITH cte AS (
              |  SELECT f.date_id, f.store_id
              |  FROM dpp_fact f JOIN dpp_dim d ON f.store_id = d.store_id AND d.country = 'NL'
              |)
              |SELECT date_id, store_id FROM cte
              |UNION ALL
              |SELECT date_id, store_id FROM cte
              |""".stripMargin)
          assert(df.queryExecution.optimizedPlan.collectWithSubqueries {
            case r: CTEReuseRelation => r
          }.size == 2, s"Expected the CTE to be sealed:\n${df.queryExecution.optimizedPlan}")
          assertCTEReuseApplied(df)

          // No raw (unevaluable) DynamicPruningSubquery may survive into the prepared plan.
          val prepared = df.queryExecution.executedPlan
          val unplanned = prepared.collectWithSubqueries {
            case p if p.expressions.exists(_.exists(_.isInstanceOf[DynamicPruningSubquery])) => p
          }
          assert(unplanned.isEmpty,
            s"Unexpected unplanned DynamicPruningSubquery remaining:\n${prepared.treeString}")

          // Collect from this Dataset's own plan: a derived Dataset would be analyzed again.
          val rows = df.collect().map(r => (r.getInt(0), r.getInt(1))).toSeq.sorted
          assert(rows == Seq((1000, 1), (1000, 1), (1010, 2), (1010, 2), (1020, 2), (1020, 2)))

          // The rows are the same without pruning, so check that the fact scan inside the sealed
          // body was pruned: it carries a planned pruning filter and read only the NL partitions.
          val scans = prepared.collectWithSubqueries {
            case s: FileSourceScanExec if s.tableIdentifier.exists(_.table == "dpp_fact") => s
          }
          assert(scans.nonEmpty && scans.forall { s =>
            s.partitionFilters.exists(_.isInstanceOf[DynamicPruningExpression]) &&
              s.metrics("numPartitions").value == 2
          }, s"Expected the fact scan to read only the two NL partitions:\n${prepared.treeString}")
        }
      }
    }
  }

  Seq(
    "one SQL reference" -> "",
    "two SQL references" -> "UNION ALL SELECT store_id, store_id FROM d WHERE country = 'NL'"
  ).foreach { case (name, extra) =>
    test(s"dynamic partition pruning reuses the broadcast of a sealed CTE (AQE off, $name)") {
      withCTEReuseNoAQE {
        withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true") {
          withTable("cte_dpp_fact", "cte_dpp_dim") {
            sql("CREATE TABLE cte_dpp_fact (v INT, store_id INT) USING parquet " +
              "PARTITIONED BY (store_id)")
            sql("INSERT INTO cte_dpp_fact " +
              "SELECT CAST(id AS INT), CAST(id % 10 AS INT) FROM range(100)")
            sql("CREATE TABLE cte_dpp_dim (store_id INT, country STRING) USING parquet")
            sql("INSERT INTO cte_dpp_dim VALUES (1, 'NL'), (2, 'NL'), (3, 'DE'), (4, 'US')")
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
                assert(df.queryExecution.optimizedPlan.collectWithSubqueries {
                  case r: CTEReuseRelation => r
                }.size >= 2, s"$bodyName: expected the CTE to be sealed:\n" +
                  df.queryExecution.optimizedPlan)
                val expected = (0 until 100).filter(i => i % 10 == 1 || i % 10 == 2)
                  .map(i => Row(i, i % 10)) ++
                  (if (extra.isEmpty) Nil else Seq(Row(1, 1), Row(2, 2)))
                checkAnswer(df, expected)
                assertCTEReuseApplied(df)
                val scans = df.queryExecution.executedPlan.collectWithSubqueries {
                  case s: FileSourceScanExec
                      if s.tableIdentifier.exists(_.table == "cte_dpp_fact") => s
                }
                assert(scans.size == 1 && scans.head.metrics("numPartitions").value == 2,
                  s"$bodyName: expected the scan to read only the two NL partitions:\n" +
                    df.queryExecution.executedPlan.treeString)
            }
          }
        }
      }
    }
  }

  test("joins inside a sealed CTE body are bucket-coalesced (AQE off)") {
    withCTEReuseNoAQE {
      withSQLConf(
          SQLConf.COALESCE_BUCKETS_IN_JOIN_ENABLED.key -> "true",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        withTable("cte_bkt8", "cte_bkt4") {
          spark.range(32).selectExpr("CAST(id AS INT) AS k", "CAST(id AS INT) AS v")
            .write.bucketBy(8, "k").saveAsTable("cte_bkt8")
          spark.range(32).selectExpr("CAST(id AS INT) AS k", "CAST(id * 2 AS INT) AS w")
            .write.bucketBy(4, "k").saveAsTable("cte_bkt4")
          val text =
            """WITH cte AS (
              |  SELECT a.k, a.v, b.w FROM cte_bkt8 a JOIN cte_bkt4 b ON a.k = b.k
              |)
              |SELECT * FROM cte UNION ALL SELECT * FROM cte
              |""".stripMargin
          val df = sqlWithForcedCTEReuse(text)
          checkAnswer(df, (0 until 32).flatMap(i => Seq.fill(2)(Row(i, i, i * 2))))
          assertCTEReuseApplied(df)
          assertNoDanglingReusedExchange(df)
          // The body's sort-merge join reads both bucketed scans without a shuffle, and the only
          // live shuffle is the one CTE shuffle that both references reuse.
          val shuffles = df.queryExecution.executedPlan.collectWithSubqueries {
            case s: ShuffleExchangeExec => s
          }
          assert(shuffles.size == 1 &&
            shuffles.head.shuffleOrigin.isInstanceOf[LOCAL_SHUFFLE_FOR_CTE],
            s"Expected only the reused CTE shuffle:\n${df.queryExecution.executedPlan}")
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Broader scenarios: ref-count, distinct-CTE, and non-trivial materialized-body shapes. Each
  // asserts reuse held via the canonical-equality path (assertCTEReuseApplied +
  // assertNoDanglingReusedExchange).
  // ---------------------------------------------------------------------------

  test("5 refs unioned share one materialization") {
    withCTEReuseNoAQE {
      withReuseSrc {
        val refs = (1 to 5).map(_ => "SELECT id FROM cte").mkString(" UNION ALL ")
        val df = sqlWithForcedCTEReuse(
          s"""WITH cte AS (SELECT id, v, rand() as r FROM reuse_src)
             |$refs
             |""".stripMargin)
        assertCTEReuseApplied(df)
        assertNoDanglingReusedExchange(df)
        // 5 refs -> 1 primary + 4 reused shuffles.
        val reused = df.queryExecution.executedPlan.collectWithSubqueries {
          case r: ReusedExchangeExec => r
        }
        assert(reused.size >= 4,
          s"Expected >= 4 ReusedExchangeExec for a 5-ref CTE, got ${reused.size}:\n" +
            df.queryExecution.executedPlan.treeString)
        df.collect()
      }
    }
  }

  test("two distinct CTEs each reused separately") {
    withCTEReuseNoAQE {
      withReuseSrc {
        // Distinct body projections so the two CTEs cannot canonically collapse into one.
        val df = sqlWithForcedCTEReuse(
          """WITH
            |  cte_a AS (SELECT id, v + 1 as a, rand() as ra FROM reuse_src),
            |  cte_b AS (SELECT id, v * 10 as b, rand() as rb FROM reuse_src)
            |SELECT a1.a FROM cte_a a1 JOIN cte_a a2 ON a1.id = a2.id
            |UNION ALL
            |SELECT b1.b FROM cte_b b1 JOIN cte_b b2 ON b1.id = b2.id
            |""".stripMargin)
        assertCTEReuseApplied(df, expectedDistinctCTEs = 2)
        assertNoDanglingReusedExchange(df)
        // Two distinct materializations, each reused once -> >= 2 ReusedExchangeExec.
        val reused = df.queryExecution.executedPlan.collectWithSubqueries {
          case r: ReusedExchangeExec => r
        }
        assert(reused.size >= 2,
          s"Expected >= 2 ReusedExchangeExec for two distinct 2-ref CTEs, got ${reused.size}:\n" +
            df.queryExecution.executedPlan.treeString)
        df.collect()
      }
    }
  }

  test("CTE body with internal SMJ materialized and reused") {
    withCTEReuseNoAQE {
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        withReuseSrc {
          withTable("smj_right_noaqe") {
            sql("CREATE TABLE smj_right_noaqe (id2 INT, w INT) USING parquet")
            sql("INSERT INTO smj_right_noaqe VALUES (1, 100), (2, 200), (3, 300)")
            // The materialized body is itself a shuffle join (extra ENSURE_REQUIREMENTS shuffles
            // inside the materialization); the outer CTE shuffle must still be reused across refs.
            val df = sqlWithForcedCTEReuse(
              """WITH cte AS (
                |  SELECT s.id, s.v, r.w, rand() as rnd
                |  FROM reuse_src s JOIN smj_right_noaqe r ON s.id = r.id2
                |)
                |SELECT c1.id, c2.w FROM cte c1 JOIN cte c2 ON c1.id = c2.id
                |""".stripMargin)
            assertCTEReuseApplied(df)
            assertNoDanglingReusedExchange(df)
            df.collect()
          }
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Subquery *inside* the materialized CTE body (distinct from a subquery that references the
  // CTE). The body is planned eagerly in SparkStrategies and lifted into the tree by
  // UnwrapCTEReuseExchange, which runs BEFORE PlanSubqueries -- so PlanSubqueries reaches the
  // body's own subquery expressions and plans them physically, as execution requires.
  // ---------------------------------------------------------------------------

  test("scalar subquery inside the CTE body is fully planned and reused") {
    withCTEReuseNoAQE {
      withReuseSrc {
        withTable("dim_noaqe") {
          sql("CREATE TABLE dim_noaqe (m INT) USING parquet")
          sql("INSERT INTO dim_noaqe VALUES (1), (2), (3)")
          // The CTE body contains a scalar subquery `(SELECT max(m) FROM dim_noaqe)`.
          // forceSkipInline, which sqlWithForcedCTEReuse sets, keeps the def materialized so both
          // refs share it.
          val df = sqlWithForcedCTEReuse(
            """WITH cte AS (
              |  SELECT id, v, (SELECT max(m) FROM dim_noaqe) as mx, rand() as r FROM reuse_src
              |)
              |SELECT c1.id, c1.mx FROM cte c1 JOIN cte c2 ON c1.id = c2.id
              |""".stripMargin)
          // Must not throw at execution (the body's subquery must be physically planned), and reuse
          // of the shared materialization must hold.
          assertCTEReuseApplied(df)
          assertNoDanglingReusedExchange(df)
          // mx is the scalar subquery result max(m)=3, identical for every row.
          checkAnswer(df.select("mx").distinct(), Seq(Row(3)))
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Deep / mixed nesting matrix: multi-level nested CTEs, subqueries nested inside subqueries
  // inside CTE bodies, CTEs referenced across several scopes and nesting depths at once. These
  // stress the recursion in UnwrapCTEReuseExchange (nested CTEReuseExchange) and the ordering of
  // subquery planning vs unwrapping at every level.
  // ---------------------------------------------------------------------------

  test("3-level nested CTEs: inner -> mid -> outer, all materialized and reused") {
    withCTEReuseNoAQE {
      withReuseSrc {
        // cte_inner referenced twice in cte_mid; cte_mid referenced twice in cte_outer; cte_outer
        // referenced twice in the main query -> three distinct materializations, each reused.
        val df = sqlWithForcedCTEReuse(
          """WITH
            |  cte_inner AS (SELECT id, v, rand() as ri FROM reuse_src),
            |  cte_mid AS (
            |    SELECT a.id, a.v + b.v as mv, rand() as rm
            |    FROM cte_inner a JOIN cte_inner b ON a.id = b.id
            |  ),
            |  cte_outer AS (
            |    SELECT a.id, a.mv + b.mv as ov, rand() as ro
            |    FROM cte_mid a JOIN cte_mid b ON a.id = b.id
            |  )
            |SELECT c1.id, c2.ov FROM cte_outer c1 JOIN cte_outer c2 ON c1.id = c2.id
            |""".stripMargin)
        assertCTEReuseApplied(df, expectedDistinctCTEs = 3)
        assertNoDanglingReusedExchange(df)
        // Three distinct CTE materializations -> at least 3 ReusedExchangeExec (one per level).
        val reused = df.queryExecution.executedPlan.collectWithSubqueries {
          case r: ReusedExchangeExec => r
        }
        assert(reused.size >= 3,
          s"Expected >= 3 ReusedExchangeExec for 3-level nested CTEs, got ${reused.size}:\n" +
            df.queryExecution.executedPlan.treeString)
        df.collect()
      }
    }
  }

  test("nested subquery inside a subquery inside the CTE body") {
    withCTEReuseNoAQE {
      withTable("nsub_src", "nsub_a", "nsub_b") {
        sql("CREATE TABLE nsub_src (id INT, v INT) USING parquet")
        sql("INSERT INTO nsub_src VALUES (1, 10), (2, 20), (3, 30)")
        sql("CREATE TABLE nsub_a (a INT) USING parquet")
        sql("INSERT INTO nsub_a VALUES (5), (6), (7)")
        sql("CREATE TABLE nsub_b (b INT) USING parquet")
        sql("INSERT INTO nsub_b VALUES (100), (200)")
        // The CTE body has a scalar subquery whose own body has another scalar subquery
        // (two levels of subquery nesting buried in the materialized CTE body). Both must be
        // physically planned for execution to succeed.
        val df = sqlWithForcedCTEReuse(
          """WITH cte AS (
            |  SELECT id, v,
            |    (SELECT max(a) + (SELECT max(b) FROM nsub_b) FROM nsub_a) as mx,
            |    rand() as r
            |  FROM nsub_src
            |)
            |SELECT c1.id, c1.mx FROM cte c1 JOIN cte c2 ON c1.id = c2.id
            |""".stripMargin)
        assertCTEReuseApplied(df)
        assertNoDanglingReusedExchange(df)
        // max(a)=7 + max(b)=200 = 207, identical for every row.
        checkAnswer(df.select("mx").distinct(), Seq(Row(207)))
      }
    }
  }

  test("CTE ref inside a subquery that is inside another CTE's body") {
    withCTEReuseNoAQE {
      withReuseSrc {
        // Both references to cte_base sit inside cte_wrap's body, one in its FROM clause and one
        // in a scalar subquery -> cte_base is reused between cte_wrap's body and a subquery
        // nested in it. The main query reads only cte_wrap.
        val df = sqlWithForcedCTEReuse(
          """WITH
            |  cte_base AS (SELECT id, v, rand() as rb FROM reuse_src),
            |  cte_wrap AS (
            |    SELECT id, v, (SELECT avg(v) FROM cte_base) as av, rand() as rw FROM cte_base
            |  )
            |SELECT w1.id, w2.av FROM cte_wrap w1 JOIN cte_wrap w2 ON w1.id = w2.id
            |""".stripMargin)
        assertCTEReuseApplied(df, expectedDistinctCTEs = 2)
        assertNoDanglingReusedExchange(df)
        df.collect()
      }
    }
  }

  test("nested inner CTE referenced in outer's body and in the main query") {
    withCTEReuseNoAQE {
      withReuseSrc {
        // cte_inner is referenced inside cte_outer's body AND directly in the main query -> the
        // inner materialization is shared across the outer CTE's body scope and the main scope.
        val df = sqlWithForcedCTEReuse(
          """WITH
            |  cte_inner AS (SELECT id, v, rand() as ri FROM reuse_src),
            |  cte_outer AS (
            |    SELECT a.id, a.v + b.v as ov, rand() as ro
            |    FROM cte_inner a JOIN cte_inner b ON a.id = b.id
            |  )
            |SELECT o.id, i.v FROM cte_outer o JOIN cte_inner i ON o.id = i.id
            |""".stripMargin)
        assertCTEReuseApplied(df)
        assertNoDanglingReusedExchange(df)
        df.collect()
      }
    }
  }

  test("IN-subquery inside the CTE body is fully planned and reused") {
    withCTEReuseNoAQE {
      withReuseSrc {
        withTable("in_dim_noaqe") {
          sql("CREATE TABLE in_dim_noaqe (k INT) USING parquet")
          sql("INSERT INTO in_dim_noaqe VALUES (1), (2), (3)")
          // The CTE body carries an IN-subquery predicate (not a scalar subquery) -> exercises the
          // IN_SUBQUERY planning path inside the materialized body.
          val df = sqlWithForcedCTEReuse(
            """WITH cte AS (
              |  SELECT id, v, rand() as r FROM reuse_src WHERE id IN (SELECT k FROM in_dim_noaqe)
              |)
              |SELECT c1.id, c2.v FROM cte c1 JOIN cte c2 ON c1.id = c2.id
              |""".stripMargin)
          assertCTEReuseApplied(df)
          assertNoDanglingReusedExchange(df)
          df.collect()
        }
      }
    }
  }

  test("CTE referenced at three different nesting depths at once") {
    withCTEReuseNoAQE {
      withReuseSrc {
        // cte is referenced in the main query (depth 0), in a scalar subquery (depth 1), and in a
        // subquery nested inside that subquery (depth 2). One materialization shared across all.
        val df = sqlWithForcedCTEReuse(
          """WITH cte AS (SELECT id, v, rand() as r FROM reuse_src)
            |SELECT id FROM cte m
            |WHERE m.v > (
            |  SELECT avg(v) FROM cte s
            |  WHERE s.v < (SELECT max(v) FROM cte)
            |)
            |""".stripMargin)
        assertCTEReuseApplied(df)
        assertNoDanglingReusedExchange(df)
        df.collect()
      }
    }
  }

  test("two nested CTE chains, each 2 levels, distinct materializations") {
    withCTEReuseNoAQE {
      withReuseSrc {
        // Two independent nested chains (A_inner->A_outer, B_inner->B_outer) with distinct bodies,
        // unioned. Four distinct materializations, each reused -> no cross-chain collapse.
        val df = sqlWithForcedCTEReuse(
          """WITH
            |  a_inner AS (SELECT id, v + 1 as av, rand() as rai FROM reuse_src),
            |  a_outer AS (
            |    SELECT x.id, x.av + y.av as ao FROM a_inner x JOIN a_inner y ON x.id = y.id
            |  ),
            |  b_inner AS (SELECT id, v * 7 as bv, rand() as rbi FROM reuse_src),
            |  b_outer AS (
            |    SELECT x.id, x.bv + y.bv as bo FROM b_inner x JOIN b_inner y ON x.id = y.id
            |  )
            |SELECT p.id, q.ao FROM a_outer p JOIN a_outer q ON p.id = q.id
            |UNION ALL
            |SELECT p.id, q.bo FROM b_outer p JOIN b_outer q ON p.id = q.id
            |""".stripMargin)
        assertCTEReuseApplied(df, expectedDistinctCTEs = 4)
        assertNoDanglingReusedExchange(df)
        val reused = df.queryExecution.executedPlan.collectWithSubqueries {
          case r: ReusedExchangeExec => r
        }
        assert(reused.size >= 4,
          s"Expected >= 4 ReusedExchangeExec for two 2-level chains, got ${reused.size}:\n" +
            df.queryExecution.executedPlan.treeString)
        df.collect()
      }
    }
  }

  test("outer subquery references a reused CTE whose body contains a subquery") {
    withCTEReuseNoAQE {
      withReuseSrc {
        withTable("osq_dim") {
          sql("CREATE TABLE osq_dim (m INT) USING parquet")
          sql("INSERT INTO osq_dim VALUES (1), (2), (3)")
          // The exact triple-nested scope: an OUTER scalar subquery (in the main query's WHERE)
          // whose body references a reused CTE (cte c1 JOIN cte c2), and that CTE's body itself
          // contains a scalar subquery ((SELECT max(m) FROM osq_dim)).
          //
          // This forces the recursion: the CTE lives only inside the outer subquery, so it is
          // unwrapped at that subquery's OWN preparation level (prepareExecutedPlan re-runs the
          // full reordered batch), and only then can that level's PlanSubqueries reach the
          // CTE-body's inner subquery. If UnwrapCTEReuseExchange did not run before PlanSubqueries
          // at every recursive level, the inner subquery would stay logical and crash with
          // "... cannot be cast to SparkPlan".
          val df = sqlWithForcedCTEReuse(
            """WITH cte AS (
              |  SELECT id, v, (SELECT max(m) FROM osq_dim) as mx, rand() as r FROM reuse_src
              |)
              |SELECT o.id FROM reuse_src o
              |WHERE o.v >= (
              |  SELECT min(c1.mx) FROM cte c1 JOIN cte c2 ON c1.id = c2.id
              |)
              |""".stripMargin)
          // Must not throw at execution (both the outer subquery and the CTE-body subquery must be
          // physically planned), reuse must hold, and no dangling references.
          assertCTEReuseApplied(df)
          assertNoDanglingReusedExchange(df)
          df.collect()
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // VerifyCTEReuse runs only for plans that seal a CTE.
  // ---------------------------------------------------------------------------

  // Exposes the main-query preparation rules of a query.
  private class PreparationsInspector(df: DataFrame)
      extends QueryExecution(spark, df.queryExecution.logical) {
    def rules: Seq[Rule[SparkPlan]] = preparations
  }

  // The main-query preparation rules of `df`'s query.
  private def mainQueryPreparations(df: DataFrame): Seq[Rule[SparkPlan]] = {
    val qe = new PreparationsInspector(df)
    qe.assertOptimized()
    qe.rules
  }

  private def hasVerifier(rules: Seq[Rule[SparkPlan]]): Boolean =
    rules.exists(_.isInstanceOf[VerifyCTEReuse])

  test("VerifyCTEReuse is not added for a query without a sealed CTE") {
    withCTEReuseNoAQE {
      withReuseSrc {
        val df = sql("SELECT v, count(*) FROM reuse_src GROUP BY v")
        assert(!hasVerifier(mainQueryPreparations(df)))
        // A forced CTE referenced once is not sealed either.
        val single = sqlWithForcedCTEReuse(
          "WITH cte AS (SELECT id, rand() AS r FROM reuse_src) SELECT * FROM cte")
        assert(!hasVerifier(mainQueryPreparations(single)))
        val shared = sqlWithForcedCTEReuse(
          """WITH cte AS (SELECT id, rand() AS r FROM reuse_src)
            |SELECT c1.id FROM cte c1 JOIN cte c2 ON c1.id = c2.id
            |""".stripMargin)
        assert(hasVerifier(mainQueryPreparations(shared)))
      }
    }
  }

  test("VerifyCTEReuse is not added and nothing is unwrapped with the reuse flag off") {
    withCTEReuseNoAQE {
      withSQLConf(cteReuseConf -> "false") {
        withReuseSrc {
          val df = sqlWithForcedCTEReuse(
            """WITH cte AS (SELECT id, rand() AS r FROM reuse_src)
              |SELECT c1.id FROM cte c1 JOIN cte c2 ON c1.id = c2.id
              |""".stripMargin)
          assert(!hasVerifier(mainQueryPreparations(df)))
          assert(df.queryExecution.sparkPlan.collectWithSubqueries {
            case c: CTEReuseExchange => c
          }.isEmpty)
          df.collect()
        }
      }
    }
  }

  test("exchange reuse disabled: a CTE shared only inside a subquery still fails verification") {
    withCTEReuseNoAQE {
      withSQLConf(SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
        withReuseSrc {
          // Both references sit in one scalar subquery, so only the subquery plan holds the
          // CTE reuse relations; the main-query verifier must still be added and must throw.
          val df = sqlWithForcedCTEReuse(
            """WITH cte AS (SELECT id, v, rand() as r FROM reuse_src)
              |SELECT id FROM reuse_src o
              |WHERE o.v >= (SELECT min(c1.v) FROM cte c1 JOIN cte c2 ON c1.id = c2.id)
              |""".stripMargin)
          val e = intercept[SparkException] {
            df.queryExecution.executedPlan
          }
          assert(e.getMessage.contains("Guaranteed CTE shuffle reuse (AQE off) failed"),
            s"Expected the fail-on-reuse internal error, got:\n${e.getMessage}")
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Root shapes, identical bodies, pins and the immutable CTE shuffle.
  // ---------------------------------------------------------------------------

  // The live (non-reused) LOCAL_SHUFFLE_FOR_CTE shuffles of `df`'s executed plan.
  private def liveCTEShuffles(df: DataFrame): Seq[ShuffleExchangeExec] =
    df.queryExecution.executedPlan.collectWithSubqueries {
      case s: ShuffleExchangeExec if s.shuffleOrigin.isInstanceOf[LOCAL_SHUFFLE_FOR_CTE] => s
    }

  test("a REPARTITION(n) root plans one CTE shuffle with n partitions") {
    withCTEReuseNoAQE {
      withReuseSrc {
        val df = sqlWithForcedCTEReuse(
          """WITH cte AS (SELECT /*+ REPARTITION(3) */ id, rand() AS r FROM reuse_src)
            |SELECT c1.r = c2.r AS same FROM cte c1 JOIN cte c2 ON c1.id = c2.id
            |""".stripMargin)
        assertCTEReuseApplied(df)
        val shuffles = liveCTEShuffles(df)
        assert(shuffles.size == 1 && shuffles.head.outputPartitioning.numPartitions == 3,
          s"Expected one live 3-partition CTE shuffle:\n${df.queryExecution.executedPlan}")
        // The root is stamped in place: no second shuffle sits below the CTE shuffle.
        assert(shuffles.head.child.collect { case s: ShuffleExchangeExec => s }.isEmpty,
          s"Expected no shuffle below the CTE shuffle:\n${shuffles.head.treeString}")
        checkAnswer(df, (1 to 5).map(_ => Row(true)))
      }
    }
  }

  // Asserts that `m` is sealed together with `f`, whose body only selects it: the three
  // references share one plan-reuse id, and the executed plan runs one live shuffle of `m`'s
  // body, the CTE reuse shuffle that the other copies reuse, and no plain shuffle of it.
  private def assertRetainedSealedWithForced(df: DataFrame): Unit = {
    val reuses = df.queryExecution.optimizedPlan.collectWithSubqueries {
      case r: CTEReuseRelation => r
    }
    assert(reuses.size == 3 && reuses.map(_.cteId).distinct.size == 1,
      s"Expected m to be sealed with f under one id:\n${df.queryExecution.optimizedPlan}")
    assertCTEReuseApplied(df)
    val executed = df.queryExecution.executedPlan
    assert(liveCTEShuffles(df).size == 1,
      s"Expected one live CTE reuse shuffle:\n${executed.treeString}")
    val plainShuffles = executed.collectWithSubqueries {
      case s: ShuffleExchangeExec if s.shuffleOrigin == REBALANCE_PARTITIONS_BY_NONE => s
    }
    assert(plainShuffles.isEmpty,
      s"Expected no plain shuffle of the MATERIALIZED body:\n${executed.treeString}")
  }

  test("a forced CTE that only selects a MATERIALIZED CTE read directly shares its shuffle") {
    withCTEReuseNoAQE {
      // Only `f` is forced. Its body lowers to `m`'s own lowered root, so `m` is sealed together
      // with `f`, and `m`'s body runs once, under one shuffle that every reference of both reads,
      // so all three see the same value for each id. The UDF returns a fresh value on every call,
      // so a second evaluation of `m`'s body shows, which rand() with its fixed seed would hide.
      // The ids are not nullable and every column is read, so no filter or projection is pushed
      // into `f`'s body, which stays a bare reference to `m`.
      spark.udf.register("retained_cte_uuid",
        udf(() => java.util.UUID.randomUUID().toString).asNondeterministic())
      val df = sqlWithForcedCTEs(
        """WITH m AS MATERIALIZED (SELECT id, retained_cte_uuid() AS r FROM range(1, 6)),
          |  f AS (SELECT * FROM m)
          |SELECT a.id, a.r = b.r AS ab, a.r = c.r AS ac
          |FROM f a JOIN f b ON a.id = b.id JOIN m c ON a.id = c.id
          |""".stripMargin, names = Set("f"))
      checkAnswer(df, (1L to 5L).map(Row(_, true, true)))
      assertRetainedSealedWithForced(df)
    }
  }

  test("a forced CTE that only selects a deterministic MATERIALIZED CTE shuffles it once") {
    withCTEReuseNoAQE {
      // The reads are combined with UNION ALL, so no consumer requires a distribution and the
      // only shuffles are those of the CTE lowering.
      val df = sqlWithForcedCTEs(
        """WITH m AS MATERIALIZED (SELECT id, id * 2 AS w FROM range(1, 6)),
          |  f AS (SELECT * FROM m)
          |SELECT * FROM f UNION ALL SELECT * FROM f UNION ALL SELECT * FROM m
          |""".stripMargin, names = Set("f"))
      checkAnswer(df, Seq.fill(3)((1L to 5L).map(i => Row(i, i * 2))).flatten)
      assertRetainedSealedWithForced(df)
    }
  }

  test("forced CTEs with identical deterministic bodies run one shuffle") {
    withCTEReuseNoAQE {
      withReuseSrc {
        def run(bodyA: String, bodyB: String): DataFrame = sqlWithForcedCTEReuse(
          s"""WITH a AS ($bodyA), b AS ($bodyB)
             |SELECT * FROM a UNION ALL SELECT * FROM a
             |UNION ALL SELECT * FROM b UNION ALL SELECT * FROM b
             |""".stripMargin)
        val same =
          run("SELECT id, v * 2 AS w FROM reuse_src", "SELECT id, v * 2 AS w FROM reuse_src")
        assert(liveCTEShuffles(same).size == 1,
          s"Expected one live CTE shuffle:\n${same.queryExecution.executedPlan}")
        assert(same.queryExecution.executedPlan.collectWithSubqueries {
          case r: ReusedExchangeExec => r
        }.size == 3, s"Expected three reused exchanges:\n${same.queryExecution.executedPlan}")
        assert(same.collect().length == 20)
        val nondeterministic = run(
          "SELECT id, rand(42) AS w FROM reuse_src", "SELECT id, rand(42) AS w FROM reuse_src")
        assert(liveCTEShuffles(nondeterministic).size == 2,
          s"Expected two live CTE shuffles:\n${nondeterministic.queryExecution.executedPlan}")
        val different = run(
          "SELECT id, v * 2 AS w FROM reuse_src", "SELECT id, v * 3 AS w FROM reuse_src")
        assert(liveCTEShuffles(different).size == 2,
          s"Expected two live CTE shuffles:\n${different.queryExecution.executedPlan}")
      }
    }
  }

  test("a CTE whose body equals a sealed forced body reuses its CTE shuffle") {
    withCTEReuseNoAQE {
      withReuseSrc {
        // `a` is forced and read twice; `b` has the same deterministic body but is read once, as
        // a forced CTE or as an ordinary MATERIALIZED CTE, so it is not sealed on its own.
        val body = "SELECT id, v * 2 AS w FROM reuse_src"
        Seq(
          "forced CTE read once" -> (s"b AS ($body)", Set("a", "b")),
          "MATERIALIZED CTE" -> (s"b AS MATERIALIZED ($body)", Set("a"))).foreach {
          case (name, (twin, forced)) =>
            Seq(s"a AS ($body), $twin", s"$twin, a AS ($body)").foreach { defs =>
              val df = sqlWithForcedCTEs(
                s"""WITH $defs
                   |SELECT * FROM a UNION ALL SELECT * FROM a UNION ALL SELECT * FROM b
                   |""".stripMargin, forced)
              // One live CTE shuffle, which the two other copies, `b`'s included, reuse; no plain
              // shuffle of the body is left.
              val executed = df.queryExecution.executedPlan
              assertCTEReuseApplied(df)
              assert(liveCTEShuffles(df).size == 1 &&
                executed.collectWithSubqueries { case r: ReusedExchangeExec => r }.size == 2,
                s"$name ($defs): expected one live CTE shuffle, reused twice:\n$executed")
              assert(executed.collectWithSubqueries {
                case s: ShuffleExchangeExec if s.shuffleOrigin == REBALANCE_PARTITIONS_BY_NONE => s
              }.isEmpty, s"$name ($defs): expected no plain shuffle of the body:\n$executed")
              assert(df.collect().length == 15)
            }
        }
      }
    }
  }

  test("a CTE shuffle that is not the co-partition target is wrapped, not rewritten") {
    withCTEReuseNoAQE {
      // No inferred IS NOT NULL filter, so the CTE shuffle is the join's direct child.
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.CONSTRAINT_PROPAGATION_ENABLED.key -> "false") {
        withReuseSrc {
          withTable("bucketed_side") {
            sql("CREATE TABLE bucketed_side (id INT, w INT) USING parquet " +
              "CLUSTERED BY (id) INTO 8 BUCKETS")
            sql("INSERT INTO bucketed_side VALUES (1, 100), (2, 200), (3, 300)")
            // c1's CTE shuffle (hash on id into 4 partitions) satisfies the join's clustering, but
            // the bucketed side (8 buckets, no exchange) is the co-partition target, so the CTE
            // shuffle must be wrapped in a new shuffle rather than rewritten in place; c2 reads
            // the CTE as is, and both copies must stay reusable.
            val df = sqlWithForcedCTEReuse(
              """WITH cte AS (SELECT /*+ REPARTITION(4, id) */ id, v, rand() AS r FROM reuse_src)
                |SELECT c1.id FROM cte c1 JOIN bucketed_side b ON c1.id = b.id
                |UNION ALL
                |SELECT c2.id FROM cte c2
                |""".stripMargin)
            assertCTEReuseApplied(df)
            assertNoDanglingReusedExchange(df)
            // The join side reads the CTE through a new 8-partition shuffle over the CTE shuffle.
            val wrappers = df.queryExecution.executedPlan.collect {
              case s: ShuffleExchangeExec if s.child.isInstanceOf[ReusedExchangeExec] ||
                  s.child.isInstanceOf[ShuffleExchangeExec] => s
            }
            assert(wrappers.exists(_.outputPartitioning.numPartitions == 8),
              s"Expected a shuffle above the CTE shuffle:\n${df.queryExecution.executedPlan}")
            checkAnswer(df, Seq(1, 2, 3, 1, 2, 3, 4, 5).map(Row(_)))
          }
        }
      }
    }
  }

  // Shuffles that sit directly above a CTE reuse shuffle, live or reused.
  private def cteShuffleWrappers(df: DataFrame): Seq[ShuffleExchangeExec] = {
    def isCTEShuffle(p: SparkPlan): Boolean = p match {
      case s: ShuffleExchangeExec => s.isCreatedForSubplanReuse
      case r: ReusedExchangeExec => isCTEShuffle(r.child)
      case _ => false
    }
    df.queryExecution.executedPlan.collectWithSubqueries {
      case s: ShuffleExchangeExec if isCTEShuffle(s.child) => s
    }
  }

  // Shuffles in the main query whose input reads `table`.
  private def shufflesOverTable(df: DataFrame, table: String): Seq[ShuffleExchangeExec] =
    df.queryExecution.executedPlan.collect {
      case s: ShuffleExchangeExec if s.child.collectFirst {
        case f: FileSourceScanExec if f.tableIdentifier.exists(_.table == table) => f
      }.isDefined => s
    }

  test("a CTE shuffle next to a side that is shuffled anyway keeps its layout") {
    withCTEReuseNoAQE {
      // No inferred IS NOT NULL filter, so the CTE shuffle is the join's direct child.
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.CONSTRAINT_PROPAGATION_ENABLED.key -> "false",
          SQLConf.SHUFFLE_PARTITIONS.key -> "10") {
        withReuseSrc {
          withTable("plain_side") {
            sql("CREATE TABLE plain_side (id INT, w INT) USING parquet")
            sql("INSERT INTO plain_side VALUES (1, 100), (2, 200), (3, 300)")
            // plain_side has to be shuffled for the join either way. The CTE shuffle (hash on id
            // into 4 partitions) cannot be re-partitioned in place, so it is the co-partition
            // target even though it has fewer partitions than spark.sql.shuffle.partitions:
            // plain_side is shuffled into 4 partitions, and the CTE output is shuffled once.
            val df = sqlWithForcedCTEReuse(
              """WITH cte AS (SELECT /*+ REPARTITION(4, id) */ id, v, rand() AS r FROM reuse_src)
                |SELECT c1.id FROM cte c1 JOIN plain_side p ON c1.id = p.id
                |UNION ALL
                |SELECT c2.id FROM cte c2
                |""".stripMargin)
            assertCTEReuseApplied(df)
            assertNoDanglingReusedExchange(df)
            val executed = df.queryExecution.executedPlan
            val cteShuffles = liveCTEShuffles(df)
            assert(cteShuffles.size == 1 &&
              cteShuffles.head.outputPartitioning.numPartitions == 4,
              s"Expected one live 4-partition CTE shuffle:\n$executed")
            assert(cteShuffleWrappers(df).isEmpty,
              s"Expected no shuffle above the CTE shuffle:\n$executed")
            val sideShuffles = shufflesOverTable(df, "plain_side")
            assert(sideShuffles.size == 1 &&
              sideShuffles.head.outputPartitioning.numPartitions == 4,
              s"Expected plain_side to be shuffled into the CTE's 4 partitions:\n$executed")
            checkAnswer(df, Seq(1, 2, 3, 1, 2, 3, 4, 5).map(Row(_)))
          }
        }
      }
    }
  }

  test("a CTE shuffle with more partitions than a bucketed side is the co-partition target") {
    withCTEReuseNoAQE {
      // No inferred IS NOT NULL filter, so the CTE shuffle is the join's direct child.
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.CONSTRAINT_PROPAGATION_ENABLED.key -> "false") {
        withReuseSrc {
          withTable("bucketed_side") {
            sql("CREATE TABLE bucketed_side (id INT, w INT) USING parquet " +
              "CLUSTERED BY (id) INTO 2 BUCKETS")
            sql("INSERT INTO bucketed_side VALUES (1, 100), (2, 200), (3, 300)")
            // Neither side needs a new shuffle on its own: the CTE shuffle (hash on id into 4
            // partitions) and the bucketed scan (2 buckets) both keep their layout for free, so
            // the one with more partitions is the co-partition target, as with AQE on. The
            // bucketed side is shuffled into 4 partitions and the CTE shuffle is not wrapped.
            val df = sqlWithForcedCTEReuse(
              """WITH cte AS (SELECT /*+ REPARTITION(4, id) */ id, v, rand() AS r FROM reuse_src)
                |SELECT c1.id FROM cte c1 JOIN bucketed_side b ON c1.id = b.id
                |UNION ALL
                |SELECT c2.id FROM cte c2
                |""".stripMargin)
            assertCTEReuseApplied(df)
            assertNoDanglingReusedExchange(df)
            val executed = df.queryExecution.executedPlan
            assert(cteShuffleWrappers(df).isEmpty,
              s"Expected no shuffle above the CTE shuffle:\n$executed")
            val sideShuffles = shufflesOverTable(df, "bucketed_side")
            assert(sideShuffles.size == 1 &&
              sideShuffles.head.outputPartitioning.numPartitions == 4,
              s"Expected bucketed_side to be shuffled into the CTE's 4 partitions:\n$executed")
            checkAnswer(df, Seq(1, 2, 3, 1, 2, 3, 4, 5).map(Row(_)))
          }
        }
      }
    }
  }

  test("a pinned forced CTE materializes on the pinned keys and partition count") {
    withCTEReuseNoAQE {
      withReuseSrc {
        val df = sqlWithPinnedCTEReuse(
          """WITH cte AS (SELECT id, v, rand() AS r FROM reuse_src)
            |SELECT c1.id, c2.v FROM cte c1 JOIN cte c2 ON c1.id = c2.id
            |""".stripMargin, numPartitions = 3)
        assertCTEReuseApplied(df)
        val shuffles = liveCTEShuffles(df)
        assert(shuffles.size == 1, df.queryExecution.executedPlan.treeString)
        shuffles.head.outputPartitioning match {
          case h: HashPartitioning =>
            assert(h.numPartitions == 3 && h.expressions.map(_.references.head.name) == Seq("id"),
              s"Expected the pinned partitioning, got $h")
          case other => fail(s"Expected a hash partitioning, got $other")
        }
        checkAnswer(df, Seq(Row(1, 10), Row(2, 20), Row(3, 30), Row(4, 40), Row(5, 50)))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Streaming and Dataset.inputFiles.
  // ---------------------------------------------------------------------------

  test("a streaming forced CTE with a stateful body referenced twice runs micro-batches") {
    withCTEReuseNoAQE {
      import testImplicits._
      val input = new MemoryStream[Int](0, spark)
      withTempView("cte_stream_src") {
        input.toDF().createOrReplaceTempView("cte_stream_src")
        val df = sqlWithForcedCTEReuse(
          """WITH c AS (SELECT value % 2 AS k, count(*) AS cnt FROM cte_stream_src GROUP BY 1)
            |SELECT k, cnt FROM c WHERE k = 0
            |UNION ALL
            |SELECT k, cnt FROM c WHERE k = 1
            |""".stripMargin)
        val query = df.writeStream
          .format("memory")
          .queryName("cte_stream_sink")
          .outputMode(OutputMode.Complete())
          .start()
        try {
          input.addData(1, 2, 3)
          query.processAllAvailable()
          checkAnswer(spark.table("cte_stream_sink"), Seq(Row(0, 1L), Row(1, 2L)))
          input.addData(4, 5)
          query.processAllAvailable()
          checkAnswer(spark.table("cte_stream_sink"), Seq(Row(0, 2L), Row(1, 3L)))
          val lastOptimized = query.asInstanceOf[StreamingQueryWrapper]
            .streamingQuery.lastExecution.optimizedPlan
          assert(lastOptimized.collectWithSubqueries { case r: CTEReuseRelation => r }.isEmpty,
            s"Expected no CTE reuse relation in a micro-batch:\n$lastOptimized")
        } finally {
          query.stop()
        }
      }
    }
  }

  test("a static forced CTE in a streaming query gets new random values in every micro-batch") {
    withCTEReuseNoAQE {
      import testImplicits._
      val input = new MemoryStream[Int](0, spark)
      withTable("cte_static_src") {
        sql("CREATE TABLE cte_static_src (id INT) USING parquet")
        sql("INSERT INTO cte_static_src VALUES (1), (2)")
        withTempView("cte_rand_stream_src") {
          input.toDF().createOrReplaceTempView("cte_rand_stream_src")
          val df = sqlWithForcedCTEReuse(
            """WITH c AS (SELECT id, rand() AS r FROM cte_static_src)
              |SELECT s.value, c1.r FROM cte_rand_stream_src s
              |JOIN c c1 ON s.value = c1.id
              |JOIN c c2 ON c1.id = c2.id
              |""".stripMargin)
          val query = df.writeStream
            .format("memory")
            .queryName("cte_rand_stream_sink")
            .outputMode(OutputMode.Append())
            .start()
          try {
            input.addData(1)
            query.processAllAvailable()
            input.addData(1)
            query.processAllAvailable()
            val rows = spark.table("cte_rand_stream_sink").collect()
            assert(rows.length == 2 && rows.forall(_.getInt(0) == 1), rows.mkString(", "))
            assert(rows.map(_.getDouble(1)).distinct.length == 2,
              s"Expected new random values in each micro-batch, got ${rows.mkString(", ")}")
            val lastOptimized = query.asInstanceOf[StreamingQueryWrapper]
              .streamingQuery.lastExecution.optimizedPlan
            assert(lastOptimized.collectWithSubqueries { case r: CTEReuseRelation => r }.isEmpty,
              s"Expected no CTE reuse relation in a micro-batch:\n$lastOptimized")
          } finally {
            query.stop()
          }
        }
      }
    }
  }

  test("inputFiles includes the files read inside a sealed forced CTE") {
    withCTEReuseNoAQE {
      withReuseSrc {
        val plain = spark.table("reuse_src").inputFiles.toSet
        assert(plain.nonEmpty)
        val df = sqlWithForcedCTEReuse(
          """WITH cte AS (SELECT id, v, rand() AS r FROM reuse_src)
            |SELECT c1.id FROM cte c1 JOIN cte c2 ON c1.id = c2.id
            |""".stripMargin)
        assert(df.queryExecution.optimizedPlan.collectWithSubqueries {
          case r: CTEReuseRelation => r
        }.nonEmpty)
        assert(df.inputFiles.toSet == plain)

        // A sealed CTE nested inside another sealed CTE's body: `inner_cte` is read twice inside
        // `outer_cte`'s body, and `outer_cte` twice by the main query, so the table is scanned
        // only inside the nested shared body.
        val nested = sqlWithForcedCTEReuse(
          """WITH inner_cte AS (SELECT id, v, rand() AS r FROM reuse_src),
            |  outer_cte AS (
            |    SELECT i1.id, i1.r FROM inner_cte i1 JOIN inner_cte i2 ON i1.id = i2.id)
            |SELECT o1.id FROM outer_cte o1 JOIN outer_cte o2 ON o1.id = o2.id
            |""".stripMargin)
        val optimized = nested.queryExecution.optimizedPlan
        val outerRefs = optimized.collectWithSubqueries { case r: CTEReuseRelation => r }
        assert(outerRefs.size == 2 && outerRefs.map(_.cteId).distinct.size == 1,
          s"Expected the outer CTE to be sealed:\n${optimized.treeString}")
        val outerBody = outerRefs.head.sharedSubplan
        val innerRefs = outerBody.collectWithSubqueries { case r: CTEReuseRelation => r }
        assert(innerRefs.size == 2 && innerRefs.map(_.cteId).distinct.size == 1 &&
          innerRefs.head.cteId != outerRefs.head.cteId,
          s"Expected the inner CTE to be sealed inside the outer body:\n${outerBody.treeString}")
        assert(Seq(optimized, outerBody).forall(_.collect {
          case l: LogicalRelation => l
        }.isEmpty), s"Expected the table to be scanned only inside the nested shared body:\n" +
          outerBody.treeString)
        assert(nested.inputFiles.toSet == plain)
      }
    }
  }
}
