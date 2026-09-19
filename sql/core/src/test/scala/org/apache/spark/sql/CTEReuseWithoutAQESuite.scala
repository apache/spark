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



import org.apache.spark.sql.execution.{BaseSubqueryExec, ReusedSubqueryExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{CTEReuseExchange, Exchange, ReusedExchangeExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for CTE reuse when AQE is off, using [[exchange.UnwrapCTEReuseExchange]] and
 * [[exchange.VerifyCTEReuse]]. Each CTEReuseExchange is unwrapped into a tagged
 * LOCAL_SHUFFLE_FOR_CTE shuffle; the stock ReuseExchangeAndSubquery then reuses the
 * canonically-equal copies into ReusedExchangeExec, and VerifyCTEReuse confirms reuse held.
 */
class CTEReuseWithoutAQESuite
    extends QueryTest with SharedSparkSession {

  private val cteReuseConf =
    "spark.sql.optimizer.replaceCTERefWithCTEReuse.enabled"

  private def withCTEReuseNoAQE(f: => Unit): Unit = {
    withSQLConf(
      cteReuseConf.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false"
    )(f)
  }

  /**
   * Asserts guaranteed CTE shuffle reuse held (AQE off): the executed plan has at least one
   * [[ReusedExchangeExec]] and no [[CTEReuseExchange]] left. (With FAIL_ON_CTE_REUSE_WITHOUT_AQE
   * test-defaulting to true, a reuse failure would already have thrown during preparation.)
   */
  private def assertCTEReuseApplied(df: DataFrame): Unit = {
    val executedPlan = df.queryExecution.executedPlan
    assert(executedPlan.collectWithSubqueries { case c: CTEReuseExchange => c }.isEmpty,
      s"Expected no CTEReuseExchange in executedPlan:\n${executedPlan.treeString}")
    assert(executedPlan.collectWithSubqueries { case r: ReusedExchangeExec => r }.nonEmpty,
      s"Expected >= 1 ReusedExchangeExec for CTE reuse:\n${executedPlan.treeString}")
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

  test("basic: 2-ref CTE join") {
    withCTEReuseNoAQE {
      withTable("cte_noaqe_src") {
        sql("CREATE TABLE cte_noaqe_src (id INT, v INT) USING parquet")
        sql("INSERT INTO cte_noaqe_src VALUES (1, 10), (2, 20), (3, 30)")

        val df = sql(
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

        // executedPlan should have no CTEReuseExchange
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

        val df = sql(
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
        assertCTEReuseApplied(df)

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

        val df = sql(
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
        val df = sql(
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
        assertCTEReuseApplied(df)

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
  // The CTE shuffle is deduplicated into ReusedExchangeExec, and subqueries add their own
  // reuse scope (ReuseExchangeAndSubquery runs per-subquery in PlanSubqueries, then again on the
  // main plan, re-pointing reused nodes). These tests make sure the CTE-shuffle reuse survives
  // that interaction with no dangling ReusedExchangeExec references.
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
        val df = sql(
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
        val df = sql(
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
        val df = sql(
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
        val df = sql(
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
        assertCTEReuseApplied(df)
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
            val df = sql(
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
        // CTEReuseRelation, exercising the partitioning path (not just LocalPartition).
        val df = sql(
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
        val df = sql(
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
          // >= 2 live shuffles sharing a cteId; the FAIL_ON_CTE_REUSE_WITHOUT_AQE_NOT_APPLIED flag
          // governs whether that throws. Turn the flag off here so the query runs and we assert the
          // fallback: no reuse, the "reuse not applied" signal recorded, correct results.
          withSQLConf(
            "spark.sql.optimizer.failOnCTEReuseWithoutAQE.enabled" -> "false") {
            val df = sql(
              """WITH cte AS (SELECT id, v, rand() as r FROM reuse_src)
                |SELECT c1.id, c2.v FROM cte c1 JOIN cte c2 ON c1.id = c2.id
                |""".stripMargin)
            val executedPlan = df.queryExecution.executedPlan
            assert(executedPlan.collectWithSubqueries { case r: ReusedExchangeExec => r }.isEmpty,
              s"Expected no ReusedExchangeExec when exchange reuse is disabled:\n" +
                executedPlan.treeString)
            // VerifyCTEReuse ran with the fail flag off, so the query runs without throwing.
            df.collect()
          }
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Broader scenarios ported from the CTE-reuse prototype (#218516): ref-count,
  // distinct-CTE, and non-trivial materialized-body shapes. Each asserts reuse held via the
  // canonical-equality path (assertCTEReuseApplied + assertNoDanglingReusedExchange).
  // ---------------------------------------------------------------------------

  test("5 refs unioned share one materialization") {
    withCTEReuseNoAQE {
      withReuseSrc {
        val refs = (1 to 5).map(_ => "SELECT id FROM cte").mkString(" UNION ALL ")
        val df = sql(
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
        val df = sql(
          """WITH
            |  cte_a AS (SELECT id, v + 1 as a, rand() as ra FROM reuse_src),
            |  cte_b AS (SELECT id, v * 10 as b, rand() as rb FROM reuse_src)
            |SELECT a1.a FROM cte_a a1 JOIN cte_a a2 ON a1.id = a2.id
            |UNION ALL
            |SELECT b1.b FROM cte_b b1 JOIN cte_b b2 ON b1.id = b2.id
            |""".stripMargin)
        assertCTEReuseApplied(df)
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
            val df = sql(
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
  // UnwrapCTEReuseExchange, which runs AFTER PlanSubqueries -- so the body's own subquery
  // expressions must still be physically planned for execution to succeed.
  // ---------------------------------------------------------------------------

  test("scalar subquery inside the CTE body is fully planned and reused") {
    withCTEReuseNoAQE {
      withReuseSrc {
        withTable("dim_noaqe") {
          sql("CREATE TABLE dim_noaqe (m INT) USING parquet")
          sql("INSERT INTO dim_noaqe VALUES (1), (2), (3)")
          // The CTE body contains a scalar subquery `(SELECT max(m) FROM dim_noaqe)`. rand() keeps
          // the def non-deterministic so it materializes and both refs share it.
          val df = sql(
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
        val df = sql(
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
        assertCTEReuseApplied(df)
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
        val df = sql(
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
        // cte_base is referenced in the main query AND inside a scalar subquery that sits in
        // cte_wrap's body -> cte_base is reused across the main scope and a subquery nested in
        // another CTE's materialized body.
        val df = sql(
          """WITH
            |  cte_base AS (SELECT id, v, rand() as rb FROM reuse_src),
            |  cte_wrap AS (
            |    SELECT id, v, (SELECT avg(v) FROM cte_base) as av, rand() as rw FROM cte_base
            |  )
            |SELECT w1.id, w2.av FROM cte_wrap w1 JOIN cte_wrap w2 ON w1.id = w2.id
            |""".stripMargin)
        assertCTEReuseApplied(df)
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
        val df = sql(
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
          val df = sql(
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
        val df = sql(
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
        val df = sql(
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
        assertCTEReuseApplied(df)
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
          val df = sql(
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
}
