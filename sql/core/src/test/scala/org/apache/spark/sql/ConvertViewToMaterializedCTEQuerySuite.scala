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

import org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Integration tests for the `ConvertViewToMaterializedCTE` optimizer rule: repeated
 * references to the same view are rewritten into one CTE definition with multiple
 * references when `spark.sql.optimizer.convertViewToMaterializedCTE.enabled` is enabled. After
 * the final `Replace CTE with Repartition` batch, a converted view shows up as one
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
    df.queryExecution.optimizedPlan.collect {
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

  test("the converted view body is computed once") {
    // Exchange reuse deduplicates the per-reference shuffles added by the conversion,
    // so the view's body is evaluated once. Pin it on the executed plan: with AQE on
    // the reused exchange hides behind AdaptiveSparkPlanExec and query stage nodes,
    // whose subtrees are not reachable through plain `children` traversal; the
    // `collect` from AdaptiveSparkPlanHelper descends through them.
    withSelfJoinedView {
      Seq(true, false).foreach { aqeEnabled =>
        withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqeEnabled.toString) {
          val df = spark.sql(selfJoinQuery)
          df.collect()
          assert(collect(df.queryExecution.executedPlan) {
            case _: ReusedExchangeExec => 1
          }.length == 1)
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

  test("a divergent view occurrence is not rebound to a converted definition") {
    // Two references to v are resolved while v has one body; a third reference sees a
    // replaced body. The identifier maps to two distinct canonicalized bodies, so the
    // rule must not convert at all: rewriting the divergent occurrence against the
    // qualifying pair's definition would return the old body's rows for it.
    withTempView("v") {
      sql("CREATE OR REPLACE TEMP VIEW v AS SELECT id, id AS k FROM range(4)")
      val held = spark.table("v")
      val pair = held.join(held.as("b"), "id").select($"id")
      sql("CREATE OR REPLACE TEMP VIEW v AS SELECT id, id + 100 AS k FROM range(4)")
      // A fresh Dataset per setting so each collect analyzes a new plan (the rule runs
      // during analysis; reusing one Dataset would reuse its cached analyzed plan).
      def mkQuery(): DataFrame = pair.join(spark.table("v"), "id").select($"id", $"k")
      assert(countRepartitions(mkQuery()) == 0)
      val expected = mkQuery().collect()
      withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
        // Pin the no-conversion contract on the plan as well: rows matching the
        // baseline would not catch a conversion that is semantically harmless here,
        // but no repartition boundary may appear even then.
        assert(countRepartitions(mkQuery()) == 0)
        checkAnswer(mkQuery(), expected)
      }
    }
  }

  test("stale view pair inside a scalar subquery does not crash") {
    // The stale-view scenario where the pair sits inside a scalar subquery: the
    // top-level pass declines the identifier (divergent bodies), so the rule must
    // not convert inside the Subquery-rooted pass either - attaching definitions
    // there would wrap the Subquery root in WithCTE and break FinishAnalysis's
    // destructuring of the subquery result.
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

  test("view with top-level ORDER BY is not converted") {
    // The shuffle boundary added above the definition would destroy the view's
    // ORDER BY, changing what an outer LIMIT sees; the view must stay unconverted.
    // OFFSET composes into Offset/limit nodes wrapping the Sort, so those bodies
    // must be rejected as well.
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

  test("insert into a table selecting from a repeatedly referenced view") {
    withTable("dest") {
      withSelfJoinedView {
        sql("CREATE TABLE dest (id BIGINT, k BIGINT) USING parquet")
        withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
          sql(s"INSERT INTO dest $selfJoinQuery")
        }
        checkAnswer(spark.table("dest"), spark.range(0, 10).select($"id", ($"id" % 10)))
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
        // Only v converts: t and s occur once per v body copy at rule time, but after the
        // rewrite each has a single reference inside v's definition, so InlineCTE flattens
        // them instead of materializing them behind shuffles nothing reuses. Each of the
        // two v sites carries exactly one (reused) shuffle boundary.
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
        // Only v2 converts into a materialized definition: v1's second reference dies with
        // the dropped second occurrence's body, leaving v1 with a single reference inside
        // v2's definition, which InlineCTE flattens back. So each of the 2 v2 sites carries
        // exactly one (reused) shuffle boundary.
        assert(countRepartitions(query) == 2)
      }
    }
  }

  test("a reference inside an unreferenced CTE does not trigger materialization") {
    // `WITH c AS (SELECT id FROM v) SELECT id FROM v WHERE id < 5` reads v once, but the
    // unreferenced `c` still holds a second occurrence when the rule runs. Materializing
    // v for it would add a shuffle over a single live reference; instead the converted
    // definition must stay inlinable, so InlineCTE drops `c` and flattens v's definition.
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
    // A SQL view's body is re-analyzed per occurrence, and the re-analysis re-substitutes
    // the body's inner CTEs, minting a fresh definition id each time. The rule normalizes
    // inner CTE ids before grouping, so occurrences of such a view still convert.
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
