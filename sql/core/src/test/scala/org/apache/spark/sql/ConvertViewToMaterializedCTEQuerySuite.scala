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
import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Integration tests for the `ConvertViewToMaterializedCTE` optimizer rule: repeated
 * references to the same view are rewritten into one CTE definition with multiple
 * references when `spark.sql.optimizer.convertViewToMaterializedCTE` is enabled. After
 * the final `Replace CTE with Repartition` batch, a converted view shows up as one
 * repartition node per reference site; exchange reuse deduplicates them at execution
 * time.
 */
class ConvertViewToMaterializedCTEQuerySuite extends QueryTest with SharedSparkSession {
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
    spark.sql(query).queryExecution.optimizedPlan.collect {
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
      withSQLConf(SQLConf.CONVERT_VIEW_TO_MATERIALIZED_CTE.key -> "true") {
        checkAnswer(spark.sql(query), expected)
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
        // Both views convert: each reference site of v2 renders its own shuffle
        // boundary with v1's boundary nested inside, so 2 sites x 2 boundaries = 4.
        // (v1 alone would yield 2, no conversion 0.)
        assert(countRepartitions(query) == 4)
      }
    }
  }
}
