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

import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class OptimizeExpandQuerySuite
    extends QueryTest with SharedSparkSession {

  private def checkOptimizationCorrectness(
      sqlText: String): Unit = {
    withTempView("t") {
      spark.range(10000)
        .selectExpr(
          "id % 100 as key",
          "cast(id % 50 as int) as col1",
          "cast(id % 30 as int) as col2",
          "cast(id % 20 as int) as col3",
          "cast(id % 10 as int) as col4",
          "cast(id % 5 as int) as col5",
          "cast(id as double) as value",
          "cast(id as int) as id_val")
        .createOrReplaceTempView("t")

      // collect() inside withSQLConf to materialize the plan
      // while the config is active (lazy plans read conf at
      // optimization time, not at DataFrame creation time).
      val expected = withSQLConf(
        SQLConf.OPTIMIZE_EXPAND_RATIO.key -> "-1") {
        spark.sql(sqlText).collect()
      }
      withSQLConf(
        SQLConf.OPTIMIZE_EXPAND_RATIO.key -> "2") {
        checkAnswer(spark.sql(sqlText), expected.toSeq)
      }
    }
  }

  test("correctness: pure count distinct with group by") {
    checkOptimizationCorrectness(
      """SELECT key,
        |  count(distinct col1),
        |  count(distinct col2),
        |  count(distinct col3),
        |  count(distinct col4),
        |  count(distinct col5)
        |FROM t GROUP BY key""".stripMargin)
  }

  test("correctness: pure count distinct without group by") {
    checkOptimizationCorrectness(
      """SELECT
        |  count(distinct col1),
        |  count(distinct col2),
        |  count(distinct col3),
        |  count(distinct col4),
        |  count(distinct col5)
        |FROM t""".stripMargin)
  }

  test("correctness: with null values") {
    withTempView("t") {
      spark.range(10000)
        .selectExpr(
          "id % 100 as key",
          "IF(id % 7 = 0, null, id % 50) as col1",
          "IF(id % 11 = 0, null, id % 30) as col2",
          "IF(id % 13 = 0, null, id % 20) as col3")
        .createOrReplaceTempView("t")

      val sqlText =
        """SELECT key,
          |  count(distinct col1),
          |  count(distinct col2),
          |  count(distinct col3)
          |FROM t GROUP BY key""".stripMargin

      val expected = withSQLConf(
        SQLConf.OPTIMIZE_EXPAND_RATIO.key -> "-1") {
        spark.sql(sqlText).collect()
      }
      withSQLConf(
        SQLConf.OPTIMIZE_EXPAND_RATIO.key -> "2") {
        checkAnswer(spark.sql(sqlText), expected.toSeq)
      }
    }
  }

  test("correctness: with filter on count distinct") {
    checkOptimizationCorrectness(
      """SELECT key,
        |  count(distinct col1) FILTER (WHERE id_val > 100),
        |  count(distinct col2) FILTER (WHERE id_val > 200),
        |  count(distinct col3)
        |FROM t GROUP BY key""".stripMargin)
  }

  test("skips optimization when distinct agg has filter clause") {
    withTempView("t") {
      spark.range(1000)
        .selectExpr(
          "id % 10 as key",
          "cast(id % 50 as int) as col1",
          "cast(id % 30 as int) as col2",
          "cast(id % 20 as int) as col3",
          "cast(id as int) as id_val")
        .createOrReplaceTempView("t")

      val sqlText =
        """SELECT key,
          |  count(distinct col1) FILTER (WHERE id_val > 100),
          |  count(distinct col2),
          |  count(distinct col3)
          |FROM t GROUP BY key""".stripMargin

      val hasPreAgg = withSQLConf(
        SQLConf.OPTIMIZE_EXPAND_RATIO.key -> "2") {
        spark.sql(sqlText).queryExecution.optimizedPlan.collect {
          case e: Expand => e.child.isInstanceOf[Aggregate]
        }.exists(identity)
      }
      assert(!hasPreAgg,
        "Should skip optimization when distinct agg has filter clause")
    }
  }

  test("correctness: sum distinct") {
    checkOptimizationCorrectness(
      """SELECT key,
        |  sum(distinct col1),
        |  sum(distinct col2),
        |  count(distinct col3)
        |FROM t GROUP BY key""".stripMargin)
  }

  test("correctness: sum distinct with actual duplicates") {
    withTempView("t") {
      spark.range(10000)
        .selectExpr(
          "id % 10 as key",
          "cast(id % 7 as int) as col1",
          "cast(id % 13 as int) as col2",
          "cast(id % 11 as int) as col3")
        .createOrReplaceTempView("t")

      val sqlText =
        """SELECT key,
          |  sum(distinct col1),
          |  sum(distinct col2),
          |  count(distinct col3)
          |FROM t GROUP BY key""".stripMargin

      val expected = withSQLConf(
        SQLConf.OPTIMIZE_EXPAND_RATIO.key -> "-1") {
        spark.sql(sqlText).collect()
      }
      withSQLConf(
        SQLConf.OPTIMIZE_EXPAND_RATIO.key -> "2") {
        checkAnswer(spark.sql(sqlText), expected.toSeq)
      }
    }
  }

  test("skips optimization for expression-based distinct (col1 + col2)") {
    withTempView("t") {
      spark.range(10000)
        .selectExpr(
          "id % 100 as key",
          "cast(id % 7 as int) as col1",
          "cast(id % 11 as int) as col2",
          "cast(id % 20 as int) as col3")
        .createOrReplaceTempView("t")

      val sqlText =
        """SELECT key,
          |  count(distinct col1 + col2),
          |  count(distinct col3)
          |FROM t GROUP BY key""".stripMargin

      val expected = withSQLConf(
        SQLConf.OPTIMIZE_EXPAND_RATIO.key -> "-1") {
        spark.sql(sqlText).collect()
      }
      withSQLConf(
        SQLConf.OPTIMIZE_EXPAND_RATIO.key -> "2") {
        val df = spark.sql(sqlText)
        // Verify optimization is skipped
        val hasPreAgg = df.queryExecution.optimizedPlan.collect {
          case e: Expand => e.child.isInstanceOf[Aggregate]
        }.exists(identity)
        assert(!hasPreAgg,
          "Should skip optimization for expression-based distinct")
        // Verify correctness
        checkAnswer(df, expected.toSeq)
      }
    }
  }
}
