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

package org.apache.spark.sql.execution

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.window.{WindowExec, WindowGroupLimitExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * SQL end-to-end tests for the exchange-minimizing window stack reordering done by
 * the `TransposeWindow` optimizer rule, run through a full SparkSession. The
 * logical-rule transformations themselves are covered in
 * [[org.apache.spark.sql.catalyst.optimizer.TransposeWindowSuite]].
 */
class TransposeWindowQuerySuite extends QueryTest with SharedSparkSession {

  private def withInput(f: => Unit): Unit = {
    withTempView("t") {
      spark.range(1000).selectExpr(
        "cast(id % 10 as string) k1", "cast(id % 7 as string) k2",
        "cast(id % 5 as string) k3", "cast(id % 3 as string) k4", "id v")
        .createOrReplaceTempView("t")
      f
    }
  }

  private def numExchanges(df: DataFrame): Int =
    df.queryExecution.executedPlan.collect { case _: ShuffleExchangeExec => () }.size

  private def numWindows(df: DataFrame): Int =
    df.queryExecution.executedPlan.collect { case _: WindowExec => () }.size

  private def numSorts(df: DataFrame): Int =
    df.queryExecution.executedPlan.collect { case _: SortExec => () }.size

  test("stacked windows are regrouped to minimize exchanges") {
    // Partition specs (k1, k2), (k1, k2, k3) and (k1, k4) interleaved in select-list order;
    // (k1, k2) and (k1, k4) are the minimal specs, so 2 exchanges are optimal. Distinct
    // order specs keep CollapseWindow from merging the same-spec windows.
    val query =
      """
        |SELECT k1, k2, k3, k4, v,
        |  sum(v) OVER (PARTITION BY k1, k2 ORDER BY k1) AS f1,
        |  sum(v) OVER (PARTITION BY k1, k2, k3 ORDER BY k1) AS p1,
        |  sum(v) OVER (PARTITION BY k1, k4 ORDER BY k1) AS s1,
        |  sum(v) OVER (PARTITION BY k1, k2 ORDER BY k2) AS f2,
        |  sum(v) OVER (PARTITION BY k1, k2, k3 ORDER BY k2) AS p2,
        |  sum(v) OVER (PARTITION BY k1, k4 ORDER BY k2) AS s2
        |FROM t
      """.stripMargin

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withInput {
        val actual = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = sql(query)
          assert(numWindows(df) == 6)
          assert(numExchanges(df) == 2)
          df.collect().toSeq
        }
        // The reordered plan must produce the same result as the default (reorder off) plan.
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          sql(query).collect().toSeq
        }
        assert(actual == expected)
      }
    }
  }

  test("windows sharing a partition spec share one exchange but keep their sorts") {
    val query =
      """
        |SELECT k1, k2, v,
        |  sum(v) OVER (PARTITION BY k1, k2 ORDER BY k1) AS a,
        |  sum(v) OVER (PARTITION BY k1, k2 ORDER BY k2) AS b
        |FROM t
      """.stripMargin

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withInput {
        val actual = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = sql(query)
          assert(numWindows(df) == 2)
          assert(numExchanges(df) == 1)
          assert(numSorts(df) == 2)
          df.collect().toSeq
        }
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          sql(query).collect().toSeq
        }
        assert(actual == expected)
      }
    }
  }

  test("rank filter over a reordered chain still gets a window group limit") {
    // The row_number window is pinned on top for InferWindowGroupLimit; the two windows
    // below are reordered so that the whole chain rides a single exchange.
    val query =
      """
        |SELECT * FROM (
        |  SELECT k1, k2, k3, k4, v,
        |    sum(v) OVER (PARTITION BY k1, k2, k3 ORDER BY k1) AS p1,
        |    sum(v) OVER (PARTITION BY k1, k2 ORDER BY k1) AS f1,
        |    row_number() OVER (PARTITION BY k1, k2, k3 ORDER BY v) AS rn
        |  FROM t
        |) WHERE rn <= 1
      """.stripMargin

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withInput {
        val actual = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = sql(query)
          assert(df.queryExecution.executedPlan.collect {
            case _: WindowGroupLimitExec => ()
          }.nonEmpty)
          assert(numExchanges(df) == 1)
          df.collect().toSeq
        }
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          sql(query).collect().toSeq
        }
        assert(actual == expected)
      }
    }
  }
}
