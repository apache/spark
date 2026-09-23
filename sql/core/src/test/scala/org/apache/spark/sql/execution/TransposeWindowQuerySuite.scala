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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.QueryTest
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
        val actualDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = sql(query)
          assert(numWindows(df) == 6)
          assert(numExchanges(df) == 2)
          df
        }
        // The reordered plan must produce the same result as the default (reorder off)
        // plan. The queries have no outer ORDER BY, so `checkAnswer` compares the answers
        // without making physical row order part of the contract.
        val expectedDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          sql(query)
        }
        checkAnswer(actualDf, expectedDf)
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
        val actualDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = sql(query)
          assert(numWindows(df) == 2)
          assert(numExchanges(df) == 1)
          assert(numSorts(df) == 2)
          df
        }
        val expectedDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          sql(query)
        }
        checkAnswer(actualDf, expectedDf)
      }
    }
  }

  test("windows with equal order specs become adjacent to share one sort") {
    // The three windows share the partition spec (k1, k2), so they all ride one exchange in
    // any order, and the two k1-ordered ones come from different scopes (the analyzer groups
    // only same-spec windows within one select list). The original order (k1, k2, k1) pays
    // one sort per window; moving the two k1-ordered windows next to each other lets the
    // second ride the first one's sort, for one fewer (and CollapseWindow then also merges
    // the two equal-spec windows in a later iteration). `checkAnswer` compares the
    // answers without making the physical row order of the unordered queries part of
    // the contract.
    val query =
      """
        |SELECT a1, a2, sum(v) OVER (PARTITION BY k1, k2 ORDER BY k1) AS s1
        |FROM (
        |  SELECT k1, k2, v,
        |    sum(v) OVER (PARTITION BY k1, k2 ORDER BY k1) AS a1,
        |    sum(v) OVER (PARTITION BY k1, k2 ORDER BY k2) AS a2
        |  FROM t
        |)
      """.stripMargin

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withInput {
        val actualDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = sql(query)
          assert(numWindows(df) == 2)
          assert(numExchanges(df) == 1)
          assert(numSorts(df) == 2)
          df
        }
        val expectedDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          val df = sql(query)
          // Without reordering the equal-order windows are separated and each pays a sort.
          assert(numWindows(df) == 3)
          assert(numSorts(df) == 3)
          df
        }
        checkAnswer(actualDf, expectedDf)
      }
    }
  }

  test("window with an empty order spec and a relative frame blocks reordering") {
    // The running sum has no ORDER BY, so its ROWS ... CURRENT ROW frame covers whatever
    // row prefix the operator receives; reordering the chain would change that order and
    // hence the results. The rule must skip the chain, so the config-on results stay
    // identical to the config-off ones. The partition specs are pairwise incomparable, so
    // the disabled adjacent-pair transposition also leaves the plan untouched and the
    // config-off plan is a valid reference. `checkAnswer` compares the answers without
    // making the physical row order of the unordered query part of the contract.
    val query =
      """
        |SELECT k1, k2, k3, k4, v,
        |  sum(v) OVER (PARTITION BY k1, k2 ORDER BY k1) AS a1,
        |  sum(v) OVER (PARTITION BY k1, k3 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r1,
        |  sum(v) OVER (PARTITION BY k1, k4 ORDER BY k1) AS s1,
        |  sum(v) OVER (PARTITION BY k1, k2 ORDER BY k2) AS a2
        |FROM t
      """.stripMargin

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withInput {
        val (actualDf, onPlan) = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = sql(query)
          assert(numWindows(df) == 4)
          (df, df.queryExecution.executedPlan)
        }
        val (expectedDf, offPlan) = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          val df = sql(query)
          (df, df.queryExecution.executedPlan)
        }
        // The enabled rule must skip the chain, leaving the plan shape identical to the
        // disabled one; `sameResult` normalizes the expression ids of the two runs.
        assert(onPlan.sameResult(offPlan))
        checkAnswer(actualDf, expectedDf)
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
        val actualDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = sql(query)
          assert(df.queryExecution.executedPlan.collect {
            case _: WindowGroupLimitExec => ()
          }.nonEmpty)
          assert(numExchanges(df) == 1)
          df
        }
        val expectedDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          sql(query)
        }
        checkAnswer(actualDf, expectedDf)
      }
    }
  }
}
