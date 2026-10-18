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
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, row_number, sum}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.ArrayType

/**
 * SQL end-to-end tests for window stack reordering done by the `TransposeWindow` optimizer
 * rule, run through a full SparkSession. The logical-rule transformations themselves are
 * covered in [[org.apache.spark.sql.catalyst.optimizer.TransposeWindowSuite]].
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

  /** Plans the DataFrame eagerly so the plan is fixed under the current withSQLConf scope. */
  private def planned(df: DataFrame): DataFrame = {
    df.queryExecution.executedPlan
    df
  }

  test("stacked windows are regrouped to minimize exchanges") {
    // Partition specs (k1, k2), (k1, k2, k3) and (k1, k4) interleaved in select-list order;
    // (k1, k2) and (k1, k4) are the minimal specs, so reordering targets 2 exchanges on a
    // scan child. Distinct order specs keep CollapseWindow from merging the same-spec windows.
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
          planned(df)
        }
        // The reordered plan must produce the same result as the default (reorder off)
        // plan. The queries have no outer ORDER BY, so `checkAnswer` compares the answers
        // without making physical row order part of the contract.
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(actualDf, expected)
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
          planned(df)
        }
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(actualDf, expected)
      }
    }
  }

  test("window over a derived projection over a window still shares one exchange") {
    // The derived alias `d` cuts the window chain into two one-window runs, so there is
    // nothing to stack-reorder, but the legacy pairwise Window-Project-Window swap at the
    // cut must still fire with the flag on: the (k1) window moves below the (k1, k2)
    // window and its exchange satisfies the wider spec -- one exchange, as with the flag
    // off.
    val query =
      """
        |SELECT *, sum(v) OVER (PARTITION BY k1 ORDER BY k1) AS s
        |FROM (SELECT *, sum(v) OVER (PARTITION BY k1, k2 ORDER BY k1) * 2 AS d FROM t)
      """.stripMargin

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withInput {
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          val df = sql(query)
          assert(numExchanges(df) == 1)
          planned(df)
        }
        val actualDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = sql(query)
          assert(numWindows(df) == 2)
          assert(numExchanges(df) == 1)
          planned(df)
        }
        checkAnswer(actualDf, expected.collect())
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
          planned(df)
        }
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          val df = sql(query)
          // Without reordering the equal-order windows are separated and each pays a sort.
          assert(numWindows(df) == 3)
          assert(numSorts(df) == 3)
          df.collect()
        }
        checkAnswer(actualDf, expected)
      }
    }
  }

  test("window with an empty order spec and a relative frame blocks reordering") {
    // The running sum has no ORDER BY, so its ROWS ... CURRENT ROW frame covers whatever
    // row prefix the operator receives; a full stack permute would change that order and
    // hence the results, so `reorderable` rejects the chain. Every adjacent upper spec
    // is incomparable to the window below it, so the pairwise fallback (and flag-off)
    // leave the plan untouched; config-on results stay identical to config-off.
    // `checkAnswer` compares
    // the answers without making the physical row order of the unordered query part of
    // the contract.
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
        // Flag-on rejects the stack and the pairwise fallback is a no-op here, so the
        // plan shape matches flag-off; `sameResult` normalizes expression ids.
        assert(onPlan.sameResult(offPlan))
        checkAnswer(actualDf, expectedDf)
      }
    }
  }

  test("rank filter over a reordered chain still gets a window group limit") {
    // The row_number window is pinned on top for InferWindowGroupLimit. A merge-safe pin
    // refuses to put the same-spec RANGE sum directly under rn (that would lose the group
    // limit after CollapseWindow), so this asserts WGL rather than a specific exchange count.
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
          planned(df)
        }
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(actualDf, expected)
      }
    }
  }

  test("rank filter with a computed top project still gets a window group limit") {
    // The derived alias n is computed by the subquery project between the row_number
    // window and the outer rank filter. The rank conjunct pins the top window; predicate
    // pushdown later restores the Filter-over-Window shape for InferWindowGroupLimit.
    val query =
      """
        |SELECT * FROM (
        |  SELECT k1, k2, k3, k4, v, p1, f1, rn, v + k1 AS n
        |  FROM (
        |    SELECT k1, k2, k3, k4, v,
        |      sum(v) OVER (PARTITION BY k1, k2, k3 ORDER BY k1) AS p1,
        |      sum(v) OVER (PARTITION BY k1, k2 ORDER BY k1) AS f1,
        |      row_number() OVER (PARTITION BY k1, k2, k3 ORDER BY v) AS rn
        |    FROM t
        |  )
        |) WHERE rn <= 1 AND n < 100
    """.stripMargin

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withInput {
        val actualDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = sql(query)
          assert(df.queryExecution.executedPlan.collect {
            case _: WindowGroupLimitExec => ()
          }.nonEmpty)
          planned(df)
        }
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(actualDf, expected)
      }
    }
  }

  test("rank filter pins when unpinned order would displace the rank window") {
    // Unpinned order places rn under the incomparable (k1, k4) window; the pin must keep
    // rn on top so WindowGroupLimit still fires. Distinct ORDER BY on rn keeps analysis from
    // merging it into the (k1, k2) sum window.
    val query =
      """
        |SELECT * FROM (
        |  SELECT k1, k2, k3, k4, v,
        |    sum(v) OVER (PARTITION BY k1, k2 ORDER BY k1) AS f1,
        |    sum(v) OVER (PARTITION BY k1, k4 ORDER BY k1) AS s1,
        |    row_number() OVER (PARTITION BY k1, k2 ORDER BY v) AS rn
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
          planned(df)
        }
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(actualDf, expected)
      }
    }
  }

  test("rank filter with a long literal bound still gets a window group limit") {
    // DataFrame lit(1L) analyzes as CAST(rn AS BIGINT) <= 1L; the pin must normalize that
    // bound so rn is not buried under an incomparable RANGE sum window.
    def rankFiltered: DataFrame = {
      val base = sql(
        """
          |SELECT k1, k2, v,
          |  sum(v) OVER (PARTITION BY k1 ORDER BY v) AS a,
          |  sum(v) OVER (PARTITION BY k2 ORDER BY v) AS s,
          |  row_number() OVER (PARTITION BY k1 ORDER BY v DESC) AS rn
          |FROM t
        """.stripMargin)
      base.where(base("rn") <= lit(1L))
    }

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withInput {
        val actualDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = rankFiltered
          assert(df.queryExecution.executedPlan.collect {
            case _: WindowGroupLimitExec => ()
          }.nonEmpty)
          planned(df)
        }
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          rankFiltered.collect()
        }
        checkAnswer(actualDf, expected)
      }
    }
  }

  test("pinned reorder keeps WindowGroupLimit when a same-spec sum sits in the chain") {
    // The sum and rank windows share a spec but are separated by an incomparable window.
    // Unpinned reordering would place rn below that window, so the pin keeps rn on top.
    // It must also avoid putting the same-spec sum directly below rn, which would merge
    // into rn and defeat InferWindowGroupLimit.
    def rankFiltered: DataFrame = {
      val t = spark.table("t")
      val wA = Window.partitionBy("k1", "k2").orderBy("k1")
      val wS = Window.partitionBy("k1", "k4").orderBy("k1")
      val wRn = Window.partitionBy("k1", "k2").orderBy("k1")
      t.withColumn("a", sum("v").over(wA))
        .withColumn("s", sum("v").over(wS))
        .withColumn("rn", row_number().over(wRn))
        .where(col("rn") <= 1)
    }

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withInput {
        val actualDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = rankFiltered
          assert(df.queryExecution.executedPlan.collect {
            case _: WindowGroupLimitExec => ()
          }.nonEmpty)
          planned(df)
        }
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          rankFiltered.collect()
        }
        checkAnswer(actualDf, expected)
      }
    }
  }

  test("rank filter does not pin when a same-spec sum lets the top window ride an exchange") {
    // withColumn builds separate Window operators that share partition/order with rn; SQL
    // analysis would have merged those into one Window. The exchange-optimal order lets rn
    // reuse sum a's exchange while staying on top, so it is intentionally not pinned even
    // though putting a directly below rn makes CollapseWindow remove WindowGroupLimit.
    def rankFiltered: DataFrame = {
      val t = spark.table("t")
      val wB = Window.partitionBy("k3").orderBy("k1")
      val wA = Window.partitionBy("k1").orderBy("k2")
      val wC = Window.partitionBy("k3").orderBy("k2")
      val wRn = Window.partitionBy("k1").orderBy("k2")
      t.withColumn("b", sum("v").over(wB))
        .withColumn("a", sum("v").over(wA))
        .withColumn("c", sum("v").over(wC))
        .withColumn("rn", row_number().over(wRn))
        .where(col("rn") <= 1)
    }

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withInput {
        val actualDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = rankFiltered
          assert(df.queryExecution.executedPlan.collect {
            case _: WindowGroupLimitExec => ()
          }.isEmpty)
          planned(df)
        }
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          rankFiltered.collect()
        }
        checkAnswer(actualDf, expected)
      }
    }
  }

  test("pinned reorder survives a bottom-up merge cascade into the rank window") {
    // withColumn builds separate Window operators connected by transparent projects. r is
    // a merge-safe tail under rn in isolation, but the order it emits packs its group
    // below it (x, y, s, r), and CollapseWindow merges bottom-up -- the same-spec RANGE
    // sum s into r, then into rn -- defeating InferWindowGroupLimit. Merge safety must be
    // checked against the order a candidate tail would actually emit.
    def rankFiltered: DataFrame = {
      val t = spark.table("t")
      val wF = Window.partitionBy("k1", "k2").orderBy("k1")
      val wX = Window.partitionBy("k3").orderBy("k1")
      val wY = Window.partitionBy("k4").orderBy("k1")
      t.withColumn("s", sum("v").over(wF))
        .withColumn("x", sum("v").over(wX))
        .withColumn("r", row_number().over(wF))
        .withColumn("y", sum("v").over(wY))
        .withColumn("rn", row_number().over(wF))
        .where(col("rn") <= 1)
    }

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withInput {
        val actualDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = rankFiltered
          assert(df.queryExecution.executedPlan.collect {
            case _: WindowGroupLimitExec => ()
          }.nonEmpty)
          planned(df)
        }
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          rankFiltered.collect()
        }
        checkAnswer(actualDf, expected)
      }
    }
  }

  test("collect_list output stays above the other group's exchange") {
    // (k1) and (k2) key the two minimal exchanges; collect_list on (k1, k3) rides
    // only the (k1) exchange ((k2) is not a subset of its keys, so the (k2) group stays
    // narrow). First appearance would move it below the (k2) exchange and ship each
    // partition's array through that shuffle, so the wide group must rank last instead.
    // ORDER BY v is unique, so the arrays are deterministic across the compared plans.
    val query =
      """
        |SELECT k1, k2, k3, v,
        |  sum(v) OVER (PARTITION BY k1 ORDER BY v) AS s1,
        |  sum(v) OVER (PARTITION BY k2 ORDER BY v) AS s4,
        |  collect_list(v) OVER (PARTITION BY k1, k3 ORDER BY v) AS evs
        |FROM t
      """.stripMargin

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withInput {
        val actualDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = sql(query)
          assert(numWindows(df) == 3)
          assert(numExchanges(df) == 2)
          val topWindow = df.queryExecution.executedPlan.collect {
            case w: WindowExec => w
          }.head
          assert(topWindow.windowExpression.exists(_.dataType.isInstanceOf[ArrayType]))
          planned(df)
        }
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(actualDf, expected)
      }
    }
  }

  test("pinned reorder survives a merge cascade through an inherited order spec") {
    // Same stack, but r has no ORDER BY: it cannot merge into rn on its own (an
    // empty-order child merging into an ordered parent is gated off by default), yet
    // packed above the same-spec ordered sum s it inherits s's ORDER BY when they merge,
    // and the cascade into rn defeats InferWindowGroupLimit all the same.
    def rankFiltered: DataFrame = {
      val t = spark.table("t")
      val wF = Window.partitionBy("k1", "k2").orderBy("k1")
      val wR = Window.partitionBy("k1", "k2")
      val wX = Window.partitionBy("k3").orderBy("k1")
      val wY = Window.partitionBy("k4").orderBy("k1")
      t.withColumn("s", sum("v").over(wF))
        .withColumn("x", sum("v").over(wX))
        .withColumn("r", sum("v").over(wR))
        .withColumn("y", sum("v").over(wY))
        .withColumn("rn", row_number().over(wF))
        .where(col("rn") <= 1)
    }

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withInput {
        val actualDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = rankFiltered
          assert(df.queryExecution.executedPlan.collect {
            case _: WindowGroupLimitExec => ()
          }.nonEmpty)
          planned(df)
        }
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          rankFiltered.collect()
        }
        checkAnswer(actualDf, expected)
      }
    }
  }
}
