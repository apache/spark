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
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.window.{WindowExec, WindowGroupLimitExec}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, row_number, sum}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, MapType, StringType}

/**
 * SQL end-to-end tests for window stack reordering done by the `TransposeWindow` optimizer
 * rule, run through a full SparkSession. The logical-rule transformations themselves are
 * covered in [[org.apache.spark.sql.catalyst.optimizer.TransposeWindowSuite]].
 */
class TransposeWindowQuerySuite extends QueryTest with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  // The key moduli are pairwise coprime, so no key is a function of another: (k1, k3)
  // groups rows differently from (k1), and a reorder that dropped a partition key or
  // swapped two windows' outputs changes the answers the tests compare. `u` is a column no
  // window partitions by, so ordering by it gives a frame that varies row to row instead
  // of a key-ordered window whose value is the whole partition.
  private def withInput(f: => Unit): Unit = {
    withTempView("t") {
      spark.range(1000).selectExpr(
        "cast(id % 3 as string) k1", "cast(id % 5 as string) k2",
        "cast(id % 7 as string) k3", "cast(id % 11 as string) k4", "id v",
        "cast(id % 13 as int) u")
        .createOrReplaceTempView("t")
      f
    }
  }

  // AQE-aware counters: under AQE the plan's top is an AdaptiveSparkPlanExec and each
  // stage boundary is a QueryStageExec, and both are leaf wrappers, so a plain collect
  // on the executed plan sees none of the operators below them. The
  // AdaptiveSparkPlanHelper walk counts through the wrappers, and behaves identically
  // to a plain collect when AQE is disabled. distinct guards against counting an
  // exchange reachable through more than one query stage twice.
  private def numExchanges(df: DataFrame): Int =
    collect(df.queryExecution.executedPlan) {
      case s: ShuffleExchangeExec => s
    }.distinct.size

  private def numWindows(df: DataFrame): Int =
    collect(df.queryExecution.executedPlan) { case w: WindowExec => w }.size

  private def numSorts(df: DataFrame): Int =
    collect(df.queryExecution.executedPlan) { case s: SortExec => s }.size

  /** Plans the DataFrame eagerly so the plan is fixed under the current withSQLConf scope. */
  private def planned(df: DataFrame): DataFrame = {
    df.queryExecution.executedPlan
    df
  }

  /**
   * Six windows over three interleaved specs, two orderings each: (k1, k2) and (k1, k4) are
   * the minimal specs, so reordering targets 2 exchanges, and the (k1, k2, k3) windows ride
   * the (k1, k2) exchange. Distinct order specs keep CollapseWindow from merging the
   * same-spec windows. Used by the grouping, AQE, distinguishability and multi-child
   * tests.
   */
  private def stackedOn(from: String): String =
    s"""
      |SELECT k1, k2, k3, k4, v,
      |  sum(v) OVER (PARTITION BY k1, k2 ORDER BY v) AS f1,
      |  sum(v) OVER (PARTITION BY k1, k2, k3 ORDER BY v) AS p1,
      |  sum(v) OVER (PARTITION BY k1, k4 ORDER BY v) AS s1,
      |  sum(v) OVER (PARTITION BY k1, k2 ORDER BY u) AS f2,
      |  sum(v) OVER (PARTITION BY k1, k2, k3 ORDER BY u) AS p2,
      |  sum(v) OVER (PARTITION BY k1, k4 ORDER BY u) AS s2
      |FROM $from
    """.stripMargin

  private val stacked = stackedOn("t")

  test("stacked windows are regrouped to minimize exchanges") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withInput {
        val actualDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = sql(stacked)
          assert(numWindows(df) == 6)
          assert(numExchanges(df) == 2)
          planned(df)
        }
        // The reordered plan must produce the same result as the default (reorder off)
        // plan. The queries have no outer ORDER BY, so `checkAnswer` compares the answers
        // without making physical row order part of the contract.
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          val df = sql(stacked)
          // Reorder-off control for the counts above: source order keeps all six
          // windows and pays four exchanges. The `stacked` comment below cites
          // this 4-against-2 pair.
          assert(numWindows(df) == 6)
          assert(numExchanges(df) == 4)
          df.collect()
        }
        checkAnswer(actualDf, expected)
      }
    }
  }

  test("the compared window answers stay distinguishable") {
    // `checkAnswer` is the only check in this suite that would notice a mis-wired reorder --
    // the counters see operator counts, not column values -- so it only guards anything
    // while the compared columns differ. Two fixture properties flatten entire columns into
    // equal answers and quietly blind it: a key computed from another key ((k1, k3) then
    // groups rows like (k1), so p1 would repeat f1), and an ORDER BY over a window's own
    // partition key (every row of the partition is a peer, so the value is the partition
    // total and f2 would repeat f1). Both are asserted here so that a fixture change cannot
    // silently retire the answer comparison.
    withInput {
      val rows = sql(stacked).collect().toSeq
      val names = Seq("f1", "p1", "s1", "f2", "p2", "s2")
      val columns = names.map(name => rows.map(_.getAs[Any](name)))
      names.indices.foreach { i =>
        names.indices.filter(_ > i).foreach { j =>
          assert(columns(i) != columns(j),
            s"${names(i)} and ${names(j)} agree on every row; a reorder that swapped them " +
              "would not change the compared answers")
        }
      }
    }
  }

  test("windows sharing a partition spec share one exchange but keep their sorts") {
    val query =
      """
        |SELECT k1, k2, v,
        |  sum(v) OVER (PARTITION BY k1, k2 ORDER BY v) AS a,
        |  sum(v) OVER (PARTITION BY k1, k2 ORDER BY u) AS b
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
    // any order, and the two v-ordered ones come from different scopes (the analyzer groups
    // only same-spec windows within one select list). The original order (v, u, v) pays
    // one sort per window; moving the two v-ordered windows next to each other lets the
    // second ride the first one's sort, for one fewer (and CollapseWindow then also merges
    // the two equal-spec windows in a later iteration). `checkAnswer` compares the
    // answers without making the physical row order of the unordered queries part of
    // the contract.
    val query =
      """
        |SELECT a1, a2, sum(v) OVER (PARTITION BY k1, k2 ORDER BY v) AS s1
        |FROM (
        |  SELECT k1, k2, v,
        |    sum(v) OVER (PARTITION BY k1, k2 ORDER BY v) AS a1,
        |    sum(v) OVER (PARTITION BY k1, k2 ORDER BY u) AS a2
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

  test("a window rides a longer sort its required ordering is a prefix of") {
    // Bottom-to-top: m PARTITION BY k3 ORDER BY v, l PARTITION BY k1 ORDER BY k3, v, k2,
    // x PARTITION BY k1, k3 ORDER BY v. x can ride both minimal specs, [k3] and [k1], and
    // its required ordering [k1, k3, v] is a proper prefix of l's [k1, k3, v, k2], so it
    // joins l's group and reuses l's sort: 2 exchanges and 2 sorts. Joining the [k3]
    // group instead would emit m, x, l, forcing a third sort for x over hash(k3).
    // `checkAnswer` compares the answers without making the physical row order of the
    // unordered queries part of the contract.
    val query =
      """
        |SELECT k1, k2, k3, v,
        |  sum(v) OVER (PARTITION BY k3 ORDER BY v) AS m,
        |  sum(v) OVER (PARTITION BY k1 ORDER BY k3, v, k2) AS l,
        |  sum(v) OVER (PARTITION BY k1, k3 ORDER BY v) AS x
        |FROM t
      """.stripMargin

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withInput {
        val actualDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = sql(query)
          assert(numWindows(df) == 3)
          assert(numExchanges(df) == 2)
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

  test("a prefix rider moves beside its provider across a burying window") {
    // Bottom-to-top: m PARTITION BY k3 ORDER BY v, l PARTITION BY k1 ORDER BY k3, v, k4,
    // b PARTITION BY k1, k2 ORDER BY k4, x PARTITION BY k1, k3 ORDER BY v. b is not a
    // rider of l ([k1, k2, k4] is not a prefix of [k1, k3, v, k4]), so in the input
    // order b's sort buries l's ordering under x and x -- whose required ordering
    // [k1, k3, v] is a proper prefix of l's -- pays its own sort: 4 sorts, and no
    // pairwise swap applies. The reorder pulls x directly above l, one sort less.
    // `checkAnswer` compares the answers without making the physical row order of the
    // unordered queries part of the contract.
    val query =
      """
        |SELECT k1, k2, k3, k4, v,
        |  sum(v) OVER (PARTITION BY k3 ORDER BY v) AS m,
        |  sum(v) OVER (PARTITION BY k1 ORDER BY k3, v, k4) AS l,
        |  sum(v) OVER (PARTITION BY k1, k2 ORDER BY k4) AS b,
        |  sum(v) OVER (PARTITION BY k1, k3 ORDER BY v) AS x
        |FROM t
      """.stripMargin

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withInput {
        val actualDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = sql(query)
          assert(numWindows(df) == 4)
          assert(numExchanges(df) == 2)
          assert(numSorts(df) == 3)
          planned(df)
        }
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          val df = sql(query)
          assert(numWindows(df) == 4)
          assert(numExchanges(df) == 2)
          assert(numSorts(df) == 4)
          df.collect()
        }
        checkAnswer(actualDf, expected)
      }
    }
  }

  test("a maximal candidate leads to save a sort") {
    // Bottom-to-top: l2 PARTITION BY k1 ORDER BY k2, v, l1 PARTITION BY k1 ORDER BY
    // k3, w PARTITION BY k1, k2 ORDER BY v, k3. l2 is the first [k1] candidate but
    // not maximal: w extends its ordering [k1, k2, v]. No pairwise swap applies, so
    // the default plan keeps l2 at the bottom and its sort [k1, k2, v] is stranded
    // under l1's incompatible [k1, k3] (EnsureRequirements cannot push w's sort below
    // l1): 3 sorts. The maximal candidate l1 leads and w's chain follows with l2
    // riding its sort: 2 sorts.
    // `checkAnswer` compares the answers without making the physical row order of the
    // unordered queries part of the contract.
    val query =
      """
        |SELECT k1, k2, v,
        |  sum(v) OVER (PARTITION BY k1 ORDER BY k2, v) AS l2,
        |  sum(v) OVER (PARTITION BY k1 ORDER BY k3) AS l1,
        |  sum(v) OVER (PARTITION BY k1, k2 ORDER BY v, k3) AS w
        |FROM t
      """.stripMargin

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withInput {
        val actualDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = sql(query)
          assert(numWindows(df) == 3)
          assert(numExchanges(df) == 1)
          assert(numSorts(df) == 2)
          planned(df)
        }
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          val df = sql(query)
          assert(numWindows(df) == 3)
          assert(numExchanges(df) == 1)
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
    // All three windows ride the single minimal spec [k1, k2], so the unpinned reorder
    // (f1, p1, rn bottom-to-top) already keeps rn on top riding f1's exchange: no pin
    // is needed. The reorder inserts a pass-through restoring project at the chain top,
    // and PushDownPredicates moves the filter below it, restoring the Filter-over-Window
    // shape InferWindowGroupLimit needs.
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

  test("rank filter below the other windows still gets a window group limit") {
    // The rank window is listed first, so it is the chain's bottom window and the filter
    // cannot keep it in place by pinning it. The reorder is made at the filter instead: the
    // chain is reordered with rn on top and the restoring project goes above the filter, so
    // InferWindowGroupLimit still matches. Before that rewrite this shape lost the group
    // limit that the disabled rule keeps through its pairwise swaps.
    val query =
      """
        |SELECT * FROM (
        |  SELECT k1, k2, k3, k4, v,
        |    row_number() OVER (PARTITION BY k1, k2, k3 ORDER BY v) AS rn,
        |    sum(v) OVER (PARTITION BY k1, k2 ORDER BY k1) AS f1,
        |    sum(v) OVER (PARTITION BY k1 ORDER BY k1) AS f2
        |  FROM t
        |) WHERE rn <= 1
      """.stripMargin

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withInput {
        val (actualDf, actualExchanges, actualGroupLimits) =
          withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
            val df = sql(query)
            (planned(df), numExchanges(df), collect(df.queryExecution.executedPlan) {
              case _: WindowGroupLimitExec => ()
            }.size)
          }
        val (expectedDf, expectedExchanges, expectedGroupLimits) =
          withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
            val df = sql(query)
            (planned(df), numExchanges(df), collect(df.queryExecution.executedPlan) {
              case _: WindowGroupLimitExec => ()
            }.size)
          }
        // The rewrite survives, and it does not cost a shuffle: that trade is what the flag's
        // doc promises, so pin both sides of it.
        assert(expectedGroupLimits == 1)
        assert(actualGroupLimits == 1)
        assert(actualExchanges == expectedExchanges)
        // A window appends its columns, so the reorder must not change the query's column
        // order: checkAnswer compares rows positionally.
        checkAnswer(actualDf, expectedDf)
      }
    }
  }

  test("rank filter with a computed top project still gets a window group limit") {
    // The derived alias n is computed by the subquery project above the row_number
    // window. PushDownPredicates runs before TransposeWindow in the operator batch and
    // pushes both conjuncts below the project (n rewrites to v + k1), so the reorder
    // sees the filter directly on the window stack; as in the previous test the unpinned
    // order keeps rn on top riding f1's exchange, so no pin is needed.
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

  test("pinned reorder keeps WindowGroupLimit by rejecting a mergeable under-pin tail") {
    // x and a share rn's [k1, k2] spec, s is incomparable. Pinning ties the unpinned order
    // at 2 exchanges, so the pin is kept, and the last emitted member of the [k1, k2]
    // group is a, which would merge into rn (same spec and order) and defeat
    // InferWindowGroupLimit: the pin must put the safe x there instead. Ordering by v as
    // well keeps row_number's stay/leave choices deterministic across the two plans.
    def rankFiltered: DataFrame = {
      val t = spark.table("t")
      val wX = Window.partitionBy("k1", "k2").orderBy("k3")
      val wA = Window.partitionBy("k1", "k2").orderBy("k1", "v")
      val wS = Window.partitionBy("k1", "k4").orderBy("k1")
      val wRn = Window.partitionBy("k1", "k2").orderBy("k1", "v")
      t.withColumn("x", sum("v").over(wX))
        .withColumn("a", sum("v").over(wA))
        .withColumn("s", sum("v").over(wS))
        .withColumn("rn", row_number().over(wRn))
        .where(col("rn") <= 1)
    }

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withInput {
        val actualDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = rankFiltered
          assert(numExchanges(df) == 2)
          assert(numSorts(df) == 4)
          assert(df.queryExecution.executedPlan.collect {
            case _: WindowGroupLimitExec => ()
          }.nonEmpty)
          planned(df)
        }
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          val df = rankFiltered
          assert(numExchanges(df) == 3)
          assert(numSorts(df) == 5)
          df.collect()
        }
        checkAnswer(actualDf, expected)
      }
    }
  }

  test("rank filter still pins across a derived-alias cut") {
    // The middle project's derived alias cuts the chain, so the rank window is a lone
    // window above the cut. Both orders the pin check compares are that one window, so
    // the check cannot see the swap the disabled rule makes across the cut, and the pin
    // is kept: rn pays an exchange of its own on top of s's -- 2 exchanges against the
    // disabled rule's 1 -- and keeps the group limit. This is the trade the conf doc
    // warns about; if the check learns to price cross-cut swaps, this test flips.
    val query =
      """
        |SELECT * FROM (
        |  SELECT *, rank() OVER (PARTITION BY k1, k2 ORDER BY v) AS rn
        |  FROM (
        |    SELECT *, v + u AS x FROM (
        |      SELECT *, sum(v) OVER (PARTITION BY k1, k2, k3 ORDER BY v) AS s FROM t))
        |) WHERE rn = 1
      """.stripMargin

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withInput {
        val actualDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = sql(query)
          assert(numExchanges(df) == 2)
          assert(df.queryExecution.executedPlan.collect {
            case _: WindowGroupLimitExec => ()
          }.nonEmpty)
          planned(df)
        }
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          val df = sql(query)
          assert(numExchanges(df) == 1)
          df.collect()
        }
        checkAnswer(actualDf, expected)
      }
    }
  }

  test("rank filter does not pin when the rank window leads a shared exchange") {
    // rn partitions by (k1, k2) and s by (k1, k2, k3), so one minimal spec keys a single
    // exchange and the unpinned order emits rn below s: the rank window keys the exchange
    // s rides. Pinned, rn keeps an exchange of its own on top of s's, one shuffle more
    // than the disabled rule's adjacent swap, while the group limit only saves
    // post-shuffle work, so the shared plan wins. The v tie-breaker makes rn's selected
    // row deterministic across the compared plans.
    val query =
      """
        |SELECT * FROM (
        |  SELECT k1, k2, k3, v,
        |    sum(v) OVER (PARTITION BY k1, k2, k3 ORDER BY v) AS s,
        |    row_number() OVER (PARTITION BY k1, k2 ORDER BY u, v) AS rn
        |  FROM t
        |) WHERE rn = 1
      """.stripMargin

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withInput {
        val actualDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = sql(query)
          assert(numExchanges(df) == 1)
          planned(df)
        }
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(actualDf, expected)
      }
    }
  }

  test("rank filter does not pin when the pinned order would pay for the share") {
    // The sum and rank windows share a spec but are separated by an incomparable window.
    // Unpinned, rn rides a's exchange for 2 exchanges in total. The pinned search cannot
    // put a's group last -- a would merge into rn (same spec and order), defeating
    // InferWindowGroupLimit -- so the pin would emit s last and give rn a third exchange.
    // The group limit only saves post-shuffle work, so the shared plan wins. Ordering by v
    // as well keeps row_number's stay/leave choices deterministic across the two plans.
    def rankFiltered: DataFrame = {
      val t = spark.table("t")
      val wA = Window.partitionBy("k1", "k2").orderBy("k1", "v")
      val wS = Window.partitionBy("k1", "k4").orderBy("k1")
      val wRn = Window.partitionBy("k1", "k2").orderBy("k1", "v")
      t.withColumn("a", sum("v").over(wA))
        .withColumn("s", sum("v").over(wS))
        .withColumn("rn", row_number().over(wRn))
        .where(col("rn") <= 1)
    }

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withInput {
        val actualDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = rankFiltered
          assert(numExchanges(df) == 2)
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

  test("rank filter does not pin when a same-spec sum lets the top window ride an exchange") {
    // withColumn builds separate Window operators that share partition/order with rn; SQL
    // analysis would have merged those into one Window. The exchange-optimal order lets rn
    // reuse sum a's exchange while staying on top, so it is intentionally not pinned even
    // though putting a directly below rn would merge the two in CollapseWindow, which
    // keeps the group limit from being inferred at all. The v tie-breaker makes the row
    // selected by rn deterministic across the two plans.
    def rankFiltered: DataFrame = {
      val t = spark.table("t")
      val wB = Window.partitionBy("k3").orderBy("k1")
      val wA = Window.partitionBy("k1").orderBy("k2", "v")
      val wC = Window.partitionBy("k3").orderBy("k2")
      val wRn = Window.partitionBy("k1").orderBy("k2", "v")
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
          // The reorder pays two exchanges here, not four: that saving is what buys the
          // group limit this test gives up.
          assert(numExchanges(df) == 2)
          assert(df.queryExecution.executedPlan.collect {
            case _: WindowGroupLimitExec => ()
          }.isEmpty)
          planned(df)
        }
        // The other side of the tradeoff: the un-reordered stack pays one exchange per
        // window (4) but keeps the group limit -- two WindowGroupLimitExec nodes, since
        // the planner splits the logical WindowGroupLimit into Partial below the rank
        // window's exchange and Final above it.
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          val df = rankFiltered
          val rows = df.collect()
          assert(numExchanges(df) == 4)
          assert(df.queryExecution.executedPlan.collect {
            case _: WindowGroupLimitExec => ()
          }.size == 2)
          rows
        }
        checkAnswer(actualDf, expected)
      }
    }
  }

  test("rank filter does not pin when safe tails still cost an exchange") {
    // withColumn builds separate Window operators connected by transparent projects. Only
    // tails in rn's own group are defeated: r's emitted order packs its group below it
    // (x, y, s, r), and CollapseWindow merges bottom-up -- the same-spec RANGE sum s into
    // r, then into rn. The x and y tails are safe because their different specs block a
    // merge into rn, while s and r merge only with each other. Neither group is ridable,
    // so the last safe order is s, r, x, y, rn: four exchanges against the unpinned
    // order's three, and pinPaysAnExchange skips the pin. Ordering wF by a partition key
    // alone would leave every row of a partition a peer, so `row_number` would pick an
    // arbitrary staying row and the two plans could keep different rows; v breaks the tie
    // deterministically.
    def rankFiltered: DataFrame = {
      val t = spark.table("t")
      val wF = Window.partitionBy("k1", "k2").orderBy("k1", "v")
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
          assert(numExchanges(df) == 3)
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

  test("map output stays above the other group's exchange") {
    // Same stack as the collect_list test with the wide window typed as a bare MapType:
    // first_value over a map output reaches hasWideOutput's MapType arm, which no
    // collect_list test exercises. (k1) and (k2) key the two minimal exchanges; the map
    // window on (k1, k3) rides only the (k1) exchange, and first appearance would move
    // it below the (k2) exchange and ship each partition's map through that shuffle, so
    // the wide group must rank last instead. ORDER BY v is unique, so the maps are
    // deterministic across the compared plans.
    val query =
      """
        |SELECT k1, k2, k3, v,
        |  sum(v) OVER (PARTITION BY k1 ORDER BY v) AS s1,
        |  sum(v) OVER (PARTITION BY k2 ORDER BY v) AS s4,
        |  first_value(map(k1, v)) OVER (PARTITION BY k1, k3 ORDER BY v) AS m
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
          assert(topWindow.windowExpression.exists(_.dataType.isInstanceOf[MapType]))
          planned(df)
        }
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(actualDf, expected)
      }
    }
  }

  test("listagg output stays above the other group's exchange") {
    // `listagg` returns the aggregated string type rather than a collection, yet its payload
    // grows with the frame just like collect_list's array. (k1) and (k2) key the two
    // minimal exchanges; the listagg window on (k1, k3) rides only the (k1) exchange, so
    // first appearance would move it below the (k2) exchange and ship every row's running
    // string through that shuffle. ORDER BY v is unique, so the strings are deterministic
    // across the compared plans.
    val query =
      """
        |SELECT k1, k2, k3, v,
        |  sum(v) OVER (PARTITION BY k1 ORDER BY v) AS s1,
        |  sum(v) OVER (PARTITION BY k2 ORDER BY v) AS s4,
        |  listagg(k1, ',') OVER (PARTITION BY k1, k3 ORDER BY v) AS l
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
          assert(topWindow.windowExpression.exists(_.dataType.isInstanceOf[StringType]))
          planned(df)
        }
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          sql(query).collect()
        }
        checkAnswer(actualDf, expected)
      }
    }
  }

  test("rank filter does not pin with an empty-order tail when pinning costs an exchange") {
    // Empty-order variant of "rank filter does not pin when safe tails still cost an
    // exchange": r has no ORDER BY, so it cannot merge into rn on its own (an empty-order
    // child merging into an ordered parent is gated off by default), yet packed above the
    // same-spec ordered sum s it inherits s's ORDER BY when they merge, and the cascade
    // into rn defeats InferWindowGroupLimit all the same. The x and y tails remain safe,
    // so the pinned order is again s, r, x, y, rn: four exchanges against the unpinned
    // order's three, and the pin is skipped.
    def rankFiltered: DataFrame = {
      val t = spark.table("t")
      val wF = Window.partitionBy("k1", "k2").orderBy("k1", "v")
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
          assert(numExchanges(df) == 3)
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

  // One `stacked` branch narrowed to `v < bound`, with the filter below the windows so the
  // two branches' or sides' exchange subtrees differ: an identical stack would let
  // `ReuseExchange` share one side's exchanges with the other and hide which side was
  // regrouped. Each stack pays four exchanges in source order and two when regrouped, as
  // the single-stack test above pins (4 against 2 on `stacked`).
  private def stackedBranch(bound: Int): String =
    stackedOn(s"(SELECT * FROM t WHERE v < $bound)")

  test("window chains under both union branches are regrouped") {
    // A multi-child parent rewrites every child in one application: each branch's stack is
    // regrouped, not just the first one's, so the plan pays four exchanges, not eight.
    val query =
      s"SELECT * FROM (${stackedBranch(900)}) UNION ALL SELECT * FROM (${stackedBranch(910)})"

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withInput {
        val actualDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = sql(query)
          assert(numExchanges(df) == 4)
          planned(df)
        }
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          val df = sql(query)
          assert(numExchanges(df) == 8)
          df.collect()
        }
        checkAnswer(actualDf, expected)
      }
    }
  }

  test("window chains under both join sides are regrouped") {
    // The same two stacks on both sides of a join. The join's own broadcast is not a
    // shuffle, so the counts are the two stacks' own: four exchanges with the reorder,
    // eight without.
    val query =
      s"SELECT * FROM (${stackedBranch(50)}) l JOIN (${stackedBranch(60)}) r " +
        "ON l.k1 = r.k1 AND l.k2 = r.k2"

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withInput {
        val actualDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = sql(query)
          assert(numExchanges(df) == 4)
          planned(df)
        }
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          val df = sql(query)
          assert(numExchanges(df) == 8)
          df.collect()
        }
        checkAnswer(actualDf, expected)
      }
    }
  }

  test("a stack the pin would reorder to a riding order reorders once") {
    // sum m and rank rn share [k1, k2] with different orderings, and the x sum's longer
    // (k1, v) ordering separates them from the incomparable [k3] sum. The pinned order puts
    // rn on top of x's exchange, so pinning buys no lower-exchange plan: the rule emits that
    // stack's own order instead, reaching its final plan in one application. That order
    // packs m directly under rn, so CollapseWindow merges the two and the merged window's
    // RANGE-frame sum keeps InferWindowGroupLimit from firing -- the trade the skip makes:
    // one window and one exchange fewer, no group limit, and the un-reordered answers.
    def query: DataFrame = {
      val t = spark.table("t")
      t.withColumn("m", sum("v").over(Window.partitionBy("k1", "k2").orderBy("k1")))
        .withColumn("o", sum("v").over(Window.partitionBy("k3").orderBy("k1")))
        .withColumn("x", sum("v").over(Window.partitionBy("k1", "k2").orderBy("k1", "v")))
        .withColumn("rn", row_number().over(Window.partitionBy("k1", "k2").orderBy("k1")))
        .where(col("rn") <= 1)
    }

    def groupLimits(df: DataFrame): Int =
      df.queryExecution.executedPlan.collect { case _: WindowGroupLimitExec => () }.size

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withInput {
        val actualDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = query
          assert(numExchanges(df) == 2)
          assert(numWindows(df) == 3)
          assert(groupLimits(df) == 0)
          planned(df)
        }
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          val df = query
          assert(numExchanges(df) == 3)
          assert(numWindows(df) == 4)
          assert(groupLimits(df) > 0)
          df.collect()
        }
        checkAnswer(actualDf, expected)
      }
    }
  }

  test("filter-boundary reorder and output restore survive adaptive execution") {
    // rn starts below both sums, so the filter-boundary rewrite raises it and restores the
    // original output order above the filter. AQE re-optimization must retain the group limit
    // and the restored output.
    def rankFiltered: DataFrame = {
      val t = spark.table("t")
      val wRn = Window.partitionBy("k1", "k2", "k3").orderBy("k1")
      val wF = Window.partitionBy("k1", "k2").orderBy("k1")
      val wS = Window.partitionBy("k1").orderBy("k1")
      t.withColumn("rn", row_number().over(wRn))
        .withColumn("f", sum("v").over(wF))
        .withColumn("s", sum("v").over(wS))
        .where(col("rn") <= 1)
    }

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withInput {
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          rankFiltered.collect()
        }
        withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = rankFiltered
          assert(df.queryExecution.optimizedPlan.collect {
            case Project(_, _: Filter) => ()
          }.nonEmpty)
          df.collect()
          assert(collect(df.queryExecution.executedPlan) {
            case _: WindowGroupLimitExec => ()
          }.nonEmpty)
          checkAnswer(df, expected)
        }
      }
    }
  }

  test("reorder win and pinned WindowGroupLimit survive adaptive execution") {
    // Cover the exchange reduction and ordinary top-window pin under AQE; the preceding test
    // covers the filter-boundary path. Under AQE the final plan exists only once the query has
    // run, so each side is collected inside its conf scope before the counters read it. AQE
    // re-optimizes exchange widths from runtime statistics, but a distribution-required
    // exchange is never removed, so the distinct exchange count and the WindowGroupLimit
    // node stay assertable.
    // rn orders by v, unlike the same-spec sum a, so a cannot merge into rn: it is a
    // merge-safe tail the pin can put last, and pinning then costs no exchange (see the
    // AQE-off displacement test).
    def rankFiltered: DataFrame = {
      val t = spark.table("t")
      val wA = Window.partitionBy("k1", "k2").orderBy("k1")
      val wS = Window.partitionBy("k1", "k4").orderBy("k1")
      val wRn = Window.partitionBy("k1", "k2").orderBy("v")
      t.withColumn("a", sum("v").over(wA))
        .withColumn("s", sum("v").over(wS))
        .withColumn("rn", row_number().over(wRn))
        .where(col("rn") <= 1)
    }

    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withInput {
        // Baseline: the un-reordered plan under AQE.
        val expected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          val df = sql(stacked)
          val rows = df.collect()
          assert(numExchanges(df) == 4)
          rows
        }

        // The reordered plan keeps its exchange win under AQE.
        val actualDf = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = sql(stacked)
          df.collect()
          assert(numExchanges(df) == 2)
          df
        }
        checkAnswer(actualDf, expected)

        // The pin keeps the rank window's WindowGroupLimit under AQE, and its rows match
        // the un-reordered plan's.
        val pinnedExpected = withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "false") {
          rankFiltered.collect()
        }
        withSQLConf(SQLConf.WINDOW_REORDER_ENABLED.key -> "true") {
          val df = rankFiltered
          df.collect()
          assert(collect(df.queryExecution.executedPlan) {
            case _: WindowGroupLimitExec => ()
          }.nonEmpty)
          checkAnswer(df, pinnedExpected)
        }
      }
    }
  }
}
