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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate.Partial
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for runtime adaptive partial aggregation
 * (see [[SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED]]). When a partial aggregate is not reducing
 * rows, the operator stops aggregating and streams the remaining rows through as single-row partial
 * buffers for the Final aggregate to merge. It must never change results.
 *
 * The suite has two halves:
 *   1. Correctness: output is identical to the reference (feature-off) run across the full matrix
 *      of codegen on/off, two-level map on/off, and spill/no-spill, over a range of aggregate
 *      shapes, key types, and `Expand`-bearing plans (ROLLUP / CUBE / GROUPING SETS /
 *      multi-distinct).
 *   2. Triggering: the `numBypassingRows` metric proves the bypass actually fires when (and only
 *      when) it should -- high-cardinality input bypasses, low-cardinality input keeps aggregating,
 *      the feature switch and eligibility rules are honored, and both check points work.
 */
class AdaptivePartialAggregationSuite extends QueryTest with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  import testImplicits._

  // A `testFallbackStartsAt` setting ("fastMapCounter, regularMapCounter") that makes the regular
  // map fall back (spill) periodically, exercising the spill-check decision path in both the
  // codegen and interpreted aggregation paths. Kept moderate so low-cardinality inputs (which are
  // never bypassed and therefore really spill) do not open an unbounded number of spill readers.
  private val forceSpillFallback = "4, 16"

  // The upstream `CombineAdjacentAggregation` and `ReplaceHashWithSortAgg` rules would change the
  // plan of these small single-partition queries away from a Partial+Final `HashAggregateExec`:
  // the former merges the two adjacent phases (no shuffle in between) into a single `Complete`
  // aggregate, and the latter converts a hash aggregate to a sort aggregate when the input is
  // already sorted by the grouping key (a `Range` over an ascending `id` key). The adaptive
  // feature lives in the partial hash aggregation, so both rules are disabled to keep that
  // structure in the tests.
  private val fixedPlanConfs = Seq(
    SQLConf.COMBINE_ADJACENT_AGGREGATION_ENABLED.key -> "false",
    SQLConf.REPLACE_HASH_WITH_SORT_AGG_ENABLED.key -> "false")

  /**
   * Runs `df` with adaptive partial aggregation disabled (the reference) and then across the full
   * configuration matrix with it enabled, asserting every enabled run matches the reference.
   */
  private def checkAdaptiveMatchesReference(build: () => DataFrame): Unit = {
    val reference = withSQLConf(
      (SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> "false") +: fixedPlanConfs: _*) {
      build().collect().toSeq
    }
    for {
      wholeStage <- Seq(true, false)
      twoLevelMap <- Seq(true, false)
      forceSpill <- Seq(true, false)
    } {
      val spillConf = if (forceSpill) {
        Seq("spark.sql.TungstenAggregate.testFallbackStartsAt" -> forceSpillFallback)
      } else {
        Nil
      }
      withSQLConf(
        (Seq(
          SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> "true",
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> wholeStage.toString,
          SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> twoLevelMap.toString,
          // Small sample so the no-spill (the periodic check) path triggers on modest inputs.
          SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_ROWS.key -> "8") ++
          spillConf ++ fixedPlanConfs): _*) {
        val msg = s"wholeStage=$wholeStage twoLevelMap=$twoLevelMap forceSpill=$forceSpill"
        withClue(msg) {
          checkAnswer(build(), reference)
        }
      }
    }
  }

  /**
   * The observable per-run counters we assert on, all read from the partial `HashAggregateExec` in
   * a single execution so the metrics are not double-counted:
   *   - `skipped`: our self-reported `numBypassingRows` metric.
   *   - `partialOutputRows`: the partial aggregate's own `numOutputRows`. An independent,
   *     pre-existing counter driven by the normal output path, so it is the ground truth for
   *     whether rows were streamed through -- it equals the distinct key count when aggregation is
   *     effective and climbs toward the input row count once the operator bypasses.
   *   - `spillBytes`: the partial aggregate's `spillSize`. Reliable only when no fallback is
   *     forced: on the interpreted path this is derived from the task-cumulative memory-spill
   *     counter, so a forced fallback (or downstream shuffle-write spill) can inflate it.
   *     Asserted only by the periodic check test, which forces no fallback; use
   *     `tasksFallBacked` otherwise.
   *   - `tasksFallBacked`: the partial aggregate's `numTasksFallBacked`, incremented only when the
   *     regular map actually falls back into sort-based aggregation. When the spill check bypasses
   *     at the spill boundary the sorter is never created, so this stays 0 -- direct, per-operator
   *     evidence the bypass replaced the sort fallback.
   */
  private case class AggCounters(
      skipped: Long,
      partialOutputRows: Long,
      spillBytes: Long,
      tasksFallBacked: Long)

  // Verifies `df` (an already-collected bypassing run) produces the same results as the feature-off
  // reference. `build` is re-run for the reference so it gets a genuinely non-adaptive plan rather
  // than reusing the bypassing run's cached one.
  private def checkAgainstReference(df: DataFrame, build: () => DataFrame): Unit = {
    val reference = withSQLConf(
      SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> "false") {
      build().collect().toSeq
    }
    checkAnswer(df, reference)
  }

  private def runAndReadCounters(build: () => DataFrame): AggCounters = {
    // The triggering tests assert on metrics, so also verify the bypassing run produces the same
    // results as the feature-off reference.
    val df = build()
    df.collect()
    val partialAggs = collect(df.queryExecution.executedPlan) {
      case agg: HashAggregateExec if agg.aggregateExpressions.forall(_.mode == Partial) => agg
    }
    // A partial aggregate is always present for the grouped queries these tests use.
    assert(partialAggs.nonEmpty, "expected a partial HashAggregateExec in the plan")
    val counters = AggCounters(
      skipped = partialAggs.map(_.metrics("numBypassingRows").value).sum,
      partialOutputRows = partialAggs.map(_.metrics("numOutputRows").value).sum,
      spillBytes = partialAggs.map(_.metrics("spillSize").value).sum,
      tasksFallBacked = partialAggs.map(_.metrics("numTasksFallBacked").value).sum)
    checkAgainstReference(df, build)
    counters
  }

  private def numBypassingRows(build: () => DataFrame): Long = runAndReadCounters(build).skipped

  // Returns the bypassed-row count per Partial-mode `HashAggregateExec`, keyed by the number of
  // grouping keys, and verifies the run matches the feature-off reference. A `count(DISTINCT ...)`
  // group-by has two such Partial phases -- the de-duplication partial (grouping on key + distinct
  // columns) and the distinct partial (grouping on the keys only) -- so their bypasses can be told
  // apart by the grouping key count.
  private def bypassRowsByGroupingKeyCount(build: () => DataFrame): Map[Int, Long] = {
    val df = build()
    df.collect()
    val byKeyCount = collect(df.queryExecution.executedPlan) {
      case agg: HashAggregateExec if agg.aggregateExpressions.forall(_.mode == Partial) =>
        agg.groupingExpressions.length -> agg.metrics("numBypassingRows").value
    }.groupBy(_._1).map { case (n, pairs) => n -> pairs.map(_._2).sum }
    checkAgainstReference(df, build)
    byKeyCount
  }

  /**
   * Runs `body` once per (wholeStage, twoLevelMap) combination with the feature enabled and a small
   * sample, threading a descriptive clue for failure messages.
   *
   * The fast (first-level) map is append-only and never spills; adaptive partial aggregation
   * governs only the regular (second-level) map. With the default fast-map capacity (2^16) a small
   * high-cardinality input would be fully absorbed by the fast map and never reach the regular map,
   * so nothing could ever bypass. To make the triggering tests meaningful when the two-level map is
   * on, we shrink the fast map via the first field of `testFallbackStartsAt` so rows fall through
   * to the regular map. `regularFallback` optionally sets the second field to also force the
   * regular map to spill (for the spill check); when 0 the regular map does not spill.
   */
  private def forEachCodegenAndMap(
      minRows: Long = 8,
      regularFallback: Int = 0,
      minCompaction: Double = -1.0)(
      body: String => Unit): Unit = {
    for {
      wholeStage <- Seq(true, false)
      twoLevelMap <- Seq(true, false)
    } {
      // Shrink the fast map to 4 keys when it is on so rows reach the regular map. The second field
      // controls regular-map spilling; 0 means "never" (a large sentinel).
      val fallbackConf = if (twoLevelMap || regularFallback > 0) {
        val fastCap = if (twoLevelMap) 4 else 1
        val regular = if (regularFallback > 0) regularFallback else Int.MaxValue
        Seq("spark.sql.TungstenAggregate.testFallbackStartsAt" -> s"$fastCap, $regular")
      } else {
        Nil
      }
      // A negative value means "leave the threshold at its default".
      val thresholdConf = if (minCompaction >= 0.0) {
        Seq(SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_COMPACTION.key -> minCompaction.toString)
      } else {
        Nil
      }
      withSQLConf(
        (Seq(
          SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> "true",
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> wholeStage.toString,
          SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> twoLevelMap.toString,
          SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_ROWS.key -> minRows.toString) ++
          fallbackConf ++ thresholdConf ++ fixedPlanConfs): _*) {
        body(s"wholeStage=$wholeStage twoLevelMap=$twoLevelMap")
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // Part 1: Correctness -- results identical to the feature-off reference.
  /////////////////////////////////////////////////////////////////////////////

  test("results unchanged for high-cardinality input that bypasses partial aggregation") {
    // Every grouping key is distinct, so partial aggregation reduces nothing and should be
    // bypassed by the periodic check.
    checkAdaptiveMatchesReference { () =>
      spark.range(0, 200, 1, 1)
        .select($"id" as "k", ($"id" * 2) as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s", count(lit(1)) as "c", max($"v") as "m")
    }
  }

  test("results unchanged for low-cardinality input that keeps partial aggregation") {
    // Few distinct keys, high reduction: partial aggregation is effective and should be kept.
    checkAdaptiveMatchesReference { () =>
      spark.range(0, 600, 1, 1)
        .select(($"id" % 5) as "k", $"id" as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s", count(lit(1)) as "c", min($"v") as "mn", max($"v") as "mx")
    }
  }

  test("results unchanged for medium-cardinality input near the reduction threshold") {
    // Roughly half the rows are distinct keys; exercises the boundary of the ratio checks.
    checkAdaptiveMatchesReference { () =>
      spark.range(0, 1000, 1, 1)
        .select(($"id" % 500) as "k", $"id" as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s", count(lit(1)) as "c")
    }
  }

  test("results unchanged with multiple grouping keys and string keys") {
    checkAdaptiveMatchesReference { () =>
      spark.range(0, 500, 1, 1)
        .select(
          concat(lit("g"), ($"id" % 300).cast("string")) as "k1",
          ($"id" % 7) as "k2",
          $"id" as "v")
        .groupBy($"k1", $"k2")
        .agg(sum($"v") as "s", count(lit(1)) as "c")
    }
  }

  test("results unchanged with nullable grouping keys") {
    checkAdaptiveMatchesReference { () =>
      spark.range(0, 400, 1, 1)
        .select(
          when($"id" % 4 === 0, lit(null)).otherwise($"id") as "k",
          $"id" as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s", count(lit(1)) as "c")
    }
  }

  test("results unchanged with average (multi-slot buffer) aggregate") {
    // avg has a two-slot partial buffer (sum, count); pass-through buffers must carry all slots.
    checkAdaptiveMatchesReference { () =>
      spark.range(0, 300, 1, 1)
        .select($"id" as "k", ($"id" + 1) as "v")
        .groupBy($"k")
        .agg(avg($"v") as "a", sum($"v") as "s")
    }
  }

  test("results unchanged with a mix of many aggregate functions and buffer types") {
    // Exercises a wide pass-through buffer spanning several aggregate buffer layouts at once:
    // sum (decimal), avg (double), count, min/max, first/last, and stddev (imperative buffer).
    checkAdaptiveMatchesReference { () =>
      spark.range(0, 400, 1, 1)
        .select(
          $"id" as "k",
          ($"id" % 97).cast("decimal(10,2)") as "d",
          ($"id" % 13).cast("double") as "dbl")
        .groupBy($"k")
        .agg(
          sum($"d") as "sd",
          avg($"dbl") as "ad",
          count(lit(1)) as "c",
          min($"dbl") as "mn",
          max($"dbl") as "mx",
          first($"dbl") as "f",
          last($"dbl") as "l",
          stddev($"dbl") as "sd2")
    }
  }

  test("results unchanged with filtered aggregate functions") {
    // A `FILTER (WHERE ...)` aggregate is compiled into a per-row guard around the buffer update
    // rather than a separate filtering operator: `If(filter, update, buffer)` in the interpreted
    // path and an `if (!cond) continue` guard in the generated code. Pass-through reuses those
    // exact update expressions, so a bypassed row whose filter is false contributes nothing to its
    // single-row buffer. The all-true and all-false filters pin the two extremes, and the fully
    // distinct grouping keys ensure rows bypass (in the regular-map-only configurations) so the
    // filter guard actually runs in the pass-through path.
    withTempView("t") {
      spark.range(0, 400, 1, 1)
        .select($"id" as "k", ($"id" % 100) as "v")
        .createOrReplaceTempView("t")
      checkAdaptiveMatchesReference { () =>
        spark.sql(
          """SELECT k,
            |       sum(v) FILTER (WHERE v % 2 = 0) AS s_even,
            |       count(1) FILTER (WHERE v > 50) AS c_gt50,
            |       avg(v) FILTER (WHERE v > 25) AS a_gt25,
            |       sum(v) FILTER (WHERE true) AS s_all,
            |       sum(v) FILTER (WHERE false) AS s_none
            |FROM t GROUP BY k""".stripMargin)
      }
    }
  }

  test("results unchanged with decimal and date grouping keys") {
    checkAdaptiveMatchesReference { () =>
      spark.range(0, 300, 1, 1)
        .select(
          ($"id" % 280).cast("decimal(12,3)") as "k1",
          date_add(lit(java.sql.Date.valueOf("2020-01-01")), ($"id" % 250).cast("int")) as "k2",
          $"id" as "v")
        .groupBy($"k1", $"k2")
        .agg(sum($"v") as "s", count(lit(1)) as "c")
    }
  }

  test("results unchanged for group-by-only (distinct) with no aggregate functions") {
    // No aggregate functions: the pass-through buffer is a zero-column UnsafeRow, so the output is
    // just the grouping key. High-cardinality keys should bypass, and the de-duplicated result must
    // still match the reference.
    checkAdaptiveMatchesReference { () =>
      spark.range(0, 400, 1, 1)
        .select(($"id" % 350) as "k1", ($"id" % 11) as "k2")
        .distinct()
    }
  }

  test("results unchanged for group-by-only with duplicate keys (Final phase must not bypass)") {
    // A group-by-only aggregate has an empty `aggregateExpressions`, so checking the aggregate
    // modes alone is vacuously true and could wrongly admit the `Final` phase of the two-phase
    // plan. With duplicate keys, a bypassing `Final` would skip its de-duplication and return
    // duplicate rows. The two-level map off variants route the rows to the regular map so the
    // periodic check fires and the regression would show up.
    checkAdaptiveMatchesReference { () =>
      spark.range(0, 1000, 1, 1)
        .select(($"id" % 10) as "c")
        .distinct()
    }
  }

  test("results unchanged when a large frozen map is output before pass-through streaming") {
    // A larger sample lets the map accumulate many keys before the periodic check bypasses, so the
    // early map output (which also frees the map) spans several drain cycles and re-enters the
    // map-output function; the results must still match the feature-off reference.
    val query = () => spark.range(0, 400000, 1, 1)
      .select($"id" as "k", $"id" as "v")
      .groupBy($"k")
      .agg(sum($"v") as "s")
    withSQLConf(
      (Seq(
        SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_ROWS.key -> "200000",
        SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> "false") ++ fixedPlanConfs): _*) {
      val reference = withSQLConf(
        SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> "false") {
        query().collect().toSeq
      }
      checkAnswer(query(), reference)
    }
  }

  test("distinct aggregation stays correct") {
    checkAdaptiveMatchesReference { () =>
      spark.range(0, 300, 1, 1)
        .select($"id" as "k", ($"id" % 50) as "v")
        .groupBy($"k")
        .agg(countDistinct($"v") as "cd", sum($"v") as "s")
    }
  }

  test("distinct aggregation bypasses on high-cardinality input") {
    // The `PartialMerge` phase of the multi-phase distinct plan always aggregates (it is not
    // `Partial` mode and requires a distribution), so the rows reaching the distinct `Partial`
    // phase are de-duplicated and pass-through carries exactly one distinct value each.
    forEachCodegenAndMap() { clue =>
      val df = () => spark.range(0, 1000, 1, 1)
        .select(($"id" % 100) as "k", $"id" as "v")
        .groupBy($"k")
        .agg(countDistinct($"v") as "cd")
      withClue(clue) {
        assert(numBypassingRows(df) > 0,
          "expected a distinct partial aggregation to bypass for high-cardinality input")
      }
    }
  }

  test("count distinct: the de-duplication partial aggregate bypasses") {
    // `count(DISTINCT v) GROUP BY k` plans two `Partial` phases: the de-duplication partial groups
    // on (k, v) and the distinct partial groups on (k). Fully distinct (k, v) pairs make the
    // de-duplication partial (2 grouping keys) reduce nothing, so it must bypass.
    forEachCodegenAndMap() { clue =>
      val df = () => spark.range(0, 400, 1, 1)
        .select(($"id" % 4) as "k", $"id" as "v")
        .groupBy($"k")
        .agg(countDistinct($"v") as "cd")
      withClue(clue) {
        val byKeyCount = bypassRowsByGroupingKeyCount(df)
        assert(byKeyCount.get(2).exists(_ > 0),
          s"expected the (k, v) de-duplication partial to bypass, got $byKeyCount")
      }
    }
  }

  test("count distinct: the distinct partial aggregate bypasses") {
    // Mirror of the test above for the other phase: with many distinct keys but few distinct
    // values per key, the (k, v) de-duplication partial reduces well while the distinct partial
    // (1 grouping key) sees a fresh key per row and must bypass.
    forEachCodegenAndMap() { clue =>
      val df = () => spark.range(0, 400, 1, 1)
        .select($"id" as "k", ($"id" % 2) as "v")
        .groupBy($"k")
        .agg(countDistinct($"v") as "cd")
      withClue(clue) {
        val byKeyCount = bypassRowsByGroupingKeyCount(df)
        assert(byKeyCount.get(1).exists(_ > 0),
          s"expected the distinct partial (grouping on k) to bypass, got $byKeyCount")
      }
    }
  }

  test("count distinct: both partial aggregates bypass and results stay correct") {
    // Fully distinct keys and fully distinct values: neither partial phase reduces anything, so
    // both bypass in the same execution. The de-duplication partial keeps the (k, v) pairs unique
    // and the distinct partial counts them, so the result must still match the reference.
    forEachCodegenAndMap() { clue =>
      val df = () => spark.range(0, 400, 1, 1)
        .select($"id" as "k", $"id" as "v")
        .groupBy($"k")
        .agg(countDistinct($"v") as "cd")
      withClue(clue) {
        val byKeyCount = bypassRowsByGroupingKeyCount(df)
        assert(byKeyCount.get(2).exists(_ > 0),
          s"expected the (k, v) de-duplication partial to bypass, got $byKeyCount")
        assert(byKeyCount.get(1).exists(_ > 0),
          s"expected the distinct partial (grouping on k) to bypass, got $byKeyCount")
      }
    }
  }

  test("global aggregation (no grouping keys) is never bypassed and stays correct") {
    withSQLConf(SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> "true") {
      checkAnswer(
        spark.range(0, 100, 1, 1).agg(sum($"id") as "s", count(lit(1)) as "c"),
        Row(4950L, 100L))
    }
  }

  test("results unchanged with an empty input") {
    checkAdaptiveMatchesReference { () =>
      spark.range(0, 0, 1, 1)
        .select($"id" as "k", $"id" as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s", count(lit(1)) as "c")
    }
  }

  // The following four tests cover plans where an `ExpandExec` sits below the partial aggregate
  // (ROLLUP / CUBE / GROUPING SETS / multi-distinct). PR apache/spark#28804 statically disabled its
  // skip-partial-aggregate optimization whenever an Expand was present, but that was a performance
  // heuristic guarding its *static* row sampling, not a correctness requirement. Our decision is
  // made at runtime from the observed reduction ratio, so we deliberately do not port that
  // exclusion. These tests assert results stay correct with the exclusion absent.

  test("results unchanged for ROLLUP (Expand below partial aggregate)") {
    checkAdaptiveMatchesReference { () =>
      spark.range(0, 400, 1, 1)
        .select(($"id" % 200) as "k1", ($"id" % 7) as "k2", $"id" as "v")
        .rollup($"k1", $"k2")
        .agg(sum($"v") as "s", count(lit(1)) as "c")
    }
  }

  test("results unchanged for CUBE (Expand below partial aggregate)") {
    checkAdaptiveMatchesReference { () =>
      spark.range(0, 400, 1, 1)
        .select(($"id" % 150) as "k1", ($"id" % 5) as "k2", $"id" as "v")
        .cube($"k1", $"k2")
        .agg(sum($"v") as "s", count(lit(1)) as "c")
    }
  }

  test("results unchanged for GROUPING SETS (Expand below partial aggregate)") {
    withTempView("t") {
      spark.range(0, 400, 1, 1)
        .select(($"id" % 180) as "k1", ($"id" % 6) as "k2", $"id" as "v")
        .createOrReplaceTempView("t")
      checkAdaptiveMatchesReference { () =>
        spark.sql(
          """SELECT k1, k2, sum(v) AS s, count(1) AS c
            |FROM t
            |GROUP BY k1, k2 GROUPING SETS ((k1, k2), (k1), ())""".stripMargin)
      }
    }
  }

  test("results unchanged for multi-distinct (Expand below partial aggregate)") {
    checkAdaptiveMatchesReference { () =>
      spark.range(0, 400, 1, 1)
        .select(($"id" % 100) as "k", ($"id" % 30) as "a", ($"id" % 40) as "b")
        .groupBy($"k")
        .agg(countDistinct($"a") as "da", countDistinct($"b") as "db", sum($"a") as "s")
    }
  }

  test("results unchanged under a fused Union (child yields from a nested helper)") {
    // `UnionExec` wraps each child's produce in its own helper, so a streamed row that fills the
    // output buffer returns only as far as the aggregate's build loop. Reaching the end of the
    // child's produce therefore does not mean the input is exhausted, and treating it as such
    // drops the rest of the partition.
    checkAdaptiveMatchesReference { () =>
      spark.range(0, 100, 1, 2).union(spark.range(100, 200, 1, 2)).groupBy("id").count()
    }
  }

  test("results unchanged below a generator that cannot yield mid-fan-out") {
    // `GenerateExec` expands a collection without checking `shouldStop()`, so the aggregate cannot
    // rely on the child to bound the output buffer -- each streamed row has to be able to leave
    // the build loop on its own.
    checkAdaptiveMatchesReference { () =>
      spark.range(0, 1, 1, 1)
        .select(explode(sequence(lit(1), lit(500))) as "k")
        .groupBy($"k").count()
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // Part 2: Triggering -- the bypass fires when, and only when, it should.
  /////////////////////////////////////////////////////////////////////////////

  test("pass-through fires for high-cardinality input, not for low-cardinality input") {
    forEachCodegenAndMap() { clue =>
      // Fully distinct keys: partial aggregation reduces nothing, so rows must bypass.
      val highCard = () => spark.range(0, 200, 1, 1)
        .select($"id" as "k", $"id" as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s")
      withClue(clue) {
        assert(numBypassingRows(highCard) > 0,
          "expected some rows to bypass partial aggregation for high-cardinality input")
      }
      // Few distinct keys, high reduction: partial aggregation is effective, nothing bypasses.
      val lowCard = () => spark.range(0, 600, 1, 1)
        .select(($"id" % 5) as "k", $"id" as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s")
      withClue(clue) {
        assert(numBypassingRows(lowCard) == 0,
          "expected no rows to bypass partial aggregation for low-cardinality input")
      }
    }
  }

  test("group-by-only pass-through fires for high-cardinality input") {
    forEachCodegenAndMap() { clue =>
      val distinctKeys = () => spark.range(0, 200, 1, 1).select($"id" as "k").distinct()
      withClue(clue) {
        assert(numBypassingRows(distinctKeys) > 0,
          "expected group-by-only rows to bypass partial aggregation for high-cardinality input")
      }
    }
  }

  test("filtered aggregate functions are eligible for pass-through") {
    // The filter clause does not change eligibility: a partial aggregate over `FILTER (WHERE ...)`
    // functions still bypasses on high-cardinality input, and the per-row filter guard runs inside
    // the pass-through single-row buffer update.
    forEachCodegenAndMap() { clue =>
      withTempView("t") {
        spark.range(0, 200, 1, 1)
          .select($"id" as "k", ($"id" % 100) as "v")
          .createOrReplaceTempView("t")
        val df = () => spark.sql(
          """SELECT k, sum(v) FILTER (WHERE v % 2 = 0) AS s
            |FROM t GROUP BY k""".stripMargin)
        withClue(clue) {
          assert(numBypassingRows(df) > 0,
            "expected filtered-aggregate rows to bypass partial aggregation for high-cardinality " +
              "input")
        }
      }
    }
  }

  test("pass-through fires for high-cardinality input below an Expand") {
    // The static PR#28804 heuristic would have refused to skip whenever an Expand was present; our
    // runtime decision skips because the expanded rows genuinely do not reduce. GROUPING SETS over
    // two single-column, fully-distinct sets is used (rather than ROLLUP/CUBE) so there is no
    // grand-total group dragging the reduction ratio below the threshold: every expanded row is a
    // fresh key, so all configurations bypass.
    forEachCodegenAndMap() { clue =>
      withTempView("t") {
        spark.range(0, 200, 1, 1)
          .select($"id".as("k1"), ($"id" + 1000).as("k2"), $"id".as("v"))
          .createOrReplaceTempView("t")
        val gs = () => spark.sql(
          """SELECT k1, k2, sum(v) AS s
            |FROM t
            |GROUP BY k1, k2 GROUPING SETS ((k1), (k2))""".stripMargin)
        withClue(clue) {
          assert(numBypassingRows(gs) > 0,
            "expected rows below an Expand to bypass partial aggregation for high-cardinality " +
              "input")
        }
      }
    }
  }

  test("no pass-through when the feature is disabled") {
    // The metric must stay zero across the whole matrix when the switch is off, even for input that
    // would otherwise bypass.
    for {
      wholeStage <- Seq(true, false)
      twoLevelMap <- Seq(true, false)
    } {
      withSQLConf(
        (Seq(
          SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> "false",
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> wholeStage.toString,
          SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> twoLevelMap.toString) ++ fixedPlanConfs): _*) {
        val df = () => spark.range(0, 200, 1, 1)
          .select($"id" as "k", $"id" as "v")
          .groupBy($"k")
          .agg(sum($"v") as "s")
        withClue(s"wholeStage=$wholeStage twoLevelMap=$twoLevelMap") {
          assert(numBypassingRows(df) == 0,
            "no rows should bypass partial aggregation when the feature is disabled")
        }
      }
    }
  }

  test("no pass-through for a global aggregation with no grouping keys") {
    // Global aggregation is ineligible (`groupingExpressions` is empty): there is a single group,
    // so there is nothing to stream through. The partial aggregate must never bypass regardless of
    // codegen or map settings, even under a forced fallback.
    forEachCodegenAndMap(regularFallback = 16) { clue =>
      val df = () => spark.range(0, 200, 1, 1)
        .agg(sum($"id") as "s", count(lit(1)) as "c")
      withClue(clue) {
        assert(numBypassingRows(df) == 0,
          "a global aggregation is not eligible and must never bypass")
      }
    }
  }

  test("rows absorbed by the fast map count toward the compaction ratio") {
    // The compaction ratio is measured at the operator level, so the rows the fast map absorbs
    // must count in the numerator just as its keys count in the denominator. This input is
    // dominated by a hot-key prefix that the fast map serves without ever reaching the regular
    // map, followed by a short distinct tail that does reach it. Counting only the regular map's
    // traffic would see the tail alone -- a ratio near 1 -- and bypass an aggregation that is in
    // fact collapsing rows heavily.
    forEachCodegenAndMap() { clue =>
      // With the fast map on it holds 4 keys, so `k < 4` is served there and `k >= 4` falls
      // through. 400 hot-key rows against 4 hot keys plus 20 tail keys is a ratio of about 17.5,
      // far above the default 1.1, so nothing may bypass.
      val df = () => spark.range(0, 420, 1, 1)
        .select(when($"id" < 400, $"id" % 4).otherwise($"id" - 396) as "k", $"id" as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s")
      withClue(clue) {
        assert(numBypassingRows(df) == 0,
          "an aggregation collapsing ~17 rows per key must not bypass; fast-map hits are " +
            "missing from the numerator if it does")
      }
    }
  }

  test("the periodic check fires when the sample shows no reduction, without spilling") {
    // No forced regular-map spill: only the periodic check can trigger the bypass. Fully
    // distinct keys over a small sample cross `noSpillReductionRatioThreshold`, so rows bypass and
    // the regular map never spills.
    forEachCodegenAndMap() { clue =>
      val df = () => spark.range(0, 200, 1, 1)
        .select($"id" as "k", $"id" as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s")
      withClue(clue) {
        val c = runAndReadCounters(df)
        assert(c.skipped > 0,
          "the periodic check should bypass fully distinct input under the sample")
        assert(c.spillBytes == 0, "the periodic check must decide before any spill happens")
        assert(c.tasksFallBacked == 0,
          "the periodic check must not fall back to sort-based aggregation")
      }
    }
  }

  test("the spill check fires when the map would spill on high-cardinality input") {
    // Force the regular map to fall back quickly. High-cardinality input that reaches the fallback
    // point should bypass via the spill check rather than spilling. Use a sample larger than the
    // input so the periodic check cannot fire first and the spill check is the one exercised.
    forEachCodegenAndMap(minRows = 100000, regularFallback = 16) { clue =>
      val df = () => spark.range(0, 200, 1, 1)
        .select($"id" as "k", $"id" as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s")
      withClue(clue) {
        val c = runAndReadCounters(df)
        assert(c.skipped > 0,
          "the spill check should bypass high-cardinality input at the spill boundary")
        // The whole point of the spill check is to bypass *instead of* falling back to sort-based
        // aggregation, so the sorter is never created. `numTasksFallBacked` is the reliable
        // per-operator signal for that (the `spillSize` metric on the interpreted path is derived
        // from the task-cumulative memory-spill counter and can be inflated by unrelated spilling
        // such as the downstream shuffle write, so it is not asserted here).
        assert(c.tasksFallBacked == 0,
          "the spill check must replace the sort fallback, not trigger it")
      }
    }
  }

  test("a new in-memory map epoch after a spill can still bypass") {
    // A spill starts a new in-memory map epoch and restarts the row counters, so an input whose
    // cardinality only turns unfavorable after an early spill is still caught. The first 100 rows
    // repeat 5 keys and keep the aggregation effective while the forced fallback makes the map
    // spill; the remaining 300 rows are fully distinct, so the new epoch is judged ineffective and
    // the rest of the input is passed through. Both the spill and the bypass must be observable.
    for {
      wholeStage <- Seq(true, false)
      twoLevelMap <- Seq(true, false)
    } {
      val fastCap = if (twoLevelMap) 4 else 1
      withSQLConf(
        (Seq(
          SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> "true",
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> wholeStage.toString,
          SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> twoLevelMap.toString,
          SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_ROWS.key -> "8",
          "spark.sql.TungstenAggregate.testFallbackStartsAt" -> s"$fastCap, 40") ++
          fixedPlanConfs): _*) {
        val df = () => spark.range(0, 400, 1, 1)
          .select(when($"id" < 100, $"id" % 5).otherwise($"id") as "k", $"id" as "v")
          .groupBy($"k")
          .agg(sum($"v") as "s")
        withClue(s"wholeStage=$wholeStage twoLevelMap=$twoLevelMap") {
          val c = runAndReadCounters(df)
          assert(c.tasksFallBacked > 0,
            "the low-cardinality prefix should still spill and fall back to sort")
          assert(c.skipped > 0,
            "the high-cardinality tail after the spill should be passed through")
        }
      }
    }
  }

  test("without the feature the same input really does fall back to sort") {
    // Sanity check for the spill check assertion above: with adaptive disabled, the identical
    // high-cardinality input under the same forced fallback genuinely falls back to sort-based
    // aggregation. This proves the spill check's `tasksFallBacked == 0` reflects the bypass and
    // not merely an input that never reached the spill boundary.
    for {
      wholeStage <- Seq(true, false)
      twoLevelMap <- Seq(true, false)
    } {
      val fastCap = if (twoLevelMap) 4 else 1
      withSQLConf(
        (Seq(
          SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> "false",
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> wholeStage.toString,
          SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> twoLevelMap.toString,
          "spark.sql.TungstenAggregate.testFallbackStartsAt" -> s"$fastCap, 16") ++
          fixedPlanConfs): _*) {
        val df = () => spark.range(0, 200, 1, 1)
          .select($"id" as "k", $"id" as "v")
          .groupBy($"k")
          .agg(sum($"v") as "s")
        withClue(s"wholeStage=$wholeStage twoLevelMap=$twoLevelMap") {
          val c = runAndReadCounters(df)
          assert(c.skipped == 0, "feature disabled: nothing should bypass")
          assert(c.tasksFallBacked > 0,
            "feature disabled: the forced fallback should trigger sort-based aggregation")
        }
      }
    }
  }

  test("the spill check decides identically at the exact ratio boundary with codegen on and off") {
    // The check before a spill evaluates the ratio over the rows already aggregated, excluding the
    // failed insertion that becomes the first pass-through row, so both execution paths must judge
    // the same row set and reach the same decision. `id % 40` over 400 rows gives 40 keys when the
    // map fills at 50 aggregated rows, i.e. a compaction ratio of exactly 1.25: demanding 1.25 the
    // aggregation is kept (`50 < 40 * 1.25` is false), demanding 1.3 it is bypassed.
    Seq(1.25 -> false, 1.3 -> true).foreach { case (minCompaction, shouldBypass) =>
      val skippedPerCodegen = Seq(true, false).map { wholeStage =>
        withSQLConf(
          (Seq(
            SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> "true",
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> wholeStage.toString,
            SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> "false",
            // A `minRows` larger than the input keeps the periodic check out of the picture.
            SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_ROWS.key -> "100000",
            SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_COMPACTION.key -> minCompaction.toString,
            "spark.sql.TungstenAggregate.testFallbackStartsAt" -> "1, 50") ++
            fixedPlanConfs): _*) {
          val df = () => spark.range(0, 400, 1, 1)
            .select(($"id" % 40) as "k", $"id" as "v")
            .groupBy($"k")
            .agg(sum($"v") as "s")
          withClue(s"minCompaction=$minCompaction wholeStage=$wholeStage") {
            numBypassingRows(df)
          }
        }
      }
      withClue(s"minCompaction=$minCompaction skipped=$skippedPerCodegen") {
        assert(skippedPerCodegen.forall(_ > 0) == shouldBypass,
          s"expected bypass=$shouldBypass at the ratio boundary")
        assert(skippedPerCodegen.map(_ > 0).distinct.length == 1,
          "codegen and interpreted paths must reach the same decision at the boundary")
      }
    }
  }

  test("a very high minCompaction always bypasses once minRows has been processed") {
    // Demanding an unreachable compaction ratio is the most aggressive setting: even a
    // low-cardinality input that the default threshold keeps aggregating is bypassed at the first
    // check point. The results must still match the feature-off reference.
    forEachCodegenAndMap(minCompaction = 1000000.0) { clue =>
      val df = () => spark.range(0, 600, 1, 1)
        .select(($"id" % 5) as "k", $"id" as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s")
      withClue(clue) {
        assert(numBypassingRows(df) > 0,
          "an unreachable compaction ratio must bypass even a low-cardinality input")
      }
    }
  }

  test("minRows = 0 disables the periodic check but keeps the spill check") {
    // `minRows = 0` is a sentinel for "never evaluate periodically". Without a forced regular-map
    // spill there is no check point at all, so fully distinct input -- which the default settings
    // bypass immediately -- must be aggregated all the way through. This is what proves the
    // periodic check is genuinely off rather than merely deferred.
    forEachCodegenAndMap(minRows = 0) { clue =>
      val df = () => spark.range(0, 200, 1, 1)
        .select($"id" as "k", $"id" as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s")
      withClue(clue) {
        assert(numBypassingRows(df) == 0,
          "minRows = 0 must disable the periodic check, so nothing may bypass without a spill")
      }
    }
  }

  test("minRows = 0 still bypasses at the spill boundary") {
    // The other half of the sentinel: with the periodic check off, the spill check alone still
    // bypasses instead of paying the spill I/O, which is the spill-only operating mode.
    forEachCodegenAndMap(minRows = 0, regularFallback = 16) { clue =>
      val df = () => spark.range(0, 200, 1, 1)
        .select($"id" as "k", $"id" as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s")
      withClue(clue) {
        val c = runAndReadCounters(df)
        assert(c.skipped > 0,
          "the spill check must still bypass when the periodic check is disabled")
        assert(c.tasksFallBacked == 0,
          "the spill check must replace the sort fallback, not trigger it")
      }
    }
  }

  test("minCompaction rejects values that cannot be generated as a Java literal") {
    // The threshold is interpolated straight into the generated source, where a non-finite double
    // renders as `InfinityD` -- not a valid Java literal. Reject it at configuration time rather
    // than failing to compile the stage (or silently deoptimizing when codegen falls back).
    Seq("Infinity", "-Infinity", "NaN", "1e309").foreach { v =>
      val e = intercept[IllegalArgumentException] {
        spark.conf.set(SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_COMPACTION.key, v)
      }
      assert(e.getMessage.contains("finite"), s"expected a finiteness error for '$v': $e")
    }
    spark.conf.unset(SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_COMPACTION.key)
  }

  test("larger sample defers the decision so a small high-cardinality input is not bypassed") {
    // With a sample larger than the whole input and no regular-map spill forced, the periodic check
    // point is never reached, so nothing bypasses even though the keys are fully distinct.
    forEachCodegenAndMap(minRows = 100000) { clue =>
      val df = () => spark.range(0, 200, 1, 1)
        .select($"id" as "k", $"id" as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s")
      withClue(clue) {
        assert(numBypassingRows(df) == 0,
          "no bypass expected before the sample size is reached")
      }
    }
  }

  test("partial aggregate output row count reflects the bypass (independent of the skip metric)") {
    // `numOutputRows` on the partial aggregate is the ground truth: it is driven by the normal
    // aggregation output path, not by our self-reported `numBypassingRows` metric. This test cross
    // checks the two and pins the observable data-side effect of bypassing.
    val numRows = 200
    forEachCodegenAndMap() { clue =>
      // Fully distinct keys: once the bypass fires the operator stops collapsing rows, so the
      // partial aggregate emits far more than the handful of keys a real aggregation would. Read
      // all counters from a single execution so the metrics are not double-counted.
      val highCard = () => spark.range(0, numRows, 1, 1)
        .select($"id" as "k", $"id" as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s")
      withClue(clue) {
        val c = runAndReadCounters(highCard)
        assert(c.skipped > 0, "high-cardinality input should bypass")
        // Every partial output row is either a real (aggregated) group or a bypassed row, so the
        // partial output count must be at least the number of bypassed rows, and it climbs toward
        // the input row count -- well above the heavy reduction a kept aggregation would give.
        assert(c.partialOutputRows >= c.skipped,
          s"partial output ${c.partialOutputRows} should be >= bypassed rows ${c.skipped}")
        assert(c.partialOutputRows > numRows / 2,
          s"partial output (${c.partialOutputRows}) should climb toward the input row count")
      }

      // Low-cardinality reference: partial aggregation stays effective, so its output equals the
      // small number of distinct keys and nothing is bypassed.
      val lowCard = () => spark.range(0, 600, 1, 1)
        .select(($"id" % 5) as "k", $"id" as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s")
      withClue(clue) {
        val c = runAndReadCounters(lowCard)
        assert(c.skipped == 0, "low-cardinality input should not bypass")
        assert(c.partialOutputRows == 5,
          "an effective partial aggregate should emit exactly the distinct key count")
      }
    }
  }
}
