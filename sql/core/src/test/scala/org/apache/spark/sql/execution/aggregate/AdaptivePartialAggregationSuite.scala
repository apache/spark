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
 * buffers for the Final aggregate to merge. Once pass-through is active the map is frozen, and its
 * output always precedes the passed-through rows: a row that collides with a frozen key is held
 * behind the map and flushed only after it drains, so every group merges its buffers in the same
 * order as a run that never bypasses, including order-sensitive aggregates such as `first`/`last`.
 *
 * The suite has two halves:
 *   1. Correctness: aggregate results are identical to the reference (feature-off) run across the
 *      full matrix of codegen on/off, two-level map on/off, and spill/no-spill, over a range of
 *      aggregate shapes, key types, and `Expand`-bearing plans (ROLLUP / CUBE / GROUPING SETS /
 *      multi-distinct). Order-sensitive aggregates are tested against the reference too, including
 *      under a fan-out child that queues its whole batch behind the frozen map.
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
   * Runs `build` with adaptive partial aggregation disabled (the reference) and then across the
   * full configuration matrix with it enabled, asserting every enabled run matches the reference.
   *
   * `build` takes the number of input partitions, which the matrix varies along with everything
   * else, because the plan shape decides which parts of the feature run at all. When the two
   * aggregates end up in one whole-stage -- no `Exchange` between them -- the partial aggregate's
   * output feeds the Final's `doConsume` directly and never reaches
   * `BufferedRowIterator.currentRows`, so `shouldStop()` stays false for the whole build and
   * neither `needStopCheck` nor the resumed-build path is exercised. Splitting them puts the
   * streamed rows through the output buffer and runs both.
   *
   * More than one input partition is necessary but not sufficient for that split: a `Range` keyed
   * directly on `id` already reports an output partitioning that satisfies the Final aggregate's
   * `ClusteredDistribution`, so `EnsureRequirements` inserts no `Exchange` however many partitions
   * it has. Tests that want the split shape group on a derived key (a cast, say) so the input
   * partitioning no longer satisfies the requirement.
   *
   * `expectBypass` ties the correctness guarantee to the triggering guarantee: beyond matching the
   * reference, every cell must either actually stream rows through (when true) or keep
   * aggregating (when false). Without it a test could silently stop exercising pass-through if the
   * input stopped being bypassable, and only this assertion makes that fail loudly.
   */
  private def checkAdaptiveMatchesReference(
      build: Int => DataFrame,
      expectBypass: Boolean = true): Unit = {
    for {
      inputPartitions <- Seq(1, 2)
      wholeStage <- Seq(true, false)
      twoLevelMap <- Seq(true, false)
      forceSpill <- Seq(true, false)
    } {
      // The reference is built with the same partitioning, so only the feature differs.
      val reference = withSQLConf(
        (SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> "false") +: fixedPlanConfs: _*) {
        build(inputPartitions).collect().toSeq
      }
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
          // Small `minRows` so the periodic check runs on modest inputs.
          SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_ROWS.key -> "8") ++
          spillConf ++ fixedPlanConfs): _*) {
        val msg = s"inputPartitions=$inputPartitions wholeStage=$wholeStage " +
          s"twoLevelMap=$twoLevelMap forceSpill=$forceSpill"
        withClue(msg) {
          // Collect once so the metrics are populated, then check whether the bypass fired for
          // this cell. The metric lives on the `Partial`-mode `HashAggregateExec`, so that is the
          // operator the assertion reads.
          val df = build(inputPartitions)
          df.collect()
          val skipped = collect(df.queryExecution.executedPlan) {
            case agg: HashAggregateExec if agg.aggregateExpressions.forall(_.mode == Partial) =>
              agg.metrics.get("numBypassingRows").map(_.value).getOrElse(0L)
          }.sum
          if (expectBypass) {
            assert(skipped > 0,
              s"expected rows to bypass partial aggregation, got $skipped bypassed rows")
          } else {
            assert(skipped == 0,
              s"expected no rows to bypass partial aggregation, got $skipped bypassed rows")
          }
          checkAnswer(df, reference)
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
      // The metric is only registered on aggregates the feature applies to; an aggregate without
      // it bypassed nothing.
      skipped = partialAggs.map(_.metrics.get("numBypassingRows").map(_.value).getOrElse(0L)).sum,
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
        agg.groupingExpressions.length ->
          agg.metrics.get("numBypassingRows").map(_.value).getOrElse(0L)
    }.groupBy(_._1).map { case (n, pairs) => n -> pairs.map(_._2).sum }
    checkAgainstReference(df, build)
    byKeyCount
  }

  /**
   * Runs `body` once per (wholeStage, twoLevelMap) combination with the feature enabled and a small
   * `minRows`, threading a descriptive clue for failure messages.
   *
   * The fast (first-level) map is append-only and never spills, so only the regular (second-level)
   * map can reach a spill boundary. With the default fast-map capacity (2^16) a small
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
    checkAdaptiveMatchesReference { parts =>
      spark.range(0, 200, 1, parts)
        .select($"id".cast("string") as "k", ($"id" * 2) as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s", count(lit(1)) as "c", max($"v") as "m")
    }
  }

  test("results unchanged for low-cardinality input that keeps partial aggregation") {
    // Few distinct keys, high reduction: partial aggregation is effective and should be kept, so
    // the bypass metric must stay zero in every cell.
    checkAdaptiveMatchesReference(
      expectBypass = false,
      build = { parts =>
        spark.range(0, 600, 1, parts)
          .select(($"id" % 5).cast("string") as "k", $"id" as "v")
          .groupBy($"k")
          .agg(sum($"v") as "s", count(lit(1)) as "c", min($"v") as "mn", max($"v") as "mx")
      })
  }

  test("results unchanged for medium-cardinality input near the reduction threshold") {
    // Roughly half the rows are distinct keys; exercises the boundary of the ratio checks. The
    // overall compaction ratio (~2.0) is above the threshold, but the *first* periodic check still
    // sees the leading distinct keys and fires, so the bypass must be observable too.
    checkAdaptiveMatchesReference { parts =>
      spark.range(0, 1000, 1, parts)
        .select(($"id" % 500).cast("string") as "k", $"id" as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s", count(lit(1)) as "c")
    }
  }

  test("results unchanged with multiple grouping keys and string keys") {
    checkAdaptiveMatchesReference { parts =>
      spark.range(0, 500, 1, parts)
        .select(
          concat(lit("g"), ($"id" % 300).cast("string")) as "k1",
          ($"id" % 7) as "k2",
          $"id" as "v")
        .groupBy($"k1", $"k2")
        .agg(sum($"v") as "s", count(lit(1)) as "c")
    }
  }

  test("results unchanged with nullable grouping keys") {
    // Nulls are sparse enough (1 in 40) that the keys stay close to unique and the input really
    // does bypass; a denser null key would lift the compaction ratio above the threshold and the
    // test would never engage the feature.
    checkAdaptiveMatchesReference { parts =>
      spark.range(0, 400, 1, parts)
        .select(
          when($"id" % 40 === 0, lit(null)).otherwise($"id").cast("string") as "k",
          $"id" as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s", count(lit(1)) as "c")
    }
  }

  test("results unchanged with average (multi-slot buffer) aggregate") {
    // avg has a two-slot partial buffer (sum, count); pass-through buffers must carry all slots.
    checkAdaptiveMatchesReference { parts =>
      spark.range(0, 300, 1, parts)
        .select($"id".cast("string") as "k", ($"id" + 1) as "v")
        .groupBy($"k")
        .agg(avg($"v") as "a", sum($"v") as "s")
    }
  }

  test("results unchanged with a mix of many aggregate functions and buffer types") {
    // Exercises a wide pass-through buffer spanning several aggregate buffer layouts at once:
    // sum (decimal), avg (double), count, min/max, first/last, and stddev (declarative buffer).
    // The imperative-buffer case is covered separately (see the `approx_count_distinct` test).
    checkAdaptiveMatchesReference { parts =>
      spark.range(0, 400, 1, parts)
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

  test("results unchanged with an imperative-buffer aggregate") {
    // `approx_count_distinct` uses `HyperLogLogPlusPlus`, an `ImperativeAggregate` whose buffer
    // state is written by `initialize(buffer)` rather than by a projection, so a pass-through
    // single-row buffer has to be re-initialized with `copyFrom(initialAggregationBuffer)` for
    // every row. No declarative aggregate exercises that reset. It also reports
    // `supportCodegen = false`, so the operator only ever runs on `TungstenAggregationIterator`;
    // keep it in its own test rather than folding it into a codegen cell that would quietly
    // become interpreted.
    checkAdaptiveMatchesReference { parts =>
      spark.range(0, 300, 1, parts)
        .select($"id".cast("string") as "k", $"id" as "v")
        .groupBy($"k")
        .agg(approx_count_distinct($"v") as "c")
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
        .select($"id".cast("string") as "k", ($"id" % 100) as "v")
        .createOrReplaceTempView("t")
      checkAdaptiveMatchesReference { parts =>
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
    checkAdaptiveMatchesReference { parts =>
      spark.range(0, 300, 1, parts)
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
    checkAdaptiveMatchesReference { parts =>
      spark.range(0, 400, 1, parts)
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
    checkAdaptiveMatchesReference { parts =>
      spark.range(0, 1000, 1, parts)
        .select(($"id" % 10) as "c")
        .distinct()
    }
  }

  test("results unchanged when a large frozen map is output before pass-through streaming") {
    // A larger `minRows` lets the map accumulate many keys before the periodic check bypasses,
    // so the early map output (which also frees the map) spans several drain cycles and re-enters
    // the map-output function; the results must still match the feature-off reference.
    val query = () => spark.range(0, 400000, 1, 1)
      .select($"id".cast("string") as "k", $"id" as "v")
      .groupBy($"k")
      .agg(sum($"v") as "s")
    withSQLConf(
      (Seq(
        SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_ROWS.key -> "200000",
        SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> "false") ++ fixedPlanConfs): _*) {
      val df = query()
      df.collect()
      val skipped = collect(df.queryExecution.executedPlan) {
        case agg: HashAggregateExec if agg.aggregateExpressions.forall(_.mode == Partial) =>
          agg.metrics.get("numBypassingRows").map(_.value).getOrElse(0L)
      }.sum
      assert(skipped > 0,
        s"expected the large frozen map to eventually bypass, got $skipped bypassed rows")
      val reference = withSQLConf(
        SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> "false") {
        query().collect().toSeq
      }
      checkAnswer(df, reference)
    }
  }

  test("distinct aggregation stays correct") {
    checkAdaptiveMatchesReference { parts =>
      spark.range(0, 300, 1, parts)
        .select($"id".cast("string") as "k", ($"id" % 50) as "v")
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
        .select(($"id" % 100).cast("string") as "k", $"id" as "v")
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
        .select(($"id" % 4).cast("string") as "k", $"id" as "v")
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
        .select($"id".cast("string") as "k", ($"id" % 2) as "v")
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
        .select($"id".cast("string") as "k", $"id" as "v")
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
    // No rows means the check points never fire; the metric must stay zero.
    checkAdaptiveMatchesReference(
      expectBypass = false,
      build = { parts =>
        spark.range(0, 0, 1, 1)
          .select($"id".cast("string") as "k", $"id" as "v")
          .groupBy($"k")
          .agg(sum($"v") as "s", count(lit(1)) as "c")
      })
  }

  // The following four tests cover plans where an `ExpandExec` sits below the partial aggregate
  // (ROLLUP / CUBE / GROUPING SETS / multi-distinct). PR apache/spark#28804 statically disabled its
  // skip-partial-aggregate optimization whenever an Expand was present, but that was a performance
  // heuristic guarding its *static* row sampling, not a correctness requirement. Our decision is
  // made at runtime from the observed compaction ratio, so we deliberately do not port that
  // exclusion. These tests assert results stay correct with the exclusion absent.
  //
  // The ROLLUP and CUBE cases below use two grouping columns, where the grand-total set keeps the
  // compaction ratio high enough that they decline to bypass -- they cover the eligible-but-
  // declining side. (Widening the rollup lowers the ratio: with five distinct columns the same
  // shape does bypass.) The GROUPING SETS and multi-distinct tests, and `pass-through fires for
  // high-cardinality input below an Expand`, cover an Expand that bypasses.

  test("results unchanged for ROLLUP (Expand below partial aggregate)") {
    // The grand-total group repeats on every expanded row, so the compaction ratio stays above the
    // threshold and the partial aggregate declines to bypass in every cell.
    checkAdaptiveMatchesReference(
      expectBypass = false,
      build = { parts =>
        spark.range(0, 400, 1, parts)
          .select(($"id" % 200) as "k1", ($"id" % 7) as "k2", $"id" as "v")
          .rollup($"k1", $"k2")
          .agg(sum($"v") as "s", count(lit(1)) as "c")
      })
  }

  test("results unchanged for CUBE (Expand below partial aggregate)") {
    // Same as ROLLUP: the grand-total group keeps the ratio above the threshold, so nothing
    // bypasses.
    checkAdaptiveMatchesReference(
      expectBypass = false,
      build = { parts =>
        spark.range(0, 400, 1, parts)
          .select(($"id" % 150) as "k1", ($"id" % 5) as "k2", $"id" as "v")
          .cube($"k1", $"k2")
          .agg(sum($"v") as "s", count(lit(1)) as "c")
      })
  }

  test("results unchanged for GROUPING SETS (Expand below partial aggregate)") {
    // No `()` grouping set, and both keys distinct, so every expanded row is a fresh key and the
    // input genuinely bypasses. A grand-total set would collapse all rows into one group and lift
    // the compaction ratio above the threshold (see the ROLLUP and CUBE tests below).
    withTempView("t") {
      spark.range(0, 400, 1, 1)
        .select($"id" as "k1", ($"id" + 1000) as "k2", $"id" as "v")
        .createOrReplaceTempView("t")
      checkAdaptiveMatchesReference { parts =>
        spark.sql(
          """SELECT k1, k2, sum(v) AS s, count(1) AS c
            |FROM t
            |GROUP BY k1, k2 GROUPING SETS ((k1, k2), (k1), (k2))""".stripMargin)
      }
    }
  }

  test("results unchanged for multi-distinct (Expand below partial aggregate)") {
    checkAdaptiveMatchesReference { parts =>
      spark.range(0, 400, 1, parts)
        .select(($"id" % 100).cast("string") as "k", ($"id" % 30) as "a", ($"id" % 40) as "b")
        .groupBy($"k")
        .agg(countDistinct($"a") as "da", countDistinct($"b") as "db", sum($"a") as "s")
    }
  }

  test("results unchanged under a fused Union (child yields from a nested helper)") {
    // `UnionExec` wraps each child's produce in its own helper, so a streamed row that fills the
    // output buffer returns only as far as the aggregate's build loop. Reaching the end of the
    // child's produce therefore does not mean the input is exhausted, and treating it as such
    // drops the rest of the partition.
    checkAdaptiveMatchesReference { parts =>
      spark.range(0, 100, 1, parts + 1)
        .union(spark.range(100, 200, 1, parts + 1))
        .groupBy("id").count()
    }
  }

  test("results unchanged when an Exchange separates the two aggregates") {
    // Grouping on a derived key stops the `Range`'s output partitioning from satisfying the Final
    // aggregate's `ClusteredDistribution`, so the plan keeps an `Exchange` and the two aggregates
    // land in separate whole-stages. That is the shape where streamed rows actually pass through
    // `BufferedRowIterator.currentRows` -- fused, they go straight into the Final's hash map and
    // neither `needStopCheck` nor the resumed build is reached.
    checkAdaptiveMatchesReference { parts =>
      spark.range(0, 200, 1, parts)
        .select($"id".cast("string") as "k", ($"id" * 2) as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s", count(lit(1)) as "c", max($"v") as "m")
    }
  }

  test("order-sensitive aggregates match a non-bypassed run") {
    // Bypassing must merge a group's buffers in the same order as a run that never bypasses: a
    // group can straddle the freeze and hold both a map buffer and pass-through buffers, and the
    // `Final` merges in emit order. The queue holds each passed-through row behind the frozen map
    // and flushes it only after the map drains, so the map buffer always precedes the colliding
    // pass-through buffers on both execution paths -- exactly the merge order of a run that never
    // bypasses, so every enabled cell must match the feature-off reference and
    // `spark.sql.codegen.wholeStage` must not be observable.
    //
    // The flip fires at the first periodic check: with `minRows=8` it lands on the 8th aggregated
    // row, when the map already holds 8 distinct keys (id 0 maps to -1, ids 1-7 to keys 1-7), so
    // the compaction ratio 1.0 is below `minCompaction` (1.05) and every remaining row streams;
    // id 8 is the first bypassed row. At `dupAt=8` the colliding row is that first bypassed row,
    // queued behind the frozen map and flushed only after the map drains, so its buffer still
    // reaches the `Final` after the frozen map's and `first`/`last` match the merge order; at
    // `dupAt=9` the colliding row streams directly after the map and the group stays in input
    // order. Both plan shapes are exercised separately: splitting the aggregates with an
    // `Exchange` (or not) changes whether streamed rows pass through
    // `BufferedRowIterator.currentRows` or feed the `Final`'s `doConsume` directly.
    for {
      splits <- Seq(1, 2)
      derivedKey <- Seq(false, true)
      dupAt <- Seq(8, 9)
      wholeStage <- Seq(true, false)
      twoLevelMap <- Seq(true, false)
      forceSpill <- Seq(true, false)
    } {
      val query = () => {
        val base = when($"id" === 0 || $"id" === dupAt, lit(-1L)).otherwise($"id")
        spark.range(0, 40, 1, splits)
          .select(if (derivedKey) base.cast("string") else base as "k", $"id" as "v")
          .toDF("k", "v")
          .groupBy($"k")
          .agg(first($"v") as "f", last($"v") as "l")
      }
      val spillConf = if (forceSpill) {
        Seq("spark.sql.TungstenAggregate.testFallbackStartsAt" -> forceSpillFallback)
      } else {
        Nil
      }
      def run(enabled: Boolean): Seq[String] = withSQLConf(
        (Seq(
          SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> enabled.toString,
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> wholeStage.toString,
          SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> twoLevelMap.toString,
          SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_ROWS.key -> "8") ++
          spillConf ++ fixedPlanConfs): _*) {
        query().collect().toSeq.map(_.toString()).sorted
      }
      withClue(s"splits=$splits derivedKey=$derivedKey dupAt=$dupAt " +
        s"wholeStage=$wholeStage twoLevelMap=$twoLevelMap forceSpill=$forceSpill: ") {
        assert(run(enabled = true) == run(enabled = false),
          "adaptive partial aggregation changed an order-sensitive result")
      }
    }
  }

  test("fan-out below a split aggregate preserves the merge order") {
    // A `GenerateExec` expands a collection without checking `shouldStop()`, so in the
    // exchange-split shape the whole fan-out batch of the trigger input row is queued at once
    // rather than one row at a time. Each queued row advances the frozen-map output by one row,
    // and the queue is flushed only after the map fully drains, so a group whose rows straddle
    // the freeze point still merges its map buffer before its pass-through buffers, matching a
    // non-bypassed run.
    //
    // The colliding row is the first row of the trigger batch, so it is the first one queued: a
    // design that emitted queued rows ahead of the frozen map would put it before its map buffer.
    // Key 2 is inserted early (by id 1) and drained third, so id 4's first exploded element
    // collides with it; the other two fan-out tests cover collisions on later batch elements.
    checkAdaptiveMatchesReference { parts =>
      spark.range(0, 20, 1, parts)
        .select($"id", explode(array(
          when($"id" === 4, lit(2L)).otherwise($"id" * 2),
          $"id" * 2 + 1)) as "k")
        .select($"k", ($"id" * 100 + $"k") as "v")
        .groupBy($"k").agg(first($"v") as "f", last($"v") as "l")
    }
  }

  test("a wide fan-out batch stays queued behind the frozen map") {
    // A colliding key must not merge before its map buffer. This batch is four rows wide and
    // collides twice, so the colliding pass-through rows must both wait behind the frozen map for
    // the whole map to drain; a design that let any part of the batch escape ahead of the map
    // breaks the merge order for at least one collision.
    checkAdaptiveMatchesReference { parts =>
      spark.range(0, 20, 1, parts)
        .select($"id", explode(array(
          $"id" * 4,
          when($"id" === 2, lit(3L)).otherwise($"id" * 4 + 1),
          $"id" * 4 + 2,
          when($"id" === 2, lit(7L)).otherwise($"id" * 4 + 3))) as "k")
        .select($"k", ($"id" * 100 + $"k") as "v")
        .groupBy($"k").agg(first($"v") as "f", last($"v") as "l")
    }
  }

  test("a frozen map larger than the fan-out batch preserves the merge order") {
    // The queue bounds the held rows to the batch width regardless of the map size, and the flush
    // waits for the whole map to drain. This query grows the map well past the batch width, so a
    // design that flushed the held rows early would leave the colliding key's map buffer behind
    // them. Every enabled cell must match the feature-off reference, so each compares directly
    // against a non-bypassed run, across both the generated and interpreted paths.
    for {
      splits <- Seq(1, 2)
      wholeStage <- Seq(true, false)
      twoLevelMap <- Seq(true, false)
      forceSpill <- Seq(true, false)
    } {
      val query = () => {
        spark.range(0, 100, 1, splits)
          .select($"id", explode(array(
            $"id" * 2,
            when($"id" === 16, lit(5L)).otherwise($"id" * 2 + 1))) as "k")
          .select($"k", ($"id" * 100 + $"k") as "v")
          .groupBy($"k").agg(first($"v") as "f", last($"v") as "l")
      }
      val spillConf = if (forceSpill) {
        Seq("spark.sql.TungstenAggregate.testFallbackStartsAt" -> forceSpillFallback)
      } else {
        Nil
      }
      def run(enabled: Boolean): DataFrame = withSQLConf(
        (Seq(
          SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> enabled.toString,
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> wholeStage.toString,
          SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> twoLevelMap.toString,
          SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_ROWS.key -> "32") ++
          spillConf ++ fixedPlanConfs): _*) {
        query()
      }
      val reference = run(enabled = false)
      val adaptive = run(enabled = true)
      withClue(s"splits=$splits wholeStage=$wholeStage " +
        s"twoLevelMap=$twoLevelMap forceSpill=$forceSpill: ") {
        checkAnswer(adaptive, reference)
      }
    }
  }

  test("results unchanged below a generator that cannot yield mid-fan-out") {
    // `GenerateExec` expands a collection without checking `shouldStop()`, so it cannot bound the
    // output buffer between the rows of one input row. Each passed-through row queues a copy and
    // advances the frozen-map output by one row, so the whole fan-out batch is held behind the map
    // and the memory is bounded by the batch width rather than by how many rows the child emits.
    checkAdaptiveMatchesReference { parts =>
      spark.range(0, 1, 1, parts)
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
        .select($"id".cast("string") as "k", $"id" as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s")
      withClue(clue) {
        assert(numBypassingRows(highCard) > 0,
          "expected some rows to bypass partial aggregation for high-cardinality input")
      }
      // Few distinct keys, high reduction: partial aggregation is effective, nothing bypasses.
      val lowCard = () => spark.range(0, 600, 1, 1)
        .select(($"id" % 5).cast("string") as "k", $"id" as "v")
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
          .select($"id".cast("string") as "k", ($"id" % 100) as "v")
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
    // grand-total group lifting the compaction ratio above the threshold: every expanded row is a
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
          .select($"id".cast("string") as "k", $"id" as "v")
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

  test("no pass-through for a session_window grouping key") {
    // A batch `session_window` grouping is not streaming, but its partial aggregate feeds a
    // `MergingSessionsExec` that merges overlapping sessions, and passing single-row buffers into
    // it is untested. It is gated out in `adaptivePartialAggEnabled`. This input has fully
    // distinct sessions (ratio 1.0) and a small `minRows`, so without the gate the periodic check
    // would bypass and the `expectBypass = false` assertion would fail.
    checkAdaptiveMatchesReference(
      expectBypass = false,
      build = { parts =>
        spark.range(0, 40, 1, parts)
          .select($"id" as "v", timestamp_seconds($"id" * 60) as "time")
          .groupBy(session_window($"time", "10 seconds"))
          .agg(sum($"v") as "s")
      })
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
      // far above the default, so nothing may bypass.
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

  test("a distinct-heavy prefix commits the task even when later rows collapse") {
    // The flip is one-way, so the decision is made from the rows seen so far, not the partition as
    // a whole. Here the first `minRows` rows are all distinct, so the periodic check bypasses and
    // the pass-through stays on permanently: the remaining rows -- which all collapse onto a single
    // key and would have aggregated well -- stream through as single-row partial buffers. The
    // overall compaction ratio (40 rows / 8 keys = 5.0) would keep aggregation, but the prefix has
    // already committed the task. The mirror case (a compacting prefix with a distinct tail) is
    // covered by the test above; this pins the other side of the same asymmetry.
    forEachCodegenAndMap() { clue =>
      val df = () => spark.range(0, 40, 1, 1)
        .select(when($"id" < 8, $"id").otherwise(lit(0L)) as "k", $"id" as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s")
      withClue(clue) {
        val c = runAndReadCounters(df)
        assert(c.skipped == 32,
          s"expected the 32 rows after the flip to bypass, got ${c.skipped}")
        assert(c.partialOutputRows == 40,
          s"expected every input row to stream through the partial, got ${c.partialOutputRows}")
      }
    }
  }

  test("the periodic check fires when the ratio shows no reduction, without spilling") {
    // No forced regular-map spill: only the periodic check can trigger the bypass. Fully
    // distinct keys give a compaction ratio of 1.0, below `minCompaction`, so rows bypass and
    // the regular map never spills.
    forEachCodegenAndMap() { clue =>
      val df = () => spark.range(0, 200, 1, 1)
        .select($"id".cast("string") as "k", $"id" as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s")
      withClue(clue) {
        val c = runAndReadCounters(df)
        assert(c.skipped > 0,
          "the periodic check should bypass fully distinct input")
        assert(c.spillBytes == 0, "the periodic check must decide before any spill happens")
        assert(c.tasksFallBacked == 0,
          "the periodic check must not fall back to sort-based aggregation")
      }
    }
  }

  test("the spill check fires when the map would spill on high-cardinality input") {
    // Force the regular map to fall back quickly. High-cardinality input that reaches the fallback
    // point should bypass via the spill check rather than spilling. Use a `minRows` larger than the
    // input so the periodic check cannot fire first and the spill check is the one exercised.
    forEachCodegenAndMap(minRows = 100000, regularFallback = 16) { clue =>
      val df = () => spark.range(0, 200, 1, 1)
        .select($"id".cast("string") as "k", $"id" as "v")
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
          .select($"id".cast("string") as "k", $"id" as "v")
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
            .select(($"id" % 40).cast("string") as "k", $"id" as "v")
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
        .select(($"id" % 5).cast("string") as "k", $"id" as "v")
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
        .select($"id".cast("string") as "k", $"id" as "v")
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
        .select($"id".cast("string") as "k", $"id" as "v")
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

  test("a larger minRows defers the decision so a small high-cardinality input is not bypassed") {
    // With `minRows` larger than the whole input and no regular-map spill forced, the periodic
    // check point is never reached, so nothing bypasses even though the keys are fully distinct.
    forEachCodegenAndMap(minRows = 100000) { clue =>
      val df = () => spark.range(0, 200, 1, 1)
        .select($"id".cast("string") as "k", $"id" as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s")
      withClue(clue) {
        assert(numBypassingRows(df) == 0,
          "no bypass expected before `minRows` rows have been processed")
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
        .select($"id".cast("string") as "k", $"id" as "v")
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
        .select(($"id" % 5).cast("string") as "k", $"id" as "v")
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
