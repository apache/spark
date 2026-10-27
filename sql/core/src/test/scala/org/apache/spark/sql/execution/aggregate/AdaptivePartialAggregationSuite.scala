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
import org.apache.spark.sql.catalyst.expressions.aggregate.{Partial, PartialMerge}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.tags.ExtendedSQLTest

/**
 * Tests for runtime adaptive partial aggregation
 * (see [[SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED]]). When a partial aggregate is not reducing
 * rows, the operator stops aggregating and streams the remaining rows through as single-row partial
 * buffers for the Final aggregate to merge. Once pass-through is active the map is frozen, and its
 * output always precedes the passed-through rows: a row that collides with a frozen key is held
 * behind the map and flushed only after it drains, so every group merges its buffers in the same
 * order as a run that never bypasses, including order-sensitive aggregates such as `first`/`last`.
 *
 * The suite has three parts:
 *   1. Correctness: aggregate results are identical to the reference (feature-off) run across the
 *      full matrix of codegen on/off, two-level map on/off, and spill/no-spill, over a range of
 *      aggregate shapes, key types, and `Expand`-bearing plans (ROLLUP / CUBE / GROUPING SETS /
 *      multi-distinct). Order-sensitive aggregates are tested against the reference too, including
 *      under a fan-out child that queues its whole batch behind the frozen map.
 *   2. Triggering: the `numPassthroughRows` metric proves the bypass actually fires when (and only
 *      when) it should -- high-cardinality input bypasses, low-cardinality input keeps aggregating,
 *      the feature switch and eligibility rules are honored, and both check points work.
 *   3. Generated code: the pass-through path keeps the code layout that bounds its size.
 */
@ExtendedSQLTest
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

  // Whether `agg` is a partial or partial-merge phase. This matches every `HashAggregateExec`
  // whose modes are all `Partial`/`PartialMerge`, including the DISTINCT intermediate phase
  // (`PartialMerge ++ Partial`, the non-distinct aggregates in `PartialMerge` and the distinct
  // ones in `Partial`) that the feature now bypasses. It is a superset of the phases the feature
  // actually applies to: a pure `PartialMerge` de-duplication phase (excluded by its required
  // distribution) and an ineligible group-by-only `Final` (vacuously all-`Partial`) also match,
  // but neither registers a `numPassthroughRows` metric, so they add 0 to the bypass count below.
  private def isPartialPhase(agg: HashAggregateExec): Boolean =
    agg.aggregateExpressions.forall(a => a.mode == Partial || a.mode == PartialMerge)

  private val passThroughRowsMetric = "numPassthroughRows"

  // The rows `agg` passed through. The metric is only registered on aggregates the feature applies
  // to; an aggregate without it passed nothing through.
  private def passThroughRows(agg: HashAggregateExec): Long =
    agg.metrics.get(passThroughRowsMetric).map(_.value).getOrElse(0L)

  // The rows the partial phases of an already-collected `df` passed through.
  private def passThroughRows(df: DataFrame): Long =
    collect(df.queryExecution.executedPlan) {
      case agg: HashAggregateExec if isPartialPhase(agg) => passThroughRows(agg)
    }.sum

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
          // this cell. The metric lives on the partial `HashAggregateExec` phases, so those are the
          // operators the assertion reads.
          val df = build(inputPartitions)
          df.collect()
          val passedThrough = passThroughRows(df)
          val msg = s"got $passedThrough passed-through rows"
          if (expectBypass) {
            assert(passedThrough > 0, s"expected rows to bypass partial aggregation, $msg")
          } else {
            assert(passedThrough == 0, s"expected no rows to bypass partial aggregation, $msg")
          }
          checkAnswer(df, reference)
        }
      }
    }
  }

  /**
   * The observable per-run counters we assert on, all read from the partial `HashAggregateExec` in
   * a single execution so the metrics are not double-counted:
   *   - `passedThrough`: our self-reported `numPassthroughRows` metric.
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
      passedThrough: Long,
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
      case agg: HashAggregateExec if isPartialPhase(agg) => agg
    }
    // A partial aggregate is always present for the grouped queries these tests use.
    assert(partialAggs.nonEmpty, "expected a partial HashAggregateExec in the plan")
    val counters = AggCounters(
      passedThrough = partialAggs.map(passThroughRows).sum,
      partialOutputRows = partialAggs.map(_.metrics("numOutputRows").value).sum,
      spillBytes = partialAggs.map(_.metrics("spillSize").value).sum,
      tasksFallBacked = partialAggs.map(_.metrics("numTasksFallBacked").value).sum)
    checkAgainstReference(df, build)
    counters
  }

  private def numPassthroughRows(build: () => DataFrame): Long =
    runAndReadCounters(build).passedThrough

  // Returns the pass-through row count per partial `HashAggregateExec` phase, keyed by the number
  // of grouping keys, and verifies the run matches the feature-off reference. A
  // `count(DISTINCT ...)` group-by has two such phases -- the de-duplication partial (grouping on
  // key + distinct columns) and the distinct partial (grouping on the keys only, whose non-distinct
  // aggregates run in `PartialMerge`) -- so their bypasses can be told apart by the grouping key
  // count.
  private def passThroughRowsByGroupingKeyCount(build: () => DataFrame): Map[Int, Long] = {
    val df = build()
    df.collect()
    val byKeyCount = collect(df.queryExecution.executedPlan) {
      case agg: HashAggregateExec if isPartialPhase(agg) =>
        agg.groupingExpressions.length -> passThroughRows(agg)
    }.groupBy(_._1).map { case (n, pairs) => n -> pairs.map(_._2).sum }
    // The pure `PartialMerge` de-duplication phase (grouping on key + distinct columns) is the one
    // phase the `exists(_.mode == Partial)` guard exists to exclude, and only when it carries
    // non-distinct aggregates: with none at all its `aggregateExpressions` is empty, so the guard's
    // `isEmpty` disjunct admits it and only its required distribution keeps it out, and
    // `dedupPhases` below (which requires a non-empty `aggregateExpressions`) does not collect it.
    // It shares the 2-key bucket above with the leading `Partial` phase, so pin its ineligibility
    // directly.
    val dedupPhases = collect(df.queryExecution.executedPlan) {
      case agg: HashAggregateExec if agg.aggregateExpressions.nonEmpty &&
        agg.aggregateExpressions.forall(_.mode == PartialMerge) => agg
    }
    assert(dedupPhases.forall(!_.metrics.contains(passThroughRowsMetric)),
      "the pure-PartialMerge de-duplication phase must stay ineligible")
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

  test("collation-equivalent keys across the bypass cutoff stay in one group") {
    forEachCodegenAndMap() { clue =>
      // The first eight rows are distinct, so the eighth row flips pass-through on. Its key is
      // already in the frozen map when the equivalent spelling on the ninth row bypasses it.
      val df = () => spark.range(0, 200, 1, 1)
        .selectExpr(
          """CAST(CASE
            |  WHEN id = 7 THEN 'COLLATED'
            |  WHEN id = 8 THEN 'collated'
            |  ELSE CONCAT('key-', id)
            |END AS STRING COLLATE UTF8_LCASE) AS k""".stripMargin,
          "id AS v")
        .groupBy($"k")
        .agg(sum($"v") as "s", count(lit(1)) as "c")
      withClue(clue) {
        assert(numPassthroughRows(df) > 0,
          "expected the equivalent spelling after the cutoff to bypass partial aggregation")
      }
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
      val passedThrough = passThroughRows(df)
      assert(passedThrough > 0,
        s"expected the large frozen map to eventually bypass, got $passedThrough passed-through " +
          "rows")
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

  test("distinct with plain and filtered non-distinct aggregates") {
    // One query carries all three shapes through the DISTINCT intermediate phase
    // (`PartialMerge ++ Partial`): the distinct aggregate (`count(DISTINCT v)`), a plain
    // non-distinct aggregate (`sum(v)`), and a filtered non-distinct aggregate
    // (`avg(v) FILTER (...)`, whose `FILTER` is applied in the leading `Partial` phase only).
    // Fully distinct keys and values make neither partial phase reduce anything, so both bypass in
    // the same execution: asserting the 2-key phase (de-duplication, all `Partial`) and the 1-key
    // phase (distinct partial, `PartialMerge ++ Partial`) together proves the two bypasses coexist,
    // the plain and filtered non-distinct buffers pass through correctly, and the results still
    // match the feature-off reference.
    withTempView("t") {
      spark.range(0, 400, 1, 1)
        .select($"id".cast("string") as "k", $"id" as "v")
        .createOrReplaceTempView("t")
      forEachCodegenAndMap() { clue =>
        val df = () => spark.sql(
          """SELECT k,
            |       count(DISTINCT v) AS cd,
            |       sum(v) AS s,
            |       avg(v) FILTER (WHERE v > 25) AS a_gt25
            |FROM t GROUP BY k""".stripMargin)
        withClue(clue) {
          val byKeyCount = passThroughRowsByGroupingKeyCount(df)
          assert(byKeyCount.get(2).exists(_ > 0),
            s"expected the de-duplication partial (grouping on k, v) to bypass, got $byKeyCount")
          assert(byKeyCount.get(1).exists(_ > 0),
            s"expected the distinct partial (PartialMerge++Partial, grouping on k) to bypass, " +
              s"got $byKeyCount")
        }
      }
    }
  }

  test("distinct with an order-sensitive non-distinct aggregate across partitions") {
    // A single-partition `range` fuses the whole four-phase DISTINCT plan into one stage, so the
    // split-topology path (the frozen map draining one row per queued row, the queue flush, and
    // the `shouldStop()` re-entry) never runs for a `PartialMerge` member, including an
    // order-sensitive one. Deriving both grouping columns and varying the partition count forces
    // the exchanges; carrying `first`/`last` makes the pass-through's merge-into-an-empty-buffer
    // step observable. The forced-spill cells are omitted because the sort-based fallback reorders
    // `first`/`last` in the feature-off reference arm too.
    forEachCodegenAndMap() { clue =>
      Seq(1, 2).foreach { parts =>
        val df = () => spark.range(0, 400, 1, parts)
          .select(($"id" % 100).cast("string") as "k", ($"id" % 7) as "v", $"id" as "w")
          .groupBy($"k")
          .agg(countDistinct($"v") as "cd", sum($"w") as "s",
            first($"w") as "f", last($"w") as "l")
        withClue(s"$clue parts=$parts") {
          val byKeyCount = passThroughRowsByGroupingKeyCount(df)
          assert(byKeyCount.get(2).exists(_ > 0),
            s"expected the de-duplication partial (grouping on k, v) to bypass, got $byKeyCount")
          assert(byKeyCount.get(1).exists(_ > 0),
            s"expected the distinct partial (PartialMerge++Partial, grouping on k) to bypass, " +
              s"got $byKeyCount")
        }
      }
    }
  }

  test("an imperative aggregate stays correct in the distinct intermediate phase") {
    // `approx_count_distinct` uses `HyperLogLogPlusPlus`, an `ImperativeAggregate` whose buffer is
    // written by `initialize`/`merge` rather than a projection, so the distinct intermediate phase
    // runs on `TungstenAggregationIterator` (supportCodegen = false). Asserting both phases bypass
    // -- the de-duplication partial (grouping on `k` + `v`) and the distinct partial (grouping on
    // `k`, whose `PartialMerge` member is imperative) -- proves the imperative buffer is reset then
    // merged with the incoming buffer on pass-through, and the results still match the reference.
    forEachCodegenAndMap() { clue =>
      val df = () => spark.range(0, 400, 1, 1)
        .select(($"id" % 50).cast("string") as "k", ($"id" % 20) as "v", $"id" as "x")
        .groupBy($"k")
        .agg(approx_count_distinct($"x") as "acd", countDistinct($"v") as "cd")
      withClue(clue) {
        val byKeyCount = passThroughRowsByGroupingKeyCount(df)
        assert(byKeyCount.get(2).exists(_ > 0),
          s"expected the de-duplication partial (grouping on k, v) to bypass, got $byKeyCount")
        assert(byKeyCount.get(1).exists(_ > 0),
          s"expected the distinct partial (imperative PartialMerge, grouping on k) to bypass, " +
            s"got $byKeyCount")
      }
    }
  }

  test("distinct aggregation bypasses on high-cardinality input") {
    // The `PartialMerge` phase of the multi-phase distinct plan always aggregates (it requires a
    // distribution, so it is never eligible), so the rows reaching the distinct `Partial` phase
    // are de-duplicated and pass-through carries exactly one distinct value each.
    forEachCodegenAndMap() { clue =>
      val df = () => spark.range(0, 1000, 1, 1)
        .select(($"id" % 100).cast("string") as "k", $"id" as "v")
        .groupBy($"k")
        .agg(countDistinct($"v") as "cd")
      withClue(clue) {
        assert(numPassthroughRows(df) > 0,
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
        val byKeyCount = passThroughRowsByGroupingKeyCount(df)
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
        val byKeyCount = passThroughRowsByGroupingKeyCount(df)
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
        val byKeyCount = passThroughRowsByGroupingKeyCount(df)
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
  // high-cardinality input below an Expand`, cover an Expand that bypasses. The forced-spill
  // epochs of ROLLUP and CUBE compact too little to keep aggregating past a spill under the
  // default `minCompactionToSpill`, which rightly passes them through, so they pin it to 1.0 to
  // judge the spill check by `minCompaction` as well.

  test("results unchanged for ROLLUP (Expand below partial aggregate)") {
    // The grand-total group repeats on every expanded row, so the compaction ratio stays above the
    // threshold and the partial aggregate declines to bypass in every cell.
    withSQLConf(SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_COMPACTION_TO_SPILL.key -> "1.0") {
      checkAdaptiveMatchesReference(
        expectBypass = false,
        build = { parts =>
          spark.range(0, 400, 1, parts)
            .select(($"id" % 200) as "k1", ($"id" % 7) as "k2", $"id" as "v")
            .rollup($"k1", $"k2")
            .agg(sum($"v") as "s", count(lit(1)) as "c")
        })
    }
  }

  test("results unchanged for CUBE (Expand below partial aggregate)") {
    // Same as ROLLUP: the grand-total group keeps the ratio above the threshold, so nothing
    // bypasses.
    withSQLConf(SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_COMPACTION_TO_SPILL.key -> "1.0") {
      checkAdaptiveMatchesReference(
        expectBypass = false,
        build = { parts =>
          spark.range(0, 400, 1, parts)
            .select(($"id" % 150) as "k1", ($"id" % 5) as "k2", $"id" as "v")
            .cube($"k1", $"k2")
            .agg(sum($"v") as "s", count(lit(1)) as "c")
        })
    }
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
        assert(numPassthroughRows(highCard) > 0,
          "expected some rows to bypass partial aggregation for high-cardinality input")
      }
      // Few distinct keys, high reduction: partial aggregation is effective, nothing bypasses.
      val lowCard = () => spark.range(0, 600, 1, 1)
        .select(($"id" % 5).cast("string") as "k", $"id" as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s")
      withClue(clue) {
        assert(numPassthroughRows(lowCard) == 0,
          "expected no rows to bypass partial aggregation for low-cardinality input")
      }
    }
  }

  test("group-by-only pass-through fires for high-cardinality input") {
    forEachCodegenAndMap() { clue =>
      val distinctKeys = () => spark.range(0, 200, 1, 1).select($"id" as "k").distinct()
      withClue(clue) {
        assert(numPassthroughRows(distinctKeys) > 0,
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
          assert(numPassthroughRows(df) > 0,
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
          assert(numPassthroughRows(gs) > 0,
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
          assert(numPassthroughRows(df) == 0,
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
        assert(numPassthroughRows(df) == 0,
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
        assert(numPassthroughRows(df) == 0,
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
        assert(c.passedThrough == 32,
          s"expected the 32 rows after the flip to bypass, got ${c.passedThrough}")
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
        assert(c.passedThrough > 0,
          "the periodic check should bypass fully distinct input")
        assert(c.spillBytes == 0, "the periodic check must decide before any spill happens")
        assert(c.tasksFallBacked == 0,
          "the periodic check must not fall back to sort-based aggregation")
      }
    }
  }

  test("the spill check does not bypass an epoch smaller than minRows") {
    // The spill check, like the periodic check, judges an epoch only once it holds `minRows` rows,
    // because the bypass cannot be undone within the task. Force the regular map to fall back
    // every 16 rows with `minRows = 32`: every epoch is below the floor, so even fully distinct
    // input must spill and fall back to sort-based aggregation instead of bypassing. The 200-row
    // input as a whole exceeds `minRows`, so this also pins that the floor counts the rows of the
    // current epoch rather than of the task, which would bypass at the second spill.
    forEachCodegenAndMap(minRows = 32, regularFallback = 16) { clue =>
      val df = () => spark.range(0, 200, 1, 1)
        .select($"id".cast("string") as "k", $"id" as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s")
      withClue(clue) {
        val c = runAndReadCounters(df)
        assert(c.passedThrough == 0,
          "the spill check must not bypass an epoch smaller than minRows")
        assert(c.tasksFallBacked > 0,
          "an epoch below the minRows floor must spill and fall back to sort")
      }
    }
  }

  test("the spill check bypasses between periodic checks once minRows rows are processed") {
    // With `minRows = 10` the periodic check runs at the 10th aggregated row, when ids 0-9 have
    // collapsed into two keys (ratio 5.0, above `minCompaction`), so it keeps aggregating. Ids
    // 10-18 then add nine distinct keys, and the forced fallback makes the map full when id 19
    // arrives. That is past the floor but before the next periodic check at 20 rows. With a single
    // periodic check so far, the spill check judges the whole epoch, and 19 rows over 11 keys is
    // below `minCompactionToSpill = 2.0`, so it bypasses at exactly that row: ids 19-399 stream
    // through and the sorter is never created.
    Seq(true, false).foreach { wholeStage =>
      withSQLConf(
        (Seq(
          SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> "true",
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> wholeStage.toString,
          SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> "false",
          SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_ROWS.key -> "10",
          SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_COMPACTION_TO_SPILL.key -> "2.0",
          "spark.sql.TungstenAggregate.testFallbackStartsAt" -> "1, 19") ++
          fixedPlanConfs): _*) {
        val df = () => spark.range(0, 400, 1, 1)
          .select(when($"id" < 10, $"id" % 2).otherwise($"id").cast("string") as "k",
            $"id" as "v")
          .groupBy($"k")
          .agg(sum($"v") as "s")
        withClue(s"wholeStage=$wholeStage") {
          val c = runAndReadCounters(df)
          assert(c.passedThrough == 400 - 19,
            "the spill check should bypass from the row that found the map full")
          // The spill check bypasses *instead of* falling back to sort-based aggregation, so the
          // sorter is never created. `numTasksFallBacked` is the reliable per-operator signal for
          // that (the `spillSize` metric on the interpreted path is derived from the
          // task-cumulative memory-spill counter and can be inflated by unrelated spilling such as
          // the downstream shuffle write, so it is not asserted here).
          assert(c.tasksFallBacked == 0,
            "the spill check must replace the sort fallback, not trigger it")
        }
      }
    }
  }

  test("the spill check passes through an epoch that compacts too little to spill") {
    // Keys `floor(2 * id / 3)` collapse every three rows into two keys, a steady ratio of about
    // 1.5 with no key repeating once its rows have passed. That is above `minCompaction`, so the
    // periodic checks at 10, 20 and 30 rows keep aggregating. The forced fallback fills the map
    // when the next row arrives, and the spill check judges the rows since the check before last,
    // which compact about the same, 1.4 to 1.7. Keeping the aggregation would aggregate the rest
    // through spills for that little, so the default `minCompactionToSpill` (2.0) passes the rest
    // through instead and the sorter is never created. Lowering it to 1.3 keeps the old behavior:
    // every epoch spills, and nothing is passed through. Two input partitions put an `Exchange`
    // between the Partial and the Final, so passed-through rows go through the output buffer and
    // the resumed build; one partition fuses the two.
    for {
      (minCompactionToSpill, shouldBypass) <- Seq(None -> true, Some("1.3") -> false)
      partitions <- Seq(1, 2)
      wholeStage <- Seq(true, false)
    } {
      withSQLConf(
        (Seq(
          SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> "true",
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> wholeStage.toString,
          SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> "false",
          SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_ROWS.key -> "10",
          "spark.sql.TungstenAggregate.testFallbackStartsAt" -> "1, 30") ++
          minCompactionToSpill.map(
            SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_COMPACTION_TO_SPILL.key -> _) ++
          fixedPlanConfs): _*) {
        val df = () => spark.range(0, 400, 1, partitions)
          .select(($"id" * 2 / 3).cast("long").cast("string") as "k", $"id" as "v")
          .groupBy($"k")
          .agg(sum($"v") as "s")
        withClue(s"minCompactionToSpill=$minCompactionToSpill partitions=$partitions " +
            s"wholeStage=$wholeStage") {
          val c = runAndReadCounters(df)
          if (shouldBypass) {
            assert(c.passedThrough == 400 - 30 * partitions,
              "each task should pass through from the row that found its map full")
            assert(c.tasksFallBacked == 0,
              "the spill check must replace the sort fallback, not trigger it")
          } else {
            assert(c.passedThrough == 0,
              "an epoch above minCompactionToSpill must keep aggregating")
            assert(c.tasksFallBacked > 0, "an epoch that keeps aggregating must spill")
          }
        }
      }
    }
  }

  test("the spill check keeps aggregating when the recent rows repeat keys") {
    // Ids 0-35 map to `floor(2 * id / 3)`, keys 0-23, and ids 36-53 to `id % 20`, keys the map
    // already holds. The periodic checks at 10, 20, 30 and 40 rows see ratios from 1.4 to 1.7, so
    // they keep aggregating, and the forced fallback fills the map when id 45 arrives, after 45
    // rows over 24 keys. That ratio of 1.875 is below `minCompactionToSpill`, but the spill check
    // judges the rows since the check before last: the 15 rows since the check at 30 rows added
    // only 4 keys, a ratio of 3.75, because the rest found their key in the map. So the map spills
    // and keeps aggregating, and the last 9 rows are too few for another check. Demanding 4.0 at
    // the spill check passes those 9 rows through instead.
    for {
      (minCompactionToSpill, passedThrough) <- Seq(None -> 0, Some("4.0") -> 9)
      wholeStage <- Seq(true, false)
    } {
      withSQLConf(
        (Seq(
          SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> "true",
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> wholeStage.toString,
          SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> "false",
          SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_ROWS.key -> "10",
          "spark.sql.TungstenAggregate.testFallbackStartsAt" -> "1, 45") ++
          minCompactionToSpill.map(
            SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_COMPACTION_TO_SPILL.key -> _) ++
          fixedPlanConfs): _*) {
        val df = () => spark.range(0, 54, 1, 1)
          .select(when($"id" < 36, ($"id" * 2 / 3).cast("long")).otherwise($"id" % 20)
            .cast("string") as "k", $"id" as "v")
          .groupBy($"k")
          .agg(sum($"v") as "s")
        withClue(s"minCompactionToSpill=$minCompactionToSpill wholeStage=$wholeStage") {
          val c = runAndReadCounters(df)
          assert(c.passedThrough == passedThrough)
          assert((c.tasksFallBacked > 0) == (passedThrough == 0),
            "the map spills exactly when the spill check keeps aggregating")
        }
      }
    }
  }

  test("a new in-memory map epoch after a spill can still bypass") {
    // A spill starts a new in-memory map epoch and restarts the row counters, so an input whose
    // cardinality only turns unfavorable after an early spill is still caught. The first 80 rows
    // repeat 5 keys and keep the aggregation effective while the forced fallback makes the map
    // spill at ids 40 and 80; the remaining 320 rows are fully distinct, so the third epoch's
    // first periodic check, at its 8th row (id 87), judges it ineffective and the rest of the
    // input is passed through. With the two-level map on the codegen path, the fast map absorbs
    // keys 0-3, so each forced spill waits for the next row that needs the regular map, and the
    // third epoch starts at id 84 instead. The exact count also pins that the check row restarts
    // with the epoch. `minCompactionToSpill` is pinned to 1.5 so the count does not rest on a tie:
    // in the two-level codegen cell, the window when id 84 finds the map full holds 8 rows over 4
    // keys, exactly the default 2.0.
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
          SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_COMPACTION_TO_SPILL.key -> "1.5",
          "spark.sql.TungstenAggregate.testFallbackStartsAt" -> s"$fastCap, 40") ++
          fixedPlanConfs): _*) {
        val df = () => spark.range(0, 400, 1, 1)
          .select(when($"id" < 80, $"id" % 5).otherwise($"id") as "k", $"id" as "v")
          .groupBy($"k")
          .agg(sum($"v") as "s")
        withClue(s"wholeStage=$wholeStage twoLevelMap=$twoLevelMap") {
          val c = runAndReadCounters(df)
          assert(c.tasksFallBacked > 0,
            "the low-cardinality prefix should still spill and fall back to sort")
          val firstPassedThrough = if (wholeStage && twoLevelMap) 92 else 88
          assert(c.passedThrough == 400 - firstPassedThrough,
            "the high-cardinality tail after the spills should be passed through")
        }
      }
    }
  }

  test("without the feature the same input really does fall back to sort") {
    // Sanity check for the spill check assertions: with adaptive disabled, the high-cardinality
    // input that "minRows = 0 still bypasses at the spill boundary" uses, under the same forced
    // fallback, genuinely falls back to sort-based aggregation. This proves that test's
    // `tasksFallBacked == 0` reflects the bypass and not merely an input that never reached the
    // spill boundary.
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
          assert(c.passedThrough == 0, "feature disabled: nothing should bypass")
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
    // aggregation is kept (`50 < 40 * 1.25` is false), demanding 1.3 it is bypassed. The spill
    // check demands the larger of `minCompaction` and `minCompactionToSpill`, so either one can
    // set the boundary, from either side.
    Seq(
      (1.0, 1.25, false),
      (1.0, 1.3, true),
      (1.25, 1.0, false),
      (1.3, 1.0, true)
    ).foreach { case (minCompaction, minCompactionToSpill, shouldBypass) =>
      val passedThroughPerCodegen = Seq(true, false).map { wholeStage =>
        withSQLConf(
          (Seq(
            SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> "true",
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> wholeStage.toString,
            SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> "false",
            // `minRows = 0` keeps the periodic check out of the picture and leaves the spill check
            // without a row floor and judging the whole epoch, so it alone decides at the boundary.
            SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_ROWS.key -> "0",
            SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_COMPACTION.key -> minCompaction.toString,
            SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_COMPACTION_TO_SPILL.key ->
              minCompactionToSpill.toString,
            "spark.sql.TungstenAggregate.testFallbackStartsAt" -> "1, 50") ++
            fixedPlanConfs): _*) {
          val df = () => spark.range(0, 400, 1, 1)
            .select(($"id" % 40).cast("string") as "k", $"id" as "v")
            .groupBy($"k")
            .agg(sum($"v") as "s")
          withClue(s"minCompaction=$minCompaction minCompactionToSpill=$minCompactionToSpill " +
              s"wholeStage=$wholeStage") {
            numPassthroughRows(df)
          }
        }
      }
      withClue(s"minCompaction=$minCompaction minCompactionToSpill=$minCompactionToSpill " +
          s"passedThrough=$passedThroughPerCodegen") {
        assert(passedThroughPerCodegen.forall(_ > 0) == shouldBypass,
          s"expected bypass=$shouldBypass at the ratio boundary")
        assert(passedThroughPerCodegen.map(_ > 0).distinct.length == 1,
          "codegen and interpreted paths must reach the same decision at the boundary")
      }
    }
  }

  test("the spill check judges the recent rows at their exact ratio boundary") {
    // With `minRows = 10`, ids 0-9 add 9 keys and ids 10-29 add 10 more, two rows each, so when
    // the forced fallback fills the map at 30 rows, the rows since the check before last (at 10
    // rows) are 20 rows over 10 keys, a ratio of exactly 2.0, while the whole epoch compacts by
    // only 30 / 19. Demanding 2.0 the aggregation is kept (`20 < 10 * 2.0` is false) and the map
    // spills; demanding 2.1 the last 10 rows are passed through. A check over the whole epoch
    // would pass them through at both.
    for {
      (minCompactionToSpill, passedThrough) <- Seq(2.0 -> 0, 2.1 -> 10)
      wholeStage <- Seq(true, false)
    } {
      withSQLConf(
        (Seq(
          SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> "true",
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> wholeStage.toString,
          SQLConf.ENABLE_TWOLEVEL_AGG_MAP.key -> "false",
          SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_ROWS.key -> "10",
          SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_COMPACTION_TO_SPILL.key ->
            minCompactionToSpill.toString,
          "spark.sql.TungstenAggregate.testFallbackStartsAt" -> "1, 30") ++
          fixedPlanConfs): _*) {
        val df = () => spark.range(0, 40, 1, 1)
          .select(when($"id" < 9, $"id").when($"id" === 9, 0L)
            .otherwise(($"id" - 10) / 2 + 9).cast("long").cast("string") as "k", $"id" as "v")
          .groupBy($"k")
          .agg(sum($"v") as "s")
        withClue(s"minCompactionToSpill=$minCompactionToSpill wholeStage=$wholeStage") {
          val c = runAndReadCounters(df)
          assert(c.passedThrough == passedThrough)
          assert((c.tasksFallBacked > 0) == (passedThrough == 0),
            "the map spills exactly when the spill check keeps aggregating")
        }
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
        assert(numPassthroughRows(df) > 0,
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
        assert(numPassthroughRows(df) == 0,
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
        assert(c.passedThrough > 0,
          "the spill check must still bypass when the periodic check is disabled")
        assert(c.tasksFallBacked == 0,
          "the spill check must replace the sort fallback, not trigger it")
      }
    }
  }

  test("the compaction thresholds reject values that cannot be generated as a Java literal") {
    // The threshold is interpolated straight into the generated source, where a non-finite double
    // renders as `InfinityD` -- not a valid Java literal. Reject it at configuration time rather
    // than failing to compile the stage (or silently deoptimizing when codegen falls back).
    for {
      key <- Seq(SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_COMPACTION.key,
        SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_COMPACTION_TO_SPILL.key)
      v <- Seq("Infinity", "-Infinity", "NaN", "1e309")
    } {
      val e = intercept[IllegalArgumentException] {
        spark.conf.set(key, v)
      }
      assert(e.getMessage.contains("finite"), s"expected a finiteness error for $key='$v': $e")
      spark.conf.unset(key)
    }
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
        assert(numPassthroughRows(df) == 0,
          "no bypass expected before `minRows` rows have been processed")
      }
    }
  }

  test("a split adaptive aggregate times only its build, never the pass-through rows") {
    // Split by an exchange, a passed-through row returns from the build and re-enters it, so a
    // timer around the resumed build or around each passed-through row would cost two
    // `System.nanoTime()` calls per row. The generated code of the partial aggregate's stage, whose
    // other operators take no time, must hold only the build timer: its start, and its stop in the
    // map setup of the output function, which runs once, before the aggregate outputs anything.
    withSQLConf(
      (Seq(
        SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> "true",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_MIN_ROWS.key -> "8") ++ fixedPlanConfs): _*) {
      val df = spark.range(0, 400, 1, 2)
        .select($"id".cast("string") as "k", $"id" as "v")
        .groupBy($"k")
        .agg(sum($"v") as "s")
      df.collect()
      assert(passThroughRows(df) > 0, "the high-cardinality input should be passed through")
      val partialStages = collect(df.queryExecution.executedPlan) {
        case stage: WholeStageCodegenExec if (stage.child match {
          case agg: HashAggregateExec => isPartialPhase(agg)
          case _ => false
        }) => stage
      }
      assert(partialStages.length == 1, "expected the partial aggregate in a stage of its own")
      val code = partialStages.head.doCodeGen()._2.body
      def timerCalls(code: String): Seq[String] =
        code.split("\n").filter(_.contains("System.nanoTime()")).toSeq
      assert(timerCalls(code).length == 2, timerCalls(code).mkString("\n"))
      val outputFunction = ("(?s)private void \\w*outputMapAndFlush_\\d+\\(\\).*?" +
        "(?=\\n\\s*(private|protected|public) |\\z)").r.findFirstIn(code)
      assert(outputFunction.exists(timerCalls(_).length == 1),
        "the map setup in the output function should stop the build timer")
    }
  }

  test("partial aggregate output row count reflects the bypass (independent of the pass-through " +
      "metric)") {
    // `numOutputRows` on the partial aggregate is the ground truth: it is driven by the normal
    // aggregation output path, not by our self-reported `numPassthroughRows` metric. This test
    // cross checks the two and pins the observable data-side effect of bypassing.
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
        assert(c.passedThrough > 0, "high-cardinality input should bypass")
        // Every partial output row is either a real (aggregated) group or a bypassed row, so the
        // partial output count must be at least the number of passed-through rows, and it climbs
        // toward the input row count -- well above the heavy reduction a kept aggregation would
        // give.
        assert(c.partialOutputRows >= c.passedThrough, s"partial output ${c.partialOutputRows} " +
          s"should be >= passed-through rows ${c.passedThrough}")
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
        assert(c.passedThrough == 0, "low-cardinality input should not bypass")
        assert(c.partialOutputRows == 5,
          "an effective partial aggregate should emit exactly the distinct key count")
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // Part 3: Generated code -- the layout that bounds the pass-through code size.
  /////////////////////////////////////////////////////////////////////////////

  test("pass-through resets its buffer with copyFrom from buffers registered once") {
    // Each passed-through row resets a reusable buffer with one `copyFrom` of the initial
    // aggregation buffer rather than projecting the initial values again, so the per-row code does
    // not grow with the number of buffer slots. Under a fused Union the consume code is generated
    // once per child, so each child resets the buffer, but all of them must share the same pair
    // of buffers rather than registering a pair each.
    withSQLConf(
      (Seq(
        SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_ENABLED.key -> "true",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
        SQLConf.WHOLESTAGE_UNION_CODEGEN_ENABLED.key -> "true") ++ fixedPlanConfs): _*) {
      val df = spark.range(0, 100, 1, 2)
        .union(spark.range(100, 200, 1, 2))
        .groupBy("id")
        .agg(sum($"id") as "s", avg($"id") as "a", max($"id") as "m")
      df.collect()
      val code = collect(df.queryExecution.executedPlan) {
        case w: WholeStageCodegenExec => w.doCodeGen()._2.body
      }.mkString("\n")
      val resets = """(\w+)\.copyFrom\((\w+)\);""".r.findAllMatchIn(code)
        .map(m => (m.group(1), m.group(2)))
        .filter(_._2.contains("adaptiveEmptyBuffer"))
        .toSeq
      assert(resets.length == 2,
        s"expected one reset per child of the fused Union, got ${resets.length}:\n$code")
      assert(resets.distinct.length == 1,
        s"expected every child to reset the same pair of buffers, got ${resets.distinct}")
    }
  }
}
