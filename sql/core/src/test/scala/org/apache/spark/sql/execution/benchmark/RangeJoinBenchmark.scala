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

package org.apache.spark.sql.execution.benchmark

import java.sql.Date

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.planning.{GreaterPartialRangeJoin, IntervalOverlapJoin, LessPartialRangeJoin, PointInRangeJoin, RangeJoinKind}
import org.apache.spark.sql.execution.joins.{BroadcastNestedLoopJoinExec, BroadcastRangeJoinExec}
import org.apache.spark.sql.functions.{broadcast, col, date_add, lit, lpad, when}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType

/**
 * Benchmark broadcast range join against broadcast nested loop join.
 *
 * Long keys cross every supported shape with every supported join type.
 * Inner joins broadcast each side once. Left anti probes values that match
 * nothing. A partial range whose bound sits at the median is not selective.
 * Wide interval overlap is mostly selective: a probe overlaps a build
 * interval only when |i - j| <= 20, so about 2% of the 100,000 probes hit
 * up to 41 of the 2,000 build intervals each and the rest hit nothing.
 * Point-in-range inner is repeated for date, timestamp, decimal,
 * and UNICODE_CI strings.
 *
 * More cases probe the cost model directly: a streamed side of 1, 10, 100,
 * 1000, 5000, or 10000 rows, where range join pays to sort the whole build
 * side instead of one nested-loop scan, and a fully overlapping inner join,
 * where every pair matches and the index prunes nothing.
 *
 * Staggered high-overlap builds cover the regime between those: most, but not
 * all, build intervals overlap each probe window, miss intervals are
 * interleaved so no subtree's max high falls under the probe, and the build
 * has no common core, so the tree is walked without pruning and evaluated per
 * candidate. Long and double (boxed) keys both run at 50% and 90% overlap.
 *
 * To run this benchmark:
 * {{{
 *   1. build/sbt "sql/Test/runMain org.apache.spark.sql.execution.benchmark.RangeJoinBenchmark"
 *   2. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt \
 *        "sql/Test/runMain org.apache.spark.sql.execution.benchmark.RangeJoinBenchmark"
 *      Results will be written to "benchmarks/RangeJoinBenchmark-results.txt".
 * }}}
 */
object RangeJoinBenchmark extends SqlBasedBenchmark {

  // An optional regex in the environment variable RANGE_BENCH_FILTER runs only the
  // sections whose name matches, e.g. RANGE_BENCH_FILTER=".*tiny probe.*|.*full overlap.*".
  // A workaround for sbt splitting runMain arguments on spaces.
  private var nameFilter: Option[String] = Option(System.getenv("RANGE_BENCH_FILTER"))

  private def shouldRun(name: String): Boolean = nameFilter.forall(name.matches)

  private val buildRows = 4000
  private val probeRows = 400000
  private val width = 10
  // Wide overlap is measured over 100,000 probes and 2,000 build intervals.
  private val heavyBuildRows = 2000
  private val heavyProbeRows = 100000
  private val halfBuildRows = 1000
  private val halfProbeRows = 10000
  private val wideSpan = 20 * width

  private val epoch = lit(Date.valueOf("1970-01-01"))

  // [0, buildRows * width). Each range covers `width` values and none overlap.
  private lazy val ranges: DataFrame = cached(spark.range(buildRows).select(
    (col("id") * width).as("lo"),
    (col("id") * width + (width - 1)).as("hi")))

  // One hit per point: the value falls in exactly one range above.
  private lazy val points: DataFrame = cached(spark.range(probeRows).select(
    (col("id") % (buildRows.toLong * width)).as("p")))

  // Above every range, so a point-in-range probe matches nothing.
  private lazy val misses: DataFrame = cached(spark.range(probeRows).select(
    (lit(buildRows.toLong * width) + (col("id") % width)).as("p")))

  // Short intervals. A probe overlaps the build intervals whose starts are near it.
  private lazy val probeIntervals: DataFrame =
    cached(intervals(probeRows, "a", width + width / 2))
  private lazy val buildIntervals: DataFrame =
    cached(intervals(buildRows, "b", width + width / 2))
  // Starts after every build interval.
  private lazy val missIntervals: DataFrame = cached(spark.range(probeRows).select(
    (lit(buildRows.toLong * width + width) + col("id") * width).as("alo"),
    (lit(buildRows.toLong * width + width * 2) + col("id") * width).as("ahi")))

  // Mostly selective: a probe overlaps a build interval only when |i - j| <= 20,
  // so about 2% of the 100,000 probes hit up to 41 of the 2,000 build intervals
  // each and the rest hit nothing (see the class doc above).
  private lazy val wideProbe: DataFrame = cached(intervals(heavyProbeRows, "a", wideSpan))
  private lazy val wideBuild: DataFrame = cached(intervals(heavyBuildRows, "b", wideSpan))

  // Build keys 0 .. buildRows - 1. High probes sit at the top, low probes at the bottom,
  // so a selective inequality returns only a few candidates.
  private lazy val bounds: DataFrame = cached(spark.range(buildRows).select(col("id").as("b")))
  private lazy val highProbes: DataFrame = cached(spark.range(probeRows).select(
    (lit(buildRows - 1) - (col("id") % width)).as("a")))
  private lazy val lowProbes: DataFrame = cached(spark.range(probeRows).select(
    (col("id") % width).as("a")))
  // Strictly above every bound, so `a < b` matches nothing.
  private lazy val lessMisses: DataFrame = cached(spark.range(probeRows).select(
    lit(buildRows.toLong).as("a")))
  // `a > b` matches nothing when every bound is non-negative.
  private lazy val greaterMisses: DataFrame = cached(spark.range(probeRows).select(lit(0L).as("a")))

  // `a` is the median bound, so each inequality matches about half the build side.
  private lazy val halfBounds: DataFrame =
    cached(spark.range(halfBuildRows).select(col("id").as("b")))
  private lazy val halfProbes: DataFrame = cached(spark.range(halfProbeRows).select(
    lit(halfBuildRows.toLong / 2).as("a")))

  // M approx 1, 10, 100, 1000, 5000, and 10000. The build side still has to be
  // sorted to answer a few streamed probes, so this weighs that one-time O(N log N)
  // cost against one O(N) nested-loop scan of the same build side, and shows how
  // fast that cost amortizes
  // as M grows just past one.
  private val tinyProbeRowCounts = Seq(1, 10, 100, 1000, 5000, 10000)
  private lazy val tinyPoints: Map[Int, DataFrame] = tinyProbeRowCounts.map { n =>
    n -> cached(spark.range(n).select((col("id") % (buildRows.toLong * width)).as("p")))
  }.toMap
  private lazy val tinyBounds: Map[Int, DataFrame] = tinyProbeRowCounts.map { n =>
    n -> cached(spark.range(n).select(lit(buildRows.toLong / 2).as("a")))
  }.toMap

  // K approx N. Every build range covers the whole probe domain, so every probe
  // matches every build row: an inner join emits all N * M pairs and the index
  // prunes nothing, only adding its build cost on top of the join itself.
  private val fullBuildRows = 1000
  private val fullProbeRows = 2000
  private lazy val fullRanges: DataFrame = cached(spark.range(fullBuildRows).select(
    lit(0L).as("lo"),
    lit(fullBuildRows.toLong * width).as("hi")))
  private lazy val fullPoints: DataFrame = cached(spark.range(fullProbeRows).select(
    (col("id") % (fullBuildRows.toLong * width)).as("p")))

  // Sweep the whole broadcast budget for cache behavior: the walk touches
  // three long arrays far beyond any cache, and the sort and copy dominate
  // the build side.
  private val largeBuildRows = 1000000
  private val largeProbeRows = 10000
  private lazy val largeRanges: DataFrame = cached(spark.range(largeBuildRows).select(
    (col("id") * width).as("lo"),
    (col("id") * width + (width - 1)).as("hi")))
  private lazy val largePoints: DataFrame = cached(spark.range(largeProbeRows).select(
    ((col("id") * width + width / 2) % lit(largeBuildRows.toLong * width)).as("p")))

  // Staggered high-overlap builds. Every miss interval ends before the probe
  // window starts and every keep interval spans it, but the two interleave in
  // low order, so no subtree's max high falls under the probe and the walk
  // prunes nothing. The build has no common core either, so no shortcut can
  // answer the probe: this measures the full walk plus K candidate checks
  // against nested loop's B condition checks, at K/B of 50% and 90%.
  private val staggerBuildRows = 2000
  private val staggerProbeRows = 10000
  private def staggerBuild(missEvery: Int, boxedAsDouble: Boolean = false): DataFrame = {
    val raw = spark.range(staggerBuildRows).select(
      (col("id") * lit(10L)).as("blo"),
      when(col("id") % lit(missEvery.toLong) === lit(0L),
        col("id") * lit(10L) + lit(9000L))
        .otherwise(col("id") * lit(10L) + lit(40000L)).as("bhi"))
    cached(if (boxedAsDouble) raw.select(
      col("blo").cast("double").as("blo"),
      col("bhi").cast("double").as("bhi")) else raw)
  }
  private def staggerProbe(boxedAsDouble: Boolean = false): DataFrame = {
    val raw = spark.range(staggerProbeRows).select(
      (lit(30000L) + (col("id") % lit(3L))).as("alo"),
      (lit(30010L) + (col("id") % lit(3L))).as("ahi"))
    cached(if (boxedAsDouble) raw.select(
      col("alo").cast("double").as("alo"),
      col("ahi").cast("double").as("ahi")) else raw)
  }
  private lazy val staggerBuild50: DataFrame = staggerBuild(missEvery = 2)
  private lazy val staggerBuild90: DataFrame = staggerBuild(missEvery = 10)
  private lazy val staggerBuild50Double: DataFrame = staggerBuild(2, boxedAsDouble = true)
  private lazy val staggerBuild90Double: DataFrame = staggerBuild(10, boxedAsDouble = true)
  private lazy val staggerProbeLong: DataFrame = staggerProbe()
  private lazy val staggerProbeDouble: DataFrame = staggerProbe(boxedAsDouble = true)

  private lazy val dateRanges: DataFrame = cached(ranges.select(
    date_add(epoch, col("lo").cast("int")).as("lo"),
    date_add(epoch, col("hi").cast("int")).as("hi")))
  private lazy val datePoints: DataFrame = cached(points.select(
    date_add(epoch, col("p").cast("int")).as("p")))

  private lazy val timestampRanges: DataFrame = cached(ranges.select(
    (col("lo") * lit(1000000L)).cast("timestamp").as("lo"),
    (col("hi") * lit(1000000L)).cast("timestamp").as("hi")))
  private lazy val timestampPoints: DataFrame = cached(points.select(
    (col("p") * lit(1000000L)).cast("timestamp").as("p")))

  private lazy val decimalRanges: DataFrame = cached(ranges.select(
    col("lo").cast("decimal(18, 2)").as("lo"),
    col("hi").cast("decimal(18, 2)").as("hi")))
  private lazy val decimalPoints: DataFrame = cached(points.select(
    col("p").cast("decimal(18, 2)").as("p")))

  private lazy val stringRanges: DataFrame = cached(ranges.select(
    collated(col("lo")).as("lo"),
    collated(col("hi")).as("hi")))
  private lazy val stringPoints: DataFrame = cached(points.select(collated(col("p")).as("p")))

  private def cached(df: DataFrame): DataFrame = {
    val stored = df.cache()
    stored.count()
    stored
  }

  private def intervals(rows: Int, prefix: String, span: Int): DataFrame = {
    spark.range(rows).select(
      (col("id") * width).as(s"${prefix}lo"),
      (col("id") * width + span).as(s"${prefix}hi"))
  }

  // Zero-padded so collation order matches numeric order.
  private def collated(value: Column): Column =
    lpad(value.cast("string"), 8, "0").cast(StringType("UNICODE_CI"))

  private def pointCond: Column = col("p") >= col("lo") && col("p") <= col("hi")

  private def overlapCond: Column = col("alo") <= col("bhi") && col("blo") <= col("ahi")

  private def run(df: => DataFrame, rangeEnabled: Boolean, shape: String): Unit = {
    withSQLConf(
        SQLConf.BROADCAST_RANGE_JOIN_ENABLED.key -> rangeEnabled.toString,
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val planned = df
      val plan = planned.queryExecution.sparkPlan
      if (rangeEnabled) {
        val found = plan.collect { case j: BroadcastRangeJoinExec => shapeName(j.rangeJoinKind) }
        assert(found == Seq(shape), plan)
      } else {
        assert(plan.exists(_.isInstanceOf[BroadcastNestedLoopJoinExec]), plan)
      }
      planned.noop()
    }
  }

  /** The tag name without a case class's arguments, for Shape assertions. */
  private def shapeName(rangeJoinKind: RangeJoinKind): String = rangeJoinKind match {
    case PointInRangeJoin => "PointInRangeJoin"
    case IntervalOverlapJoin => "IntervalOverlapJoin"
    case _: LessPartialRangeJoin => "LessPartialRangeJoin"
    case _: GreaterPartialRangeJoin => "GreaterPartialRangeJoin"
  }

  private def compare(name: String, rows: Long, shape: String)(df: => DataFrame): Unit = {
    if (!shouldRun(name)) return
    val benchmark = new Benchmark(name, rows, output = output)
    benchmark.addCase("Broadcast nested loop join") { _ =>
      run(df, rangeEnabled = false, shape)
    }
    benchmark.addCase("Broadcast range join") { _ =>
      run(df, rangeEnabled = true, shape)
    }
    benchmark.run()
  }

  private def prepare(): Unit = {
    ranges
    points
    misses
    probeIntervals
    buildIntervals
    missIntervals
    wideProbe
    wideBuild
    bounds
    highProbes
    lowProbes
    lessMisses
    greaterMisses
    halfBounds
    halfProbes
    tinyPoints
    tinyBounds
    fullRanges
    fullPoints
    largeRanges
    largePoints
    dateRanges
    datePoints
    timestampRanges
    timestampPoints
    decimalRanges
    decimalPoints
    stringRanges
    stringPoints
    staggerBuild50
    staggerBuild90
    staggerBuild50Double
    staggerBuild90Double
    staggerProbeLong
    staggerProbeDouble
  }

  private def pointCases(): Unit = {
    val n = s"probe=$probeRows, ranges=$buildRows"
    compare(s"point-in-range inner, build ranges ($n)", probeRows, "PointInRangeJoin") {
      points.join(broadcast(ranges), pointCond, "inner")
    }
    // The index is degenerate points. The streamed side probes with (lo, hi).
    compare(s"point-in-range inner, build points ($n)", buildRows, "PointInRangeJoin") {
      broadcast(points).join(ranges, pointCond, "inner")
    }
    compare(s"point-in-range left outer ($n)", probeRows, "PointInRangeJoin") {
      points.join(broadcast(ranges), pointCond, "left")
    }
    compare(s"point-in-range right outer ($n)", probeRows, "PointInRangeJoin") {
      broadcast(ranges).join(points, pointCond, "right")
    }
    compare(s"point-in-range left semi ($n)", probeRows, "PointInRangeJoin") {
      points.join(broadcast(ranges), pointCond, "leftsemi")
    }
    compare(s"point-in-range left anti ($n)", probeRows, "PointInRangeJoin") {
      misses.join(broadcast(ranges), pointCond, "leftanti")
    }
    tinyProbeRowCounts.foreach { n =>
      compare(
          s"point-in-range inner, tiny probe (probe=$n, ranges=$buildRows)",
          buildRows, "PointInRangeJoin") {
        tinyPoints(n).join(broadcast(ranges), pointCond, "inner")
      }
    }
    compare(
        s"point-in-range inner, full overlap (probe=$fullProbeRows, ranges=$fullBuildRows)",
        fullProbeRows, "PointInRangeJoin") {
      fullPoints.join(broadcast(fullRanges), pointCond, "inner")
    }
    compare(
        s"point-in-range inner, large build (probe=$largeProbeRows, ranges=$largeBuildRows)",
        largeProbeRows, "PointInRangeJoin") {
      largePoints.join(broadcast(largeRanges), pointCond, "inner")
    }
  }

  private def intervalCases(): Unit = {
    val n = s"probe=$probeRows, ranges=$buildRows"
    compare(s"interval overlap inner, build right ($n)", probeRows, "IntervalOverlapJoin") {
      probeIntervals.join(broadcast(buildIntervals), overlapCond, "inner")
    }
    compare(s"interval overlap inner, build left ($n)", buildRows, "IntervalOverlapJoin") {
      broadcast(probeIntervals).join(buildIntervals, overlapCond, "inner")
    }
    compare(s"interval overlap left outer ($n)", probeRows, "IntervalOverlapJoin") {
      probeIntervals.join(broadcast(buildIntervals), overlapCond, "left")
    }
    compare(s"interval overlap right outer ($n)", probeRows, "IntervalOverlapJoin") {
      broadcast(buildIntervals).join(probeIntervals, overlapCond, "right")
    }
    compare(s"interval overlap left semi ($n)", probeRows, "IntervalOverlapJoin") {
      probeIntervals.join(broadcast(buildIntervals), overlapCond, "leftsemi")
    }
    compare(s"interval overlap left anti ($n)", probeRows, "IntervalOverlapJoin") {
      missIntervals.join(broadcast(buildIntervals), overlapCond, "leftanti")
    }
    val heavy = s"probe=$heavyProbeRows, ranges=$heavyBuildRows"
    compare(s"interval overlap inner, wide ($heavy)", heavyProbeRows, "IntervalOverlapJoin") {
      wideProbe.join(broadcast(wideBuild), overlapCond, "inner")
    }
  }

  private def partialLessCases(): Unit = {
    val n = s"probe=$probeRows, bounds=$buildRows"
    // Build holds the upper bound; the strict `<` makes the point index scan with
    // fromExclusive(), skipping build keys equal to the probe value.
    compare(s"partial < inner, build upper ($n)", probeRows, "LessPartialRangeJoin") {
      highProbes.join(broadcast(bounds), col("a") < col("b"), "inner")
    }
    // Build holds the lower bound; the strict `<` makes the point index scan with
    // upToExclusive(), skipping build keys equal to the probe value.
    compare(s"partial < inner, build lower ($n)", buildRows, "LessPartialRangeJoin") {
      broadcast(highProbes).join(bounds, col("a") < col("b"), "inner")
    }
    compare(s"partial < left outer ($n)", probeRows, "LessPartialRangeJoin") {
      highProbes.join(broadcast(bounds), col("a") < col("b"), "left")
    }
    compare(s"partial < right outer ($n)", probeRows, "LessPartialRangeJoin") {
      broadcast(bounds).join(lowProbes, col("b") < col("a"), "right")
    }
    compare(s"partial < left semi ($n)", probeRows, "LessPartialRangeJoin") {
      highProbes.join(broadcast(bounds), col("a") < col("b"), "leftsemi")
    }
    compare(s"partial < left anti ($n)", probeRows, "LessPartialRangeJoin") {
      lessMisses.join(broadcast(bounds), col("a") < col("b"), "leftanti")
    }
    val half = s"probe=$halfProbeRows, bounds=$halfBuildRows"
    compare(s"partial < inner, median bound ($half)", halfProbeRows, "LessPartialRangeJoin") {
      halfProbes.join(broadcast(halfBounds), col("a") < col("b"), "inner")
    }
    tinyProbeRowCounts.foreach { n =>
      compare(
          s"partial < inner, tiny probe (probe=$n, bounds=$buildRows)",
          buildRows, "LessPartialRangeJoin") {
        tinyBounds(n).join(broadcast(bounds), col("a") < col("b"), "inner")
      }
    }
  }

  private def partialGreaterCases(): Unit = {
    val n = s"probe=$probeRows, bounds=$buildRows"
    // Build holds the lower bound; the strict `>` makes the point index scan with
    // upToExclusive(), skipping build keys equal to the probe value.
    compare(s"partial > inner, build lower ($n)", probeRows, "GreaterPartialRangeJoin") {
      lowProbes.join(broadcast(bounds), col("a") > col("b"), "inner")
    }
    // Build holds the upper bound; the strict `>` makes the point index scan with
    // fromExclusive(), skipping build keys equal to the probe value.
    compare(s"partial > inner, build upper ($n)", buildRows, "GreaterPartialRangeJoin") {
      broadcast(lowProbes).join(bounds, col("a") > col("b"), "inner")
    }
    compare(s"partial > left outer ($n)", probeRows, "GreaterPartialRangeJoin") {
      lowProbes.join(broadcast(bounds), col("a") > col("b"), "left")
    }
    compare(s"partial > right outer ($n)", probeRows, "GreaterPartialRangeJoin") {
      broadcast(bounds).join(highProbes, col("b") > col("a"), "right")
    }
    compare(s"partial > left semi ($n)", probeRows, "GreaterPartialRangeJoin") {
      lowProbes.join(broadcast(bounds), col("a") > col("b"), "leftsemi")
    }
    compare(s"partial > left anti ($n)", probeRows, "GreaterPartialRangeJoin") {
      greaterMisses.join(broadcast(bounds), col("a") > col("b"), "leftanti")
    }
    val half = s"probe=$halfProbeRows, bounds=$halfBuildRows"
    compare(s"partial > inner, median bound ($half)", halfProbeRows, "GreaterPartialRangeJoin") {
      halfProbes.join(broadcast(halfBounds), col("a") > col("b"), "inner")
    }
  }

  private def keyTypeCases(): Unit = {
    val n = s"probe=$probeRows, ranges=$buildRows"
    compare(s"point-in-range inner, date ($n)", probeRows, "PointInRangeJoin") {
      datePoints.join(broadcast(dateRanges), pointCond, "inner")
    }
    compare(s"point-in-range inner, timestamp ($n)", probeRows, "PointInRangeJoin") {
      timestampPoints.join(broadcast(timestampRanges), pointCond, "inner")
    }
    compare(s"point-in-range inner, decimal ($n)", probeRows, "PointInRangeJoin") {
      decimalPoints.join(broadcast(decimalRanges), pointCond, "inner")
    }
    compare(s"point-in-range inner, collated string ($n)", probeRows, "PointInRangeJoin") {
      stringPoints.join(broadcast(stringRanges), pointCond, "inner")
    }
  }

  private def staggeredOverlapCases(): Unit = {
    val n = s"probe=$staggerProbeRows, ranges=$staggerBuildRows"
    compare(s"interval overlap inner, staggered 50% overlap ($n)",
      staggerProbeRows, "IntervalOverlapJoin") {
      staggerProbeLong.join(broadcast(staggerBuild50), overlapCond, "inner")
    }
    compare(s"interval overlap inner, staggered 90% overlap ($n)",
      staggerProbeRows, "IntervalOverlapJoin") {
      staggerProbeLong.join(broadcast(staggerBuild90), overlapCond, "inner")
    }
    // Double keys stay on the generic boxed index, so the same regime rides
    // Ordering[Any] at every tree node.
    compare(s"interval overlap inner, staggered 50% overlap on double ($n)",
      staggerProbeRows, "IntervalOverlapJoin") {
      staggerProbeDouble.join(broadcast(staggerBuild50Double), overlapCond, "inner")
    }
    compare(s"interval overlap inner, staggered 90% overlap on double ($n)",
      staggerProbeRows, "IntervalOverlapJoin") {
      staggerProbeDouble.join(broadcast(staggerBuild90Double), overlapCond, "inner")
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    if (nameFilter.isEmpty) prepare()
    runBenchmark("Range Join Benchmark") {
      pointCases()
      intervalCases()
      partialLessCases()
      partialGreaterCases()
      keyTypeCases()
      staggeredOverlapCases()
    }
  }
}
