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

package org.apache.spark.sql.execution.exchange

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark

/**
 * Benchmark to compare the in-process pipelined channel shuffle against the regular
 * (materializing) shuffle on simple batch queries (SPARK-57399).
 *
 * Fair-comparison constraints, read before trusting any number:
 *   - Runs on `local[N]` with N = physical cores, and only queries whose pipelined
 *     whole-group slot demand is <= N, so the concurrently-scheduled pipelined stages do NOT
 *     oversubscribe the cores. A demand > cores run would measure thread thrash, not the
 *     transport, and is intentionally avoided here.
 *   - Pipelined overlaps map+reduce stages (uses more concurrent slots) vs the baseline's
 *     sequential map-then-reduce; with demand <= cores neither is slot-limited, so the delta
 *     reflects stage overlap minus channel overhead, which is the honest comparison.
 *
 * To run this benchmark:
 * {{{
 *   1. build/sbt "sql/Test/runMain
 *      org.apache.spark.sql.execution.exchange.PipelinedShuffleBenchmark"
 *   2. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt
 *      "sql/Test/runMain
 *      org.apache.spark.sql.execution.exchange.PipelinedShuffleBenchmark"
 *      Results will be written to "benchmarks/PipelinedShuffleBenchmark-results.txt".
 * }}}
 */
object PipelinedShuffleBenchmark extends SqlBasedBenchmark {

  override def getSparkSession: SparkSession = {
    SparkSession.builder()
      .master("local[1]")
      .appName(this.getClass.getCanonicalName)
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  private val cores = Runtime.getRuntime.availableProcessors()
  private val numRows = 20000000L  // 20M: large enough that transport cost dominates startup
  // Derive input partitions from the machine so the whole-group slot demand
  // (inputParts + 8 shuffle + 1 final) stays within `cores` -- otherwise the pipelined runs do
  // not just get slower, they fail gang admission (CONCURRENT_SCHEDULER_INSUFFICIENT_SLOT) and
  // the run dies. Needs cores >= 11 for the minimum shape (inputParts = 2); runBenchmarkSuite
  // skips below that. Capped at 6 so a big machine still measures the small-fan-in shape.
  private val inputParts = math.min(6, cores - 9)

  private val channelManagerClass =
    "org.apache.spark.shuffle.local.pipelined.PipelinedChannelShuffleManager"

  private def buildSession(
      pipelined: Boolean,
      shufflePartitions: Int,
      master: String,
      aqe: Boolean): SparkSession = {
    val b = SparkSession.builder()
      .master(master)
      .appName("pipelined-shuffle-benchmark")
      .config("spark.sql.adaptive.enabled", aqe.toString)
      .config("spark.speculation", "false")
      .config("spark.sql.shuffle.partitions", shufflePartitions.toString)
      .config("spark.ui.enabled", "false")
    if (pipelined) {
      b.config("spark.shuffle.manager.incremental", channelManagerClass)
        .config("spark.sql.shuffle.localPipelined.enabled", "true")
    }
    b.getOrCreate()
  }

  private def buildTransportSession(
      mode: String, shufflePartitions: Int): SparkSession = {
    val b = SparkSession.builder()
      .master(s"local[$cores]")
      .appName("pipelined-transport-benchmark")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.speculation", "false")
      .config("spark.sql.shuffle.partitions", shufflePartitions.toString)
      .config("spark.ui.enabled", "false")
    mode match {
      case "regular" =>
      case "streaming" =>
        b.config("spark.sql.shuffle.localPipelined.enabled", "true")
      case "channel" =>
        b.config("spark.sql.shuffle.localPipelined.enabled", "true")
          .config("spark.shuffle.manager.incremental", channelManagerClass)
    }
    b.getOrCreate()
  }

  // Each transport runs under its OWN SparkSession, because the shuffle manager and the
  // pipelined flag are SparkContext-level and fixed at startup -- they cannot be switched with
  // setConf on a shared session. So a case cannot reuse the base trait's session; it builds its
  // own. Two consequences handled here:
  //   - Session build/teardown must be EXCLUDED from the measured time (it is seconds of fixed
  //     cost that would swamp the workload). addTimerCase gives manual timing: the session is
  //     built before startTiming() and stopped after stopTiming(), so only the workload is timed
  //     while the framework still runs its own warm-up + best-of-N iterations.
  //   - Any lingering active session (the base trait eagerly creates a throwaway one, and a prior
  //     case leaves none but be defensive) must be stopped first, or getOrCreate would REUSE it
  //     and silently ignore this case's master/manager config.
  private def addModeCase(
      benchmark: Benchmark,
      caseName: String,
      buildSession: => SparkSession)(workload: SparkSession => Unit): Unit = {
    benchmark.addTimerCase(caseName) { timer =>
      // Stop whatever session is around (active or default -- the base trait's throwaway is set
      // as the default) so the fresh build below is not short-circuited by getOrCreate reuse.
      SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession).foreach(_.stop())
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      val sparkSession = buildSession
      try {
        timer.startTiming()
        workload(sparkSession)
        timer.stopTiming()
      } finally {
        sparkSession.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }
  }

  private def transportComparison(): Unit = {
    runBenchmark(
      "Transport comparison: regular vs RPC-streaming vs in-process channel") {
      // repartition(k)+count
      val repartitionWorkload: SparkSession => Unit = { spark =>
        import spark.implicits._
        spark.range(0, numRows, 1, inputParts)
          .withColumn("k", $"id" % 1000).repartition($"k").count()
      }

      val b1 = new Benchmark(
        "repartition(k)+count",
        numRows,
        output = output)
      addModeCase(b1, "regular", buildTransportSession("regular", 8))(
        repartitionWorkload)
      addModeCase(b1, "streaming", buildTransportSession("streaming", 8))(
        repartitionWorkload)
      addModeCase(b1, "channel", buildTransportSession("channel", 8))(
        repartitionWorkload)
      b1.run()

      // groupBy(k).count
      val groupByWorkload: SparkSession => Unit = { spark =>
        import spark.implicits._
        spark.range(0, numRows, 1, inputParts)
          .withColumn("k", $"id" % 1000).groupBy($"k").count().count()
      }

      val b2 = new Benchmark(
        "groupBy(k).count",
        numRows,
        output = output)
      addModeCase(b2, "regular", buildTransportSession("regular", 8))(
        groupByWorkload)
      addModeCase(b2, "streaming", buildTransportSession("streaming", 8))(
        groupByWorkload)
      addModeCase(b2, "channel", buildTransportSession("channel", 8))(
        groupByWorkload)
      b2.run()

      // join 10M x 10M on unique k+count
      val joinWorkload: SparkSession => Unit = { spark =>
        import spark.implicits._
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        val left = spark.range(0, 10000000L, 1, 2)
          .select($"id".as("k"), $"id".as("lv"))
        val right = spark.range(0, 10000000L, 1, 2)
          .select($"id".as("k"), $"id".as("rv"))
        left.join(right, "k").count()
      }

      val b3 = new Benchmark(
        "join 10M x 10M on unique k+count",
        10000000L,
        output = output)
      addModeCase(b3, "regular", buildTransportSession("regular", 8))(
        joinWorkload)
      addModeCase(b3, "streaming", buildTransportSession("streaming", 8))(
        joinWorkload)
      addModeCase(b3, "channel", buildTransportSession("channel", 8))(
        joinWorkload)
      b3.run()

      // prototype 1M uniq
      val prototypeWorkload: SparkSession => Unit = { spark =>
        import org.apache.spark.sql.functions.{col, lit, repeat, sum}
        spark.range(0L, 100000000L, 100L, inputParts)
          .select(
            col("id"),
            col("id").cast("string").as("id2"),
            (col("id") + 1).as("id3"),
            repeat((col("id") + 1).cast("string"), 100000).as("id4"))
          .repartition(col("id")).agg(sum(lit(1L))).collect()
      }

      val b4 = new Benchmark(
        "prototype 1M uniq",
        100000000L,
        output = output)
      addModeCase(b4, "regular", buildTransportSession("regular", 8))(
        prototypeWorkload)
      addModeCase(b4, "streaming", buildTransportSession("streaming", 8))(
        prototypeWorkload)
      addModeCase(b4, "channel", buildTransportSession("channel", 8))(
        prototypeWorkload)
      b4.run()
    }
  }

  private def aqeOffComparison(): Unit = {
    runBenchmark("Regular vs channel, AQE off") {
      // repartition(k)+count
      val repartitionWorkload: SparkSession => Unit = { spark =>
        import spark.implicits._
        spark.range(0, numRows, 1, inputParts)
          .withColumn("k", $"id" % 1000).repartition($"k").count()
      }

      val b1 = new Benchmark(
        "repartition(k)+count",
        numRows,
        output = output)
      addModeCase(b1, "regular", buildSession(
        pipelined = false, 8, s"local[$cores]", aqe = false))(
        repartitionWorkload)
      addModeCase(b1, "pipelined", buildSession(
        pipelined = true, 8, s"local[$cores]", aqe = false))(
        repartitionWorkload)
      b1.run()

      // groupBy(k).count
      val groupByWorkload: SparkSession => Unit = { spark =>
        import spark.implicits._
        spark.range(0, numRows, 1, inputParts)
          .withColumn("k", $"id" % 1000).groupBy($"k").count().count()
      }

      val b2 = new Benchmark(
        "groupBy(k).count",
        numRows,
        output = output)
      addModeCase(b2, "regular", buildSession(
        pipelined = false, 8, s"local[$cores]", aqe = false))(
        groupByWorkload)
      addModeCase(b2, "pipelined", buildSession(
        pipelined = true, 8, s"local[$cores]", aqe = false))(
        groupByWorkload)
      b2.run()

      // repartitionByRange(k)+count
      val repartitionByRangeWorkload: SparkSession => Unit = { spark =>
        import spark.implicits._
        spark.range(0, numRows, 1, inputParts)
          .withColumn("k", $"id" % 1000).repartitionByRange($"k").count()
      }

      val b3 = new Benchmark(
        "repartitionByRange(k)+count",
        numRows,
        output = output)
      addModeCase(b3, "regular", buildSession(
        pipelined = false, 8, s"local[$cores]", aqe = false))(
        repartitionByRangeWorkload)
      addModeCase(b3, "pipelined", buildSession(
        pipelined = true, 8, s"local[$cores]", aqe = false))(
        repartitionByRangeWorkload)
      b3.run()

      // groupBy(k).count+orderBy(k)
      val groupByOrderByWorkload: SparkSession => Unit = { spark =>
        import spark.implicits._
        spark.range(0, numRows, 1, inputParts)
          .withColumn("k", $"id" % 1000).groupBy($"k").count().orderBy($"k")
          .collect()
      }

      val b4 = new Benchmark(
        "groupBy(k).count+orderBy(k)",
        numRows,
        output = output)
      addModeCase(b4, "regular", buildSession(
        pipelined = false, 4, s"local[$cores]", aqe = false))(
        groupByOrderByWorkload)
      addModeCase(b4, "pipelined", buildSession(
        pipelined = true, 4, s"local[$cores]", aqe = false))(
        groupByOrderByWorkload)
      b4.run()

      // prototype: repartition(id)+count, 1M uniq
      val prototypeWorkload: SparkSession => Unit = { spark =>
        import org.apache.spark.sql.functions.{col, lit, repeat, sum}
        spark.range(0L, 100000000L, 100L, inputParts)
          .select(
            col("id"),
            col("id").cast("string").as("id2"),
            (col("id") + 1).as("id3"),
            repeat((col("id") + 1).cast("string"), 100000).as("id4"))
          .repartition(col("id")).agg(sum(lit(1L))).collect()
      }

      val b5 = new Benchmark(
        "prototype: repartition(id)+count, 1M uniq",
        100000000L,
        output = output)
      addModeCase(b5, "regular", buildSession(
        pipelined = false, 8, s"local[$cores]", aqe = false))(
        prototypeWorkload)
      addModeCase(b5, "pipelined", buildSession(
        pipelined = true, 8, s"local[$cores]", aqe = false))(
        prototypeWorkload)
      b5.run()
    }
  }

  private def aqeOnComparison(): Unit = {
    runBenchmark("Regular vs channel, AQE on") {
      // repartition(k)+count (AQE)
      val repartitionWorkload: SparkSession => Unit = { spark =>
        import spark.implicits._
        spark.range(0, numRows, 1, inputParts)
          .withColumn("k", $"id" % 1000).repartition($"k").count()
      }

      val b1 = new Benchmark(
        "repartition(k)+count (AQE)",
        numRows,
        output = output)
      addModeCase(b1, "regular", buildSession(
        pipelined = false, 8, s"local[$cores]", aqe = true))(
        repartitionWorkload)
      addModeCase(b1, "pipelined", buildSession(
        pipelined = true, 8, s"local[$cores]", aqe = true))(
        repartitionWorkload)
      b1.run()

      // groupBy(k).count (AQE)
      val groupByWorkload: SparkSession => Unit = { spark =>
        import spark.implicits._
        spark.range(0, numRows, 1, inputParts)
          .withColumn("k", $"id" % 1000).groupBy($"k").count().count()
      }

      val b2 = new Benchmark(
        "groupBy(k).count (AQE)",
        numRows,
        output = output)
      addModeCase(b2, "regular", buildSession(
        pipelined = false, 8, s"local[$cores]", aqe = true))(
        groupByWorkload)
      addModeCase(b2, "pipelined", buildSession(
        pipelined = true, 8, s"local[$cores]", aqe = true))(
        groupByWorkload)
      b2.run()

      // groupBy(k).count+orderBy(k) (AQE)
      val groupByOrderByWorkload: SparkSession => Unit = { spark =>
        import spark.implicits._
        spark.range(0, numRows, 1, inputParts)
          .withColumn("k", $"id" % 1000).groupBy($"k").count().orderBy($"k")
          .collect()
      }

      val b3 = new Benchmark(
        "groupBy(k).count+orderBy(k) (AQE)",
        numRows,
        output = output)
      addModeCase(b3, "regular", buildSession(
        pipelined = false, 4, s"local[$cores]", aqe = true))(
        groupByOrderByWorkload)
      addModeCase(b3, "pipelined", buildSession(
        pipelined = true, 4, s"local[$cores]", aqe = true))(
        groupByOrderByWorkload)
      b3.run()

      // prototype 1M uniq (AQE)
      val prototypeWorkload: SparkSession => Unit = { spark =>
        import org.apache.spark.sql.functions.{col, lit, repeat, sum}
        spark.range(0L, 100000000L, 100L, inputParts)
          .select(
            col("id"),
            col("id").cast("string").as("id2"),
            (col("id") + 1).as("id3"),
            repeat((col("id") + 1).cast("string"), 100000).as("id4"))
          .repartition(col("id")).agg(sum(lit(1L))).collect()
      }

      val b4 = new Benchmark(
        "prototype 1M uniq (AQE)",
        100000000L,
        output = output)
      addModeCase(b4, "regular", buildSession(
        pipelined = false, 8, s"local[$cores]", aqe = true))(
        prototypeWorkload)
      addModeCase(b4, "pipelined", buildSession(
        pipelined = true, 8, s"local[$cores]", aqe = true))(
        prototypeWorkload)
      b4.run()
    }
  }

  private def oversubscribeComparison(): Unit = {
    runBenchmark("Prototype 32 maps, oversubscribed") {
      // prototype 32 maps
      val prototypeWorkload: SparkSession => Unit = { spark =>
        import org.apache.spark.sql.functions.{col, lit, repeat, sum}
        spark.range(0L, 100000000L, 100L, 32)
          .select(
            col("id"),
            col("id").cast("string").as("id2"),
            (col("id") + 1).as("id3"),
            repeat((col("id") + 1).cast("string"), 100000).as("id4"))
          .repartition(col("id")).agg(sum(lit(1L))).collect()
      }

      val b1 = new Benchmark(
        "prototype 32 maps",
        100000000L,
        output = output)
      addModeCase(b1, "regular", buildSession(
        pipelined = false, 8, "local[48]", aqe = false))(
        prototypeWorkload)
      addModeCase(b1, "pipelined", buildSession(
        pipelined = true, 8, "local[48]", aqe = false))(
        prototypeWorkload)
      b1.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    // The pipelined groups gang-schedule inputParts + 8 + 1 stages; below 11 cores even the
    // minimum shape (inputParts = 2) exceeds the machine and fails gang admission rather than
    // producing a slower number. Skip loudly so the run does not just die with an
    // insufficient-slot error, and so checked-in results are only ever generated where the fair
    // comparison actually holds. (oversubscribeComparison intentionally over-subscribes, but on
    // its own local[48]; it is skipped here too to keep the file all-or-nothing per machine.)
    if (cores < 11) {
      // scalastyle:off println
      println(s"[skip] PipelinedShuffleBenchmark needs >= 11 cores for the gang to fit; " +
        s"this machine has $cores. Skipping.")
      // scalastyle:on println
      return
    }
    transportComparison()
    aqeOffComparison()
    aqeOnComparison()
    oversubscribeComparison()
  }
}
