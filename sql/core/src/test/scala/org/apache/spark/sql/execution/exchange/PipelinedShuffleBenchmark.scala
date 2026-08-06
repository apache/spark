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

import org.apache.spark.sql.SparkSession

/**
 * WIP benchmark (SPARK-57399 local-repartition v2). Compares the in-process pipelined channel
 * shuffle against the regular (materializing) shuffle on simple batch queries.
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
 * {{{
 *   build/sbt "sql/Test/runMain org.apache.spark.sql.execution.exchange.PipelinedShuffleBenchmark"
 * }}}
 */
object PipelinedShuffleBenchmark {

  private val cores = Runtime.getRuntime.availableProcessors()

  private def newSession(
      pipelined: Boolean,
      shufflePartitions: Int,
      master: String): SparkSession = {
    val b = SparkSession.builder()
      .master(master)
      .appName("pipelined-shuffle-benchmark")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.speculation", "false")
      .config("spark.sql.shuffle.partitions", shufflePartitions.toString)
      .config("spark.ui.enabled", "false")
    if (pipelined) {
      b.config("spark.shuffle.manager.incremental",
        "org.apache.spark.shuffle.local.pipelined.PipelinedChannelShuffleManager")
        .config("spark.sql.pipelinedShuffle.enabled", "true")
    }
    b.getOrCreate()
  }

  private val numRows = 20000000L  // 20M: large enough that transport cost dominates startup noise
  private val inputParts = 6       // demand = 6 (input) + 8 (shuffle) = 14 <= 16 cores: no oversub
  private val iters = 6            // report best of N timed runs after a warm-up

  // Time a workload under a fresh session (created/stopped OUTSIDE the timed region so session
  // startup and manager init are not counted). One warm-up run, then `iters` timed runs; report
  // the best (min) wall-clock ms.
  private def bestMs(
      pipelined: Boolean,
      shufflePartitions: Int,
      master: String,
      workload: SparkSession => Unit): Long = {
    val spark = newSession(pipelined, shufflePartitions, master)
    try {
      workload(spark) // warm up (JIT, caches, codegen)
      var best = Long.MaxValue
      var i = 0
      while (i < iters) {
        val t0 = System.nanoTime()
        workload(spark)
        best = math.min(best, (System.nanoTime() - t0) / 1000000L)
        i += 1
      }
      best
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  private def compare(
      name: String,
      shufflePartitions: Int = 8,
      master: String = s"local[$cores]")(workload: SparkSession => Unit): Unit = {
    val regular = bestMs(pipelined = false, shufflePartitions, master, workload)
    val pipe = bestMs(pipelined = true, shufflePartitions, master, workload)
    val speedup = regular.toDouble / pipe
    // scalastyle:off println
    println(f"[bench] $name%-40s  regular=${regular}%5dms  pipelined=${pipe}%5dms  " +
      f"speedup=${speedup}%.2fx")
    // scalastyle:on println
  }

  def main(args: Array[String]): Unit = {
    // scalastyle:off println
    println(s"[bench] local[$cores], $numRows rows, best of $iters iters")
    // scalastyle:on println

    compare("repartition(k) + count")({ spark =>
      import spark.implicits._
      spark.range(0, numRows, 1, inputParts).withColumn("k", $"id" % 1000)
        .repartition($"k").count()
    })

    compare("groupBy(k).count")({ spark =>
      import spark.implicits._
      spark.range(0, numRows, 1, inputParts).withColumn("k", $"id" % 1000)
        .groupBy($"k").count().count()
    })

    // Range over a plain scan (control): RangePartitioner's sample job runs the scan once,
    // then the main job runs it again -- for BOTH modes (there is no shuffle below the range
    // exchange, so regular has nothing materialized to reuse either). Expect no differential
    // penalty. Demand: 6 (scan) + 8 (range) + 1 (final count) = 15 <= 16.
    compare("repartitionByRange(k) + count")({ spark =>
      import spark.implicits._
      spark.range(0, numRows, 1, inputParts).withColumn("k", $"id" % 1000)
        .repartitionByRange($"k").count()
    })

    // Range ABOVE a shuffle (the differential case): the range exchange's child contains the
    // groupBy's hash shuffle. The sample job executes that child; the main job then needs it
    // again. Regular reuses the hash shuffle's materialized map output (map stage skipped on
    // the second run); pipelined channels are single-shot and completed-job stages are
    // cleaned up, so the whole scan + partial agg + hash map side re-runs. Expect pipelined
    // to pay roughly one extra scan+map pass. shufflePartitions = 4 keeps whole-group demand
    // 6 + 4 + 4 = 14 <= 16 (sample job's own group is 6 + 4 = 10).
    compare("groupBy(k).count + orderBy(k)", shufflePartitions = 4)({ spark =>
      import spark.implicits._
      spark.range(0, numRows, 1, inputParts).withColumn("k", $"id" % 1000)
        .groupBy($"k").count().orderBy($"k").collect()
    })

    // v1's "prototype workload" (LocalRepartitionBenchmark), the case where v1 measured
    // its largest win (4.1-4.5x over regular shuffle, AQE off, on local[32]): 1M rows,
    // UNIQUE key per row, uncached. ColumnPruning removes the wide repeat(...) column, so
    // this is pure per-row transport overhead with no batching benefit from key
    // collisions -- small data, so the regular shuffle's fixed write/read cost dominates.
    // Demand here: 6 (input) + 8 (repartition) + 1 (final agg) = 15 <= 16.
    compare("prototype: repartition(id)+count, 1M uniq")({ spark =>
      import org.apache.spark.sql.functions.{col, lit, repeat, sum}
      spark.range(0L, 100000000L, 100L, inputParts)
        .select(
          col("id"),
          col("id").cast("string").as("id2"),
          (col("id") + 1).as("id3"),
          repeat((col("id") + 1).cast("string"), 100000).as("id4"))
        .repartition(col("id")).agg(sum(lit(1L))).collect()
    })

    // Same prototype with the HISTORICAL 32 map tasks (the config where v1 recorded
    // 4.1-4.5x: many tiny map tasks multiply the regular shuffle's per-task/per-segment
    // fixed cost). v2's gang demand is 32 + 8 + 1 = 41, far past the 16 honest slots, so
    // this REQUIRES oversubscription (local[48]); the regular baseline's 32-wide map
    // stage also exceeds the 16 cores there, so both sides oversubscribe symmetrically.
    compare("prototype 32 maps (local[48], oversub)", master = "local[48]")({ spark =>
      import org.apache.spark.sql.functions.{col, lit, repeat, sum}
      spark.range(0L, 100000000L, 100L, 32)
        .select(
          col("id"),
          col("id").cast("string").as("id2"),
          (col("id") + 1).as("id3"),
          repeat((col("id") + 1).cast("string"), 100000).as("id4"))
        .repartition(col("id")).agg(sum(lit(1L))).collect()
    })
  }
}
