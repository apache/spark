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

package org.apache.spark

import java.io.PrintStream

import org.apache.commons.io.output.TeeOutputStream
import org.mockito.Mockito.{doReturn, mock}

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.internal.config
import org.apache.spark.scheduler.HighlyCompressedMapStatus
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.SizeEstimator

/**
 * Benchmark for building a HighlyCompressedMapStatus, which every map task of a shuffle with more
 * than spark.shuffle.minNumPartitionsToHighlyCompress partitions has to pay for. It covers both
 * the case where skewed block sizes are recorded accurately and the case where they are not.
 * It also reports the heap the driver retains per map status, since the driver holds one map
 * status per map task for the lifetime of the shuffle.
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/HighlyCompressedMapStatusBenchmark-results.txt".
 * }}}
 */
object HighlyCompressedMapStatusBenchmark extends BenchmarkBase {

  private val loc = BlockManagerId("a", "host", 1000)

  private def envWithSkewedFactor(skewedFactor: Double): SparkEnv = {
    val conf = new SparkConf()
      .set(config.SHUFFLE_ACCURATE_BLOCK_SKEWED_FACTOR.key, skewedFactor.toString)
    val env = mock(classOf[SparkEnv])
    doReturn(conf, Seq.empty: _*).when(env).conf
    env
  }

  private def skewedBlockSizes(numPartitions: Int): Array[Long] = {
    val r = new scala.util.Random(912)
    Array.tabulate(numPartitions) { i =>
      // A few blocks are orders of magnitude larger than the rest, as in a skewed shuffle.
      if (i % 500 == 0) (r.nextDouble() * 512 * 1024 * 1024).toLong
      else (r.nextDouble() * 64 * 1024).toLong
    }
  }

  /**
   * Sizes that rise and then fall. This is the worst case for selecting an order statistic with a
   * fixed pivot choice, so it covers the cost of the fallback to sorting.
   */
  private def organPipeBlockSizes(numPartitions: Int): Array[Long] = {
    Array.tabulate(numPartitions)(i => Math.min(i, numPartitions - 1 - i).toLong)
  }

  /**
   * Sizes where more blocks are tied at the cutoff size than may be recorded accurately. This is
   * the distribution that fills the per map task budget of accurately recorded sizes, so it bounds
   * the heap the driver retains for them.
   */
  private def tiedSkewedBlockSizes(numPartitions: Int): Array[Long] = {
    val numSkewedBlocks = config.SHUFFLE_MAX_ACCURATE_SKEWED_BLOCK_NUMBER.defaultValue.get + 1
    Array.tabulate(numPartitions) { i =>
      if (i < numPartitions - numSkewedBlocks) 10 * 1024L else 100 * 1024L
    }
  }

  /**
   * The driver reads every block size out of every map status when it computes the statistics
   * adaptive query execution plans on, so the lookup cost is paid numPartitions * numMapTasks
   * times per shuffle.
   */
  private def runLookupBenchmark(envWithSkewedSizes: SparkEnv): Unit = {
    val numMapTasks = 100
    Seq(2048, 50000).foreach { numPartitions =>
      SparkEnv.set(envWithSkewedSizes)
      val sizes = tiedSkewedBlockSizes(numPartitions)
      val statuses =
        Array.tabulate(numMapTasks)(id => HighlyCompressedMapStatus(loc, sizes, id.toLong))
      val benchmark = new Benchmark(
        s"$numPartitions shuffle partitions", numPartitions.toLong * numMapTasks, output = output)
      benchmark.addCase("sum every block size over the map statuses", 10) { _ =>
        var total = 0L
        var reduceId = 0
        while (reduceId < numPartitions) {
          var mapIndex = 0
          while (mapIndex < numMapTasks) {
            total += statuses(mapIndex).getSizeForBlock(reduceId)
            mapIndex += 1
          }
          reduceId += 1
        }
        assert(total > 0)
      }
      benchmark.run()
    }
  }

  /**
   * Registers a whole stage worth of map statuses with a real MapOutputTrackerMaster and measures
   * what the driver ends up holding: the deserialized map statuses it keeps for the lifetime of
   * the shuffle, and the serialized copy it caches to answer executor requests, which is held in
   * a broadcast variable once it is larger than spark.shuffle.mapOutput.minSizeForBroadcast.
   */
  private def measureDriverHeap(
      skewedFactor: Double,
      numPartitions: Int,
      numMapTasks: Int): (Long, Long) = {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("HighlyCompressedMapStatusBenchmark")
      .set(config.SHUFFLE_ACCURATE_BLOCK_SKEWED_FACTOR.key, skewedFactor.toString)
    val sc = new SparkContext(conf)
    try {
      val sizes = tiedSkewedBlockSizes(numPartitions)
      val tracker = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
      val shuffleId = 0
      tracker.registerShuffle(shuffleId, numMapTasks, numPartitions)
      (0 until numMapTasks).foreach { mapIndex =>
        tracker.registerMapOutput(
          shuffleId, mapIndex, HighlyCompressedMapStatus(loc, sizes, mapIndex.toLong))
      }
      val shuffleStatus = tracker.shuffleStatuses(shuffleId)
      val retained = SizeEstimator.estimate(shuffleStatus.mapStatuses)
      val serialized = shuffleStatus.serializedMapStatus(sc.env.broadcastManager, isLocal = false,
        conf.get(config.SHUFFLE_MAPOUTPUT_MIN_SIZE_FOR_BROADCAST).toInt, conf)
      // Once the serialized statuses are broadcast, what the driver retains is the broadcast
      // blocks, not the small placeholder that is sent in their stead.
      val cached = Option(shuffleStatus.cachedSerializedBroadcast)
        .map(_.value.map(_.length.toLong).sum)
        .getOrElse(serialized.length.toLong)
      (retained, cached)
    } finally {
      sc.stop()
    }
  }

  private def runDriverHeapReport(): Unit = {
    val numPartitions = 2001
    val numMapTasks = 10000
    val measurements = Seq(
      "skewed block sizes not recorded" -> -1.0,
      "skewed block sizes recorded accurately" -> 5.0).map { case (name, skewedFactor) =>
      name -> measureDriverHeap(skewedFactor, numPartitions, numMapTasks)
    }

    val out = output
      .map(o => new PrintStream(new TeeOutputStream(System.out, o)))
      .getOrElse(System.out)
    // scalastyle:off println
    out.printf("%-42s %14s %16s %14s\n",
      s"$numPartitions partitions, $numMapTasks map tasks",
      "retained MiB", "bytes per task", "serialized KiB")
    out.println("-" * 90)
    measurements.foreach { case (name, (retained, cached)) =>
      out.printf("%-42s %14s %16s %14s\n",
        name,
        (retained / (1024 * 1024)).toString,
        (retained / numMapTasks).toString,
        (cached / 1024).toString)
    }
    out.println()
    // scalastyle:on println
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val envWithoutSkewedSizes = envWithSkewedFactor(-1.0)
    val envWithSkewedSizes = envWithSkewedFactor(5.0)
    Seq("skewed" -> skewedBlockSizes _, "organ pipe" -> organPipeBlockSizes _).foreach {
      case (distribution, blockSizes) =>
        runBenchmark(s"Build HighlyCompressedMapStatus, $distribution block sizes") {
          Seq(2048, 10000, 50000).foreach { numPartitions =>
            val benchmark = new Benchmark(s"$numPartitions shuffle partitions", 1, output = output)
            val sizes = blockSizes(numPartitions)

            benchmark.addCase("skewed block sizes not recorded", 10) { _ =>
              SparkEnv.set(envWithoutSkewedSizes)
              HighlyCompressedMapStatus(loc, sizes, 0)
            }

            benchmark.addCase("skewed block sizes recorded accurately", 10) { _ =>
              SparkEnv.set(envWithSkewedSizes)
              HighlyCompressedMapStatus(loc, sizes, 0)
            }

            benchmark.run()
          }
        }
    }
    runBenchmark("Read block sizes out of HighlyCompressedMapStatus") {
      runLookupBenchmark(envWithSkewedSizes)
    }
    SparkEnv.set(null)
    runBenchmark("Driver heap retained by MapOutputTracker") {
      runDriverHeapReport()
    }
  }
}
