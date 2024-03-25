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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.benchmark.BenchmarkBase
import org.apache.spark.scheduler.{CompressedMapStatus, MergeStatus}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

/**
 * Benchmark for MapStatuses serialization & deserialization performance.
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/MapStatusesSerDeserBenchmark-results.txt".
 * }}}
 */
object MapStatusesSerDeserBenchmark extends BenchmarkBase {

  var sc: SparkContext = null
  var tracker: MapOutputTrackerMaster = null

  def serDeserBenchmark(numMaps: Int, blockSize: Int, enableBroadcast: Boolean): Unit = {
    val minBroadcastSize = if (enableBroadcast) {
      0
    } else {
      Int.MaxValue
    }

    val benchmark = new Benchmark(s"$numMaps MapOutputs, $blockSize blocks " + {
      if (enableBroadcast) "w/ " else "w/o "
    } + "broadcast", numMaps, output = output)

    val shuffleId = 10

    tracker.registerShuffle(shuffleId, numMaps, MergeStatus.SHUFFLE_PUSH_DUMMY_NUM_REDUCES)
    val r = new scala.util.Random(912)
    (0 until numMaps).foreach { i =>
      tracker.registerMapOutput(shuffleId, i,
        new CompressedMapStatus(BlockManagerId(s"node$i", s"node$i.spark.apache.org", 1000),
          Array.fill(blockSize) {
            // Creating block size ranging from 0byte to 1GB
            (r.nextDouble() * 1024 * 1024 * 1024).toLong
          }, i))
    }

    val shuffleStatus = tracker.shuffleStatuses.get(shuffleId).head

    var serializedMapStatusSizes = 0
    var serializedBroadcastSizes = 0L

    val (serializedMapStatus, serializedBroadcast) = MapOutputTracker.serializeOutputStatuses(
      shuffleStatus.mapStatuses, tracker.broadcastManager, tracker.isLocal, minBroadcastSize,
      sc.getConf)
    serializedMapStatusSizes = serializedMapStatus.length
    if (serializedBroadcast != null) {
      serializedBroadcastSizes = serializedBroadcast.value.foldLeft(0L)(_ + _.length)
    }

    benchmark.addCase("Serialization") { _ =>
      MapOutputTracker.serializeOutputStatuses(shuffleStatus.mapStatuses, tracker.broadcastManager,
        tracker.isLocal, minBroadcastSize, sc.getConf)
    }

    benchmark.addCase("Deserialization") { _ =>
      val result = MapOutputTracker.deserializeOutputStatuses(serializedMapStatus, sc.getConf)
      assert(result.length == numMaps)
    }

    benchmark.run()
    // scalastyle:off println
    benchmark.out.println("Compressed Serialized MapStatus sizes: " +
      Utils.bytesToString(serializedMapStatusSizes))
    benchmark.out.println("Compressed Serialized Broadcast MapStatus sizes: " +
      Utils.bytesToString(serializedBroadcastSizes) + "\n\n")
    // scalastyle:on

    tracker.unregisterShuffle(shuffleId)
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    createSparkContext()
    tracker = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    val rpcEnv = sc.env.rpcEnv
    val masterEndpoint = new MapOutputTrackerMasterEndpoint(rpcEnv, tracker, sc.getConf)
    rpcEnv.stop(tracker.trackerEndpoint)
    rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME, masterEndpoint)

    serDeserBenchmark(200000, 10, true)
    serDeserBenchmark(200000, 10, false)

    serDeserBenchmark(200000, 100, true)
    serDeserBenchmark(200000, 100, false)

    serDeserBenchmark(200000, 1000, true)
    serDeserBenchmark(200000, 1000, false)
  }

  def createSparkContext(): Unit = {
    val conf = new SparkConf()
    if (sc != null) {
      sc.stop()
    }
    sc = new SparkContext("local", "MapStatusesSerializationBenchmark", conf)
  }

  override def afterAll(): Unit = {
    tracker.stop()
    if (sc != null) {
      sc.stop()
    }
  }
}
