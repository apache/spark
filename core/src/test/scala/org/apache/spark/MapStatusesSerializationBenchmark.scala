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
import org.apache.spark.scheduler.CompressedMapStatus
import org.apache.spark.storage.BlockManagerId

/**
 * Benchmark for MapStatuses serialization performance.
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class>
 *        --jars <catalyst test jar>,<core test jar>,<spark-avro jar> <avro test jar>
 *   2. build/sbt "avro/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/test:runMain <this class>"
 *      Results will be written to "benchmarks/AvroReadBenchmark-results.txt".
 * }}}
 */
object MapStatusesSerializationBenchmark extends BenchmarkBase {

  var sc: SparkContext = null

  def serializationBenchmark(numMaps: Int, blockSize: Int,
    minBroadcastSize: Int = Int.MaxValue): Unit = {
    val benchmark = new Benchmark(s"MapStatuses Serialization with $numMaps MapOutput",
      numMaps, output = output)

    val shuffleId = 10
    val tracker = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    val rpcEnv = sc.env.rpcEnv
    val masterEndpoint = new MapOutputTrackerMasterEndpoint(rpcEnv, tracker, sc.getConf)
    rpcEnv.stop(tracker.trackerEndpoint)
    rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME, masterEndpoint)


    tracker.registerShuffle(shuffleId, numMaps)
    val r = new scala.util.Random(912)
    (0 until numMaps).foreach { i =>
      tracker.registerMapOutput(shuffleId, i,
        new CompressedMapStatus(BlockManagerId(s"node$i", s"node$i.spark.apache.org", 1000),
          Array.range(0, 500).map(i => math.abs(r.nextLong())), i))
    }

    val shuffleStatus = tracker.shuffleStatuses.get(shuffleId).head


    var serializedMapStatusSizes = 0
    var serializedBroadcastSizes = 0

    val (serializedMapStatus, serializedBroadcast) = MapOutputTracker.serializeMapStatuses(
      shuffleStatus.mapStatuses, tracker.broadcastManager, tracker.isLocal, minBroadcastSize)
    serializedMapStatusSizes = serializedMapStatus.length
    if (serializedBroadcast != null) {
      serializedBroadcastSizes = serializedBroadcast.value.length
    }


    benchmark.addCase("Serialization") { _ =>
      MapOutputTracker.serializeMapStatuses(
        shuffleStatus.mapStatuses, tracker.broadcastManager, tracker.isLocal, minBroadcastSize)
    }

    benchmark.run()
    tracker.unregisterShuffle(shuffleId)
    tracker.stop()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    createSparkContext()
    serializationBenchmark(200000, 500)
  }

  def createSparkContext(): Unit = {
    val conf = new SparkConf()
    if (sc != null) {
      sc.stop()
    }
    sc = new SparkContext("local", "MapStatusesSerializationBenchmark", conf)
  }

  override def afterAll(): Unit = {
    if (sc != null) {
      sc.stop()
    }
  }
}