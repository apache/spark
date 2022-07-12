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

import org.roaringbitmap.RoaringBitmap

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.scheduler.{HighlyCompressedMapStatus, MapStatus, MergeStatus}
import org.apache.spark.storage.BlockManagerId

/**
 * Benchmark to measure performance for converting mapStatuses and mergeStatuses.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/MapStatusesConvertBenchmark-results.txt".
 * }}}
 * */
object MapStatusesConvertBenchmark extends BenchmarkBase {

  private def convertMapStatus(numIters: Int): Unit = {

    val benchmark = new Benchmark("MapStatuses Convert", 1, output = output)

    val blockManagerNumber = 1000
    val mapNumber = 50000
    val shufflePartitions = 10000

    val shuffleId: Int = 0
    // First reduce task will fetch map data from startPartition to endPartition
    val startPartition = 0
    val startMapIndex = 0
    val endMapIndex = mapNumber
    val blockManagers = Array.tabulate(blockManagerNumber) { i =>
      BlockManagerId("a", "host" + i, 7337)
    }
    val mapStatuses: Array[MapStatus] = Array.tabulate(mapNumber) { mapTaskId =>
      HighlyCompressedMapStatus(
        blockManagers(mapTaskId % blockManagerNumber),
        Array.tabulate(shufflePartitions)(i => if (i % 50 == 0) 1 else 0),
        mapTaskId)
    }
    val bitmap = new RoaringBitmap()
    Range(0, 4000).foreach(bitmap.add(_))
    val mergeStatuses = Array.tabulate(shufflePartitions) { part =>
      MergeStatus(blockManagers(part % blockManagerNumber), shuffleId, bitmap, 100)
    }

    Array(499, 999, 1499).foreach { endPartition =>
      benchmark.addCase(
        s"Num Maps: $mapNumber Fetch partitions:${endPartition - startPartition + 1}",
        numIters) { _ =>
        MapOutputTracker.convertMapStatuses(
          shuffleId,
          startPartition,
          endPartition,
          mapStatuses,
          startMapIndex,
          endMapIndex,
          Some(mergeStatuses))
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val numIters = 3
    runBenchmark("MapStatuses Convert Benchmark") {
      convertMapStatus(numIters)
    }
  }
}
