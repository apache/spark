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

package org.apache.spark.rdd

import scala.collection.immutable

import org.apache.spark.SparkContext
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

/**
 * Benchmark for CoalescedRDD.
 * Measures rdd.coalesce performance under various combinations of
 * coalesced partitions and preferred hosts
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar>
 *   2. build/sbt "core/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/test:runMain <this class>"
 *      Results will be written to "benchmarks/CoalescedRDD-results.txt".
 * }}}
 * */
object CoalescedRDDBenchmark extends BenchmarkBase {
  val seed = 0x1337
  val sc = new SparkContext(master = "local[4]", appName = "test")

  private def coalescedRDD(numIters: Int): Unit = {
    val numBlocks = 100000
    val benchmark = new Benchmark("Coalesced RDD", numBlocks, output = output)
    for (numPartitions <- Seq(100, 500, 1000, 5000, 10000)) {
      for (numHosts <- Seq(1, 5, 10, 20, 40, 80)) {

        import collection.mutable
        val hosts = mutable.ArrayBuffer[String]()
        (1 to numHosts).foreach(hosts += "m" + _)
        hosts.length
        val rnd = scala.util.Random
        rnd.setSeed(seed)
        val blocks: immutable.Seq[(Int, Seq[String])] = (1 to numBlocks).map { i =>
          (i, hosts(rnd.nextInt(hosts.size)) :: Nil)
        }

        benchmark.addCase(
          s"Coalesce Num Partitions: $numPartitions Num Hosts: $numHosts",
          numIters) { _ =>
          performCoalesce(blocks, numPartitions)
        }
      }
    }

    benchmark.run()
  }

  private def performCoalesce(blocks: immutable.Seq[(Int, Seq[String])], numPartitions: Int) {
    sc.makeRDD(blocks).coalesce(numPartitions).partitions
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val numIters = 3
    runBenchmark("Coalesced RDD , large scale") {
      coalescedRDD(numIters)
    }
  }
}
