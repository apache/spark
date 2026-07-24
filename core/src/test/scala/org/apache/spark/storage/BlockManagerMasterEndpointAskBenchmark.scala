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

package org.apache.spark.storage

import java.util.concurrent.ExecutorService

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.internal.config.STORAGE_BLOCK_MANAGER_MASTER_VIRTUAL_THREADS
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Benchmark the fan-out behaviour of the `BlockManagerMasterEndpoint` ask
 * thread pool under platform threads (current default, cap 100) and under
 * virtual threads (SPARK-49807).
 *
 * The benchmark submits `N` concurrent tasks, each sleeping `sleepMillis` to
 * simulate an RPC round-trip, and measures wall-clock time for the whole
 * `Future.sequence`. With the 100-thread cap, N > 100 serialises into
 * `ceil(N/100)` waves; virtual threads should complete in ~1 wave regardless
 * of N.
 *
 * {{{
 *   To run this benchmark:
 *   1. build/sbt "core/Test/runMain \
 *        org.apache.spark.storage.BlockManagerMasterEndpointAskBenchmark"
 *   2. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt \
 *        "core/Test/runMain org.apache.spark.storage.BlockManagerMasterEndpointAskBenchmark"
 *      Results will be written to
 *      "core/benchmarks/BlockManagerMasterEndpointAskBenchmark-results.txt".
 *   Virtual-thread cases require Java 21+ and are skipped on earlier JDKs.
 * }}}
 */
object BlockManagerMasterEndpointAskBenchmark extends BenchmarkBase {

  /** Simulated RPC round-trip latency per task. */
  private val sleepMillis = 20L

  /** Number of benchmark iterations per case. */
  private val iterations = 3

  /** Upper bound for any individual fan-out run. */
  private val awaitTimeout = 5.minutes

  private val platformConf = new SparkConf()
  private val virtualConf = new SparkConf()
    .set(STORAGE_BLOCK_MANAGER_MASTER_VIRTUAL_THREADS, true)

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark(s"Ask fan-out (${sleepMillis}ms simulated RPC per endpoint)") {
      Seq(50, 500, 5000).foreach { n =>
        val benchmark = new Benchmark(
          s"Fan-out over $n endpoints", n.toLong, iterations, output = output)

        benchmark.addCase("Platform threads (cap=100)") { _ =>
          val pool = BlockManagerMasterEndpoint.createAskThreadPool(platformConf)
          try runFanout(pool, n) finally pool.shutdownNow()
        }

        if (Utils.isJavaVersionAtLeast21) {
          benchmark.addCase("Virtual threads") { _ =>
            val pool = BlockManagerMasterEndpoint.createAskThreadPool(virtualConf)
            try runFanout(pool, n) finally pool.shutdownNow()
          }
        }

        benchmark.run()
      }
    }
  }

  private def runFanout(pool: ExecutorService, n: Int): Unit = {
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(pool)
    val futures = (1 to n).map(_ => Future(Thread.sleep(sleepMillis)))
    ThreadUtils.awaitResult(Future.sequence(futures), awaitTimeout)
  }
}
