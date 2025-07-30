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

package org.apache.spark.memory

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.internal.config.{MEMORY_FRACTION, MEMORY_OFFHEAP_ENABLED, MEMORY_OFFHEAP_SIZE}
import org.apache.spark.internal.config.Tests.{IS_TESTING, TEST_MEMORY}

/**
 * Benchmark for Spark memory consumer and its downstream memory APIs. To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/MemoryConsumerBenchmark-results.txt".
 * }}}
 */
object MemoryConsumerBenchmark extends BenchmarkBase {
  private val N = 10000
  private val testTaskMemoryManager =
    new TaskMemoryManager(createTestMemoryManager(1L * 1000 * 1000, 1L * 1000 * 1000), 0)
  private val userTaskMemoryManager =
    new TaskMemoryManager(createUnifiedMemoryManager(1L * 1000 * 1000, 1L * 1000 * 1000), 0)
  private val testOnHeapConsumer =
    new TestMemoryConsumer(testTaskMemoryManager, MemoryMode.ON_HEAP)
  private val testOffHeapConsumer =
    new TestMemoryConsumer(testTaskMemoryManager, MemoryMode.OFF_HEAP)
  private val userOnHeapConsumer =
    new TestMemoryConsumer(userTaskMemoryManager, MemoryMode.ON_HEAP)
  private val userOffHeapConsumer =
    new TestMemoryConsumer(userTaskMemoryManager, MemoryMode.OFF_HEAP)


  /**
   * Creates a test memory manager which is used as the benchmark baseline.
   */
  private def createTestMemoryManager(
    maxOnHeapExecutionMemory: Long,
    maxOffHeapExecutionMemory: Long): TestMemoryManager = {
    val conf = new SparkConf()
      .set(IS_TESTING, true)
      .set(TEST_MEMORY, maxOnHeapExecutionMemory)
      .set(MEMORY_FRACTION, 1.0)
      .set(MEMORY_OFFHEAP_ENABLED, true)
      .set(MEMORY_OFFHEAP_SIZE, maxOffHeapExecutionMemory)
    new TestMemoryManager(conf)
  }

  /**
   * Creates a unified memory manager which reflects the case in user
   * environments.
   */
  private def createUnifiedMemoryManager(
      maxOnHeapExecutionMemory: Long,
      maxOffHeapExecutionMemory: Long): UnifiedMemoryManager = {
    val conf = new SparkConf()
      .set(IS_TESTING, true)
      .set(TEST_MEMORY, maxOnHeapExecutionMemory)
      .set(MEMORY_FRACTION, 1.0)
      .set(MEMORY_OFFHEAP_ENABLED, true)
      .set(MEMORY_OFFHEAP_SIZE, maxOffHeapExecutionMemory)
    UnifiedMemoryManager(conf, numCores = 2)
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val onHeapBenchmark = new Benchmark(s"On-heap allocations", N, output = output)
    onHeapBenchmark.addCase("use and free - TestMemoryManager", 3) { _ =>
      testOnHeapConsumer.use(1L)
      testOnHeapConsumer.free(1L)
    }
    onHeapBenchmark.addCase("use and free - UnifiedMemoryManager", 3) { _ =>
      userOnHeapConsumer.use(1L)
      userOnHeapConsumer.free(1L)
    }
    onHeapBenchmark.run()

    val offHeapBenchmark = new Benchmark(s"Off-heap allocations", N, output = output)
    offHeapBenchmark.addCase("use and free - TestMemoryManager", 3) { _ =>
      testOffHeapConsumer.use(1L)
      testOffHeapConsumer.free(1L)
    }
    offHeapBenchmark.addCase("use and free - UnifiedMemoryManager", 3) { _ =>
      userOffHeapConsumer.use(1L)
      userOffHeapConsumer.free(1L)
    }
    offHeapBenchmark.run()
  }
}
