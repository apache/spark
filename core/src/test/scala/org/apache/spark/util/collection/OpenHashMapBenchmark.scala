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

package org.apache.spark.util.collection

import java.util.{HashMap => JHashMap, Random}

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

/**
 * Benchmark for OpenHashMap vs java.util.HashMap.
 * Measures insert, aggregate (changeValue/merge), and random-order lookup performance
 * with String keys and Long values.
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/OpenHashMapBenchmark-results.txt".
 * }}}
 */
object OpenHashMapBenchmark extends BenchmarkBase {

  private val numKeys = 1000000
  private val numAggOps = 5000000

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("OpenHashMap vs java.util.HashMap") {
      insertBenchmark()
      aggregateBenchmark()
      lookupBenchmark()
    }
  }

  private def stringKeys: Array[String] = Array.tabulate(numKeys)(i => "key_" + i)

  private def insertBenchmark(): Unit = {
    val keys = stringKeys
    val benchmark = new Benchmark(s"Insert $numKeys distinct String keys", numKeys,
      output = output)
    benchmark.addCase("OpenHashMap") { _ =>
      val map = new OpenHashMap[String, Long](64)
      var i = 0
      while (i < numKeys) {
        map.update(keys(i), i.toLong)
        i += 1
      }
    }
    benchmark.addCase("java.util.HashMap") { _ =>
      val map = new JHashMap[String, java.lang.Long](16)
      var i = 0
      while (i < numKeys) {
        map.put(keys(i), i.toLong)
        i += 1
      }
    }
    benchmark.run()
  }

  private def aggregateBenchmark(): Unit = {
    val keys = stringKeys
    val random = new Random(42)
    val aggIndices = Array.fill(numAggOps)(random.nextInt(numKeys))
    val benchmark = new Benchmark(s"Aggregate $numAggOps ops on $numKeys String keys", numAggOps,
      output = output)
    benchmark.addCase("OpenHashMap changeValue") { _ =>
      val map = new OpenHashMap[String, Long](64)
      var i = 0
      while (i < numAggOps) {
        map.changeValue(keys(aggIndices(i)), 1L, _ + 1L)
        i += 1
      }
    }
    benchmark.addCase("java.util.HashMap merge") { _ =>
      val map = new JHashMap[String, java.lang.Long](16)
      var i = 0
      while (i < numAggOps) {
        map.merge(keys(aggIndices(i)), 1L, (a, b) => a + b)
        i += 1
      }
    }
    benchmark.run()
  }

  private def lookupBenchmark(): Unit = {
    val keys = stringKeys
    val random = new Random(42)
    // Look up in random order so that neither map benefits from the memory layout produced by
    // sequential insertion.
    val lookupIndices = Array.fill(numKeys)(random.nextInt(numKeys))
    val openHashMap = new OpenHashMap[String, Long](64)
    val jHashMap = new JHashMap[String, java.lang.Long](16)
    var i = 0
    while (i < numKeys) {
      openHashMap.update(keys(i), i.toLong)
      jHashMap.put(keys(i), i.toLong)
      i += 1
    }
    val benchmark = new Benchmark(s"Look up $numKeys String keys in random order", numKeys,
      output = output)
    benchmark.addCase("OpenHashMap") { _ =>
      var sum = 0L
      var i = 0
      while (i < numKeys) {
        sum += openHashMap(keys(lookupIndices(i)))
        i += 1
      }
    }
    benchmark.addCase("java.util.HashMap") { _ =>
      var sum = 0L
      var i = 0
      while (i < numKeys) {
        sum += jHashMap.get(keys(lookupIndices(i)))
        i += 1
      }
    }
    benchmark.run()
  }
}
