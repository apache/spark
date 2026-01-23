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

package org.apache.spark.sql

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.util.sketch.BloomFilter._
import org.apache.spark.util.sketch.BloomFilter.Version._

/**
 * Benchmark for Spark's BloomFilter implementations (BloomFilterImpl and BloomFilterImplV2)
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> <spark catalyst test jar>
 *   2. build/sbt "catalyst/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "catalyst/Test/runMain <this class>"
 *      Results will be written to "benchmarks/SparkBloomFilterBenchmark-results.txt".
 * }}}
 */
object SparkBloomFilterBenchmark extends BenchmarkBase {

  private val DEFAULT_SEED = 0
  private val SMALL_ITEMS = 10000
  private val MEDIUM_ITEMS = 100000
  private val LARGE_ITEMS = 1000000

  /**
   * Tests PUT operation performance with different data sizes.
   */
  private def benchmarkPutOperation(
      numItems: Int,
      valuesPerIteration: Int,
      fpp: Double = 0.03): Unit = {
    val benchmark = new Benchmark(
      s"Put Operation - $numItems items, FPP: $fpp",
      valuesPerIteration,
      output = output)

    // Test BloomFilterImpl (V1)
    benchmark.addCase(s"BloomFilterImpl V1 - $numItems", 3) { _ =>
      val bf = create(V1, numItems, optimalNumOfBits(numItems, fpp), DEFAULT_SEED)
      var i = 0
      while (i < valuesPerIteration) {
        bf.put(i.toLong)
        i += 1
      }
    }

    // Test BloomFilterImplV2 (V2)
    benchmark.addCase(s"BloomFilterImplV2 - $numItems", 3) { _ =>
      val bf = create(V2, numItems, optimalNumOfBits(numItems, fpp), DEFAULT_SEED)
      var i = 0
      while (i < valuesPerIteration) {
        bf.put(i.toLong)
        i += 1
      }
    }

    benchmark.run()
  }

  /**
   * Tests query operation performance with different hit rates and data sizes
   */
  private def benchmarkMightContainOperation(
      numItems: Int,
      valuesPerIteration: Int,
      fpp: Double = 0.03,
      hitRate: Double = 0.5): Unit = {
    val benchmark = new Benchmark(
      s"MightContain Operation (Hit Rate: ${hitRate * 100}%) - $numItems items, FPP: $fpp",
      valuesPerIteration,
      output = output)

    // Prepare BloomFilter with existing items
    val existingItems = (0 until numItems).toArray
    val testItems = (0 until valuesPerIteration).map { i =>
      if (i < (valuesPerIteration * hitRate).toInt) {
        // Existing items (hits)
        existingItems(i % numItems).toLong
      } else {
        // New items (potential misses)
        (numItems + i).toLong
      }
    }.toArray

    // Test BloomFilterImpl (V1)
    benchmark.addTimerCase(s"BloomFilterImpl V1 - $numItems", 3) { timer =>
      val bf = create(V1, numItems, optimalNumOfBits(numItems, fpp), DEFAULT_SEED)
      // Populate with existing items
      existingItems.foreach(i => bf.put(i.toLong))

      timer.startTiming()
      var i = 0
      while (i < valuesPerIteration) {
        bf.mightContain(testItems(i))
        i += 1
      }
      timer.stopTiming()
    }

    // Test BloomFilterImplV2 (V2)
    benchmark.addTimerCase(s"BloomFilterImplV2 - $numItems", 3) { timer =>
      val bf = create(V2, numItems, optimalNumOfBits(numItems, fpp), DEFAULT_SEED)
      // Populate with existing items
      existingItems.foreach(i => bf.put(i.toLong))

      timer.startTiming()
      var i = 0
      while (i < valuesPerIteration) {
        bf.mightContain(testItems(i))
        i += 1
      }
      timer.stopTiming()
    }

    benchmark.run()
  }

  private def benchmarkBinaryPutOperation(
      numItems: Int,
      valuesPerIteration: Int,
      fpp: Double = 0.03): Unit = {
    val benchmark = new Benchmark(
      s"Binary PUT Operation - $numItems items, FPP: $fpp",
      valuesPerIteration,
      output = output)

    // Prepare binary data - simple UTF-8 conversion like putString implementation
    val binaryData = (0 until numItems).map { i =>
      s"item_${i}_test_binary_data_for_put_${System.currentTimeMillis()}".getBytes("UTF-8")
    }.toArray

    // Test V1 with binary PUT
    benchmark.addCase(s"BloomFilterImpl V1 - $numItems", 3) { _ =>
      val bf = create(V1, numItems, optimalNumOfBits(numItems, fpp), DEFAULT_SEED)
      var i = 0
      while (i < valuesPerIteration) {
        bf.putBinary(binaryData(i % numItems))
        i += 1
      }
    }

    // Test V2 with binary PUT
    benchmark.addCase(s"BloomFilterImplV2 - $numItems", 3) { _ =>
      val bf = create(V2, numItems, optimalNumOfBits(numItems, fpp), DEFAULT_SEED)
      var i = 0
      while (i < valuesPerIteration) {
        bf.putBinary(binaryData(i % numItems))
        i += 1
      }
    }

    benchmark.run()
  }

  private def benchmarkBinaryMightContainOperation(
      numItems: Int,
      valuesPerIteration: Int,
      fpp: Double = 0.03,
      hitRate: Double = 0.5): Unit = {
    val benchmark = new Benchmark(
      s"Binary Query Operation (Hit Rate: ${hitRate*100}%) - $numItems items, FPP: $fpp",
      valuesPerIteration,
      output = output)

    // Prepare binary data for existing items
    val binaryData = (0 until numItems).map { i =>
      s"item_${i}_test_binary_data_for_query_${System.currentTimeMillis()}".getBytes("UTF-8")
    }.toArray

    // Prepare query data with specified hit rate
    val queryBinaryData = (0 until valuesPerIteration).map { i =>
      if (i < (valuesPerIteration * hitRate).toInt) {
        binaryData(i % numItems)  // Existing data (hit)
      } else {
        // New binary data (likely miss)
        s"new_item_${i}_not_in_filter_${System.currentTimeMillis()}".getBytes("UTF-8")
      }
    }.toArray

    // Test V1 with binary QUERY
    benchmark.addTimerCase(s"BloomFilterImpl V1 - $numItems", 3) { timer =>
      val bf = create(V1, numItems, optimalNumOfBits(numItems, fpp), DEFAULT_SEED)
      // Populate with binary data
      binaryData.foreach(data => bf.putBinary(data))

      timer.startTiming()
      var i = 0
      while (i < valuesPerIteration) {
        bf.mightContainBinary(queryBinaryData(i))
        i += 1
      }
      timer.stopTiming()
    }

    // Test V2 with binary QUERY
    benchmark.addTimerCase(s"BloomFilterImplV2 - $numItems", 3) { timer =>
      val bf = create(V2, numItems, optimalNumOfBits(numItems, fpp), DEFAULT_SEED)
      // Populate with binary data
      binaryData.foreach(data => bf.putBinary(data))

      timer.startTiming()
      var i = 0
      while (i < valuesPerIteration) {
        bf.mightContainBinary(queryBinaryData(i))
        i += 1
      }
      timer.stopTiming()
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    runBenchmark("Put Operation - Small Scale") {
      benchmarkPutOperation(SMALL_ITEMS, 10000)
    }

    runBenchmark("Put Operation - Medium Scale") {
      benchmarkPutOperation(MEDIUM_ITEMS, 100000)
    }

    runBenchmark("Put Operation - Large Scale") {
      benchmarkPutOperation(LARGE_ITEMS, 1000000)
    }

    runBenchmark("MightContain Operation - Small Scale") {
      benchmarkMightContainOperation(SMALL_ITEMS, 10000, 0.03, 0.5)
    }

    runBenchmark("MightContain Operation - Medium Scale") {
      benchmarkMightContainOperation(MEDIUM_ITEMS, 100000, 0.03, 0.5)
    }

    runBenchmark("MightContain Operation - Large Scale") {
      benchmarkMightContainOperation(LARGE_ITEMS, 1000000, 0.03, 0.5)
    }

    runBenchmark("FPP Impact on Put Operations") {
      benchmarkPutOperation(MEDIUM_ITEMS, 100000, 0.01)  // Low FPP
      benchmarkPutOperation(MEDIUM_ITEMS, 100000, 0.03)  // Default FPP
      benchmarkPutOperation(MEDIUM_ITEMS, 100000, 0.05)  // High FPP
    }

    runBenchmark("FPP Impact on Query Operations") {
      benchmarkMightContainOperation(MEDIUM_ITEMS, 100000, 0.01, 0.5)  // Low FPP, 50% hit rate
      benchmarkMightContainOperation(MEDIUM_ITEMS, 100000, 0.03, 0.5)  // Default FPP, 50% hit rate
      benchmarkMightContainOperation(MEDIUM_ITEMS, 100000, 0.05, 0.5)  // High FPP, 50% hit rate
    }

    runBenchmark("Hit Rate Impact Analysis") {
      benchmarkMightContainOperation(MEDIUM_ITEMS, 100000, 0.03, 0.1)  // 10% hit rate
      benchmarkMightContainOperation(MEDIUM_ITEMS, 100000, 0.03, 0.5)  // 50% hit rate
      benchmarkMightContainOperation(MEDIUM_ITEMS, 100000, 0.03, 0.9)  // 90% hit rate
    }

    runBenchmark("Binary Put Operation - Small Scale") {
      benchmarkBinaryPutOperation(SMALL_ITEMS, 10000)
    }

    runBenchmark("Binary Put Operation - Medium Scale") {
      benchmarkBinaryPutOperation(MEDIUM_ITEMS, 100000)
    }

    runBenchmark("Binary Put Operation - Large Scale") {
      benchmarkBinaryPutOperation(LARGE_ITEMS, 1000000)
    }

    runBenchmark("Binary Query Operation - Small Scale") {
      benchmarkBinaryMightContainOperation(SMALL_ITEMS, 10000, 0.03, 0.5)
    }

    runBenchmark("Binary Query Operation - Medium Scale") {
      benchmarkBinaryMightContainOperation(MEDIUM_ITEMS, 100000, 0.03, 0.5)
    }

    runBenchmark("Binary Query Operation - Large Scale") {
      benchmarkBinaryMightContainOperation(LARGE_ITEMS, 1000000, 0.03, 0.5)
    }

    runBenchmark("FPP Impact on Binary Put Operations") {
      benchmarkBinaryPutOperation(MEDIUM_ITEMS, 100000, 0.01)  // Low FPP
      benchmarkBinaryPutOperation(MEDIUM_ITEMS, 100000, 0.03)  // Default FPP
      benchmarkBinaryPutOperation(MEDIUM_ITEMS, 100000, 0.05)  // High FPP
    }

    runBenchmark("FPP Impact on Binary Query Operations") {
      benchmarkBinaryMightContainOperation(MEDIUM_ITEMS, 100000, 0.01, 0.5)  // Low FPP
      benchmarkBinaryMightContainOperation(MEDIUM_ITEMS, 100000, 0.03, 0.5)  // Default FPP
      benchmarkBinaryMightContainOperation(MEDIUM_ITEMS, 100000, 0.05, 0.5)  // High FPP
    }

    runBenchmark("Hit Rate Impact on Binary Operations") {
      benchmarkBinaryMightContainOperation(MEDIUM_ITEMS, 100000, 0.03, 0.1)  // 10% hit rate
      benchmarkBinaryMightContainOperation(MEDIUM_ITEMS, 100000, 0.03, 0.5)  // 50% hit rate
      benchmarkBinaryMightContainOperation(MEDIUM_ITEMS, 100000, 0.03, 0.9)  // 90% hit rate
    }
  }
}
