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

package org.apache.spark.sql.execution.benchmark

import scala.collection.mutable
import scala.concurrent.duration._

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.BloomFilterMightContain
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.metric.SQLShuffleWriteMetricsReporter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.storage.StorageLevel

/**
 * Measures runtime Bloom filtering when a materialized cache hides a selective predicate.
 *
 * To run this benchmark:
 * {{{
 *   build/sbt "sql/Test/runMain
 *     org.apache.spark.sql.execution.benchmark.RuntimeBloomFilterCachedInputBenchmark"
 * }}}
 *
 * The additional shuffle metric reports only the wide fact-side exchange, independently of
 * exchanges used to aggregate results or construct the Bloom filter.
 */
object RuntimeBloomFilterCachedInputBenchmark extends SqlBasedBenchmark {

  private val factRows = 500000L
  private val filteringStride = 100L
  private val partitions = 4

  override def getSparkSession: SparkSession = {
    SparkSession.builder()
      .master("local[4]")
      .appName(this.getClass.getCanonicalName)
      .config(SQLConf.SHUFFLE_PARTITIONS.key, 4)
      .config("spark.ui.enabled", false)
      .getOrCreate()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SHUFFLE_PARTITIONS.key -> partitions.toString,
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "0",
        SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "10MB",
        SQLConf.IGNORE_CORRUPT_FILES.key -> "false",
        SQLConf.IGNORE_MISSING_FILES.key -> "false") {
      val fact = spark.range(0, factRows, 1, partitions).selectExpr(
        "id AS fact_key",
        "sha2(cast(id AS STRING), 256) AS payload")
      val dimension = spark.range(0, factRows, 1, partitions)
        .filter(s"id % $filteringStride = 0")
        .selectExpr("id AS dimension_key")
        .persist(StorageLevel.MEMORY_AND_DISK)

      try {
        assert(dimension.count() == factRows / filteringStride)

        runBenchmark("Runtime Bloom filter from a materialized selective cache") {
          val factShuffleBytes = mutable.Map.empty[Boolean, Long]
          val benchmark = new Benchmark(
            "Cached input runtime Bloom filter",
            factRows,
            minNumIters = 2,
            warmupTime = 1.second,
            minTime = Duration.Zero,
            output = output)

          Seq(false, true).foreach { enabled =>
            val name = if (enabled) {
              "Runtime Bloom filter enabled"
            } else {
              "Runtime Bloom filter disabled"
            }
            benchmark.addCase(name, numIters = 2) { _ =>
              withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> enabled.toString) {
                val result = fact.join(dimension, fact("fact_key") === dimension("dimension_key"))
                  .selectExpr("sum(length(payload)) AS payload_bytes")
                val observed = result.collect().head.getLong(0)
                assert(observed == factRows / filteringStride * 64)

                val optimizedPlan = result.queryExecution.optimizedPlan
                val hasRuntimeBloomFilter = optimizedPlan.exists { node =>
                  node.expressions.exists(_.exists(_.isInstanceOf[BloomFilterMightContain]))
                }
                assert(hasRuntimeBloomFilter == enabled,
                  s"Expected runtime Bloom filter enabled=$enabled:\n$optimizedPlan")

                val factExchanges = result.queryExecution.executedPlan.collect {
                  case exchange: ShuffleExchangeExec
                      if exchange.output.exists(_.name == "fact_key") => exchange
                }
                assert(factExchanges.size == 1,
                  s"Expected one fact-side shuffle exchange, found ${factExchanges.size}")
                factShuffleBytes(enabled) =
                  factExchanges.head.metrics(SQLShuffleWriteMetricsReporter.SHUFFLE_BYTES_WRITTEN)
                    .value
              }
            }
          }

          benchmark.run()

          val baseline = factShuffleBytes(false)
          val optimized = factShuffleBytes(true)
          assert(baseline > 0 && optimized < baseline,
            s"Expected the Bloom filter to reduce fact shuffle bytes: $baseline -> $optimized")
          val reduction = (baseline - optimized) * 100.0 / baseline
          // scalastyle:off println
          benchmark.out.println(s"Fact-side shuffle bytes without runtime Bloom filter: $baseline")
          benchmark.out.println(s"Fact-side shuffle bytes with runtime Bloom filter: $optimized")
          benchmark.out.println(f"Fact-side shuffle reduction: $reduction%.2f%%")
          // scalastyle:on println
        }
      } finally {
        dimension.unpersist(blocking = true)
      }
    }
  }
}
