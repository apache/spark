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

import java.util.concurrent.Callable

import scala.concurrent.duration.Duration
import scala.util.Random

import com.github.benmanes.caffeine.cache.{CacheLoader => CaffeineCacheLoader, Caffeine}
import com.google.common.cache.{CacheBuilder, CacheLoader}

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.util.ThreadUtils

/**
 * Benchmark for Guava Cache vs Caffeine.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar>
 *   2. build/sbt "core/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/test:runMain <this class>"
 *      Results will be written to "benchmarks/LocalCacheBenchmark-results.txt".
 * }}}
 */
object LocalCacheBenchmark extends BenchmarkBase {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Loading Cache") {
      val size = 10000
      val parallelism = 8
      val guavaCacheConcurrencyLevel = 8
      val dataset = (1 to parallelism)
        .map(_ => Random.shuffle(List.range(0, size)))
        .map(list => list.map(i => TestData(i)))
      val executor = ThreadUtils.newDaemonFixedThreadPool(parallelism, "Loading Cache Test Pool")
      val guavaCacheLoader = new CacheLoader[TestData, TestData]() {
        override def load(id: TestData): TestData = {
          id
        }
      }
      val caffeineCacheLoader = new CaffeineCacheLoader[TestData, TestData]() {
        override def load(id: TestData): TestData = {
          id
        }
      }

      val benchmark = new Benchmark("Loading Cache", size * parallelism, 3, output = output)
      benchmark.addCase("Guava Cache") { _ =>
        val cache = CacheBuilder.newBuilder()
          .concurrencyLevel(guavaCacheConcurrencyLevel).build[TestData, TestData](guavaCacheLoader)
        dataset.map(dataList => executor.submit(new Callable[Unit] {
          override def call(): Unit = {
            dataList.foreach(key => cache.get(key))
          }
        })).foreach(future => ThreadUtils.awaitResult(future, Duration.Inf))
        cache.cleanUp()
      }

      benchmark.addCase("Caffeine") { _ =>
        val cache = Caffeine.newBuilder().build[TestData, TestData](caffeineCacheLoader)
        dataset.map(dataList => executor.submit(new Callable[Unit] {
          override def call(): Unit = {
            dataList.foreach(key => cache.get(key))
          }
        })).foreach(future => ThreadUtils.awaitResult(future, Duration.Inf))
        cache.cleanUp()
      }

      benchmark.run()
    }
  }

  case class TestData(content: Int)
}

