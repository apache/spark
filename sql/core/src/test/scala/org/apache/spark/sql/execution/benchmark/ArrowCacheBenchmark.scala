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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializer
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

/**
 * Benchmark to measure cache performance with Arrow format vs Default format.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *     --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/ArrowCacheBenchmark-results.txt".
 * }}}
 */
object ArrowCacheBenchmark extends SqlBasedBenchmark {

  // Create separate sessions for each cache format since SPARK_CACHE_SERIALIZER is static
  private def createSession(serializer: String): SparkSession = {
    SparkSession.builder()
      .master("local[1]")
      .appName(s"ArrowCacheBenchmark-$serializer")
      .config(SQLConf.SHUFFLE_PARTITIONS.key, 1)
      .config(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, 1)
      .config(UI_ENABLED.key, false)
      .config(StaticSQLConf.SPARK_CACHE_SERIALIZER.key, serializer)
      .getOrCreate()
  }

  private def cachePrimitiveTypes(): Unit = {
    val numRows = 5000000 // 5M rows for faster benchmarking
    runBenchmark("Cache primitive types") {
      val benchmark = new Benchmark("Cache 5M rows with primitives", numRows, output = output)

      val sparkDefault = createSession(
        "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer")
      val sparkArrow = createSession(classOf[ArrowCachedBatchSerializer].getName)

      try {
        // Create data in both sessions
        val dfDefault = sparkDefault.range(numRows).selectExpr(
          "id as int_col",
          "id * 2L as long_col",
          "cast(id as double) as double_col"
        )

        val dfArrow = sparkArrow.range(numRows).selectExpr(
          "id as int_col",
          "id * 2L as long_col",
          "cast(id as double) as double_col"
        )

        benchmark.addCase("Default cache - write + read") { _ =>
          dfDefault.cache()
          dfDefault.count()
          dfDefault.unpersist(blocking = true)
        }

        benchmark.addCase("Arrow cache - write + read") { _ =>
          dfArrow.cache()
          dfArrow.count()
          dfArrow.unpersist(blocking = true)
        }

        benchmark.run()
      } finally {
        sparkDefault.stop()
        sparkArrow.stop()
      }
    }
  }

  private def cacheWithFilters(): Unit = {
    val numRows = 5000000 // 5M rows
    runBenchmark("Cache with filter pushdown") {
      val benchmark = new Benchmark("Cache 5M rows + filter", numRows, output = output)

      val sparkDefault = createSession(
        "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer")
      val sparkArrow = createSession(classOf[ArrowCachedBatchSerializer].getName)

      try {
        val dfDefault = sparkDefault.range(numRows).selectExpr(
          "id as int_col",
          "cast(id as double) as double_col"
        )

        val dfArrow = sparkArrow.range(numRows).selectExpr(
          "id as int_col",
          "cast(id as double) as double_col"
        )

        // Pre-cache the data
        val cachedDefault = dfDefault.cache()
        cachedDefault.count()

        val cachedArrow = dfArrow.cache()
        cachedArrow.count()

        benchmark.addCase("Default cache - filter") { _ =>
          cachedDefault.filter("int_col > 2500000").count()
        }

        benchmark.addCase("Arrow cache - filter (with stats)") { _ =>
          cachedArrow.filter("int_col > 2500000").count()
        }

        cachedDefault.unpersist(blocking = true)
        cachedArrow.unpersist(blocking = true)

        benchmark.run()
      } finally {
        sparkDefault.stop()
        sparkArrow.stop()
      }
    }
  }

  private def cacheColumnarInput(): Unit = {
    val numRows = 2000000 // 2M rows
    withTempPath { dir =>
      val path = dir.getAbsolutePath

      // Write parquet file (columnar format) using the default spark session
      spark.range(numRows).selectExpr(
        "id as int_col",
        "id * 2L as long_col",
        "cast(id as double) as double_col"
      ).write.parquet(path)

      runBenchmark("Cache columnar input (Parquet)") {
        val benchmark = new Benchmark("Cache 2M rows from Parquet", numRows, output = output)

        val sparkDefault = createSession(
          "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer")
        val sparkArrow = createSession(classOf[ArrowCachedBatchSerializer].getName)

        try {
          val parquetDefault = sparkDefault.read.parquet(path)
          val parquetArrow = sparkArrow.read.parquet(path)

          benchmark.addCase("Default cache - columnar input") { _ =>
            parquetDefault.cache()
            parquetDefault.count()
            parquetDefault.unpersist(blocking = true)
          }

          benchmark.addCase("Arrow cache - columnar input (zero-copy)") { _ =>
            parquetArrow.cache()
            parquetArrow.count()
            parquetArrow.unpersist(blocking = true)
          }

          benchmark.run()
        } finally {
          sparkDefault.stop()
          sparkArrow.stop()
        }
      }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Arrow Cache vs Default Cache") {
      cachePrimitiveTypes()
      cacheWithFilters()
      cacheColumnarInput()
    }
  }
}
