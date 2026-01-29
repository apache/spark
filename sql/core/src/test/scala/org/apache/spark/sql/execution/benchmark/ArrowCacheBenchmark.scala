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

  // Do NOT access the inherited `spark` session - it uses default serializer
  // Instead, create fresh sessions for each benchmark

  // Create separate sessions for each cache format since SPARK_CACHE_SERIALIZER is static
  // CRITICAL: Can only have one active SparkContext at a time
  private def createFreshSession(serializer: String): SparkSession = {
    // Stop any existing session and clear the registry
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    // CRITICAL: Clear the cached serializer instance in InMemoryRelation
    // This singleton is stored statically and persists across sessions
    org.apache.spark.sql.execution.columnar.InMemoryRelation.clearSerializer()

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

      // Run Default cache benchmark
      benchmark.addCase("Default cache - write + read") { _ =>
        val spark = createFreshSession(
          "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer")
        try {
          val df = spark.range(numRows).selectExpr(
            "id as int_col",
            "id * 2L as long_col",
            "cast(id as double) as double_col"
          )
          df.cache()
          df.count()
          df.unpersist(blocking = true)
        } finally {
          spark.stop()
        }
      }

      // Run Arrow cache benchmark
      benchmark.addCase("Arrow cache - write + read") { _ =>
        val spark = createFreshSession(classOf[ArrowCachedBatchSerializer].getName)
        try {
          val df = spark.range(numRows).selectExpr(
            "id as int_col",
            "id * 2L as long_col",
            "cast(id as double) as double_col"
          )
          df.cache()
          df.count()
          df.unpersist(blocking = true)
        } finally {
          spark.stop()
        }
      }

      benchmark.run()
    }
  }

  private def cacheWithFilters(): Unit = {
    val numRows = 5000000 // 5M rows
    runBenchmark("Cache with filter pushdown") {
      val benchmark = new Benchmark("Cache 5M rows + filter", numRows, output = output)

      // Default cache filter benchmark
      benchmark.addCase("Default cache - filter") { _ =>
        val spark = createFreshSession(
          "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer")
        try {
          val df = spark.range(numRows).selectExpr(
            "id as int_col",
            "cast(id as double) as double_col"
          )
          df.cache()
          df.count() // Materialize cache
          df.filter("int_col > 2500000").count()
          df.unpersist(blocking = true)
        } finally {
          spark.stop()
        }
      }

      // Arrow cache filter benchmark
      benchmark.addCase("Arrow cache - filter (with stats)") { _ =>
        val spark = createFreshSession(classOf[ArrowCachedBatchSerializer].getName)
        try {
          val df = spark.range(numRows).selectExpr(
            "id as int_col",
            "cast(id as double) as double_col"
          )
          df.cache()
          df.count() // Materialize cache
          df.filter("int_col > 2500000").count()
          df.unpersist(blocking = true)
        } finally {
          spark.stop()
        }
      }

      benchmark.run()
    }
  }

  private def cacheColumnarInput(): Unit = {
    val numRows = 2000000 // 2M rows
    withTempPath { dir =>
      val path = dir.getAbsolutePath

      // Write parquet file using a temporary session
      val tempSpark = createFreshSession(
        "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer")
      try {
        tempSpark.range(numRows).selectExpr(
          "id as int_col",
          "id * 2L as long_col",
          "cast(id as double) as double_col"
        ).write.parquet(path)
      } finally {
        tempSpark.stop()
      }

      runBenchmark("Cache columnar input (Parquet)") {
        val benchmark = new Benchmark("Cache 2M rows from Parquet", numRows, output = output)

        benchmark.addCase("Default cache - columnar input") { _ =>
          val spark = createFreshSession(
            "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer")
          try {
            val parquet = spark.read.parquet(path)
            parquet.cache()
            parquet.count()
            parquet.unpersist(blocking = true)
          } finally {
            spark.stop()
          }
        }

        benchmark.addCase("Arrow cache - columnar input") { _ =>
          val spark = createFreshSession(classOf[ArrowCachedBatchSerializer].getName)
          try {
            val parquet = spark.read.parquet(path)
            parquet.cache()
            parquet.count()
            parquet.unpersist(blocking = true)
          } finally {
            spark.stop()
          }
        }

        benchmark.run()
      }
    }
  }

  private def recacheArrowData(): Unit = {
    val numRows = 2000000 // 2M rows
    runBenchmark("Re-cache Arrow cached data (zero-copy test)") {
      val benchmark = new Benchmark("Re-cache 2M rows (zero-copy)", numRows, output = output)

      benchmark.addCase("Default cache - cache a cached DF") { _ =>
        val spark = createFreshSession(
          "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer")
        try {
          // Create and cache initial data
          val df = spark.range(numRows).selectExpr(
            "id as int_col",
            "id * 2L as long_col",
            "cast(id as double) as double_col"
          )
          df.cache()
          df.count() // Materialize cache

          // Cache the cached DataFrame again
          // Drop a column to create a different logical plan
          val df2 = df.drop("double_col")
          df2.cache()
          df2.count()
          df2.unpersist(blocking = true)
          df.unpersist(blocking = true)
        } finally {
          spark.stop()
        }
      }

      benchmark.addCase("Arrow cache - cache a cached DF (zero-copy)") { _ =>
        val spark = createFreshSession(classOf[ArrowCachedBatchSerializer].getName)
        try {
          // Create and cache initial data
          val df = spark.range(numRows).selectExpr(
            "id as int_col",
            "id * 2L as long_col",
            "cast(id as double) as double_col"
          )
          df.cache()
          df.count() // Materialize cache

          // Cache the cached DataFrame again
          // Drop a column to create a different logical plan
          // This preserves ArrowColumnVector for remaining columns, enabling zero-copy
          val df2 = df.drop("double_col")
          df2.cache()
          df2.count()
          df2.unpersist(blocking = true)
          df.unpersist(blocking = true)
        } finally {
          spark.stop()
        }
      }

      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Arrow Cache vs Default Cache") {
      cachePrimitiveTypes()
      cacheWithFilters()
      cacheColumnarInput()
      recacheArrowData()
    }
  }
}
