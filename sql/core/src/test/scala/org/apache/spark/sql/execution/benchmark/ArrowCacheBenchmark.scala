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

  override def getSparkSession: SparkSession = {
    // Use default cache serializer for this benchmark
    // We'll switch between serializers within individual benchmarks
    super.getSparkSession
  }

  private def cachePrimitiveTypes(): Unit = {
    val numRows = 10000000 // 10M rows
    runBenchmark("Cache primitive types") {
      val benchmark = new Benchmark("Cache 10M rows with primitives", numRows, output = output)

      val df = spark.range(numRows).selectExpr(
        "id as int_col",
        "id * 2L as long_col",
        "cast(id as double) as double_col",
        "cast(id % 2 = 0 as boolean) as bool_col"
      )

      benchmark.addCase("Default cache - write") { _ =>
        withSQLConf(
          StaticSQLConf.SPARK_CACHE_SERIALIZER.key ->
            "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer") {
          df.cache()
          df.count()
          df.unpersist(blocking = true)
        }
      }

      benchmark.addCase("Arrow cache - write") { _ =>
        withSQLConf(
          StaticSQLConf.SPARK_CACHE_SERIALIZER.key ->
            classOf[ArrowCachedBatchSerializer].getName) {
          df.cache()
          df.count()
          df.unpersist(blocking = true)
        }
      }

      // Now test read performance with cached data
      val cachedDfDefault = withSQLConf(
        StaticSQLConf.SPARK_CACHE_SERIALIZER.key ->
          "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer") {
        val cached = df.cache()
        cached.count() // Materialize cache
        cached
      }

      benchmark.addCase("Default cache - read") { _ =>
        cachedDfDefault.count()
      }

      val cachedDfArrow = withSQLConf(
        StaticSQLConf.SPARK_CACHE_SERIALIZER.key ->
          classOf[ArrowCachedBatchSerializer].getName) {
        cachedDfDefault.unpersist(blocking = true)
        val cached = df.cache()
        cached.count() // Materialize cache
        cached
      }

      benchmark.addCase("Arrow cache - read") { _ =>
        cachedDfArrow.count()
      }

      cachedDfArrow.unpersist(blocking = true)

      benchmark.run()
    }
  }

  private def cacheStringTypes(): Unit = {
    val numRows = 5000000 // 5M rows
    runBenchmark("Cache string types") {
      val benchmark = new Benchmark("Cache 5M rows with strings", numRows, output = output)

      val df = spark.range(numRows).selectExpr(
        "concat('string_', id) as str_col",
        "concat('long_string_value_', id, '_more_data') as long_str_col"
      )

      benchmark.addCase("Default cache - write") { _ =>
        withSQLConf(
          StaticSQLConf.SPARK_CACHE_SERIALIZER.key ->
            "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer") {
          df.cache()
          df.count()
          df.unpersist(blocking = true)
        }
      }

      benchmark.addCase("Arrow cache - write") { _ =>
        withSQLConf(
          StaticSQLConf.SPARK_CACHE_SERIALIZER.key ->
            classOf[ArrowCachedBatchSerializer].getName) {
          df.cache()
          df.count()
          df.unpersist(blocking = true)
        }
      }

      benchmark.run()
    }
  }

  private def cacheComplexTypes(): Unit = {
    val numRows = 1000000 // 1M rows
    runBenchmark("Cache complex types") {
      val benchmark = new Benchmark("Cache 1M rows with complex types", numRows, output = output)

      val df = spark.range(numRows).selectExpr(
        "array(id, id + 1, id + 2) as array_col",
        "struct(id as a, id * 2 as b) as struct_col",
        "map('key1', id, 'key2', id * 2) as map_col"
      )

      benchmark.addCase("Default cache - write") { _ =>
        withSQLConf(
          StaticSQLConf.SPARK_CACHE_SERIALIZER.key ->
            "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer") {
          df.cache()
          df.count()
          df.unpersist(blocking = true)
        }
      }

      benchmark.addCase("Arrow cache - write") { _ =>
        withSQLConf(
          StaticSQLConf.SPARK_CACHE_SERIALIZER.key ->
            classOf[ArrowCachedBatchSerializer].getName) {
          df.cache()
          df.count()
          df.unpersist(blocking = true)
        }
      }

      benchmark.run()
    }
  }

  private def cacheColumnarInput(): Unit = {
    val numRows = 5000000 // 5M rows
    withTempPath { dir =>
      val path = dir.getAbsolutePath

      // Write parquet file (columnar format)
      spark.range(numRows).selectExpr(
        "id as int_col",
        "id * 2L as long_col",
        "cast(id as double) as double_col",
        "concat('str_', id) as str_col"
      ).write.parquet(path)

      runBenchmark("Cache columnar input (Parquet)") {
        val benchmark = new Benchmark(
          "Cache 5M rows from Parquet", numRows, output = output)

        val parquetDf = spark.read.parquet(path)

        benchmark.addCase("Default cache - columnar input") { _ =>
          withSQLConf(
            StaticSQLConf.SPARK_CACHE_SERIALIZER.key ->
              "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer") {
            parquetDf.cache()
            parquetDf.count()
            parquetDf.unpersist(blocking = true)
          }
        }

        benchmark.addCase("Arrow cache - columnar input (zero-copy)") { _ =>
          withSQLConf(
            StaticSQLConf.SPARK_CACHE_SERIALIZER.key ->
              classOf[ArrowCachedBatchSerializer].getName) {
            parquetDf.cache()
            parquetDf.count()
            parquetDf.unpersist(blocking = true)
          }
        }

        benchmark.run()
      }
    }
  }

  private def cacheWithFilters(): Unit = {
    val numRows = 10000000 // 10M rows
    runBenchmark("Cache with filter pushdown") {
      val benchmark = new Benchmark(
        "Cache 10M rows + filter", numRows, output = output)

      val df = spark.range(numRows).selectExpr(
        "id as int_col",
        "cast(id as double) as double_col",
        "concat('str_', id) as str_col"
      )

      // Cache with default serializer
      val cachedDfDefault = withSQLConf(
        StaticSQLConf.SPARK_CACHE_SERIALIZER.key ->
          "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer") {
        val cached = df.cache()
        cached.count() // Materialize cache
        cached
      }

      benchmark.addCase("Default cache - filter") { _ =>
        cachedDfDefault.filter("int_col > 5000000").count()
      }

      // Cache with Arrow serializer
      val cachedDfArrow = withSQLConf(
        StaticSQLConf.SPARK_CACHE_SERIALIZER.key ->
          classOf[ArrowCachedBatchSerializer].getName) {
        cachedDfDefault.unpersist(blocking = true)
        val cached = df.cache()
        cached.count() // Materialize cache
        cached
      }

      benchmark.addCase("Arrow cache - filter (with stats)") { _ =>
        cachedDfArrow.filter("int_col > 5000000").count()
      }

      cachedDfArrow.unpersist(blocking = true)

      benchmark.run()
    }
  }

  private def cacheWithCompression(): Unit = {
    val numRows = 5000000 // 5M rows
    runBenchmark("Cache with compression") {
      val benchmark = new Benchmark(
        "Cache 5M rows with compression", numRows, output = output)

      val df = spark.range(numRows).selectExpr(
        "id as int_col",
        "id * 2L as long_col",
        "cast(id as double) as double_col",
        "concat('string_value_', id) as str_col"
      )

      benchmark.addCase("Arrow cache - no compression") { _ =>
        withSQLConf(
          StaticSQLConf.SPARK_CACHE_SERIALIZER.key ->
            classOf[ArrowCachedBatchSerializer].getName,
          SQLConf.ARROW_EXECUTION_COMPRESSION_CODEC.key -> "none") {
          df.cache()
          df.count()
          df.unpersist(blocking = true)
        }
      }

      benchmark.addCase("Arrow cache - zstd compression") { _ =>
        withSQLConf(
          StaticSQLConf.SPARK_CACHE_SERIALIZER.key ->
            classOf[ArrowCachedBatchSerializer].getName,
          SQLConf.ARROW_EXECUTION_COMPRESSION_CODEC.key -> "zstd") {
          df.cache()
          df.count()
          df.unpersist(blocking = true)
        }
      }

      benchmark.addCase("Arrow cache - lz4 compression") { _ =>
        withSQLConf(
          StaticSQLConf.SPARK_CACHE_SERIALIZER.key ->
            classOf[ArrowCachedBatchSerializer].getName,
          SQLConf.ARROW_EXECUTION_COMPRESSION_CODEC.key -> "lz4") {
          df.cache()
          df.count()
          df.unpersist(blocking = true)
        }
      }

      benchmark.run()
    }
  }

  private def cacheWithVectorizedReader(): Unit = {
    val numRows = 5000000 // 5M rows
    runBenchmark("Cache with vectorized reader") {
      val benchmark = new Benchmark(
        "Cache 5M rows - vectorized read", numRows, output = output)

      val df = spark.range(numRows).selectExpr(
        "id as int_col",
        "id * 2L as long_col",
        "cast(id as double) as double_col"
      )

      // Cache with Arrow serializer + vectorized reader off
      val cachedDfArrowNoVec = withSQLConf(
        StaticSQLConf.SPARK_CACHE_SERIALIZER.key ->
          classOf[ArrowCachedBatchSerializer].getName,
        SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "false") {
        val cached = df.cache()
        cached.count() // Materialize cache
        cached
      }

      benchmark.addCase("Arrow cache - vectorized off") { _ =>
        cachedDfArrowNoVec.count()
      }

      // Cache with Arrow serializer + vectorized reader on
      val cachedDfArrowVec = withSQLConf(
        StaticSQLConf.SPARK_CACHE_SERIALIZER.key ->
          classOf[ArrowCachedBatchSerializer].getName,
        SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true") {
        cachedDfArrowNoVec.unpersist(blocking = true)
        val cached = df.cache()
        cached.count() // Materialize cache
        cached
      }

      benchmark.addCase("Arrow cache - vectorized on") { _ =>
        cachedDfArrowVec.count()
      }

      cachedDfArrowVec.unpersist(blocking = true)

      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Arrow Cache vs Default Cache") {
      cachePrimitiveTypes()
      cacheStringTypes()
      cacheComplexTypes()
      cacheColumnarInput()
      cacheWithFilters()
      cacheWithCompression()
      cacheWithVectorizedReader()
    }
  }
}
