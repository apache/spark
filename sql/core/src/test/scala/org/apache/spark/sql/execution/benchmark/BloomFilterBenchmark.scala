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

import java.util.UUID

import scala.util.Random

import org.apache.parquet.hadoop.{ParquetInputFormat, ParquetOutputFormat}

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.functions.col

/**
 * Benchmark to measure read performance with Bloom filters.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *     --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/BloomFilterBenchmark-results.txt".
 * }}}
 */
object BloomFilterBenchmark extends SqlBasedBenchmark {
  import spark.implicits._

  private val scaleFactor = 100
  private val N = scaleFactor * 1000 * 1000
  private val df1 = spark.range(N).map(_ => Random.nextInt)

  private val df2 = Seq.fill(N) {UUID.randomUUID().toString.replace("-", "")}.toDF

  private def writeORCBenchmark(): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      runBenchmark(s"ORC Write") {
        val benchmark = new Benchmark(s"Write ${scaleFactor}M rows", N, output = output)
        benchmark.addCase("Without bloom filter") { _ =>
          df1.write.mode("overwrite").orc(path + "/withoutBF")
        }
        benchmark.addCase("With bloom filter") { _ =>
          df1.write.mode("overwrite")
            .option("orc.bloom.filter.columns", "value").orc(path + "/withBF")
        }
        benchmark.run()
      }
    }
  }

  private def readORCBenchmark(): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      df1.write.orc(path + "/withoutBF")
      df1.write.option("orc.bloom.filter.columns", "value").orc(path + "/withBF")

      runBenchmark(s"ORC Read") {
        val benchmark = new Benchmark(s"Read a row from ${scaleFactor}M rows", N, output = output)
        benchmark.addCase("Without bloom filter") { _ =>
          spark.read.orc(path + "/withoutBF").where("value = 0").noop()
        }
        benchmark.addCase("With bloom filter") { _ =>
          spark.read.orc(path + "/withBF").where("value = 0").noop()
        }
        benchmark.run()
      }
    }
  }

  private def readORCBenchmarkForInSet(): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val samples = df2.sample(0.000003, 128).select("value").as[String].collect()
      val filter = "value IN (" + samples.map ( x => s"'$x'").mkString(", ") + ")"

      df2.repartition(col("value")).sort(col("value")).write.orc(path + "/withoutBF")
      df2.repartition(col("value")).sort(col("value"))
        .write.option("orc.bloom.filter.columns", "value").orc(path + "/withBF")

      runBenchmark(s"ORC Read for IN set") {
        val benchmark = new Benchmark(s"Read a row from ${scaleFactor}M rows", N, output = output)
        benchmark.addCase("Without bloom filter") { _ =>
          spark.read.orc(path + "/withoutBF").where(filter).noop()
        }
        benchmark.addCase("With bloom filter") { _ =>
          spark.read.orc(path + "/withBF").where(filter).noop()
        }
        benchmark.run()
      }
    }
  }

  private def writeParquetBenchmark(): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      runBenchmark(s"Parquet Write") {
        val benchmark = new Benchmark(s"Write ${scaleFactor}M rows", N, output = output)
        benchmark.addCase("Without bloom filter") { _ =>
          df1.write.mode("overwrite").parquet(path + "/withoutBF")
        }
        benchmark.addCase("With bloom filter") { _ =>
          df1.write.mode("overwrite")
            .option(ParquetOutputFormat.BLOOM_FILTER_ENABLED + "#value", true)
            .option("parquet.bloom.filter.expected.ndv#value", "100000000")
            .parquet(path + "/withBF")
        }
        benchmark.run()
      }
    }
  }

  private def readParquetBenchmark(): Unit = {
    val blockSizes = Seq(2 * 1024 * 1024, 3 * 1024 * 1024, 4 * 1024 * 1024, 5 * 1024 * 1024,
      6 * 1024 * 1024, 8 * 1024 * 1024, 16 * 1024 * 1024, 32 * 1024 * 1024, 64 * 1024 * 1024,
      128 * 1024 * 1024)
    for (blocksize <- blockSizes) {
      withTempPath { dir =>
        val path = dir.getCanonicalPath

        df1.write.option("parquet.block.size", blocksize).parquet(path + "/withoutBF")
        df1.write.option(ParquetOutputFormat.BLOOM_FILTER_ENABLED + "#value", true)
          .option("parquet.bloom.filter.expected.ndv#value", "100000000")
          .option("parquet.block.size", blocksize)
          .parquet(path + "/withBF")

        runBenchmark(s"Parquet Read") {
          val benchmark = new Benchmark(s"Read a row from ${scaleFactor}M rows", N, output = output)
          benchmark.addCase("Without bloom filter, blocksize: " + blocksize) { _ =>
            spark.read.parquet(path + "/withoutBF").where("value = 0").noop()
          }
          benchmark.addCase("With bloom filter, blocksize: " + blocksize) { _ =>
            spark.read.option(ParquetInputFormat.BLOOM_FILTERING_ENABLED, true)
              .parquet(path + "/withBF").where("value = 0").noop()
          }
          benchmark.run()
        }
      }
    }
  }

  private def readParquetBenchmarkForInSet(): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val samples = df2.sample(0.000003, 128).select("value").as[String].collect()
      val filter = "value IN (" + samples.map ( x => s"'$x'").mkString(", ") + ")"

      df2.repartition(col("value")).sort(col("value")).write.parquet(path + "/withoutBF")
      df2.repartition(col("value")).sort(col("value"))
        .write.option(ParquetOutputFormat.BLOOM_FILTER_ENABLED + "#value", true)
        .option("parquet.bloom.filter.expected.ndv#value", "100000000")
        .parquet(path + "/withBF")

      runBenchmark(s"Parquet Read for IN set") {
        val benchmark = new Benchmark(s"Read a row from ${scaleFactor}M rows", N, output = output)
        benchmark.addCase("Without bloom filter") { _ =>
          spark.read.option("spark.sql.parquet.pushdown.inFilterThreshold", 320)
            .parquet(path + "/withoutBF").where(filter).noop()
        }
        benchmark.addCase("With bloom filter") { _ =>
          spark.read.option(ParquetInputFormat.BLOOM_FILTERING_ENABLED, true)
            .option("spark.sql.parquet.pushdown.inFilterThreshold", 3100)
            .parquet(path + "/withBF").where(filter).noop
        }
        benchmark.run()
      }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    writeORCBenchmark()
    readORCBenchmark()
    readORCBenchmarkForInSet()
    writeParquetBenchmark()
    readParquetBenchmark()
    readParquetBenchmarkForInSet()
  }
}
