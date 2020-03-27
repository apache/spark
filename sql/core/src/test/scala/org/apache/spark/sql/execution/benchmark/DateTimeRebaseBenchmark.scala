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

import java.time.{LocalDateTime, ZoneOffset}

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.internal.SQLConf

/**
 * Synthetic benchmark for rebasing of date and timestamp in read/write.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/DateTimeRebaseBenchmark-results.txt".
 * }}}
 */
object DateTimeRebaseBenchmark extends SqlBasedBenchmark {
  import spark.implicits._

  private def genTs(cardinality: Int, start: LocalDateTime, end: LocalDateTime): DataFrame = {
    val startSec = start.toEpochSecond(ZoneOffset.UTC)
    val endSec = end.toEpochSecond(ZoneOffset.UTC)
    spark.range(0, cardinality, 1, 1)
      .select((($"id" % (endSec - startSec)) + startSec).as("seconds"))
      .select($"seconds".cast("timestamp").as("ts"))
  }

  private def genTsAfter1582(cardinality: Int): DataFrame = {
    val start = LocalDateTime.of(1582, 10, 15, 0, 0, 0)
    val end = LocalDateTime.of(3000, 1, 1, 0, 0, 0)
    genTs(cardinality, start, end)
  }

  private def genTsBefore1582(cardinality: Int): DataFrame = {
    val start = LocalDateTime.of(10, 1, 1, 0, 0, 0)
    val end = LocalDateTime.of(1580, 1, 1, 0, 0, 0)
    genTs(cardinality, start, end)

  }

  private def save(df: DataFrame, path: String, format: String = "parquet"): Unit = {
    df.write.mode("overwrite").format(format).save(path)
  }

  private def load(path: String, format: String = "parquet"): Unit = {
    spark.read.format(format).load(path).noop()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    withTempPath { path =>
      runBenchmark("Parquet read/write") {
        val rowsNum = 100000000
        var numIters = 1
        var benchmark = new Benchmark("Save timestamps to parquet", rowsNum, output = output)
        benchmark.addCase("after 1582, noop", numIters) { _ =>
          genTsAfter1582(rowsNum).noop()
        }
        val ts_after_1582_off = path.getAbsolutePath + "/ts_after_1582_off"
        benchmark.addCase("after 1582, rebase off", numIters) { _ =>
          withSQLConf(SQLConf.LEGACY_PARQUET_REBASE_DATETIME.key -> "false") {
            save(genTsAfter1582(rowsNum), ts_after_1582_off)
          }
        }
        val ts_after_1582_on = path.getAbsolutePath + "/ts_after_1582_on"
        benchmark.addCase("after 1582, rebase on", numIters) { _ =>
          withSQLConf(SQLConf.LEGACY_PARQUET_REBASE_DATETIME.key -> "true") {
            save(genTsAfter1582(rowsNum), ts_after_1582_on)
          }
        }
        benchmark.addCase("before 1582, noop", numIters) { _ =>
          genTsBefore1582(rowsNum).noop()
        }
        val ts_before_1582_off = path.getAbsolutePath + "/ts_before_1582_off"
        benchmark.addCase("before 1582, rebase off", numIters) { _ =>
          withSQLConf(SQLConf.LEGACY_PARQUET_REBASE_DATETIME.key -> "false") {
            save(genTsBefore1582(rowsNum), ts_before_1582_off)
          }
        }
        val ts_before_1582_on = path.getAbsolutePath + "/ts_before_1582_on"
        benchmark.addCase("before 1582, rebase on", numIters) { _ =>
          withSQLConf(SQLConf.LEGACY_PARQUET_REBASE_DATETIME.key -> "true") {
            save(genTsBefore1582(rowsNum), ts_before_1582_on)
          }
        }
        benchmark.run()

        numIters = 3
        benchmark = new Benchmark("Load timestamps from parquet", rowsNum, output = output)
        benchmark.addCase("after 1582, vec off, rebase off", numIters) { _ =>
          withSQLConf(
            SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false",
            SQLConf.LEGACY_PARQUET_REBASE_DATETIME.key -> "false") {
            load(ts_after_1582_off)
          }
        }
        benchmark.addCase("after 1582, vec off, rebase on", numIters) { _ =>
          withSQLConf(
            SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false",
            SQLConf.LEGACY_PARQUET_REBASE_DATETIME.key -> "true") {
            load(ts_after_1582_on)
          }
        }
        benchmark.addCase("after 1582, vec on, rebase off", numIters) { _ =>
          withSQLConf(
            SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
            SQLConf.LEGACY_PARQUET_REBASE_DATETIME.key -> "false") {
            load(ts_after_1582_off)
          }
        }
        benchmark.addCase("after 1582, vec on, rebase on", numIters) { _ =>
          withSQLConf(
            SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
            SQLConf.LEGACY_PARQUET_REBASE_DATETIME.key -> "true") {
            load(ts_after_1582_on)
          }
        }

        benchmark.addCase("before 1582, vec off, rebase off", numIters) { _ =>
          withSQLConf(
            SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false",
            SQLConf.LEGACY_PARQUET_REBASE_DATETIME.key -> "false") {
            load(ts_before_1582_off)
          }
        }
        benchmark.addCase("before 1582, vec off, rebase on", numIters) { _ =>
          withSQLConf(
            SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false",
            SQLConf.LEGACY_PARQUET_REBASE_DATETIME.key -> "true") {
            load(ts_before_1582_on)
          }
        }
        benchmark.addCase("before 1582, vec on, rebase off", numIters) { _ =>
          withSQLConf(
            SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
            SQLConf.LEGACY_PARQUET_REBASE_DATETIME.key -> "false") {
            load(ts_before_1582_off)
          }
        }
        benchmark.addCase("after 1582, vec on, rebase on", numIters) { _ =>
          withSQLConf(
            SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
            SQLConf.LEGACY_PARQUET_REBASE_DATETIME.key -> "true") {
            load(ts_before_1582_on)
          }
        }

        benchmark.run()
      }
    }
  }
}
