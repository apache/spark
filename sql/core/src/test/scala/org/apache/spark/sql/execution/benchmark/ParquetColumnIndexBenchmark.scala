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

import java.io.File

import scala.util.Random

import org.apache.parquet.hadoop.ParquetInputFormat

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Benchmark to measure read performance with Parquet column index.
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/ParquetFilterPushdownBenchmark-results.txt".
 * }}}
 */
object ParquetColumnIndexBenchmark extends SqlBasedBenchmark {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      // Since `spark.master` always exists, overrides this value
      .set("spark.master", "local[1]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")
      .setIfMissing("orc.compression", "snappy")
      .setIfMissing("spark.sql.parquet.compression.codec", "snappy")

    SparkSession.builder().config(conf).getOrCreate()
  }

  private val numRows = 1024 * 1024 * 15
  private val width = 5
  private val mid = numRows / 2

  def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally tableNames.foreach(spark.catalog.dropTempView)
  }

  private def prepareTable(
                            dir: File, numRows: Int): Unit = {
    import spark.implicits._

    val df = spark.range(numRows).map(i => (i, i + ":f" + "o" * Random.nextInt(200))).toDF()

    saveAsTable(df, dir)
  }

  private def saveAsTable(df: DataFrame, dir: File, useDictionary: Boolean = false): Unit = {
    val parquetPath = dir.getCanonicalPath + "/parquet"
    df.write.mode("overwrite").parquet(parquetPath)
    spark.read.parquet(parquetPath).createOrReplaceTempView("parquetTable")
  }

  def filterPushDownBenchmark(
                               values: Int,
                               title: String,
                               whereExpr: String,
                               selectExpr: String = "*"): Unit = {
    val benchmark = new Benchmark(title, values, minNumIters = 5, output = output)

    Seq(false, true).foreach { columnIndexEnabled =>
      val name = s"Parquet Vectorized ${if (columnIndexEnabled) s"(columnIndex)" else ""}"
      benchmark.addCase(name) { _ =>
        withSQLConf(ParquetInputFormat.COLUMN_INDEX_FILTERING_ENABLED -> s"$columnIndexEnabled") {
          spark.sql(s"SELECT $selectExpr FROM parquetTable WHERE $whereExpr").noop()
        }
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Pushdown for single value filter") {
      withTempPath { dir =>
        withTempTable("parquetTable") {
          prepareTable(dir, numRows)
          filterPushDownBenchmark(numRows, "simple filters", s" _1 = $numRows - 100 ")
        }
      }
    }

    runBenchmark("Pushdown for range filter") {
      withTempPath { dir =>
        withTempTable("parquetTable") {
          prepareTable(dir, numRows)
          filterPushDownBenchmark(numRows,
            "range filters", s" _1 > ($numRows - 1000000) and _1 < ($numRows - 1000)")
        }
      }
    }

    runBenchmark("Pushdown for multi range filter") {
      withTempPath { dir =>
        withTempTable("parquetTable") {
          prepareTable(dir, numRows)
          filterPushDownBenchmark(numRows,
            "multi range filters",
            s" (_1 > ($numRows - 3000000) and _1 < ($numRows - 2000000))" +
              s" or ( _1 > ($numRows - 1000000) and _1 < ($numRows - 1000))")
        }
      }
    }

  }
}
