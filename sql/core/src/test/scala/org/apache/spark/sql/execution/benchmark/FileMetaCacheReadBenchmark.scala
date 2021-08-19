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

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark to measure the performance of fileMetaCache in data source read.
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/FileMetaCacheReadBenchmark-results.txt".
 * }}}
 */
object FileMetaCacheReadBenchmark extends SqlBasedBenchmark {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("FileMetaCacheReadBenchmark")
      // Since `spark.master` always exists, overrides this value
      .set("spark.master", "local[1]")
      .setIfMissing("spark.driver.memory", "4g")
      .setIfMissing("spark.executor.memory", "4g")

    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    // Set default configs. Individual cases will change them if necessary.
    sparkSession.conf.set(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.FILE_META_CACHE_TTL_SINCE_LAST_ACCESS_SEC.key, "5")

    sparkSession
  }

  def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally tableNames.foreach(spark.catalog.dropTempView)
  }

  private def prepareTable(dir: File, df: DataFrame, fileCount: Int): Unit =
    saveAsOrcTable( df.repartition(fileCount).write, dir.getCanonicalPath + "/orc")

  private def saveAsOrcTable(df: DataFrameWriter[Row], dir: String): Unit = {
    df.mode("overwrite").option("compression", "snappy").orc(dir)
    spark.read.orc(dir).createOrReplaceTempView("orcTable")
  }

  def countBenchmark(values: Int, width: Int, fileCount: Int): Unit = {
    val benchmark = new Benchmark(
      s"count(*) from $width columns with $fileCount files",
      values,
      output = output)

    withTempPath { dir =>
      withTempTable("t1", "orcTable") {
        import spark.implicits._
        val selectExpr = (1 to width).map(i => s"value as c$i")
        val dataFrame = spark.range(values).map(_ => Random.nextLong).toDF()
        dataFrame.selectExpr(selectExpr: _*).createOrReplaceTempView("t1")

        prepareTable(dir, spark.sql("SELECT * FROM t1"), fileCount)

        val filter = {
          val rows =
            spark.sql(s"SELECT c1, count(*) FROM orcTable group by c1")
              .collect()
              .sortBy(r => r.getLong(1))
          rows.head.getLong(0)
        }

        benchmark.addCase("count(*): fileMetaCacheEnabled = false") { _ =>
          spark.sql(s"SELECT count(*) FROM orcTable").noop()
        }

        benchmark.addCase("count(*): fileMetaCacheEnabled = true") { _ =>
          withSQLConf(SQLConf.FILE_META_CACHE_ENABLED_SOURCE_LIST.key -> "orc") {
            spark.sql(s"SELECT count(*) FROM orcTable").noop()
          }
        }

        benchmark.addCase("count(*) with Filter: fileMetaCacheEnabled = false") { _ =>
          spark.sql(s"SELECT count(*) FROM orcTable where c1 = $filter").noop()
        }

        benchmark.addCase("count(*) with Filter: fileMetaCacheEnabled = true") { _ =>
          withSQLConf(SQLConf.FILE_META_CACHE_ENABLED_SOURCE_LIST.key -> "orc") {
            spark.sql(s"SELECT count(*) FROM orcTable where c1 = $filter").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    for (fileCount <- List(100, 500, 1000)) {
      runBenchmark(s"count(*) From $fileCount files") {
        for (columnWidth <- List(10, 50, 100)) {
          countBenchmark(1024 * 1024 * 5, columnWidth, fileCount)
        }
      }
    }
  }
}
