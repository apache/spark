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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark to measure like any expressions performance.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *     --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/LikeAnyBenchmark-results.txt".
 * }}}
 */
object LikeAnyBenchmark extends SqlBasedBenchmark {
  import spark.implicits._

  private val numRows = 50000
  private val width = 5

  def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally tableNames.foreach(spark.catalog.dropTempView)
  }

  private def saveAsTable(df: DataFrame, dir: File): Unit = {
    val parquetPath = dir.getCanonicalPath + "/parquet"
    df.write.mode("overwrite").parquet(parquetPath)
    spark.read.parquet(parquetPath).createOrReplaceTempView("parquetTable")
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    withTempPath { dir =>
      withTempTable("parquetTable") {
        val selectExpr = (1 to width).map(i => s"CAST(value + 1000000 AS STRING) c$i")
        val df = spark.range(0, numRows, 1, 100)
          .map(_ => Random.nextLong).selectExpr(selectExpr: _*)
        saveAsTable(df, dir)

        val benchmark =
          new Benchmark("Multi like query", numRows, minNumIters = 3, output = output)
        val multiLikeExpr =
          Range(1000, 1200).map(i => s"c1 like '%$i%'").mkString(" or ")
        benchmark.addCase("Query with multi like", numIters = 3) { _ =>
          spark.sql(s"SELECT * FROM parquetTable WHERE $multiLikeExpr").noop()
        }

        val likeAnyExpr =
          Range(1000, 1200).map(i => s"'%$i%'").mkString("c1 like any(", ", ", ")")
        benchmark.addCase("Query with LikeAny simplification", numIters = 3) { _ =>
          spark.sql(s"SELECT * FROM parquetTable WHERE $likeAnyExpr").noop()
        }

        benchmark.addCase("Query without LikeAny simplification", numIters = 3) { _ =>
          withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
            "org.apache.spark.sql.catalyst.optimizer.LikeSimplification") {
            spark.sql(s"SELECT * FROM parquetTable WHERE $likeAnyExpr").noop()
          }
        }
        benchmark.run()
      }
    }
  }
}
