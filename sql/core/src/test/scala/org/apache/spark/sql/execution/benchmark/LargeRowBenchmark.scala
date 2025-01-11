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

/**
 * Benchmark to measure performance for wide table.
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/WideTableBenchmark-results.txt".
 * }}}
 */
object LargeRowBenchmark extends SqlBasedBenchmark {

  def runLargeRowBenchmark(rowsNum: Int, numCols: Int, cellSize_mb: Double): Unit = {
    withTempPath { path =>
      val benchmark = new Benchmark(
        s"#rows: $rowsNum, #cols: $numCols, cell: $cellSize_mb MB", rowsNum, output = output)
      writeLargeRow(path.getAbsolutePath, rowsNum, numCols, cellSize_mb)
      val df = spark.read.parquet(path.getAbsolutePath)
      df.createOrReplaceTempView("T")
      benchmark.addCase("built-in UPPER") { _ =>
        val sql_select = df.columns.map(c => s"UPPER($c) as $c").mkString(", ")
        spark.sql(s"SELECT $sql_select FROM T").noop()
      }
      benchmark.addCase("udf UPPER") { _ =>
        val sql_select = df.columns.map(c => s"udf_upper($c) as $c").mkString(", ")
        spark.sql(s"SELECT $sql_select FROM T").noop()
      }
      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Large Row Benchmark") {
      val udf_upper = (s: String) => s.toUpperCase()
      spark.udf.register("udf_upper", udf_upper(_: String): String)

      val benchmarks = Array(
        Map("rows" -> 100, "cols" -> 10, "cellSize_mb" -> 1.6) //  OutOfMemory @ 100, 10, 2
        //        Map("rows" -> 1, "cols" -> 1, "cellSize_mb" -> 300), //  OutOfMemory @ 1, 1, 400
        //        Map("rows" -> 1, "cols" -> 200, "cellSize_mb" -> 1) //  OutOfMemory @ 1, 300, 1
      )

      benchmarks.foreach { b =>
        val rows = b("rows").asInstanceOf[Int]
        val cols = b("cols").asInstanceOf[Int]
        val cellSize_mb = b("cellSize_mb")
        runLargeRowBenchmark(rows, cols, cellSize_mb)
        runLargeRowBenchmark(rows, cols, cellSize_mb)
      }
    }
  }
}
