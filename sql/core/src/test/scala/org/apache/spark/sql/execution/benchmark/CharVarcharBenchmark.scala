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
 * Benchmark for measure writing and reading char/varchar values with implicit length check
 * and padding.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/CharVarcharBenchmark-results.txt".
 * }}}
 */
object CharVarcharBenchmark extends SqlBasedBenchmark {
  import spark.implicits._

  private def withTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }

  private def createTable(tblName: String, colType: String, path: String): Unit = {
    spark.sql(s"CREATE TABLE $tblName (c $colType) USING PARQUET LOCATION '$path'")
  }

  def writeBenchmark(card: Long, length: Int, hasSpaces: Boolean): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val benchmark =
        new Benchmark(s"Write with length $length", card, output = output)
      Seq("string", "char", "varchar").foreach { typ =>
        val colType = if (typ == "string") typ else s"$typ($length)"
        val tblName = s"${typ}_${length}_$card"

        benchmark.addCase(s"write $typ with length $length", 3) { _ =>
          withTable(tblName) {
            createTable(tblName, colType, path)
            spark.range(card).map { _ =>
              if (hasSpaces) {
                "st" + " " * length
              } else {
                "st"
              }
            }.write.insertInto(tblName)
          }
        }
      }
      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val N = 200L * 1000 * 1000
    val range = Range(20, 101, 20)

    runBenchmark("Char Varchar Write Side Perf w/o Tailing Spaces") {
      for (len <- Seq(5, 10) ++ range) {
        writeBenchmark(N / len, len, hasSpaces = false)
      }
    }

    runBenchmark("Char Varchar Write Side Perf w/ Tailing Spaces") {
      for (len <- Seq(5, 10) ++ range) {
        // in write side length check, we only visit the last few spaces
        writeBenchmark(N / len, len, hasSpaces = true)
      }
    }
  }
}
