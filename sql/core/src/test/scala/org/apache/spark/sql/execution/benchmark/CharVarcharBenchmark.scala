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

/**
 * Benchmark for measure writing and reading char/varchar values with implicit length check
 * and padding.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/CharVarcharBenchmark-results.txt".
 * }}}
 */
object CharVarcharBenchmark extends SqlBasedBenchmark {
  import spark.implicits._

  def withTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }

  def readBenchmark(card: Long, length: Int): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(card).map { v =>
        val str = v.toString
        str + " " * (length - str.length)
      }.write.parquet(path)

      Seq("char", "varchar").foreach { typ =>
        codegenBenchmark(s"read $typ with length $length", card) {
          val tblName = s"${typ}_${length}_$card"
          withTable(tblName) {
            spark.sql(s"CREATE TABLE $tblName (c $typ($length)) USING PARQUET LOCATION '$path'")
            spark.table(tblName).noop()
          }
        }
      }
    }
  }

  def writeBenchmark(card: Long, length: Int): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq("char", "varchar").foreach { typ =>
        codegenBenchmark(s"write $typ with length $length", card) {
          val tblName = s"${typ}_${length}_$card"
          withTable(tblName) {
            spark.sql(s"CREATE TABLE $tblName (c $typ($length)) USING PARQUET LOCATION '$path'")
            spark.range(card).map { v =>
              val str = v.toString
              str + " " * length
            }.write.insertInto(tblName)
          }
        }
      }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Char Varchar Read Side Perf") {
      for (len <- Range(20, 100, 20)) {
        readBenchmark(100L * 1000 * 1000, len)
      }
    }

    runBenchmark("Char Varchar Write Side Perf") {
      for (len <- Range(20, 100, 20)) {
        writeBenchmark(10L * 1000 * 1000, len)
      }
    }
  }
}
