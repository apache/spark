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
package org.apache.spark.sql.execution

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

/**
 * The benchmarks aims to measure performance of the queries where there are subexpression
 * elimination or not.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar>,
 *        <spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/SubExprEliminationBenchmark-results.txt".
 * }}}
 */
object SubExprEliminationBenchmark extends SqlBasedBenchmark {
  import spark.implicits._

  def withFromJson(rowsNum: Int, numIters: Int): Unit = {
    val benchmark = new Benchmark("from_json as subExpr", rowsNum, output = output)

    withTempPath { path =>
      prepareDataInfo(benchmark)
      val numCols = 1000
      val schema = writeWideRow(path.getAbsolutePath, rowsNum, numCols)

      val cols = (0 until numCols).map { idx =>
        from_json('value, schema).getField(s"col$idx")
      }

      // We only benchmark subexpression performance under codegen/non-codegen, so disabling
      // json optimization.
      benchmark.addCase("subexpressionElimination off, codegen on", numIters) { _ =>
        withSQLConf(
          SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED.key -> "false",
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
          SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY",
          SQLConf.JSON_EXPRESSION_OPTIMIZATION.key -> "false") {
          val df = spark.read
            .text(path.getAbsolutePath)
            .select(cols: _*)
          df.collect()
        }
      }

      benchmark.addCase("subexpressionElimination off, codegen off", numIters) { _ =>
        withSQLConf(
          SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED.key -> "false",
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
          SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN",
          SQLConf.JSON_EXPRESSION_OPTIMIZATION.key -> "false") {
          val df = spark.read
            .text(path.getAbsolutePath)
            .select(cols: _*)
          df.collect()
        }
      }

      benchmark.addCase("subexpressionElimination on, codegen on", numIters) { _ =>
        withSQLConf(
            SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED.key -> "true",
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
            SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY",
            SQLConf.JSON_EXPRESSION_OPTIMIZATION.key -> "false") {
          val df = spark.read
            .text(path.getAbsolutePath)
            .select(cols: _*)
          df.collect()
        }
      }

      benchmark.addCase("subexpressionElimination on, codegen off", numIters) { _ =>
        withSQLConf(
          SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED.key -> "true",
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
          SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN",
          SQLConf.JSON_EXPRESSION_OPTIMIZATION.key -> "false") {
          val df = spark.read
            .text(path.getAbsolutePath)
            .select(cols: _*)
          df.collect()
        }
      }

      benchmark.run()
    }
  }


  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val numIters = 3
    runBenchmark("Benchmark for performance of subexpression elimination") {
      withFromJson(100, numIters)
    }
  }
}
