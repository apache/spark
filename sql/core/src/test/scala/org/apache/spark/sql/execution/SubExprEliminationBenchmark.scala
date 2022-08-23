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
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Or}
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
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/SubExprEliminationBenchmark-results.txt".
 * }}}
 */
object SubExprEliminationBenchmark extends SqlBasedBenchmark {
  import spark.implicits._

  def withFromJson(rowsNum: Int, numIters: Int): Unit = {
    val benchmark = new Benchmark("from_json as subExpr in Project", rowsNum, output = output)

    withTempPath { path =>
      prepareDataInfo(benchmark)
      val numCols = 500
      val schema = writeWideRow(path.getAbsolutePath, rowsNum, numCols)

      val cols = (0 until numCols).map { idx =>
        from_json($"value", schema).getField(s"col$idx")
      }

      Seq(
        ("false", "true", "CODEGEN_ONLY"),
        ("false", "false", "NO_CODEGEN"),
        ("true", "true", "CODEGEN_ONLY"),
        ("true", "false", "NO_CODEGEN")
      ).foreach { case (subExprEliminationEnabled, codegenEnabled, codegenFactory) =>
        // We only benchmark subexpression performance under codegen/non-codegen, so disabling
        // json optimization.
        val caseName = s"subExprElimination $subExprEliminationEnabled, codegen: $codegenEnabled"
        benchmark.addCase(caseName, numIters) { _ =>
          withSQLConf(
            SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED.key -> subExprEliminationEnabled,
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegenEnabled,
            SQLConf.CODEGEN_FACTORY_MODE.key -> codegenFactory,
            SQLConf.JSON_EXPRESSION_OPTIMIZATION.key -> "false") {
            val df = spark.read
              .text(path.getAbsolutePath)
              .select(cols: _*)
            df.write.mode("overwrite").format("noop").save()
          }
        }
      }

      benchmark.run()
    }
  }

  def withFilter(rowsNum: Int, numIters: Int): Unit = {
    val benchmark = new Benchmark("from_json as subExpr in Filter", rowsNum, output = output)

    withTempPath { path =>
      prepareDataInfo(benchmark)
      val numCols = 500
      val schema = writeWideRow(path.getAbsolutePath, rowsNum, numCols)

      val predicate = (0 until numCols).map { idx =>
        (from_json($"value", schema).getField(s"col$idx") >= Literal(100000)).expr
      }.asInstanceOf[Seq[Expression]].reduce(Or)

      Seq(
        ("false", "true", "CODEGEN_ONLY"),
        ("false", "false", "NO_CODEGEN"),
        ("true", "true", "CODEGEN_ONLY"),
        ("true", "false", "NO_CODEGEN")
      ).foreach { case (subExprEliminationEnabled, codegenEnabled, codegenFactory) =>
        // We only benchmark subexpression performance under codegen/non-codegen, so disabling
        // json optimization.
        val caseName = s"subExprElimination $subExprEliminationEnabled, codegen: $codegenEnabled"
        benchmark.addCase(caseName, numIters) { _ =>
          withSQLConf(
            SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED.key -> subExprEliminationEnabled,
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegenEnabled,
            SQLConf.CODEGEN_FACTORY_MODE.key -> codegenFactory,
            SQLConf.JSON_EXPRESSION_OPTIMIZATION.key -> "false") {
            val df = spark.read
              .text(path.getAbsolutePath)
              .where(Column(predicate))
            df.write.mode("overwrite").format("noop").save()
          }
        }
      }

      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val numIters = 3
    runBenchmark("Benchmark for performance of subexpression elimination") {
      withFromJson(100, numIters)
      withFilter(100, numIters)
    }
  }
}
