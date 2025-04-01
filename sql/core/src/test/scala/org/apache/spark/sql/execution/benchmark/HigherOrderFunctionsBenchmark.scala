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
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

/**
 * Synthetic benchmark for higher order functions.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/HigherOrderFunctionsBenchmark-results.txt".
 * }}}
 */
object HigherOrderFunctionsBenchmark extends SqlBasedBenchmark {
  private val N = 100_000_00
  private val M = 10

  private val df = spark.range(N).select(array(col("id"), col("id"), col("id")).alias("arr"))

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Higher order functions") {
      def benchFunction(name: String, col: Column) = {
        var benchmark = new Benchmark(name, N, output = output)
        benchmark.addCase("codegen", M) { _ =>
          withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key ->
              CodegenObjectFactoryMode.CODEGEN_ONLY.toString()) {
            df.select(col).noop()
          }
        }

        benchmark.addCase("interpreted", M) { _ =>
          withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key ->
              CodegenObjectFactoryMode.NO_CODEGEN.toString()) {
            df.select(col).noop()
          }
        }
        benchmark.run()
      }

      benchFunction("transform", transform(col("arr"), x => x + 1))
      benchFunction("filter", filter(col("arr"), x => x > 1))
      benchFunction("forall - fast", forall(col("arr"), x => x < 0))
      benchFunction("forall - slow", forall(col("arr"), x => x >= 0))
      benchFunction("exists - fast", exists(col("arr"), x => x >= 0))
      benchFunction("exists - slow", exists(col("arr"), x => x < 0))
      benchFunction("aggregate", aggregate(col("arr"), lit(0L), (acc, x) => acc + x))
    }
  }
}
