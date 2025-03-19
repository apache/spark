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
import org.apache.spark.sql.functions._

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
      var benchmark = new Benchmark("transform", N, output = output)
      benchmark.addCase("codegen", M) { _ =>
        df.select(transform(col("arr"), x => x + 1)).noop()
      }
      benchmark.addCase("interpreted", M) { _ =>
        withSQLConf("spark.sql.codegen.factoryMode" -> "NO_CODEGEN") {
          df.select(transform(col("arr"), x => x + 1)).noop()
        }
      }
      benchmark.run()

      benchmark = new Benchmark("filter", N, output = output)
      benchmark.addCase("codegen", M) { _ =>
        df.select(filter(col("arr"), x => x > 1)).noop()
      }
      benchmark.addCase("interpreted", M) { _ =>
        withSQLConf("spark.sql.codegen.factoryMode" -> "NO_CODEGEN") {
          df.select(filter(col("arr"), x => x > 1)).noop()
        }
      }
      benchmark.run()

      benchmark = new Benchmark("forall - fast", N, output = output)
      benchmark.addCase("codegen", M) { _ =>
        df.select(forall(col("arr"), x => x < 0)).noop()
      }
      benchmark.addCase("interpreted", M) { _ =>
        withSQLConf("spark.sql.codegen.factoryMode" -> "NO_CODEGEN") {
          df.select(forall(col("arr"), x => x < 0)).noop()
        }
      }
      benchmark.run()

      benchmark = new Benchmark("forall - slow", N, output = output)
      benchmark.addCase("codegen", M) { _ =>
        df.select(forall(col("arr"), x => x >= 0)).noop()
      }
      benchmark.addCase("interpreted", M) { _ =>
        withSQLConf("spark.sql.codegen.factoryMode" -> "NO_CODEGEN") {
          df.select(forall(col("arr"), x => x >= 0)).noop()
        }
      }
      benchmark.run()

      benchmark = new Benchmark("exists - fast", N, output = output)
      benchmark.addCase("codegen", M) { _ =>
        df.select(exists(col("arr"), x => x >= 0)).noop()
      }
      benchmark.addCase("interpreted", M) { _ =>
        withSQLConf("spark.sql.codegen.factoryMode" -> "NO_CODEGEN") {
          df.select(exists(col("arr"), x => x >= 0)).noop()
        }
      }
      benchmark.run()

      benchmark = new Benchmark("exists - slow", N, output = output)
      benchmark.addCase("codegen", M) { _ =>
        df.select(exists(col("arr"), x => x < 0)).noop()
      }
      benchmark.addCase("interpreted", M) { _ =>
        withSQLConf("spark.sql.codegen.factoryMode" -> "NO_CODEGEN") {
          df.select(exists(col("arr"), x => x < 0)).noop()
        }
      }
      benchmark.run()

      benchmark = new Benchmark("aggregate", N, output = output)
      benchmark.addCase("codegen", M) { _ =>
        df.select(aggregate(col("arr"), lit(0L), (acc, x) => acc + x)).noop()
      }
      benchmark.addCase("interpreted", M) { _ =>
        withSQLConf("spark.sql.codegen.factoryMode" -> "NO_CODEGEN") {
          df.select(aggregate(col("arr"), lit(0L), (acc, x) => acc + x)).noop()
        }
      }
      benchmark.run()
    }
  }
}
