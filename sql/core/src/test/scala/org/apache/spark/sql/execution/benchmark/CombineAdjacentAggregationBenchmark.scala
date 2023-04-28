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
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark to measure performance for CombineAdjacentAggregation.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/CombineAdjacentAggregationBenchmark-results.txt".
 * }}}
 */
object CombineAdjacentAggregationBenchmark extends SqlBasedBenchmark {
  import spark.implicits._

  private def combineAdjacentAggregation(N: Int, cardinality: Int): Unit = {
    val benchmark = new Benchmark(s"Combine adjacent aggregation cardinality: $cardinality",
      valuesPerIteration = N, minNumIters = 3, output = output)

    val df = spark.range(N)
      .selectExpr(s"id % ($N/$cardinality) as key", "id % 7 as value")

    benchmark.addCase("no adjacent aggregation") { _ =>
      df.groupBy("key")
        .agg(sum($"value"), count($"value"), avg($"value"), max($"value"), min($"value"))
        .noop()
    }

    benchmark.addCase("combine adjacent aggregation - disable") { _ =>
      withSQLConf(SQLConf.COMBINE_ADJACENT_AGGREGATION_ENABLED.key -> "false") {
        df.repartition($"key")
          .groupBy("key")
          .agg(sum($"value"), count($"value"), avg($"value"), max($"value"), min($"value"))
          .noop()
      }
    }

    benchmark.addCase("combine adjacent aggregation - enable") { _ =>
      withSQLConf(SQLConf.COMBINE_ADJACENT_AGGREGATION_ENABLED.key -> "true") {
        df.repartition($"key")
          .groupBy("key")
          .agg(sum($"value"), count($"value"), avg($"value"), max($"value"), min($"value"))
          .noop()
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    withSQLConf(
        SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "5",
        SQLConf.SHUFFLE_PARTITIONS.key -> "5") {
      runBenchmark("Combine adjacent aggregation high cardinality") {
        combineAdjacentAggregation(20 * 1000 * 1000, 5)
      }

      runBenchmark("Combine adjacent aggregation low cardinality") {
        combineAdjacentAggregation(20 * 1000 * 1000, 50)
      }
    }
  }
}
