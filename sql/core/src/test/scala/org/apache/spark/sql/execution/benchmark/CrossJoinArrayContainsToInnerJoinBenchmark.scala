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
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark for CrossJoinArrayContainsToInnerJoin optimization.
 *
 * To run this benchmark:
 * {{{
 *   1. build/sbt "sql/Test/runMain <this class>"
 *   2. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 * }}}
 */
object CrossJoinArrayContainsToInnerJoinBenchmark extends SqlBasedBenchmark {

  private val ruleName =
    "org.apache.spark.sql.catalyst.optimizer.CrossJoinArrayContainsToInnerJoin"

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    // 10000 x 1000 shows ~10X speedup - the sweet spot for this optimization
    val numOrders = 10000
    val numItems = 1000
    val arraySize = 10

    runBenchmark("CrossJoinArrayContainsToInnerJoin") {
      val benchmark = new Benchmark(
        s"array_contains optimization ($numOrders x $numItems, array=$arraySize)",
        numOrders.toLong * numItems,
        output = output
      )

      // Create test data with arrays containing repeated elements
      spark.range(numOrders)
        .selectExpr("id as order_id",
          s"array_repeat(cast((id % $numItems) as int), $arraySize) as arr")
        .cache()
        .createOrReplaceTempView("orders")

      spark.range(numItems)
        .selectExpr("cast(id as int) as item_id", "concat('item_', id) as name")
        .cache()
        .createOrReplaceTempView("items")

      // Force cache materialization
      spark.sql("SELECT count(*) FROM orders").collect()
      spark.sql("SELECT count(*) FROM items").collect()

      val query = "SELECT o.order_id, i.item_id FROM orders o, items i " +
        "WHERE array_contains(o.arr, i.item_id)"

      benchmark.addCase("cross join + array_contains (rule off)", 3) { _ =>
        withSQLConf(
          SQLConf.CROSS_JOINS_ENABLED.key -> "true",
          SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ruleName) {
          spark.sql(query).noop()
        }
      }

      benchmark.addCase("inner join + explode (rule on)", 3) { _ =>
        withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
          spark.sql(query).noop()
        }
      }

      benchmark.run()

      spark.catalog.clearCache()
    }
  }
}
