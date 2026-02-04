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
    // Use larger dataset to show the optimization benefit
    // 10000 orders with 10-element arrays, 1000 items
    val numOrders = 10000
    val numItems = 1000
    val arraySize = 10

    runBenchmark("CrossJoinArrayContainsToInnerJoin") {
      val benchmark = new Benchmark(
        s"array_contains optimization ($numOrders x $numItems, array=$arraySize)",
        numOrders.toLong * numItems,
        output = output
      )

      // Create test data with arrays containing distinct random elements
      // Each order has an array of arraySize distinct item_ids to simulate real use case
      val arrExpr = s"transform(sequence(0, $arraySize - 1), " +
        s"x -> cast((id + x * 100) % $numItems as int))"
      spark.range(numOrders)
        .selectExpr("id as order_id", s"$arrExpr as arr")
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

      // Case 1: Without our rule - uses CartesianProduct + Filter
      // This is the expensive O(N*M) approach
      benchmark.addCase("cross join + array_contains (rule off)", 3) { _ =>
        withSQLConf(
          SQLConf.CROSS_JOINS_ENABLED.key -> "true",
          SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ruleName,
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          spark.sql(query).noop()
        }
      }

      // Case 2: With our rule - uses explode + hash join
      // This is the efficient O(N*arraySize + M) approach
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
