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
    runStandardBenchmark()
    runLargeArraySmallTableBenchmark()
  }

  /**
   * Standard benchmark: small arrays, large right table.
   * This is the scenario where the optimization provides significant benefit.
   */
  private def runStandardBenchmark(): Unit = {
    // Use larger dataset to show the optimization benefit
    // 10000 orders with 10-element arrays, 1000 items
    val numOrders = 10000
    val numItems = 1000
    val arraySize = 10

    runBenchmark("CrossJoinArrayContainsToInnerJoin - Standard Case") {
      val benchmark = new Benchmark(
        s"small arrays ($numOrders orders x $numItems items, array=$arraySize)",
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

  /**
   * Large array benchmark: large arrays, small right table.
   * This is the scenario where the optimization might NOT be beneficial.
   * When array_size >> right_table_size, exploding creates more rows than cross join.
   *
   * Using same cross-product size as standard case for fair comparison:
   * - Cross join + filter: O(left_rows * right_rows) = 10000 * 1000 = 10,000,000
   * - Explode + join: O(left_rows * array_size) = 10000 * 1000 = 10,000,000
   *
   * The difference is that exploded rows join with only 100 items (small table),
   * while in standard case they join with 1000 items.
   */
  private def runLargeArraySmallTableBenchmark(): Unit = {
    val numOrders = 10000     // Same as standard case
    val numItems = 100        // Small right table (10x smaller than standard)
    val arraySize = 1000      // Large arrays (10x larger than standard)

    runBenchmark("CrossJoinArrayContainsToInnerJoin - Large Arrays (potential regression)") {
      val benchmark = new Benchmark(
        s"large arrays ($numOrders orders x $numItems items, array=$arraySize)",
        numOrders.toLong * numItems,
        output = output
      )

      // Create test data with large arrays
      val arrExpr = s"transform(sequence(0, $arraySize - 1), " +
        s"x -> cast((id + x) % ($numItems * 10) as int))"
      spark.range(numOrders)
        .selectExpr("id as order_id", s"$arrExpr as arr")
        .cache()
        .createOrReplaceTempView("orders_large_arr")

      spark.range(numItems)
        .selectExpr("cast(id as int) as item_id", "concat('item_', id) as name")
        .cache()
        .createOrReplaceTempView("items_small")

      // Force cache materialization
      spark.sql("SELECT count(*) FROM orders_large_arr").collect()
      spark.sql("SELECT count(*) FROM items_small").collect()

      val query = "SELECT o.order_id, i.item_id FROM orders_large_arr o, items_small i " +
        "WHERE array_contains(o.arr, i.item_id)"

      // Case 1: Without our rule - cross join might be faster here
      benchmark.addCase("cross join + array_contains (rule off)", 3) { _ =>
        withSQLConf(
          SQLConf.CROSS_JOINS_ENABLED.key -> "true",
          SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ruleName,
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          spark.sql(query).noop()
        }
      }

      // Case 2: With our rule - explode might be slower here
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
