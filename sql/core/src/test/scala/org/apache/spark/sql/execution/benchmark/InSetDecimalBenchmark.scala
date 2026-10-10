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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DecimalType

/**
 * Benchmark for InSet membership over decimal columns: the sorted-array binary search vs the
 * generic boxed Set. Decimal.hashCode is toBigDecimal.hashCode (allocates a java.math.BigDecimal
 * per row for compact decimals), while Decimal.compare is a cheap long compare for compact
 * decimals; binary search avoids the hash allocation. Covers compact decimal(12,1) (precision <=
 * 18, long-backed) and BigDecimal-backed decimal(30,7). IN values are non-matching so every row
 * evaluates the full predicate.
 *
 * {{{
 *   build/sbt "sql/Test/runMain org.apache.spark.sql.execution.benchmark.InSetDecimalBenchmark"
 * }}}
 */
object InSetDecimalBenchmark extends SqlBasedBenchmark {

  import spark.implicits._

  private val numRows = 10L * 1000 * 1000
  private val sizes = Seq(20, 100, 200)

  private def sweep(label: String, df: DataFrame, values: Seq[String]): Unit = {
    df.createOrReplaceTempView("t")

    def q(): Unit = spark.sql(s"SELECT * FROM t WHERE id IN (${values.mkString(",")})").noop()

    val benchmark = new Benchmark(label, numRows, output = output)
    benchmark.addCase("InSet boxed (Set)", numIters = 3) { _ =>
      withSQLConf(
        SQLConf.OPTIMIZER_INSET_CONVERSION_THRESHOLD.key -> "1",
        SQLConf.OPTIMIZER_INSET_BINARY_SEARCH_ENABLED.key -> "false") {
        q()
      }
    }
    benchmark.addCase("InSet binarySearch", numIters = 3) { _ =>
      withSQLConf(
        SQLConf.OPTIMIZER_INSET_CONVERSION_THRESHOLD.key -> "1",
        SQLConf.OPTIMIZER_INSET_BINARY_SEARCH_ENABLED.key -> "true") {
        q()
      }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Decimal IN membership: sorted-array binary search vs boxed Set") {
      sizes.foreach { n =>
        val small = spark.range(numRows).select($"id".cast(DecimalType(12, 1)).as("id"))
        val smallVals = (1 to n).map(v => s"CAST(${-v} AS decimal(12,1))")
        sweep(s"compact decimal(12,1), $n values", small, smallVals)
        val large = spark.range(numRows).select($"id".cast(DecimalType(30, 7)).as("id"))
        val largeVals = (1 to n).map(v => s"CAST(${-v} AS decimal(30,7))")
        sweep(s"big decimal(30,7), $n values", large, largeVals)
      }
    }
  }
}
