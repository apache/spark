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

package org.apache.spark.sql.hive.thriftserver

import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StringType}

/**
 * Benchmark for `RowSetUtils.toTRowSet`'s column-based path: materializing the fetched result
 * rows as a Scala `List` (the pre-existing `SparkExecuteStatementOperation.getNextRowSetInternal`
 * behavior, `.toList`) versus an `IndexedSeq` (`.toIndexedSeq`, the current behavior).
 *
 * `toColumnBasedSet` now does a single pass over `rows`, dispatching per cell via a `match` on a
 * precomputed per-column kind code (see `RowSetUtils.kindOf`), rather than re-scanning the full
 * row collection once per column as it used to. That single-pass rewrite is the dominant source of
 * speedup at scale; switching the caller from `List` to `IndexedSeq` only avoids `List`'s O(n)
 * `.length`, which is now called once per column from the single pass rather than per Thrift
 * column-value helper, and so matters far less than it used to.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar>
 *   2. build/sbt "hive-thriftserver/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "hive-thriftserver/Test/runMain <this class>"
 *      Results will be written to "benchmarks/RowSetUtilsBenchmark-results.txt".
 * }}}
 */
object RowSetUtilsBenchmark extends BenchmarkBase {

  private val protocolVersion = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10

  private def schemaOf(numCols: Int): Array[DataType] =
    (0 until numCols).map { i =>
      i % 3 match {
        case 0 => IntegerType
        case 1 => DoubleType
        case _ => StringType
      }
    }.toArray

  private def rowOf(schema: Array[DataType], i: Int): Row = Row.fromSeq(schema.zipWithIndex.map {
    case (IntegerType, j) => i + j
    case (DoubleType, j) => (i + j).toDouble
    case (_, j) => s"val-$i-$j"
  }.toIndexedSeq)

  private def runScaleBenchmark(numRows: Int, numCols: Int): Unit = {
    val schema = schemaOf(numCols)
    // Both cases share the same underlying Row objects (built once) so that large row counts
    // don't require two independent copies of the row payload resident at the same time.
    val rowsIndexedSeq = (0 until numRows).map(rowOf(schema, _)).toIndexedSeq
    val rowsList = rowsIndexedSeq.toList

    val benchmark = new Benchmark(
      s"toTRowSet, $numRows rows x $numCols cols", numRows, output = output)

    benchmark.addCase("rows: List (pre-existing .toList)") { _ =>
      RowSetUtils.toTRowSet(0L, rowsList, schema, protocolVersion)
    }

    benchmark.addCase("rows: IndexedSeq (.toIndexedSeq)") { _ =>
      RowSetUtils.toTRowSet(0L, rowsIndexedSeq, schema, protocolVersion)
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("RowSetUtils.toTRowSet: List vs Array result materialization") {
      // Column count fixed at a realistic wide-result width (150); row count scales from a small
      // page up to a large `maxRows` fetch.
      Seq(100, 1000, 10000, 100000, 1000000).foreach { numRows =>
        runScaleBenchmark(numRows, numCols = 150)
      }
    }
  }
}
