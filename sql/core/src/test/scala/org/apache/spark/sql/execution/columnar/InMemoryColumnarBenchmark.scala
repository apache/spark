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
package org.apache.spark.sql.execution.columnar

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.execution.ColumnarToRowExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark

/**
 * Benchmark to low level memory access using different ways to manage buffers.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar> <rowsNum>
 *   2. build/sbt "sql/Test/runMain <this class> <rowsNum>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>
 *        <rowsNum>"
 *      Results will be written to "benchmarks/InMemoryColumnarBenchmark-results.txt".
 * }}}
 */
object InMemoryColumnarBenchmark extends SqlBasedBenchmark with AdaptiveSparkPlanHelper {
  def intCache(rowsNum: Long, numIters: Int): Unit = {
    val data = spark.range(0, rowsNum, 1, 1).toDF("i").cache()

    val inMemoryScan = collect(data.queryExecution.executedPlan) {
      case m: InMemoryTableScanExec => m
    }

    val columnarScan = ColumnarToRowExec(inMemoryScan(0))
    val rowScan = inMemoryScan(0)

    assert(inMemoryScan.size == 1)

    val benchmark = new Benchmark("Int In-Memory scan", rowsNum, output = output)

    benchmark.addCase("columnar deserialization + columnar-to-row", numIters) { _ =>
      columnarScan.executeCollect()
    }

    benchmark.addCase("row-based deserialization", numIters) { _ =>
      rowScan.executeCollect()
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val rowsNum = if (mainArgs.length > 0) mainArgs(0).toLong else 1000000
    runBenchmark(s"Int In-memory with $rowsNum rows") {
      intCache(rowsNum = rowsNum, numIters = 3)
    }
  }
}
