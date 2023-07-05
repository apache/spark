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

import org.apache.spark.sql.TPCHSchema
import org.apache.spark.sql.types.StructType

/**
 * Benchmark to measure TPCH query performance.
 * To run this:
 * {{{
 *   1. without sbt:
 *        bin/spark-submit --jars <spark core test jar>,<spark catalyst test jar>
 *          --class <this class> <spark sql test jar> --data-location <location>
 *   2. build/sbt "sql/Test/runMain <this class> --data-location <TPCH data location>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt
 *        "sql/Test/runMain <this class> --data-location <location>"
 *      Results will be written to "benchmarks/TPCHQueryBenchmark-results.txt".
 * }}}
 */
object TPCHQueryBenchmark extends TPCBenchmarkUtils {
  override val tables: Seq[String] = Seq("part", "supplier", "partsupp", "customer", "orders",
    "lineitem", "nation", "region")
  override val queryType: String = "TPCH"
  val dataLocationEnv: String = "SPARK_TPCH_DATA"

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val benchmarkArgs = new TPCQueryBenchmarkArguments(mainArgs, dataLocationEnv)

    // List of all TPC-H queries
    val tpchQueries = Seq("q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10",
      "q11", "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19", "q20", "q21", "q22")

    // If `--query-filter` defined, filters the queries that this option selects
    val queriesToRun = filterQueries(tpchQueries, benchmarkArgs.queryFilter)

    if (queriesToRun.isEmpty) {
      throw new RuntimeException(
        s"Empty queries to run. Bad query name filter: ${benchmarkArgs.queryFilter}")
    }

    val tableSizes = setupTables(benchmarkArgs.dataLocation,
      TPCHSchemaHelper.getTableColumns)
    configureCbo(benchmarkArgs)

    runTpcQueries(queryLocation = "tpch", queries = queriesToRun, tableSizes)
  }
}

object TPCHSchemaHelper extends TPCHSchema {
  def getTableColumns: Map[String, StructType] =
    tableColumns.map(kv => kv._1 -> StructType.fromDDL(kv._2))
}
