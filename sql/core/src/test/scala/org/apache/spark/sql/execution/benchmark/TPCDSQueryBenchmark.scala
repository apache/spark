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

import org.apache.spark.sql.TPCDSSchema
import org.apache.spark.sql.types.StructType

/**
 * Benchmark to measure TPCDS query performance.
 * To run this:
 * {{{
 *   1. without sbt:
 *        bin/spark-submit --jars <spark core test jar>,<spark catalyst test jar>
 *          --class <this class> <spark sql test jar> --data-location <location>
 *   2. build/sbt "sql/Test/runMain <this class> --data-location <TPCDS data location>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt
 *        "sql/Test/runMain <this class> --data-location <location>"
 *      Results will be written to "benchmarks/TPCDSQueryBenchmark-results.txt".
 * }}}
 */
object TPCDSQueryBenchmark extends TPCBasedBenchmark {

  override val tables: Seq[String] = Seq("catalog_page", "catalog_returns", "customer",
    "customer_address", "customer_demographics", "date_dim", "household_demographics",
    "inventory", "item", "promotion", "store", "store_returns", "catalog_sales",
    "web_sales", "store_sales", "web_returns", "web_site", "reason", "call_center",
    "warehouse", "ship_mode", "income_band", "time_dim", "web_page")
  override val queryType: String = "TPCDS"
  val dataLocationEnv: String = "SPARK_TPCDS_DATA"

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val benchmarkArgs = new TPCQueryBenchmarkArguments(mainArgs, dataLocationEnv)

    // List of all TPC-DS v1.4 queries
    val tpcdsQueries = Seq(
      "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11",
      "q12", "q13", "q14a", "q14b", "q15", "q16", "q17", "q18", "q19", "q20",
      "q21", "q22", "q23a", "q23b", "q24a", "q24b", "q25", "q26", "q27", "q28", "q29", "q30",
      "q31", "q32", "q33", "q34", "q35", "q36", "q37", "q38", "q39a", "q39b", "q40",
      "q41", "q42", "q43", "q44", "q45", "q46", "q47", "q48", "q49", "q50",
      "q51", "q52", "q53", "q54", "q55", "q56", "q57", "q58", "q59", "q60",
      "q61", "q62", "q63", "q64", "q65", "q66", "q67", "q68", "q69", "q70",
      "q71", "q72", "q73", "q74", "q75", "q76", "q77", "q78", "q79", "q80",
      "q81", "q82", "q83", "q84", "q85", "q86", "q87", "q88", "q89", "q90",
      "q91", "q92", "q93", "q94", "q95", "q96", "q97", "q98", "q99")

    // This list only includes TPC-DS v2.7 queries that are different from v1.4 ones
    val nameSuffixForQueriesV2_7 = "-v2.7"
    val tpcdsQueriesV2_7 = Seq(
      "q5a", "q6", "q10a", "q11", "q12", "q14", "q14a", "q18a",
      "q20", "q22", "q22a", "q24", "q27a", "q34", "q35", "q35a", "q36a", "q47", "q49",
      "q51a", "q57", "q64", "q67a", "q70a", "q72", "q74", "q75", "q77a", "q78",
      "q80a", "q86a", "q98")

    // If `--query-filter` defined, filters the queries that this option selects
    val queriesV1_4ToRun = filterQueries(tpcdsQueries, benchmarkArgs.queryFilter)
    val queriesV2_7ToRun = filterQueries(tpcdsQueriesV2_7, benchmarkArgs.queryFilter,
      nameSuffix = nameSuffixForQueriesV2_7)

    if ((queriesV1_4ToRun ++ queriesV2_7ToRun).isEmpty) {
      throw new RuntimeException(
        s"Empty queries to run. Bad query name filter: ${benchmarkArgs.queryFilter}")
    }

    val tableSizes = setupTables(benchmarkArgs.dataLocation,
      TPCDSSchemaHelper.getTableColumns)
    configureCbo(benchmarkArgs)

    runTpcQueries(queryLocation = "tpcds", queries = queriesV1_4ToRun, tableSizes)
    runTpcQueries(queryLocation = "tpcds-v2.7.0", queries = queriesV2_7ToRun, tableSizes,
      nameSuffix = nameSuffixForQueriesV2_7)
  }
}

object TPCDSSchemaHelper extends TPCDSSchema {
  def getTableColumns: Map[String, StructType] =
    tableColumns.map(kv => kv._1 -> StructType.fromDDL(kv._2))
}
