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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark

/**
 * This object used for track all TPC-DS queries optimized plans.
 * To run this tracker:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *   Results will be written to "benchmarks/TPCDSQueryOptimizerTracker-results.txt".
 * }}}
 */
object TPCDSQueryOptimizerTracker extends SqlBasedBenchmark {

  override def getSparkSession: SparkSession = {
    SparkSession.builder
      .master("local[1]")
      .appName(this.getClass.getSimpleName)
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()
  }

  def runTpcdsQueries(
      queryLocation: String,
      queries: Seq[String],
      nameSuffix: String = ""): Unit = {
    queries.foreach { name =>
      runBenchmark(name + nameSuffix) {
        val sql = resourceToString(s"$queryLocation/$name.sql",
          classLoader = Thread.currentThread().getContextClassLoader)
        output.get.write(spark.sql(sql).queryExecution.optimizedPlan.treeNodeName().getBytes)
        output.get.write('\n')
      }
    }
  }

  override def runBenchmarkSuite(args: Array[String]): Unit = {
    TPCDSUtils.setupTables(spark)
    runTpcdsQueries(queryLocation = "tpcds", queries = TPCDSUtils.tpcdsQueries)
    runTpcdsQueries(
      queryLocation = "tpcds-v2.7.0", queries = TPCDSUtils.tpcdsQueriesV2_7_0, nameSuffix = "-v2.7")
  }
}
