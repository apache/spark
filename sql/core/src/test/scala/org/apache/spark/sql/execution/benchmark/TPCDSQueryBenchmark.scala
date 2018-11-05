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

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, TPCDSUtils}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
 * Benchmark to measure TPCDS query performance.
 * To run this:
 *  spark-submit --class <this class> <spark sql test jar> --data-location <TPCDS data location>
 */
object TPCDSQueryBenchmark extends Logging {
  val conf =
    new SparkConf()
      .setMaster("local[1]")
      .setAppName("test-sql-context")
      .set("spark.sql.parquet.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "4")
      .set("spark.driver.memory", "3g")
      .set("spark.executor.memory", "3g")
      .set("spark.sql.autoBroadcastJoinThreshold", (20 * 1024 * 1024).toString)
      .set("spark.sql.crossJoin.enabled", "true")

  val spark = SparkSession.builder.config(conf).getOrCreate()

  val tables = Seq("catalog_page", "catalog_returns", "customer", "customer_address",
    "customer_demographics", "date_dim", "household_demographics", "inventory", "item",
    "promotion", "store", "store_returns", "catalog_sales", "web_sales", "store_sales",
    "web_returns", "web_site", "reason", "call_center", "warehouse", "ship_mode", "income_band",
    "time_dim", "web_page")

  def setupTables(dataLocation: String): Map[String, Long] = {
    tables.map { tableName =>
      spark.read.parquet(s"$dataLocation/$tableName").createOrReplaceTempView(tableName)
      tableName -> spark.table(tableName).count()
    }.toMap
  }

  def runTpcdsQueries(
      queryLocation: String,
      queries: Seq[String],
      tableSizes: Map[String, Long],
      nameSuffix: String = ""): Unit = {
    queries.foreach { name =>
      val queryString = resourceToString(s"$queryLocation/$name.sql",
        classLoader = Thread.currentThread().getContextClassLoader)

      // This is an indirect hack to estimate the size of each query's input by traversing the
      // logical plan and adding up the sizes of all tables that appear in the plan.
      val queryRelations = scala.collection.mutable.HashSet[String]()
      spark.sql(queryString).queryExecution.analyzed.foreach {
        case SubqueryAlias(alias, _: LogicalRelation) =>
          queryRelations.add(alias.identifier)
        case LogicalRelation(_, _, Some(catalogTable), _) =>
          queryRelations.add(catalogTable.identifier.table)
        case HiveTableRelation(tableMeta, _, _) =>
          queryRelations.add(tableMeta.identifier.table)
        case _ =>
      }
      val numRows = queryRelations.map(tableSizes.getOrElse(_, 0L)).sum
      val benchmark = new Benchmark(s"TPCDS Snappy", numRows, 5)
      benchmark.addCase(s"$name$nameSuffix") { _ =>
        spark.sql(queryString).collect()
      }
      logInfo(s"\n\n===== TPCDS QUERY BENCHMARK OUTPUT FOR $name =====\n")
      benchmark.run()
      logInfo(s"\n\n===== FINISHED $name =====\n")
    }
  }

  def filterQueries(
      origQueries: Seq[String],
      args: TPCDSQueryBenchmarkArguments): Seq[String] = {
    if (args.queryFilter.nonEmpty) {
      origQueries.filter(args.queryFilter.contains)
    } else {
      origQueries
    }
  }

  def main(args: Array[String]): Unit = {
    val benchmarkArgs = new TPCDSQueryBenchmarkArguments(args)

    // If `--query-filter` defined, filters the queries that this option selects
    val queriesV1_4ToRun = filterQueries(TPCDSUtils.tpcdsQueries, benchmarkArgs)
    val queriesV2_7ToRun = filterQueries(TPCDSUtils.tpcdsQueriesV2_7_0, benchmarkArgs)

    if ((queriesV1_4ToRun ++ queriesV2_7ToRun).isEmpty) {
      throw new RuntimeException(
        s"Empty queries to run. Bad query name filter: ${benchmarkArgs.queryFilter}")
    }

    val tableSizes = setupTables(benchmarkArgs.dataLocation)
    runTpcdsQueries(queryLocation = "tpcds", queries = queriesV1_4ToRun, tableSizes)
    runTpcdsQueries(queryLocation = "tpcds-v2.7.0", queries = queriesV2_7ToRun, tableSizes,
      nameSuffix = "-v2.7")
  }
}
