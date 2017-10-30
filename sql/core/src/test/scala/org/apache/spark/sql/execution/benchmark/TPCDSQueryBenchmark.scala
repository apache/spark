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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.util.Benchmark

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

  def tpcdsAll(dataLocation: String, queries: Seq[String]): Unit = {
    val tableSizes = setupTables(dataLocation)
    queries.foreach { name =>
      val queryString = resourceToString(s"tpcds/$name.sql",
        classLoader = Thread.currentThread().getContextClassLoader)

      // This is an indirect hack to estimate the size of each query's input by traversing the
      // logical plan and adding up the sizes of all tables that appear in the plan.
      val queryRelations = scala.collection.mutable.HashSet[String]()
      spark.sql(queryString).queryExecution.analyzed.foreach {
        case SubqueryAlias(alias, _: LogicalRelation) =>
          queryRelations.add(alias)
        case LogicalRelation(_, _, Some(catalogTable), _) =>
          queryRelations.add(catalogTable.identifier.table)
        case HiveTableRelation(tableMeta, _, _) =>
          queryRelations.add(tableMeta.identifier.table)
        case _ =>
      }
      val numRows = queryRelations.map(tableSizes.getOrElse(_, 0L)).sum
      val benchmark = new Benchmark(s"TPCDS Snappy", numRows, 5)
      benchmark.addCase(name) { i =>
        spark.sql(queryString).collect()
      }
      logInfo(s"\n\n===== TPCDS QUERY BENCHMARK OUTPUT FOR $name =====\n")
      benchmark.run()
      logInfo(s"\n\n===== FINISHED $name =====\n")
    }
  }

  def main(args: Array[String]): Unit = {
    val benchmarkArgs = new TPCDSQueryBenchmarkArguments(args)

    // List of all TPC-DS queries
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

    // If `--query-filter` defined, filters the queries that this option selects
    val queriesToRun = if (benchmarkArgs.queryFilter.nonEmpty) {
      val queries = tpcdsQueries.filter { case queryName =>
        benchmarkArgs.queryFilter.contains(queryName)
      }
      if (queries.isEmpty) {
        throw new RuntimeException(
          s"Empty queries to run. Bad query name filter: ${benchmarkArgs.queryFilter}")
      }
      queries
    } else {
      tpcdsQueries
    }

    tpcdsAll(benchmarkArgs.dataLocation, queries = queriesToRun)
  }
}
