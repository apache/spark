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

import scala.util.Try

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.TPCHSchema
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_SECOND
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf
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
object TPCHQueryBenchmark extends SqlBasedBenchmark with Logging {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setMaster(System.getProperty("spark.sql.test.master", "local[1]"))
      .setAppName("test-sql-context")
      .set("spark.sql.parquet.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", System.getProperty("spark.sql.shuffle.partitions", "4"))
      .set("spark.driver.memory", "3g")
      .set("spark.executor.memory", "3g")
      .set("spark.sql.autoBroadcastJoinThreshold", (20 * 1024 * 1024).toString)
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "true")

    SparkSession.builder.config(conf).getOrCreate()
  }

  val tables = Seq("part", "supplier", "partsupp", "customer", "orders",
    "lineitem", "nation", "region")

  def setupTables(dataLocation: String, tableColumns: Map[String, StructType]): Map[String, Long] =
    tables.map { tableName =>
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      val options = Map("path" -> s"$dataLocation/$tableName")
      spark.catalog.createTable(tableName, "parquet", tableColumns(tableName), options)
      // Recover partitions but don't fail if a table is not partitioned.
      Try {
        spark.sql(s"ALTER TABLE $tableName RECOVER PARTITIONS")
      }.getOrElse {
        logInfo(s"Recovering partitions of table $tableName failed")
      }
      tableName -> spark.table(tableName).count()
    }.toMap

  def runTpchQueries(
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
      spark.sparkContext.setJobGroup(name, s"$name:\n$queryString", true)
      spark.sql(queryString).queryExecution.analyzed.foreach {
        case SubqueryAlias(alias, _: LogicalRelation) =>
          queryRelations.add(alias.name)
        case LogicalRelation(_, _, Some(catalogTable), _) =>
          queryRelations.add(catalogTable.identifier.table)
        case HiveTableRelation(tableMeta, _, _, _, _) =>
          queryRelations.add(tableMeta.identifier.table)
        case _ =>
      }
      val numRows = queryRelations.map(tableSizes.getOrElse(_, 0L)).sum
      val benchmark = new Benchmark(s"TPCH Snappy", numRows, 2, output = output)
      benchmark.addCase(s"$name$nameSuffix") { _ =>
        spark.sql(queryString).noop()
      }
      benchmark.run()
    }
  }

  private def filterQueries(
                             origQueries: Seq[String],
                             queryFilter: Set[String],
                             nameSuffix: String = ""): Seq[String] = {
    if (queryFilter.nonEmpty) {
      if (nameSuffix.nonEmpty) {
        origQueries.filter { name => queryFilter.contains(s"$name$nameSuffix") }
      } else {
        origQueries.filter(queryFilter.contains)
      }
    } else {
      origQueries
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val benchmarkArgs = new TPCHQueryBenchmarkArguments(mainArgs)

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
    if (benchmarkArgs.cboEnabled) {
      spark.sql(s"SET ${SQLConf.CBO_ENABLED.key}=true")
      spark.sql(s"SET ${SQLConf.PLAN_STATS_ENABLED.key}=true")
      spark.sql(s"SET ${SQLConf.JOIN_REORDER_ENABLED.key}=true")
      spark.sql(s"SET ${SQLConf.HISTOGRAM_ENABLED.key}=true")

      // Analyze all the tables before running TPCH queries
      val startTime = System.nanoTime()
      tables.foreach { tableName =>
        spark.sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR ALL COLUMNS")
      }
      logInfo("The elapsed time to analyze all the tables is " +
        s"${(System.nanoTime() - startTime) / NANOS_PER_SECOND.toDouble} seconds")
    } else {
      spark.sql(s"SET ${SQLConf.CBO_ENABLED.key}=false")
    }

    runTpchQueries(queryLocation = "tpch", queries = queriesToRun, tableSizes)
  }
}

object TPCHSchemaHelper extends TPCHSchema {
  def getTableColumns: Map[String, StructType] =
    tableColumns.map(kv => kv._1 -> StructType.fromDDL(kv._2))
}
