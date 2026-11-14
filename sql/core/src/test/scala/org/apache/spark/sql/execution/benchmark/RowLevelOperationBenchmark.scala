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
import org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTableCatalog
import org.apache.spark.sql.execution.CommandExecutionMode

/**
 * Benchmark to measure the analysis of DSv2 row-level operations on wide tables. The analyzer
 * rewrites DELETE, UPDATE and MERGE into plans that produce the rows to write.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/RowLevelOperationBenchmark-results.txt".
 * }}}
 */
object RowLevelOperationBenchmark extends SqlBasedBenchmark {

  private val catalogName = "benchmark_catalog"
  private val widths = Seq(100, 1000, 10000)

  /**
   * Creates a table `pk INT NOT NULL, c_1 INT, ..., c_<width> INT, dep STRING` partitioned by
   * `dep` for each width, and returns the table names by width.
   */
  private def createTables(prefix: String, properties: String): Seq[(Int, String)] = {
    widths.map { width =>
      val table = s"$catalogName.ns.${prefix}_$width"
      val columns = (1 to width).map(i => s"c_$i INT").mkString(", ")
      spark.sql(s"CREATE TABLE $table (pk INT NOT NULL, $columns, dep STRING) " +
        s"PARTITIONED BY (dep) $properties")
      width -> table
    }
  }

  /** Measures the analysis of the statement on each table, without executing it. */
  private def analyze(
      name: String,
      tables: Seq[(Int, String)])(statement: String => String): Unit = {
    val benchmark = new Benchmark(name, 1, output = output)
    tables.foreach { case (width, table) =>
      val sqlText = statement(table)
      benchmark.addCase(s"$width columns") { _ =>
        val plan = spark.sessionState.sqlParser.parsePlan(sqlText)
        spark.sessionState.executePlan(plan, CommandExecutionMode.SKIP).analyzed
      }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    spark.conf.set(s"spark.sql.catalog.$catalogName",
      classOf[InMemoryRowLevelOperationTableCatalog].getName)
    val groupBasedTables = createTables("group_based", properties = "")
    val deltaBasedTables =
      createTables("delta_based", properties = "TBLPROPERTIES ('supports-deltas' = 'true')")
    spark.sql("SELECT 1 AS pk, -1 AS v").createOrReplaceTempView("source")

    runBenchmark("analyze UPDATE") {
      val update = (t: String) => s"UPDATE $t SET c_1 = -1 WHERE pk = 1"
      analyze("analyze group-based UPDATE", groupBasedTables)(update)
      analyze("analyze delta-based UPDATE", deltaBasedTables)(update)
    }

    runBenchmark("analyze DELETE") {
      analyze("analyze group-based DELETE", groupBasedTables)(t => s"DELETE FROM $t WHERE pk = 1")
    }

    runBenchmark("analyze MERGE") {
      val merge = (t: String) =>
        s"MERGE INTO $t t USING source s ON t.pk = s.pk WHEN MATCHED THEN UPDATE SET c_1 = s.v"
      analyze("analyze group-based MERGE", groupBasedTables)(merge)
      analyze("analyze delta-based MERGE", deltaBasedTables)(merge)
    }
  }
}
