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

import org.apache.spark.sql.catalyst.TableIdentifier

trait TPCDSBase extends TPCBase with TPCDSSchema {

  // The TPCDS queries below are based on v1.4
  private val tpcdsAllQueries: Seq[String] = Seq(
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

  protected def excludedTpcdsQueries: Set[String] = if (regenerateGoldenFiles) {
    Set()
  } else {
    // Since `tpcdsQueriesV2_7_0` has almost the same queries with these ones below,
    // we skip them in the TPCDS-related tests.
    // NOTE: q6" and "q75" can cause flaky test results, so we must exclude them.
    // For more details, see SPARK-35327.
    Set("q6", "q34", "q64", "q74", "q75", "q78")
  }

  val tpcdsQueries: Seq[String] = tpcdsAllQueries.filterNot(excludedTpcdsQueries.contains)

  // This list only includes TPCDS v2.7 queries that are different from v1.4 ones
  val tpcdsQueriesV2_7_0 = Seq(
    "q5a", "q6", "q10a", "q11", "q12", "q14", "q14a", "q18a",
    "q20", "q22", "q22a", "q24", "q27a", "q34", "q35", "q35a", "q36a", "q47", "q49",
    "q51a", "q57", "q64", "q67a", "q70a", "q72", "q74", "q75", "q77a", "q78",
    "q80a", "q86a", "q98")

  // These queries are from https://github.com/cloudera/impala-tpcds-kit/tree/master/queries
  val modifiedTPCDSQueries = Seq(
    "q3", "q7", "q10", "q19", "q27", "q34", "q42", "q43", "q46", "q52", "q53", "q55", "q59",
    "q63", "q65", "q68", "q73", "q79", "q89", "q98", "ss_max")

  protected def partitionedByClause(tableName: String): String = {
    tablePartitionColumns.get(tableName) match {
      case Some(cols) if cols.nonEmpty => s"PARTITIONED BY (${cols.mkString(", ")})"
      case _ => ""
    }
  }

  val tableNames: Iterable[String] = tableColumns.keys

  def createTable(
      spark: SparkSession,
      tableName: String,
      format: String = "parquet",
      options: Seq[String] = Nil): Unit = {
    spark.sql(
      s"""
         |CREATE TABLE `$tableName` (${tableColumns(tableName)})
         |USING $format
         |${partitionedByClause(tableName)}
         |${options.mkString("\n")}
       """.stripMargin)
  }

  override def createTables(): Unit = {
    tableNames.foreach { tableName =>
      createTable(spark, tableName)
      if (injectStats) {
        // To simulate plan generation on actual TPC-DS data, injects data stats here
        spark.sessionState.catalog.alterTableStats(
          TableIdentifier(tableName), Some(TPCDSTableStats.sf100TableStats(tableName)))
      }
    }
  }

  override def dropTables(): Unit = {
    tableNames.foreach { tableName =>
      spark.sessionState.catalog.dropTable(TableIdentifier(tableName), true, true)
    }
  }
}
