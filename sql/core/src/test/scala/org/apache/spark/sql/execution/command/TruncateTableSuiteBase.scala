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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}

/**
 * This base suite contains unified tests for the `TRUNCATE TABLE` command that check V1 and V2
 * table catalogs. The tests that cannot run for all supported catalogs are located in more
 * specific test suites:
 *
 *   - V2 table catalog tests: `org.apache.spark.sql.execution.command.v2.TruncateTableSuite`
 *   - V1 table catalog tests: `org.apache.spark.sql.execution.command.v1.TruncateTableSuiteBase`
 *     - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.TruncateTableSuite`
 *     - V1 Hive External catalog: `org.apache.spark.sql.hive.execution.command.TruncateTableSuite`
 */
trait TruncateTableSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "TRUNCATE TABLE"

  test("table does not exist") {
    withNamespaceAndTable("ns", "does_not_exist") { t =>
      val errMsg = intercept[AnalysisException] {
        sql(s"TRUNCATE TABLE $t")
      }.getMessage
      assert(errMsg.contains("Table not found"))
    }
  }

  test("truncate non-partitioned table") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (c0 INT, c1 INT) $defaultUsing")
      sql(s"INSERT INTO $t SELECT 0, 1")

      sql(s"TRUNCATE TABLE $t")
      QueryTest.checkAnswer(sql(s"SELECT * FROM $t"), Nil)
    }
  }

  protected def createPartTable(t: String): Unit = {
    sql(s"""
      |CREATE TABLE $t (width INT, length INT, height INT)
      |$defaultUsing
      |PARTITIONED BY (width, length)""".stripMargin)
    sql(s"INSERT INTO $t PARTITION (width = 0, length = 0) SELECT 0")
    sql(s"INSERT INTO $t PARTITION (width = 1, length = 1) SELECT 1")
    sql(s"INSERT INTO $t PARTITION (width = 1, length = 2) SELECT 3")
  }

  test("SPARK-34418: preserve partitions in truncated table") {
    withNamespaceAndTable("ns", "partTable") { t =>
      createPartTable(t)
      checkAnswer(
        sql(s"SELECT width, length, height FROM $t"),
        Seq(Row(0, 0, 0), Row(1, 1, 1), Row(1, 2, 3)))
      sql(s"TRUNCATE TABLE $t")
      checkAnswer(sql(s"SELECT width, length, height FROM $t"), Nil)
      checkPartitions(t,
        Map("width" -> "0", "length" -> "0"),
        Map("width" -> "1", "length" -> "1"),
        Map("width" -> "1", "length" -> "2"))
    }
  }
}
