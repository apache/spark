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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, CurrentNamespace, UnresolvedNamespace, UnresolvedPartitionSpec, UnresolvedTable}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.plans.logical.{ShowTablePartition, ShowTables, ShowTablesExtended}
import org.apache.spark.sql.execution.command.ShowTablesJsonCommand
import org.apache.spark.sql.test.SharedSparkSession

class ShowTablesParserSuite extends AnalysisTest with SharedSparkSession {
  private val catalog = "test_catalog"

  test("show tables") {
    comparePlans(
      parsePlan("SHOW TABLES"),
      ShowTables(CurrentNamespace, None))
    comparePlans(
      parsePlan("SHOW TABLES '*test*'"),
      ShowTables(CurrentNamespace, Some("*test*")))
    comparePlans(
      parsePlan("SHOW TABLES LIKE '*test*'"),
      ShowTables(CurrentNamespace, Some("*test*")))
    comparePlans(
      parsePlan(s"SHOW TABLES FROM $catalog.ns1.ns2.tbl"),
      ShowTables(UnresolvedNamespace(Seq(catalog, "ns1", "ns2", "tbl")), None))
    comparePlans(
      parsePlan(s"SHOW TABLES IN $catalog.ns1.ns2.tbl"),
      ShowTables(UnresolvedNamespace(Seq(catalog, "ns1", "ns2", "tbl")), None))
    comparePlans(
      parsePlan("SHOW TABLES IN ns1 '*test*'"),
      ShowTables(UnresolvedNamespace(Seq("ns1")), Some("*test*")))
    comparePlans(
      parsePlan("SHOW TABLES IN ns1 LIKE '*test*'"),
      ShowTables(UnresolvedNamespace(Seq("ns1")), Some("*test*")))
  }

  test("show tables as json") {
    val parse = spark.sessionState.sqlParser.parsePlan _
    comparePlans(
      parse("SHOW TABLES AS JSON"),
      ShowTablesJsonCommand(CurrentNamespace, None, isExtended = false))
    comparePlans(
      parse("SHOW TABLES IN ns1 AS JSON"),
      ShowTablesJsonCommand(UnresolvedNamespace(Seq("ns1")), None, isExtended = false))
    comparePlans(
      parse("SHOW TABLES IN ns1 LIKE '*test*' AS JSON"),
      ShowTablesJsonCommand(UnresolvedNamespace(Seq("ns1")), Some("*test*"), isExtended = false))
  }

  test("show table extended") {
    comparePlans(
      parsePlan("SHOW TABLE EXTENDED LIKE '*test*'"),
      ShowTablesExtended(CurrentNamespace, "*test*"))
    comparePlans(
      parsePlan(s"SHOW TABLE EXTENDED FROM $catalog.ns1.ns2 LIKE '*test*'"),
      ShowTablesExtended(UnresolvedNamespace(Seq(catalog, "ns1", "ns2")), "*test*"))
    comparePlans(
      parsePlan(s"SHOW TABLE EXTENDED IN $catalog.ns1.ns2 LIKE '*test*'"),
      ShowTablesExtended(UnresolvedNamespace(Seq(catalog, "ns1", "ns2")), "*test*"))

    comparePlans(
      parsePlan("SHOW TABLE EXTENDED LIKE '*test*' PARTITION(ds='2008-04-09', hr=11)"),
      ShowTablePartition(
        UnresolvedTable(Seq("*test*"), "SHOW TABLE EXTENDED ... PARTITION ..."),
        UnresolvedPartitionSpec(Map("ds" -> "2008-04-09", "hr" -> "11"))))
    comparePlans(
      parsePlan(s"SHOW TABLE EXTENDED FROM $catalog.ns1.ns2 LIKE '*test*' " +
        "PARTITION(ds='2008-04-09')"),
      ShowTablePartition(
        UnresolvedTable(Seq(catalog, "ns1", "ns2", "*test*"),
          "SHOW TABLE EXTENDED ... PARTITION ..."),
        UnresolvedPartitionSpec(Map("ds" -> "2008-04-09"))))
    comparePlans(
      parsePlan(s"SHOW TABLE EXTENDED IN $catalog.ns1.ns2 LIKE '*test*' " +
        "PARTITION(ds='2008-04-09')"),
      ShowTablePartition(
        UnresolvedTable(Seq(catalog, "ns1", "ns2", "*test*"),
          "SHOW TABLE EXTENDED ... PARTITION ..."),
        UnresolvedPartitionSpec(Map("ds" -> "2008-04-09"))))
  }

  test("show table extended as json") {
    val parse = spark.sessionState.sqlParser.parsePlan _
    comparePlans(
      parse("SHOW TABLE EXTENDED LIKE '*test*' AS JSON"),
      ShowTablesJsonCommand(CurrentNamespace, Some("*test*"), isExtended = true))
    comparePlans(
      parse(s"SHOW TABLE EXTENDED IN $catalog.ns1.ns2 LIKE '*test*' AS JSON"),
      ShowTablesJsonCommand(
        UnresolvedNamespace(Seq(catalog, "ns1", "ns2")), Some("*test*"), isExtended = true))
  }

  test("show table extended as json with partition should fail") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.sessionState.sqlParser.parsePlan(
          "SHOW TABLE EXTENDED LIKE '*test*' PARTITION(ds='2008-04-09') AS JSON")
      },
      condition = "UNSUPPORTED_FEATURE.SHOW_TABLE_EXTENDED_JSON_WITH_PARTITION",
      parameters = Map.empty
    )
  }
}
