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

import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedTable}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.plans.logical.RecoverPartitions
import org.apache.spark.sql.test.SharedSparkSession

class AlterTableRecoverPartitionsParserSuite extends AnalysisTest with SharedSparkSession {

  test("recover partitions without table") {
    val sql = "ALTER TABLE RECOVER PARTITIONS"
    checkError(
      exception = parseException(parsePlan)(sql),
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'PARTITIONS'", "hint" -> ""))
  }

  test("recover partitions of a table") {
    comparePlans(
      parsePlan("ALTER TABLE tbl RECOVER PARTITIONS"),
      RecoverPartitions(
        UnresolvedTable(
          Seq("tbl"),
          "ALTER TABLE ... RECOVER PARTITIONS",
          Some("Please use ALTER VIEW instead."))))
  }

  test("recover partitions of a table in a database") {
    comparePlans(
      parsePlan("alter table db.tbl recover partitions"),
      RecoverPartitions(
        UnresolvedTable(
          Seq("db", "tbl"),
          "ALTER TABLE ... RECOVER PARTITIONS",
          Some("Please use ALTER VIEW instead."))))
  }

  test("recover partitions of a table spark_catalog") {
    comparePlans(
      parsePlan("alter table spark_catalog.db.TBL recover partitions"),
      RecoverPartitions(
        UnresolvedTable(
          Seq("spark_catalog", "db", "TBL"),
          "ALTER TABLE ... RECOVER PARTITIONS",
          Some("Please use ALTER VIEW instead."))))
  }

  test("recover partitions of a table in nested namespaces") {
    comparePlans(
      parsePlan("Alter Table ns1.ns2.ns3.ns4.ns5.ns6.ns7.ns8.t Recover Partitions"),
      RecoverPartitions(
        UnresolvedTable(
          Seq("ns1", "ns2", "ns3", "ns4", "ns5", "ns6", "ns7", "ns8", "t"),
          "ALTER TABLE ... RECOVER PARTITIONS",
          Some("Please use ALTER VIEW instead."))))
  }
}
