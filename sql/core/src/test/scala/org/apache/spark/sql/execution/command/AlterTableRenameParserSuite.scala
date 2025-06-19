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

import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedTableOrView}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.plans.logical.RenameTable

class AlterTableRenameParserSuite extends AnalysisTest {
  test("rename table") {
    comparePlans(
      parsePlan("ALTER TABLE a.b.c RENAME TO x.y.z"),
      RenameTable(
        UnresolvedTableOrView(Seq("a", "b", "c"), "ALTER TABLE ... RENAME TO", true),
        Seq("x", "y", "z"),
        isView = false))
  }

  test("case sensitivity") {
    comparePlans(
      parsePlan("alter Table spark_catalog.ns.tbl RENAME to tbl"),
      RenameTable(
        UnresolvedTableOrView(Seq("spark_catalog", "ns", "tbl"), "ALTER TABLE ... RENAME TO", true),
        Seq("tbl"),
        isView = false))
  }

  test("invalid table identifiers") {
    val sql1 = "ALTER TABLE RENAME TO x.y.z"
    checkError(
      exception = parseException(parsePlan)(sql1),
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'TO'", "hint" -> ""))

    val sql2 = "ALTER TABLE _ RENAME TO .z"
    checkError(
      exception = parseException(parsePlan)(sql2),
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'.'", "hint" -> ""))
  }
}
