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

import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedFieldName, UnresolvedTable}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.plans.logical.RenameColumn
import org.apache.spark.sql.test.SharedSparkSession

class AlterTableRenameColumnParserSuite extends AnalysisTest with SharedSparkSession {

  test("alter table: rename column") {
    comparePlans(
      parsePlan("ALTER TABLE table_name RENAME COLUMN a.b.c TO d"),
      RenameColumn(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... RENAME COLUMN"),
        UnresolvedFieldName(Seq("a", "b", "c")),
        "d"))
  }

  test("alter table: rename column - hyphen in identifier") {
    checkError(
      exception = parseException(parsePlan)(
        "ALTER TABLE t RENAME COLUMN test-col TO test"),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-col"))
    checkError(
      exception = parseException(parsePlan)(
        "ALTER TABLE t RENAME COLUMN test TO test-col"),
      condition = "INVALID_IDENTIFIER",
      parameters = Map("ident" -> "test-col"))
  }

  test("alter table: rename nested column") {
    checkError(
      exception = parseException(parsePlan)(
        "ALTER TABLE t RENAME COLUMN point.x to point.y"),
      condition = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'.'", "hint" -> ""))
  }
}
