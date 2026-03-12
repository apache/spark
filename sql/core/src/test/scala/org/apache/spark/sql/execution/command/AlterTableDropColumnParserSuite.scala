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
import org.apache.spark.sql.catalyst.plans.logical.DropColumns
import org.apache.spark.sql.test.SharedSparkSession

class AlterTableDropColumnParserSuite extends AnalysisTest with SharedSparkSession {

  test("alter table: drop column") {
    comparePlans(
      parsePlan("ALTER TABLE table_name DROP COLUMN a.b.c"),
      DropColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... DROP COLUMNS"),
        Seq(UnresolvedFieldName(Seq("a", "b", "c"))),
        ifExists = false))

    comparePlans(
      parsePlan("ALTER TABLE table_name DROP COLUMN IF EXISTS a.b.c"),
      DropColumns(
        UnresolvedTable(Seq("table_name"), "ALTER TABLE ... DROP COLUMNS"),
        Seq(UnresolvedFieldName(Seq("a", "b", "c"))),
        ifExists = true))
  }

  test("alter table: drop multiple columns") {
    val sql = "ALTER TABLE table_name DROP COLUMN x, y, a.b.c"
    Seq(sql, sql.replace("COLUMN", "COLUMNS")).foreach { drop =>
      comparePlans(
        parsePlan(drop),
        DropColumns(
          UnresolvedTable(Seq("table_name"), "ALTER TABLE ... DROP COLUMNS"),
          Seq(UnresolvedFieldName(Seq("x")),
            UnresolvedFieldName(Seq("y")),
            UnresolvedFieldName(Seq("a", "b", "c"))),
          ifExists = false))
    }

    val sqlIfExists = "ALTER TABLE table_name DROP COLUMN IF EXISTS x, y, a.b.c"
    Seq(sqlIfExists, sqlIfExists.replace("COLUMN", "COLUMNS")).foreach { drop =>
      comparePlans(
        parsePlan(drop),
        DropColumns(
          UnresolvedTable(Seq("table_name"), "ALTER TABLE ... DROP COLUMNS"),
          Seq(UnresolvedFieldName(Seq("x")),
            UnresolvedFieldName(Seq("y")),
            UnresolvedFieldName(Seq("a", "b", "c"))),
          ifExists = true))
    }
  }
}
