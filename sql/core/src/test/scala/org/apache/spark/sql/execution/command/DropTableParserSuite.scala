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

import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedIdentifier}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.plans.logical.{DropTable, LogicalPlan}
import org.apache.spark.sql.test.SharedSparkSession

class DropTableParserSuite extends AnalysisTest with SharedSparkSession {
  private def parseCompare(sql: String, expected: LogicalPlan): Unit = {
    comparePlans(parsePlan(sql), expected, checkAnalysis = false)
  }

  test("drop table") {
    parseCompare("DROP TABLE testcat.ns1.ns2.tbl",
      DropTable(UnresolvedIdentifier(Seq("testcat", "ns1", "ns2", "tbl"), true),
        ifExists = false,
        purge = false))
    parseCompare(s"DROP TABLE db.tab",
      DropTable(
        UnresolvedIdentifier(Seq("db", "tab"), true),
        ifExists = false,
        purge = false))
    parseCompare(s"DROP TABLE IF EXISTS db.tab",
      DropTable(
        UnresolvedIdentifier(Seq("db", "tab"), true),
        ifExists = true,
        purge = false))
    parseCompare(s"DROP TABLE tab",
      DropTable(UnresolvedIdentifier(Seq("tab"), true), ifExists = false, purge = false))
    parseCompare(s"DROP TABLE IF EXISTS tab",
      DropTable(UnresolvedIdentifier(Seq("tab"), true), ifExists = true, purge = false))
    parseCompare(s"DROP TABLE tab PURGE",
      DropTable(UnresolvedIdentifier(Seq("tab"), true), ifExists = false, purge = true))
    parseCompare(s"DROP TABLE IF EXISTS tab PURGE",
      DropTable(UnresolvedIdentifier(Seq("tab"), true), ifExists = true, purge = true))
  }
}
