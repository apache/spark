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
import org.apache.spark.sql.catalyst.plans.logical.ShowColumns

class ShowColumnsParserSuite extends AnalysisTest {

  test("show columns") {
    comparePlans(
      parsePlan("SHOW COLUMNS IN a.b.c"),
      ShowColumns(
        UnresolvedTableOrView(Seq("a", "b", "c"), "SHOW COLUMNS", allowTempView = true),
        None))
    comparePlans(
      parsePlan("SHOW COLUMNS FROM a.b.c"),
      ShowColumns(
        UnresolvedTableOrView(Seq("a", "b", "c"), "SHOW COLUMNS", allowTempView = true),
        None))
    comparePlans(
      parsePlan("SHOW COLUMNS IN a.b.c FROM a.b"),
      ShowColumns(UnresolvedTableOrView(Seq("a", "b", "c"), "SHOW COLUMNS", allowTempView = true),
        Some(Seq("a", "b"))))
    comparePlans(
      parsePlan("SHOW COLUMNS FROM a.b.c IN a.b"),
      ShowColumns(UnresolvedTableOrView(Seq("a", "b", "c"), "SHOW COLUMNS", allowTempView = true),
        Some(Seq("a", "b"))))
  }

  test("illegal characters in unquoted identifier") {
    checkError(
      exception = parseException(parsePlan)("SHOW COLUMNS IN t FROM test-db"),
      errorClass = "INVALID_IDENTIFIER",
      sqlState = "42602",
      parameters = Map("ident" -> "test-db")
    )
  }
}
