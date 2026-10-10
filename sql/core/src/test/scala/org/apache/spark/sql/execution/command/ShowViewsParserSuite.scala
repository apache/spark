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

import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, CurrentNamespace, UnresolvedNamespace}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Parser tests for the `AS JSON` variant of `SHOW VIEWS`. The plain variant is covered by
 * `DDLParserSuite`; `AS JSON` is parsed by `SparkSqlAstBuilder`, so it needs a session.
 */
class ShowViewsParserSuite extends AnalysisTest with SharedSparkSession {
  private val catalog = "test_catalog"

  test("show views as json") {
    val parse = spark.sessionState.sqlParser.parsePlan _
    comparePlans(
      parse("SHOW VIEWS AS JSON"),
      ShowViewsJsonCommand(CurrentNamespace, None))
    comparePlans(
      parse("SHOW VIEWS IN ns1 AS JSON"),
      ShowViewsJsonCommand(UnresolvedNamespace(Seq("ns1")), None))
    comparePlans(
      parse(s"SHOW VIEWS FROM $catalog.ns1.ns2 AS JSON"),
      ShowViewsJsonCommand(UnresolvedNamespace(Seq(catalog, "ns1", "ns2")), None))
    comparePlans(
      parse("SHOW VIEWS IN ns1 '*test*' AS JSON"),
      ShowViewsJsonCommand(UnresolvedNamespace(Seq("ns1")), Some("*test*")))
    comparePlans(
      parse("SHOW VIEWS IN ns1 LIKE '*test*' AS JSON"),
      ShowViewsJsonCommand(UnresolvedNamespace(Seq("ns1")), Some("*test*")))
  }
}
