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

import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedNamespace}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.plans.logical.ShowFunctions

class ShowFunctionsParserSuite extends AnalysisTest {
  val nsPlan = UnresolvedNamespace(Nil)

  test("show functions in the scope") {
    comparePlans(
      parsePlan("SHOW FUNCTIONS"),
      ShowFunctions(nsPlan, true, true, None))
    comparePlans(
      parsePlan("SHOW USER FUNCTIONS"),
      ShowFunctions(nsPlan, true, false, None))
    comparePlans(
      parsePlan("SHOW user FUNCTIONS"),
      ShowFunctions(nsPlan, true, false, None))
    comparePlans(
      parsePlan("SHOW SYSTEM FUNCTIONS"),
      ShowFunctions(nsPlan, false, true, None))
    comparePlans(
      parsePlan("SHOW ALL FUNCTIONS"),
      ShowFunctions(nsPlan, true, true, None))
  }

  test("show functions matched to a pattern") {
    comparePlans(
      parsePlan("SHOW FUNCTIONS 'funct*'"),
      ShowFunctions(nsPlan, true, true, Some("funct*")))
    comparePlans(
      parsePlan("SHOW FUNCTIONS LIKE 'funct*'"),
      ShowFunctions(nsPlan, true, true, Some("funct*")))
    comparePlans(
      parsePlan("SHOW FUNCTIONS IN db LIKE 'funct*'"),
      ShowFunctions(UnresolvedNamespace(Seq("db")), true, true, Some("funct*")))
  }

  test("show functions using the legacy syntax") {
    comparePlans(
      parsePlan("SHOW FUNCTIONS a"),
      ShowFunctions(nsPlan, true, true, Some("a")))
    comparePlans(
      parsePlan("SHOW FUNCTIONS LIKE a"),
      ShowFunctions(nsPlan, true, true, Some("a")))
    comparePlans(
      parsePlan("SHOW FUNCTIONS LIKE a.b.c"),
      ShowFunctions(UnresolvedNamespace(Seq("a", "b")), true, true, Some("c")))
  }
}
