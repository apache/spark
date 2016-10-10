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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.{DescribeFunctionCommand, DescribeTableCommand,
  ShowFunctionsCommand}
import org.apache.spark.sql.internal.SQLConf

/**
 * Parser test cases for rules defined in [[SparkSqlParser]].
 *
 * See [[org.apache.spark.sql.catalyst.parser.PlanParserSuite]] for rules
 * defined in the Catalyst module.
 */
class SparkSqlParserSuite extends PlanTest {

  private lazy val parser = new SparkSqlParser(new SQLConf)

  private def assertEqual(sqlCommand: String, plan: LogicalPlan): Unit = {
    comparePlans(parser.parsePlan(sqlCommand), plan)
  }

  private def intercept(sqlCommand: String, messages: String*): Unit = {
    val e = intercept[ParseException](parser.parsePlan(sqlCommand))
    messages.foreach { message =>
      assert(e.message.contains(message))
    }
  }

  test("show functions") {
    assertEqual("show functions", ShowFunctionsCommand(None, None, true, true))
    assertEqual("show all functions", ShowFunctionsCommand(None, None, true, true))
    assertEqual("show user functions", ShowFunctionsCommand(None, None, true, false))
    assertEqual("show system functions", ShowFunctionsCommand(None, None, false, true))
    intercept("show special functions", "SHOW special FUNCTIONS")
    assertEqual("show functions foo",
      ShowFunctionsCommand(None, Some("foo"), true, true))
    assertEqual("show functions foo.bar",
      ShowFunctionsCommand(Some("foo"), Some("bar"), true, true))
    assertEqual("show functions 'foo\\\\.*'",
      ShowFunctionsCommand(None, Some("foo\\.*"), true, true))
    intercept("show functions foo.bar.baz", "Unsupported function name")
  }

  test("describe function") {
    assertEqual("describe function bar",
      DescribeFunctionCommand(FunctionIdentifier("bar", database = None), isExtended = false))
    assertEqual("describe function extended bar",
      DescribeFunctionCommand(FunctionIdentifier("bar", database = None), isExtended = true))
    assertEqual("describe function foo.bar",
      DescribeFunctionCommand(
        FunctionIdentifier("bar", database = Option("foo")), isExtended = false))
    assertEqual("describe function extended f.bar",
      DescribeFunctionCommand(FunctionIdentifier("bar", database = Option("f")), isExtended = true))
  }

  test("SPARK-17328 Fix NPE with EXPLAIN DESCRIBE TABLE") {
    assertEqual("describe table t",
      DescribeTableCommand(
        TableIdentifier("t"), Map.empty, isExtended = false, isFormatted = false))
    assertEqual("describe table extended t",
      DescribeTableCommand(
        TableIdentifier("t"), Map.empty, isExtended = true, isFormatted = false))
    assertEqual("describe table formatted t",
      DescribeTableCommand(
        TableIdentifier("t"), Map.empty, isExtended = false, isFormatted = true))

    intercept("explain describe tables x", "Unsupported SQL statement")
  }
}
