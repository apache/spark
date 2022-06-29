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

package org.apache.spark.sql.execution.command.v1

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TableFunctionRegistry}
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.execution.command

/**
 * This base suite contains unified tests for the `SHOW FUNCTIONS` command that checks V1
 * table catalogs. The tests that cannot run for all V1 catalogs are located in more
 * specific test suites:
 *
 *   - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.ShowFunctionsSuite`
 *   - V1 Hive External catalog:
 *     `org.apache.spark.sql.hive.execution.command.ShowFunctionsSuite`
 */
trait ShowFunctionsSuiteBase extends command.ShowFunctionsSuiteBase
  with command.TestsV1AndV2Commands {

  test("show functions in the default database of the session catalog") {
    def getFunctions(pattern: String): Seq[Row] = {
      StringUtils.filterPattern(
        spark.sessionState.catalog.listFunctions("default").map(_._1.funcName)
          ++ FunctionRegistry.builtinOperators.keys, pattern)
        .map(Row(_))
    }

    def createFunction(names: Seq[String]): Unit = {
      names.foreach { name =>
        spark.udf.register(name, (arg1: Int, arg2: String) => arg2 + arg1)
      }
    }

    def dropFunction(names: Seq[String]): Unit = {
      names.foreach { name =>
        spark.sessionState.catalog.dropTempFunction(name, false)
      }
    }

    val functions = Array("ilog", "logi", "logii", "logiii", "crc32i", "cubei", "cume_disti",
      "isize", "ispace", "to_datei", "date_addi", "current_datei")

    createFunction(functions)

    checkAnswer(sql("SHOW functions"), getFunctions("*"))
    assert(sql("SHOW functions").collect().size > 200)

    Seq("^c*", "*e$", "log*", "*date*").foreach { pattern =>
      // For the pattern part, only '*' and '|' are allowed as wildcards.
      // For '*', we need to replace it to '.*'.
      checkAnswer(sql(s"SHOW FUNCTIONS '$pattern'"), getFunctions(pattern))
    }
    dropFunction(functions)
  }

  test("show functions in a scope") {
    withUserDefinedFunction("add_one" -> true) {
      val numFunctions = FunctionRegistry.functionSet.size.toLong +
        TableFunctionRegistry.functionSet.size.toLong +
        FunctionRegistry.builtinOperators.size.toLong
      assert(sql("show functions").count() === numFunctions)
      assert(sql("show system functions").count() === numFunctions)
      assert(sql("show all functions").count() === numFunctions)
      assert(sql("show user functions").count() === 0L)
      spark.udf.register("add_one", (x: Long) => x + 1)
      assert(sql("show functions").count() === numFunctions + 1L)
      assert(sql("show system functions").count() === numFunctions)
      assert(sql("show all functions").count() === numFunctions + 1L)
      assert(sql("show user functions").count() === 1L)
    }
  }
}

/**
 * The class contains tests for the `SHOW FUNCTIONS` command to check V1 In-Memory
 * table catalog.
 */
class ShowFunctionsSuite extends ShowFunctionsSuiteBase with CommandSuiteBase {
  override def commandVersion: String = super[ShowFunctionsSuiteBase].commandVersion
}
