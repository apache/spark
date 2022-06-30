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
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
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

  test("show a function") {
    withNamespaceAndFun("ns", "iiilog") { (ns, f) =>
      val totalFuns = sql(s"SHOW FUNCTIONS IN $ns").count()
      createFunction(f)
      assert(sql(s"SHOW FUNCTIONS IN $ns").count() - totalFuns === 1)
      assert(!sql(s"SHOW FUNCTIONS IN $ns").filter("contains(function, 'iiilog')").isEmpty)
    }
  }

  test("show a function in the USER name space") {
    withNamespaceAndFun("ns", "logiii") { (ns, f) =>
      assert(sql(s"SHOW USER FUNCTIONS IN $ns").count() === 0)
      createFunction(f)
      checkAnswer(sql(s"SHOW USER FUNCTIONS IN $ns"), Row(showFun("ns", "logiii")))
    }
  }

  test("look up functions in the SYSTEM name space") {
    withNamespaceAndFun("ns", "date_addi") { (ns, f) =>
      val systemFuns = sql(s"SHOW SYSTEM FUNCTIONS IN $ns").count()
      assert(systemFuns > 0)
      createFunction(f)
      assert(sql(s"SHOW SYSTEM FUNCTIONS IN $ns").count() === systemFuns)
    }
  }

  test("show functions among both user and system defined functions") {
    withNamespaceAndFun("ns", "current_datei") { (ns, f) =>
      val allFuns = sql(s"SHOW ALL FUNCTIONS IN $ns").collect()
      assert(allFuns.nonEmpty)
      createFunction(f)
      checkAnswer(
        sql(s"SHOW ALL FUNCTIONS IN $ns"),
        allFuns :+ Row(showFun("ns", "current_datei")))
    }
  }

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
}

/**
 * The class contains tests for the `SHOW FUNCTIONS` command to check V1 In-Memory
 * table catalog.
 */
class ShowFunctionsSuite extends ShowFunctionsSuiteBase with CommandSuiteBase {
  override def commandVersion: String = super[ShowFunctionsSuiteBase].commandVersion

  override protected def createFunction(name: String): Unit = {
    spark.udf.register(name, (arg1: Int, arg2: String) => arg2 + arg1)
  }

  override protected def dropFunction(name: String): Unit = {
    spark.sessionState.catalog.dropTempFunction(name, false)
  }

  override protected def showFun(ns: String, name: String): String = s"$catalog.$ns.$name"
}
