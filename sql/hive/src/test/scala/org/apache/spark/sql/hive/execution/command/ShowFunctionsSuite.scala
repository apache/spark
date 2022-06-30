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

package org.apache.spark.sql.hive.execution.command

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.command.v1
import org.apache.spark.sql.hive.execution.UDFToListInt

/**
 * The class contains tests for the `SHOW FUNCTIONS` command to check permanent functions.
 */
class ShowFunctionsSuite extends v1.ShowFunctionsSuiteBase with CommandSuiteBase {
  override def commandVersion: String = super[ShowFunctionsSuiteBase].commandVersion

  override protected def createFunction(name: String): Unit = {
    sql(s"CREATE FUNCTION $name AS '${classOf[UDFToListInt].getName}'")
  }
  override protected def dropFunction(name: String): Unit = {
    sql(s"DROP FUNCTION IF EXISTS $name")
  }

  test("show a function by its string name") {
    val testFuns = Seq("crc32i", "crc16j")
    withNamespaceAndFuns("ns", testFuns) { (ns, funs) =>
      assert(sql(s"SHOW USER FUNCTIONS IN $ns").isEmpty)
      funs.foreach(createFunction)
      checkAnswer(
        sql(s"SHOW USER FUNCTIONS IN $ns 'crc32i'"),
        Row(showFun("ns", "crc32i")))
    }
  }

  test("show functions matched to the '|' pattern") {
    val testFuns = Seq("crc32i", "crc16j", "date1900", "Date1")
    withNamespaceAndFuns("ns", testFuns) { (ns, funs) =>
      assert(sql(s"SHOW USER FUNCTIONS IN $ns").isEmpty)
      funs.foreach(createFunction)
      checkAnswer(
        sql(s"SHOW USER FUNCTIONS IN $ns LIKE 'crc32i|date1900'"),
        Seq("crc32i", "date1900").map(testFun => Row(showFun("ns", testFun))))
      checkAnswer(
        sql(s"SHOW USER FUNCTIONS IN $ns LIKE 'crc32i|date*'"),
        Seq("crc32i", "date1900", "Date1").map(testFun => Row(showFun("ns", testFun))))
    }
  }

  test("show a function by its id") {
    withNamespaceAndFun("ns", "crc32i") { (ns, fun) =>
      assert(sql(s"SHOW USER FUNCTIONS IN $ns").isEmpty)
      createFunction(fun)
      checkAnswer(
        sql(s"SHOW USER FUNCTIONS $fun"),
        Row(showFun("ns", "crc32i")))
    }
  }
}
