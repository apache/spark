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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * This base suite contains unified tests for the `SHOW FUNCTIONS` command that check V1 and V2
 * table catalogs. The tests that cannot run for all supported catalogs are located in more
 * specific test suites:
 *
 *   - V2 catalog tests: `org.apache.spark.sql.execution.command.v2.ShowFunctionsSuite`
 *   - V1 catalog tests: `org.apache.spark.sql.execution.command.v1.ShowFunctionsSuiteBase`
 *     - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.ShowFunctionsSuite`
 *     - V1 Hive External catalog: `org.apache.spark.sql.hive.execution.command.ShowFunctionsSuite`
 */
trait ShowFunctionsSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "SHOW FUNCTIONS"

  protected def funCatalog: String = catalog
  protected def createFunction(name: String): Unit
  protected def dropFunction(name: String): Unit
  protected def qualifiedFunName(ns: String, name: String): String = {
    Seq(funCatalog, ns, name).quoted
  }

  /**
   * Drops function `funName` after calling `f`.
   */
  protected def withFunction(functionNames: String*)(f: => Unit): Unit = {
    Utils.tryWithSafeFinally(f) {
      functionNames.foreach(dropFunction)
    }
  }

  protected def withNamespaceAndFuns(ns: String, funNames: Seq[String])
      (f: (String, Seq[String]) => Unit): Unit = {
    val nsCat = s"$funCatalog.$ns"
    withNamespace(nsCat) {
      sql(s"CREATE NAMESPACE $nsCat")
      val nsCatFns = funNames.map(funName => s"$nsCat.$funName")
      withFunction(nsCatFns: _*) {
        f(nsCat, nsCatFns)
      }
    }
  }

  protected def withNamespaceAndFun(ns: String, funName: String)
      (f: (String, String) => Unit): Unit = {
    withNamespaceAndFuns(ns, Seq(funName)) { case (ns, Seq(name)) =>
      f(ns, name)
    }
  }

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
      QueryTest.checkAnswer(
        sql(s"SHOW USER FUNCTIONS IN $ns"),
        Row(qualifiedFunName("ns", "logiii")) :: Nil)
    }
  }

  test("show a temporary function as an USER function") {
    withNamespaceAndFun("ns", "poggi") { (ns, f0) =>
      createFunction(f0)
      val f1 = "temp_test_fun"
      withUserDefinedFunction(f1 -> true) {
        spark.udf.register(f1, (arg1: Int, arg2: String) => arg2 + arg1)
        QueryTest.checkAnswer(
          sql(s"SHOW USER FUNCTIONS IN $ns"),
          Row(qualifiedFunName("ns", "poggi")) :: Row(f1) :: Nil)
        QueryTest.checkAnswer(
          sql(s"SHOW ALL FUNCTIONS IN $ns").filter(s"function='$f1'"),
          Row(f1) :: Nil)
        QueryTest.checkAnswer(
          sql(s"SHOW SYSTEM FUNCTIONS IN $ns").filter(s"function='$f1'"),
          Nil)
      }
    }
  }

  test("show functions in the SYSTEM name space") {
    withNamespaceAndFun("ns", "date_addi") { (ns, f) =>
      val systemFuns = sql(s"SHOW SYSTEM FUNCTIONS IN $ns")
      assert(systemFuns.count() > 0)
      createFunction(f)
      assert(sql(s"SHOW SYSTEM FUNCTIONS IN $ns").count() === systemFuns.count())
      // Built-in operators
      assert(!systemFuns.filter("function='case'").isEmpty)
      // Built-in functions
      assert(!systemFuns.filter("function='substring'").isEmpty)
    }
  }

  test("show functions among both user and system defined functions") {
    withNamespaceAndFun("ns", "current_datei") { (ns, f) =>
      val allFuns = sql(s"SHOW ALL FUNCTIONS IN $ns").collect()
      assert(allFuns.nonEmpty)
      createFunction(f)
      QueryTest.checkAnswer(
        sql(s"SHOW ALL FUNCTIONS IN $ns"),
        (allFuns :+ Row(qualifiedFunName("ns", "current_datei"))).toImmutableArraySeq)
    }
  }

  test("show functions matched to the wildcard pattern") {
    val testFuns = Seq("crc32i", "crc16j", "date1900", "Date1")
    withNamespaceAndFuns("ns", testFuns) { (ns, funs) =>
      assert(sql(s"SHOW USER FUNCTIONS IN $ns").isEmpty)
      funs.foreach(createFunction)
      QueryTest.checkAnswer(
        sql(s"SHOW USER FUNCTIONS IN $ns LIKE '*'"),
        testFuns.map(testFun => Row(qualifiedFunName("ns", testFun))))
      QueryTest.checkAnswer(
        sql(s"SHOW USER FUNCTIONS IN $ns LIKE '*rc*'"),
        Seq("crc32i", "crc16j").map(testFun => Row(qualifiedFunName("ns", testFun))))
    }
  }

  test("show a function by its string name") {
    val testFuns = Seq("crc32i", "crc16j")
    withNamespaceAndFuns("ns", testFuns) { (ns, funs) =>
      assert(sql(s"SHOW USER FUNCTIONS IN $ns").isEmpty)
      funs.foreach(createFunction)
      QueryTest.checkAnswer(
        sql(s"SHOW USER FUNCTIONS IN $ns 'crc32i'"),
        Row(qualifiedFunName("ns", "crc32i")) :: Nil)
    }
  }

  test("show functions matched to the '|' pattern") {
    val testFuns = Seq("crc32i", "crc16j", "date1900", "Date1")
    withNamespaceAndFuns("ns", testFuns) { (ns, funs) =>
      assert(sql(s"SHOW USER FUNCTIONS IN $ns").isEmpty)
      funs.foreach(createFunction)
      QueryTest.checkAnswer(
        sql(s"SHOW USER FUNCTIONS IN $ns LIKE 'crc32i|date1900'"),
        Seq("crc32i", "date1900").map(testFun => Row(qualifiedFunName("ns", testFun))))
      QueryTest.checkAnswer(
        sql(s"SHOW USER FUNCTIONS IN $ns LIKE 'crc32i|date*'"),
        Seq("crc32i", "date1900", "Date1").map(testFun => Row(qualifiedFunName("ns", testFun))))
    }
  }
}
