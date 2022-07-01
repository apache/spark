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

import java.util.Locale

import org.apache.spark.sql.QueryTest
import org.apache.spark.util.Utils

/**
 * This base suite contains unified tests for the `SHOW FUNCTIONS` command that check V1 and V2
 * table catalogs. The tests that cannot run for all supported catalogs are located in more
 * specific test suites:
 *
 *   - V2 catalog tests: `org.apache.spark.sql.execution.command.v2.ShowFunctionsSuite`
 *   - V1 catalog tests:
 *     `org.apache.spark.sql.execution.command.v1.ShowFunctionsSuiteBase`
 *     - Temporary functions:
 *        `org.apache.spark.sql.execution.command.v1.ShowTempFunctionsSuite`
 *     - Permanent functions:
 *        `org.apache.spark.sql.hive.execution.command.ShowFunctionsSuite`
 */
trait ShowFunctionsSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "SHOW FUNCTIONS"

  protected def createFunction(name: String): Unit = {}
  protected def dropFunction(name: String): Unit = {}
  protected def showFun(ns: String, name: String): String = s"$ns.$name".toLowerCase(Locale.ROOT)

  /**
   * Drops function `funName` after calling `f`.
   */
  protected def withFunction(functionNames: String*)(f: => Unit): Unit = {
    Utils.tryWithSafeFinally(f) {
      functionNames.foreach(dropFunction)
    }
  }

  protected def withNamespaceAndFuns(ns: String, funNames: Seq[String], cat: String = catalog)
      (f: (String, Seq[String]) => Unit): Unit = {
    val nsCat = s"$cat.$ns"
    withNamespace(nsCat) {
      sql(s"CREATE NAMESPACE $nsCat")
      val nsCatFns = funNames.map(funName => s"$nsCat.$funName")
      withFunction(nsCatFns: _*) {
        f(nsCat, nsCatFns)
      }
    }
  }

  protected def withNamespaceAndFun(ns: String, funName: String, cat: String = catalog)
      (f: (String, String) => Unit): Unit = {
    withNamespaceAndFuns(ns, Seq(funName), cat) { case (ns, Seq(name)) =>
      f(ns, name)
    }
  }
}
