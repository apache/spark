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

import org.apache.spark.sql.execution.command.v1
import org.apache.spark.sql.internal.SQLConf

/**
 * The class contains tests for the `SHOW NAMESPACES` and `SHOW DATABASES` commands to check
 * V1 Hive external table catalog.
 */
class ShowNamespacesSuite extends v1.ShowNamespacesSuiteBase with CommandSuiteBase {
  override def commandVersion: String = "V2" // There is only V2 variant of SHOW NAMESPACES.

  test("case sensitivity") {
    Seq(true, false).foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        withNamespace(s"$catalog.AAA", s"$catalog.bbb") {
          sql(s"CREATE NAMESPACE $catalog.AAA")
          sql(s"CREATE NAMESPACE $catalog.bbb")
          runShowNamespacesSql(
            s"SHOW NAMESPACES IN $catalog",
            Seq("aaa", "bbb") ++ builtinTopNamespaces)
          runShowNamespacesSql(s"SHOW NAMESPACES IN $catalog LIKE 'AAA'", Seq("aaa"))
          runShowNamespacesSql(s"SHOW NAMESPACES IN $catalog LIKE 'aaa'", Seq("aaa"))
        }
      }
    }
  }

  test("hive client calls") {
    withNamespace(s"$catalog.ns1", s"$catalog.ns2") {
      sql(s"CREATE NAMESPACE $catalog.ns1")
      sql(s"CREATE NAMESPACE $catalog.ns2")

      checkHiveClientCalls(expected = 1) {
        sql(s"SHOW NAMESPACES IN $catalog")
      }
    }
  }
}
