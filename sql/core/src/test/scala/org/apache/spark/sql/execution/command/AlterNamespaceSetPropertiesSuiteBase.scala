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

import org.apache.spark.sql.{AnalysisException, QueryTest}

/**
 * This base suite contains unified tests for the `ALTER NAMESPACE ... SET PROPERTIES` command that
 * check V1 and V2 table catalogs. The tests that cannot run for all supported catalogs are located
 * in more specific test suites:
 *
 *   - V2 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v2.AlterNamespaceSetPropertiesSuite`
 *   - V1 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v1.AlterNamespaceSetPropertiesSuiteBase`
 *     - V1 In-Memory catalog:
 *       `org.apache.spark.sql.execution.command.v1.AlterNamespaceSetPropertiesSuite`
 *     - V1 Hive External catalog:
 *        `org.apache.spark.sql.hive.execution.command.AlterNamespaceSetPropertiesSuite`
 */
trait AlterNamespaceSetPropertiesSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "ALTER NAMESPACE ... SET PROPERTIES"

  protected def namespace: String

  protected def notFoundMsgPrefix: String

  test("Namespace does not exist") {
    val ns = "not_exist"
    val message = intercept[AnalysisException] {
      sql(s"ALTER DATABASE $catalog.$ns SET PROPERTIES ('d'='d')")
    }.getMessage
    assert(message.contains(s"$notFoundMsgPrefix '$ns' not found"))
  }

  test("basic test") {
    val ns = s"$catalog.$namespace"
    withNamespace(ns) {
      sql(s"CREATE NAMESPACE $ns")
      sql(s"ALTER NAMESPACE $ns SET PROPERTIES ('a'='a', 'b'='b', 'c'='c')")
      assert(getProperties(ns) === "((a,a), (b,b), (c,c))")
      sql(s"ALTER DATABASE $ns SET PROPERTIES ('d'='d')")
      assert(getProperties(ns) === "((a,a), (b,b), (c,c), (d,d))")
    }
  }

  protected def getProperties(namespace: String): String = {
    val locationRow = sql(s"DESCRIBE NAMESPACE EXTENDED $namespace")
      .toDF("key", "value")
      .where(s"key like 'Properties%'")
      .collect()
    assert(locationRow.length == 1)
    locationRow(0).getString(1)
  }
}
