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

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.connector.catalog.SupportsNamespaces

/**
 * This base suite contains unified tests for the `ALTER NAMESPACE ... SET LOCATION` command that
 * check V1 and V2 table catalogs. The tests that cannot run for all supported catalogs are located
 * in more specific test suites:
 *
 *   - V2 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v2.AlterNamespaceSetLocationSuite`
 *   - V1 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v1.AlterNamespaceSetLocationSuiteBase`
 *     - V1 In-Memory catalog:
 *       `org.apache.spark.sql.execution.command.v1.AlterNamespaceSetLocationSuite`
 *     - V1 Hive External catalog:
 *        `org.apache.spark.sql.hive.execution.command.AlterNamespaceSetLocationSuite`
 */
trait AlterNamespaceSetLocationSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "ALTER NAMESPACE ... SET LOCATION"

  protected def namespace: String

  protected def notFoundMsgPrefix: String

  test("Empty location string") {
    val ns = s"$catalog.$namespace"
    withNamespace(ns) {
      sql(s"CREATE NAMESPACE $ns")
      val sqlText = s"ALTER NAMESPACE $ns SET LOCATION ''"
      checkError(
        exception = intercept[SparkIllegalArgumentException] {
          sql(sqlText)
        },
        condition = "INVALID_EMPTY_LOCATION",
        parameters = Map("location" -> ""))
    }
  }

  test("Namespace does not exist") {
    val ns = "not_exist"
    val e = intercept[AnalysisException] {
      sql(s"ALTER DATABASE $catalog.$ns SET LOCATION 'loc'")
    }
    checkError(e,
      condition = "SCHEMA_NOT_FOUND",
      parameters = Map("schemaName" -> s"`$catalog`.`$ns`"))
  }

  // Hive catalog does not support "ALTER NAMESPACE ... SET LOCATION", thus
  // this is called from non-Hive v1 and v2 tests.
  protected def runBasicTest(): Unit = {
    val ns = s"$catalog.$namespace"
    withNamespace(ns) {
      sql(s"CREATE NAMESPACE IF NOT EXISTS $ns COMMENT " +
        "'test namespace' LOCATION '/tmp/loc_test_1'")
      sql(s"ALTER NAMESPACE $ns SET LOCATION '/tmp/loc_test_2'")
      assert(getLocation(ns).contains("file:/tmp/loc_test_2"))
    }
  }

  protected def getLocation(namespace: String): String = {
    val locationRow = sql(s"DESCRIBE NAMESPACE EXTENDED $namespace")
      .toDF("key", "value")
      .where(s"key like '${SupportsNamespaces.PROP_LOCATION.capitalize}%'")
      .collect()
    assert(locationRow.length == 1)
    locationRow(0).getString(1)
  }
}
