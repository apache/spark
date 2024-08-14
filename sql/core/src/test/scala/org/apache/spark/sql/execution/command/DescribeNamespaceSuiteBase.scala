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
import org.apache.spark.sql.internal.SQLConf

/**
 * This base suite contains unified tests for the `DESCRIBE NAMESPACE` command that check V1 and V2
 * table catalogs. The tests that cannot run for all supported catalogs are located in more
 * specific test suites:
 *
 *   - V2 table catalog tests: `org.apache.spark.sql.execution.command.v2.DescribeNamespaceSuite`
 *   - V1 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v1.DescribeNamespaceSuiteBase`
 *     - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.DescribeNamespaceSuite`
 *     - V1 Hive External catalog:
*        `org.apache.spark.sql.hive.execution.command.DescribeNamespaceSuite`
 */
trait DescribeNamespaceSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "DESCRIBE NAMESPACE"

  protected def notFoundMsgPrefix: String

  test("namespace does not exists") {
    val ns = "db1"
    val e = intercept[AnalysisException] {
      sql(s"DESCRIBE NAMESPACE EXTENDED $catalog.$ns")
    }
    checkError(e,
      errorClass = "SCHEMA_NOT_FOUND",
      parameters = Map("schemaName" -> s"`$catalog`.`$ns`"))
  }

  test("Keep the legacy output schema") {
    Seq(true, false).foreach { keepLegacySchema =>
      withSQLConf(SQLConf.LEGACY_KEEP_COMMAND_OUTPUT_SCHEMA.key -> keepLegacySchema.toString) {
        val ns = "db1"
        withNamespace(ns) {
          sql(s"CREATE NAMESPACE $ns")
          val schema = sql(s"DESCRIBE NAMESPACE $ns").schema.fieldNames.toSeq
          if (keepLegacySchema) {
            assert(schema === Seq("database_description_item", "database_description_value"))
          } else {
            assert(schema === Seq("info_name", "info_value"))
          }
        }
      }
    }
  }
}
