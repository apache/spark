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
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.internal.SQLConf

/**
 * This base suite contains unified tests for the `DESCRIBE NAMESPACE` command that checks V1
 * table catalogs. The tests that cannot run for all V1 catalogs are located in more
 * specific test suites:
 *
 *   - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.DescribeNamespaceSuite`
 *   - V1 Hive External catalog:
 *     `org.apache.spark.sql.hive.execution.command.DescribeNamespaceSuite`
 */
trait DescribeNamespaceSuiteBase extends command.DescribeNamespaceSuiteBase {
  override def notFoundMsgPrefix: String = "Database"

  test("basic") {
    val ns = "db1"
    withNamespace(ns) {
      sql(s"CREATE NAMESPACE $ns")

      val result = sql(s"DESCRIBE NAMESPACE EXTENDED $ns")
        .toDF("key", "value")
        .where("key not like 'Owner%'") // filter for consistency with in-memory catalog
        .collect()

      assert(result.length == 4)
      assert(result(0) === Row("Database Name", ns))
      assert(result(1) === Row("Comment", ""))
      // Check only the key for "Location" since its value depends on warehouse path, etc.
      assert(result(2).getString(0) === "Location")
      assert(result(3) === Row("Properties", ""))
    }
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

/**
 * The class contains tests for the `DESCRIBE NAMESPACE` command to check V1 In-Memory
 * table catalog.
 */
class DescribeNamespaceSuite extends DescribeNamespaceSuiteBase with CommandSuiteBase
