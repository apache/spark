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
import org.apache.spark.sql.catalyst.TableIdentifier

/**
 * This base suite contains unified tests for the `ALTER TABLE .. SET TBLPROPERTIES`
 * command that check V1 and V2 table catalogs. The tests that cannot run for all supported
 * catalogs are located in more specific test suites:
 *
 *   - V2 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v2.AlterTableSetTblPropertiesSuite`
 *   - V1 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v1.AlterTableSetTblPropertiesSuiteBase`
 *     - V1 In-Memory catalog:
 *       `org.apache.spark.sql.execution.command.v1.AlterTableSetTblPropertiesSuite`
 *     - V1 Hive External catalog:
 *        `org.apache.spark.sql.hive.execution.command.AlterTableSetTblPropertiesSuite`
 */
trait AlterTableSetTblPropertiesSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "ALTER TABLE .. SET TBLPROPERTIES"

  def checkTblProps(tableIdent: TableIdentifier, expectedTblProps: Map[String, String]): Unit

  test("alter table set tblproperties") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (col1 int, col2 string, a int, b int) $defaultUsing")
      val tableIdent = TableIdentifier("tbl", Some("ns"), Some(catalog))
      checkTblProps(tableIdent, Map.empty[String, String])

      sql(s"ALTER TABLE $t SET TBLPROPERTIES ('k1' = 'v1', 'k2' = 'v2', 'k3' = 'v3')")
      checkTblProps(tableIdent, Map("k1" -> "v1", "k2" -> "v2", "k3" -> "v3"))

      sql(s"USE $catalog.ns")
      sql(s"ALTER TABLE tbl SET TBLPROPERTIES ('k1' = 'v1', 'k2' = 'v2', 'k3' = 'v3')")
      checkTblProps(tableIdent, Map("k1" -> "v1", "k2" -> "v2", "k3" -> "v3"))

      sql(s"ALTER TABLE $t SET TBLPROPERTIES ('k1' = 'v1', 'k2' = 'v8')")
      checkTblProps(tableIdent, Map("k1" -> "v1", "k2" -> "v8", "k3" -> "v3"))

      // table to alter does not exist
      checkError(
        exception = intercept[AnalysisException] {
          sql("ALTER TABLE does_not_exist SET TBLPROPERTIES ('winner' = 'loser')")
        },
        errorClass = "TABLE_OR_VIEW_NOT_FOUND",
        parameters = Map("relationName" -> "`does_not_exist`"),
        context = ExpectedContext(fragment = "does_not_exist", start = 12, stop = 25)
      )
    }
  }
}
