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
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.errors.DataTypeErrors.toSQLId
import org.apache.spark.sql.internal.SQLConf

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

  def getTblPropertyValue(tableIdent: TableIdentifier, key: String): String

  test("table to alter does not exist") {
    withNamespaceAndTable("ns", "does_not_exist") { t =>
      val sqlText = s"ALTER TABLE $t SET TBLPROPERTIES ('k1' = 'v1')"
      checkError(
        exception = intercept[AnalysisException] {
          sql(sqlText)
        },
        condition = "TABLE_OR_VIEW_NOT_FOUND",
        parameters = Map("relationName" -> toSQLId(t)),
      context = ExpectedContext(
        fragment = t,
        start = 12,
        stop = 11 + t.length)
      )
    }
  }

  test("alter table set properties") {
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
    }
  }

  test("alter table set reserved properties") {
    import TableCatalog._
    val keyParameters = Map[String, String](
      PROP_PROVIDER -> "please use the USING clause to specify it",
      PROP_LOCATION -> "please use the LOCATION clause to specify it",
      PROP_OWNER -> "it will be set to the current user",
      PROP_EXTERNAL -> "please use CREATE EXTERNAL TABLE"
    )
    withSQLConf((SQLConf.LEGACY_PROPERTY_NON_RESERVED.key, "false")) {
      tableLegacyProperties.foreach { key =>
        withNamespaceAndTable("ns", "tbl") { t =>
          val sqlText = s"ALTER TABLE $t SET TBLPROPERTIES ('$key'='bar')"
          checkError(
            exception = intercept[ParseException] {
              sql(sqlText)
            },
            condition = "UNSUPPORTED_FEATURE.SET_TABLE_PROPERTY",
            parameters = Map(
              "property" -> key,
              "msg" -> keyParameters.getOrElse(
                key, "please remove it from the TBLPROPERTIES list.")),
            context = ExpectedContext(
              fragment = sqlText,
              start = 0,
              stop = 40 + t.length + key.length))
        }
      }
    }
    withSQLConf((SQLConf.LEGACY_PROPERTY_NON_RESERVED.key, "true")) {
      tableLegacyProperties.foreach { key =>
        Seq("OPTIONS", "TBLPROPERTIES").foreach { clause =>
          withNamespaceAndTable("ns", "tbl") { t =>
            sql(s"CREATE TABLE $t (key int) USING parquet $clause ('$key'='bar')")
            val tableIdent = TableIdentifier("tbl", Some("ns"), Some(catalog))

            val originValue = getTblPropertyValue(tableIdent, key)
            assert(originValue != "bar", "reserved properties should not have side effects")

            sql(s"ALTER TABLE $t SET TBLPROPERTIES ('$key'='newValue')")
            assert(getTblPropertyValue(tableIdent, key) == originValue,
              "reserved properties should not have side effects")
          }
        }
      }
    }
  }
}
