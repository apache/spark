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
 * This base suite contains unified tests for the `ALTER TABLE ... CLUSTER BY` command
 * that check V1 and V2 table catalogs. The tests that cannot run for all supported catalogs are
 * located in more specific test suites:
 *
 *   - V2 table catalog tests: `org.apache.spark.sql.execution.command.v2.AlterTableClusterBySuite`
 *   - V1 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v1.AlterTableClusterBySuiteBase`
 *     - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.AlterTableClusterBySuite`
 *     - V1 Hive External catalog:
 *        `org.apache.spark.sql.hive.execution.command.AlterTableClusterBySuite`
 */
trait AlterTableClusterBySuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "ALTER TABLE CLUSTER BY"

  protected val nestedColumnSchema: String =
    "col1 INT, col2 STRUCT<col3 INT, `col4 1` INT>, col3 STRUCT<`col4.1` INT>"
  protected val nestedClusteringColumns: Seq[String] =
    Seq("col2.col3", "col2.`col4 1`", "col3.`col4.1`")
  protected val nestedClusteringColumnsNew: Seq[String] =
    Seq("col3.`col4.1`", "col2.`col4 1`", "col2.col3")

  def validateClusterBy(tableName: String, clusteringColumns: Seq[String]): Unit

  test("test basic ALTER TABLE with clustering columns") {
    withNamespaceAndTable("ns", "table") { tbl =>
      sql(s"CREATE TABLE $tbl (id INT, data STRING) $defaultUsing CLUSTER BY (id, data)")
      validateClusterBy(tbl, Seq("id", "data"))
      sql(s"ALTER TABLE $tbl CLUSTER BY (data, id)")
      validateClusterBy(tbl, Seq("data", "id"))
      sql(s"ALTER TABLE $tbl CLUSTER BY NONE")
      validateClusterBy(tbl, Seq.empty)
      sql(s"ALTER TABLE $tbl CLUSTER BY (id)")
      validateClusterBy(tbl, Seq("id"))
    }
  }

  test("test clustering columns with comma") {
    withNamespaceAndTable("ns", "table") { tbl =>
      sql(s"CREATE TABLE $tbl (`i,d` INT, data STRING) $defaultUsing CLUSTER BY (`i,d`, data)")
      validateClusterBy(tbl, Seq("`i,d`", "data"))
      sql(s"ALTER TABLE $tbl CLUSTER BY (data, `i,d`)")
      validateClusterBy(tbl, Seq("data", "`i,d`"))
    }
  }

  test("test nested clustering columns") {
    withNamespaceAndTable("ns", "table") { tbl =>
      sql(s"CREATE TABLE $tbl " +
        s"($nestedColumnSchema) " +
        s"$defaultUsing CLUSTER BY (${nestedClusteringColumns.mkString(",")})")
      validateClusterBy(tbl, nestedClusteringColumns)
      sql(s"ALTER TABLE $tbl CLUSTER BY (${nestedClusteringColumnsNew.mkString(",")})")
      validateClusterBy(tbl, nestedClusteringColumnsNew)
    }
  }

  test("clustering columns not defined in schema") {
    withNamespaceAndTable("ns", "table") { tbl =>
      sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing CLUSTER BY (id)")
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"ALTER TABLE $tbl CLUSTER BY (unknown)")
        },
        condition = "_LEGACY_ERROR_TEMP_3060",
        parameters = Map("i" -> "unknown",
          "schema" ->
            """root
              | |-- id: long (nullable = true)
              | |-- data: string (nullable = true)
              |""".stripMargin)
      )
    }
  }

  // Converts three-part table name (catalog.namespace.table) to TableIdentifier.
  protected def parseTableName(threePartTableName: String): (String, String, String) = {
    val tablePath = threePartTableName.split('.')
    assert(tablePath.length === 3)
    (tablePath(0), tablePath(1), tablePath(2))
  }
}
