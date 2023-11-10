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
 * This base suite contains unified tests for the `CREATE/REPLACE TABLE ... CLUSTER BY` command
 * that check V1 and V2 table catalogs. The tests that cannot run for all supported catalogs are
 * located in more specific test suites:
 *
 *   - V2 table catalog tests: `org.apache.spark.sql.execution.command.v2.CreateTableClusterBySuite`
 *   - V1 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v1.CreateTableClusterBySuiteBase`
 *     - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.CreateTableClusterBySuite`
 *     - V1 Hive External catalog:
 *        `org.apache.spark.sql.hive.execution.command.CreateTableClusterBySuite`
 */
trait CreateTableClusterBySuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "CREATE/REPLACE TABLE CLUSTER BY"

  protected val nestedColumnSchema: String =
    "col1 INT, col2 STRUCT<col3 INT, `col4 1` INT>, col3 STRUCT<`col4.1` INT>"
  protected val nestedClusteringColumns: Seq[String] =
    Seq("col2.col3", "col2.`col4 1`", "col3.`col4.1`")

  def validateClusterBy(tableName: String, clusteringColumns: Seq[String]): Unit

  test("test basic CREATE TABLE with clustering columns") {
    withNamespaceAndTable("ns", "table") { tbl =>
      spark.sql(s"CREATE TABLE $tbl (id INT, data STRING) $defaultUsing CLUSTER BY (id, data)")
      validateClusterBy(tbl, Seq("id", "data"))
    }
  }

  test("test clustering columns with comma") {
    withNamespaceAndTable("ns", "table") { tbl =>
      spark.sql(s"CREATE TABLE $tbl (`i,d` INT, data STRING) $defaultUsing " +
        "CLUSTER BY (`i,d`, data)")
      validateClusterBy(tbl, Seq("`i,d`", "data"))
    }
  }

  test("test nested clustering columns") {
    withNamespaceAndTable("ns", "table") { tbl =>
      spark.sql(s"CREATE TABLE $tbl " +
        s"($nestedColumnSchema) " +
        s"$defaultUsing CLUSTER BY (${nestedClusteringColumns.mkString(",")})")
      validateClusterBy(tbl, nestedClusteringColumns)
    }
  }

  test("clustering columns not defined in schema") {
    withNamespaceAndTable("ns", "table") { tbl =>
      val err = intercept[AnalysisException] {
        sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing CLUSTER BY (unknown)")
      }
      assert(err.message.contains("Couldn't find column unknown in:"))
    }
  }

  // Converts three-part table name (catalog.namespace.table) to TableIdentifier.
  protected def parseTableName(threePartTableName: String): (String, String, String) = {
    val tablePath = threePartTableName.split('.')
    assert(tablePath.length === 3)
    (tablePath(0), tablePath(1), tablePath(2))
  }
}
