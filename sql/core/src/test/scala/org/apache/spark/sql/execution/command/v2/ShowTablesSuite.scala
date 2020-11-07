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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.connector.InMemoryTableCatalog
import org.apache.spark.sql.execution.command.{ShowTablesSuite => CommonShowTablesSuite}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructType}

class ShowTablesSuite extends QueryTest with SharedSparkSession with CommonShowTablesSuite {
  override def catalog: String = "test_catalog_v2"
  override protected def defaultUsing: String = "USING foo"
  override protected def namespaceColumn: String = "namespace"
  override protected def showSchema: StructType = {
    new StructType()
      .add("namespace", StringType, nullable = false)
      .add("tableName", StringType, nullable = false)
  }
  override protected def getRows(showRows: Seq[ShowRow]): Seq[Row] = {
    showRows.map {
      case ShowRow(namespace, table, _) => Row(namespace, table)
    }
  }

  override def sparkConf: SparkConf = super.sparkConf
    .set(s"spark.sql.catalog.$catalog", classOf[InMemoryTableCatalog].getName)

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    sql(s"CREATE DATABASE $catalog.$namespace")
    sql(s"CREATE TABLE $catalog.$namespace.$table (name STRING, id INT) $defaultUsing")
  }

  protected override def afterAll(): Unit = {
    sql(s"DROP TABLE $catalog.$namespace.$table")
    sql(s"DROP DATABASE $catalog.$namespace")
    super.afterAll()
  }

  test("show table in a not existing namespace") {
    runShowTablesSql(s"SHOW TABLES IN $catalog.unknown", Seq())
  }

  // The test fails for V1 catalog with the error:
  // org.apache.spark.sql.AnalysisException:
  //   The namespace in session catalog must have exactly one name part: spark_catalog.n1.n2.db
  test("show tables in nested namespaces") {
    withTable(s"$catalog.n1.n2.db") {
      spark.sql(s"CREATE TABLE $catalog.n1.n2.db.table_name (id bigint, data string) $defaultUsing")
      runShowTablesSql(
        s"SHOW TABLES FROM $catalog.n1.n2.db",
        Seq(ShowRow("n1.n2.db", "table_name", false)))
    }
  }

  test("ShowViews: using v2 catalog, command not supported.") {
    val exception = intercept[AnalysisException] {
      sql(s"SHOW VIEWS FROM $catalog")
    }
    assert(exception.getMessage.contains(s"Catalog $catalog doesn't support SHOW VIEWS," +
      " only SessionCatalog supports this command."))
  }

  test("ShowTables: using v2 catalog with empty namespace") {
    withTable(s"$catalog.table") {
      spark.sql(s"CREATE TABLE $catalog.table (id bigint, data string) $defaultUsing")
      runShowTablesSql(s"SHOW TABLES FROM $catalog", Seq(ShowRow("", "table", false)))
    }
  }

  test("ShowTables: namespace is not specified and default v2 catalog is set") {
    withSQLConf(SQLConf.DEFAULT_CATALOG.key -> catalog) {
      withTable(s"$catalog.table") {
        spark.sql(s"CREATE TABLE $catalog.table (id bigint, data string) $defaultUsing")
        // v2 catalog is used where default namespace is empty for TestInMemoryTableCatalog.
        runShowTablesSql("SHOW TABLES", Seq(ShowRow("", "table", false)))
      }
    }
  }

  test("SHOW TABLE EXTENDED not valid v1 database") {
    def testV1CommandNamespace(sqlCommand: String, namespace: String): Unit = {
      val e = intercept[AnalysisException] {
        sql(sqlCommand)
      }
      assert(e.message.contains(s"The database name is not valid: ${namespace}"))
    }

    val namespace = s"$catalog.ns1.ns2"
    val table = "tbl"
    withTable(s"$namespace.$table") {
      sql(s"CREATE TABLE $namespace.$table (id bigint, data string) " +
        s"$defaultUsing PARTITIONED BY (id)")

      testV1CommandNamespace(s"SHOW TABLE EXTENDED FROM $namespace LIKE 'tb*'",
        namespace)
      testV1CommandNamespace(s"SHOW TABLE EXTENDED IN $namespace LIKE 'tb*'",
        namespace)
      testV1CommandNamespace("SHOW TABLE EXTENDED " +
        s"FROM $namespace LIKE 'tb*' PARTITION(id=1)",
        namespace)
      testV1CommandNamespace("SHOW TABLE EXTENDED " +
        s"IN $namespace LIKE 'tb*' PARTITION(id=1)",
        namespace)
    }
  }
}
