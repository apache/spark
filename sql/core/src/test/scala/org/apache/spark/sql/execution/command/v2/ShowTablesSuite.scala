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

import scala.jdk.CollectionConverters._

import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.{InMemoryRelationCatalog, TableCatalog, TableSummary}
import org.apache.spark.sql.execution.command
import org.apache.spark.util.Utils

/**
 * The class contains tests for the `SHOW TABLES` command to check V2 table catalogs.
 */
class ShowTablesSuite extends command.ShowTablesSuiteBase with CommandSuiteBase {
  override def defaultNamespace: Seq[String] = Nil
  // Extended JSON now uses TableSummary.tableType(), and TableCatalog.listTableSummaries
  // defaults a missing V2 table type property to TableSummary.FOREIGN_TABLE_TYPE.
  override def expectedTableTypeInJson: String = TableSummary.FOREIGN_TABLE_TYPE
  override def expectsSessionTempViews: Boolean = false

  // The test fails for V1 catalog with the error:
  // org.apache.spark.sql.AnalysisException:
  //   The namespace in session catalog must have exactly one name part: spark_catalog.n1.n2.db
  test("show tables in nested namespaces") {
    withTable(s"$catalog.n1.n2.db") {
      spark.sql(s"CREATE TABLE $catalog.n1.n2.db.table_name (id bigint, data string) $defaultUsing")
      runShowTablesSql(
        s"SHOW TABLES FROM $catalog.n1.n2.db",
        Seq(Row("n1.n2.db", "table_name", false)))
    }
  }

  // The test fails for V1 catalog with the error:
  // org.apache.spark.sql.AnalysisException:
  //   The namespace in session catalog must have exactly one name part: spark_catalog.table
  test("using v2 catalog with empty namespace") {
    withTable(s"$catalog.table") {
      spark.sql(s"CREATE TABLE $catalog.table (id bigint, data string) $defaultUsing")
      runShowTablesSql(s"SHOW TABLES FROM $catalog", Seq(Row("", "table", false)))
    }
  }

  test("show table extended formats table properties") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint) $defaultUsing " +
        "TBLPROPERTIES ('p1'='v1', 'p2'='v2')")

      val information = sql(s"SHOW TABLE EXTENDED IN $catalog.ns LIKE 'tbl'")
        .collect()(0)(3).toString

      assert(information.split("\n").contains("Table Properties: [p1=v1, p2=v2]"))
    }
  }

  gridTest("show table extended preserves reserved metadata with display properties")(
      Seq(false, true)) { external =>
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint) $defaultUsing " +
        "COMMENT 'table comment' LOCATION 'file:/tmp/display-properties' " +
        "TBLPROPERTIES ('persisted' = 'stored')")
      val table = loadTable(catalog, "ns", "tbl")
      if (external) {
        table.properties.put(TableCatalog.PROP_EXTERNAL, "true")
      }
      table.setDisplayProperties(Map(
        "catalog-label" -> "catalog-value",
        "persisted" -> "display-value",
        TableCatalog.PROP_EXTERNAL -> (!external).toString,
        TableCatalog.PROP_COMMENT -> "display comment",
        TableCatalog.PROP_LOCATION -> "file:/display-location",
        TableCatalog.PROP_PROVIDER -> "display-provider",
        TableCatalog.PROP_OWNER -> "display-owner").asJava)

      val information = sql(s"SHOW TABLE EXTENDED IN $catalog.ns LIKE 'tbl'")
        .head().getString(3)
      val tableType = if (external) "EXTERNAL" else "MANAGED"
      assert(information ===
        s"""Catalog: $catalog
           |Namespace: ns
           |Table: tbl
           |Type: $tableType
           |Comment: table comment
           |Location: file:/tmp/display-properties
           |Provider: _
           |Owner: ${Utils.getCurrentUserName()}
           |Table Properties: [catalog-label=catalog-value, persisted=stored]
           |Schema: root
           | |-- id: long (nullable = true)
           |
           |""".stripMargin)
    }
  }

  test("show table extended omits empty table properties") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint) $defaultUsing")

      val information = sql(s"SHOW TABLE EXTENDED IN $catalog.ns LIKE 'tbl'")
        .collect()(0)(3).toString

      assert(!information.split("\n").exists(_.startsWith("Table Properties")))
    }
  }

  test("show table extended as json returns relation catalog table and view types") {
    val relationCatalog = "show_table_extended_json_relation_catalog"
    withSQLConf(s"spark.sql.catalog.$relationCatalog" ->
        classOf[InMemoryRelationCatalog].getName) {
      withNamespaceAndTable("ns", "tbl", relationCatalog) { t =>
        sql(s"CREATE TABLE $t (id INT) $defaultUsing")
        withView(s"$relationCatalog.ns.vw") {
          sql(s"CREATE VIEW $relationCatalog.ns.vw AS SELECT 1 AS id")

          val jsonStr = sql(
            s"SHOW TABLE EXTENDED IN $relationCatalog.ns LIKE '*' AS JSON")
            .collect()(0).getString(0)
          val tables = (parse(jsonStr) \ "tables").asInstanceOf[JArray].arr
          def entry(name: String): JValue = {
            tables.find(e => (e \ "name").extract[String] == name)
              .getOrElse(fail(s"Missing $name in SHOW TABLE EXTENDED AS JSON: $tables"))
          }

          assert((entry("tbl") \ "type").extract[String] ===
            TableSummary.FOREIGN_TABLE_TYPE)
          assert((entry("vw") \ "type").extract[String] ===
            TableSummary.VIEW_TABLE_TYPE)
          assert(tables.map(e => (e \ "name").extract[String]).toSet === Set("tbl", "vw"))
        }
      }
    }
  }

  override protected def extendedPartInNonPartedTableError(
      catalog: String,
      namespace: String,
      table: String): (String, Map[String, String]) = {
    ("PARTITIONS_NOT_FOUND",
      Map("partitionList" -> "`id`", "tableName" -> s"`$catalog`.`$namespace`.`$table`"))
  }

  protected override def namespaceKey: String = "Namespace"

  protected override def extendedTableInfo: String =
    s"""Type: MANAGED
       |Provider: _
       |Owner: ${Utils.getCurrentUserName()}""".stripMargin

  protected override def extendedTableSchema: String =
    s"""Schema: root
       | |-- id: long (nullable = true)
       | |-- data: string (nullable = true)""".stripMargin

  protected override def selectCommandSchema: Array[String] = Array("id", "data")
}
