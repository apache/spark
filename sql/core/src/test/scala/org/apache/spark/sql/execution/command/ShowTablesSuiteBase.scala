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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.internal.SQLConf

/**
 * This base suite contains unified tests for the `SHOW TABLES` command that check V1 and V2
 * table catalogs. The tests that cannot run for all supported catalogs are located in more
 * specific test suites:
 *
 *   - V2 table catalog tests: `org.apache.spark.sql.execution.command.v2.ShowTablesSuite`
 *   - V1 table catalog tests: `org.apache.spark.sql.execution.command.v1.ShowTablesSuiteBase`
 *     - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.ShowTablesSuite`
 *     - V1 Hive External catalog: `org.apache.spark.sql.hive.execution.command.ShowTablesSuite`
 */
trait ShowTablesSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "SHOW TABLES"
  protected def defaultNamespace: Seq[String]

  protected def runShowTablesSql(sqlText: String, expected: Seq[Row]): Unit = {
    val df = spark.sql(sqlText)
    checkAnswer(df, expected)
  }

  test("show an existing table") {
    withNamespaceAndTable("ns", "table") { t =>
      sql(s"CREATE TABLE $t (name STRING, id INT) $defaultUsing")
      runShowTablesSql(s"SHOW TABLES IN $catalog.ns", Seq(Row("ns", "table", false)))
    }
  }

  test("show tables with a pattern") {
    withNamespace(s"$catalog.ns1", s"$catalog.ns2") {
      sql(s"CREATE NAMESPACE $catalog.ns1")
      sql(s"CREATE NAMESPACE $catalog.ns2")
      withTable(
        s"$catalog.ns1.table",
        s"$catalog.ns1.table_name_1a",
        s"$catalog.ns1.table_name_2b",
        s"$catalog.ns2.table_name_2b") {
        sql(s"CREATE TABLE $catalog.ns1.table (id bigint, data string) $defaultUsing")
        sql(s"CREATE TABLE $catalog.ns1.table_name_1a (id bigint, data string) $defaultUsing")
        sql(s"CREATE TABLE $catalog.ns1.table_name_2b (id bigint, data string) $defaultUsing")
        sql(s"CREATE TABLE $catalog.ns2.table_name_2b (id bigint, data string) $defaultUsing")

        runShowTablesSql(
          s"SHOW TABLES FROM $catalog.ns1",
          Seq(
            Row("ns1", "table", false),
            Row("ns1", "table_name_1a", false),
            Row("ns1", "table_name_2b", false)))

        runShowTablesSql(
          s"SHOW TABLES FROM $catalog.ns1 LIKE '*name*'",
          Seq(
            Row("ns1", "table_name_1a", false),
            Row("ns1", "table_name_2b", false)))

        runShowTablesSql(
          s"SHOW TABLES FROM $catalog.ns1 LIKE 'table_name_1*|table_name_2*'",
          Seq(
            Row("ns1", "table_name_1a", false),
            Row("ns1", "table_name_2b", false)))

        runShowTablesSql(
          s"SHOW TABLES FROM $catalog.ns1 LIKE '*2b'",
          Seq(Row("ns1", "table_name_2b", false)))
      }
    }
  }

  test("show tables with current catalog and namespace") {
    withSQLConf(SQLConf.DEFAULT_CATALOG.key -> catalog) {
      val tblName = (catalog +: defaultNamespace :+ "table").quoted
      withTable(tblName) {
        sql(s"CREATE TABLE $tblName (name STRING, id INT) $defaultUsing")
        val ns = defaultNamespace.mkString(".")
        runShowTablesSql("SHOW TABLES", Seq(Row(ns, "table", false)))
      }
    }
  }

  test("SPARK-34560: unique attribute references") {
    withNamespaceAndTable("ns1", "tbl1") { t1 =>
      sql(s"CREATE TABLE $t1 (col INT) $defaultUsing")
      val show1 = sql(s"SHOW TABLES IN $catalog.ns1")
      withNamespaceAndTable("ns2", "tbl2") { t2 =>
        sql(s"CREATE TABLE $t2 (col INT) $defaultUsing")
        val show2 = sql(s"SHOW TABLES IN $catalog.ns2")
        assert(!show1.join(show2).where(show1("tableName") =!= show2("tableName")).isEmpty)
      }
    }
  }

  test("change current catalog and namespace with USE statements") {
    withCurrentCatalogAndNamespace {
      withNamespaceAndTable("ns", "table") { t =>
        sql(s"CREATE TABLE $t (name STRING, id INT) $defaultUsing")

        sql(s"USE $catalog")
        // No table is matched since the current namespace is not ["ns"]
        assert(defaultNamespace != Seq("ns"))
        runShowTablesSql("SHOW TABLES", Seq())

        // Update the current namespace to match "ns.tbl".
        sql(s"USE $catalog.ns")
        runShowTablesSql("SHOW TABLES", Seq(Row("ns", "table", false)))
      }
    }
  }
}
