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

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.StructType

trait ShowTablesSuiteBase extends QueryTest with SQLTestUtils {
  protected def version: String
  protected def catalog: String
  protected def defaultNamespace: Seq[String]
  protected def defaultUsing: String
  case class ShowRow(namespace: String, table: String, isTemporary: Boolean)
  protected def getRows(showRows: Seq[ShowRow]): Seq[Row]
  // Gets the schema of `SHOW TABLES`
  protected def showSchema: StructType

  protected def runShowTablesSql(sqlText: String, expected: Seq[ShowRow]): Unit = {
    val df = spark.sql(sqlText)
    assert(df.schema === showSchema)
    checkAnswer(df, getRows(expected))
  }

  override def test(testName: String, testTags: Tag*)(testFun: => Any)
      (implicit pos: Position): Unit = {
    super.test(s"SHOW TABLES $version: " + testName, testTags: _*)(testFun)
  }

  test("show an existing table") {
    withNamespace(s"$catalog.ns") {
      sql(s"CREATE NAMESPACE $catalog.ns")
      withTable(s"$catalog.ns.table") {
        sql(s"CREATE TABLE $catalog.ns.table (name STRING, id INT) $defaultUsing")
        runShowTablesSql(s"SHOW TABLES IN $catalog.ns", Seq(ShowRow("ns", "table", false)))
      }
    }
  }

  test("show table in a not existing namespace") {
    val msg = intercept[NoSuchNamespaceException] {
      runShowTablesSql(s"SHOW TABLES IN $catalog.unknown", Seq())
    }.getMessage
    assert(msg.matches("(Database|Namespace) 'unknown' not found"))
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
            ShowRow("ns1", "table", false),
            ShowRow("ns1", "table_name_1a", false),
            ShowRow("ns1", "table_name_2b", false)))

        runShowTablesSql(
          s"SHOW TABLES FROM $catalog.ns1 LIKE '*name*'",
          Seq(
            ShowRow("ns1", "table_name_1a", false),
            ShowRow("ns1", "table_name_2b", false)))

        runShowTablesSql(
          s"SHOW TABLES FROM $catalog.ns1 LIKE 'table_name_1*|table_name_2*'",
          Seq(
            ShowRow("ns1", "table_name_1a", false),
            ShowRow("ns1", "table_name_2b", false)))

        runShowTablesSql(
          s"SHOW TABLES FROM $catalog.ns1 LIKE '*2b'",
          Seq(ShowRow("ns1", "table_name_2b", false)))
      }
    }
  }

  test("show tables with current catalog and namespace") {
    withSQLConf(SQLConf.DEFAULT_CATALOG.key -> catalog) {
      val tblName = (catalog +: defaultNamespace :+ "table").quoted
      withTable(tblName) {
        sql(s"CREATE TABLE $tblName (name STRING, id INT) $defaultUsing")
        val ns = defaultNamespace.mkString(".")
        runShowTablesSql("SHOW TABLES", Seq(ShowRow(ns, "table", false)))
      }
    }
  }

  test("change current catalog and namespace with USE statements") {
    withNamespace(s"$catalog.ns") {
      sql(s"CREATE NAMESPACE $catalog.ns")
      withTable(s"$catalog.ns.table") {
        sql(s"CREATE TABLE $catalog.ns.table (name STRING, id INT) $defaultUsing")

        sql(s"USE $catalog")
        // No table is matched since the current namespace is not ["ns"]
        assert(defaultNamespace != Seq("ns"))
        runShowTablesSql("SHOW TABLES", Seq())

        // Update the current namespace to match "ns.tbl".
        sql(s"USE $catalog.ns")
        runShowTablesSql("SHOW TABLES", Seq(ShowRow("ns", "table", false)))
      }
    }
  }
}
