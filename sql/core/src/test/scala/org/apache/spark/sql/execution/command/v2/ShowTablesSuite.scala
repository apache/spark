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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchPartitionException}
import org.apache.spark.sql.execution.command
import org.apache.spark.util.Utils

/**
 * The class contains tests for the `SHOW TABLES` command to check V2 table catalogs.
 */
class ShowTablesSuite extends command.ShowTablesSuiteBase with CommandSuiteBase {
  override def defaultNamespace: Seq[String] = Nil

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

  test("show table in a not existing namespace") {
    val msg = intercept[NoSuchNamespaceException] {
      runShowTablesSql(s"SHOW TABLES IN $catalog.unknown", Seq())
    }.getMessage
    assert(msg.matches("(Database|Namespace) 'unknown' not found"))
  }

  test("SHOW TABLE EXTENDED for v2 tables") {
    val namespace = "ns1.ns2"
    val table = "tbl"
    withNamespaceAndTable(s"$namespace", s"$table", s"$catalog") { tbl =>
      sql(s"CREATE TABLE $tbl (id bigint, data string) " +
        s"$defaultUsing PARTITIONED BY (id)")
      sql(s"ALTER TABLE $tbl ADD PARTITION (id=1)")

      val result1 = sql(s"SHOW TABLE EXTENDED FROM $catalog.$namespace LIKE 'tb*'")
      assert(result1.schema.fieldNames ===
        Seq("namespace", "tableName", "isTemporary", "information"))
      assert(result1.collect()(0).length == 4)
      assert(result1.collect()(0)(3) ==
        s"""Namespace: ns1.ns2
           |Table: tbl
           |Type: MANAGED
           |Provider: _
           |Owner: ${Utils.getCurrentUserName()}
           |Partition Provider: Catalog
           |Partition Columns: `id`
           |Schema: root
           | |-- id: long (nullable = true)
           | |-- data: string (nullable = true)
           |""".stripMargin)

      val result2 = sql(s"SHOW TABLE EXTENDED IN $catalog.$namespace LIKE 'tb*'")
      assert(result2.schema.fieldNames ===
        Seq("namespace", "tableName", "isTemporary", "information"))
      assert(result2.collect()(0).length == 4)
      assert(result2.collect()(0)(3) ===
        s"""Namespace: ns1.ns2
           |Table: tbl
           |Type: MANAGED
           |Provider: _
           |Owner: ${Utils.getCurrentUserName()}
           |Partition Provider: Catalog
           |Partition Columns: `id`
           |Schema: root
           | |-- id: long (nullable = true)
           | |-- data: string (nullable = true)
           |""".stripMargin)

      val result3 = sql(s"SHOW TABLE EXTENDED FROM $catalog.$namespace " +
        s"LIKE 'tb*' PARTITION(id = 1)")
      assert(result3.schema.fieldNames ===
        Seq("namespace", "tableName", "isTemporary", "information"))
      assert(result3.collect()(0).length == 4)
      assert(result3.collect()(0)(3) ===
        """Partition Values: [id=1]
          |""".stripMargin)

      val result4 = sql(s"SHOW TABLE EXTENDED IN $catalog.$namespace LIKE 'tb*' PARTITION(id = 1)")
      assert(result4.schema.fieldNames ===
        Seq("namespace", "tableName", "isTemporary", "information"))
      assert(result4.collect()(0).length == 4)
      assert(result4.collect()(0)(3) ===
        """Partition Values: [id=1]
          |""".stripMargin)

      sql(s"ALTER TABLE $tbl SET LOCATION 's3://bucket/path'")
      val result5 = sql(s"SHOW TABLE EXTENDED IN $catalog.$namespace LIKE 'tb*'")
      assert(result5.schema.fieldNames ===
        Seq("namespace", "tableName", "isTemporary", "information"))
      assert(result5.collect()(0).length == 4)
      assert(result5.collect()(0)(1) === "tbl")
      assert(result5.collect()(0)(3) ===
        s"""Namespace: ns1.ns2
           |Table: tbl
           |Type: MANAGED
           |Location: s3://bucket/path
           |Provider: _
           |Owner: ${Utils.getCurrentUserName()}
           |Schema: root
           | |-- id: long (nullable = true)
           | |-- data: string (nullable = true)
           |""".stripMargin)
    }
  }

  test("SHOW TABLE EXTENDED in a not existing partition") {
    val namespace = "ns1.ns2"
    val table = "tbl"
    withNamespaceAndTable(s"$namespace", s"$table", s"$catalog") { tbl =>
      sql(s"CREATE TABLE $tbl (id bigint, data string) " +
        s"$defaultUsing PARTITIONED BY (id)")
      sql(s"ALTER TABLE $tbl ADD PARTITION (id=1)")
      val errMsg = intercept[NoSuchPartitionException] {
        sql(s"SHOW TABLE EXTENDED IN $catalog.$namespace LIKE 'tb*' PARTITION(id = 2)")
      }.getMessage
      assert(errMsg === "Partition not found in table ns1.ns2.tbl: 2 -> id")
    }
  }

  test("SHOW TABLE EXTENDED in a not existing table") {
    val table = "people"
    withTable(s"$catalog.$table") {
      val result = sql(s"SHOW TABLE EXTENDED FROM $catalog LIKE '*$table*'")
      assert(result.schema.fieldNames ===
          Seq("namespace", "tableName", "isTemporary", "information"))
      assert(result.collect().isEmpty)
    }
  }
}
