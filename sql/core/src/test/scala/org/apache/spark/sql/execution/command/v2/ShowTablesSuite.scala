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

import org.apache.spark.sql.{AnalysisException, Row}
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
    val e = intercept[NoSuchNamespaceException] {
      runShowTablesSql(s"SHOW TABLES IN $catalog.unknown", Seq())
    }
    checkError(e,
      errorClass = "SCHEMA_NOT_FOUND",
      parameters = Map("schemaName" -> "`unknown`"))
  }

  test("show table extended in a not existing namespace") {
    checkError(
      exception = intercept[NoSuchNamespaceException] {
        sql(s"SHOW TABLE EXTENDED FROM $catalog.unknown LIKE '*tbl*'")
      },
      errorClass = "SCHEMA_NOT_FOUND",
      parameters = Map("schemaName" -> "`unknown`"))
  }

  test("show table extended in a not existing table") {
    val table = "nonexist"
    withTable(s"$catalog.$table") {
      val result = sql(s"SHOW TABLE EXTENDED FROM $catalog LIKE '*$table*'")
      assert(result.schema.fieldNames ===
        Seq("namespace", "tableName", "isTemporary", "information"))
      assert(result.collect().isEmpty)
    }
  }

  test("show table extended in non-partitioned table") {
    val namespace = "ns1.ns2"
    val table = "tbl"
    withNamespaceAndTable(namespace, table, catalog) { tbl =>
      sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing")
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SHOW TABLE EXTENDED IN $catalog.$namespace LIKE '$table' PARTITION(id = 1)")
        },
        errorClass = "_LEGACY_ERROR_TEMP_1231",
        parameters = Map("key" -> "id", "tblName" -> s"$catalog.$namespace.$table")
      )
    }
  }

  test("show table extended in multi-partition table") {
    val namespace = "ns1.ns2"
    val table = "tbl"
    withNamespaceAndTable(namespace, table, catalog) { tbl =>
      sql(s"CREATE TABLE $tbl (id1 bigint, id2 bigint, data string) " +
        s"$defaultUsing PARTITIONED BY (id1, id2)")
      sql(s"ALTER TABLE $tbl ADD PARTITION (id1 = 1, id2 = 2)")

      val result = sql(s"SHOW TABLE EXTENDED FROM $catalog.$namespace " +
        s"LIKE '$table' PARTITION(id1 = 1, id2 = 2)")
      assert(result.schema.fieldNames ===
        Seq("namespace", "tableName", "isTemporary", "information"))
      assert(result.collect()(0).length == 4)
      assert(result.collect()(0)(0) === namespace)
      assert(result.collect()(0)(1) === s"$catalog.$namespace.$table")
      assert(result.collect()(0)(2) === false)
      assert(result.collect()(0)(3) ===
        """Partition Values: [id1=1, id2=2]
          |
          |""".stripMargin)

      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SHOW TABLE EXTENDED IN $catalog.$namespace LIKE '$table' PARTITION(id1 = 1)")
        },
        errorClass = "_LEGACY_ERROR_TEMP_1232",
        parameters = Map(
          "specKeys" -> "id1",
          "partitionColumnNames" -> "id1, id2",
          "tableName" -> s"$catalog.$namespace.$table")
      )
    }
  }

  test("show table extended in a not existing partition") {
    val namespace = "ns1.ns2"
    val table = "tbl"
    withNamespaceAndTable(namespace, table, catalog) { tbl =>
      sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
      sql(s"ALTER TABLE $tbl ADD PARTITION (id = 1)")
      checkError(
        exception = intercept[NoSuchPartitionException] {
          sql(s"SHOW TABLE EXTENDED IN $catalog.$namespace LIKE '$table' PARTITION(id = 2)")
        },
        errorClass = "PARTITIONS_NOT_FOUND",
        parameters = Map(
          "partitionList" -> "PARTITION (`id` = 2)",
          "tableName" -> "`ns1`.`ns2`.`tbl`"
        )
      )
    }
  }

  test("show table extended for v2 tables") {
    val namespace = "ns1.ns2"
    val table = "tbl"
    withNamespaceAndTable(namespace, table, catalog) { tbl =>
      sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
      sql(s"ALTER TABLE $tbl ADD PARTITION (id = 1)")

      val result1 = sql(s"SHOW TABLE EXTENDED FROM $catalog.$namespace LIKE '$table'")
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
           |
           |""".stripMargin)

      val result2 = sql(s"SHOW TABLE EXTENDED IN $catalog.$namespace LIKE '$table'")
      assert(result2.schema.fieldNames ===
        Seq("namespace", "tableName", "isTemporary", "information"))
      assert(result2.collect()(0).length == 4)
      assert(result2.collect()(0)(3) ==
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
           |
           |""".stripMargin)

      val result3 = sql(s"SHOW TABLE EXTENDED FROM $catalog.$namespace LIKE 'tb*'")
      assert(result3.schema.fieldNames ===
        Seq("namespace", "tableName", "isTemporary", "information"))
      assert(result3.collect()(0).length == 4)
      assert(result3.collect()(0)(3) ==
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
           |
           |""".stripMargin)

      val result4 = sql(s"SHOW TABLE EXTENDED IN $catalog.$namespace LIKE 'tb*'")
      assert(result4.schema.fieldNames ===
        Seq("namespace", "tableName", "isTemporary", "information"))
      assert(result4.collect()(0).length == 4)
      assert(result4.collect()(0)(3) ===
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
           |
           |""".stripMargin)

      val result5 = sql(s"SHOW TABLE EXTENDED FROM $catalog.$namespace " +
        s"LIKE '$table' PARTITION(id = 1)")
      assert(result5.schema.fieldNames ===
        Seq("namespace", "tableName", "isTemporary", "information"))
      assert(result5.collect()(0).length == 4)
      assert(result5.collect()(0)(3) ===
        """Partition Values: [id=1]
          |
          |""".stripMargin)

      val result6 = sql(s"SHOW TABLE EXTENDED IN $catalog.$namespace " +
        s"LIKE '$table' PARTITION(id = 1)")
      assert(result6.schema.fieldNames ===
        Seq("namespace", "tableName", "isTemporary", "information"))
      assert(result6.collect()(0).length == 4)
      assert(result6.collect()(0)(3) ===
        """Partition Values: [id=1]
          |
          |""".stripMargin)

      sql(s"ALTER TABLE $tbl SET LOCATION 's3://bucket/path'")
      val result7 = sql(s"SHOW TABLE EXTENDED IN $catalog.$namespace LIKE 'tb*'")
      assert(result7.schema.fieldNames ===
        Seq("namespace", "tableName", "isTemporary", "information"))
      assert(result7.collect()(0).length == 4)
      assert(result7.collect()(0)(1) === "tbl")
      assert(result7.collect()(0)(3) ===
        s"""Namespace: ns1.ns2
           |Table: tbl
           |Type: MANAGED
           |Location: s3://bucket/path
           |Provider: _
           |Owner: ${Utils.getCurrentUserName()}
           |Schema: root
           | |-- id: long (nullable = true)
           | |-- data: string (nullable = true)
           |
           |""".stripMargin)
    }
  }

  test("show table extended for v2 multi tables") {
    val namespace = "ns1.ns2"
    val table = "tbl"
    withNamespaceAndTable(namespace, table, catalog) { tbl =>
      sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
      
      val table1 = "tbl1"
      val table2 = "tbl2"
      withTable(table1, table2) {
        sql(s"CREATE TABLE $catalog.$namespace.$table1 (id1 bigint, data1 string) " +
          s"$defaultUsing PARTITIONED BY (id1)")
        sql(s"CREATE TABLE $catalog.$namespace.$table2 (id2 bigint, data2 string) " +
          s"$defaultUsing PARTITIONED BY (id2)")

        val result = sql(s"SHOW TABLE EXTENDED FROM $catalog.$namespace LIKE '$table*'")
          .sort("tableName")
        assert(result.schema.fieldNames ===
          Seq("namespace", "tableName", "isTemporary", "information"))
        assert(result.collect().length == 3)

        assert(result.collect()(0).length == 4)
        assert(result.collect()(0)(1) === "tbl")
        assert(result.collect()(0)(3) ===
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
             |
             |""".stripMargin)

        assert(result.collect()(1).length == 4)
        assert(result.collect()(1)(1) === "tbl1")
        assert(result.collect()(1)(3) ===
          s"""Namespace: ns1.ns2
             |Table: tbl1
             |Type: MANAGED
             |Provider: _
             |Owner: ${Utils.getCurrentUserName()}
             |Partition Provider: Catalog
             |Partition Columns: `id1`
             |Schema: root
             | |-- id1: long (nullable = true)
             | |-- data1: string (nullable = true)
             |
             |""".stripMargin)

        assert(result.collect()(2).length == 4)
        assert(result.collect()(2)(1) === "tbl2")
        assert(result.collect()(2)(3) ===
          s"""Namespace: ns1.ns2
             |Table: tbl2
             |Type: MANAGED
             |Provider: _
             |Owner: ${Utils.getCurrentUserName()}
             |Partition Provider: Catalog
             |Partition Columns: `id2`
             |Schema: root
             | |-- id2: long (nullable = true)
             | |-- data2: string (nullable = true)
             |
             |""".stripMargin)
      }
    }
  }
}
