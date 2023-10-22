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

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

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

  test("show table in a not existing namespace") {
    checkError(
      exception = intercept[AnalysisException] {
        sql(s"SHOW TABLES IN $catalog.nonexist")
      },
      errorClass = "SCHEMA_NOT_FOUND",
      parameters = Map("schemaName" -> "`nonexist`"))
  }

  test("show table extended in a not existing namespace") {
    checkError(
      exception = intercept[AnalysisException] {
        sql(s"SHOW TABLE EXTENDED IN $catalog.nonexist LIKE '*tbl*'")
      },
      errorClass = "SCHEMA_NOT_FOUND",
      parameters = Map("schemaName" -> "`nonexist`"))
  }

  test("show table extended in a not existing table") {
    val namespace = "ns1"
    val table = "nonexist"
    withNamespaceAndTable(namespace, table, catalog) { _ =>
      val result = sql(s"SHOW TABLE EXTENDED IN $catalog.$namespace LIKE '*$table*'")
      assert(result.schema.fieldNames ===
        Seq("namespace", "tableName", "isTemporary", "information"))
      assert(result.collect().isEmpty)
    }
  }

  test("show table extended in non-partitioned table") {
    val namespace = "ns1"
    val table = "tbl"
    withNamespaceAndTable(namespace, table, catalog) { tbl =>
      sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing")
      val e = intercept[AnalysisException] {
        sql(s"SHOW TABLE EXTENDED IN $catalog.$namespace LIKE '$table' PARTITION(id = 1)")
      }
      checkError(
        exception = e,
        errorClass = e.getErrorClass,
        parameters = e.getErrorClass match {
          case "_LEGACY_ERROR_TEMP_1251" =>
            Map("action" -> "SHOW TABLE EXTENDED", "tableName" -> table) // v1 v2
          case "_LEGACY_ERROR_TEMP_1231" =>
            Map("key" -> "id", "tblName" -> s"`$catalog`.`$namespace`.`$table`") // hive
        }
      )
    }
  }

  test("show table extended in a not existing partition") {
    val namespace = "ns1"
    val table = "tbl"
    withNamespaceAndTable(namespace, table, catalog) { tbl =>
      sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
      sql(s"ALTER TABLE $tbl ADD PARTITION (id = 1)")
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SHOW TABLE EXTENDED IN $catalog.$namespace LIKE '$table' PARTITION(id = 2)")
        },
        errorClass = "PARTITIONS_NOT_FOUND",
        parameters = Map(
          "partitionList" -> "PARTITION (`id` = 2)",
          "tableName" -> "`ns1`.`tbl`"
        )
      )
    }
  }

  test("show table extended in multi partition key table") {
    val namespace = "ns1"
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
      assert(result.collect()(0)(1) === table)
      assert(result.collect()(0)(2) === false)
      val actualResult = exclude(result.collect()(0)(3).toString)
      val expectedResult_v1_v2 = "Partition Values: [id1=1, id2=2]"
      val expectedResult_hive =
        """Partition Values: [id1=1, id2=2]
          |Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
          |InputFormat: org.apache.hadoop.mapred.TextInputFormat
          |OutputFormat: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
          |Storage Properties: [serialization.format=1]""".stripMargin
      assert(actualResult === expectedResult_v1_v2 || actualResult === expectedResult_hive)

      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SHOW TABLE EXTENDED IN $catalog.$namespace " +
            s"LIKE '$table' PARTITION(id1 = 1)")
        },
        errorClass = "_LEGACY_ERROR_TEMP_1232",
        parameters = Map(
          "specKeys" -> "id1",
          "partitionColumnNames" -> "id1, id2",
          "tableName" -> s"`$catalog`.`$namespace`.`$table`")
      )
    }
  }

  test("show table extended in multi tables") {
    val namespace = "ns1"
    val table = "tbl"
    withNamespaceAndTable(namespace, table, catalog) { _ =>
      sql(s"CREATE TABLE $catalog.$namespace.$table (id bigint, data string) " +
        s"$defaultUsing PARTITIONED BY (id)")
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
        assert(result.collect()(0)(1) === table)
        assert(result.collect()(0)(2) === false)
        val actualResult_0_3 = exclude(result.collect()(0)(3).toString)

        // exclude "Created Time", "Last Access", "Created By", "Location"
        val expectedResult_0_3_v1 =
          s"""Catalog: $catalog
             |Database: $namespace
             |Table: $table
             |Type: MANAGED
             |Provider: parquet
             |Partition Provider: Catalog
             |Partition Columns: [`id`]
             |Schema: root
             | |-- data: string (nullable = true)
             | |-- id: long (nullable = true)""".stripMargin
        val expectedResult_0_3_v2 =
          s"""Namespace: $namespace
             |Table: $table
             |Type: MANAGED
             |Provider: _
             |Owner: ${Utils.getCurrentUserName()}
             |Partition Provider: Catalog
             |Partition Columns: [`id`]
             |Schema: root
             | |-- data: string (nullable = true)
             | |-- id: long (nullable = true)""".stripMargin

        // exclude "Table Properties"
        val expectedResult_0_3_hive =
          s"""Catalog: $catalog
             |Database: $namespace
             |Table: $table
             |Owner: ${Utils.getCurrentUserName()}
             |Type: MANAGED
             |Provider: hive
             |Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
             |InputFormat: org.apache.hadoop.mapred.TextInputFormat
             |OutputFormat: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
             |Storage Properties: [serialization.format=1]
             |Partition Provider: Catalog
             |Partition Columns: [`id`]
             |Schema: root
             | |-- data: string (nullable = true)
             | |-- id: long (nullable = true)""".stripMargin
        assert(actualResult_0_3 === expectedResult_0_3_v1 ||
          actualResult_0_3 === expectedResult_0_3_v2 ||
          actualResult_0_3 === expectedResult_0_3_hive)

        assert(result.collect()(1).length == 4)
        assert(result.collect()(1)(1) === table1)
        assert(result.collect()(1)(2) === false)
        val actualResult_1_3 = exclude(result.collect()(1)(3).toString)

        // exclude "Created Time", "Last Access", "Created By", "Location"
        val expectedResult_1_3_v1 =
          s"""Catalog: $catalog
             |Database: $namespace
             |Table: $table1
             |Type: MANAGED
             |Provider: parquet
             |Partition Provider: Catalog
             |Partition Columns: [`id1`]
             |Schema: root
             | |-- data1: string (nullable = true)
             | |-- id1: long (nullable = true)""".stripMargin
        val expectedResult_1_3_v2 =
          s"""Namespace: $namespace
             |Table: $table1
             |Type: MANAGED
             |Provider: _
             |Owner: ${Utils.getCurrentUserName()}
             |Partition Provider: Catalog
             |Partition Columns: [`id1`]
             |Schema: root
             | |-- data1: string (nullable = true)
             | |-- id1: long (nullable = true)""".stripMargin

        // exclude "Table Properties"
        val expectedResult_1_3_hive =
          s"""Catalog: $catalog
             |Database: $namespace
             |Table: $table1
             |Owner: ${Utils.getCurrentUserName()}
             |Type: MANAGED
             |Provider: hive
             |Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
             |InputFormat: org.apache.hadoop.mapred.TextInputFormat
             |OutputFormat: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
             |Storage Properties: [serialization.format=1]
             |Partition Provider: Catalog
             |Partition Columns: [`id1`]
             |Schema: root
             | |-- data1: string (nullable = true)
             | |-- id1: long (nullable = true)""".stripMargin
        assert(actualResult_1_3 === expectedResult_1_3_v1 ||
          actualResult_1_3 === expectedResult_1_3_v2 ||
          actualResult_1_3 === expectedResult_1_3_hive)

        assert(result.collect()(2).length == 4)
        assert(result.collect()(2)(1) === table2)
        assert(result.collect()(2)(2) === false)
        val actualResult_2_3 = exclude(result.collect()(2)(3).toString)

        // exclude "Created Time", "Last Access", "Created By", "Location"
        val expectedResult_2_3_v1 =
          s"""Catalog: $catalog
             |Database: $namespace
             |Table: $table2
             |Type: MANAGED
             |Provider: parquet
             |Partition Provider: Catalog
             |Partition Columns: [`id2`]
             |Schema: root
             | |-- data2: string (nullable = true)
             | |-- id2: long (nullable = true)""".stripMargin
        val expectedResult_2_3_v2 =
          s"""Namespace: $namespace
             |Table: $table2
             |Type: MANAGED
             |Provider: _
             |Owner: ${Utils.getCurrentUserName()}
             |Partition Provider: Catalog
             |Partition Columns: [`id2`]
             |Schema: root
             | |-- data2: string (nullable = true)
             | |-- id2: long (nullable = true)""".stripMargin

        // exclude "Table Properties"
        val expectedResult_2_3_hive =
          s"""Catalog: $catalog
             |Database: $namespace
             |Table: $table2
             |Owner: ${Utils.getCurrentUserName()}
             |Type: MANAGED
             |Provider: hive
             |Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
             |InputFormat: org.apache.hadoop.mapred.TextInputFormat
             |OutputFormat: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
             |Storage Properties: [serialization.format=1]
             |Partition Provider: Catalog
             |Partition Columns: [`id2`]
             |Schema: root
             | |-- data2: string (nullable = true)
             | |-- id2: long (nullable = true)""".stripMargin
        assert(actualResult_2_3 === expectedResult_2_3_v1 ||
          actualResult_2_3 === expectedResult_2_3_v2 ||
          actualResult_2_3 === expectedResult_2_3_hive)
      }
    }
  }

  test("show table extended in/from") {
    val namespace = "ns1"
    val table = "tbl"
    withNamespaceAndTable(namespace, table, catalog) { _ =>
      sql(s"CREATE TABLE $catalog.$namespace.$table (id bigint, data string) " +
        s"$defaultUsing PARTITIONED BY (id)")
      sql(s"ALTER TABLE $catalog.$namespace.$table ADD PARTITION (id = 1)")

      val result1 = sql(s"SHOW TABLE EXTENDED FROM $catalog.$namespace LIKE '$table'")
      assert(result1.schema.fieldNames ===
        Seq("namespace", "tableName", "isTemporary", "information"))
      assert(result1.collect()(0).length == 4)
      assert(result1.collect()(0)(2) === false)
      val actualResult1 = exclude(result1.collect()(0)(3).toString)
      val expectedResult_v1 =
        s"""Namespace: $namespace
           |Table: $table
           |Type: MANAGED
           |Provider: _
           |Owner: ${Utils.getCurrentUserName()}
           |Partition Provider: Catalog
           |Partition Columns: [`id`]
           |Schema: root
           | |-- data: string (nullable = true)
           | |-- id: long (nullable = true)""".stripMargin
      val expectedResult_v2 =
        s"""Catalog: $catalog
           |Database: $namespace
           |Table: $table
           |Type: MANAGED
           |Provider: parquet
           |Partition Provider: Catalog
           |Partition Columns: [`id`]
           |Schema: root
           | |-- data: string (nullable = true)
           | |-- id: long (nullable = true)""".stripMargin
      val expectedResult_hive =
        s"""Catalog: $catalog
           |Database: $namespace
           |Table: $table
           |Owner: ${Utils.getCurrentUserName()}
           |Type: MANAGED
           |Provider: hive
           |Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
           |InputFormat: org.apache.hadoop.mapred.TextInputFormat
           |OutputFormat: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
           |Storage Properties: [serialization.format=1]
           |Partition Provider: Catalog
           |Partition Columns: [`id`]
           |Schema: root
           | |-- data: string (nullable = true)
           | |-- id: long (nullable = true)""".stripMargin
      assert(actualResult1 === expectedResult_v1 ||
        actualResult1 === expectedResult_v2 ||
        actualResult1 === expectedResult_hive)

      val result2 = sql(s"SHOW TABLE EXTENDED IN $catalog.$namespace LIKE '$table'")
      assert(result2.schema.fieldNames ===
        Seq("namespace", "tableName", "isTemporary", "information"))
      assert(result2.collect()(0).length == 4)
      assert(result2.collect()(0)(2) === false)
      val actualResult2 = exclude(result2.collect()(0)(3).toString)
      assert(actualResult2 === expectedResult_v1 ||
        actualResult2 === expectedResult_v2 ||
        actualResult2 === expectedResult_hive)

      val result3 = sql(s"SHOW TABLE EXTENDED FROM $catalog.$namespace LIKE 'tb*'")
      assert(result3.schema.fieldNames ===
        Seq("namespace", "tableName", "isTemporary", "information"))
      assert(result3.collect()(0).length == 4)
      assert(result3.collect()(0)(2) === false)
      val actualResult3 = exclude(result3.collect()(0)(3).toString)
      assert(actualResult3 === expectedResult_v1 ||
        actualResult3 === expectedResult_v2 ||
        actualResult3 === expectedResult_hive)

      val result4 = sql(s"SHOW TABLE EXTENDED IN $catalog.$namespace LIKE 'tb*'")
      assert(result4.schema.fieldNames ===
        Seq("namespace", "tableName", "isTemporary", "information"))
      assert(result4.collect()(0).length == 4)
      assert(result4.collect()(0)(2) === false)
      val actualResult4 = exclude(result4.collect()(0)(3).toString)
      assert(actualResult4 === expectedResult_v1 ||
        actualResult4 === expectedResult_v2 ||
        actualResult4 === expectedResult_hive)

      val result5 = sql(s"SHOW TABLE EXTENDED FROM $catalog.$namespace " +
        s"LIKE '$table' PARTITION(id = 1)")
      assert(result5.schema.fieldNames ===
        Seq("namespace", "tableName", "isTemporary", "information"))
      assert(result5.collect()(0).length == 4)
      assert(result5.collect()(0)(2) === false)
      val actualResult5 = exclude(result5.collect()(0)(3).toString)
      val expectedResultPartition_v1_v2 = "Partition Values: [id=1]"
      val expectedResultPartition_hive =
        """Partition Values: [id=1]
          |Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
          |InputFormat: org.apache.hadoop.mapred.TextInputFormat
          |OutputFormat: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
          |Storage Properties: [serialization.format=1]""".stripMargin
      assert(actualResult5 === expectedResultPartition_v1_v2 ||
        actualResult5 === expectedResultPartition_hive)

      val result6 = sql(s"SHOW TABLE EXTENDED IN $catalog.$namespace " +
        s"LIKE '$table' PARTITION(id = 1)")
      assert(result6.schema.fieldNames ===
        Seq("namespace", "tableName", "isTemporary", "information"))
      assert(result6.collect()(0).length == 4)
      assert(result6.collect()(0)(2) === false)
      val actualResult6 = exclude(result6.collect()(0)(3).toString)
      assert(actualResult6 === expectedResultPartition_v1_v2 ||
        actualResult6 === expectedResultPartition_hive)

      withTempDir { dir =>
        sql(s"ALTER TABLE $catalog.$namespace.$table " +
          s"SET LOCATION 'file://${dir.getCanonicalPath}'")
        val result7 = sql(s"SHOW TABLE EXTENDED IN $catalog.$namespace LIKE 'tb*'")
        assert(result7.schema.fieldNames ===
          Seq("namespace", "tableName", "isTemporary", "information"))
        assert(result7.collect()(0).length == 4)
        assert(result7.collect()(0)(1) === table)
        assert(result7.collect()(0)(2) === false)
        val actualResult7 = exclude(result7.collect()(0)(3).toString)
        val expectedResult7_v1 =
        s"""Catalog: $catalog
           |Database: $namespace
           |Table: $table
           |Type: MANAGED
           |Provider: parquet
           |Partition Provider: Catalog
           |Partition Columns: [`id`]
           |Schema: root
           | |-- data: string (nullable = true)
           | |-- id: long (nullable = true)""".stripMargin
        val expectedResult7_v2 =
          s"""Namespace: $namespace
             |Table: $table
             |Type: MANAGED
             |Provider: _
             |Owner: ${Utils.getCurrentUserName()}
             |Schema: root
             | |-- id: long (nullable = true)
             | |-- data: string (nullable = true)""".stripMargin
        val expectedResult7_hive =
          s"""Catalog: $catalog
             |Database: $namespace
             |Table: $table
             |Owner: ${Utils.getCurrentUserName()}
             |Type: MANAGED
             |Provider: hive
             |Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
             |InputFormat: org.apache.hadoop.mapred.TextInputFormat
             |OutputFormat: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
             |Storage Properties: [serialization.format=1]
             |Partition Provider: Catalog
             |Partition Columns: [`id`]
             |Schema: root
             | |-- data: string (nullable = true)
             | |-- id: long (nullable = true)""".stripMargin
        assert(actualResult7 === expectedResult7_v1 ||
          actualResult7 === expectedResult7_v2 ||
          actualResult7 === expectedResult7_hive)
        }
    }
  }

  test("show table extended from temp view") {
    val namespace = "ns"
    val table = "tbl"
    withNamespaceAndTable(namespace, table, catalog) { t =>
      sql(s"CREATE TABLE $t (id int) $defaultUsing")
      val viewName = "temp_view"
      withView(viewName) {
        sql(s"CREATE TEMPORARY VIEW $viewName AS SELECT id FROM $t")
        val result = sql(s"SHOW TABLE EXTENDED FROM $catalog.$namespace LIKE '$viewName'")
        assert(result.schema.fieldNames ===
          Seq("namespace", "tableName", "isTemporary", "information"))
        if (!result.collect().isEmpty) {
          assert(result.collect()(0)(1) === viewName)
          assert(result.collect()(0)(2) === true)
          val actualResult = exclude(result.collect()(0)(3).toString)
          val expectedResult_v1_hive =
            s"""Table: $viewName
               |Type: VIEW
               |View Text: SELECT id FROM $catalog.$namespace.$table
               |View Catalog and Namespace: $catalog.default
               |View Query Output Columns: [id]
               |Schema: root
               | |-- id: integer (nullable = true)""".stripMargin
          assert(actualResult === expectedResult_v1_hive)
        }
      }
    }
  }

  // Exclude some non-deterministic values for easy comparison of results,
  // such as `Created Time`, etc
  private def exclude(text: String): String = {
    text.split("\n").filter(line =>
      !line.startsWith("Created Time:") &&
        !line.startsWith("Last Access:") &&
        !line.startsWith("Created By:") &&
        !line.startsWith("Location:") &&
        !line.startsWith("Table Properties:") &&
        !line.startsWith("Partition Parameters:")).mkString("\n")
  }

  test("show tables from temp view") {
    val namespace = "ns"
    val table = "tbl"
    withNamespaceAndTable(namespace, table, catalog) { t =>
      sql(s"CREATE TABLE $t (id int) $defaultUsing")
      val viewName = "temp_view"
      withView(viewName) {
        sql(s"CREATE TEMPORARY VIEW $viewName AS SELECT id FROM $t")
        val result = sql(s"SHOW TABLES FROM $catalog.$namespace LIKE '$viewName'")
        assert(result.schema.fieldNames === Seq("namespace", "tableName", "isTemporary"))
        if (!result.collect().isEmpty) {
          assert(result.collect()(0)(1) === viewName)
          val expectedResult_v1_hive = true
          assert(result.collect()(0)(2) === expectedResult_v1_hive)
        }
      }
    }
  }
}
