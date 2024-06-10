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

  // the error class & error parameters of
  // `SHOW TABLE EXTENDED ... PARTITION ... in non-partitioned table`
  protected def extendedPartInNonPartedTableError(
      catalog: String,
      namespace: String,
      table: String): (String, Map[String, String])

  protected def extendedPartExpectedResult: String =
    "Partition Values: [id1=1, id2=2]"

  protected def namespaceKey: String = "Database"

  protected def extendedTableInfo: String

  protected def extendedTableSchema: String =
    s"""Schema: root
       | |-- data: string (nullable = true)
       | |-- id: long (nullable = true)""".stripMargin

  private def extendedTableExpectedResult(
      catalog: String,
      namespace: String,
      table: String,
      dataColName: String,
      partColName: String): String = {
    s"""Catalog: $catalog
       |$namespaceKey: $namespace
       |Table: $table
       |$extendedTableInfo
       |Partition Provider: Catalog
       |Partition Columns: [`$partColName`]
       |Schema: root
       | |-- $dataColName: string (nullable = true)
       | |-- $partColName: long (nullable = true)""".stripMargin
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

  test("show table extended with no matching table") {
    val namespace = "ns1"
    val table = "nonexist"
    withNamespaceAndTable(namespace, table, catalog) { _ =>
      val result = sql(s"SHOW TABLE EXTENDED IN $catalog.$namespace LIKE '*$table*'")
      assert(result.schema.fieldNames ===
        Seq("namespace", "tableName", "isTemporary", "information"))
      assert(result.collect().isEmpty)
    }
  }

  test("show table extended with a not existing partition") {
    val namespace = "ns1"
    val table = "tbl"
    withNamespaceAndTable(namespace, table, catalog) { tbl =>
      sql(s"CREATE TABLE $tbl (data string, id bigint) $defaultUsing PARTITIONED BY (id)")
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

  test("show table extended in non-partitioned table") {
    val namespace = "ns1"
    val table = "tbl"
    withNamespaceAndTable(namespace, table, catalog) { tbl =>
      sql(s"CREATE TABLE $tbl (data string, id bigint) $defaultUsing")
      val e = intercept[AnalysisException] {
        sql(s"SHOW TABLE EXTENDED IN $catalog.$namespace LIKE '$table' PARTITION(id = 1)")
      }
      val (errorClass, parameters) = extendedPartInNonPartedTableError(catalog, namespace, table)
      checkError(exception = e, errorClass = errorClass, parameters = parameters)
    }
  }

  test("show table extended in multi partition key - " +
    "the command's partition parameters are complete") {
    val namespace = "ns1"
    val table = "tbl"
    withNamespaceAndTable(namespace, table, catalog) { tbl =>
      sql(s"CREATE TABLE $tbl (data string, id1 bigint, id2 bigint) " +
        s"$defaultUsing PARTITIONED BY (id1, id2)")
      sql(s"ALTER TABLE $tbl ADD PARTITION (id1 = 1, id2 = 2)")

      val result = sql(s"SHOW TABLE EXTENDED FROM $catalog.$namespace " +
        s"LIKE '$table' PARTITION(id1 = 1, id2 = 2)")
      assert(result.schema.fieldNames ===
        Seq("namespace", "tableName", "isTemporary", "information"))
      val resultCollect = result.collect()
      assert(resultCollect(0).length == 4)
      assert(resultCollect(0)(0) === namespace)
      assert(resultCollect(0)(1) === table)
      assert(resultCollect(0)(2) === false)
      val actualResult = replace(resultCollect(0)(3).toString)
      assert(actualResult === extendedPartExpectedResult)
    }
  }

  test("show table extended in multi partition key - " +
    "the command's partition parameters are incomplete") {
    val namespace = "ns1"
    val table = "tbl"
    withNamespaceAndTable(namespace, table, catalog) { tbl =>
      sql(s"CREATE TABLE $tbl (data string, id1 bigint, id2 bigint) " +
        s"$defaultUsing PARTITIONED BY (id1, id2)")
      sql(s"ALTER TABLE $tbl ADD PARTITION (id1 = 1, id2 = 2)")

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
      sql(s"CREATE TABLE $catalog.$namespace.$table (data string, id bigint) " +
        s"$defaultUsing PARTITIONED BY (id)")
      val table1 = "tbl1"
      val table2 = "tbl2"
      withTable(table1, table2) {
        sql(s"CREATE TABLE $catalog.$namespace.$table1 (data1 string, id1 bigint) " +
          s"$defaultUsing PARTITIONED BY (id1)")
        sql(s"CREATE TABLE $catalog.$namespace.$table2 (data2 string, id2 bigint) " +
          s"$defaultUsing PARTITIONED BY (id2)")

        val result = sql(s"SHOW TABLE EXTENDED FROM $catalog.$namespace LIKE '$table*'")
          .sort("tableName")
        assert(result.schema.fieldNames ===
          Seq("namespace", "tableName", "isTemporary", "information"))
        val resultCollect = result.collect()
        assert(resultCollect.length == 3)

        assert(resultCollect(0).length == 4)
        assert(resultCollect(0)(1) === table)
        assert(resultCollect(0)(2) === false)
        // replace "Created Time", "Last Access", "Created By", "Location"
        val actualResult_0_3 = replace(resultCollect(0)(3).toString)
        val expectedResult_0_3 = extendedTableExpectedResult(
          catalog, namespace, table, "data", "id")
        assert(actualResult_0_3 === expectedResult_0_3)

        assert(resultCollect(1).length == 4)
        assert(resultCollect(1)(1) === table1)
        assert(resultCollect(1)(2) === false)
        val actualResult_1_3 = replace(resultCollect(1)(3).toString)
        // replace "Table Properties"
        val expectedResult_1_3 = extendedTableExpectedResult(
          catalog, namespace, table1, "data1", "id1")
        assert(actualResult_1_3 === expectedResult_1_3)

        assert(resultCollect(2).length == 4)
        assert(resultCollect(2)(1) === table2)
        assert(resultCollect(2)(2) === false)
        val actualResult_2_3 = replace(resultCollect(2)(3).toString)
        // replace "Table Properties"
        val expectedResult_2_3 = extendedTableExpectedResult(
          catalog, namespace, table2, "data2", "id2")
        assert(actualResult_2_3 === expectedResult_2_3)
      }
    }
  }

  test("show table extended with temp views") {
    val namespace = "ns"
    val table = "tbl"
    withNamespaceAndTable(namespace, table, catalog) { t =>
      sql(s"CREATE TABLE $t (id int) $defaultUsing")
      val viewName = table + "_view"
      val localTmpViewName = viewName + "_local_tmp"
      val globalTmpViewName = viewName + "_global_tmp"
      val globalNamespace = "global_temp"
      withView(localTmpViewName, globalNamespace + "." + globalTmpViewName) {
        sql(s"CREATE TEMPORARY VIEW $localTmpViewName AS SELECT id FROM $t")
        sql(s"CREATE GLOBAL TEMPORARY VIEW $globalTmpViewName AS SELECT id FROM $t")

        // temp local view
        val localResult = sql(s"SHOW TABLE EXTENDED LIKE '$viewName*'").sort("tableName")
        assert(localResult.schema.fieldNames ===
          Seq("namespace", "tableName", "isTemporary", "information"))
        val localResultCollect = localResult.collect()
        assert(localResultCollect.length == 1)
        assert(localResultCollect(0).length == 4)
        assert(localResultCollect(0)(1) === localTmpViewName)
        assert(localResultCollect(0)(2) === true)
        val actualLocalResult = replace(localResultCollect(0)(3).toString)
        val expectedLocalResult =
          s"""Table: $localTmpViewName
             |Created Time: <created time>
             |Last Access: <last access>
             |Created By: <created by>
             |Type: VIEW
             |View Text: SELECT id FROM $catalog.$namespace.$table
             |View Schema Mode: BINDING
             |View Catalog and Namespace: spark_catalog.default
             |View Query Output Columns: [id]
             |Schema: root
             | |-- id: integer (nullable = true)""".stripMargin
        assert(actualLocalResult === expectedLocalResult)

        // temp global view
        val globalResult = sql(s"SHOW TABLE EXTENDED IN global_temp LIKE '$viewName*'").
          sort("tableName")
        assert(globalResult.schema.fieldNames ===
          Seq("namespace", "tableName", "isTemporary", "information"))
        val globalResultCollect = globalResult.collect()
        assert(globalResultCollect.length == 2)

        assert(globalResultCollect(0).length == 4)
        assert(globalResultCollect(0)(1) === globalTmpViewName)
        assert(globalResultCollect(0)(2) === true)
        val actualGlobalResult1 = replace(globalResultCollect(0)(3).toString)
        val expectedGlobalResult1 =
          s"""Database: $globalNamespace
             |Table: $globalTmpViewName
             |Created Time: <created time>
             |Last Access: <last access>
             |Created By: <created by>
             |Type: VIEW
             |View Text: SELECT id FROM $catalog.$namespace.$table
             |View Schema Mode: BINDING
             |View Catalog and Namespace: spark_catalog.default
             |View Query Output Columns: [id]
             |Schema: root
             | |-- id: integer (nullable = true)""".stripMargin
        assert(actualGlobalResult1 === expectedGlobalResult1)

        assert(globalResultCollect(1).length == 4)
        assert(globalResultCollect(1)(1) === localTmpViewName)
        assert(globalResultCollect(1)(2) === true)
        val actualLocalResult2 = replace(globalResultCollect(1)(3).toString)
        val expectedLocalResult2 =
          s"""Table: $localTmpViewName
             |Created Time: <created time>
             |Last Access: <last access>
             |Created By: <created by>
             |Type: VIEW
             |View Text: SELECT id FROM $catalog.$namespace.$table
             |View Schema Mode: BINDING
             |View Catalog and Namespace: spark_catalog.default
             |View Query Output Columns: [id]
             |Schema: root
             | |-- id: integer (nullable = true)""".stripMargin
        assert(actualLocalResult2 === expectedLocalResult2)
      }
    }
  }

  // Replace some non-deterministic values with deterministic value
  // for easy comparison of results, such as `Created Time`, etc
  protected def replace(text: String): String = {
    text.split("\n").map {
      case s"Created Time:$_" => "Created Time: <created time>"
      case s"Last Access:$_" => "Last Access: <last access>"
      case s"Created By:$_" => "Created By: <created by>"
      case s"Location:$_" => "Location: <location>"
      case s"Table Properties:$_" => "Table Properties: <table properties>"
      case s"Partition Parameters:$_" => "Partition Parameters: <partition parameters>"
      case other => other
    }.mkString("\n")
  }

  /**
   * - V1: `show extended` and `select *` display the schema,
   *     the partition columns will be always displayed at the end.
   * - V2: `show extended` and `select *` display the schema,
   *     the columns order will respect the original table schema.
   */
  protected def selectCommandSchema: Array[String] = Array("data", "id")
  test("show table extended: the display order of the columns is different in v1 and v2") {
    val namespace = "ns1"
    val table = "tbl"
    withNamespaceAndTable(namespace, table, catalog) { _ =>
      sql(s"CREATE TABLE $catalog.$namespace.$table (data string, id bigint) " +
        s"$defaultUsing PARTITIONED BY (id)")
      sql(s"INSERT INTO $catalog.$namespace.$table PARTITION (id = 1) (data) VALUES ('data1')")
      val result = sql(s"SELECT * FROM $catalog.$namespace.$table")
      assert(result.schema.fieldNames === Array("data", "id"))

      val table1 = "tbl1"
      withTable(table1) {
        sql(s"CREATE TABLE $catalog.$namespace.$table1 (id bigint, data string) " +
          s"$defaultUsing PARTITIONED BY (id)")
        sql(s"INSERT INTO $catalog.$namespace.$table1 PARTITION (id = 1) (data) VALUES ('data2')")

        val result1 = sql(s"SELECT * FROM $catalog.$namespace.$table1")
        assert(result1.schema.fieldNames === selectCommandSchema)

        val extendedResult = sql(s"SHOW TABLE EXTENDED IN $catalog.$namespace LIKE '$table*'").
          sort("tableName")
        val extendedResultCollect = extendedResult.collect()

        assert(extendedResultCollect(0)(1) === table)
        assert(extendedResultCollect(0)(3).toString.contains(
          s"""Schema: root
             | |-- data: string (nullable = true)
             | |-- id: long (nullable = true)""".stripMargin))

        assert(extendedResultCollect(1)(1) === table1)
        assert(extendedResultCollect(1)(3).toString.contains(extendedTableSchema))
      }
    }
  }
}
