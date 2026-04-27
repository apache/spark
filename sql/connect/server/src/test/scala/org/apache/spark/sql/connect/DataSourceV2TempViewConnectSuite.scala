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

package org.apache.spark.sql.connect

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.classic
import org.apache.spark.sql.connector.catalog.{BufferedRows, Column, Identifier, InMemoryBaseTable, InMemoryTableCatalog, TableChange, TableInfo, TableWritePrivilege}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

/**
 * DSv2 temp view with stored plan tests for Spark Connect, mirroring the classic
 * DataSourceV2DataFrameSuite temp view scenarios.
 *
 * Uses an in-process Connect server ([[SparkConnectServerTest]]) so that the test can
 * access the server's catalog directly. A Connect client creates temp views and performs
 * SQL operations; external changes go through the catalog API, which bypasses the
 * Connect session's analysis.
 */
class DataSourceV2TempViewConnectSuite extends SparkConnectServerTest {

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    .set("spark.sql.catalog.testcat.copyOnLoad", "true")

  private val T = "testcat.ns1.ns2.tbl"
  private val ident = Identifier.of(Array("ns1", "ns2"), "tbl")

  /** Get the catalog from the server-side session. */
  private def serverCatalog(
      serverSession: classic.SparkSession): InMemoryTableCatalog =
    serverSession.sessionState.catalogManager
      .catalog("testcat").asInstanceOf[InMemoryTableCatalog]

  // Temp views with stored plans.
  // Each test creates a DSv2 table with initial data, builds a temp view with a filter
  // (to demonstrate that the stored plan is non-trivial), and then verifies the view
  // behavior after various table modifications (session or external).

  // Scenario 1: session and external writes.
  test("[connect] temp view with stored plan reflects session write") {
    withSession { s =>
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

      s.table(T).filter("salary < 999").createOrReplaceTempView("v")
      val serverSession = getServerSession(s)
      checkAnswer(serverSession.table("v"), Seq(Row(1, 100)))

      s.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

      checkAnswer(serverSession.table("v"), Seq(Row(1, 100), Row(2, 200)))

      s.sql("DROP VIEW IF EXISTS v").collect()
      s.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  test("[connect] temp view with stored plan reflects external write") {
    withSession { s =>
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

      s.table(T).filter("salary < 999").createOrReplaceTempView("v")
      val serverSession = getServerSession(s)
      checkAnswer(serverSession.table("v"), Seq(Row(1, 100)))

      // external writer adds (2, 200) via direct catalog API
      val schema = StructType.fromDDL("id INT, salary INT")
      val cat = serverCatalog(serverSession)
      val extTable = cat
        .loadTable(ident, util.Set.of(TableWritePrivilege.INSERT))
        .asInstanceOf[InMemoryBaseTable]
      extTable.withData(Array(
        new BufferedRows(Seq.empty, schema).withRow(InternalRow(2, 200))))

      checkAnswer(serverSession.table("v"), Seq(Row(1, 100), Row(2, 200)))

      s.sql("DROP VIEW IF EXISTS v").collect()
      s.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 2: adding new columns and data.
  test("[connect] temp view with stored plan preserves schema after session ADD COLUMN") {
    withSession { s =>
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

      s.table(T).filter("salary < 999").createOrReplaceTempView("v")
      val serverSession = getServerSession(s)
      checkAnswer(serverSession.table("v"), Seq(Row(1, 100)))

      s.sql(s"ALTER TABLE $T ADD COLUMN new_column INT").collect()
      s.sql(s"INSERT INTO $T VALUES (2, 200, -1)").collect()

      // view preserves original 2-column schema, filter still applied
      checkAnswer(serverSession.table("v"), Seq(Row(1, 100), Row(2, 200)))

      s.sql("DROP VIEW IF EXISTS v").collect()
      s.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  test("[connect] temp view with stored plan preserves schema after external ADD COLUMN") {
    withSession { s =>
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

      s.table(T).filter("salary < 999").createOrReplaceTempView("v")
      val serverSession = getServerSession(s)
      checkAnswer(serverSession.table("v"), Seq(Row(1, 100)))

      // external schema change via catalog API
      val cat = serverCatalog(serverSession)
      val addCol = TableChange.addColumn(Array("new_column"), IntegerType, true)
      cat.alterTable(ident, addCol)

      // external writer adds data with new schema
      val schema3 = StructType.fromDDL("id INT, salary INT, new_column INT")
      val extTable = cat
        .loadTable(ident, util.Set.of(TableWritePrivilege.INSERT))
        .asInstanceOf[InMemoryBaseTable]
      extTable.withData(Array(
        new BufferedRows(Seq.empty, schema3).withRow(InternalRow(2, 200, -1))))

      // view preserves original 2-column schema, filter still applied
      checkAnswer(serverSession.table("v"), Seq(Row(1, 100), Row(2, 200)))

      s.sql("DROP VIEW IF EXISTS v").collect()
      s.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 3: removing existing columns.
  test("[connect] temp view with stored plan detects session column removal") {
    withSession { s =>
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

      s.table(T).filter("salary < 999").createOrReplaceTempView("v")
      val serverSession = getServerSession(s)
      checkAnswer(serverSession.table("v"), Seq(Row(1, 100)))

      s.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()

      checkError(
        exception = intercept[AnalysisException] {
          serverSession.table("v").collect()
        },
        condition = "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION",
        parameters = Map(
          "viewName" -> "`v`",
          "tableName" -> "`testcat`.`ns1`.`ns2`.`tbl`",
          "colType" -> "data",
          "errors" -> "- `salary` INT has been removed"))

      s.sql("DROP VIEW IF EXISTS v").collect()
      s.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  test("[connect] temp view with stored plan detects external column removal") {
    withSession { s =>
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

      s.table(T).filter("salary < 999").createOrReplaceTempView("v")
      val serverSession = getServerSession(s)
      checkAnswer(serverSession.table("v"), Seq(Row(1, 100)))

      // external schema change via catalog API
      val cat = serverCatalog(serverSession)
      val dropCol = TableChange.deleteColumn(Array("salary"), false)
      cat.alterTable(ident, dropCol)

      checkError(
        exception = intercept[AnalysisException] {
          serverSession.table("v").collect()
        },
        condition = "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION",
        parameters = Map(
          "viewName" -> "`v`",
          "tableName" -> "`testcat`.`ns1`.`ns2`.`tbl`",
          "colType" -> "data",
          "errors" -> "- `salary` INT has been removed"))

      s.sql("DROP VIEW IF EXISTS v").collect()
      s.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 4: drop and re-create table.
  test("[connect] temp view with stored plan resolves to session recreated table") {
    withSession { s =>
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

      s.table(T).filter("salary < 999").createOrReplaceTempView("v")
      val serverSession = getServerSession(s)
      checkAnswer(serverSession.table("v"), Seq(Row(1, 100)))

      s.sql(s"DROP TABLE $T").collect()
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()

      // view resolves to the new empty table
      checkAnswer(serverSession.table("v"), Seq.empty)

      // insert new data and verify the view picks it up
      s.sql(s"INSERT INTO $T VALUES (2, 200)").collect()
      checkAnswer(serverSession.table("v"), Seq(Row(2, 200)))

      s.sql("DROP VIEW IF EXISTS v").collect()
      s.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  test("[connect] temp view with stored plan resolves to externally recreated table") {
    withSession { s =>
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

      s.table(T).filter("salary < 999").createOrReplaceTempView("v")
      val serverSession = getServerSession(s)
      checkAnswer(serverSession.table("v"), Seq(Row(1, 100)))

      val cat = serverCatalog(serverSession)
      val originalTableId = cat.loadTable(ident).id

      // external drop and recreate via catalog API
      cat.dropTable(ident)
      cat.createTable(
        ident,
        new TableInfo.Builder()
          .withColumns(Array(
            Column.create("id", IntegerType),
            Column.create("salary", IntegerType)))
          .build())

      val newTableId = cat.loadTable(ident).id
      assert(originalTableId != newTableId)

      // view resolves to the new empty table
      checkAnswer(serverSession.table("v"), Seq.empty)

      // insert new data and verify the view picks it up
      s.sql(s"INSERT INTO $T VALUES (2, 200)").collect()
      checkAnswer(serverSession.table("v"), Seq(Row(2, 200)))

      s.sql("DROP VIEW IF EXISTS v").collect()
      s.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 5: drop and re-add column with the same name and type.
  test("[connect] temp view with stored plan after session drop and re-add column same type") {
    withSession { s =>
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

      s.table(T).filter("salary < 999").createOrReplaceTempView("v")
      s.table(T).createOrReplaceTempView("v_unfiltered")
      val serverSession = getServerSession(s)
      checkAnswer(serverSession.table("v"), Seq(Row(1, 100)))

      // drop and re-add column with same name and type
      s.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()
      s.sql(s"ALTER TABLE $T ADD COLUMN salary INT").collect()

      // salary data is no longer preserved after drop and re-add
      // null < 999 evaluates to null (falsy), so no rows pass the filter
      checkAnswer(serverSession.table("v"), Seq.empty)
      // unfiltered view shows all rows with null salary
      checkAnswer(
        serverSession.table("v_unfiltered"),
        Seq(Row(1, null), Row(10, null)))

      s.sql("DROP VIEW IF EXISTS v").collect()
      s.sql("DROP VIEW IF EXISTS v_unfiltered").collect()
      s.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  test("[connect] temp view with stored plan after external drop and re-add column same type") {
    withSession { s =>
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

      s.table(T).filter("salary < 999").createOrReplaceTempView("v")
      s.table(T).createOrReplaceTempView("v_unfiltered")
      val serverSession = getServerSession(s)
      checkAnswer(serverSession.table("v"), Seq(Row(1, 100)))

      // external drop and re-add column via catalog API (two separate calls,
      // matching how separate ALTER TABLE statements work in practice)
      val cat = serverCatalog(serverSession)
      val dropCol = TableChange.deleteColumn(Array("salary"), false)
      cat.alterTable(ident, dropCol)
      val addCol = TableChange.addColumn(Array("salary"), IntegerType, true)
      cat.alterTable(ident, addCol)

      // salary data is no longer preserved after drop and re-add
      checkAnswer(serverSession.table("v"), Seq.empty)
      // unfiltered view shows all rows with null salary
      checkAnswer(
        serverSession.table("v_unfiltered"),
        Seq(Row(1, null), Row(10, null)))

      s.sql("DROP VIEW IF EXISTS v").collect()
      s.sql("DROP VIEW IF EXISTS v_unfiltered").collect()
      s.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 6: drop and re-add column with the same name but different type.
  test("[connect] temp view with stored plan detects session column type change") {
    withSession { s =>
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

      s.table(T).filter("salary < 999").createOrReplaceTempView("v")
      val serverSession = getServerSession(s)
      checkAnswer(serverSession.table("v"), Seq(Row(1, 100)))

      // drop and re-add column with same name but different type
      s.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()
      s.sql(s"ALTER TABLE $T ADD COLUMN salary STRING").collect()

      checkError(
        exception = intercept[AnalysisException] {
          serverSession.table("v").collect()
        },
        condition = "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION",
        parameters = Map(
          "viewName" -> "`v`",
          "tableName" -> "`testcat`.`ns1`.`ns2`.`tbl`",
          "colType" -> "data",
          "errors" -> "- `salary` type has changed from INT to STRING"))

      s.sql("DROP VIEW IF EXISTS v").collect()
      s.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  test("[connect] temp view with stored plan detects external column type change") {
    withSession { s =>
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

      s.table(T).filter("salary < 999").createOrReplaceTempView("v")
      val serverSession = getServerSession(s)
      checkAnswer(serverSession.table("v"), Seq(Row(1, 100)))

      // external drop and re-add column with different type via catalog API
      val cat = serverCatalog(serverSession)
      val dropCol = TableChange.deleteColumn(Array("salary"), false)
      cat.alterTable(ident, dropCol)
      val addCol = TableChange.addColumn(Array("salary"), StringType, true)
      cat.alterTable(ident, addCol)

      checkError(
        exception = intercept[AnalysisException] {
          serverSession.table("v").collect()
        },
        condition = "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION",
        parameters = Map(
          "viewName" -> "`v`",
          "tableName" -> "`testcat`.`ns1`.`ns2`.`tbl`",
          "colType" -> "data",
          "errors" -> "- `salary` type has changed from INT to STRING"))

      s.sql("DROP VIEW IF EXISTS v").collect()
      s.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 7: type widening from INT to BIGINT.
  test("[connect] temp view with stored plan detects type widening") {
    withSession { s =>
      s.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      s.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

      s.table(T).filter("salary < 999").createOrReplaceTempView("v")
      val serverSession = getServerSession(s)
      checkAnswer(serverSession.table("v"), Seq(Row(1, 100)))

      // widen salary type from INT to BIGINT via catalog API
      val cat = serverCatalog(serverSession)
      val updateType =
        TableChange.updateColumnType(Array("salary"), LongType)
      cat.alterTable(ident, updateType)

      checkError(
        exception = intercept[AnalysisException] {
          serverSession.table("v").collect()
        },
        condition = "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION",
        parameters = Map(
          "viewName" -> "`v`",
          "tableName" -> "`testcat`.`ns1`.`ns2`.`tbl`",
          "colType" -> "data",
          "errors" -> "- `salary` type has changed from INT to BIGINT"))

      s.sql("DROP VIEW IF EXISTS v").collect()
      s.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }
}
