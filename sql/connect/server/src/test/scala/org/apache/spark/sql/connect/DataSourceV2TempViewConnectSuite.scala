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
import org.apache.spark.sql.connector.catalog.{BufferedRows, CachingInMemoryTableCatalog, Column, Identifier, InMemoryBaseTable, InMemoryTableCatalog, TableChange, TableInfo, TableWritePrivilege}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

/**
 * DSv2 temp view with stored plan tests for Spark Connect, mirroring the classic
 * DataSourceV2DataFrameSuite temp view scenarios.
 *
 * Uses an in-process Connect server ([[SparkConnectServerTest]]) so that the test can access the
 * server's catalog directly for external changes. All data reads go through the Connect client
 * session to simulate the real client experience.
 */
class DataSourceV2TempViewConnectSuite extends SparkConnectServerTest {

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    .set("spark.sql.catalog.testcat.copyOnLoad", "true")
    .set("spark.sql.catalog.cachingcat", classOf[CachingInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.cachingcat.copyOnLoad", "true")

  private val T = "testcat.ns1.ns2.tbl"
  private val CT = "cachingcat.ns1.ns2.tbl"
  private val ident = Identifier.of(Array("ns1", "ns2"), "tbl")

  /**
   * Assert that rows collected through the Connect client match expected rows (order-agnostic).
   */
  private def assertRows(actual: Array[Row], expected: Seq[Row]): Unit = {
    assert(
      actual.map(_.toString()).toSet == expected.map(_.toString()).toSet,
      s"Expected ${expected.mkString(", ")} but got ${actual.mkString(", ")}")
  }

  /** Get the catalog from the server-side session. */
  private def serverCatalog(serverSession: classic.SparkSession): InMemoryTableCatalog =
    serverSession.sessionState.catalogManager
      .catalog("testcat")
      .asInstanceOf[InMemoryTableCatalog]

  /** Get the caching catalog from the server-side session. */
  private def serverCachingCatalog(
      serverSession: classic.SparkSession): CachingInMemoryTableCatalog =
    serverSession.sessionState.catalogManager
      .catalog("cachingcat")
      .asInstanceOf[CachingInMemoryTableCatalog]

  // Temp views with stored plans: scenarios from the DSv2 table refresh tests.
  // Each test creates a DSv2 table with initial data, builds a temp view with a filter
  // (to demonstrate that the stored plan is non-trivial), and then verifies the view
  // behavior after various table modifications (session or external).

  // Scenario 1.1 (session write)
  test("[connect] temp view with stored plan reflects session write") {
    withSession { session =>
      session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      session.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

      session.table(T).filter("salary < 999").createOrReplaceTempView("v")
      assertRows(session.table("v").collect(), Seq(Row(1, 100)))

      session.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

      assertRows(session.table("v").collect(), Seq(Row(1, 100), Row(2, 200)))

      session.sql("DROP VIEW IF EXISTS v").collect()
      session.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 1.2 (external write)
  test("[connect] temp view with stored plan reflects external write") {
    withSession { session =>
      session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      session.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

      session.table(T).filter("salary < 999").createOrReplaceTempView("v")
      val serverSession = getServerSession(session)
      assertRows(session.table("v").collect(), Seq(Row(1, 100)))

      // external writer adds (2, 200) via direct catalog API
      val schema = StructType.fromDDL("id INT, salary INT")
      val cat = serverCatalog(serverSession)
      val extTable = cat
        .loadTable(ident, util.Set.of(TableWritePrivilege.INSERT))
        .asInstanceOf[InMemoryBaseTable]
      extTable.withData(Array(new BufferedRows(Seq.empty, schema).withRow(InternalRow(2, 200))))

      assertRows(session.table("v").collect(), Seq(Row(1, 100), Row(2, 200)))

      session.sql("DROP VIEW IF EXISTS v").collect()
      session.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 1.2 connector w/ cache (external write, caching connector)
  test("[connect] connector w/ cache: temp view stale after external write") {
    withSession { session =>
      session.sql(s"CREATE TABLE $CT (id INT, salary INT) USING foo").collect()
      session.sql(s"INSERT INTO $CT VALUES (1, 100), (10, 1000)").collect()

      session.table(CT).filter("salary < 999").createOrReplaceTempView("v")
      assertRows(session.table("v").collect(), Seq(Row(1, 100)))

      val serverSession = getServerSession(session)
      val cat = serverCachingCatalog(serverSession)
      val schema = StructType.fromDDL("id INT, salary INT")
      val extTable = cat
        .loadTable(ident, util.Set.of(TableWritePrivilege.INSERT))
        .asInstanceOf[InMemoryBaseTable]
      extTable.withData(Array(new BufferedRows(Seq.empty, schema).withRow(InternalRow(2, 200))))

      // Caching connector returns stale table: external write invisible
      assertRows(session.table("v").collect(), Seq(Row(1, 100)))

      // REFRESH TABLE invalidates the connector cache, external write becomes visible
      session.sql(s"REFRESH TABLE $CT").collect()
      assertRows(session.table("v").collect(), Seq(Row(1, 100), Row(2, 200)))

      session.sql("DROP VIEW IF EXISTS v").collect()
      session.sql(s"DROP TABLE IF EXISTS $CT").collect()
      CachingInMemoryTableCatalog.clearCache()
    }
  }

  // Scenario 2.1 (session ADD COLUMN)
  test("[connect] temp view with stored plan preserves schema after session ADD COLUMN") {
    withSession { session =>
      session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      session.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

      session.table(T).filter("salary < 999").createOrReplaceTempView("v")
      assertRows(session.table("v").collect(), Seq(Row(1, 100)))

      session.sql(s"ALTER TABLE $T ADD COLUMN new_column INT").collect()
      session.sql(s"INSERT INTO $T VALUES (2, 200, -1)").collect()

      // view preserves original 2-column schema, filter still applied
      assertRows(session.table("v").collect(), Seq(Row(1, 100), Row(2, 200)))

      session.sql("DROP VIEW IF EXISTS v").collect()
      session.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 2.2 (external ADD COLUMN)
  test("[connect] temp view with stored plan preserves schema after external ADD COLUMN") {
    withSession { session =>
      session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      session.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

      session.table(T).filter("salary < 999").createOrReplaceTempView("v")
      val serverSession = getServerSession(session)
      assertRows(session.table("v").collect(), Seq(Row(1, 100)))

      // external schema change via catalog API
      val cat = serverCatalog(serverSession)
      val addCol = TableChange.addColumn(Array("new_column"), IntegerType, true)
      cat.alterTable(ident, addCol)

      // external writer adds data with new schema
      val schema3 = StructType.fromDDL("id INT, salary INT, new_column INT")
      val extTable = cat
        .loadTable(ident, util.Set.of(TableWritePrivilege.INSERT))
        .asInstanceOf[InMemoryBaseTable]
      extTable.withData(
        Array(new BufferedRows(Seq.empty, schema3).withRow(InternalRow(2, 200, -1))))

      // view preserves original 2-column schema, filter still applied
      assertRows(session.table("v").collect(), Seq(Row(1, 100), Row(2, 200)))

      session.sql("DROP VIEW IF EXISTS v").collect()
      session.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 2.2 connector w/ cache (external ADD COLUMN, caching connector)
  test("[connect] connector w/ cache: temp view stale after external ADD COLUMN") {
    withSession { session =>
      session.sql(s"CREATE TABLE $CT (id INT, salary INT) USING foo").collect()
      session.sql(s"INSERT INTO $CT VALUES (1, 100), (10, 1000)").collect()

      session.table(CT).filter("salary < 999").createOrReplaceTempView("v")
      assertRows(session.table("v").collect(), Seq(Row(1, 100)))

      val serverSession = getServerSession(session)
      val cat = serverCachingCatalog(serverSession)
      val addCol = TableChange.addColumn(Array("new_column"), IntegerType, true)
      cat.alterTable(ident, addCol)

      val schema3 = StructType.fromDDL("id INT, salary INT, new_column INT")
      val extTable = cat
        .loadTable(ident, util.Set.of(TableWritePrivilege.INSERT))
        .asInstanceOf[InMemoryBaseTable]
      extTable.withData(
        Array(new BufferedRows(Seq.empty, schema3).withRow(InternalRow(2, 200, -1))))

      // Caching connector returns stale table: external changes invisible
      assertRows(session.table("v").collect(), Seq(Row(1, 100)))

      // REFRESH TABLE invalidates the connector cache, view preserves original 2-column schema
      session.sql(s"REFRESH TABLE $CT").collect()
      assertRows(session.table("v").collect(), Seq(Row(1, 100), Row(2, 200)))

      session.sql("DROP VIEW IF EXISTS v").collect()
      session.sql(s"DROP TABLE IF EXISTS $CT").collect()
      CachingInMemoryTableCatalog.clearCache()
    }
  }

  // Scenario 3.2 (external column removal)
  test("[connect] temp view with stored plan detects external column removal") {
    withSession { session =>
      session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      session.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

      session.table(T).filter("salary < 999").createOrReplaceTempView("v")
      val serverSession = getServerSession(session)
      assertRows(session.table("v").collect(), Seq(Row(1, 100)))

      // external schema change via catalog API
      val cat = serverCatalog(serverSession)
      val dropCol = TableChange.deleteColumn(Array("salary"), false)
      cat.alterTable(ident, dropCol)

      checkError(
        exception = intercept[AnalysisException] {
          session.table("v").collect()
        },
        condition = "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION",
        parameters = Map(
          "viewName" -> "`v`",
          "tableName" -> "`testcat`.`ns1`.`ns2`.`tbl`",
          "colType" -> "data",
          "errors" -> "- `salary` INT has been removed"))

      session.sql("DROP VIEW IF EXISTS v").collect()
      session.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 3.2 connector w/ cache (external column removal, caching connector)
  test("[connect] connector w/ cache: temp view stale after external column removal") {
    withSession { session =>
      session.sql(s"CREATE TABLE $CT (id INT, salary INT) USING foo").collect()
      session.sql(s"INSERT INTO $CT VALUES (1, 100), (10, 1000)").collect()

      session.table(CT).filter("salary < 999").createOrReplaceTempView("v")
      assertRows(session.table("v").collect(), Seq(Row(1, 100)))

      val serverSession = getServerSession(session)
      val cat = serverCachingCatalog(serverSession)
      val dropCol = TableChange.deleteColumn(Array("salary"), false)
      cat.alterTable(ident, dropCol)

      // Caching connector returns stale table: column removal invisible, no error
      assertRows(session.table("v").collect(), Seq(Row(1, 100)))

      // REFRESH TABLE invalidates the connector cache, column removal detected
      session.sql(s"REFRESH TABLE $CT").collect()
      checkError(
        exception = intercept[AnalysisException] {
          session.table("v").collect()
        },
        condition = "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION",
        parameters = Map(
          "viewName" -> "`v`",
          "tableName" -> "`cachingcat`.`ns1`.`ns2`.`tbl`",
          "colType" -> "data",
          "errors" -> "- `salary` INT has been removed"))

      session.sql("DROP VIEW IF EXISTS v").collect()
      session.sql(s"DROP TABLE IF EXISTS $CT").collect()
      CachingInMemoryTableCatalog.clearCache()
    }
  }

  // Scenario 4.1 (session drop and recreate table)
  test("[connect] temp view with stored plan resolves to session-recreated table") {
    withSession { session =>
      session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      session.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

      session.table(T).filter("salary < 999").createOrReplaceTempView("v")
      val serverSession = getServerSession(session)
      assertRows(session.table("v").collect(), Seq(Row(1, 100)))

      val cat = serverCatalog(serverSession)
      val originalTableId = cat.loadTable(ident).id

      // session drop and recreate via SQL
      session.sql(s"DROP TABLE $T").collect()
      session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()

      val newTableId = cat.loadTable(ident).id
      assert(originalTableId != newTableId)

      // view resolves to the new empty table
      assert(session.table("v").schema.fieldNames.toSeq == Seq("id", "salary"))
      assertRows(session.table("v").collect(), Seq.empty)

      // insert new data and verify the view picks it up
      session.sql(s"INSERT INTO $T VALUES (2, 200)").collect()
      assertRows(session.table("v").collect(), Seq(Row(2, 200)))

      session.sql("DROP VIEW IF EXISTS v").collect()
      session.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 4.2 (external drop and recreate table)
  test("[connect] temp view with stored plan resolves to externally recreated table") {
    withSession { session =>
      session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      session.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

      session.table(T).filter("salary < 999").createOrReplaceTempView("v")
      val serverSession = getServerSession(session)
      assertRows(session.table("v").collect(), Seq(Row(1, 100)))

      val cat = serverCatalog(serverSession)
      val originalTableId = cat.loadTable(ident).id

      // external drop and recreate via catalog API
      cat.dropTable(ident)
      cat.createTable(
        ident,
        new TableInfo.Builder()
          .withColumns(
            Array(Column.create("id", IntegerType), Column.create("salary", IntegerType)))
          .build())

      val newTableId = cat.loadTable(ident).id
      assert(originalTableId != newTableId)

      // view resolves to the new empty table
      assert(session.table("v").schema.fieldNames.toSeq == Seq("id", "salary"))
      assertRows(session.table("v").collect(), Seq.empty)

      // insert new data and verify the view picks it up
      session.sql(s"INSERT INTO $T VALUES (2, 200)").collect()
      assertRows(session.table("v").collect(), Seq(Row(2, 200)))

      session.sql("DROP VIEW IF EXISTS v").collect()
      session.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 4.2 connector w/ cache (external drop/recreate, caching connector)
  test("[connect] connector w/ cache: temp view stale after external drop/recreate") {
    withSession { session =>
      session.sql(s"CREATE TABLE $CT (id INT, salary INT) USING foo").collect()
      session.sql(s"INSERT INTO $CT VALUES (1, 100), (10, 1000)").collect()

      session.table(CT).filter("salary < 999").createOrReplaceTempView("v")
      assertRows(session.table("v").collect(), Seq(Row(1, 100)))

      val serverSession = getServerSession(session)
      val cat = serverCachingCatalog(serverSession)
      cat.dropTable(ident)
      cat.createTable(
        ident,
        new TableInfo.Builder()
          .withColumns(
            Array(Column.create("id", IntegerType), Column.create("salary", IntegerType)))
          .build())

      // Caching connector returns stale table: drop/recreate invisible
      assertRows(session.table("v").collect(), Seq(Row(1, 100)))

      // REFRESH TABLE invalidates the connector cache, view resolves to new empty table
      session.sql(s"REFRESH TABLE $CT").collect()
      assert(session.table("v").schema.fieldNames.toSeq == Seq("id", "salary"))
      assertRows(session.table("v").collect(), Seq.empty)

      session.sql("DROP VIEW IF EXISTS v").collect()
      session.sql(s"DROP TABLE IF EXISTS $CT").collect()
      CachingInMemoryTableCatalog.clearCache()
    }
  }

  // Scenario 5.1 (session drop and re-add column with same type)
  test("[connect] temp view with stored plan after session drop and re-add column same type") {
    withSession { session =>
      session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      session.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

      session.table(T).filter("salary < 999").createOrReplaceTempView("v")
      session.table(T).createOrReplaceTempView("v_no_filter")
      session.table(T).filter("salary IS NULL").createOrReplaceTempView("v_filter_is_null")
      assertRows(session.table("v").collect(), Seq(Row(1, 100)))
      assertRows(session.table("v_no_filter").collect(), Seq(Row(1, 100), Row(10, 1000)))
      assert(session.table("v_filter_is_null").schema.fieldNames.toSeq == Seq("id", "salary"))
      assertRows(session.table("v_filter_is_null").collect(), Seq.empty)

      // drop and re-add column with same name and type
      session.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()
      session.sql(s"ALTER TABLE $T ADD COLUMN salary INT").collect()

      // salary values are now null, so the filtered view returns nothing
      assert(session.table("v").schema.fieldNames.toSeq == Seq("id", "salary"))
      assertRows(session.table("v").collect(), Seq.empty)
      // unfiltered view returns rows with null salary
      assertRows(session.table("v_no_filter").collect(), Seq(Row(1, null), Row(10, null)))
      // IS NULL filter now matches all rows
      assertRows(session.table("v_filter_is_null").collect(), Seq(Row(1, null), Row(10, null)))

      session.sql("DROP VIEW IF EXISTS v_filter_is_null").collect()
      session.sql("DROP VIEW IF EXISTS v_no_filter").collect()
      session.sql("DROP VIEW IF EXISTS v").collect()
      session.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 5.2 (external drop and re-add column with same type)
  test("[connect] temp view with stored plan after external drop and re-add column same type") {
    withSession { session =>
      session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      session.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

      session.table(T).filter("salary < 999").createOrReplaceTempView("v")
      session.table(T).createOrReplaceTempView("v_no_filter")
      session.table(T).filter("salary IS NULL").createOrReplaceTempView("v_filter_is_null")
      val serverSession = getServerSession(session)
      assertRows(session.table("v").collect(), Seq(Row(1, 100)))
      assertRows(session.table("v_no_filter").collect(), Seq(Row(1, 100), Row(10, 1000)))
      assert(session.table("v_filter_is_null").schema.fieldNames.toSeq == Seq("id", "salary"))
      assertRows(session.table("v_filter_is_null").collect(), Seq.empty)

      // external drop and re-add column via catalog API
      val cat = serverCatalog(serverSession)
      val dropCol = TableChange.deleteColumn(Array("salary"), false)
      val addCol = TableChange.addColumn(Array("salary"), IntegerType, true)
      cat.alterTable(ident, dropCol, addCol)

      // salary values are now null, so the filtered view returns nothing
      assert(session.table("v").schema.fieldNames.toSeq == Seq("id", "salary"))
      assertRows(session.table("v").collect(), Seq.empty)
      // unfiltered view returns rows with null salary
      assertRows(session.table("v_no_filter").collect(), Seq(Row(1, null), Row(10, null)))
      // IS NULL filter now matches all rows
      assertRows(session.table("v_filter_is_null").collect(), Seq(Row(1, null), Row(10, null)))

      session.sql("DROP VIEW IF EXISTS v_filter_is_null").collect()
      session.sql("DROP VIEW IF EXISTS v_no_filter").collect()
      session.sql("DROP VIEW IF EXISTS v").collect()
      session.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 5.2 connector w/ cache (external drop/re-add column, caching connector)
  test("[connect] connector w/ cache: temp view stale after external drop/re-add column") {
    withSession { session =>
      session.sql(s"CREATE TABLE $CT (id INT, salary INT) USING foo").collect()
      session.sql(s"INSERT INTO $CT VALUES (1, 100), (10, 1000)").collect()

      session.table(CT).filter("salary < 999").createOrReplaceTempView("v")
      assertRows(session.table("v").collect(), Seq(Row(1, 100)))

      val serverSession = getServerSession(session)
      val cat = serverCachingCatalog(serverSession)
      val dropCol = TableChange.deleteColumn(Array("salary"), false)
      val addCol = TableChange.addColumn(Array("salary"), IntegerType, true)
      cat.alterTable(ident, dropCol, addCol)

      // Caching connector returns stale table: column drop/re-add invisible
      assertRows(session.table("v").collect(), Seq(Row(1, 100)))

      // REFRESH TABLE invalidates the connector cache, salary values are null
      session.sql(s"REFRESH TABLE $CT").collect()
      assert(session.table("v").schema.fieldNames.toSeq == Seq("id", "salary"))
      assertRows(session.table("v").collect(), Seq.empty)

      session.sql("DROP VIEW IF EXISTS v").collect()
      session.sql(s"DROP TABLE IF EXISTS $CT").collect()
      CachingInMemoryTableCatalog.clearCache()
    }
  }

  // Scenario 6.1 (session drop and re-add column with different type)
  test("[connect] temp view with stored plan detects session column type change") {
    withSession { session =>
      session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      session.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

      session.table(T).filter("salary < 999").createOrReplaceTempView("v")
      assertRows(session.table("v").collect(), Seq(Row(1, 100)))

      // drop and re-add column with same name but different type
      session.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()
      session.sql(s"ALTER TABLE $T ADD COLUMN salary STRING").collect()

      checkError(
        exception = intercept[AnalysisException] {
          session.table("v").collect()
        },
        condition = "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION",
        parameters = Map(
          "viewName" -> "`v`",
          "tableName" -> "`testcat`.`ns1`.`ns2`.`tbl`",
          "colType" -> "data",
          "errors" -> "- `salary` type has changed from INT to STRING"))

      session.sql("DROP VIEW IF EXISTS v").collect()
      session.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 6.2 (external drop and re-add column with different type)
  test("[connect] temp view with stored plan detects external column type change") {
    withSession { session =>
      session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      session.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

      session.table(T).filter("salary < 999").createOrReplaceTempView("v")
      val serverSession = getServerSession(session)
      assertRows(session.table("v").collect(), Seq(Row(1, 100)))

      // external drop and re-add column with different type via catalog API
      val cat = serverCatalog(serverSession)
      val dropCol = TableChange.deleteColumn(Array("salary"), false)
      val addCol = TableChange.addColumn(Array("salary"), StringType, true)
      cat.alterTable(ident, dropCol, addCol)

      checkError(
        exception = intercept[AnalysisException] {
          session.table("v").collect()
        },
        condition = "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION",
        parameters = Map(
          "viewName" -> "`v`",
          "tableName" -> "`testcat`.`ns1`.`ns2`.`tbl`",
          "colType" -> "data",
          "errors" -> "- `salary` type has changed from INT to STRING"))

      session.sql("DROP VIEW IF EXISTS v").collect()
      session.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

  // Scenario 6.2 connector w/ cache (external column type change, caching connector)
  test("[connect] connector w/ cache: temp view stale after external column type change") {
    withSession { session =>
      session.sql(s"CREATE TABLE $CT (id INT, salary INT) USING foo").collect()
      session.sql(s"INSERT INTO $CT VALUES (1, 100), (10, 1000)").collect()

      session.table(CT).filter("salary < 999").createOrReplaceTempView("v")
      assertRows(session.table("v").collect(), Seq(Row(1, 100)))

      val serverSession = getServerSession(session)
      val cat = serverCachingCatalog(serverSession)
      val dropCol = TableChange.deleteColumn(Array("salary"), false)
      val addCol = TableChange.addColumn(Array("salary"), StringType, true)
      cat.alterTable(ident, dropCol, addCol)

      // Caching connector returns stale table: type change invisible, no error
      assertRows(session.table("v").collect(), Seq(Row(1, 100)))

      // REFRESH TABLE invalidates the connector cache, type change detected
      session.sql(s"REFRESH TABLE $CT").collect()
      checkError(
        exception = intercept[AnalysisException] {
          session.table("v").collect()
        },
        condition = "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION",
        parameters = Map(
          "viewName" -> "`v`",
          "tableName" -> "`cachingcat`.`ns1`.`ns2`.`tbl`",
          "colType" -> "data",
          "errors" -> "- `salary` type has changed from INT to STRING"))

      session.sql("DROP VIEW IF EXISTS v").collect()
      session.sql(s"DROP TABLE IF EXISTS $CT").collect()
      CachingInMemoryTableCatalog.clearCache()
    }
  }

  // Scenario 7 (type widening from INT to BIGINT)
  test("[connect] temp view with stored plan detects type widening") {
    withSession { session =>
      session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      session.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

      session.table(T).filter("salary < 999").createOrReplaceTempView("v")
      val serverSession = getServerSession(session)
      assertRows(session.table("v").collect(), Seq(Row(1, 100)))

      // widen salary type from INT to BIGINT via catalog API
      val cat = serverCatalog(serverSession)
      val updateType =
        TableChange.updateColumnType(Array("salary"), LongType)
      cat.alterTable(ident, updateType)

      checkError(
        exception = intercept[AnalysisException] {
          session.table("v").collect()
        },
        condition = "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION",
        parameters = Map(
          "viewName" -> "`v`",
          "tableName" -> "`testcat`.`ns1`.`ns2`.`tbl`",
          "colType" -> "data",
          "errors" -> "- `salary` type has changed from INT to BIGINT"))

      session.sql("DROP VIEW IF EXISTS v").collect()
      session.sql(s"DROP TABLE IF EXISTS $T").collect()
    }
  }

}
