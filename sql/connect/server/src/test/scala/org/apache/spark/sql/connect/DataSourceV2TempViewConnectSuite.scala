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
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.classic
import org.apache.spark.sql.connector.catalog.{BufferedRows, CachingInMemoryTableCatalog, Column, Identifier, InMemoryBaseTable, InMemoryTableCatalog, TableCatalog, TableChange, TableInfo, TableWritePrivilege}
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
      actual.toSeq.sortBy(_.toString()) == expected.sortBy(_.toString()),
      s"Expected ${expected.mkString(", ")} but got ${actual.mkString(", ")}")
  }

  /** Get a catalog from the server-side session by name. */
  private def serverCatalog[T <: TableCatalog](
      serverSession: classic.SparkSession,
      name: String): T =
    serverSession.sessionState.catalogManager.catalog(name).asInstanceOf[T]

  /** Appends a row to a DSv2 table via the catalog API, bypassing the session. */
  private def externalAppend(
      cat: TableCatalog,
      ident: Identifier,
      schema: StructType,
      row: InternalRow): Unit = {
    val extTable = cat
      .loadTable(ident, util.Set.of(TableWritePrivilege.INSERT))
      .asInstanceOf[InMemoryBaseTable]
    extTable.withData(Array(new BufferedRows(Seq.empty, schema).withRow(row)))
  }

  /** Ensure views and table are dropped even if the test body throws. */
  private def withTableAndViews(
      session: SparkSession,
      table: String,
      views: Seq[String],
      clearCachingCatalog: Boolean = false)(fn: => Unit): Unit = {
    try {
      fn
    } finally {
      views.foreach(v => session.sql(s"DROP VIEW IF EXISTS $v").collect())
      session.sql(s"DROP TABLE IF EXISTS $table").collect()
      if (clearCachingCatalog) CachingInMemoryTableCatalog.clearCache()
    }
  }

  // Temp views with stored plans: scenarios from the DSv2 table refresh tests.
  // Each test creates a DSv2 table with initial data, builds a temp view with a filter
  // (to demonstrate that the stored plan is non-trivial), and then verifies the view
  // behavior after various table modifications (session or external).

  // Scenario 1.1 (session write)
  test("[connect] temp view with stored plan reflects session write") {
    withSession { session =>
      withTableAndViews(session = session, table = T, views = Seq("v")) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

        session.table(T).filter("salary < 999").createOrReplaceTempView("v")
        assertRows(session.table("v").collect(), Seq(Row(1, 100)))

        session.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

        assertRows(session.table("v").collect(), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  // Scenario 1.2 (external write)
  test("[connect] temp view with stored plan reflects external write") {
    withSession { session =>
      withTableAndViews(session = session, table = T, views = Seq("v")) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

        session.table(T).filter("salary < 999").createOrReplaceTempView("v")
        val serverSession = getServerSession(session)
        assertRows(session.table("v").collect(), Seq(Row(1, 100)))

        // external writer adds (2, 200) via direct catalog API
        val cat = serverCatalog[InMemoryTableCatalog](serverSession, "testcat")
        externalAppend(
          cat = cat,
          ident = ident,
          schema = StructType.fromDDL("id INT, salary INT"),
          row = InternalRow(2, 200))

        assertRows(session.table("v").collect(), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  // Scenario 1.2 connector w/ cache (external write, caching connector)
  test("[connect] connector w/ cache: temp view stale after external write") {
    withSession { session =>
      withTableAndViews(
        session = session,
        table = CT,
        views = Seq("v"),
        clearCachingCatalog = true) {
        session.sql(s"CREATE TABLE $CT (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $CT VALUES (1, 100), (10, 1000)").collect()

        session.table(CT).filter("salary < 999").createOrReplaceTempView("v")
        assertRows(session.table("v").collect(), Seq(Row(1, 100)))

        val serverSession = getServerSession(session)
        val cat = serverCatalog[CachingInMemoryTableCatalog](serverSession, "cachingcat")
        externalAppend(
          cat = cat,
          ident = ident,
          schema = StructType.fromDDL("id INT, salary INT"),
          row = InternalRow(2, 200))

        // Caching connector returns stale table: external write invisible
        assertRows(session.table("v").collect(), Seq(Row(1, 100)))

        // REFRESH TABLE invalidates the connector cache, external write becomes visible
        session.sql(s"REFRESH TABLE $CT").collect()
        assertRows(session.table("v").collect(), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  // Scenario 2.1 (session ADD COLUMN)
  test("[connect] temp view with stored plan preserves schema after session ADD COLUMN") {
    withSession { session =>
      withTableAndViews(session = session, table = T, views = Seq("v")) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

        session.table(T).filter("salary < 999").createOrReplaceTempView("v")
        assertRows(session.table("v").collect(), Seq(Row(1, 100)))

        session.sql(s"ALTER TABLE $T ADD COLUMN new_column INT").collect()
        session.sql(s"INSERT INTO $T VALUES (2, 200, -1)").collect()

        // view preserves original 2-column schema, filter still applied
        assertRows(session.table("v").collect(), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  // Scenario 2.2 (external ADD COLUMN)
  test("[connect] temp view with stored plan preserves schema after external ADD COLUMN") {
    withSession { session =>
      withTableAndViews(session = session, table = T, views = Seq("v")) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

        session.table(T).filter("salary < 999").createOrReplaceTempView("v")
        val serverSession = getServerSession(session)
        assertRows(session.table("v").collect(), Seq(Row(1, 100)))

        // external schema change via catalog API
        val cat = serverCatalog[InMemoryTableCatalog](serverSession, "testcat")
        val addCol = TableChange.addColumn(Array("new_column"), IntegerType, true)
        cat.alterTable(ident, addCol)

        // external writer adds data with new schema
        externalAppend(
          cat = cat,
          ident = ident,
          schema = StructType.fromDDL("id INT, salary INT, new_column INT"),
          row = InternalRow(2, 200, -1))

        // view preserves original 2-column schema, filter still applied
        assertRows(session.table("v").collect(), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  // Scenario 2.2 connector w/ cache (external ADD COLUMN, caching connector)
  test("[connect] connector w/ cache: temp view stale after external ADD COLUMN") {
    withSession { session =>
      withTableAndViews(
        session = session,
        table = CT,
        views = Seq("v"),
        clearCachingCatalog = true) {
        session.sql(s"CREATE TABLE $CT (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $CT VALUES (1, 100), (10, 1000)").collect()

        session.table(CT).filter("salary < 999").createOrReplaceTempView("v")
        assertRows(session.table("v").collect(), Seq(Row(1, 100)))

        val serverSession = getServerSession(session)
        val cat = serverCatalog[CachingInMemoryTableCatalog](serverSession, "cachingcat")
        val addCol = TableChange.addColumn(Array("new_column"), IntegerType, true)
        cat.alterTable(ident, addCol)

        externalAppend(
          cat = cat,
          ident = ident,
          schema = StructType.fromDDL("id INT, salary INT, new_column INT"),
          row = InternalRow(2, 200, -1))

        // Caching connector returns stale table: external changes invisible
        assertRows(session.table("v").collect(), Seq(Row(1, 100)))

        // REFRESH TABLE invalidates the connector cache, view preserves original 2-column schema
        session.sql(s"REFRESH TABLE $CT").collect()
        assertRows(session.table("v").collect(), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  // Scenario 3.1 (session column removal)
  test("[connect] temp view with stored plan detects session column removal") {
    withSession { session =>
      withTableAndViews(session = session, table = T, views = Seq("v")) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

        session.table(T).filter("salary < 999").createOrReplaceTempView("v")
        assertRows(session.table("v").collect(), Seq(Row(1, 100)))

        // session schema change via SQL
        session.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()

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
      }
    }
  }

  // Scenario 3.2 (external column removal)
  test("[connect] temp view with stored plan detects external column removal") {
    withSession { session =>
      withTableAndViews(session = session, table = T, views = Seq("v")) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

        session.table(T).filter("salary < 999").createOrReplaceTempView("v")
        val serverSession = getServerSession(session)
        assertRows(session.table("v").collect(), Seq(Row(1, 100)))

        // external schema change via catalog API
        val cat = serverCatalog[InMemoryTableCatalog](serverSession, "testcat")
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
      }
    }
  }

  // Scenario 3.2 connector w/ cache (external column removal, caching connector)
  test("[connect] connector w/ cache: temp view stale after external column removal") {
    withSession { session =>
      withTableAndViews(
        session = session,
        table = CT,
        views = Seq("v"),
        clearCachingCatalog = true) {
        session.sql(s"CREATE TABLE $CT (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $CT VALUES (1, 100), (10, 1000)").collect()

        session.table(CT).filter("salary < 999").createOrReplaceTempView("v")
        assertRows(session.table("v").collect(), Seq(Row(1, 100)))

        val serverSession = getServerSession(session)
        val cat = serverCatalog[CachingInMemoryTableCatalog](serverSession, "cachingcat")
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
      }
    }
  }

  // Scenario 4.1 (session drop and recreate table)
  test("[connect] temp view with stored plan resolves to session-recreated table") {
    withSession { session =>
      withTableAndViews(session = session, table = T, views = Seq("v")) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

        session.table(T).filter("salary < 999").createOrReplaceTempView("v")
        val serverSession = getServerSession(session)
        assertRows(session.table("v").collect(), Seq(Row(1, 100)))

        val cat = serverCatalog[InMemoryTableCatalog](serverSession, "testcat")
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
      }
    }
  }

  // Scenario 4.2 (external drop and recreate table)
  test("[connect] temp view with stored plan resolves to externally recreated table") {
    withSession { session =>
      withTableAndViews(session = session, table = T, views = Seq("v")) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

        session.table(T).filter("salary < 999").createOrReplaceTempView("v")
        val serverSession = getServerSession(session)
        assertRows(session.table("v").collect(), Seq(Row(1, 100)))

        val cat = serverCatalog[InMemoryTableCatalog](serverSession, "testcat")
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
      }
    }
  }

  // Scenario 4.2 connector w/ cache (external drop/recreate, caching connector)
  test("[connect] connector w/ cache: temp view stale after external drop/recreate") {
    withSession { session =>
      withTableAndViews(
        session = session,
        table = CT,
        views = Seq("v"),
        clearCachingCatalog = true) {
        session.sql(s"CREATE TABLE $CT (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $CT VALUES (1, 100), (10, 1000)").collect()

        session.table(CT).filter("salary < 999").createOrReplaceTempView("v")
        assertRows(session.table("v").collect(), Seq(Row(1, 100)))

        val serverSession = getServerSession(session)
        val cat = serverCatalog[CachingInMemoryTableCatalog](serverSession, "cachingcat")
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
      }
    }
  }

  // Scenario 5.1 (session drop and re-add column with same type)
  test("[connect] temp view with stored plan after session drop and re-add column same type") {
    withSession { session =>
      withTableAndViews(
        session = session,
        table = T,
        views = Seq("v", "v_no_filter", "v_filter_is_null")) {
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
      }
    }
  }

  // Scenario 5.2 (external drop and re-add column with same type)
  test("[connect] temp view with stored plan after external drop and re-add column same type") {
    withSession { session =>
      withTableAndViews(
        session = session,
        table = T,
        views = Seq("v", "v_no_filter", "v_filter_is_null")) {
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
        val cat = serverCatalog[InMemoryTableCatalog](serverSession, "testcat")
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
      }
    }
  }

  // Scenario 5.2 connector w/ cache (external drop/re-add column, caching connector)
  test("[connect] connector w/ cache: temp view stale after external drop/re-add column") {
    withSession { session =>
      withTableAndViews(
        session = session,
        table = CT,
        views = Seq("v"),
        clearCachingCatalog = true) {
        session.sql(s"CREATE TABLE $CT (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $CT VALUES (1, 100), (10, 1000)").collect()

        session.table(CT).filter("salary < 999").createOrReplaceTempView("v")
        assertRows(session.table("v").collect(), Seq(Row(1, 100)))

        val serverSession = getServerSession(session)
        val cat = serverCatalog[CachingInMemoryTableCatalog](serverSession, "cachingcat")
        val dropCol = TableChange.deleteColumn(Array("salary"), false)
        val addCol = TableChange.addColumn(Array("salary"), IntegerType, true)
        cat.alterTable(ident, dropCol, addCol)

        // Caching connector returns stale table: column drop/re-add invisible
        assertRows(session.table("v").collect(), Seq(Row(1, 100)))

        // REFRESH TABLE invalidates the connector cache, salary values are null
        session.sql(s"REFRESH TABLE $CT").collect()
        assert(session.table("v").schema.fieldNames.toSeq == Seq("id", "salary"))
        assertRows(session.table("v").collect(), Seq.empty)
      }
    }
  }

  // Scenario 6.1 (session drop and re-add column with different type)
  test("[connect] temp view with stored plan detects session column type change") {
    withSession { session =>
      withTableAndViews(session = session, table = T, views = Seq("v")) {
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
      }
    }
  }

  // Scenario 6.2 (external drop and re-add column with different type)
  test("[connect] temp view with stored plan detects external column type change") {
    withSession { session =>
      withTableAndViews(session = session, table = T, views = Seq("v")) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

        session.table(T).filter("salary < 999").createOrReplaceTempView("v")
        val serverSession = getServerSession(session)
        assertRows(session.table("v").collect(), Seq(Row(1, 100)))

        // external drop and re-add column with different type via catalog API
        val cat = serverCatalog[InMemoryTableCatalog](serverSession, "testcat")
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
      }
    }
  }

  // Scenario 6.2 connector w/ cache (external column type change, caching connector)
  test("[connect] connector w/ cache: temp view stale after external column type change") {
    withSession { session =>
      withTableAndViews(
        session = session,
        table = CT,
        views = Seq("v"),
        clearCachingCatalog = true) {
        session.sql(s"CREATE TABLE $CT (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $CT VALUES (1, 100), (10, 1000)").collect()

        session.table(CT).filter("salary < 999").createOrReplaceTempView("v")
        assertRows(session.table("v").collect(), Seq(Row(1, 100)))

        val serverSession = getServerSession(session)
        val cat = serverCatalog[CachingInMemoryTableCatalog](serverSession, "cachingcat")
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
      }
    }
  }

  // Scenario 7.1 (session type widening from INT to BIGINT)
  test("[connect] temp view with stored plan detects session type widening") {
    withSession { session =>
      withTableAndViews(session = session, table = T, views = Seq("v")) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

        session.table(T).filter("salary < 999").createOrReplaceTempView("v")
        assertRows(session.table("v").collect(), Seq(Row(1, 100)))

        // session type widening via SQL
        session.sql(s"ALTER TABLE $T ALTER COLUMN salary TYPE LONG").collect()

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
      }
    }
  }

  // Scenario 7.2 (external type widening from INT to BIGINT)
  test("[connect] temp view with stored plan detects external type widening") {
    withSession { session =>
      withTableAndViews(session = session, table = T, views = Seq("v")) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100), (10, 1000)").collect()

        session.table(T).filter("salary < 999").createOrReplaceTempView("v")
        val serverSession = getServerSession(session)
        assertRows(session.table("v").collect(), Seq(Row(1, 100)))

        // widen salary type from INT to BIGINT via catalog API
        val cat = serverCatalog[InMemoryTableCatalog](serverSession, "testcat")
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
      }
    }
  }

  // Scenario 7.2 connector w/ cache (external type widening, caching connector)
  test("[connect] connector w/ cache: temp view stale after external type widening") {
    withSession { session =>
      withTableAndViews(
        session = session,
        table = CT,
        views = Seq("v"),
        clearCachingCatalog = true) {
        session.sql(s"CREATE TABLE $CT (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $CT VALUES (1, 100), (10, 1000)").collect()

        session.table(CT).filter("salary < 999").createOrReplaceTempView("v")
        assertRows(session.table("v").collect(), Seq(Row(1, 100)))

        val serverSession = getServerSession(session)
        val cat = serverCatalog[CachingInMemoryTableCatalog](serverSession, "cachingcat")
        val updateType =
          TableChange.updateColumnType(Array("salary"), LongType)
        cat.alterTable(ident, updateType)

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
            "errors" -> "- `salary` type has changed from INT to BIGINT"))
      }
    }
  }
}
