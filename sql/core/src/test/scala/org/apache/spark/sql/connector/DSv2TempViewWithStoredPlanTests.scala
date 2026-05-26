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

package org.apache.spark.sql.connector

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{CachingInMemoryTableCatalog, Column, InMemoryTableCatalog, TableChange, TableInfo}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

/**
 * Shared temp view with stored plan tests for DSv2 tables. These tests verify that temp views
 * backed by DSv2 tables correctly handle data changes, schema changes, and table recreation,
 * both via session SQL and external catalog mutations.
 *
 * NOTE: All `session.sql(...)` calls append `.collect()` because Connect client DataFrames
 * are lazy and require an action to trigger execution. In classic mode `.collect()` on DDL
 * is a no-op (DDL executes eagerly), so this is harmless.
 */
trait DSv2TempViewWithStoredPlanTests extends DSv2ExternalMutationTestBase {

  // Uses testTable, cachingTestTable, and testIdent from DSv2ExternalMutationTestBase.

  // Scenario 1.1 (session write)
  test(s"${testPrefix}temp view with stored plan reflects session write") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable, Seq("v")) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        session.table(T).filter("salary < 999").createOrReplaceTempView("v")
        checkRows(session.table("v"), Seq(Row(1, 100)))

        session.sql(s"INSERT INTO $testTable VALUES (2, 200)").collect()
        checkRows(session.table("v"), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  // Scenario 1.2 (external write)
  test(s"${testPrefix}temp view with stored plan reflects external write") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable, Seq("v")) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        session.table(T).filter("salary < 999").createOrReplaceTempView("v")
        checkRows(session.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[InMemoryTableCatalog](session, "testcat")
        externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2, 200))

        checkRows(session.table("v"), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  // Scenario 1.2 connector w/ cache (external write, caching connector)
  test(s"${testPrefix}connector w/ cache: temp view stale after external write") {
    withTestSession { session =>
      withTestTableAndViews(session, cachingTestTable, Seq("v")) {
        session.sql(s"CREATE TABLE $cachingTestTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $cachingTestTable VALUES (1, 100), (10, 1000)").collect()

        session.table(CT).filter("salary < 999").createOrReplaceTempView("v")
        checkRows(session.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[CachingInMemoryTableCatalog](session, "cachingcat")
        externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2, 200))

        // Caching connector returns stale table: external write invisible
        checkRows(session.table("v"), Seq(Row(1, 100)))

        // REFRESH TABLE invalidates the connector cache, external write becomes visible
        session.sql(s"REFRESH TABLE $cachingTestTable").collect()
        checkRows(session.table("v"), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  // Scenario 2.1 (session ADD COLUMN)
  test(s"${testPrefix}temp view with stored plan preserves schema after session ADD COLUMN") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable, Seq("v")) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        session.table(T).filter("salary < 999").createOrReplaceTempView("v")
        checkRows(session.table("v"), Seq(Row(1, 100)))

        session.sql(s"ALTER TABLE $testTable ADD COLUMN new_column INT").collect()
        session.sql(s"INSERT INTO $testTable VALUES (2, 200, -1)").collect()

        // view preserves original 2-column schema, filter still applied
        checkRows(session.table("v"), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  // Scenario 2.2 (external ADD COLUMN)
  test(s"${testPrefix}temp view with stored plan preserves schema after external ADD COLUMN") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable, Seq("v")) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        session.table(T).filter("salary < 999").createOrReplaceTempView("v")
        checkRows(session.table("v"), Seq(Row(1, 100)))

        // external schema change via catalog API
        val catalog = getTableCatalog[InMemoryTableCatalog](session, "testcat")
        val addCol = TableChange.addColumn(Array("new_column"), IntegerType, true)
        catalog.alterTable(testIdent, addCol)

        externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2, 200, -1))

        // view preserves original 2-column schema, filter still applied
        checkRows(session.table("v"), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  // Scenario 2.2 connector w/ cache (external ADD COLUMN, caching connector)
  test(s"${testPrefix}connector w/ cache: temp view stale after external ADD COLUMN") {
    withTestSession { session =>
      withTestTableAndViews(session, cachingTestTable, Seq("v")) {
        session.sql(s"CREATE TABLE $cachingTestTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $cachingTestTable VALUES (1, 100), (10, 1000)").collect()

        session.table(CT).filter("salary < 999").createOrReplaceTempView("v")
        checkRows(session.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[CachingInMemoryTableCatalog](session, "cachingcat")
        val addCol = TableChange.addColumn(Array("new_column"), IntegerType, true)
        catalog.alterTable(testIdent, addCol)

        externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2, 200, -1))

        // Caching connector returns stale table: external changes invisible
        checkRows(session.table("v"), Seq(Row(1, 100)))

        // REFRESH TABLE invalidates the connector cache, view preserves original 2-column schema
        session.sql(s"REFRESH TABLE $cachingTestTable").collect()
        checkRows(session.table("v"), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  // Scenario 3.1 (session column removal)
  test(s"${testPrefix}temp view with stored plan detects session column removal") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable, Seq("v")) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        session.table(T).filter("salary < 999").createOrReplaceTempView("v")
        checkRows(session.table("v"), Seq(Row(1, 100)))

        session.sql(s"ALTER TABLE $testTable DROP COLUMN salary").collect()

        checkError(
          exception = intercept[AnalysisException] { session.table("v").collect() },
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
  test(s"${testPrefix}temp view with stored plan detects external column removal") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable, Seq("v")) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        session.table(T).filter("salary < 999").createOrReplaceTempView("v")
        checkRows(session.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[InMemoryTableCatalog](session, "testcat")
        val dropCol = TableChange.deleteColumn(Array("salary"), false)
        catalog.alterTable(testIdent, dropCol)

        checkError(
          exception = intercept[AnalysisException] { session.table("v").collect() },
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
  test(s"${testPrefix}connector w/ cache: temp view stale after external column removal") {
    withTestSession { session =>
      withTestTableAndViews(session, cachingTestTable, Seq("v")) {
        session.sql(s"CREATE TABLE $cachingTestTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $cachingTestTable VALUES (1, 100), (10, 1000)").collect()

        session.table(CT).filter("salary < 999").createOrReplaceTempView("v")
        checkRows(session.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[CachingInMemoryTableCatalog](session, "cachingcat")
        val dropCol = TableChange.deleteColumn(Array("salary"), false)
        catalog.alterTable(testIdent, dropCol)

        // Caching connector returns stale table: column removal invisible, no error
        checkRows(session.table("v"), Seq(Row(1, 100)))

        // REFRESH TABLE invalidates the connector cache, column removal detected
        session.sql(s"REFRESH TABLE $cachingTestTable").collect()
        checkError(
          exception = intercept[AnalysisException] { session.table("v").collect() },
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
  test(s"${testPrefix}temp view with stored plan resolves to session-recreated table") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable, Seq("v")) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        session.table(T).filter("salary < 999").createOrReplaceTempView("v")
        checkRows(session.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[InMemoryTableCatalog](session, "testcat")
        val originalTableId = catalog.loadTable(testIdent).id

        session.sql(s"DROP TABLE $testTable").collect()
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()

        val newTableId = catalog.loadTable(testIdent).id
        assert(originalTableId != newTableId)

        // view resolves to the new empty table
        checkRows(session.table("v"), Seq.empty)

        session.sql(s"INSERT INTO $testTable VALUES (2, 200)").collect()
        checkRows(session.table("v"), Seq(Row(2, 200)))
      }
    }
  }

  // Scenario 4.2 (external drop and recreate table)
  test(s"${testPrefix}temp view with stored plan resolves to externally recreated table") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable, Seq("v")) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        session.table(T).filter("salary < 999").createOrReplaceTempView("v")
        checkRows(session.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[InMemoryTableCatalog](session, "testcat")
        val originalTableId = catalog.loadTable(testIdent).id

        catalog.dropTable(testIdent)
        catalog.createTable(
          testIdent,
          new TableInfo.Builder()
            .withColumns(Array(
              Column.create("id", IntegerType),
              Column.create("salary", IntegerType)))
            .build())

        val newTableId = catalog.loadTable(testIdent).id
        assert(originalTableId != newTableId)

        // view resolves to the new empty table
        checkRows(session.table("v"), Seq.empty)

        session.sql(s"INSERT INTO $testTable VALUES (2, 200)").collect()
        checkRows(session.table("v"), Seq(Row(2, 200)))
      }
    }
  }

  // Scenario 4.2 connector w/ cache (external drop/recreate, caching connector)
  test(s"${testPrefix}connector w/ cache: temp view stale after external drop/recreate") {
    withTestSession { session =>
      withTestTableAndViews(session, cachingTestTable, Seq("v")) {
        session.sql(s"CREATE TABLE $cachingTestTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $cachingTestTable VALUES (1, 100), (10, 1000)").collect()

        session.table(CT).filter("salary < 999").createOrReplaceTempView("v")
        checkRows(session.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[CachingInMemoryTableCatalog](session, "cachingcat")
        catalog.dropTable(testIdent)
        catalog.createTable(
          testIdent,
          new TableInfo.Builder()
            .withColumns(Array(
              Column.create("id", IntegerType),
              Column.create("salary", IntegerType)))
            .build())

        // Caching connector returns stale table: drop/recreate invisible
        checkRows(session.table("v"), Seq(Row(1, 100)))

        // REFRESH TABLE invalidates the connector cache, view resolves to new empty table
        session.sql(s"REFRESH TABLE $cachingTestTable").collect()
        checkRows(session.table("v"), Seq.empty)
      }
    }
  }

  // Scenario 5.1 (session drop and re-add column with same type, multiple views)
  test(s"${testPrefix}temp view with stored plan after session drop and re-add column same type" +
      " with unfiltered view") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable, Seq("v", "v_no_filter", "v_filter_is_null")) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        session.table(T).filter("salary < 999").createOrReplaceTempView("v")
        session.table(T).createOrReplaceTempView("v_no_filter")
        session.table(T).filter("salary IS NULL").createOrReplaceTempView("v_filter_is_null")
        checkRows(session.table("v"), Seq(Row(1, 100)))
        checkRows(session.table("v_no_filter"), Seq(Row(1, 100), Row(10, 1000)))
        checkRows(session.table("v_filter_is_null"), Seq.empty)

        // drop and re-add column with same name and type
        session.sql(s"ALTER TABLE $testTable DROP COLUMN salary").collect()
        session.sql(s"ALTER TABLE $testTable ADD COLUMN salary INT").collect()

        // salary values are now null, so the filtered view returns nothing
        checkRows(session.table("v"), Seq.empty)
        // unfiltered view returns rows with null salary
        checkRows(session.table("v_no_filter"), Seq(Row(1, null), Row(10, null)))
        // IS NULL filter now matches all rows
        checkRows(session.table("v_filter_is_null"), Seq(Row(1, null), Row(10, null)))
      }
    }
  }

  // Scenario 5.2 (external drop and re-add column with same type)
  test(s"${testPrefix}temp view with stored plan after external drop and re-add column " +
      "same type") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable, Seq("v", "v_no_filter", "v_filter_is_null")) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        session.table(T).filter("salary < 999").createOrReplaceTempView("v")
        session.table(T).createOrReplaceTempView("v_no_filter")
        session.table(T).filter("salary IS NULL").createOrReplaceTempView("v_filter_is_null")
        checkRows(session.table("v"), Seq(Row(1, 100)))
        checkRows(session.table("v_no_filter"), Seq(Row(1, 100), Row(10, 1000)))
        checkRows(session.table("v_filter_is_null"), Seq.empty)

        // external drop and re-add column via catalog API
        val catalog = getTableCatalog[InMemoryTableCatalog](session, "testcat")
        val dropCol = TableChange.deleteColumn(Array("salary"), false)
        val addCol = TableChange.addColumn(Array("salary"), IntegerType, true)
        catalog.alterTable(testIdent, dropCol, addCol)

        // salary values are now null, so the filtered view returns nothing
        checkRows(session.table("v"), Seq.empty)
        // unfiltered view returns rows with null salary
        checkRows(session.table("v_no_filter"), Seq(Row(1, null), Row(10, null)))
        // IS NULL filter now matches all rows
        checkRows(session.table("v_filter_is_null"), Seq(Row(1, null), Row(10, null)))
      }
    }
  }

  // Scenario 5.2 connector w/ cache (external drop/re-add column, caching connector)
  test(s"${testPrefix}connector w/ cache: temp view stale after external drop/re-add column " +
      "same type") {
    withTestSession { session =>
      withTestTableAndViews(session, cachingTestTable, Seq("v")) {
        session.sql(s"CREATE TABLE $cachingTestTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $cachingTestTable VALUES (1, 100), (10, 1000)").collect()

        session.table(CT).filter("salary < 999").createOrReplaceTempView("v")
        checkRows(session.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[CachingInMemoryTableCatalog](session, "cachingcat")
        val dropCol = TableChange.deleteColumn(Array("salary"), false)
        val addCol = TableChange.addColumn(Array("salary"), IntegerType, true)
        catalog.alterTable(testIdent, dropCol, addCol)

        // Caching connector returns stale table: column drop/re-add invisible
        checkRows(session.table("v"), Seq(Row(1, 100)))

        // REFRESH TABLE invalidates the connector cache, salary values are null
        session.sql(s"REFRESH TABLE $cachingTestTable").collect()
        checkRows(session.table("v"), Seq.empty)
      }
    }
  }

  // Scenario 6.1 (session drop and re-add column with different type)
  test(s"${testPrefix}temp view with stored plan detects session column type change") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable, Seq("v")) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        session.table(T).filter("salary < 999").createOrReplaceTempView("v")
        checkRows(session.table("v"), Seq(Row(1, 100)))

        session.sql(s"ALTER TABLE $testTable DROP COLUMN salary").collect()
        session.sql(s"ALTER TABLE $testTable ADD COLUMN salary STRING").collect()

        checkError(
          exception = intercept[AnalysisException] { session.table("v").collect() },
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
  test(s"${testPrefix}temp view with stored plan detects external column type change") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable, Seq("v")) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        session.table(T).filter("salary < 999").createOrReplaceTempView("v")
        checkRows(session.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[InMemoryTableCatalog](session, "testcat")
        val dropCol = TableChange.deleteColumn(Array("salary"), false)
        val addCol = TableChange.addColumn(Array("salary"), StringType, true)
        catalog.alterTable(testIdent, dropCol, addCol)

        checkError(
          exception = intercept[AnalysisException] { session.table("v").collect() },
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
  test(s"${testPrefix}connector w/ cache: temp view stale after external column type change") {
    withTestSession { session =>
      withTestTableAndViews(session, cachingTestTable, Seq("v")) {
        session.sql(s"CREATE TABLE $cachingTestTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $cachingTestTable VALUES (1, 100), (10, 1000)").collect()

        session.table(CT).filter("salary < 999").createOrReplaceTempView("v")
        checkRows(session.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[CachingInMemoryTableCatalog](session, "cachingcat")
        val dropCol = TableChange.deleteColumn(Array("salary"), false)
        val addCol = TableChange.addColumn(Array("salary"), StringType, true)
        catalog.alterTable(testIdent, dropCol, addCol)

        // Caching connector returns stale table: type change invisible, no error
        checkRows(session.table("v"), Seq(Row(1, 100)))

        // REFRESH TABLE invalidates the connector cache, type change detected
        session.sql(s"REFRESH TABLE $cachingTestTable").collect()
        checkError(
          exception = intercept[AnalysisException] { session.table("v").collect() },
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
  test(s"${testPrefix}temp view with stored plan detects session type widening") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable, Seq("v")) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        session.table(T).filter("salary < 999").createOrReplaceTempView("v")
        checkRows(session.table("v"), Seq(Row(1, 100)))

        session.sql(s"ALTER TABLE $testTable ALTER COLUMN salary TYPE LONG").collect()

        checkError(
          exception = intercept[AnalysisException] { session.table("v").collect() },
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
  test(s"${testPrefix}temp view with stored plan detects external type widening") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable, Seq("v")) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        session.table(T).filter("salary < 999").createOrReplaceTempView("v")
        checkRows(session.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[InMemoryTableCatalog](session, "testcat")
        val updateType = TableChange.updateColumnType(Array("salary"), LongType)
        catalog.alterTable(testIdent, updateType)

        checkError(
          exception = intercept[AnalysisException] { session.table("v").collect() },
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
  test(s"${testPrefix}connector w/ cache: temp view stale after external type widening") {
    withTestSession { session =>
      withTestTableAndViews(session, cachingTestTable, Seq("v")) {
        session.sql(s"CREATE TABLE $cachingTestTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $cachingTestTable VALUES (1, 100), (10, 1000)").collect()

        session.table(CT).filter("salary < 999").createOrReplaceTempView("v")
        checkRows(session.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[CachingInMemoryTableCatalog](session, "cachingcat")
        val updateType = TableChange.updateColumnType(Array("salary"), LongType)
        catalog.alterTable(testIdent, updateType)

        // Caching connector returns stale table: type change invisible, no error
        checkRows(session.table("v"), Seq(Row(1, 100)))

        // REFRESH TABLE invalidates the connector cache, type change detected
        session.sql(s"REFRESH TABLE $cachingTestTable").collect()
        checkError(
          exception = intercept[AnalysisException] { session.table("v").collect() },
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
