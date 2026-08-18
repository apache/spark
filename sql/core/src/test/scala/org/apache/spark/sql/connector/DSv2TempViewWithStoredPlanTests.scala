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
 * NOTE: All `spark.sql(...)` calls append `.collect()` because Connect client DataFrames
 * are lazy and require an action to trigger execution. In classic mode `.collect()` on DDL
 * is a no-op (DDL executes eagerly), so this is harmless.
 */
trait DSv2TempViewWithStoredPlanTests extends DSv2ExternalMutationTestBase {

  // Scenario 1.1 (session write)
  test(s"${testPrefix}temp view with stored plan reflects session write") {
    withTable(testTable) {
      withView("v") {
        spark.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        spark.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        spark.table(testTable).filter("salary < 999").createOrReplaceTempView("v")
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        spark.sql(s"INSERT INTO $testTable VALUES (2, 200)").collect()
        checkAnswer(spark.table("v"), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  // Scenario 1.2 (external write)
  test(s"${testPrefix}temp view with stored plan reflects external write") {
    withTable(testTable) {
      withView("v") {
        spark.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        spark.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        spark.table(testTable).filter("salary < 999").createOrReplaceTempView("v")
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[InMemoryTableCatalog](spark, "testcat")
        externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2, 200))

        checkAnswer(spark.table("v"), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  // Scenario 1.2 connector w/ cache (external write, caching connector)
  test(s"${testPrefix}connector w/ cache: temp view stale after external write") {
    withTable(cachingTestTable) {
      withView("v") {
        spark.sql(s"CREATE TABLE $cachingTestTable (id INT, salary INT) USING foo").collect()
        spark.sql(s"INSERT INTO $cachingTestTable VALUES (1, 100), (10, 1000)").collect()

        spark.table(cachingTestTable).filter("salary < 999").createOrReplaceTempView("v")
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[CachingInMemoryTableCatalog](spark, "cachingcat")
        externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2, 200))

        // Caching connector returns stale table: external write invisible
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        // REFRESH TABLE invalidates the connector cache, external write becomes visible
        spark.sql(s"REFRESH TABLE $cachingTestTable").collect()
        checkAnswer(spark.table("v"), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  // Scenario 2.1 (session ADD COLUMN)
  test(s"${testPrefix}temp view with stored plan preserves schema after session ADD COLUMN") {
    withTable(testTable) {
      withView("v") {
        spark.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        spark.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        spark.table(testTable).filter("salary < 999").createOrReplaceTempView("v")
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        spark.sql(s"ALTER TABLE $testTable ADD COLUMN new_column INT").collect()
        spark.sql(s"INSERT INTO $testTable VALUES (2, 200, -1)").collect()

        // view preserves original 2-column schema, filter still applied
        checkAnswer(spark.table("v"), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  // Scenario 2.2 (external ADD COLUMN)
  test(s"${testPrefix}temp view with stored plan preserves schema after external ADD COLUMN") {
    withTable(testTable) {
      withView("v") {
        spark.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        spark.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        spark.table(testTable).filter("salary < 999").createOrReplaceTempView("v")
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        // external schema change via catalog API
        val catalog = getTableCatalog[InMemoryTableCatalog](spark, "testcat")
        val addCol = TableChange.addColumn(Array("new_column"), IntegerType, true)
        catalog.alterTable(testIdent, addCol)

        externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2, 200, -1))

        // view preserves original 2-column schema, filter still applied
        checkAnswer(spark.table("v"), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  // Scenario 2.2 connector w/ cache (external ADD COLUMN, caching connector)
  test(s"${testPrefix}connector w/ cache: temp view stale after external ADD COLUMN") {
    withTable(cachingTestTable) {
      withView("v") {
        spark.sql(s"CREATE TABLE $cachingTestTable (id INT, salary INT) USING foo").collect()
        spark.sql(s"INSERT INTO $cachingTestTable VALUES (1, 100), (10, 1000)").collect()

        spark.table(cachingTestTable).filter("salary < 999").createOrReplaceTempView("v")
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[CachingInMemoryTableCatalog](spark, "cachingcat")
        val addCol = TableChange.addColumn(Array("new_column"), IntegerType, true)
        catalog.alterTable(testIdent, addCol)

        externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2, 200, -1))

        // Caching connector returns stale table: external changes invisible
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        // REFRESH TABLE invalidates the connector cache, view preserves original 2-column schema
        spark.sql(s"REFRESH TABLE $cachingTestTable").collect()
        checkAnswer(spark.table("v"), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  // Scenario 3.1 (session column removal)
  test(s"${testPrefix}temp view with stored plan detects session column removal") {
    withTable(testTable) {
      withView("v") {
        spark.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        spark.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        spark.table(testTable).filter("salary < 999").createOrReplaceTempView("v")
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        spark.sql(s"ALTER TABLE $testTable DROP COLUMN salary").collect()

        checkError(
          exception = intercept[AnalysisException] { spark.table("v").collect() },
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
    withTable(testTable) {
      withView("v") {
        spark.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        spark.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        spark.table(testTable).filter("salary < 999").createOrReplaceTempView("v")
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[InMemoryTableCatalog](spark, "testcat")
        val dropCol = TableChange.deleteColumn(Array("salary"), false)
        catalog.alterTable(testIdent, dropCol)

        checkError(
          exception = intercept[AnalysisException] { spark.table("v").collect() },
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
    withTable(cachingTestTable) {
      withView("v") {
        spark.sql(s"CREATE TABLE $cachingTestTable (id INT, salary INT) USING foo").collect()
        spark.sql(s"INSERT INTO $cachingTestTable VALUES (1, 100), (10, 1000)").collect()

        spark.table(cachingTestTable).filter("salary < 999").createOrReplaceTempView("v")
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[CachingInMemoryTableCatalog](spark, "cachingcat")
        val dropCol = TableChange.deleteColumn(Array("salary"), false)
        catalog.alterTable(testIdent, dropCol)

        // Caching connector returns stale table: column removal invisible, no error
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        // REFRESH TABLE invalidates the connector cache, column removal detected
        spark.sql(s"REFRESH TABLE $cachingTestTable").collect()
        checkError(
          exception = intercept[AnalysisException] { spark.table("v").collect() },
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
    withTable(testTable) {
      withView("v") {
        spark.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        spark.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        spark.table(testTable).filter("salary < 999").createOrReplaceTempView("v")
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[InMemoryTableCatalog](spark, "testcat")
        val originalTableId = catalog.loadTable(testIdent).id

        spark.sql(s"DROP TABLE $testTable").collect()
        spark.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()

        val newTableId = catalog.loadTable(testIdent).id
        assert(originalTableId != newTableId)

        // view resolves to the new empty table
        checkAnswer(spark.table("v"), Seq.empty)

        spark.sql(s"INSERT INTO $testTable VALUES (2, 200)").collect()
        checkAnswer(spark.table("v"), Seq(Row(2, 200)))
      }
    }
  }

  // Scenario 4.2 (external drop and recreate table)
  test(s"${testPrefix}temp view with stored plan resolves to externally recreated table") {
    withTable(testTable) {
      withView("v") {
        spark.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        spark.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        spark.table(testTable).filter("salary < 999").createOrReplaceTempView("v")
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[InMemoryTableCatalog](spark, "testcat")
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
        checkAnswer(spark.table("v"), Seq.empty)

        spark.sql(s"INSERT INTO $testTable VALUES (2, 200)").collect()
        checkAnswer(spark.table("v"), Seq(Row(2, 200)))
      }
    }
  }

  // Scenario 4.2 connector w/ cache (external drop/recreate, caching connector)
  test(s"${testPrefix}connector w/ cache: temp view stale after external drop/recreate") {
    withTable(cachingTestTable) {
      withView("v") {
        spark.sql(s"CREATE TABLE $cachingTestTable (id INT, salary INT) USING foo").collect()
        spark.sql(s"INSERT INTO $cachingTestTable VALUES (1, 100), (10, 1000)").collect()

        spark.table(cachingTestTable).filter("salary < 999").createOrReplaceTempView("v")
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[CachingInMemoryTableCatalog](spark, "cachingcat")
        catalog.dropTable(testIdent)
        catalog.createTable(
          testIdent,
          new TableInfo.Builder()
            .withColumns(Array(
              Column.create("id", IntegerType),
              Column.create("salary", IntegerType)))
            .build())

        // Caching connector returns stale table: drop/recreate invisible
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        // REFRESH TABLE invalidates the connector cache, view resolves to new empty table
        spark.sql(s"REFRESH TABLE $cachingTestTable").collect()
        checkAnswer(spark.table("v"), Seq.empty)
      }
    }
  }

  // Scenario 5.1 (session drop and re-add column with same type, multiple views)
  test(s"${testPrefix}temp view with stored plan after session drop and re-add column same type" +
      " with unfiltered view") {
    withTable(testTable) {
      withView("v", "v_no_filter", "v_filter_is_null") {
        spark.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        spark.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        spark.table(testTable).filter("salary < 999").createOrReplaceTempView("v")
        spark.table(testTable).createOrReplaceTempView("v_no_filter")
        spark.table(testTable).filter("salary IS NULL")
          .createOrReplaceTempView("v_filter_is_null")
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))
        checkAnswer(spark.table("v_no_filter"), Seq(Row(1, 100), Row(10, 1000)))
        checkAnswer(spark.table("v_filter_is_null"), Seq.empty)

        // drop and re-add column with same name and type
        spark.sql(s"ALTER TABLE $testTable DROP COLUMN salary").collect()
        spark.sql(s"ALTER TABLE $testTable ADD COLUMN salary INT").collect()

        // salary values are now null, so the filtered view returns nothing
        checkAnswer(spark.table("v"), Seq.empty)
        // unfiltered view returns rows with null salary
        checkAnswer(spark.table("v_no_filter"), Seq(Row(1, null), Row(10, null)))
        // IS NULL filter now matches all rows
        checkAnswer(spark.table("v_filter_is_null"), Seq(Row(1, null), Row(10, null)))
      }
    }
  }

  // Scenario 5.2 (external drop and re-add column with same type)
  test(s"${testPrefix}temp view with stored plan after external drop and re-add column " +
      "same type") {
    withTable(testTable) {
      withView("v", "v_no_filter", "v_filter_is_null") {
        spark.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        spark.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        spark.table(testTable).filter("salary < 999").createOrReplaceTempView("v")
        spark.table(testTable).createOrReplaceTempView("v_no_filter")
        spark.table(testTable).filter("salary IS NULL")
          .createOrReplaceTempView("v_filter_is_null")
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))
        checkAnswer(spark.table("v_no_filter"), Seq(Row(1, 100), Row(10, 1000)))
        checkAnswer(spark.table("v_filter_is_null"), Seq.empty)

        // external drop and re-add column via catalog API
        val catalog = getTableCatalog[InMemoryTableCatalog](spark, "testcat")
        val dropCol = TableChange.deleteColumn(Array("salary"), false)
        val addCol = TableChange.addColumn(Array("salary"), IntegerType, true)
        catalog.alterTable(testIdent, dropCol, addCol)

        // salary values are now null, so the filtered view returns nothing
        checkAnswer(spark.table("v"), Seq.empty)
        // unfiltered view returns rows with null salary
        checkAnswer(spark.table("v_no_filter"), Seq(Row(1, null), Row(10, null)))
        // IS NULL filter now matches all rows
        checkAnswer(spark.table("v_filter_is_null"), Seq(Row(1, null), Row(10, null)))
      }
    }
  }

  // Scenario 5.2 connector w/ cache (external drop/re-add column, caching connector)
  test(s"${testPrefix}connector w/ cache: temp view stale after external drop/re-add column " +
      "same type") {
    withTable(cachingTestTable) {
      withView("v") {
        spark.sql(s"CREATE TABLE $cachingTestTable (id INT, salary INT) USING foo").collect()
        spark.sql(s"INSERT INTO $cachingTestTable VALUES (1, 100), (10, 1000)").collect()

        spark.table(cachingTestTable).filter("salary < 999").createOrReplaceTempView("v")
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[CachingInMemoryTableCatalog](spark, "cachingcat")
        val dropCol = TableChange.deleteColumn(Array("salary"), false)
        val addCol = TableChange.addColumn(Array("salary"), IntegerType, true)
        catalog.alterTable(testIdent, dropCol, addCol)

        // Caching connector returns stale table: column drop/re-add invisible
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        // REFRESH TABLE invalidates the connector cache, salary values are null
        spark.sql(s"REFRESH TABLE $cachingTestTable").collect()
        checkAnswer(spark.table("v"), Seq.empty)
      }
    }
  }

  // Scenario 6.1 (session drop and re-add column with different type)
  test(s"${testPrefix}temp view with stored plan detects session column type change") {
    withTable(testTable) {
      withView("v") {
        spark.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        spark.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        spark.table(testTable).filter("salary < 999").createOrReplaceTempView("v")
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        spark.sql(s"ALTER TABLE $testTable DROP COLUMN salary").collect()
        spark.sql(s"ALTER TABLE $testTable ADD COLUMN salary STRING").collect()

        checkError(
          exception = intercept[AnalysisException] { spark.table("v").collect() },
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
    withTable(testTable) {
      withView("v") {
        spark.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        spark.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        spark.table(testTable).filter("salary < 999").createOrReplaceTempView("v")
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[InMemoryTableCatalog](spark, "testcat")
        val dropCol = TableChange.deleteColumn(Array("salary"), false)
        val addCol = TableChange.addColumn(Array("salary"), StringType, true)
        catalog.alterTable(testIdent, dropCol, addCol)

        checkError(
          exception = intercept[AnalysisException] { spark.table("v").collect() },
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
    withTable(cachingTestTable) {
      withView("v") {
        spark.sql(s"CREATE TABLE $cachingTestTable (id INT, salary INT) USING foo").collect()
        spark.sql(s"INSERT INTO $cachingTestTable VALUES (1, 100), (10, 1000)").collect()

        spark.table(cachingTestTable).filter("salary < 999").createOrReplaceTempView("v")
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[CachingInMemoryTableCatalog](spark, "cachingcat")
        val dropCol = TableChange.deleteColumn(Array("salary"), false)
        val addCol = TableChange.addColumn(Array("salary"), StringType, true)
        catalog.alterTable(testIdent, dropCol, addCol)

        // Caching connector returns stale table: type change invisible, no error
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        // REFRESH TABLE invalidates the connector cache, type change detected
        spark.sql(s"REFRESH TABLE $cachingTestTable").collect()
        checkError(
          exception = intercept[AnalysisException] { spark.table("v").collect() },
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
    withTable(testTable) {
      withView("v") {
        spark.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        spark.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        spark.table(testTable).filter("salary < 999").createOrReplaceTempView("v")
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        spark.sql(s"ALTER TABLE $testTable ALTER COLUMN salary TYPE LONG").collect()

        checkError(
          exception = intercept[AnalysisException] { spark.table("v").collect() },
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
    withTable(testTable) {
      withView("v") {
        spark.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        spark.sql(s"INSERT INTO $testTable VALUES (1, 100), (10, 1000)").collect()

        spark.table(testTable).filter("salary < 999").createOrReplaceTempView("v")
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[InMemoryTableCatalog](spark, "testcat")
        val updateType = TableChange.updateColumnType(Array("salary"), LongType)
        catalog.alterTable(testIdent, updateType)

        checkError(
          exception = intercept[AnalysisException] { spark.table("v").collect() },
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
    withTable(cachingTestTable) {
      withView("v") {
        spark.sql(s"CREATE TABLE $cachingTestTable (id INT, salary INT) USING foo").collect()
        spark.sql(s"INSERT INTO $cachingTestTable VALUES (1, 100), (10, 1000)").collect()

        spark.table(cachingTestTable).filter("salary < 999").createOrReplaceTempView("v")
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[CachingInMemoryTableCatalog](spark, "cachingcat")
        val updateType = TableChange.updateColumnType(Array("salary"), LongType)
        catalog.alterTable(testIdent, updateType)

        // Caching connector returns stale table: type change invisible, no error
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))

        // REFRESH TABLE invalidates the connector cache, type change detected
        spark.sql(s"REFRESH TABLE $cachingTestTable").collect()
        checkError(
          exception = intercept[AnalysisException] { spark.table("v").collect() },
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
