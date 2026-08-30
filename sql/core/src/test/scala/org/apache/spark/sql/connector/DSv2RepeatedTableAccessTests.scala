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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{CachingInMemoryTableCatalog, Column, InMemoryTableCatalog, TableChange, TableInfo}
import org.apache.spark.sql.types.IntegerType

/**
 * Shared repeated table access tests with external changes for DSv2 tables. These tests verify
 * that repeated `sql()` calls correctly reflect both session and external mutations:
 *
 *  - Scenario 1 (external writes): external data appended via the catalog API is visible.
 *  - Scenario 2 (external schema changes): external ADD COLUMN via the catalog API is visible.
 *  - Scenario 3 (external drop/recreate): external drop and recreate via the catalog API
 *    resolves to the new empty table.
 *
 * Each scenario includes a session mutation baseline, an external mutation test, and a
 * caching-connector variant showing stale results until `REFRESH TABLE`.
 *
 * NOTE: All `spark.sql(...)` calls append `.collect()` because Connect client DataFrames
 * are lazy and require an action to trigger execution. In classic mode `.collect()` on
 * DDL / DML is a no-op (these execute eagerly), so this is harmless.
 */
trait DSv2RepeatedTableAccessTests extends DSv2ExternalMutationTestBase {

  // Uses testTable, cachingTestTable, and testIdent from DSv2ExternalMutationTestBase.

  // Scenario 1: data changes via writes

  test(s"${testPrefix}repeated sql() reflects session write") {
    withTable(testTable) {
      spark.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
      spark.sql(s"INSERT INTO $testTable VALUES (1, 100)").collect()
      checkAnswer(spark.sql(s"SELECT * FROM $testTable"), Seq(Row(1, 100)))

      spark.sql(s"INSERT INTO $testTable VALUES (2, 200)").collect()
      checkAnswer(spark.sql(s"SELECT * FROM $testTable"), Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test(s"${testPrefix}repeated sql() reflects external write") {
    withTable(testTable) {
      spark.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
      spark.sql(s"INSERT INTO $testTable VALUES (1, 100)").collect()
      checkAnswer(spark.sql(s"SELECT * FROM $testTable"), Seq(Row(1, 100)))

      val catalog = getTableCatalog[InMemoryTableCatalog](spark, "testcat")
      externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2, 200))

      checkAnswer(spark.sql(s"SELECT * FROM $testTable"), Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test(s"${testPrefix}connector w/ cache: repeated sql() stale after external write") {
    withTable(cachingTestTable) {
      spark.sql(s"CREATE TABLE $cachingTestTable (id INT, salary INT) USING foo").collect()
      spark.sql(s"INSERT INTO $cachingTestTable VALUES (1, 100)").collect()
      checkAnswer(spark.sql(s"SELECT * FROM $cachingTestTable"), Seq(Row(1, 100)))

      val catalog = getTableCatalog[CachingInMemoryTableCatalog](spark, "cachingcat")
      externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2, 200))

      // Caching connector returns stale table: external write invisible
      checkAnswer(spark.sql(s"SELECT * FROM $cachingTestTable"), Seq(Row(1, 100)))

      // REFRESH TABLE invalidates the connector cache, external write becomes visible
      spark.sql(s"REFRESH TABLE $cachingTestTable").collect()
      checkAnswer(spark.sql(s"SELECT * FROM $cachingTestTable"), Seq(Row(1, 100), Row(2, 200)))
    }
  }

  // Scenario 2: schema changes

  test(s"${testPrefix}repeated sql() reflects session schema change") {
    withTable(testTable) {
      spark.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
      spark.sql(s"INSERT INTO $testTable VALUES (1, 100)").collect()
      checkAnswer(spark.sql(s"SELECT * FROM $testTable"), Seq(Row(1, 100)))

      spark.sql(s"ALTER TABLE $testTable ADD COLUMN new_col INT").collect()
      spark.sql(s"INSERT INTO $testTable VALUES (2, 200, -1)").collect()
      checkAnswer(
        spark.sql(s"SELECT * FROM $testTable"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))
    }
  }

  test(s"${testPrefix}repeated sql() reflects external schema change") {
    withTable(testTable) {
      spark.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
      spark.sql(s"INSERT INTO $testTable VALUES (1, 100)").collect()
      checkAnswer(spark.sql(s"SELECT * FROM $testTable"), Seq(Row(1, 100)))

      val catalog = getTableCatalog[InMemoryTableCatalog](spark, "testcat")
      val addCol = TableChange.addColumn(Array("new_col"), IntegerType, true)
      catalog.alterTable(testIdent, addCol)

      externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2, 200, -1))

      checkAnswer(
        spark.sql(s"SELECT * FROM $testTable"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))
    }
  }

  test(s"${testPrefix}connector w/ cache: repeated sql() stale after external schema change") {
    withTable(cachingTestTable) {
      spark.sql(s"CREATE TABLE $cachingTestTable (id INT, salary INT) USING foo").collect()
      spark.sql(s"INSERT INTO $cachingTestTable VALUES (1, 100)").collect()
      checkAnswer(spark.sql(s"SELECT * FROM $cachingTestTable"), Seq(Row(1, 100)))

      val catalog = getTableCatalog[CachingInMemoryTableCatalog](spark, "cachingcat")
      val addCol = TableChange.addColumn(Array("new_col"), IntegerType, true)
      catalog.alterTable(testIdent, addCol)

      externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2, 200, -1))

      // Caching connector returns stale table: external changes invisible
      checkAnswer(spark.sql(s"SELECT * FROM $cachingTestTable"), Seq(Row(1, 100)))

      // REFRESH TABLE invalidates the connector cache, schema change + data visible
      spark.sql(s"REFRESH TABLE $cachingTestTable").collect()
      checkAnswer(
        spark.sql(s"SELECT * FROM $cachingTestTable"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))
    }
  }

  // Scenario 3: drop and recreate table

  test(s"${testPrefix}repeated sql() reflects session drop/recreate") {
    withTable(testTable) {
      spark.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
      spark.sql(s"INSERT INTO $testTable VALUES (1, 100)").collect()
      checkAnswer(spark.sql(s"SELECT * FROM $testTable"), Seq(Row(1, 100)))

      spark.sql(s"DROP TABLE $testTable").collect()
      spark.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
      checkAnswer(spark.sql(s"SELECT * FROM $testTable"), Seq.empty)
    }
  }

  test(s"${testPrefix}repeated sql() reflects external drop/recreate") {
    withTable(testTable) {
      spark.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
      spark.sql(s"INSERT INTO $testTable VALUES (1, 100)").collect()
      checkAnswer(spark.sql(s"SELECT * FROM $testTable"), Seq(Row(1, 100)))

      val catalog = getTableCatalog[InMemoryTableCatalog](spark, "testcat")
      catalog.dropTable(testIdent)
      catalog.createTable(
        testIdent,
        new TableInfo.Builder()
          .withColumns(Array(
            Column.create("id", IntegerType),
            Column.create("salary", IntegerType)))
          .build())

      checkAnswer(spark.sql(s"SELECT * FROM $testTable"), Seq.empty)
    }
  }

  test(s"${testPrefix}connector w/ cache: repeated sql() stale after external drop/recreate") {
    withTable(cachingTestTable) {
      spark.sql(s"CREATE TABLE $cachingTestTable (id INT, salary INT) USING foo").collect()
      spark.sql(s"INSERT INTO $cachingTestTable VALUES (1, 100)").collect()
      checkAnswer(spark.sql(s"SELECT * FROM $cachingTestTable"), Seq(Row(1, 100)))

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
      checkAnswer(spark.sql(s"SELECT * FROM $cachingTestTable"), Seq(Row(1, 100)))

      // REFRESH TABLE invalidates the connector cache, new empty table visible
      spark.sql(s"REFRESH TABLE $cachingTestTable").collect()
      checkAnswer(spark.sql(s"SELECT * FROM $cachingTestTable"), Seq.empty)
    }
  }
}
