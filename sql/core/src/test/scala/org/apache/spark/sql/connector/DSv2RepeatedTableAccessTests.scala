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
import org.apache.spark.sql.connector.catalog.{CachingInMemoryTableCatalog, Column, Identifier, InMemoryTableCatalog, TableChange, TableInfo}
import org.apache.spark.sql.types.IntegerType

/**
 * Shared repeated table access tests with external changes for DSv2 tables. These tests verify
 * that repeated `sql()` calls correctly reflect external mutations made via the catalog API:
 *
 *  - Scenario 1 (external writes): external data appended via the catalog API is visible.
 *  - Scenario 2 (external schema changes): external ADD COLUMN via the catalog API is visible.
 *  - Scenario 3 (external drop/recreate): external drop and recreate via the catalog API
 *    resolves to the new empty table.
 *
 * Each scenario includes a session-write baseline, an external-write test, and a
 * caching-connector variant showing stale results until `REFRESH TABLE`.
 *
 * NOTE: All `session.sql(...)` calls append `.collect()` because Connect client DataFrames
 * are lazy and require an action to trigger execution. In classic mode `.collect()` on DDL
 * is a no-op (DDL executes eagerly), so this is harmless.
 */
trait DSv2RepeatedTableAccessTests extends DSv2ExternalMutationTestBase {

  private val T = "testcat.ns1.ns2.tbl"
  private val CT = "cachingcat.ns1.ns2.tbl"
  private val testIdent = Identifier.of(Array("ns1", "ns2"), "tbl")

  // Scenario 1: data changes via writes

  test(s"${testPrefix}repeated sql() reflects session write") {
    withTestSession { session =>
      withTestTableAndViews(session, T) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
        checkRows(session.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

        session.sql(s"INSERT INTO $T VALUES (2, 200)").collect()
        checkRows(session.sql(s"SELECT * FROM $T"), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  test(s"${testPrefix}repeated sql() reflects external write") {
    withTestSession { session =>
      withTestTableAndViews(session, T) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
        checkRows(session.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[InMemoryTableCatalog](session, "testcat")
        externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2, 200))

        checkRows(session.sql(s"SELECT * FROM $T"), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  test(s"${testPrefix}connector w/ cache: repeated sql() stale after external write") {
    withTestSession { session =>
      withTestTableAndViews(session, CT) {
        session.sql(s"CREATE TABLE $CT (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $CT VALUES (1, 100)").collect()
        checkRows(session.sql(s"SELECT * FROM $CT"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[CachingInMemoryTableCatalog](session, "cachingcat")
        externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2, 200))

        // Caching connector returns stale table: external write invisible
        checkRows(session.sql(s"SELECT * FROM $CT"), Seq(Row(1, 100)))

        // REFRESH TABLE invalidates the connector cache, external write becomes visible
        session.sql(s"REFRESH TABLE $CT").collect()
        checkRows(session.sql(s"SELECT * FROM $CT"), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  // Scenario 2: schema changes

  test(s"${testPrefix}repeated sql() reflects session schema change") {
    withTestSession { session =>
      withTestTableAndViews(session, T) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
        checkRows(session.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

        session.sql(s"ALTER TABLE $T ADD COLUMN new_col INT").collect()
        session.sql(s"INSERT INTO $T VALUES (2, 200, -1)").collect()
        checkRows(
          session.sql(s"SELECT * FROM $T"),
          Seq(Row(1, 100, null), Row(2, 200, -1)))
      }
    }
  }

  test(s"${testPrefix}repeated sql() reflects external schema change") {
    withTestSession { session =>
      withTestTableAndViews(session, T) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
        checkRows(session.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[InMemoryTableCatalog](session, "testcat")
        val addCol = TableChange.addColumn(Array("new_col"), IntegerType, true)
        catalog.alterTable(testIdent, addCol)

        externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2, 200, -1))

        checkRows(
          session.sql(s"SELECT * FROM $T"),
          Seq(Row(1, 100, null), Row(2, 200, -1)))
      }
    }
  }

  test(s"${testPrefix}connector w/ cache: repeated sql() stale after external schema change") {
    withTestSession { session =>
      withTestTableAndViews(session, CT) {
        session.sql(s"CREATE TABLE $CT (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $CT VALUES (1, 100)").collect()
        checkRows(session.sql(s"SELECT * FROM $CT"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[CachingInMemoryTableCatalog](session, "cachingcat")
        val addCol = TableChange.addColumn(Array("new_col"), IntegerType, true)
        catalog.alterTable(testIdent, addCol)

        externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2, 200, -1))

        // Caching connector returns stale table: external changes invisible
        checkRows(session.sql(s"SELECT * FROM $CT"), Seq(Row(1, 100)))

        // REFRESH TABLE invalidates the connector cache, schema change + data visible
        session.sql(s"REFRESH TABLE $CT").collect()
        checkRows(
          session.sql(s"SELECT * FROM $CT"),
          Seq(Row(1, 100, null), Row(2, 200, -1)))
      }
    }
  }

  // Scenario 3: drop and recreate table

  test(s"${testPrefix}repeated sql() reflects session drop/recreate") {
    withTestSession { session =>
      withTestTableAndViews(session, T) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
        checkRows(session.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

        session.sql(s"DROP TABLE $T").collect()
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        checkRows(session.sql(s"SELECT * FROM $T"), Seq.empty)
      }
    }
  }

  test(s"${testPrefix}repeated sql() reflects external drop/recreate") {
    withTestSession { session =>
      withTestTableAndViews(session, T) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
        checkRows(session.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

        val catalog = getTableCatalog[InMemoryTableCatalog](session, "testcat")
        catalog.dropTable(testIdent)
        catalog.createTable(
          testIdent,
          new TableInfo.Builder()
            .withColumns(Array(
              Column.create("id", IntegerType),
              Column.create("salary", IntegerType)))
            .build())

        checkRows(session.sql(s"SELECT * FROM $T"), Seq.empty)
      }
    }
  }

  test(s"${testPrefix}connector w/ cache: repeated sql() stale after external drop/recreate") {
    withTestSession { session =>
      withTestTableAndViews(session, CT) {
        session.sql(s"CREATE TABLE $CT (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $CT VALUES (1, 100)").collect()
        checkRows(session.sql(s"SELECT * FROM $CT"), Seq(Row(1, 100)))

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
        checkRows(session.sql(s"SELECT * FROM $CT"), Seq(Row(1, 100)))

        // REFRESH TABLE invalidates the connector cache, new empty table visible
        session.sql(s"REFRESH TABLE $CT").collect()
        checkRows(session.sql(s"SELECT * FROM $CT"), Seq.empty)
      }
    }
  }
}
