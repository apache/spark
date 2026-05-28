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
import org.apache.spark.sql.connector.catalog.{Column, InMemoryTableCatalog, NullTableIdAndNullColumnIdInMemoryTableCatalog, TableChange, TableInfo}
import org.apache.spark.sql.types.IntegerType

/**
 * Shared join refresh tests for DSv2 tables with incrementally constructed queries.
 * df1 and df2 are analyzed at different times, then joined. The refresh phase in
 * QueryExecution must align table versions across all references.
 *
 * This trait contains tests that produce identical results in both classic and Connect
 * modes. Tests that rely on classic-mode eager analysis (e.g. detecting schema changes
 * between df1 and df2) live in [[DataSourceV2DataFrameSuite]] directly.
 *
 * NOTE: All `session.sql(...)` calls append `.collect()` because Connect client DataFrames
 * are lazy and require an action to trigger execution. In classic mode `.collect()` on
 * eager statements (DDL, INSERT) is a no-op, so this is harmless.
 */
trait DSv2JoinRefreshTests extends DSv2ExternalMutationTestBase {

  // Scenario 1: join after insert refreshes both sides to latest version

  test(s"${testPrefix}SPARK-54157: join refreshes both sides after external insert" +
      " (table with both table and column ID support)") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100)").collect()

        val df1 = session.table(testTable)

        val catalog = getTableCatalog[InMemoryTableCatalog](session, "testcat")
        externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2, 200))

        val df2 = session.table(testTable)

        checkRows(
          df1.join(df2, df1("id") === df2("id")),
          Seq(Row(1, 100, 1, 100), Row(2, 200, 2, 200)))
      }
    }
  }

  test(s"${testPrefix}SPARK-54157: join refreshes both sides after same-session insert" +
      " (table with both table and column ID support)") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100)").collect()

        val df1 = session.table(testTable)

        session.sql(s"INSERT INTO $testTable VALUES (2, 200)").collect()

        val df2 = session.table(testTable)

        checkRows(
          df1.join(df2, df1("id") === df2("id")),
          Seq(Row(1, 100, 1, 100), Row(2, 200, 2, 200)))
      }
    }
  }

  // Scenario 4c: neither table ID nor column ID detects external drop/recreate.
  // Both classic and Connect re-resolve to the new (empty) table.

  test(s"${testPrefix}SPARK-54157: join does not detect external table drop and recreate" +
      " (table without table ID support and without column ID support)") {
    val nullBothT = "nullbothidscat.ns1.ns2.tbl"
    withTestSession { session =>
      withTestTableAndViews(session, nullBothT) {
        session.sql(s"CREATE TABLE $nullBothT (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $nullBothT VALUES (1, 100)").collect()

        val df1 = session.table(nullBothT)
        val catalog = getTableCatalog[NullTableIdAndNullColumnIdInMemoryTableCatalog](
          session, "nullbothidscat")
        assert(catalog.loadTable(testIdent).id == null,
          "NullTableIdAndNullColumnIdInMemoryTableCatalog should produce null table IDs")
        assert(catalog.loadTable(testIdent).columns().forall(_.id() == null),
          "NullTableIdAndNullColumnIdInMemoryTableCatalog should produce null column IDs")

        catalog.dropTable(testIdent)
        catalog.createTable(
          testIdent,
          new TableInfo.Builder()
            .withColumns(Array(
              Column.create("id", IntegerType),
              Column.create("salary", IntegerType)))
            .build())

        val df2 = session.table(nullBothT)

        // Neither TABLE_ID_MISMATCH nor COLUMN_ID_MISMATCH fires.
        // Both sides refresh to the recreated (empty) table. The join succeeds
        // but returns no rows because the new table has no data.
        checkRows(
          df1.join(df2, df1("id") === df2("id")),
          Seq.empty)
      }
    }
  }

  // Scenario 5b: neither table ID nor column ID detects external drop+re-add column.
  // Both classic and Connect re-resolve to the altered table.

  test(s"${testPrefix}SPARK-54157: join does not detect external drop+re-add column" +
      " (table without table ID support and without column ID support)") {
    val nullBothT = "nullbothidscat.ns1.ns2.tbl"
    withTestSession { session =>
      withTestTableAndViews(session, nullBothT) {
        session.sql(s"CREATE TABLE $nullBothT (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $nullBothT VALUES (1, 100)").collect()

        val df1 = session.table(nullBothT)

        val catalog = getTableCatalog[NullTableIdAndNullColumnIdInMemoryTableCatalog](
          session, "nullbothidscat")
        catalog.alterTable(
          testIdent, TableChange.deleteColumn(Array("salary"), false))
        catalog.alterTable(
          testIdent, TableChange.addColumn(Array("salary"), IntegerType, true))

        val df2 = session.table(nullBothT)

        // Neither TABLE_ID_MISMATCH nor COLUMN_ID_MISMATCH fires.
        // The change goes undetected and the join succeeds.
        checkRows(
          df1.join(df2, df1("id") === df2("id")),
          Seq(Row(1, null, 1, null)))
      }
    }
  }
}
