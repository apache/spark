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
import org.apache.spark.sql.connector.catalog.{Column, InMemoryTableCatalog, TableCatalog, TableChange, TableInfo}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Tests for incrementally constructed queries where df1 and df2 are analyzed at different
 * times, then joined. The refresh phase in QueryExecution must align table versions across
 * all references.
 *
 * Classic and Connect modes produce different results in some scenarios because in Connect
 * mode, resolution is deferred until execution, so both sides of a join always see the
 * latest table state.
 *
 * NOTE: All `session.sql(...)` calls append `.collect()` because Connect client DataFrames
 * are lazy and require an action to trigger execution. In classic mode `.collect()` on
 * eager statements (DDL, INSERT) is a no-op, so this is harmless.
 */
trait DSv2IncrementallyConstructedQueryTests extends DSv2ExternalMutationTestBase {

  // ---------------------------------------------------------------------------
  // Scenario 1: join after insert refreshes both sides to latest version.
  // Both classic and Connect see the inserted data.
  // ---------------------------------------------------------------------------

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

  // ---------------------------------------------------------------------------
  // Scenario 2: join after ADD COLUMN.
  // Classic: df1 keeps its original 2-column schema.
  // Connect: re-resolves df1 with the new 3-column schema.
  // ---------------------------------------------------------------------------

  test(s"${testPrefix}SPARK-54157: join after external ADD COLUMN" +
      " (table with both table and column ID support)") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100)").collect()

        val df1 = session.table(testTable)

        val catalog = getTableCatalog[InMemoryTableCatalog](session, "testcat")
        catalog.alterTable(
          testIdent, TableChange.addColumn(Array("new_column"), IntegerType, true))
        externalAppend(
          catalog = catalog, ident = testIdent, row = InternalRow(2, 200, -1))

        val df2 = session.table(testTable)
        val selfJoin = df1.join(df2, df1("id") === df2("id"))

        if (isConnect) {
          // Connect re-resolves df1 with the new 3-column schema (id, salary, new_column).
          assert(selfJoin.columns.length == 6,
            s"Expected 6 columns (3 + 3) but got: ${selfJoin.columns.mkString(", ")}")
          checkRows(selfJoin,
            Seq(Row(1, 100, null, 1, 100, null), Row(2, 200, -1, 2, 200, -1)))
        } else {
          // Classic: df1 keeps its original 2-column schema (id, salary).
          assert(selfJoin.columns.length == 5,
            s"Expected 5 columns (2 + 3) but got: ${selfJoin.columns.mkString(", ")}")
          checkRows(selfJoin,
            Seq(Row(1, 100, 1, 100, null), Row(2, 200, 2, 200, -1)))
        }
      }
    }
  }

  test(s"${testPrefix}SPARK-54157: join after same-session ADD COLUMN" +
      " (table with both table and column ID support)") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100)").collect()

        val df1 = session.table(testTable)

        session.sql(s"ALTER TABLE $testTable ADD COLUMN new_column INT").collect()
        session.sql(s"INSERT INTO $testTable VALUES (2, 200, -1)").collect()

        val df2 = session.table(testTable)
        val selfJoin = df1.join(df2, df1("id") === df2("id"))

        if (isConnect) {
          // Connect re-resolves df1 with the new 3-column schema (id, salary, new_column).
          assert(selfJoin.columns.length == 6,
            s"Expected 6 columns (3 + 3) but got: ${selfJoin.columns.mkString(", ")}")
          checkRows(selfJoin,
            Seq(Row(1, 100, null, 1, 100, null), Row(2, 200, -1, 2, 200, -1)))
        } else {
          // Classic: df1 keeps its original 2-column schema (id, salary).
          assert(selfJoin.columns.length == 5,
            s"Expected 5 columns (2 + 3) but got: ${selfJoin.columns.mkString(", ")}")
          checkRows(selfJoin,
            Seq(Row(1, 100, 1, 100, null), Row(2, 200, 2, 200, -1)))
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Scenario 3: join after DROP COLUMN.
  // Classic: df1 references the dropped column, fails with COLUMNS_MISMATCH.
  // Connect: re-resolves df1 without the dropped column, join succeeds.
  // ---------------------------------------------------------------------------

  test(s"${testPrefix}SPARK-54157: join after external DROP COLUMN" +
      " (table with both table and column ID support)") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100)").collect()

        val df1 = session.table(testTable)

        val catalog = getTableCatalog[InMemoryTableCatalog](session, "testcat")
        catalog.alterTable(
          testIdent, TableChange.deleteColumn(Array("salary"), false))
        externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2))

        val df2 = session.table(testTable)

        if (isConnect) {
          // Connect re-resolves df1 without the dropped column.
          checkRows(
            df1.join(df2, df1("id") === df2("id")),
            Seq(Row(1, 1), Row(2, 2)))
        } else {
          // Classic: df1 references the dropped column.
          checkError(
            exception = intercept[AnalysisException] {
              df1.join(df2, df1("id") === df2("id")).collect()
            },
            condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH",
            matchPVals = true,
            parameters = Map("tableName" -> ".*", "errors" -> "(?s).*"))
        }
      }
    }
  }

  test(s"${testPrefix}SPARK-54157: join after same-session DROP COLUMN" +
      " (table with both table and column ID support)") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100)").collect()

        val df1 = session.table(testTable)

        session.sql(s"ALTER TABLE $testTable DROP COLUMN salary").collect()
        session.sql(s"INSERT INTO $testTable VALUES (2)").collect()

        val df2 = session.table(testTable)

        if (isConnect) {
          // Connect re-resolves df1 without the dropped column.
          checkRows(
            df1.join(df2, df1("id") === df2("id")),
            Seq(Row(1, 1), Row(2, 2)))
        } else {
          // Classic: df1 references the dropped column.
          checkError(
            exception = intercept[AnalysisException] {
              df1.join(df2, df1("id") === df2("id")).collect()
            },
            condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH",
            matchPVals = true,
            parameters = Map("tableName" -> ".*", "errors" -> "(?s).*"))
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Scenario 4: external drop and recreate table.
  // 4a: table ID detects it, TABLE_ID_MISMATCH in classic, succeeds in Connect
  // 4b: column IDs detect it, COLUMN_ID_MISMATCH in classic, succeeds in Connect
  // 4c: no IDs, goes undetected, join succeeds (both modes)
  // ---------------------------------------------------------------------------

  test(s"${testPrefix}SPARK-54157: join after external table drop and recreate" +
      " (table with both table and column ID support)") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100)").collect()

        val df1 = session.table(testTable)
        val catalog = getTableCatalog[InMemoryTableCatalog](session, "testcat")
        val originTableId = catalog.loadTable(testIdent).id

        catalog.dropTable(testIdent)
        catalog.createTable(
          testIdent,
          new TableInfo.Builder()
            .withColumns(Array(
              Column.create("id", IntegerType),
              Column.create("salary", IntegerType)))
            .build())
        externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2, 200))

        val df2 = session.table(testTable)
        val newTableId = catalog.loadTable(testIdent).id
        assert(originTableId != newTableId)

        if (isConnect) {
          // Connect re-resolves both sides to the recreated table.
          checkRows(
            df1.join(df2, df1("id") === df2("id")),
            Seq(Row(2, 200, 2, 200)))
        } else {
          // Classic: table ID changed.
          checkError(
            exception = intercept[AnalysisException] {
              df1.join(df2, df1("id") === df2("id")).collect()
            },
            condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.TABLE_ID_MISMATCH",
            matchPVals = true,
            parameters = Map(
              "tableName" -> ".*",
              "capturedTableId" -> ".*",
              "currentTableId" -> ".*"))
        }
      }
    }
  }

  test(s"${testPrefix}SPARK-54157: join after external drop/recreate" +
      " (table without table ID support, but with column ID support)") {
    val nullIdT = "nullidcat.ns1.ns2.tbl"
    withTestSession { session =>
      withTestTableAndViews(session, nullIdT) {
        session.sql(s"CREATE TABLE $nullIdT (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $nullIdT VALUES (1, 100)").collect()

        val df1 = session.table(nullIdT)
        val catalog = getTableCatalog[TableCatalog](session, "nullidcat")
        assert(catalog.loadTable(testIdent).id == null,
          "NullTableIdInMemoryTableCatalog should produce null table IDs")

        catalog.dropTable(testIdent)
        catalog.createTable(
          testIdent,
          new TableInfo.Builder()
            .withColumns(Array(
              Column.create("id", IntegerType),
              Column.create("salary", IntegerType)))
            .build())
        externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2, 200))

        val df2 = session.table(nullIdT)

        if (isConnect) {
          // Connect re-resolves both sides to the recreated table.
          checkRows(
            df1.join(df2, df1("id") === df2("id")),
            Seq(Row(2, 200, 2, 200)))
        } else {
          // Classic: column IDs changed.
          checkError(
            exception = intercept[AnalysisException] {
              df1.join(df2, df1("id") === df2("id")).collect()
            },
            condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
            matchPVals = true,
            parameters = Map("tableName" -> ".*", "errors" -> "(?s).*"))
        }
      }
    }
  }

  test(s"${testPrefix}SPARK-54157: join does not detect external table drop and recreate" +
      " (table without table ID support and without column ID support)") {
    val nullBothT = "nullbothidscat.ns1.ns2.tbl"
    withTestSession { session =>
      withTestTableAndViews(session, nullBothT) {
        session.sql(s"CREATE TABLE $nullBothT (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $nullBothT VALUES (1, 100)").collect()

        val df1 = session.table(nullBothT)
        val catalog = getTableCatalog[TableCatalog](
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
        externalAppend(catalog = catalog, ident = testIdent, row = InternalRow(2, 200))

        val df2 = session.table(nullBothT)

        // Neither TABLE_ID_MISMATCH nor COLUMN_ID_MISMATCH fires, so the drop and
        // recreate goes undetected. Both sides refresh to the recreated table and
        // the join sees the row appended after recreate, in both classic and Connect.
        checkRows(
          df1.join(df2, df1("id") === df2("id")),
          Seq(Row(2, 200, 2, 200)))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Scenario 5: external drop+re-add column.
  // 5a: column IDs detect it, COLUMN_ID_MISMATCH in classic, succeeds in Connect
  // 5b: no IDs, goes undetected, join succeeds (both modes)
  // ---------------------------------------------------------------------------

  test(s"${testPrefix}SPARK-54157: join after external drop+re-add column" +
      " (table without table ID support, but with column ID support)") {
    val nullIdT = "nullidcat.ns1.ns2.tbl"
    withTestSession { session =>
      withTestTableAndViews(session, nullIdT) {
        session.sql(s"CREATE TABLE $nullIdT (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $nullIdT VALUES (1, 100)").collect()

        val df1 = session.table(nullIdT)

        val catalog = getTableCatalog[TableCatalog](session, "nullidcat")
        catalog.alterTable(
          testIdent, TableChange.deleteColumn(Array("salary"), false))
        catalog.alterTable(
          testIdent, TableChange.addColumn(Array("salary"), IntegerType, true))

        val df2 = session.table(nullIdT)

        if (isConnect) {
          // Connect re-resolves both sides with the new column ID.
          checkRows(
            df1.join(df2, df1("id") === df2("id")),
            Seq(Row(1, null, 1, null)))
        } else {
          // Classic: column ID changed.
          checkError(
            exception = intercept[AnalysisException] {
              df1.join(df2, df1("id") === df2("id")).collect()
            },
            condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
            matchPVals = true,
            parameters = Map("tableName" -> ".*", "errors" -> "(?s).*"))
        }
      }
    }
  }

  test(s"${testPrefix}SPARK-54157: join does not detect external drop+re-add column" +
      " (table without table ID support and without column ID support)") {
    val nullBothT = "nullbothidscat.ns1.ns2.tbl"
    withTestSession { session =>
      withTestTableAndViews(session, nullBothT) {
        session.sql(s"CREATE TABLE $nullBothT (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $nullBothT VALUES (1, 100)").collect()

        val df1 = session.table(nullBothT)

        val catalog = getTableCatalog[TableCatalog](
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

  // ---------------------------------------------------------------------------
  // Scenario 6: external type change (drop INT column, add STRING column).
  // The delete removes the old column ID and the add assigns a fresh one,
  // so the column ID check fires (COLUMN_ID_MISMATCH) in classic before schema
  // validation gets a chance to compare data types.
  // Connect re-resolves both sides with the new column ID.
  // ---------------------------------------------------------------------------

  test(s"${testPrefix}SPARK-54157: join after external drop+re-add different-type column" +
      " (table with both table and column ID support)") {
    withTestSession { session =>
      withTestTableAndViews(session, testTable) {
        session.sql(s"CREATE TABLE $testTable (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $testTable VALUES (1, 100)").collect()

        val df1 = session.table(testTable)

        val catalog = getTableCatalog[InMemoryTableCatalog](session, "testcat")
        catalog.alterTable(
          testIdent, TableChange.deleteColumn(Array("salary"), false))
        catalog.alterTable(
          testIdent, TableChange.addColumn(Array("salary"), StringType, true))
        externalAppend(catalog = catalog, ident = testIdent,
          row = InternalRow(2, UTF8String.fromString("high")))

        val df2 = session.table(testTable)

        if (isConnect) {
          // Connect re-resolves both sides with the new column type.
          checkRows(
            df1.join(df2, df1("id") === df2("id")),
            Seq(Row(1, null, 1, null), Row(2, "high", 2, "high")))
        } else {
          // Classic: column ID changed.
          checkError(
            exception = intercept[AnalysisException] {
              df1.join(df2, df1("id") === df2("id")).collect()
            },
            condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
            matchPVals = true,
            parameters = Map("tableName" -> ".*", "errors" -> "(?s).*"))
        }
      }
    }
  }
}
