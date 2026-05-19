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
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.classic
import org.apache.spark.sql.connector.catalog.{BufferedRows, Column, Identifier, InMemoryBaseTable, InMemoryTableCatalog, NullTableIdAndNullColumnIdInMemoryTableCatalog, NullTableIdInMemoryTableCatalog, TableCatalog, TableChange, TableInfo, TableWritePrivilege}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * DSv2 join tests for Spark Connect mirroring the classic DataSourceV2DataFrameSuite join
 * scenarios.
 *
 * In Connect, Datasets are re-analyzed on every action, so operations that fail in classic mode
 * (where each side pins its schema at analysis time) succeed here because both sides always get a
 * fresh plan with the latest schema and data.
 */
class DataSourceV2JoinConnectSuite extends SparkConnectServerTest {

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    .set("spark.sql.catalog.testcat.copyOnLoad", "true")
    .set("spark.sql.catalog.nullidcat", classOf[NullTableIdInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.nullidcat.copyOnLoad", "true")
    .set(
      "spark.sql.catalog.nullbothidscat",
      classOf[NullTableIdAndNullColumnIdInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.nullbothidscat.copyOnLoad", "true")

  private val T = "testcat.ns1.ns2.tbl"
  private val ident = Identifier.of(Array("ns1", "ns2"), "tbl")

  /** Get a catalog from the server-side session by name. */
  private def serverCatalog[T <: TableCatalog](
      serverSession: classic.SparkSession,
      catalogName: String): T =
    serverSession.sessionState.catalogManager.catalog(catalogName).asInstanceOf[T]

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

  private def assertRows(actual: Array[Row], expected: Seq[Row]): Unit = {
    assert(
      actual.toSeq.sortBy(_.toString()) == expected.sortBy(_.toString()),
      s"Expected ${expected.mkString(", ")} but got ${actual.mkString(", ")}")
  }

  /**
   * Creates a table, inserts initial data, and provides the server session to the test body.
   * Cleans up (DROP TABLE) in `finally` so cleanup runs even when a test fails.
   */
  private def withTable(
      session: SparkSession,
      tableName: String = T,
      tableSchema: String = "id INT, salary INT",
      insertValues: String = "(1, 100)")(f: classic.SparkSession => Unit): Unit = {
    try {
      session.sql(s"CREATE TABLE $tableName ($tableSchema) USING foo").collect()
      session.sql(s"INSERT INTO $tableName VALUES $insertValues").collect()
      val serverSession = getServerSession(session)
      f(serverSession)
    } finally {
      session.sql(s"DROP TABLE IF EXISTS $tableName").collect()
    }
  }

  // Scenario 1: join after insert refreshes both sides to latest version.
  // Datasets are re-analyzed, same result as classic.
  test(
    "[connect][S1] join refreshes both sides after external insert" +
      " (table with both table and column ID support)") {
    withSession { session =>
      withTable(session) { serverSession =>
        val df1 = session.table(T)

        // external writer adds (2, 200) via direct catalog API
        val cat = serverCatalog[InMemoryTableCatalog](serverSession, "testcat")
        externalAppend(
          cat = cat,
          ident = ident,
          schema = StructType.fromDDL("id INT, salary INT"),
          row = InternalRow(2, 200))

        val df2 = session.table(T)

        assertRows(
          df1.join(df2, df1("id") === df2("id")).collect(),
          Seq(Row(1, 100, 1, 100), Row(2, 200, 2, 200)))
      }
    }
  }

  test(
    "[connect][S1] join refreshes both sides after same-session insert" +
      " (table with both table and column ID support)") {
    withSession { session =>
      withTable(session) { _ =>
        val df1 = session.table(T)

        // session insert via SQL
        session.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

        val df2 = session.table(T)

        assertRows(
          df1.join(df2, df1("id") === df2("id")).collect(),
          Seq(Row(1, 100, 1, 100), Row(2, 200, 2, 200)))
      }
    }
  }

  // Scenario 2: join after ADD COLUMN.
  // Datasets are re-analyzed: both sides see the 3-column schema.
  // Classic preserves df1's 2-column schema because the plan is pinned at analysis time.
  test(
    "[connect][S2] join after external ADD COLUMN sees new schema on both sides" +
      " (table with both table and column ID support)") {
    withSession { session =>
      withTable(session) { serverSession =>
        val df1 = session.table(T)

        // external schema change via catalog API
        val cat = serverCatalog[InMemoryTableCatalog](serverSession, "testcat")
        val addCol = TableChange.addColumn(Array("new_column"), IntegerType, true)
        cat.alterTable(ident, addCol)

        // external writer adds (2, 200, -1) with new schema
        externalAppend(
          cat = cat,
          ident = ident,
          schema = StructType.fromDDL("id INT, salary INT, new_column INT"),
          row = InternalRow(2, 200, -1))

        val df2 = session.table(T)

        assertRows(
          df1.join(df2, df1("id") === df2("id")).collect(),
          Seq(Row(1, 100, null, 1, 100, null), Row(2, 200, -1, 2, 200, -1)))
      }
    }
  }

  test(
    "[connect][S2] join after same-session ADD COLUMN sees new schema on both sides" +
      " (table with both table and column ID support)") {
    withSession { session =>
      withTable(session) { _ =>
        val df1 = session.table(T)

        // session schema change + data via SQL
        session.sql(s"ALTER TABLE $T ADD COLUMN new_column INT").collect()
        session.sql(s"INSERT INTO $T VALUES (2, 200, -1)").collect()

        val df2 = session.table(T)

        assertRows(
          df1.join(df2, df1("id") === df2("id")).collect(),
          Seq(Row(1, 100, null, 1, 100, null), Row(2, 200, -1, 2, 200, -1)))
      }
    }
  }

  // Scenario 3: join after DROP COLUMN.
  // Datasets are re-analyzed: both sides see the 1-column schema.
  // Classic fails with COLUMNS_MISMATCH because df1's pinned plan still references
  // the dropped column.
  test(
    "[connect][S3] join after external DROP COLUMN succeeds" +
      " (table with both table and column ID support)") {
    withSession { session =>
      withTable(session) { serverSession =>
        val df1 = session.table(T)

        // external column removal via catalog API
        val cat = serverCatalog[InMemoryTableCatalog](serverSession, "testcat")
        val dropCol = TableChange.deleteColumn(Array("salary"), false)
        cat.alterTable(ident, dropCol)

        // external writer adds (2) with 1-col schema
        externalAppend(
          cat = cat,
          ident = ident,
          schema = StructType.fromDDL("id INT"),
          row = InternalRow(2))

        val df2 = session.table(T)

        assertRows(df1.join(df2, df1("id") === df2("id")).collect(), Seq(Row(1, 1), Row(2, 2)))
      }
    }
  }

  test(
    "[connect][S3] join after same-session DROP COLUMN succeeds" +
      " (table with both table and column ID support)") {
    withSession { session =>
      withTable(session) { _ =>
        val df1 = session.table(T)

        // session column removal + insert via SQL
        session.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()
        session.sql(s"INSERT INTO $T VALUES (2)").collect()

        val df2 = session.table(T)

        assertRows(df1.join(df2, df1("id") === df2("id")).collect(), Seq(Row(1, 1), Row(2, 2)))
      }
    }
  }

  // Scenario 4a: join after external drop and recreate table.
  // Datasets are re-analyzed: both sides resolve against the new table.
  // Classic fails with TABLE_ID_MISMATCH because df1's pinned plan captured the
  // original table ID.
  test(
    "[connect][S4a] join after external table drop and recreate succeeds" +
      " (table with both table and column ID support)") {
    withSession { session =>
      withTable(session) { serverSession =>
        val df1 = session.table(T)

        // external drop and recreate via catalog API
        val cat = serverCatalog[InMemoryTableCatalog](serverSession, "testcat")
        cat.dropTable(ident)
        cat.createTable(
          ident,
          new TableInfo.Builder()
            .withColumns(
              Array(Column.create("id", IntegerType), Column.create("salary", IntegerType)))
            .build())

        val df2 = session.table(T)

        val result = df1.join(df2, df1("id") === df2("id"))
        assert(result.schema.fieldNames.toSeq == Seq("id", "salary", "id", "salary"))
        assertRows(result.collect(), Seq.empty)
      }
    }
  }

  // Scenario 4b: external drop/recreate with no table ID but with column IDs.
  // Datasets are re-analyzed: both sides resolve against the new table.
  // Classic fails with COLUMN_ID_MISMATCH because df1's pinned column IDs differ
  // from the recreated table's.
  test(
    "[connect][S4b] join after external drop/recreate succeeds" +
      " (table without table ID support, but with column ID support)") {
    val NC = "nullidcat.ns1.ns2.tbl"
    val ncIdent = Identifier.of(Array("ns1", "ns2"), "tbl")
    withSession { session =>
      withTable(session, tableName = NC) { serverSession =>
        val df1 = session.table(NC)

        // external drop and recreate via catalog API
        val cat = serverCatalog[NullTableIdInMemoryTableCatalog](serverSession, "nullidcat")
        cat.dropTable(ncIdent)
        cat.createTable(
          ncIdent,
          new TableInfo.Builder()
            .withColumns(
              Array(Column.create("id", IntegerType), Column.create("salary", IntegerType)))
            .build())

        val df2 = session.table(NC)

        val result = df1.join(df2, df1("id") === df2("id"))
        assert(result.schema.fieldNames.toSeq == Seq("id", "salary", "id", "salary"))
        assertRows(result.collect(), Seq.empty)
      }
    }
  }

  // Scenario 4c: external drop/recreate with no table ID and no column IDs.
  // Datasets are re-analyzed. Classic also succeeds since neither ID check fires.
  test(
    "[connect][S4c] join after external table drop and recreate succeeds" +
      " (table without table ID support and without column ID support)") {
    val NC = "nullbothidscat.ns1.ns2.tbl"
    val ncIdent = Identifier.of(Array("ns1", "ns2"), "tbl")
    withSession { session =>
      withTable(session, tableName = NC) { serverSession =>
        val df1 = session.table(NC)

        // external drop and recreate via catalog API
        val cat = serverCatalog[NullTableIdAndNullColumnIdInMemoryTableCatalog](
          serverSession,
          "nullbothidscat")
        cat.dropTable(ncIdent)
        cat.createTable(
          ncIdent,
          new TableInfo.Builder()
            .withColumns(
              Array(Column.create("id", IntegerType), Column.create("salary", IntegerType)))
            .build())

        val df2 = session.table(NC)

        val result = df1.join(df2, df1("id") === df2("id"))
        assert(result.schema.fieldNames.toSeq == Seq("id", "salary", "id", "salary"))
        assertRows(result.collect(), Seq.empty)
      }
    }
  }

  // Scenario 5a: two separate external alterTable calls assign a fresh column ID.
  // Datasets are re-analyzed: both sides see the new schema.
  // Classic fails with COLUMN_ID_MISMATCH because the re-added column gets a fresh ID.
  test(
    "[connect][S5a] join after external drop+re-add column succeeds" +
      " (table without table ID support, but with column ID support)") {
    val NC = "nullidcat.ns1.ns2.tbl"
    val ncIdent = Identifier.of(Array("ns1", "ns2"), "tbl")
    withSession { session =>
      withTable(session, tableName = NC) { serverSession =>
        val df1 = session.table(NC)

        // A single alterTable call that deletes salary (and its ID) and adds a new salary
        // column with a fresh ID.
        val cat = serverCatalog[NullTableIdInMemoryTableCatalog](serverSession, "nullidcat")
        cat.alterTable(
          ncIdent,
          TableChange.deleteColumn(Array("salary"), false),
          TableChange.addColumn(Array("salary"), IntegerType, true))

        val df2 = session.table(NC)

        val result = df1.join(df2, df1("id") === df2("id"))
        assert(result.schema.fieldNames.toSeq == Seq("id", "salary", "id", "salary"))
        assertRows(result.collect(), Seq(Row(1, null, 1, null)))
      }
    }
  }

  // Scenario 5b: external drop+re-add column with no table ID and no column IDs.
  // Datasets are re-analyzed. Classic also succeeds since neither ID check fires.
  test(
    "[connect][S5b] join after external drop+re-add column succeeds" +
      " (table without table ID support and without column ID support)") {
    val NC = "nullbothidscat.ns1.ns2.tbl"
    val ncIdent = Identifier.of(Array("ns1", "ns2"), "tbl")
    withSession { session =>
      withTable(session, tableName = NC) { serverSession =>
        val df1 = session.table(NC)

        // A single alterTable call with no column IDs to detect the change.
        val cat = serverCatalog[NullTableIdAndNullColumnIdInMemoryTableCatalog](
          serverSession,
          "nullbothidscat")
        cat.alterTable(
          ncIdent,
          TableChange.deleteColumn(Array("salary"), false),
          TableChange.addColumn(Array("salary"), IntegerType, true))

        val df2 = session.table(NC)

        val result = df1.join(df2, df1("id") === df2("id"))
        assert(result.schema.fieldNames.toSeq == Seq("id", "salary", "id", "salary"))
        assertRows(result.collect(), Seq(Row(1, null, 1, null)))
      }
    }
  }

  // Scenario 6: external type change.
  // Datasets are re-analyzed: both sides see salary as STRING.
  // Classic fails with COLUMNS_MISMATCH because df1's pinned plan still expects INT.
  test(
    "[connect][S6] join after external drop+re-add different-type column succeeds" +
      " (table with both table and column ID support)") {
    withSession { session =>
      withTable(session) { serverSession =>
        val df1 = session.table(T)

        // external drop and re-add column with different type via catalog API
        val cat = serverCatalog[InMemoryTableCatalog](serverSession, "testcat")
        val dropCol = TableChange.deleteColumn(Array("salary"), false)
        val addCol = TableChange.addColumn(Array("salary"), StringType, true)
        cat.alterTable(ident, dropCol, addCol)

        // external writer adds (2, "high") with new schema
        externalAppend(
          cat = cat,
          ident = ident,
          schema = StructType.fromDDL("id INT, salary STRING"),
          row = InternalRow(2, UTF8String.fromString("high")))

        val df2 = session.table(T)

        val result = df1.join(df2, df1("id") === df2("id"))
        assert(result.schema.fieldNames.toSeq == Seq("id", "salary", "id", "salary"))
        assertRows(result.collect(), Seq(Row(1, null, 1, null), Row(2, "high", 2, "high")))
      }
    }
  }
}
