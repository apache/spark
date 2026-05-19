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
import java.util.Collections

import org.apache.spark.SparkConf
import org.apache.spark.sql.{classic, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{BufferedRows, CachingInMemoryTableCatalog, Column, Identifier, InMemoryBaseTable, InMemoryTableCatalog, TableCatalog}
import org.apache.spark.sql.connector.catalog.{TableChange, TableWritePrivilege}
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
 * Connect-mode equivalent of the repeated-sql() tests added to DataSourceV2DataFrameSuite in the
 * classic path.
 *
 * In Connect, every sql() call creates a fresh plan that is re-analyzed on the server, so it
 * always sees the latest data, schema, and table identity.
 *
 * The "DataFrame reuse" tests (at the bottom) test Connect-specific behavior: reusing the same
 * DataFrame across external mutations. In classic Spark, the resolved plan is captured at
 * DataFrame creation time, so reusing a DF after schema changes would fail. In Connect, each
 * action re-sends the plan to the server for fresh analysis.
 */
class DataSourceV2RepeatedSQLConnectSuite extends SparkConnectServerTest {

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    .set("spark.sql.catalog.testcat.copyOnLoad", "true")
    .set("spark.sql.catalog.cachingcat", classOf[CachingInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.cachingcat.copyOnLoad", "true")

  private val T = "testcat.ns1.ns2.tbl"
  private val CT = "cachingcat.ns1.ns2.tbl"
  private val ident = Identifier.of(Array("ns1", "ns2"), "tbl")

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

  private def withCleanup(session: SparkSession, table: String)(fn: => Unit): Unit = {
    try { fn }
    finally {
      session.sql(s"DROP TABLE IF EXISTS $table").collect()
      CachingInMemoryTableCatalog.clearCache()
    }
  }

  // Scenario 1: external writes
  test("[connect] repeated sql() reflects session write") {
    withSession { session =>
      withCleanup(session, T) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
        assertRows(session.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100)))

        session.sql(s"INSERT INTO $T VALUES (2, 200)").collect()
        assertRows(session.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  test("[connect] repeated sql() reflects external write") {
    withSession { session =>
      withCleanup(session, T) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
        assertRows(session.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100)))

        // external writer adds (2, 200)
        val serverSession = getServerSession(session)
        val cat = serverCatalog[InMemoryTableCatalog](serverSession, "testcat")
        val newSchema = StructType.fromDDL("id INT, salary INT")
        externalAppend(cat = cat, ident = ident, schema = newSchema, row = InternalRow(2, 200))

        assertRows(session.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  // Scenario 1 connector w/ cache (external write, caching connector)
  test("[connect] connector w/ cache: repeated sql() stale after external write") {
    withSession { session =>
      withCleanup(session, CT) {
        session.sql(s"CREATE TABLE $CT (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $CT VALUES (1, 100)").collect()
        assertRows(session.sql(s"SELECT * FROM $CT").collect(), Seq(Row(1, 100)))

        val serverSession = getServerSession(session)
        val cat = serverCatalog[CachingInMemoryTableCatalog](serverSession, "cachingcat")
        val newSchema = StructType.fromDDL("id INT, salary INT")
        externalAppend(cat = cat, ident = ident, schema = newSchema, row = InternalRow(2, 200))

        // Caching connector returns stale table: external write invisible
        assertRows(session.sql(s"SELECT * FROM $CT").collect(), Seq(Row(1, 100)))

        // REFRESH TABLE invalidates the connector cache, external write becomes visible
        session.sql(s"REFRESH TABLE $CT").collect()
        assertRows(session.sql(s"SELECT * FROM $CT").collect(), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  // Scenario 2: external schema changes
  test("[connect] repeated sql() reflects session schema change") {
    withSession { session =>
      withCleanup(session, T) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
        assertRows(session.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100)))

        session.sql(s"ALTER TABLE $T ADD COLUMN new_col INT").collect()
        session.sql(s"INSERT INTO $T VALUES (2, 200, -1)").collect()
        assertRows(
          session.sql(s"SELECT * FROM $T").collect(),
          Seq(Row(1, 100, null), Row(2, 200, -1)))
      }
    }
  }

  test("[connect] repeated sql() reflects external schema change") {
    withSession { session =>
      withCleanup(session, T) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
        assertRows(session.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100)))

        // external schema change + data write via catalog API
        val serverSession = getServerSession(session)
        val cat = serverCatalog[InMemoryTableCatalog](serverSession, "testcat")
        val addCol = TableChange.addColumn(Array("new_col"), IntegerType, true)
        cat.alterTable(ident, addCol)

        val newSchema = StructType.fromDDL("id INT, salary INT, new_col INT")
        externalAppend(
          cat = cat,
          ident = ident,
          schema = newSchema,
          row = InternalRow(2, 200, -1))

        assertRows(
          session.sql(s"SELECT * FROM $T").collect(),
          Seq(Row(1, 100, null), Row(2, 200, -1)))
      }
    }
  }

  // Scenario 2 connector w/ cache (external schema change, caching connector)
  test("[connect] connector w/ cache: repeated sql() stale after external schema change") {
    withSession { session =>
      withCleanup(session, CT) {
        session.sql(s"CREATE TABLE $CT (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $CT VALUES (1, 100)").collect()
        assertRows(session.sql(s"SELECT * FROM $CT").collect(), Seq(Row(1, 100)))

        val serverSession = getServerSession(session)
        val cat = serverCatalog[CachingInMemoryTableCatalog](serverSession, "cachingcat")
        val addCol = TableChange.addColumn(Array("new_col"), IntegerType, true)
        cat.alterTable(ident, addCol)

        val newSchema = StructType.fromDDL("id INT, salary INT, new_col INT")
        externalAppend(
          cat = cat,
          ident = ident,
          schema = newSchema,
          row = InternalRow(2, 200, -1))

        // Caching connector returns stale table: external changes invisible
        assertRows(session.sql(s"SELECT * FROM $CT").collect(), Seq(Row(1, 100)))

        // REFRESH TABLE invalidates the connector cache, schema change + data visible
        session.sql(s"REFRESH TABLE $CT").collect()
        assertRows(
          session.sql(s"SELECT * FROM $CT").collect(),
          Seq(Row(1, 100, null), Row(2, 200, -1)))
      }
    }
  }

  // Scenario 3: drop and recreate table
  test("[connect] repeated sql() reflects session drop/recreate") {
    withSession { session =>
      withCleanup(session, T) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
        assertRows(session.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100)))

        session.sql(s"DROP TABLE $T").collect()
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        assertRows(session.sql(s"SELECT * FROM $T").collect(), Seq.empty)
      }
    }
  }

  test("[connect] repeated sql() reflects external drop/recreate") {
    withSession { session =>
      withCleanup(session, T) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
        assertRows(session.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100)))

        // external drop and recreate via catalog API
        val serverSession = getServerSession(session)
        val cat = serverCatalog[InMemoryTableCatalog](serverSession, "testcat")
        cat.dropTable(ident)
        cat.createTable(
          ident,
          Array(Column.create("id", IntegerType), Column.create("salary", IntegerType)),
          Array.empty,
          Collections.emptyMap[String, String])

        assertRows(session.sql(s"SELECT * FROM $T").collect(), Seq.empty)
      }
    }
  }

  // Scenario 3 connector w/ cache (external drop/recreate, caching connector)
  test("[connect] connector w/ cache: repeated sql() stale after external drop/recreate") {
    withSession { session =>
      withCleanup(session, CT) {
        session.sql(s"CREATE TABLE $CT (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $CT VALUES (1, 100)").collect()
        assertRows(session.sql(s"SELECT * FROM $CT").collect(), Seq(Row(1, 100)))

        val serverSession = getServerSession(session)
        val cat = serverCatalog[CachingInMemoryTableCatalog](serverSession, "cachingcat")
        cat.dropTable(ident)
        cat.createTable(
          ident,
          Array(Column.create("id", IntegerType), Column.create("salary", IntegerType)),
          Array.empty,
          Collections.emptyMap[String, String])

        // Caching connector returns stale table: drop/recreate invisible
        assertRows(session.sql(s"SELECT * FROM $CT").collect(), Seq(Row(1, 100)))

        // REFRESH TABLE invalidates the connector cache, new empty table visible
        session.sql(s"REFRESH TABLE $CT").collect()
        assertRows(session.sql(s"SELECT * FROM $CT").collect(), Seq.empty)
      }
    }
  }

  // DataFrame reuse tests: these test Connect-specific behavior where reusing the same
  // DataFrame object across mutations still sees fresh data, because Connect re-sends
  // the plan for fresh analysis on every action.

  test("[connect] reused DataFrame reflects external write") {
    withSession { session =>
      withCleanup(session, T) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100)").collect()

        val df = session.sql(s"SELECT * FROM $T")
        assertRows(df.collect(), Seq(Row(1, 100)))

        // external write via catalog API
        val serverSession = getServerSession(session)
        val cat = serverCatalog[InMemoryTableCatalog](serverSession, "testcat")
        val newSchema = StructType.fromDDL("id INT, salary INT")
        externalAppend(cat = cat, ident = ident, schema = newSchema, row = InternalRow(2, 200))

        // same df object, Connect re-analyzes and sees the new row
        assertRows(df.collect(), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }

  test("[connect] reused DataFrame reflects external schema change") {
    withSession { session =>
      withCleanup(session, T) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100)").collect()

        val df = session.sql(s"SELECT * FROM $T")
        assertRows(df.collect(), Seq(Row(1, 100)))

        // external schema change + write via catalog API
        val serverSession = getServerSession(session)
        val cat = serverCatalog[InMemoryTableCatalog](serverSession, "testcat")
        val addCol = TableChange.addColumn(Array("new_col"), IntegerType, true)
        cat.alterTable(ident, addCol)

        val newSchema = StructType.fromDDL("id INT, salary INT, new_col INT")
        externalAppend(
          cat = cat,
          ident = ident,
          schema = newSchema,
          row = InternalRow(2, 200, -1))

        // same df object, Connect re-analyzes and sees the new schema
        assertRows(df.collect(), Seq(Row(1, 100, null), Row(2, 200, -1)))
      }
    }
  }

  test("[connect] reused DataFrame reflects external drop/recreate") {
    withSession { session =>
      withCleanup(session, T) {
        session.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        session.sql(s"INSERT INTO $T VALUES (1, 100)").collect()

        val df = session.sql(s"SELECT * FROM $T")
        assertRows(df.collect(), Seq(Row(1, 100)))

        // external drop and recreate via catalog API
        val serverSession = getServerSession(session)
        val cat = serverCatalog[InMemoryTableCatalog](serverSession, "testcat")
        cat.dropTable(ident)
        cat.createTable(
          ident,
          Array(Column.create("id", IntegerType), Column.create("salary", IntegerType)),
          Array.empty,
          Collections.emptyMap[String, String])

        // same df object, Connect re-analyzes against the new empty table
        assertRows(df.collect(), Seq.empty)
      }
    }
  }
}
