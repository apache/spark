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

import java.util.Collections

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.classic
import org.apache.spark.sql.connector.catalog.{BufferedRows, Column, Identifier, InMemoryBaseTable, InMemoryTableCatalog, TableCatalog, TableChange, TableWritePrivilege}
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
 * DSv2 CACHE TABLE tests for Spark Connect covering five cross-session cache scenarios (S1
 * through S5).
 *
 * Uses an in-process Connect server ([[SparkConnectServerTest]]) so that the test can access the
 * server's catalog directly. A Connect client performs cache and SQL operations; external writes
 * go through the catalog API ([[InMemoryBaseTable.withData]]), which bypasses the
 * [[CacheManager]]. This simulates a truly external writer whose changes are invisible to cached
 * reads.
 *
 * Each scenario validates server-side cache state (via [[assertCached]] on the server session,
 * since cache plan internals are not exposed through Connect) and reads data through the Connect
 * client (via [[assertRows]]) to simulate the real client experience.
 */
class DataSourceV2CacheConnectSuite extends SparkConnectServerTest {

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    .set("spark.sql.catalog.testcat.copyOnLoad", "true")

  private val T = "testcat.ns1.ns2.tbl"
  private val ident = Identifier.of(Array("ns1", "ns2"), "tbl")

  /**
   * Assert that rows collected through the Connect client match expected rows (order-agnostic).
   */
  private def assertRows(actual: Array[Row], expected: Seq[Row]): Unit = {
    assert(
      actual.toSeq.sortBy(_.toString()) == expected.sortBy(_.toString()),
      s"Expected ${expected.mkString(", ")} but got ${actual.mkString(", ")}")
  }

  /** Get the catalog from the server-side session. */
  private def serverCatalog[T <: TableCatalog: ClassTag](
      serverSession: classic.SparkSession, name: String): T =
    serverSession.sessionState.catalogManager.catalog(name).asInstanceOf[T]

  /** Drop tables in `finally` so cleanup runs even when a test fails. */
  private def withTable(session: SparkSession, tableNames: String*)(f: => Unit): Unit = {
    try f finally {
      tableNames.foreach { name =>
        session.sql(s"UNCACHE TABLE IF EXISTS $name").collect()
        session.sql(s"DROP TABLE IF EXISTS $name").collect()
      }
    }
  }

  // Scenario 1: external write after CACHE TABLE is invisible (cache pinned).
  // Scenario 2: session write invalidates cache; subsequent external write
  // is again invisible.
  test("[S1+S2] CACHE TABLE pins state; session write invalidates, external does not") {
    withSession { connectSession =>
      withTable(connectSession, T) {
        // create table and cache via Connect
        connectSession.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        connectSession.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
        connectSession.sql(s"CACHE TABLE $T").collect()
        // get server session after first RPC establishes it
        val serverSession = getServerSession(connectSession)
        assertCached(serverSession.table(T))
        assertRows(connectSession.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100)))

        // S1: external writer adds (2, 200) via direct catalog API
        // (bypasses this session's CacheManager)
        val schema = StructType.fromDDL("id INT, salary INT")
        val cat = serverCatalog[InMemoryTableCatalog](serverSession, "testcat")
        val extTable = cat
          .loadTable(ident, java.util.Set.of(TableWritePrivilege.INSERT))
          .asInstanceOf[InMemoryBaseTable]
        extTable.withData(Array(new BufferedRows(Seq.empty, schema).withRow(InternalRow(2, 200))))

        // cache is pinned, external write invisible
        assertCached(serverSession.table(T))
        assertRows(connectSession.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100)))

        // S2: UNCACHE + re-CACHE picks up external write, then session
        // write invalidates and recaches.
        connectSession.sql(s"UNCACHE TABLE $T").collect()
        connectSession.sql(s"CACHE TABLE $T").collect()
        assertCached(serverSession.table(T))
        assertRows(
          connectSession.sql(s"SELECT * FROM $T").collect(),
          Seq(Row(1, 100), Row(2, 200)))

        connectSession.sql(s"INSERT INTO $T VALUES (3, 300)").collect()
        assertCached(serverSession.table(T))
        assertRows(
          connectSession.sql(s"SELECT * FROM $T").collect(),
          Seq(Row(1, 100), Row(2, 200), Row(3, 300)))

        // external writer adds (4, 400) via direct catalog API
        val extTable2 = cat
          .loadTable(ident, java.util.Set.of(TableWritePrivilege.INSERT))
          .asInstanceOf[InMemoryBaseTable]
        extTable2.withData(
          Array(new BufferedRows(Seq.empty, schema).withRow(InternalRow(4, 400))))

        // cache is re-pinned, external write invisible
        assertCached(serverSession.table(T))
        assertRows(
          connectSession.sql(s"SELECT * FROM $T").collect(),
          Seq(Row(1, 100), Row(2, 200), Row(3, 300)))
      }
    }
  }

  // Scenario 3: external schema change after CACHE TABLE.
  // Cache stays pinned at original 2-column schema.
  test("[S3] cached table pinned against external schema change") {
    withSession { connectSession =>
      withTable(connectSession, T) {
        connectSession.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        connectSession.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
        connectSession.sql(s"CACHE TABLE $T").collect()
        val serverSession = getServerSession(connectSession)
        assertCached(serverSession.table(T))
        assertRows(connectSession.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100)))

        // external schema change via catalog API
        val cat = serverCatalog[InMemoryTableCatalog](serverSession, "testcat")
        val addCol = TableChange.addColumn(Array("new_column"), IntegerType, true)
        cat.alterTable(ident, addCol)

        // external writer adds (2, 200, -1)
        val schema3 = StructType.fromDDL("id INT, salary INT, new_column INT")
        val extTable = cat
          .loadTable(ident, java.util.Set.of(TableWritePrivilege.INSERT))
          .asInstanceOf[InMemoryBaseTable]
        extTable.withData(
          Array(new BufferedRows(Seq.empty, schema3).withRow(InternalRow(2, 200, -1))))

        // cache stays pinned at original 2-column schema
        assertCached(serverSession.table(T))
        assertRows(connectSession.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100)))

        // REFRESH TABLE picks up external schema change and data
        connectSession.sql(s"REFRESH TABLE $T").collect()
        assertCached(serverSession.table(T))
        assertRows(
          connectSession.sql(s"SELECT * FROM $T").collect(),
          Seq(Row(1, 100, null), Row(2, 200, -1)))
      }
    }
  }

  // Scenario 4: session schema change invalidates cache; subsequent external
  // write is invisible.
  test("[S4] session schema change invalidates cache, external write invisible") {
    withSession { connectSession =>
      withTable(connectSession, T) {
        connectSession.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        connectSession.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
        connectSession.sql(s"CACHE TABLE $T").collect()
        val serverSession = getServerSession(connectSession)
        assertCached(serverSession.table(T))
        assertRows(connectSession.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100)))

        // session schema change via Connect: invalidates cache, rebuilds with new schema
        connectSession.sql(s"ALTER TABLE $T ADD COLUMN new_column INT").collect()
        assertCached(serverSession.table(T))
        assertRows(connectSession.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100, null)))

        // external writer adds (2, 200, -1) via catalog API
        val cat = serverCatalog[InMemoryTableCatalog](serverSession, "testcat")
        val schema3 = StructType.fromDDL("id INT, salary INT, new_column INT")
        val extTable = cat
          .loadTable(ident, java.util.Set.of(TableWritePrivilege.INSERT))
          .asInstanceOf[InMemoryBaseTable]
        extTable.withData(
          Array(new BufferedRows(Seq.empty, schema3).withRow(InternalRow(2, 200, -1))))

        // external write invisible: cache still shows (1, 100, null)
        assertCached(serverSession.table(T))
        assertRows(connectSession.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100, null)))

        // REFRESH TABLE picks up external write
        connectSession.sql(s"REFRESH TABLE $T").collect()
        assertCached(serverSession.table(T))
        assertRows(
          connectSession.sql(s"SELECT * FROM $T").collect(),
          Seq(Row(1, 100, null), Row(2, 200, -1)))
      }
    }
  }

  // Scenario 5: external drop and recreate with same schema.
  test("[S5] cached table after external drop and recreate sees empty table") {
    withSession { connectSession =>
      withTable(connectSession, T) {
        connectSession.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        connectSession.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
        connectSession.sql(s"CACHE TABLE $T").collect()
        val serverSession = getServerSession(connectSession)
        assertCached(serverSession.table(T))
        assertRows(connectSession.sql(s"SELECT * FROM $T").collect(), Seq(Row(1, 100)))

        // external drop and recreate via catalog API
        val cat = serverCatalog[InMemoryTableCatalog](serverSession, "testcat")
        cat.dropTable(ident)
        cat.createTable(
          ident,
          Array(Column.create("id", IntegerType), Column.create("salary", IntegerType)),
          Array.empty,
          Collections.emptyMap[String, String])

        // query sees the new empty table with preserved schema
        val result = connectSession.sql(s"SELECT * FROM $T")
        assertRows(result.collect(), Seq.empty)
        assert(result.schema.fieldNames.toSeq == Seq("id", "salary"))
      }
    }
  }
}
