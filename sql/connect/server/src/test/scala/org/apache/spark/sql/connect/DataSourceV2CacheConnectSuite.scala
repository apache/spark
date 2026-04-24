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

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{BufferedRows, Column, Identifier, InMemoryBaseTable, InMemoryTableCatalog, TableChange}
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
 * DSv2 CACHE TABLE tests for Spark Connect covering five cross-session
 * cache scenarios (S1 through S5).
 *
 * Uses an in-process Connect server ([[SparkConnectServerTest]]) so that
 * the test can access the server's catalog directly. A Connect client
 * performs cache and SQL operations; external writes go through the
 * catalog API ([[InMemoryBaseTable.withData]]), which bypasses the
 * [[CacheManager]]. This simulates a truly external writer whose
 * changes are invisible to cached reads.
 */
class DataSourceV2CacheConnectSuite extends SparkConnectServerTest {

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    .set("spark.sql.catalog.testcat.copyOnLoad", "true")

  private val T = "testcat.ns1.ns2.tbl"
  private val ident = Identifier.of(Array("ns1", "ns2"), "tbl")

  private def catalogTestcat: InMemoryTableCatalog =
    spark.sessionState.catalogManager.catalog("testcat").asInstanceOf[InMemoryTableCatalog]

  private def setupTable(): Unit = {
    spark.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
    spark.sql(s"INSERT INTO $T VALUES (1, 100)")
  }

  // Scenario 1: external write after CACHE TABLE is invisible (cache pinned).
  // Scenario 2: session write invalidates cache; subsequent external write
  // is again invisible.
  test("[S1+S2] CACHE TABLE pins state; session write invalidates, external does not") {
    withSession { connectSession =>
      withTable(T) {
        // create table and cache via Connect
        connectSession.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        connectSession.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
        connectSession.sql(s"CACHE TABLE $T").collect()
        assertCached(spark.table(T))
        checkAnswer(spark.table(T), Seq(Row(1, 100)))

        // S1: external writer adds (2, 200) via direct catalog API
        // (bypasses this session's CacheManager)
        val schema = StructType.fromDDL("id INT, salary INT")
        val extTable = catalogTestcat.loadTable(ident).asInstanceOf[InMemoryBaseTable]
        extTable.withData(Array(
          new BufferedRows(Seq.empty, schema).withRow(InternalRow(2, 200))))

        // cache is pinned, external write invisible
        assertCached(spark.table(T))
        checkAnswer(spark.table(T), Seq(Row(1, 100)))

        // S2: session write via Connect invalidates the cache entry
        connectSession.sql(s"INSERT INTO $T VALUES (2, 200)").collect()
        assertCached(spark.table(T))
        checkAnswer(spark.table(T), Seq(Row(1, 100), Row(2, 200)))

        // external writer adds (3, 300) via direct catalog API
        val extTable2 = catalogTestcat.loadTable(ident).asInstanceOf[InMemoryBaseTable]
        extTable2.withData(Array(
          new BufferedRows(Seq.empty, schema).withRow(InternalRow(3, 300))))

        // cache is re-pinned, external write invisible
        assertCached(spark.table(T))
        checkAnswer(spark.table(T), Seq(Row(1, 100), Row(2, 200)))

        // REFRESH TABLE picks up all external changes
        connectSession.sql(s"REFRESH TABLE $T").collect()
        assertCached(spark.table(T))
        checkAnswer(spark.table(T), Seq(Row(1, 100), Row(2, 200), Row(3, 300)))
      }
    }
  }

  // Scenario 3: external schema change after CACHE TABLE.
  // Cache stays pinned at original 2-column schema.
  test("[S3] cached table pinned against external schema change") {
    withSession { connectSession =>
      withTable(T) {
        connectSession.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        connectSession.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
        connectSession.sql(s"CACHE TABLE $T").collect()
        assertCached(spark.table(T))
        checkAnswer(spark.table(T), Seq(Row(1, 100)))

        // external schema change via catalog API
        val addCol = TableChange.addColumn(Array("new_column"), IntegerType, true)
        catalogTestcat.alterTable(ident, addCol)

        // external writer adds (2, 200, -1)
        val schema3 = StructType.fromDDL("id INT, salary INT, new_column INT")
        val extTable = catalogTestcat.loadTable(ident).asInstanceOf[InMemoryBaseTable]
        extTable.withData(Array(
          new BufferedRows(Seq.empty, schema3).withRow(InternalRow(2, 200, -1))))

        // cache stays pinned at original 2-column schema
        assertCached(spark.table(T))
        checkAnswer(spark.table(T), Seq(Row(1, 100)))

        // REFRESH TABLE picks up external schema change and data
        connectSession.sql(s"REFRESH TABLE $T").collect()
        assertCached(spark.table(T))
        checkAnswer(spark.table(T), Seq(Row(1, 100, null), Row(2, 200, -1)))
      }
    }
  }

  // Scenario 4: session schema change invalidates cache; subsequent external
  // write is invisible.
  test("[S4] session schema change invalidates cache, external write invisible") {
    withSession { connectSession =>
      withTable(T) {
        connectSession.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        connectSession.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
        connectSession.sql(s"CACHE TABLE $T").collect()
        assertCached(spark.table(T))
        checkAnswer(spark.table(T), Seq(Row(1, 100)))

        // session schema change via Connect: invalidates cache, rebuilds with new schema
        connectSession.sql(s"ALTER TABLE $T ADD COLUMN new_column INT").collect()
        assertCached(spark.table(T))
        checkAnswer(spark.table(T), Seq(Row(1, 100, null)))

        // external writer adds (2, 200, -1) via catalog API
        val schema3 = StructType.fromDDL("id INT, salary INT, new_column INT")
        val extTable = catalogTestcat.loadTable(ident).asInstanceOf[InMemoryBaseTable]
        extTable.withData(Array(
          new BufferedRows(Seq.empty, schema3).withRow(InternalRow(2, 200, -1))))

        // external write invisible: cache still shows (1, 100, null)
        assertCached(spark.table(T))
        checkAnswer(spark.table(T), Seq(Row(1, 100, null)))

        // REFRESH TABLE picks up external write
        connectSession.sql(s"REFRESH TABLE $T").collect()
        assertCached(spark.table(T))
        checkAnswer(spark.table(T), Seq(Row(1, 100, null), Row(2, 200, -1)))
      }
    }
  }

  // Scenario 5: external drop and recreate with same schema.
  test("[S5] cached table after external drop and recreate sees empty table") {
    withSession { connectSession =>
      withTable(T) {
        connectSession.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
        connectSession.sql(s"INSERT INTO $T VALUES (1, 100)").collect()
        connectSession.sql(s"CACHE TABLE $T").collect()
        assertCached(spark.table(T))
        checkAnswer(spark.table(T), Seq(Row(1, 100)))

        // external drop and recreate via catalog API
        catalogTestcat.dropTable(ident)
        catalogTestcat.createTable(
          ident,
          Array(
            Column.create("id", IntegerType),
            Column.create("salary", IntegerType)),
          Array.empty,
          Collections.emptyMap[String, String])

        // query sees the new empty table
        checkAnswer(spark.table(T), Seq.empty)
      }
    }
  }
}
