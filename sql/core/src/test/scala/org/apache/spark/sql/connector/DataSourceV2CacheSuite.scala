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

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.connector.catalog.{BasicInMemoryTableCatalog, CatalogV2Implicits, Identifier, InMemoryTableCatalog, SharedInMemoryTableCatalog, TableWritePrivilege, TruncatableTable}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * DSv2 CACHE TABLE tests using an external session to simulate cross-session
 * writes. Follows the SPARK-54022 pattern: cache a table, modify it externally
 * (via catalog API or a second session), and verify cache pinning behavior.
 *
 * Uses [[SharedInMemoryTableCatalog]] so that both the primary and external
 * sessions share the same underlying table data via a static map.
 *
 * External modifications via catalog API bypass the CacheManager, so cached
 * reads return stale (pinned) data. External modifications via the second
 * session's SQL go through the shared CacheManager, so cached reads see fresh
 * data.
 */
class DataSourceV2CacheSuite extends QueryTest with SharedSparkSession {

  import CatalogV2Implicits._

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.sharedcat",
      classOf[SharedInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.sharedcat.copyOnLoad", "true")
    .set("spark.sql.catalog.testcat",
      classOf[InMemoryTableCatalog].getName)
    .set("spark.sql.catalog.testcat.copyOnLoad", "true")

  private val T = "sharedcat.ns.tbl"
  private val ident = Identifier.of(Array("ns"), "tbl")

  override def afterEach(): Unit = {
    try {
      SharedInMemoryTableCatalog.reset()
    } finally {
      super.afterEach()
    }
  }

  private def catalog(name: String): BasicInMemoryTableCatalog = {
    spark.sessionState.catalogManager.catalog(name)
      .asInstanceOf[BasicInMemoryTableCatalog]
  }

  /**
   * Creates a second [[SparkSession]] sharing the same [[SparkContext]],
   * [[SharedState]], and [[CacheManager]]. Uses [[SparkSession.newSession]]
   * so both sessions share the same CacheManager. Writes from the external
   * session trigger refreshCache() on the shared CacheManager, making
   * changes visible to cached reads in the primary session.
   *
   * The external session accesses the same tables via
   * [[SharedInMemoryTableCatalog]] (configured in sparkConf).
   */
  private def withExtSession(f: SparkSession => Unit): Unit = {
    val extSession = spark.newSession()
    f(extSession)
  }

  private def setupTable(): Unit = {
    sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
    sql(s"INSERT INTO $T VALUES (1, 100)")
  }

  // =========================================================================
  // Catalog API external modifications (bypass CacheManager)
  // These follow the SPARK-54022 pattern: modify the table directly via
  // the catalog, which does NOT trigger refreshCache(). Cached reads
  // should return the pinned (stale) data.
  // =========================================================================

  test("[catalog-api] external truncate after CACHE TABLE pins state") {
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")

      assertCached(spark.table(T))
      checkAnswer(spark.table(T), Seq(Row(1, 100)))

      // Externally truncate via catalog API (bypasses CacheManager)
      val tableCatalog = catalog("sharedcat").asTableCatalog
      val table = tableCatalog
        .loadTable(ident, util.Set.of(TableWritePrivilege.DELETE))
      table.asInstanceOf[TruncatableTable].truncateTable()

      // Cached reads still return pinned data
      assertCached(spark.table(T))
      checkAnswer(spark.table(T), Seq(Row(1, 100)))

      // Session write invalidates and recaches
      sql(s"INSERT INTO $T VALUES (10, 1000)")
      assertCached(spark.table(T))
      checkAnswer(spark.table(T), Seq(Row(10, 1000)))
    }
  }

  test("[catalog-api] external drop+recreate after CACHE TABLE") {
    withTable(T) {
      setupTable()
      sql(s"CACHE TABLE $T")
      assertCached(spark.table(T))
      checkAnswer(spark.table(T), Seq(Row(1, 100)))

      // Save schema before drop
      val tableCatalog = catalog("sharedcat").asTableCatalog
      val columns = tableCatalog.loadTable(ident).columns()

      // Externally drop and recreate via catalog API
      tableCatalog.dropTable(ident)
      tableCatalog.createTable(
        ident, columns, Array.empty, new java.util.HashMap[String, String]())

      // Cache was invalidated by the drop
      assert(!spark.catalog.isCached(T))
    }
  }

  // =========================================================================
  // External session SQL modifications (shared CacheManager)
  // The second session shares the same CacheManager. SQL writes trigger
  // refreshCache(), making external changes visible.
  // =========================================================================

  test("[ext-session] external data write after CACHE TABLE") {
    withTable(T) {
      withExtSession { ext =>
        setupTable()
        sql(s"CACHE TABLE $T")
        assertCached(spark.table(T))
        checkAnswer(spark.table(T), Seq(Row(1, 100)))

        // External session writes via SQL (triggers refreshCache)
        ext.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

        // Shared CacheManager: external write triggers recache
        assertCached(spark.table(T))
        checkAnswer(
          spark.sql(s"SELECT * FROM $T"),
          Seq(Row(1, 100), Row(2, 200)))

        sql(s"UNCACHE TABLE IF EXISTS $T")
      }
    }
  }

  test("[ext-session] session write then external write after CACHE TABLE") {
    withTable(T) {
      withExtSession { ext =>
        setupTable()
        sql(s"CACHE TABLE $T")
        assertCached(spark.table(T))
        checkAnswer(spark.table(T), Seq(Row(1, 100)))

        // Session 1 writes (invalidates and recaches)
        sql(s"INSERT INTO $T VALUES (2, 200)")
        assertCached(spark.table(T))

        // External session writes
        ext.sql(s"INSERT INTO $T VALUES (3, 300)").collect()

        // All writes visible
        assertCached(spark.table(T))
        checkAnswer(
          spark.sql(s"SELECT * FROM $T"),
          Seq(Row(1, 100), Row(2, 200), Row(3, 300)))

        sql(s"UNCACHE TABLE IF EXISTS $T")
      }
    }
  }

  test("[ext-session] external schema change after CACHE TABLE") {
    withTable(T) {
      withExtSession { ext =>
        setupTable()
        sql(s"CACHE TABLE $T")
        assertCached(spark.table(T))
        checkAnswer(spark.table(T), Seq(Row(1, 100)))

        // External session adds column and inserts
        ext.sql(s"ALTER TABLE $T ADD COLUMN new_col INT").collect()
        ext.sql(s"INSERT INTO $T VALUES (2, 200, -1)").collect()

        // Schema change visible via shared CacheManager
        checkAnswer(
          spark.sql(s"SELECT * FROM $T"),
          Seq(Row(1, 100, null), Row(2, 200, -1)))

        sql(s"UNCACHE TABLE IF EXISTS $T")
      }
    }
  }

  test("[ext-session] external drop and recreate after CACHE TABLE") {
    withTable(T) {
      withExtSession { ext =>
        setupTable()
        sql(s"CACHE TABLE $T")
        assertCached(spark.table(T))
        checkAnswer(spark.table(T), Seq(Row(1, 100)))

        // External session drops and recreates
        ext.sql(s"DROP TABLE $T").collect()
        ext.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()

        // Cache invalidated by drop; recreated table is empty
        assert(!spark.catalog.isCached(T))
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq.empty)
      }
    }
  }

  test("[ext-session] REFRESH TABLE picks up external write") {
    withTable(T) {
      withExtSession { ext =>
        setupTable()
        sql(s"CACHE TABLE $T")
        assertCached(spark.table(T))

        ext.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

        sql(s"REFRESH TABLE $T")
        assertCached(spark.table(T))
        checkAnswer(
          spark.sql(s"SELECT * FROM $T"),
          Seq(Row(1, 100), Row(2, 200)))

        sql(s"UNCACHE TABLE IF EXISTS $T")
      }
    }
  }

  test("[ext-session] UNCACHE TABLE then fresh read sees external write") {
    withTable(T) {
      withExtSession { ext =>
        setupTable()
        sql(s"CACHE TABLE $T")
        assertCached(spark.table(T))

        ext.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

        sql(s"UNCACHE TABLE IF EXISTS $T")
        assert(!spark.catalog.isCached(T))
        checkAnswer(
          spark.sql(s"SELECT * FROM $T"),
          Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }
}
