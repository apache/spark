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

import org.apache.spark.sql.Row
import org.apache.spark.sql.connect.test.{QueryTest, RemoteSparkSession, SQLHelper}
import org.apache.spark.sql.connect.test.IntegrationTestUtils.isAssemblyJarsDirExists
import org.apache.spark.sql.connect.test.SparkConnectServerUtils

/**
 * DSv2 CACHE TABLE tests for Spark Connect covering the five design doc
 * scenarios from SPARK-54022 section [5] "CACHE TABLE impact on reads".
 *
 * Uses [[SharedInMemoryTableCatalog]] (configured as "sharedcat") so that
 * both sessions share the same underlying table data via a static map.
 *
 * Session 1 (Connect client) caches and reads. Session 2 (another Connect
 * client) acts as an external writer. Both clients connect to the same
 * server and share its [[CacheManager]], so session 2's writes trigger
 * refreshCache() on the shared CacheManager.
 *
 * The proposed behavior for DSv2 is cache pinning: external writes should
 * NOT invalidate session 1's cache. The expected values in these tests
 * reflect the current behavior where external writes ARE visible through
 * the shared CacheManager. When per-session caching is implemented, the
 * expected values for S1-S4 should be updated to match the proposed
 * pinning behavior documented in each test's comments.
 */
class DataSourceV2CacheConnectSuite extends QueryTest with RemoteSparkSession with SQLHelper {

  private val T = "sharedcat.ns.tbl"

  private def assumeCanRun(): Unit = {
    assume(spark != null && isAssemblyJarsDirExists, "Spark Connect server not available")
  }

  private def setupTable(): Unit = {
    spark.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
    spark.sql(s"INSERT INTO $T VALUES (1, 100)")
  }

  private def withSession2(f: SparkSession => Unit): Unit = {
    val session2 = SparkConnectServerUtils.createSparkSession()
    try {
      f(session2)
    } finally {
      session2.stop()
    }
  }

  // Scenario 1: external data write after CACHE TABLE
  //
  // Session 1 caches, then session 2 inserts a row.
  //
  // Current behavior (shared CacheManager): session 2's INSERT triggers
  // refreshCache(), so session 1 sees the new data.
  //   Result: (1, 100), (2, 200)
  //
  // Proposed behavior (per-session caching): external write does not
  // invalidate session 1's cache.
  //   Result: (1, 100)
  test("[S1] external data write after CACHE TABLE") {
    assumeCanRun()
    withTable(T) {
      withSession2 { session2 =>
        setupTable()
        spark.sql(s"CACHE TABLE $T")
        assert(spark.catalog.isCached(T))
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

        session2.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

        assert(spark.catalog.isCached(T))
        checkAnswer(
          spark.sql(s"SELECT * FROM $T"),
          Seq(Row(1, 100), Row(2, 200)))

        spark.sql(s"UNCACHE TABLE IF EXISTS $T")
      }
    }
  }

  // Scenario 2: session write then external write after CACHE TABLE
  //
  // Session 1 caches, session 1 inserts (invalidates and recaches),
  // then session 2 inserts.
  //
  // Current behavior: all three rows visible via shared CacheManager.
  //   Result: (1, 100), (2, 200), (3, 300)
  //
  // Proposed behavior: session 1's write recaches, but session 2's
  // external write does not invalidate session 1's cache.
  //   Result: (1, 100), (2, 200)
  test("[S2] session write then external write after CACHE TABLE") {
    assumeCanRun()
    withTable(T) {
      withSession2 { session2 =>
        setupTable()
        spark.sql(s"CACHE TABLE $T")
        assert(spark.catalog.isCached(T))
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

        spark.sql(s"INSERT INTO $T VALUES (2, 200)")
        assert(spark.catalog.isCached(T))

        session2.sql(s"INSERT INTO $T VALUES (3, 300)").collect()

        assert(spark.catalog.isCached(T))
        checkAnswer(
          spark.sql(s"SELECT * FROM $T"),
          Seq(Row(1, 100), Row(2, 200), Row(3, 300)))

        spark.sql(s"UNCACHE TABLE IF EXISTS $T")
      }
    }
  }

  // Scenario 3: external schema change after CACHE TABLE
  //
  // Session 2 adds a column and inserts data with the new schema.
  //
  // Current behavior: schema change + write visible via shared
  // CacheManager.
  //   Result: (1, 100, null), (2, 200, -1)
  //
  // Proposed behavior: external schema change does not invalidate
  // session 1's cache. Cache remains pinned to the original schema.
  //   Result: (1, 100)
  test("[S3] external schema change after CACHE TABLE") {
    assumeCanRun()
    withTable(T) {
      withSession2 { session2 =>
        setupTable()
        spark.sql(s"CACHE TABLE $T")
        assert(spark.catalog.isCached(T))
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

        session2.sql(s"ALTER TABLE $T ADD COLUMN new_col INT").collect()
        session2.sql(s"INSERT INTO $T VALUES (2, 200, -1)").collect()

        checkAnswer(
          spark.sql(s"SELECT * FROM $T"),
          Seq(Row(1, 100, null), Row(2, 200, -1)))

        spark.sql(s"UNCACHE TABLE IF EXISTS $T")
      }
    }
  }

  // Scenario 4: session schema change then external write
  //
  // Session 1 adds a column (invalidates cache), then session 2 inserts.
  //
  // Current behavior: both changes visible via shared CacheManager.
  //   Result: (1, 100, null), (2, 200, -1)
  //
  // Proposed behavior: session 1's schema change recaches with new
  // schema, but session 2's external write does not invalidate cache.
  //   Result: (1, 100, null)
  test("[S4] session schema change then external write") {
    assumeCanRun()
    withTable(T) {
      withSession2 { session2 =>
        setupTable()
        spark.sql(s"CACHE TABLE $T")
        assert(spark.catalog.isCached(T))
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

        spark.sql(s"ALTER TABLE $T ADD COLUMN new_col INT")

        session2.sql(s"INSERT INTO $T VALUES (2, 200, -1)").collect()

        checkAnswer(
          spark.sql(s"SELECT * FROM $T"),
          Seq(Row(1, 100, null), Row(2, 200, -1)))

        spark.sql(s"UNCACHE TABLE IF EXISTS $T")
      }
    }
  }

  // Scenario 5: external drop and recreate table
  //
  // Session 2 drops and recreates the table with the same schema.
  //
  // Current behavior: cache invalidated by drop, recreated table is
  // empty.
  //   Result: empty
  //
  // Proposed behavior: same as current (keep as is).
  //   Result: empty
  test("[S5] external drop and recreate table after CACHE TABLE") {
    assumeCanRun()
    withTable(T) {
      withSession2 { session2 =>
        setupTable()
        spark.sql(s"CACHE TABLE $T")
        assert(spark.catalog.isCached(T))
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

        session2.sql(s"DROP TABLE $T").collect()
        session2.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()

        assert(!spark.catalog.isCached(T))
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq.empty)
      }
    }
  }
}
