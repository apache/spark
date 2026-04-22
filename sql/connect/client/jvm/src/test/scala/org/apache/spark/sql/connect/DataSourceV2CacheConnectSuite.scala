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
 * DSv2 CACHE TABLE tests for Spark Connect using two sessions.
 *
 * Uses [[SharedInMemoryTableCatalog]] (configured as "sharedcat") so that both sessions share the
 * same underlying table data via a static ConcurrentHashMap. Session 1 caches and reads; session
 * 2 acts as an external writer.
 *
 * All sessions on the same server share one CacheManager (via SharedState). DSv2 writes trigger
 * refreshCache() which recaches on the shared CacheManager. These tests document the observable
 * behavior for each design doc CACHE scenario.
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

  // Scenario 1: external write after CACHE TABLE
  //
  // Session 1 caches the table, then session 2 inserts a row.
  // Because both sessions share the same CacheManager, session 2's
  // INSERT triggers refreshCache() which recaches the table. Session 1
  // sees the new data.
  test("[S1] external data write after CACHE TABLE") {
    assumeCanRun()
    withTable(T) {
      withSession2 { session2 =>
        setupTable()
        spark.sql(s"CACHE TABLE $T")
        assert(spark.catalog.isCached(T))
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

        // Session 2 writes to the same underlying table
        session2.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

        // Shared CacheManager: session 2's write triggers refreshCache(),
        // so session 1 sees the new data
        assert(spark.catalog.isCached(T))
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100), Row(2, 200)))

        spark.sql(s"UNCACHE TABLE IF EXISTS $T")
      }
    }
  }

  // Scenario 2: session write then external write after CACHE TABLE
  //
  // Session 1 caches, then session 1 inserts (invalidates cache),
  // then session 2 inserts. Both writes are visible due to shared
  // CacheManager.
  test("[S2] session write then external write after CACHE TABLE") {
    assumeCanRun()
    withTable(T) {
      withSession2 { session2 =>
        setupTable()
        spark.sql(s"CACHE TABLE $T")
        assert(spark.catalog.isCached(T))
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

        // Session 1 writes (invalidates and recaches)
        spark.sql(s"INSERT INTO $T VALUES (2, 200)")
        assert(spark.catalog.isCached(T))

        // Session 2 writes externally
        session2.sql(s"INSERT INTO $T VALUES (3, 300)").collect()

        // Both writes visible due to shared CacheManager
        assert(spark.catalog.isCached(T))
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100), Row(2, 200), Row(3, 300)))

        spark.sql(s"UNCACHE TABLE IF EXISTS $T")
      }
    }
  }

  // Scenario 3: external schema change (add column + insert)
  //
  // Session 2 adds a column and inserts data with the new schema.
  test("[S3] external schema change after CACHE TABLE") {
    assumeCanRun()
    withTable(T) {
      withSession2 { session2 =>
        setupTable()
        spark.sql(s"CACHE TABLE $T")
        assert(spark.catalog.isCached(T))
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

        // Session 2 adds a column and inserts
        session2.sql(s"ALTER TABLE $T ADD COLUMN new_col INT").collect()
        session2.sql(s"INSERT INTO $T VALUES (2, 200, -1)").collect()

        // Schema change + write visible via shared CacheManager
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100, null), Row(2, 200, -1)))

        spark.sql(s"UNCACHE TABLE IF EXISTS $T")
      }
    }
  }

  // Scenario 4: session schema change then external write
  //
  // Session 1 adds a column (invalidates cache), then session 2 inserts.
  test("[S4] session schema change then external write") {
    assumeCanRun()
    withTable(T) {
      withSession2 { session2 =>
        setupTable()
        spark.sql(s"CACHE TABLE $T")
        assert(spark.catalog.isCached(T))
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

        // Session 1 evolves schema (invalidates and recaches)
        spark.sql(s"ALTER TABLE $T ADD COLUMN new_col INT")

        // Session 2 inserts with new schema
        session2.sql(s"INSERT INTO $T VALUES (2, 200, -1)").collect()

        // Both changes visible
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100, null), Row(2, 200, -1)))

        spark.sql(s"UNCACHE TABLE IF EXISTS $T")
      }
    }
  }

  // Scenario 5: external drop and recreate table
  //
  // Session 2 drops and recreates the table with the same schema.
  test("[S5] external drop and recreate table after CACHE TABLE") {
    assumeCanRun()
    withTable(T) {
      withSession2 { session2 =>
        setupTable()
        spark.sql(s"CACHE TABLE $T")
        assert(spark.catalog.isCached(T))
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

        // Session 2 drops and recreates with same schema
        session2.sql(s"DROP TABLE $T").collect()
        session2.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()

        // Recreated table is empty; cache was invalidated by drop
        assert(!spark.catalog.isCached(T))
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq.empty)

        spark.sql(s"UNCACHE TABLE IF EXISTS $T")
      }
    }
  }

  // REFRESH TABLE after external write
  test("[S1] REFRESH TABLE picks up external write") {
    assumeCanRun()
    withTable(T) {
      withSession2 { session2 =>
        setupTable()
        spark.sql(s"CACHE TABLE $T")
        assert(spark.catalog.isCached(T))
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

        // Session 2 writes
        session2.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

        // Explicit REFRESH should pick up the external write
        spark.sql(s"REFRESH TABLE $T")
        assert(spark.catalog.isCached(T))
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100), Row(2, 200)))

        spark.sql(s"UNCACHE TABLE IF EXISTS $T")
      }
    }
  }

  // UNCACHE TABLE after external write
  test("[S1] UNCACHE TABLE then fresh read sees external write") {
    assumeCanRun()
    withTable(T) {
      withSession2 { session2 =>
        setupTable()
        spark.sql(s"CACHE TABLE $T")
        assert(spark.catalog.isCached(T))
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

        // Session 2 writes
        session2.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

        // Uncache and re-read
        spark.sql(s"UNCACHE TABLE IF EXISTS $T")
        assert(!spark.catalog.isCached(T))
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100), Row(2, 200)))
      }
    }
  }
}
