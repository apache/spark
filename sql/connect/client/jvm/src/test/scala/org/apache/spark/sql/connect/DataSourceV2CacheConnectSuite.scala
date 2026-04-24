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
 * DSv2 CACHE TABLE tests for Spark Connect covering five cross-session
 * cache scenarios (S1 through S5).
 *
 * Uses [[SharedInMemoryTableCatalog]] (configured as "sharedcat") so that
 * both sessions share the same underlying table data via a static map.
 *
 * Session 1 (Connect client) caches and reads. Session 2 (another Connect
 * client) acts as an external writer. Both clients connect to the same
 * server and share its [[CacheManager]], so session 2's writes trigger
 * refreshCache() on the shared CacheManager.
 *
 * Write operations ignore the cache: external writes should NOT
 * invalidate session 1's cache. The expected values in these tests
 * reflect the current behavior where external writes ARE visible
 * through the shared CacheManager.
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

  private def withExtSession(f: SparkSession => Unit): Unit = {
    val extSession = SparkConnectServerUtils.createSparkSession()
    try {
      f(extSession)
    } finally {
      extSession.stop()
    }
  }

  // Scenario 1: external data write after CACHE TABLE
  //
  // Session 1 caches, then session 2 inserts a row.
  test("[S1] external data write after CACHE TABLE") {
    assumeCanRun()
    withTable(T) {
      withExtSession { extSession =>
        setupTable()
        spark.sql(s"CACHE TABLE $T")
        assert(spark.catalog.isCached(T))
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

        extSession.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

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
  test("[S2] session write then external write after CACHE TABLE") {
    assumeCanRun()
    withTable(T) {
      withExtSession { extSession =>
        setupTable()
        spark.sql(s"CACHE TABLE $T")
        assert(spark.catalog.isCached(T))
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

        spark.sql(s"INSERT INTO $T VALUES (2, 200)")
        assert(spark.catalog.isCached(T))

        extSession.sql(s"INSERT INTO $T VALUES (3, 300)").collect()

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
  test("[S3] external schema change after CACHE TABLE") {
    assumeCanRun()
    withTable(T) {
      withExtSession { extSession =>
        setupTable()
        spark.sql(s"CACHE TABLE $T")
        assert(spark.catalog.isCached(T))
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

        extSession.sql(s"ALTER TABLE $T ADD COLUMN new_col INT").collect()
        extSession.sql(s"INSERT INTO $T VALUES (2, 200, -1)").collect()

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
  test("[S4] session schema change then external write") {
    assumeCanRun()
    withTable(T) {
      withExtSession { extSession =>
        setupTable()
        spark.sql(s"CACHE TABLE $T")
        assert(spark.catalog.isCached(T))
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

        spark.sql(s"ALTER TABLE $T ADD COLUMN new_col INT")

        extSession.sql(s"INSERT INTO $T VALUES (2, 200, -1)").collect()

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
  test("[S5] external drop and recreate table after CACHE TABLE") {
    assumeCanRun()
    withTable(T) {
      withExtSession { extSession =>
        setupTable()
        spark.sql(s"CACHE TABLE $T")
        assert(spark.catalog.isCached(T))
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

        extSession.sql(s"DROP TABLE $T").collect()
        extSession.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()

        assert(!spark.catalog.isCached(T))
        checkAnswer(spark.sql(s"SELECT * FROM $T"), Seq.empty)
      }
    }
  }
}
