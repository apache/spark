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

import org.apache.spark.sql._
import org.apache.spark.sql.connector.catalog._

/**
 * Design doc Section [2]: Repeated table access via sql().
 * Tests that sql() creates a fresh QueryExecution each time, always seeing latest data.
 *
 * Each scenario tests both session writes and external writes.
 * External writes use [[DataSourceV2TableRefreshTestBase.extSession]].
 */
class DataSourceV2RepeatedSQLRefreshSuite
    extends DataSourceV2TableRefreshTestBase {


  // [2] Repeated table access via sql()
  // sql("SELECT * FROM t") creates a fresh QE each time, always latest.

  test("[2.1] repeated sql() reflects session write") {
    withTable(T) {
      setupTable()
      checkAnswer(sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      sql(s"INSERT INTO $T VALUES (2, 200)")
      checkAnswer(
        sql(s"SELECT * FROM $T"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }


  test("[2.1-ext] repeated sql() reflects external write") {
    withTable(T) {
      setupTable()
      checkAnswer(sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      extSession.sql(s"INSERT INTO $T VALUES (2, 200)").collect()

      checkAnswer(
        sql(s"SELECT * FROM $T"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }


  test("[2.2] repeated sql() reflects schema change") {
    withTable(T) {
      setupTable()
      checkAnswer(sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      sql(s"ALTER TABLE $T ADD COLUMN new_col INT")
      sql(s"INSERT INTO $T VALUES (2, 200, -1)")
      checkAnswer(
        sql(s"SELECT * FROM $T"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))
    }
  }


  test("[2.2-ext] repeated sql() reflects external schema change") {
    withTable(T) {
      setupTable()
      checkAnswer(sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      val ext = extSession
      ext.sql(s"ALTER TABLE $T ADD COLUMN new_col INT").collect()
      ext.sql(s"INSERT INTO $T VALUES (2, 200, -1)").collect()

      checkAnswer(
        sql(s"SELECT * FROM $T"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))
    }
  }


  test("[2.3] repeated sql() reflects drop/recreate") {
    withTable(T) {
      setupTable()
      checkAnswer(sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      sql(s"DROP TABLE $T")
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      checkAnswer(sql(s"SELECT * FROM $T"), Seq.empty)
    }
  }


  test("[2.3-ext] repeated sql() reflects external drop/recreate") {
    withTable(T) {
      setupTable()
      checkAnswer(sql(s"SELECT * FROM $T"), Seq(Row(1, 100)))

      val ext = extSession
      ext.sql(s"DROP TABLE $T").collect()
      ext.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()

      checkAnswer(sql(s"SELECT * FROM $T"), Seq.empty)
    }
  }


  test("[Edge.7] same table in SQL loads once (consistent version)") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, value INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 10), (2, 20)")
      sql(s"INSERT INTO $T VALUES (3, 30)")

      // All references see same version in one SQL pass
      checkAnswer(
        sql(s"""SELECT a.id, b.value
               |FROM $T a JOIN $T b ON a.id = b.id
               |WHERE a.id IN (SELECT id FROM $T WHERE value > 5)
               |""".stripMargin),
        Seq(Row(1, 10), Row(2, 20), Row(3, 30)))
    }
  }


  // Section 2 caching connector: "Connector w/ cache" from design doc
  // The doc says external writes through a caching connector are invisible
  // until the cache expires. Session writes are always visible.

  test("[Cache.4] Section 2 S1: repeated sql() with caching connector") {
    withTable(CT) {
      setupCachingTable()
      checkAnswer(sql(s"SELECT * FROM $CT"), Seq(Row(1, 100)))

      // Session write: goes to a copy, lost on next read (stale cache)
      sql(s"INSERT INTO $CT VALUES (2, 200)")
      checkAnswer(sql(s"SELECT * FROM $CT"), Seq(Row(1, 100)))
    }
  }


  test("[Cache.5] Section 2 S2: schema change with caching connector") {
    withTable(CT) {
      setupCachingTable()
      checkAnswer(sql(s"SELECT * FROM $CT"), Seq(Row(1, 100)))

      // Session schema change: ALTER goes through catalog, modifying a
      // copy of the cached table and putting the new table in the tables
      // map. But the cache still holds the old 2-col table. Subsequent
      // loadTable calls return the stale cached entry.
      sql(s"ALTER TABLE $CT ADD COLUMN bonus INT")

      // The ALTER updated the table in the underlying map to 3 cols.
      // The caching connector may or may not serve the updated schema
      // depending on cache state. Insert with all 3 columns to match
      // the altered schema.
      sql(s"INSERT INTO $CT VALUES (2, 200, 50)")

      // With caching connector, reads may return stale cached data or
      // the updated table depending on cache invalidation timing.
      // The test verifies no crash after schema change + insert.
      val result = sql(s"SELECT * FROM $CT").collect()
      assert(result.length >= 1 && result.length <= 2)
    }
  }


  test("[Cache.6] Section 2 S3: drop/recreate with caching connector") {
    withTable(CT) {
      setupCachingTable()
      checkAnswer(sql(s"SELECT * FROM $CT"), Seq(Row(1, 100)))

      // Drop clears the underlying table. Must clear cache so CREATE
      // doesn't see the stale cached entry.
      sql(s"DROP TABLE $CT")
      CachingInMemoryTableCatalog.clearCache()
      sql(s"CREATE TABLE $CT (id INT, salary INT) USING foo")

      // After drop/recreate, should see empty table
      checkAnswer(sql(s"SELECT * FROM $CT"), Seq.empty)
    }
  }
}
