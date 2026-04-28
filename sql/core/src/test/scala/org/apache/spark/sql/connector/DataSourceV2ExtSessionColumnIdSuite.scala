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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, QueryTest, Row, SparkSession}
import org.apache.spark.sql.connector.catalog.SharedInMemoryTableCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for cross-session column ID detection where an external
 * [[SparkSession]] modifies a DSv2 table while another session holds
 * a stale [[DataFrame]].
 *
 * Each [[SparkSession]] has its own [[CacheManager]], so session1 does
 * not know about session2's schema changes. When session1 executes a
 * stale [[DataFrame]], the refresh logic calls [[catalog.loadTable]] to
 * get the latest table metadata from the catalog and compares the
 * captured column IDs against the current ones.
 *
 * [[SharedInMemoryTableCatalog]] makes this work by storing tables in
 * a static [[ConcurrentHashMap]] that all catalog instances share,
 * regardless of which session created them.
 */
class DataSourceV2ExtSessionColumnIdSuite extends QueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.ANSI_ENABLED, true)
    .set("spark.sql.catalog.sharedcat",
      classOf[SharedInMemoryTableCatalog].getName)
    // copyOnLoad: each loadTable returns a fresh copy, simulating a real
    // catalog where metadata is reloaded from the metastore on each access
    .set("spark.sql.catalog.sharedcat.copyOnLoad", "true")

  override def afterEach(): Unit = {
    try {
      SharedInMemoryTableCatalog.reset()
      spark.sessionState.catalogManager.reset()
    } finally {
      super.afterEach()
    }
  }

  /**
   * Creates a second [[SparkSession]] with its own [[CacheManager]] but
   * sharing the same [[SparkContext]] (and therefore the same catalog
   * configs like `spark.sql.catalog.sharedcat`).
   *
   * We clear the active/default session first so that
   * [[SparkSession.builder().create()]] allocates a brand new
   * [[SharedState]] instead of reusing the existing one. The
   * `finally` block restores the original active/default session
   * so the test's main session is not disrupted.
   *
   * This is not a true external process (same JVM), but it is
   * sufficient: session1's [[CacheManager]] is unaware of session2's
   * writes, and [[catalog.loadTable]] reads from the shared static
   * map, returning the latest metadata.
   */
  private def withExtSession(f: SparkSession => Unit): Unit = {
    val savedActive = SparkSession.getActiveSession
    val savedDefault = SparkSession.getDefaultSession
    val extSession = try {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      SparkSession.builder()
        .sparkContext(spark.sparkContext)
        .create()
    } finally {
      savedDefault.foreach(s =>
        SparkSession.setDefaultSession(s))
      savedActive.foreach(s =>
        SparkSession.setActiveSession(s))
    }
    f(extSession)
  }

  private val T = "sharedcat.ns.tbl"

  test("external write visible via fresh query") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100)")

      checkAnswer(spark.table(T), Seq(Row(1, 100)))

      // external session writes data
      withExtSession { ext =>
        ext.sql(s"INSERT INTO $T VALUES (2, 200)").collect()
      }

      // a fresh query from session1 picks up external write
      checkAnswer(
        spark.table(T),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  // spark.table(T) captures column IDs at analysis time. When
  // an external session drops and re-adds a column, the column gets
  // a new ID. Session1's stale DataFrame detects the mismatch.
  test("external drop+re-add column detected by column ID") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100)")

      val df = spark.table(T)

      // external session drops and re-adds column
      withExtSession { ext =>
        ext.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()
        ext.sql(s"ALTER TABLE $T ADD COLUMN salary INT").collect()
      }

      // column ID changed, session1 detects it
      checkError(
        exception = intercept[AnalysisException] {
          df.collect()
        },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map(
          "tableName" -> ".*", "errors" -> ".*"))
    }
  }

  test("external drop+recreate table detected by column ID") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100)")
      val df = spark.table(T)

      // external session drops and recreates table
      withExtSession { ext =>
        ext.sql(s"DROP TABLE $T").collect()
        ext.sql(s"CREATE TABLE $T (id INT, salary INT) USING foo").collect()
      }

      // table ID is null (SharedInMemoryTableCatalog extends
      // NullTableIdInMemoryTableCatalog), so column ID check catches it
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> "(?s).*"))
    }
  }

  // Adding a column from an external session preserves existing column IDs.
  // The stale DataFrame should still work because its captured column IDs
  // match the current table's unchanged columns.
  test("external add column does not trigger column ID mismatch") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100)")

      val df = spark.table(T)

      // external session adds a new column
      withExtSession { ext =>
        ext.sql(s"ALTER TABLE $T ADD COLUMN bonus INT").collect()
        ext.sql(s"INSERT INTO $T VALUES (2, 200, 50)").collect()
      }

      // session1's stale DataFrame still works: id and salary IDs unchanged
      checkAnswer(df, Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("external drop+re-add multiple columns detected by column ID") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT, bonus INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100, 10)")

      val df = spark.table(T)

      // external session drops and re-adds both salary and bonus
      withExtSession { ext =>
        ext.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()
        ext.sql(s"ALTER TABLE $T DROP COLUMN bonus").collect()
        ext.sql(s"ALTER TABLE $T ADD COLUMN salary INT").collect()
        ext.sql(s"ALTER TABLE $T ADD COLUMN bonus INT").collect()
      }

      // both column ID mismatches are detected
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*",
          "errors" -> "(?s).*salary.*bonus.*"))
    }
  }

  test("external type widening detected by data columns validation") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100)")

      val df = spark.table(T)

      // external session widens salary from INT to LONG
      // SharedInMemoryTableCatalog preserves the column ID across type
      // changes, so data columns validation catches the type mismatch
      // instead of the column ID check
      withExtSession { ext =>
        ext.sql(s"ALTER TABLE $T ALTER COLUMN salary TYPE LONG").collect()
      }

      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }
}
