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
import org.apache.spark.sql._
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for DSv2 column ID validation during table refresh.
 *
 * Verifies that column IDs detect drop+re-add column scenarios across
 * joins and DataFrame operations, including with null table IDs.
 */
class DataSourceV2TablePinningRefreshSuite
    extends QueryTest with SharedSparkSession {

  // Error condition constants
  private val COL_ID_MISMATCH =
    "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH"

  // Allow checkError without exact parameters for parameterized tests
  override protected def checkErrorIgnorableParameters
    : Map[String, Set[String]] =
    super.checkErrorIgnorableParameters ++ Map(
      COL_ID_MISMATCH -> Set("tableName", "errors"))

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.ANSI_ENABLED, true)
    .set("spark.sql.catalog.sharedcat",
      classOf[SharedInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.sharedcat.copyOnLoad", "true")
    // Null ID: tables without identity tracking. Drop/recreate
    // is not detected because table ID is always null.
    .set("spark.sql.catalog.nullidcat",
      classOf[NullIdInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.nullidcat.copyOnLoad", "true")

  override def afterEach(): Unit = {
    SharedInMemoryTableCatalog.reset()
    try {
      spark.sessionState.catalogManager.reset()
    } finally {
      super.afterEach()
    }
  }

  private val T = "sharedcat.ns.tbl"

  private def setupTable(): Unit = {
    sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
    sql(s"INSERT INTO $T VALUES (1, 100)")
  }

  /**
   * Creates a SparkSession with a SEPARATE CacheManager (separate SharedState)
   * but the same SparkContext and catalog configs. SharedInMemoryTableCatalog
   * tables are shared via the companion object, so the external session sees
   * the same table data. This simulates a truly external writer (different JVM
   * in production) whose writes do NOT invalidate Session 1's CacheManager.
   */
  private def extSession: SparkSession = {
    val savedActive = SparkSession.getActiveSession
    val savedDefault = SparkSession.getDefaultSession
    try {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      SparkSession.builder()
        .sparkContext(spark.sparkContext)
        .create()
    } finally {
      savedDefault.foreach(s => SparkSession.setDefaultSession(s))
      savedActive.foreach(s => SparkSession.setActiveSession(s))
    }
  }

  // --- Section 3: Join tests with column ID validation ---

  test("[3.4-nullid] join after drop/recreate fails with COLUMN_ID_MISMATCH") {
    val NT = "nullidcat.ns.tbl"
    withTable(NT) {
      sql(s"CREATE TABLE $NT (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NT VALUES (1, 100)")

      val df1 = spark.table(NT)

      sql(s"DROP TABLE $NT")
      sql(s"CREATE TABLE $NT (id INT, salary INT) USING foo")

      val df2 = spark.table(NT)

      // Table ID is null so TABLE_ID_MISMATCH is skipped, but column IDs
      // are assigned by InMemoryBaseTable. The new table has different
      // column IDs, so COLUMN_ID_MISMATCH is detected.
      checkError(
        exception = intercept[AnalysisException] {
          df1.join(df2, df1("id") === df2("id")).collect()
        },
        condition = COL_ID_MISMATCH)
    }
  }

  // Null table ID but column IDs are still assigned by InMemoryBaseTable.
  // Drop+add column same type triggers COLUMN_ID_MISMATCH.
  test("[3.5-nullid] join after drop+add same type fails with COLUMN_ID_MISMATCH") {
    val NT = "nullidcat.ns.tbl"
    withTable(NT) {
      sql(s"CREATE TABLE $NT (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NT VALUES (1, 100)")
      val df1 = spark.table(NT)

      sql(s"ALTER TABLE $NT DROP COLUMN salary")
      sql(s"ALTER TABLE $NT ADD COLUMN salary INT")

      val df2 = spark.table(NT)

      // Column IDs detect that the re-added column is different.
      checkError(
        exception = intercept[AnalysisException] {
          df1.join(df2, df1("id") === df2("id")).collect()
        },
        condition = COL_ID_MISMATCH)
    }
  }

  // Null table ID but column IDs are still assigned by InMemoryBaseTable.
  // Column ID mismatch is detected before schema validation.
  test("[3.6-nullid] join after drop+add different type fails with COLUMN_ID_MISMATCH") {
    val NT = "nullidcat.ns.tbl"
    withTable(NT) {
      sql(s"CREATE TABLE $NT (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $NT VALUES (1, 100)")
      val df1 = spark.table(NT)

      sql(s"ALTER TABLE $NT DROP COLUMN salary")
      sql(s"ALTER TABLE $NT ADD COLUMN salary STRING")

      val df2 = spark.table(NT)

      // Column ID mismatch is detected before schema validation.
      checkError(
        exception = intercept[AnalysisException] {
          df1.join(df2, df1("id") === df2("id")).collect()
        },
        condition = COL_ID_MISMATCH)
    }
  }

  test("[3.5] join after drop/re-add column same type fails with COLUMN_ID_MISMATCH") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)

      sql(s"ALTER TABLE $T DROP COLUMN salary")
      sql(s"ALTER TABLE $T ADD COLUMN salary INT")

      val df2 = spark.table(T)

      // Column IDs detect that the re-added column is different from the original.
      checkError(
        exception = intercept[AnalysisException] {
          df1.join(df2, df1("id") === df2("id")).collect()
        },
        condition = COL_ID_MISMATCH)
    }
  }

  test("[3.6] join after drop/re-add column different type fails with COLUMN_ID_MISMATCH") {
    withTable(T) {
      setupTable()
      val df1 = spark.table(T)

      sql(s"ALTER TABLE $T DROP COLUMN salary")
      sql(s"ALTER TABLE $T ADD COLUMN salary STRING")

      val df2 = spark.table(T)

      // Column ID mismatch is detected before schema validation.
      checkError(
        exception = intercept[AnalysisException] {
          df1.join(df2, df1("id") === df2("id")).collect()
        },
        condition = COL_ID_MISMATCH)
    }
  }

  // --- Section 4: DataFrame version pinning with column ID ---

  // Section 4 Scenario 5: drop+add column with same name and type.
  // With column IDs, this now fails because the column was replaced.
  test("[4.S5-show] DataFrame fails after session drop+add column same type") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      assert(df.count() === 1)

      sql(s"ALTER TABLE $T DROP COLUMN salary")
      sql(s"ALTER TABLE $T ADD COLUMN salary INT")

      // Column IDs detect that the re-added column is different.
      checkError(
        exception = intercept[AnalysisException] { df.count() },
        condition = COL_ID_MISMATCH)
    }
  }

  test("[4.S5-show-ext] DataFrame fails after external drop+add column same type") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      assert(df.count() === 1)

      val ext = extSession
      ext.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()
      ext.sql(s"ALTER TABLE $T ADD COLUMN salary INT").collect()

      // Column IDs detect that the re-added column is different.
      checkError(
        exception = intercept[AnalysisException] { df.count() },
        condition = COL_ID_MISMATCH)
    }
  }

  // Section 4 Scenario 6: drop+add column with same name but different type.
  // Column ID mismatch is detected before schema validation.
  test("[4.S6-show] DataFrame fails after session drop+add column different type") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      assert(df.count() === 1)

      sql(s"ALTER TABLE $T DROP COLUMN salary")
      sql(s"ALTER TABLE $T ADD COLUMN salary STRING")

      checkError(
        exception = intercept[AnalysisException] { df.count() },
        condition = COL_ID_MISMATCH)
    }
  }

  test("[4.S6-show-ext] DataFrame fails after external drop+add column different type") {
    withTable(T) {
      setupTable()
      val df = spark.table(T)
      assert(df.count() === 1)

      val ext = extSession
      ext.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()
      ext.sql(s"ALTER TABLE $T ADD COLUMN salary STRING").collect()

      // Column ID mismatch is detected before schema validation.
      checkError(
        exception = intercept[AnalysisException] { df.count() },
        condition = COL_ID_MISMATCH)
    }
  }
}
