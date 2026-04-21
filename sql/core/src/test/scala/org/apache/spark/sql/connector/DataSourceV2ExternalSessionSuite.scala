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
 * Tests for cross-session and external-write scenarios where an external
 * [[SparkSession]] or direct table manipulation modifies a DSv2 table
 * while another session holds a stale [[DataFrame]].
 *
 * Uses [[SharedInMemoryTableCatalog]] so both sessions see the same
 * table data via a static map, simulating a real shared metastore.
 */
class DataSourceV2ExternalSessionSuite extends QueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.ANSI_ENABLED, true)
    .set("spark.sql.catalog.sharedcat",
      classOf[SharedInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.sharedcat.copyOnLoad", "true")

  after {
    SharedInMemoryTableCatalog.reset()
    spark.sessionState.catalogManager.reset()
  }

  /**
   * Creates a [[SparkSession]] with a SEPARATE [[SharedState]] (separate
   * [[CacheManager]] and relation cache) but the same [[SparkContext]] and
   * catalog configs. [[SharedInMemoryTableCatalog]] tables are shared
   * via the companion object's static map, so the external session
   * sees the same table data. This simulates a truly external writer
   * whose writes do NOT invalidate this session's [[CacheManager]].
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
      savedDefault.foreach(s =>
        SparkSession.setDefaultSession(s))
      savedActive.foreach(s =>
        SparkSession.setActiveSession(s))
    }
  }

  private val T = "sharedcat.ns.tbl"

  test("external write visible via fresh query") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100)")

      checkAnswer(spark.table(T), Seq(Row(1, 100)))

      // external session writes data
      extSession.sql(
        s"INSERT INTO $T VALUES (2, 200)").collect()

      // a fresh query from session1 picks up external write
      checkAnswer(
        spark.table(T),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("external drop+re-add column detected by column ID") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100)")

      val df = spark.table(T)

      // external session drops and re-adds column
      val ext = extSession
      ext.sql(s"ALTER TABLE $T DROP COLUMN salary").collect()
      ext.sql(s"ALTER TABLE $T ADD COLUMN salary INT").collect()

      // column ID changed, session1 detects it
      checkError(
        exception = intercept[AnalysisException] {
          df.collect()
        },
        condition =
          "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS" +
            ".COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map(
          "tableName" -> ".*", "errors" -> ".*"))
    }
  }

  test("external drop+recreate table detected by table ID") {
    withTable(T) {
      sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $T VALUES (1, 100)")

      val df = spark.table(T)

      // external session drops and recreates table
      val ext = extSession
      ext.sql(s"DROP TABLE $T").collect()
      ext.sql(
        s"CREATE TABLE $T (id INT, salary INT) USING foo"
      ).collect()

      // table ID changed, session1 detects it
      checkError(
        exception = intercept[AnalysisException] {
          df.collect()
        },
        condition =
          "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS" +
            ".TABLE_ID_MISMATCH",
        matchPVals = true,
        parameters = Map(
          "tableName" -> ".*",
          "capturedTableId" -> ".*",
          "currentTableId" -> ".*"))
    }
  }

}
