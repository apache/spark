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
 * Shared infrastructure for DSv2 table refresh and pinning test suites.
 *
 * Provides catalog setup (sharedcat, cachingcat, nullidcat), cleanup,
 * and helper methods for table creation and external session simulation.
 */
trait DataSourceV2TableRefreshTestBase
    extends QueryTest with SharedSparkSession {

  // Error condition constants
  protected val COL_MISMATCH =
    "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH"
  protected val ID_MISMATCH =
    "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.TABLE_ID_MISMATCH"
  protected val VIEW_PLAN_CHANGED =
    "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION"

  override protected def checkErrorIgnorableParameters
    : Map[String, Set[String]] =
    super.checkErrorIgnorableParameters ++ Map(
      COL_MISMATCH -> Set("tableName", "errors"),
      ID_MISMATCH ->
        Set("tableName", "capturedTableId", "currentTableId"),
      VIEW_PLAN_CHANGED ->
        Set("viewName", "tableName", "colType", "errors"))

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.ANSI_ENABLED, true)
    .set("spark.sql.catalog.sharedcat",
      classOf[SharedInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.sharedcat.copyOnLoad", "true")
    .set("spark.sql.catalog.cachingcat",
      classOf[CachingInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.cachingcat.copyOnLoad", "true")
    .set("spark.sql.catalog.nullidcat",
      classOf[NullIdInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.nullidcat.copyOnLoad", "true")

  override def afterEach(): Unit = {
    SharedInMemoryTableCatalog.reset()
    CachingInMemoryTableCatalog.clearCache()
    try {
      spark.sessionState.catalogManager.reset()
    } finally {
      super.afterEach()
    }
  }

  protected val T = "sharedcat.ns.tbl"
  protected val T2 = "sharedcat.ns.tbl2"

  protected def setupTable(): Unit = {
    sql(s"CREATE TABLE $T (id INT, salary INT) USING foo")
    sql(s"INSERT INTO $T VALUES (1, 100)")
  }

  protected val CT = "cachingcat.ns.tbl"

  protected def setupCachingTable(): Unit = {
    sql(s"CREATE TABLE $CT (id INT, salary INT) USING foo")
    sql(s"INSERT INTO $CT VALUES (1, 100)")
  }

  /**
   * Creates a SparkSession with a SEPARATE CacheManager (separate SharedState)
   * but the same SparkContext and catalog configs. SharedInMemoryTableCatalog
   * tables are shared via the companion object, so the external session sees
   * the same table data. This simulates a truly external writer (different JVM
   * in production) whose writes do NOT invalidate Session 1's CacheManager.
   */
  protected def extSession: SparkSession = {
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
}
