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

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.sql.{classic, connect, SparkSession}
import org.apache.spark.sql.connect.service.{SessionKey, SparkConnectService}
import org.apache.spark.sql.connector.{DSv2CacheTableReadTests, DSv2IncrementallyConstructedQueryTests, DSv2RepeatedTableAccessTests, DSv2TempViewWithStoredPlanTests}
import org.apache.spark.sql.connector.catalog.{CachingInMemoryTableCatalog, InMemoryTableCatalog, NullTableIdAndNullColumnIdInMemoryTableCatalog, NullTableIdInMemoryTableCatalog, TableCatalog}

/**
 * Connect-mode counterpart of [[org.apache.spark.sql.connector.DataSourceV2DataFrameSuite]].
 *
 * Runs DSv2 temp view tests ([[DSv2TempViewWithStoredPlanTests]]), repeated table access tests
 * ([[DSv2RepeatedTableAccessTests]]), incrementally constructed query tests
 * ([[DSv2IncrementallyConstructedQueryTests]]), and CACHE TABLE read tests
 * ([[DSv2CacheTableReadTests]]) under Spark Connect. All test logic lives in the shared traits;
 * this class only provides the Connect-specific session, catalog access, and result comparison.
 */
class DataSourceV2DataFrameConnectSuite
    extends SessionQueryTest
    with DSv2TempViewWithStoredPlanTests
    with DSv2RepeatedTableAccessTests
    with DSv2IncrementallyConstructedQueryTests
    with DSv2CacheTableReadTests {

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    .set("spark.sql.catalog.testcat.copyOnLoad", "true")
    .set("spark.sql.catalog.cachingcat", classOf[CachingInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.cachingcat.copyOnLoad", "true")
    .set("spark.sql.catalog.nullidcat", classOf[NullTableIdInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.nullidcat.copyOnLoad", "true")
    .set(
      "spark.sql.catalog.nullbothidscat",
      classOf[NullTableIdAndNullColumnIdInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.nullbothidscat.copyOnLoad", "true")

  override protected def testPrefix: String = "[connect] "

  // Unlike the classic suite, the connect suite shares a single server-side session across all
  // tests in the suite (created once by SparkSessionBinder), so catalog state is not reset
  // between tests. Mirror the classic suite's `after` block: clear the caching connector's cache
  // and reset the catalog manager so each test starts from a clean catalog state. Without this,
  // CachingInMemoryTableCatalog retains stale table entries (it does not invalidate on drop), so a
  // later test's CREATE TABLE fails with TABLE_OR_VIEW_ALREADY_EXISTS.
  override protected def afterEach(): Unit = {
    val serverSession = getServerSession(spark)
    if (serverSession.sessionState.catalogManager.isCatalogRegistered("cachingcat")) {
      serverSession.sessionState.catalogManager
        .catalog("cachingcat")
        .asInstanceOf[CachingInMemoryTableCatalog]
        .clearCache()
    }
    super.afterEach()
  }

  protected def getServerSession(clientSession: SparkSession): classic.SparkSession = {
    val connectSession = clientSession.asInstanceOf[connect.SparkSession]
    val userId = connectSession.client.userId
    val sessionId = connectSession.sessionId
    val key = SessionKey(userId, sessionId)
    SparkConnectService.sessionManager
      .getIsolatedSessionIfPresent(key)
      .get
      .session
  }

  override protected def getTableCatalog[C <: TableCatalog: ClassTag](
      session: SparkSession,
      catalogName: String): C = {
    val catalog = getServerSession(session).sessionState.catalogManager.catalog(catalogName)
    val ct = implicitly[ClassTag[C]]
    require(
      ct.runtimeClass.isInstance(catalog),
      s"Expected ${ct.runtimeClass.getName} but got ${catalog.getClass.getName}")
    catalog.asInstanceOf[C]
  }
}
