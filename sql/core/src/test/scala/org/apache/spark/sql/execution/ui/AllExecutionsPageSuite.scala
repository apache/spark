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

package org.apache.spark.sql.execution.ui

import java.util.Locale

import jakarta.servlet.http.HttpServletRequest
import org.mockito.Mockito.{mock, when, RETURNS_SMART_NULLS}
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.Status.LIVE_UI_LOCAL_STORE_DIR
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.status.{AppStatusStore, ElementTrackingStore}
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.InMemoryStore

abstract class AllExecutionsPageSuite extends SharedSparkSession with BeforeAndAfter {

  override def sparkConf: SparkConf = {
    // Disable async kv store write in the UI, to make tests more stable here.
    super.sparkConf.set(org.apache.spark.internal.config.Status.ASYNC_TRACKING_ENABLED, false)
  }

  var kvstore: ElementTrackingStore = _

  after {
    if (kvstore != null) {
      kvstore.close()
      kvstore = null
    }
  }

  test("SPARK-55875: render skeleton page with DataTables and script includes") {
    val statusStore = createStatusStore
    val tab = mock(classOf[SQLTab], RETURNS_SMART_NULLS)
    when(tab.sqlStore).thenReturn(statusStore)

    val request = mock(classOf[HttpServletRequest])
    when(tab.conf).thenReturn(new SparkConf(false))
    when(tab.appName).thenReturn("testing")
    when(tab.headerTabs).thenReturn(Seq.empty)

    val page = new AllExecutionsPage(tab)
    val html = page.render(request).toString().toLowerCase(Locale.ROOT)
    assert(html.contains("sql-executions-table"))
    assert(html.contains("allexecutionspage.js"))
    assert(html.contains("datatables"))
  }

  protected def createStatusStore: SQLAppStatusStore
}

class AllExecutionsPageWithInMemoryStoreSuite extends AllExecutionsPageSuite {
  override protected def createStatusStore: SQLAppStatusStore = {
    val conf = sparkContext.conf
    kvstore = new ElementTrackingStore(new InMemoryStore, conf)
    val listener = new SQLAppStatusListener(conf, kvstore, live = true)
    new SQLAppStatusStore(kvstore, Some(listener))
  }
}

@SlowSQLTest
class AllExecutionsPageWithRocksDBBackendSuite extends AllExecutionsPageSuite {
  private val storePath = Utils.createTempDir()
  override protected def createStatusStore: SQLAppStatusStore = {
    val conf = sparkContext.conf
    conf.set(LIVE_UI_LOCAL_STORE_DIR, storePath.getCanonicalPath)
    val appStatusStore = AppStatusStore.createLiveStore(conf)
    kvstore = appStatusStore.store.asInstanceOf[ElementTrackingStore]
    val listener = new SQLAppStatusListener(conf, kvstore, live = true)
    new SQLAppStatusStore(kvstore, Some(listener))
  }

  protected override def afterAll(): Unit = {
    if (storePath.exists()) {
      Utils.deleteRecursively(storePath)
    }
    super.afterAll()
  }
}
