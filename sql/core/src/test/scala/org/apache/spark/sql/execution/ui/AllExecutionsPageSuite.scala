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

import java.util
import java.util.{Locale, Properties}

import scala.xml.Node

import jakarta.servlet.http.HttpServletRequest
import org.mockito.Mockito.{mock, when, RETURNS_SMART_NULLS}
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.Status.LIVE_UI_LOCAL_STORE_DIR
import org.apache.spark.internal.config.UI.UI_SQL_GROUP_SUB_EXECUTION_ENABLED
import org.apache.spark.scheduler.{JobFailed, SparkListenerJobEnd, SparkListenerJobStart}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.{SparkPlanInfo, SQLExecution}
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

  import testImplicits._

  var kvstore: ElementTrackingStore = _

  after {
    if (kvstore != null) {
      kvstore.close()
      kvstore = null
    }
  }

  test("SPARK-27019: correctly display SQL page when event reordering happens") {
    val statusStore = createStatusStore
    val tab = mock(classOf[SQLTab], RETURNS_SMART_NULLS)
    when(tab.sqlStore).thenReturn(statusStore)

    val request = mock(classOf[HttpServletRequest])
    when(tab.conf).thenReturn(new SparkConf(false))
    when(tab.appName).thenReturn("testing")
    when(tab.headerTabs).thenReturn(Seq.empty)

    val html = renderSQLPage(request, tab, statusStore).toString().toLowerCase(Locale.ROOT)
    assert(html.contains("failed queries"))
    assert(!html.contains("1970/01/01"))
  }

  test("SPARK-40834: prioritize `errorMessage` over job failures") {
    val statusStore = createStatusStore
    val tab = mock(classOf[SQLTab], RETURNS_SMART_NULLS)
    when(tab.sqlStore).thenReturn(statusStore)

    val request = mock(classOf[HttpServletRequest])
    when(tab.conf).thenReturn(new SparkConf(false))
    when(tab.appName).thenReturn("testing")
    when(tab.headerTabs).thenReturn(Seq.empty)

    Seq(Some(""), Some("testErrorMsg"), None).foreach { msg =>
      val listener = statusStore.listener.get
      val page = new AllExecutionsPage(tab)
      val df = createTestDataFrame
      listener.onOtherEvent(SparkListenerSQLExecutionStart(
        0,
        Some(0),
        "test",
        "test",
        df.queryExecution.toString,
        SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
        System.currentTimeMillis()))
      listener.onJobStart(SparkListenerJobStart(
        jobId = 0,
        time = System.currentTimeMillis(),
        stageInfos = Nil,
        createProperties(0)))
      listener.onJobEnd(SparkListenerJobEnd(
        jobId = 0,
        time = System.currentTimeMillis(),
        JobFailed(new RuntimeException("Oops"))))
      listener.onOtherEvent(SparkListenerSQLExecutionEnd(0, System.currentTimeMillis(), msg))
      val html = page.render(request).toString().toLowerCase(Locale.ROOT)

      assert(html.contains("failed queries") == !msg.contains(""))
    }
  }

  test("sorting should be successful") {
    val statusStore = createStatusStore
    val tab = mock(classOf[SQLTab], RETURNS_SMART_NULLS)
    val request = mock(classOf[HttpServletRequest])

    when(tab.conf).thenReturn(new SparkConf(false))
    when(tab.sqlStore).thenReturn(statusStore)
    when(tab.appName).thenReturn("testing")
    when(tab.headerTabs).thenReturn(Seq.empty)
    when(request.getParameter("failed.sort")).thenReturn("Duration")
    val map = new util.HashMap[String, Array[String]]()
    map.put("failed.sort", Array("duration"))
    when(request.getParameterMap()).thenReturn(map)
    val html = renderSQLPage(request, tab, statusStore).toString().toLowerCase(Locale.ROOT)
    assert(!html.contains("illegalargumentexception"))
    assert(html.contains("duration"))
  }

  test("group sub executions") {
    val statusStore = createStatusStore
    val tab = mock(classOf[SQLTab], RETURNS_SMART_NULLS)
    val request = mock(classOf[HttpServletRequest])

    val sparkConf = new SparkConf(false).set(UI_SQL_GROUP_SUB_EXECUTION_ENABLED, true)
    when(tab.conf).thenReturn(sparkConf)
    when(tab.sqlStore).thenReturn(statusStore)
    when(tab.appName).thenReturn("testing")
    when(tab.headerTabs).thenReturn(Seq.empty)

    val listener = statusStore.listener.get
    val page = new AllExecutionsPage(tab)
    val df = createTestDataFrame
    listener.onOtherEvent(SparkListenerSQLExecutionStart(
      0,
      Some(0),
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
      System.currentTimeMillis()))
    listener.onOtherEvent(SparkListenerSQLExecutionStart(
      1,
      Some(0),
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
      System.currentTimeMillis()))
    // sub execution has a missing root execution
    listener.onOtherEvent(SparkListenerSQLExecutionStart(
      2,
      Some(100),
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
      System.currentTimeMillis()))
    val html = page.render(request).toString().toLowerCase(Locale.ROOT)
    assert(html.contains("sub execution ids") && html.contains("sub-execution-list"))
    // sub execution should still be displayed if the root execution is missing
    assert(html.contains("id=2"))
  }

  test("SPARK-42754: group sub executions - backward compatibility") {
    val statusStore = createStatusStore
    val tab = mock(classOf[SQLTab], RETURNS_SMART_NULLS)
    val request = mock(classOf[HttpServletRequest])

    val sparkConf = new SparkConf(false).set(UI_SQL_GROUP_SUB_EXECUTION_ENABLED, true)
    when(tab.conf).thenReturn(sparkConf)
    when(tab.sqlStore).thenReturn(statusStore)
    when(tab.appName).thenReturn("testing")
    when(tab.headerTabs).thenReturn(Seq.empty)

    val listener = statusStore.listener.get
    val page = new AllExecutionsPage(tab)
    val df = createTestDataFrame
    // testing compatibility with old event logs for which rootExecutionId = None
    // because the field is missing when generated by a Spark version not support
    // nested execution
    listener.onOtherEvent(SparkListenerSQLExecutionStart(
      0,
      None,
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
      System.currentTimeMillis()))
    listener.onOtherEvent(SparkListenerSQLExecutionStart(
      1,
      None,
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
      System.currentTimeMillis()))
    val html = page.render(request).toString().toLowerCase(Locale.ROOT)
    assert(!html.contains("sub execution ids") && !html.contains("sub-execution-list"))
  }

  protected def createStatusStore: SQLAppStatusStore

  private def createTestDataFrame: DataFrame = {
    Seq(
      (1, 1),
      (2, 2)
    ).toDF().filter("_1 > 1")
  }

  /**
   * Render a stage page started with the given conf and return the HTML.
   * This also runs a dummy execution page to populate the page with useful content.
   */
  private def renderSQLPage(
    request: HttpServletRequest,
    tab: SQLTab,
    statusStore: SQLAppStatusStore): Seq[Node] = {

    val listener = statusStore.listener.get

    val page = new AllExecutionsPage(tab)
    Seq(0, 1).foreach { executionId =>
      val df = createTestDataFrame
      listener.onOtherEvent(SparkListenerSQLExecutionStart(
        executionId,
        Some(executionId),
        "test",
        "test",
        df.queryExecution.toString,
        SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
        System.currentTimeMillis(),
        Map.empty))
      listener.onOtherEvent(SparkListenerSQLExecutionEnd(
        executionId, System.currentTimeMillis(), Some("Oops")))
      listener.onJobStart(SparkListenerJobStart(
        jobId = 0,
        time = System.currentTimeMillis(),
        stageInfos = Nil,
        createProperties(executionId)))
      listener.onJobEnd(SparkListenerJobEnd(
        jobId = 0,
        time = System.currentTimeMillis(),
        JobFailed(new RuntimeException("Oops"))))
    }
    page.render(request)
  }

  private def createProperties(executionId: Long): Properties = {
    val properties = new Properties()
    properties.setProperty(SQLExecution.EXECUTION_ID_KEY, executionId.toString)
    properties
  }
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

