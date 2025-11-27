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

package org.apache.spark.sql.connect.ui

import java.util.{Calendar, Locale}

import jakarta.servlet.http.HttpServletRequest
import org.mockito.Mockito.{mock, when, RETURNS_SMART_NULLS}
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SharedSparkContext, SparkConf, SparkFunSuite}
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.sql.connect.service._
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.util.kvstore.InMemoryStore

class SparkConnectServerPageSuite
    extends SparkFunSuite
    with BeforeAndAfter
    with SharedSparkContext {

  private var kvstore: ElementTrackingStore = _

  after {
    if (kvstore != null) {
      kvstore.close()
      kvstore = null
    }
  }

  /**
   * Run a dummy session and return the store
   */
  private def getStatusStore: SparkConnectServerAppStatusStore = {
    kvstore = new ElementTrackingStore(new InMemoryStore, new SparkConf())
    // val server = mock(classOf[SparkConnectServer], RETURNS_SMART_NULLS)
    val sparkConf = new SparkConf

    val listener = new SparkConnectServerListener(kvstore, sparkConf)
    val statusStore = new SparkConnectServerAppStatusStore(kvstore)

    listener.onOtherEvent(
      SparkListenerConnectSessionStarted("sessionId", "userId", System.currentTimeMillis()))
    listener.onOtherEvent(
      SparkListenerConnectOperationStarted(
        "jobTag",
        "operationId",
        System.currentTimeMillis(),
        "sessionId",
        "userId",
        "userName",
        "dummy query",
        Set()))
    listener.onOtherEvent(
      SparkListenerConnectOperationAnalyzed("jobTag", "dummy plan", System.currentTimeMillis()))
    listener.onOtherEvent(SparkListenerJobStart(0, System.currentTimeMillis(), Seq()))
    listener.onOtherEvent(
      SparkListenerConnectOperationFinished("jobTag", "operationId", System.currentTimeMillis()))
    listener.onOtherEvent(
      SparkListenerConnectOperationClosed("jobTag", "operationId", System.currentTimeMillis()))
    listener.onOtherEvent(
      SparkListenerConnectSessionClosed("sessionId", "userId", System.currentTimeMillis()))

    statusStore
  }

  test("Spark Connect Server page should load successfully") {
    val store = getStatusStore

    val request = mock(classOf[HttpServletRequest])
    val tab = mock(classOf[SparkConnectServerTab], RETURNS_SMART_NULLS)
    when(tab.startTime).thenReturn(Calendar.getInstance().getTime)
    when(tab.store).thenReturn(store)
    when(tab.appName).thenReturn("testing")
    when(tab.headerTabs).thenReturn(Seq.empty)
    val page = new SparkConnectServerPage(tab)
    val html = page.render(request).toString().toLowerCase(Locale.ROOT)

    // session statistics and sql statistics tables should load successfully
    assert(html.contains("session statistics (1)"))
    assert(html.contains("request statistics (1)"))
    assert(html.contains("dummy query"))

    // Pagination support
    assert(html.contains("<label>1 pages. jump to</label>"))

    // Hiding table support
    assert(
      html.contains("class=\"collapse-aggregated-sessionstat" +
        " collapse-table\" onclick=\"collapsetable"))
  }

  test("Spark Connect Server session page should load successfully") {
    val store = getStatusStore

    val request = mock(classOf[HttpServletRequest])
    when(request.getParameter("id")).thenReturn("sessionId")
    val tab = mock(classOf[SparkConnectServerTab], RETURNS_SMART_NULLS)
    when(tab.startTime).thenReturn(Calendar.getInstance().getTime)
    when(tab.store).thenReturn(store)
    when(tab.appName).thenReturn("testing")
    when(tab.headerTabs).thenReturn(Seq.empty)
    val page = new SparkConnectServerSessionPage(tab)
    val html = page.render(request).toString().toLowerCase(Locale.ROOT)

    // session sql statistics table should load successfully
    assert(html.contains("request statistics"))
    assert(html.contains("userid"))
    assert(html.contains("jobtag"))

    // Pagination support
    assert(html.contains("<label>1 pages. jump to</label>"))

    // Hiding table support
    assert(
      html.contains("collapse-aggregated-sqlsessionstat collapse-table\"" +
        " onclick=\"collapsetable"))
  }
}
