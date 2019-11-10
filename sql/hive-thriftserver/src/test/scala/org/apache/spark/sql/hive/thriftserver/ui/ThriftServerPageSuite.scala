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

package org.apache.spark.sql.hive.thriftserver.ui

import java.util.{Calendar, Locale}
import javax.servlet.http.HttpServletRequest

import org.mockito.Mockito.{mock, when, RETURNS_SMART_NULLS}
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.thriftserver._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.util.kvstore.InMemoryStore


class ThriftServerPageSuite extends SparkFunSuite with BeforeAndAfter {

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
  private def getStatusStore: HiveThriftServer2AppStatusStore = {
    kvstore = new ElementTrackingStore(new InMemoryStore, new SparkConf())
    val server = mock(classOf[HiveThriftServer2], RETURNS_SMART_NULLS)
    val sc = mock(classOf[SparkContext])
    val sqlConf = new SQLConf

    val listener = new HiveThriftServer2Listener(kvstore, Some(server), Some(sqlConf), Some(sc))
    val statusStore = new HiveThriftServer2AppStatusStore(kvstore, Some(listener))

    listener.onOtherEvent(SparkListenerSessionCreated("localhost", "sessionid", "user",
      System.currentTimeMillis()))
    listener.onOtherEvent(SparkListenerStatementStart("id", "sessionid",
      "dummy query", "groupid", System.currentTimeMillis(), "user"))
    listener.onOtherEvent(SparkListenerStatementParsed("id", "dummy plan"))
    listener.onOtherEvent(SparkListenerJobStart(0, System.currentTimeMillis(), Seq()))
    listener.onOtherEvent(SparkListenerStatementFinish("id", System.currentTimeMillis()))
    listener.onOtherEvent(SparkListenerOperationClosed("id", System.currentTimeMillis()))
    listener.onOtherEvent(SparkListenerSessionClosed("sessionid", System.currentTimeMillis()))

    statusStore
  }

  test("thriftserver page should load successfully") {
    val store = getStatusStore

    val request = mock(classOf[HttpServletRequest])
    val tab = mock(classOf[ThriftServerTab], RETURNS_SMART_NULLS)
    when(tab.startTime).thenReturn(Calendar.getInstance().getTime)
    when(tab.store).thenReturn(store)
    when(tab.appName).thenReturn("testing")
    when(tab.headerTabs).thenReturn(Seq.empty)
    val page = new ThriftServerPage(tab)
    val html = page.render(request).toString().toLowerCase(Locale.ROOT)

    // session statistics and sql statistics tables should load successfully
    assert(html.contains("session statistics (1)"))
    assert(html.contains("sql statistics (1)"))
    assert(html.contains("dummy query"))
    assert(html.contains("dummy plan"))

    // Pagination support
    assert(html.contains("<label>1 pages. jump to</label>"))

    // Hiding table support
    assert(html.contains("class=\"collapse-aggregated-sessionstat" +
       " collapse-table\" onclick=\"collapsetable"))
  }

  test("thriftserver session page should load successfully") {
    val store = getStatusStore

    val request = mock(classOf[HttpServletRequest])
    when(request.getParameter("id")).thenReturn("sessionid")
    val tab = mock(classOf[ThriftServerTab], RETURNS_SMART_NULLS)
    when(tab.startTime).thenReturn(Calendar.getInstance().getTime)
    when(tab.store).thenReturn(store)
    when(tab.appName).thenReturn("testing")
    when(tab.headerTabs).thenReturn(Seq.empty)
    val page = new ThriftServerSessionPage(tab)
    val html = page.render(request).toString().toLowerCase(Locale.ROOT)

    // session sql statistics table should load successfully
    assert(html.contains("sql statistics"))
    assert(html.contains("user"))
    assert(html.contains("groupid"))

    // Pagination support
    assert(html.contains("<label>1 pages. jump to</label>"))

    // Hiding table support
    assert(html.contains("collapse-aggregated-sqlsessionstat collapse-table\"" +
          " onclick=\"collapsetable"))
  }
}

