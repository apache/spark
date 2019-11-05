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

import java.util.Locale
import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.mockito.Mockito.{mock, when, RETURNS_SMART_NULLS}

import org.apache.spark.SparkFunSuite
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2.HiveThriftServer2Listener
import org.apache.spark.sql.internal.SQLConf

class ThriftServerPageSuite extends SparkFunSuite {

  test("Thriftserver page should load successfully") {
    val tab = mock(classOf[ThriftServerTab], RETURNS_SMART_NULLS)
    val request = mock(classOf[HttpServletRequest])
    val listener = new HiveThriftServer2Listener(mock(classOf[HiveThriftServer2]), new SQLConf)
    when(tab.listener).thenReturn(listener)
    when(tab.appName).thenReturn("testing")
    when(tab.headerTabs).thenReturn(Seq.empty)
    // when(request.getParameter("failed.sort")).thenReturn("Duration")
//    val map = new util.HashMap[String, Array[String]]()
//    map.put("failed.sort", Array("duration"))
//    when(request.getParameterMap()).thenReturn(map)
    val html = renderThriftServerPage(request, listener, tab).toString().toLowerCase(Locale.ROOT)
    println(html)
    // session statistics and sql statistics tables should load successfully
    assert(html.contains("session statistics (1)") && html.contains("sql statistics (1)"))
    assert(html.contains("dummy query") && html.contains("dummy plan"))
    // Pagination support
    assert(html.contains("<label>1 pages. jump to</label>\n" +
      "          <input type=\"text\" name=\"sessionstat.page\"" +
      " id=\"form-sessionstat-page-no\" value=\"1\" class=\"span1\"/>\n\n  " +
      "        <label>. show </label>\n" +
      "          <input type=\"text\" id=\"form-sessionstat-page-size\" " +
      "name=\"sessionstat.pagesize\" value=\"100\" class=\"span1\"/>\n" +
      "          <label>items in a page.</label>")
      // Hiding table support
//    assert(html.contains("sessions"))
  }

  /**
    * Render a thriftserver page started with the given conf and return the HTML.
    */
  private def renderThriftServerPage(
    request: HttpServletRequest,
    listener: HiveThriftServer2Listener,
    tab: ThriftServerTab): Seq[Node] = {



    val page = new ThriftServerPage(tab)
    listener.onSessionCreated("localhost", "sessionid", "user")
    listener.onStatementStart("id", "sessionid", "dummy query", "groupid", "user")
    listener.onStatementParsed("id", "dummy plan")
    listener.onJobStart(SparkListenerJobStart(0, System.currentTimeMillis(), Seq()))
    listener.onStatementFinish("id")
    listener.onOperationClosed("id")
    listener.onSessionClosed("sessionid")
    page.render(request)
  }
}

