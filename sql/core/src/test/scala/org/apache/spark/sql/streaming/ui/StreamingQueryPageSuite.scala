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

package org.apache.spark.sql.streaming.ui

import java.util.{Locale, UUID}
import javax.servlet.http.HttpServletRequest

import org.mockito.Mockito.{mock, when, RETURNS_SMART_NULLS}
import org.scalatest.BeforeAndAfter
import scala.xml.Node

import org.apache.spark.sql.streaming.StreamingQueryProgress
import org.apache.spark.sql.test.SharedSparkSession

class StreamingQueryPageSuite extends SharedSparkSession with BeforeAndAfter {

  test("correctly display streaming query page") {
    val id = UUID.randomUUID()
    val request = mock(classOf[HttpServletRequest])
    val tab = mock(classOf[StreamingQueryTab], RETURNS_SMART_NULLS)
    val statusListener = mock(classOf[StreamingQueryStatusListener], RETURNS_SMART_NULLS)
    when(tab.appName).thenReturn("testing")
    when(tab.headerTabs).thenReturn(Seq.empty)
    when(tab.statusListener).thenReturn(statusListener)

    val streamQuery = createStreamQueryUIData(id)
    when(statusListener.allQueryStatus).thenReturn(Seq(streamQuery))
    var html = renderStreamingQueryPage(request, tab)
      .toString().toLowerCase(Locale.ROOT)
    assert(html.contains("active streaming queries (1)"))
    assert(html.contains("completed streaming queries (0)"))

    when(streamQuery.isActive).thenReturn(false)
    when(streamQuery.exception).thenReturn(None)
    html = renderStreamingQueryPage(request, tab)
      .toString().toLowerCase(Locale.ROOT)
    assert(html.contains("active streaming queries (0)"))
    assert(html.contains("completed streaming queries (1)"))
    assert(html.contains("finished"))

    when(streamQuery.isActive).thenReturn(false)
    when(streamQuery.exception).thenReturn(Option("exception in query"))
    html = renderStreamingQueryPage(request, tab)
      .toString().toLowerCase(Locale.ROOT)
    assert(html.contains("active streaming queries (0)"))
    assert(html.contains("completed streaming queries (1)"))
    assert(html.contains("failed"))
    assert(html.contains("exception in query"))
  }

  test("correctly display streaming query statistics page") {
    val id = UUID.randomUUID()
    val request = mock(classOf[HttpServletRequest])
    val tab = mock(classOf[StreamingQueryTab], RETURNS_SMART_NULLS)
    val statusListener = mock(classOf[StreamingQueryStatusListener], RETURNS_SMART_NULLS)
    when(request.getParameter("id")).thenReturn(id.toString)
    when(tab.appName).thenReturn("testing")
    when(tab.headerTabs).thenReturn(Seq.empty)
    when(tab.statusListener).thenReturn(statusListener)

    val streamQuery = createStreamQueryUIData(id)
    when(statusListener.allQueryStatus).thenReturn(Seq(streamQuery))
    val html = renderStreamingQueryStatisticsPage(request, tab)
      .toString().toLowerCase(Locale.ROOT)

    assert(html.contains("<strong>name: </strong>query<"))
    assert(html.contains("""{"x": 1001898000100, "y": 10.0}"""))
    assert(html.contains("""{"x": 1001898000100, "y": 12.0}"""))
    assert(html.contains("(<strong>3</strong> completed batches)"))
  }

  private def createStreamQueryUIData(id: UUID): StreamingQueryUIData = {
    val progress = mock(classOf[StreamingQueryProgress], RETURNS_SMART_NULLS)
    when(progress.timestamp).thenReturn("2001-10-01T01:00:00.100Z")
    when(progress.inputRowsPerSecond).thenReturn(10.0)
    when(progress.processedRowsPerSecond).thenReturn(12.0)
    when(progress.batchId).thenReturn(2)
    when(progress.prettyJson).thenReturn("""{"a":1}""")

    val streamQuery = mock(classOf[StreamingQueryUIData], RETURNS_SMART_NULLS)
    when(streamQuery.isActive).thenReturn(true)
    when(streamQuery.name).thenReturn("query")
    when(streamQuery.id).thenReturn(id)
    when(streamQuery.runId).thenReturn(id)
    when(streamQuery.startTimestamp).thenReturn(1L)
    when(streamQuery.lastProgress).thenReturn(progress)
    when(streamQuery.recentProgress).thenReturn(Array(progress))
    when(streamQuery.exception).thenReturn(None)

    streamQuery
  }

  /**
   * Render a stage page started with the given conf and return the HTML.
   * This also runs a dummy execution page to populate the page with useful content.
   */
  private def renderStreamingQueryPage(
      request: HttpServletRequest,
      tab: StreamingQueryTab): Seq[Node] = {
    val page = new StreamingQueryPage(tab)
    page.render(request)
  }

  private def renderStreamingQueryStatisticsPage(
      request: HttpServletRequest,
      tab: StreamingQueryTab): Seq[Node] = {
    val page = new StreamingQueryStatisticsPage(tab)
    page.render(request)
  }
}
