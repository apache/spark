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

import java.util.Locale
import javax.servlet.http.HttpServletRequest

import org.mockito.Mockito.{mock, when}
import org.scalatest.BeforeAndAfter

import org.apache.spark.deploy.history.{Utils => HsUtils}
import org.apache.spark.sql.execution.ui.StreamingQueryStatusStore
import org.apache.spark.sql.test.SharedSparkSession

class StreamingQueryHistorySuite extends SharedSparkSession with BeforeAndAfter {

  test("support streaming query events") {
    val logDir = Thread.currentThread().getContextClassLoader.getResource("spark-events").toString
    HsUtils.withFsHistoryProvider(logDir) { provider =>
      val appUi = provider.getAppUI("local-1596020211915", None).getOrElse {
        assert(false, "Failed to load event log of local-1596020211915.")
        null
      }
      assert(appUi.ui.appName == "StructuredKafkaWordCount")
      assert(appUi.ui.store.store.count(classOf[StreamingQueryData]) == 1)
      assert(appUi.ui.store.store.count(classOf[StreamingQueryProgressWrapper]) == 8)

      val store = new StreamingQueryStatusStore(appUi.ui.store.store)
      val tab = new StreamingQueryTab(store, appUi.ui)
      val request = mock(classOf[HttpServletRequest])
      var html = new StreamingQueryPage(tab).render(request)
        .toString().toLowerCase(Locale.ROOT)
      // 81.39: Avg Input /sec
      assert(html.contains("81.39"))
      // 157.05: Avg Process /sec
      assert(html.contains("157.05"))

      val id = "8d268dc2-bc9c-4be8-97a9-b135d2943028"
      val runId = "e225d92f-2545-48f8-87a2-9c0309580f8a"
      when(request.getParameter("id")).thenReturn(runId)
      html = new StreamingQueryStatisticsPage(tab).render(request)
        .toString().toLowerCase(Locale.ROOT)
      assert(html.contains("<strong>8</strong> completed batches"))
      assert(html.contains(id))
      assert(html.contains(runId))
    }
  }
}
