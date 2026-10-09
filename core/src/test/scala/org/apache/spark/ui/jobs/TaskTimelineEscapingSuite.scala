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

package org.apache.spark.ui.jobs

import java.util.Date

import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.TaskData

/**
 * Executor ids and hosts originate outside the web UI: they come from the cluster manager
 * or are replayed from the event log, so they can carry quotes or markup (e.g. custom
 * executor names). The timeline library treats the generated content as HTML and embeds it
 * in JavaScript string literals, so these values must be escaped to keep the task timeline
 * rendering intact. The task status is escaped along with them as defense in depth.
 */
class TaskTimelineEscapingSuite extends SparkFunSuite with MockitoSugar {

  // Closes the JS string literal and injects a tag if the values are not escaped.
  private val payload = "'); alert(document.domain); ('<img src=x onerror=alert(1)>"

  private def taskWith(executorId: String, host: String, status: String): TaskData = {
    new TaskData(0L, 0, 0, 0, new Date(1600984336352L), None, Some(100L),
      executorId, host, status, "PROCESS_LOCAL", false, Seq.empty,
      errorMessage = None, taskMetrics = None,
      executorLogs = Map(), schedulerDelay = 0L, gettingResultTime = 0L)
  }

  private def newStagesTab(): StagesTab = {
    val tab = mock[StagesTab]
    when(tab.conf).thenReturn(new SparkConf())
    tab
  }

  test("StagePage escapes executor id, host and status in the task timeline") {
    val page = new StagePage(newStagesTab(), mock[AppStatusStore])
    val task = taskWith(s"exec$payload", s"host$payload", s"FAILED$payload")
    val rendered = page.makeTimeline(
      () => Seq(task), currentTime = 1600984336400L, page = 1, pageSize = 10,
      totalPages = 1, stageId = 1, stageAttemptId = 0, totalTasks = 1).mkString

    // The raw payload must not survive anywhere in the emitted timeline markup/script.
    assert(!rendered.contains(payload), s"Raw payload survived: $rendered")
    // The single quote must be escaped so it cannot terminate the JS string literal.
    assert(rendered.contains("\\')"), s"Quote was not JavaScript escaped: $rendered")
    assert(!rendered.contains("<img"), s"Markup was not HTML escaped: $rendered")
    // The status is rendered into the tooltip's data-bs-title, which the tooltip re-parses
    // as HTML, so it needs two HTML-escape layers, matching the executor removal reason.
    assert(rendered.contains("&amp;lt;img"), s"Status was not escaped twice: $rendered")
  }
}
