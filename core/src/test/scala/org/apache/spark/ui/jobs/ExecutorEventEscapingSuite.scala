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
import org.apache.spark.status.api.v1.ExecutorSummary

/**
 * The executor removal reason originates outside the driver: it can carry an executor-side
 * exception message, YARN container diagnostics, or a Kubernetes pod status message. The timeline
 * library treats the generated content as HTML, so the reason must not be able to break out of
 * the `data-bs-title` attribute it is rendered into.
 */
class ExecutorEventEscapingSuite extends SparkFunSuite with MockitoSugar {

  // Closes the data-bs-title attribute and injects a tag if the reason is not HTML escaped.
  private val payload = "\"><img src=x onerror=alert(1)>"

  private def executorWithRemoveReason(reason: String): ExecutorSummary = {
    new ExecutorSummary("1", "host:port", false, 1,
      10, 10, 1, 1, 1,
      0, 0, 1, 100,
      1, 100, 100,
      10, false, 20, new Date(1600984336352L),
      Some(new Date(1600984336353L)), Some(reason), Map(), Option.empty, Set(), Option.empty,
      Map(), Map(), 1, false, Set())
  }

  private def newJobsTab(): JobsTab = {
    val tab = mock[JobsTab]
    when(tab.conf).thenReturn(new SparkConf())
    tab
  }

  private def checkEscaped(event: String): Unit = {
    // The payload must not survive as markup that the browser would parse as a tag.
    assert(!event.contains("<img"), s"Reason was not HTML escaped: $event")
    // Two layers of escaping, matching the adjacent job/stage name handling: the reason is parsed
    // once as an attribute value and once more when Bootstrap assigns it as tooltip innerHTML.
    assert(event.contains("&amp;lt;img src=x onerror=alert(1)&amp;gt;"),
      s"Reason was not escaped twice: $event")
  }

  test("AllJobsPage escapes the executor removal reason in the timeline tooltip") {
    val page = new AllJobsPage(newJobsTab(), mock[AppStatusStore])
    val events = page.makeExecutorEvent(Seq(executorWithRemoveReason(payload)))
    checkEscaped(events.filter(_.contains("Reason:")).mkString)
  }

  test("JobPage escapes the executor removal reason in the timeline tooltip") {
    val page = new JobPage(newJobsTab(), mock[AppStatusStore])
    val events = page.makeExecutorEvent(Seq(executorWithRemoveReason(payload)))
    checkEscaped(events.filter(_.contains("Reason:")).mkString)
  }
}
