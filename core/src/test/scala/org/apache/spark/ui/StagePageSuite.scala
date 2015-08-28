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

package org.apache.spark.ui

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.mockito.Mockito.{mock, when, RETURNS_SMART_NULLS}

import org.apache.spark.{LocalSparkContext, SparkConf, SparkFunSuite, Success}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.ui.jobs.{JobProgressListener, StagePage, StagesTab}
import org.apache.spark.ui.scope.RDDOperationGraphListener

class StagePageSuite extends SparkFunSuite with LocalSparkContext {

  test("peak execution memory only displayed if unsafe is enabled") {
    val unsafeConf = "spark.sql.unsafe.enabled"
    val conf = new SparkConf(false).set(unsafeConf, "true")
    val html = renderStagePage(conf).toString().toLowerCase
    val targetString = "peak execution memory"
    assert(html.contains(targetString))
    // Disable unsafe and make sure it's not there
    val conf2 = new SparkConf(false).set(unsafeConf, "false")
    val html2 = renderStagePage(conf2).toString().toLowerCase
    assert(!html2.contains(targetString))
    // Avoid setting anything; it should be displayed by default
    val conf3 = new SparkConf(false)
    val html3 = renderStagePage(conf3).toString().toLowerCase
    assert(html3.contains(targetString))
  }

  /**
   * Render a stage page started with the given conf and return the HTML.
   * This also runs a dummy stage to populate the page with useful content.
   */
  private def renderStagePage(conf: SparkConf): Seq[Node] = {
    val jobListener = new JobProgressListener(conf)
    val graphListener = new RDDOperationGraphListener(conf)
    val tab = mock(classOf[StagesTab], RETURNS_SMART_NULLS)
    val request = mock(classOf[HttpServletRequest])
    when(tab.conf).thenReturn(conf)
    when(tab.progressListener).thenReturn(jobListener)
    when(tab.operationGraphListener).thenReturn(graphListener)
    when(tab.appName).thenReturn("testing")
    when(tab.headerTabs).thenReturn(Seq.empty)
    when(request.getParameter("id")).thenReturn("0")
    when(request.getParameter("attempt")).thenReturn("0")
    val page = new StagePage(tab)

    // Simulate a stage in job progress listener
    val stageInfo = new StageInfo(0, 0, "dummy", 1, Seq.empty, Seq.empty, "details")
    val taskInfo = new TaskInfo(0, 0, 0, 0, "0", "localhost", TaskLocality.ANY, false)
    jobListener.onStageSubmitted(SparkListenerStageSubmitted(stageInfo))
    jobListener.onTaskStart(SparkListenerTaskStart(0, 0, taskInfo))
    taskInfo.markSuccessful()
    jobListener.onTaskEnd(
      SparkListenerTaskEnd(0, 0, "result", Success, taskInfo, TaskMetrics.empty))
    jobListener.onStageCompleted(SparkListenerStageCompleted(stageInfo))
    page.render(request)
  }

}
