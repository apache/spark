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

import java.util.Locale
import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.mockito.Mockito.{mock, when, RETURNS_SMART_NULLS}

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.storage.StorageStatusListener
import org.apache.spark.ui.exec.ExecutorsListener
import org.apache.spark.ui.jobs.{JobProgressListener, StagePage, StagesTab}
import org.apache.spark.ui.scope.RDDOperationGraphListener

class StagePageSuite extends SparkFunSuite with LocalSparkContext {

  private val peakExecutionMemory = 10

  test("peak execution memory should displayed") {
    val conf = new SparkConf(false)
    val html = renderStagePage(conf).toString().toLowerCase(Locale.ROOT)
    val targetString = "peak execution memory"
    assert(html.contains(targetString))
  }

  test("SPARK-10543: peak execution memory should be per-task rather than cumulative") {
    val conf = new SparkConf(false)
    val html = renderStagePage(conf).toString().toLowerCase(Locale.ROOT)
    // verify min/25/50/75/max show task value not cumulative values
    assert(html.contains(s"<td>$peakExecutionMemory.0 b</td>" * 5))
  }

  /**
   * Render a stage page started with the given conf and return the HTML.
   * This also runs a dummy stage to populate the page with useful content.
   */
  private def renderStagePage(conf: SparkConf): Seq[Node] = {
    val jobListener = new JobProgressListener(conf)
    val graphListener = new RDDOperationGraphListener(conf)
    val executorsListener = new ExecutorsListener(new StorageStatusListener(conf), conf)
    val tab = mock(classOf[StagesTab], RETURNS_SMART_NULLS)
    val request = mock(classOf[HttpServletRequest])
    when(tab.conf).thenReturn(conf)
    when(tab.progressListener).thenReturn(jobListener)
    when(tab.operationGraphListener).thenReturn(graphListener)
    when(tab.executorsListener).thenReturn(executorsListener)
    when(tab.appName).thenReturn("testing")
    when(tab.headerTabs).thenReturn(Seq.empty)
    when(request.getParameter("id")).thenReturn("0")
    when(request.getParameter("attempt")).thenReturn("0")
    val page = new StagePage(tab)

    // Simulate a stage in job progress listener
    val stageInfo = new StageInfo(0, 0, "dummy", 1, Seq.empty, Seq.empty, "details")
    // Simulate two tasks to test PEAK_EXECUTION_MEMORY correctness
    (1 to 2).foreach {
      taskId =>
        val taskInfo = new TaskInfo(taskId, taskId, 0, 0, "0", "localhost", TaskLocality.ANY, false)
        jobListener.onStageSubmitted(SparkListenerStageSubmitted(stageInfo))
        jobListener.onTaskStart(SparkListenerTaskStart(0, 0, taskInfo))
        taskInfo.markFinished(TaskState.FINISHED, System.currentTimeMillis())
        val taskMetrics = TaskMetrics.empty
        taskMetrics.incPeakExecutionMemory(peakExecutionMemory)
        jobListener.onTaskEnd(
          SparkListenerTaskEnd(0, 0, "result", Success, taskInfo, taskMetrics))
    }
    jobListener.onStageCompleted(SparkListenerStageCompleted(stageInfo))
    page.render(request)
  }

}
