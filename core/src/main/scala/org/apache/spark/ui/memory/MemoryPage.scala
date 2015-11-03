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

package org.apache.spark.ui.memory

import javax.servlet.http.HttpServletRequest

import scala.xml.{Node, NodeSeq}

import org.apache.spark.ui.{UIUtils, WebUIPage}

private[ui] class MemoryPage(parent: MemoryTab) extends WebUIPage("") {
  private val memoryListener = parent.memoryListener
  private val progressListener = parent.progressListener

  def render(request: HttpServletRequest): Seq[Node] = {

    val activeExecutorIdToMem = memoryListener.activeExecutorIdToMem
    val removedExecutorIdToMem = memoryListener.removedExecutorIdToMem
    val completedStages = progressListener.completedStages.reverse.toSeq
    val failedStages = progressListener.failedStages.reverse.toSeq
    val numberCompletedStages = progressListener.numCompletedStages
    val numberFailedStages = progressListener.numFailedStages
    val activeMemInfoSorted = activeExecutorIdToMem.toSeq.sortBy(_._1)
    val removedMemInfoSorted = removedExecutorIdToMem.toSeq.sortBy(_._1)
    val shouldShowActiveExecutors = activeExecutorIdToMem.nonEmpty
    val shouldShowRemovedExecutors = removedExecutorIdToMem.nonEmpty
    val shouldShowCompletedStages = completedStages.nonEmpty
    val shouldShowFailedStages = failedStages.nonEmpty

    val activeExecMemTable = new MemTableBase(activeMemInfoSorted, memoryListener)
    val removedExecMemTable = new MemTableBase(removedMemInfoSorted, memoryListener)
    val completedStagesTable = new StagesTableBase(
      completedStages, parent.basePath, progressListener)
    val failedStagesTable = new StagesTableBase(failedStages, parent.basePath, progressListener)

    val summary: NodeSeq =
      <div>
        <ul class="unstyled">
          {
            if (shouldShowActiveExecutors) {
              <li>
                <a href="#activeExec"><strong>Active Executors:</strong></a>
                {activeExecutorIdToMem.size}
              </li>
            }
          }
          {
            if (shouldShowRemovedExecutors) {
              <li>
                <a href="#removedExec"><strong>Removed Executors:</strong></a>
                {removedExecutorIdToMem.size}
              </li>
            }
          }
          {
            if (shouldShowCompletedStages) {
              <li>
                <a href="#completedStages"><strong>Completed Stages:</strong></a>
                {numberCompletedStages}
              </li>
            }
          }
          {
            if (shouldShowFailedStages) {
              <li>
                <a href="#failedStages"><strong>Failed Stages:</strong></a>
                {numberFailedStages}
              </li>
            }
          }
        </ul>
      </div>

    var content = summary
    if (shouldShowActiveExecutors) {
      content ++= <h4 id="activeExec">Active Executors ({activeExecutorIdToMem.size})</h4> ++
      activeExecMemTable.toNodeSeq
    }
    if (shouldShowRemovedExecutors) {
      content ++= <h4 id="RemovedExec">Removed Executors ({removedMemInfoSorted.size})</h4> ++
      removedExecMemTable.toNodeSeq
    }
    if (shouldShowCompletedStages) {
      content ++= <h4 id="completedStages">Completed Stages ({numberCompletedStages})</h4> ++
      completedStagesTable.toNodeSeq
    }
    if (shouldShowFailedStages) {
      content ++= <h4 id="failedStages">Failed Stages ({numberFailedStages})</h4> ++
      failedStagesTable.toNodeSeq
    }

    UIUtils.headerSparkPage("Memory Usage", content, parent)
  }
}
