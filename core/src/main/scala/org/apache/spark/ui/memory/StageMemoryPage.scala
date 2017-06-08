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

import scala.xml.Node

import org.apache.spark.ui.{UIUtils, WebUIPage}

/** Page showing memory information for a given stage */
private[ui] class StageMemoryPage(parent: MemoryTab) extends WebUIPage("stage") {
  private val memoryListener = parent.memoryListener

  def render(request: HttpServletRequest): Seq[Node] = {
    memoryListener.synchronized {
      val parameterId = request.getParameter("id")
      require(parameterId != null && parameterId.nonEmpty, "Missing id parameter")

      val parameterAttempt = request.getParameter("attempt")
      require(parameterAttempt != null && parameterAttempt.nonEmpty, "Missing attempt parameter")

      val stage = (parameterId.toInt, parameterAttempt.toInt)

      val finishedStageToMem = memoryListener.completedStagesToMem
      val content = if (finishedStageToMem.get(stage).isDefined) {
        val executorIdToMem = finishedStageToMem(stage).toSeq.sortBy(_._1)
        val execMemTable = new MemTableBase(executorIdToMem, memoryListener)
        <h4 id="activeExec">Executors ({executorIdToMem.size})</h4> ++
          execMemTable.toNodeSeq
      } else {
        Seq.empty
      }
      UIUtils.headerSparkPage("Stage Detail Memory Usage", content, parent)
    }
  }
}
