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

package spark.ui.jobs

import javax.servlet.http.HttpServletRequest

import scala.xml.{NodeSeq, Node}

import spark.scheduler.cluster.SchedulingMode
import spark.ui.Page._
import spark.ui.UIUtils._
import spark.Utils

/** Page showing list of all ongoing and recently finished stages and pools*/
private[spark] class IndexPage(parent: JobProgressUI) {
  def listener = parent.listener

  def render(request: HttpServletRequest): Seq[Node] = {
    val activeStages = listener.activeStages.toSeq
    val completedStages = listener.completedStages.reverse.toSeq
    val failedStages = listener.failedStages.reverse.toSeq
    val now = System.currentTimeMillis()

    var activeTime = 0L
    for (tasks <- listener.stageToTasksActive.values; t <- tasks) {
      activeTime += t.timeRunning(now)
    }

    val activeStagesTable = new StageTable(activeStages, parent)
    val completedStagesTable = new StageTable(completedStages, parent)
    val failedStagesTable = new StageTable(failedStages, parent)

    val poolTable = new PoolTable(listener.sc.getAllPools, listener)
    val summary: NodeSeq =
     <div>
       <ul class="unstyled">
          <li>
            <strong>CPU time: </strong>
            {parent.formatDuration(listener.totalTime + activeTime)}
          </li>
         {if (listener.totalShuffleRead > 0)
           <li>
              <strong>Shuffle read: </strong>
              {Utils.memoryBytesToString(listener.totalShuffleRead)}
            </li>
         }
         {if (listener.totalShuffleWrite > 0)
           <li>
              <strong>Shuffle write: </strong>
              {Utils.memoryBytesToString(listener.totalShuffleWrite)}
            </li>
         }
          <li><strong>Active Stages Number:</strong> {activeStages.size} </li>
          <li><strong>Completed Stages Number:</strong> {completedStages.size} </li>
          <li><strong>Failed Stages Number:</strong> {failedStages.size} </li>
          <li><strong>Scheduling Mode:</strong> {parent.sc.getSchedulingMode}</li>

       </ul>
     </div>

    val content = summary ++ 
      {if (listener.sc.getSchedulingMode == SchedulingMode.FAIR) {
         <h3>Pools</h3> ++ poolTable.toNodeSeq
      } else {
        Seq()
      }} ++
      <h3>Active Stages : {activeStages.size}</h3> ++
      activeStagesTable.toNodeSeq++
      <h3>Completed Stages : {completedStages.size}</h3> ++
      completedStagesTable.toNodeSeq++
      <h3>Failed Stages : {failedStages.size}</h3> ++
      failedStagesTable.toNodeSeq

    headerSparkPage(content, parent.sc, "Spark Stages", Jobs)
  }
}
