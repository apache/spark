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

import javax.servlet.http.HttpServletRequest

import scala.xml.{NodeSeq, Node}

import org.apache.spark.scheduler.SchedulingMode
import org.apache.spark.ui.Page._
import org.apache.spark.ui.UIUtils._


/** Page showing list of all ongoing and recently finished stages and pools*/
private[spark] class IndexPage(parent: JobProgressUI) {
  def listener = parent.listener

  def render(request: HttpServletRequest): Seq[Node] = {
    listener.synchronized {
      val activeStages = listener.activeStages.toSeq
      val completedStages = listener.completedStages.reverse.toSeq
      val failedStages = listener.failedStages.reverse.toSeq
      val now = System.currentTimeMillis()

      var activeTime = 0L
      for (tasks <- listener.stageIdToTasksActive.values; t <- tasks) {
        activeTime += t.timeRunning(now)
      }

      val activeStagesTable = new StageTable(activeStages.sortBy(_.submissionTime).reverse, parent)
      val completedStagesTable = new StageTable(completedStages.sortBy(_.submissionTime).reverse,
        parent)
      val failedStagesTable = new StageTable(failedStages.sortBy(_.submissionTime).reverse, parent)

      val pools = listener.sc.getAllPools
      val poolTable = new PoolTable(pools, listener)
      val summary: NodeSeq =
       <div>
         <ul class="unstyled">
           <li>
             <strong>Total Duration: </strong>
             {parent.formatDuration(now - listener.sc.startTime)}
           </li>
           <li><strong>Scheduling Mode:</strong> {parent.sc.getSchedulingMode}</li>
           <li>
             <a href="#active"><strong>Active Stages:</strong></a>
             {activeStages.size}
           </li>
           <li>
             <a href="#completed"><strong>Completed Stages:</strong></a>
             {completedStages.size}
           </li>
           <li>
             <a href="#failed"><strong>Failed Stages:</strong></a>
             {failedStages.size}
           </li>
         </ul>
       </div>

      val content = summary ++
        {if (listener.sc.getSchedulingMode == SchedulingMode.FAIR) {
           <h4>{pools.size} Fair Scheduler Pools</h4> ++ poolTable.toNodeSeq
        } else {
          Seq()
        }} ++
        <h4 id="active">Active Stages ({activeStages.size})</h4> ++
        activeStagesTable.toNodeSeq++
        <h4 id="completed">Completed Stages ({completedStages.size})</h4> ++
        completedStagesTable.toNodeSeq++
        <h4 id ="failed">Failed Stages ({failedStages.size})</h4> ++
        failedStagesTable.toNodeSeq

      headerSparkPage(content, parent.sc, "Spark Stages", Stages)
    }
  }
}
