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

import scala.xml.{Node, NodeSeq}

import org.apache.spark.scheduler.Schedulable
import org.apache.spark.ui.{WebUIPage, UIUtils}

/** Page showing list of all ongoing and recently finished stages and pools */
private[ui] class JobProgressPage(parent: JobProgressTab) extends WebUIPage("") {
  private val sc = parent.sc
  private val listener = parent.listener
  private def isFairScheduler = parent.isFairScheduler

  def render(request: HttpServletRequest): Seq[Node] = {
    listener.synchronized {
      val activeStages = listener.activeStages.values.toSeq
      val completedStages = listener.completedStages.reverse.toSeq
      val numCompletedStages = listener.numCompletedStages
      val failedStages = listener.failedStages.reverse.toSeq
      val numFailedStages = listener.numFailedStages
      val now = System.currentTimeMillis

      val activeStagesTable =
        new StageTableBase(activeStages.sortBy(_.submissionTime).reverse,
          parent, parent.killEnabled)
      val completedStagesTable =
        new StageTableBase(completedStages.sortBy(_.submissionTime).reverse, parent)
      val failedStagesTable =
        new FailedStageTable(failedStages.sortBy(_.submissionTime).reverse, parent)

      // For now, pool information is only accessible in live UIs
      val pools = sc.map(_.getAllPools).getOrElse(Seq.empty[Schedulable])
      val poolTable = new PoolTable(pools, parent)

      val summary: NodeSeq =
        <div>
          <ul class="unstyled">
            {if (sc.isDefined) {
              // Total duration is not meaningful unless the UI is live
              <li>
                <strong>Total Duration: </strong>
                {UIUtils.formatDuration(now - sc.get.startTime)}
              </li>
            }}
            <li>
              <strong>Scheduling Mode: </strong>
              {listener.schedulingMode.map(_.toString).getOrElse("Unknown")}
            </li>
            <li>
              <a href="#active"><strong>Active Stages:</strong></a>
              {activeStages.size}
            </li>
            <li>
              <a href="#completed"><strong>Completed Stages:</strong></a>
              {numCompletedStages}
            </li>
             <li>
             <a href="#failed"><strong>Failed Stages:</strong></a>
              {numFailedStages}
            </li>
          </ul>
        </div>

      val content = summary ++
        {if (sc.isDefined && isFairScheduler) {
          <h4>{pools.size} Fair Scheduler Pools</h4> ++ poolTable.toNodeSeq
        } else {
          Seq[Node]()
        }} ++
        <h4 id="active">Active Stages ({activeStages.size})</h4> ++
        activeStagesTable.toNodeSeq ++
        <h4 id="completed">Completed Stages ({numCompletedStages})</h4> ++
        completedStagesTable.toNodeSeq ++
        <h4 id ="failed">Failed Stages ({numFailedStages})</h4> ++
        failedStagesTable.toNodeSeq

      UIUtils.headerSparkPage("Spark Stages", content, parent)
    }
  }
}
