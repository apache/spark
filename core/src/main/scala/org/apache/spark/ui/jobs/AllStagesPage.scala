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
import org.apache.spark.status.PoolData
import org.apache.spark.status.api.v1._
import org.apache.spark.ui.{UIUtils, WebUIPage}

/** Page showing list of all ongoing and recently finished stages and pools */
private[ui] class AllStagesPage(parent: StagesTab) extends WebUIPage("") {
  private val sc = parent.sc
  private def isFairScheduler = parent.isFairScheduler

  def render(request: HttpServletRequest): Seq[Node] = {
    val allStages = parent.store.stageList(null)

    val activeStages = allStages.filter(_.status == StageStatus.ACTIVE)
    val pendingStages = allStages.filter(_.status == StageStatus.PENDING)
    val completedStages = allStages.filter(_.status == StageStatus.COMPLETE)
    val failedStages = allStages.filter(_.status == StageStatus.FAILED).reverse

    val numFailedStages = failedStages.size
    val subPath = "stages"

    val activeStagesTable =
      new StageTableBase(parent.store, request, activeStages, "active", "activeStage",
        parent.basePath, subPath, parent.isFairScheduler, parent.killEnabled, false)
    val pendingStagesTable =
      new StageTableBase(parent.store, request, pendingStages, "pending", "pendingStage",
        parent.basePath, subPath, parent.isFairScheduler, false, false)
    val completedStagesTable =
      new StageTableBase(parent.store, request, completedStages, "completed", "completedStage",
        parent.basePath, subPath, parent.isFairScheduler, false, false)
    val failedStagesTable =
      new StageTableBase(parent.store, request, failedStages, "failed", "failedStage",
        parent.basePath, subPath, parent.isFairScheduler, false, true)

    // For now, pool information is only accessible in live UIs
    val pools = sc.map(_.getAllPools).getOrElse(Seq.empty[Schedulable]).map { pool =>
      val uiPool = parent.store.asOption(parent.store.pool(pool.name)).getOrElse(
        new PoolData(pool.name, Set()))
      pool -> uiPool
    }.toMap
    val poolTable = new PoolTable(pools, parent)

    val shouldShowActiveStages = activeStages.nonEmpty
    val shouldShowPendingStages = pendingStages.nonEmpty
    val shouldShowCompletedStages = completedStages.nonEmpty
    val shouldShowFailedStages = failedStages.nonEmpty

    val appSummary = parent.store.appSummary()
    val completedStageNumStr = if (appSummary.numCompletedStages == completedStages.size) {
      s"${appSummary.numCompletedStages}"
    } else {
      s"${appSummary.numCompletedStages}, only showing ${completedStages.size}"
    }

    val summary: NodeSeq =
      <div>
        <ul class="unstyled">
          {
            if (shouldShowActiveStages) {
              <li>
                <a href="#active"><strong>Active Stages:</strong></a>
                {activeStages.size}
              </li>
            }
          }
          {
            if (shouldShowPendingStages) {
              <li>
                <a href="#pending"><strong>Pending Stages:</strong></a>
                {pendingStages.size}
              </li>
            }
          }
          {
            if (shouldShowCompletedStages) {
              <li id="completed-summary">
                <a href="#completed"><strong>Completed Stages:</strong></a>
                {completedStageNumStr}
              </li>
            }
          }
          {
            if (shouldShowFailedStages) {
              <li>
                <a href="#failed"><strong>Failed Stages:</strong></a>
                {numFailedStages}
              </li>
            }
          }
        </ul>
      </div>

    var content = summary ++
      {
        if (sc.isDefined && isFairScheduler) {
          <span class="collapse-aggregated-poolTable collapse-table"
              onClick="collapseTable('collapse-aggregated-poolTable','aggregated-poolTable')">
            <h4>
              <span class="collapse-table-arrow arrow-open"></span>
              <a>Fair Scheduler Pools ({pools.size})</a>
            </h4>
          </span> ++
          <div class="aggregated-poolTable collapsible-table">
            {poolTable.toNodeSeq}
          </div>
        } else {
          Seq.empty[Node]
        }
      }
    if (shouldShowActiveStages) {
      content ++=
        <span id="active" class="collapse-aggregated-activeStages collapse-table"
            onClick="collapseTable('collapse-aggregated-activeStages','aggregated-activeStages')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Active Stages ({activeStages.size})</a>
          </h4>
        </span> ++
        <div class="aggregated-activeStages collapsible-table">
          {activeStagesTable.toNodeSeq}
        </div>
    }
    if (shouldShowPendingStages) {
      content ++=
        <span id="pending" class="collapse-aggregated-pendingStages collapse-table"
            onClick="collapseTable('collapse-aggregated-pendingStages','aggregated-pendingStages')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Pending Stages ({pendingStages.size})</a>
          </h4>
        </span> ++
        <div class="aggregated-pendingStages collapsible-table">
          {pendingStagesTable.toNodeSeq}
        </div>
    }
    if (shouldShowCompletedStages) {
      content ++=
        <span id="completed" class="collapse-aggregated-completedStages collapse-table"
            onClick="collapseTable('collapse-aggregated-completedStages',
            'aggregated-completedStages')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Completed Stages ({completedStageNumStr})</a>
          </h4>
        </span> ++
        <div class="aggregated-completedStages collapsible-table">
          {completedStagesTable.toNodeSeq}
        </div>
    }
    if (shouldShowFailedStages) {
      content ++=
        <span id ="failed" class="collapse-aggregated-failedStages collapse-table"
            onClick="collapseTable('collapse-aggregated-failedStages',
            'aggregated-failedStages')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Failed Stages ({numFailedStages})</a>
          </h4>
        </span> ++
        <div class="aggregated-failedStages collapsible-table">
          {failedStagesTable.toNodeSeq}
        </div>
    }
    UIUtils.headerSparkPage("Stages for All Jobs", content, parent)
  }
}
