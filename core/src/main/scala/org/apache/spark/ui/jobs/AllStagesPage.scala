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

import scala.xml.{Attribute, Elem, Node, NodeSeq, Null, Text}

import org.apache.spark.scheduler.Schedulable
import org.apache.spark.status.PoolData
import org.apache.spark.status.api.v1.StageStatus
import org.apache.spark.ui.{UIUtils, WebUIPage}

/** Page showing list of all ongoing and recently finished stages and pools */
private[ui] class AllStagesPage(parent: StagesTab) extends WebUIPage("") {
  private val sc = parent.sc
  private lazy val allStages = parent.store.stageList(null)
  private lazy val appSummary = parent.store.appSummary()
  private val subPath = "stages"
  private def isFairScheduler = parent.isFairScheduler

  def render(request: HttpServletRequest): Seq[Node] = {
    // For now, pool information is only accessible in live UIs
    val pools = sc.map(_.getAllPools).getOrElse(Seq.empty[Schedulable]).map { pool =>
      val uiPool = parent.store.asOption(parent.store.pool(pool.name)).getOrElse(
        new PoolData(pool.name, Set()))
      pool -> uiPool
    }.toMap
    val poolTable = new PoolTable(pools, parent)

    val allStatuses = Seq(StageStatus.ACTIVE, StageStatus.PENDING, StageStatus.COMPLETE,
      StageStatus.SKIPPED, StageStatus.FAILED)

    val (summaries, tables) = allStatuses.map(summaryAndTableForStatus(_, request)).unzip

    val summary: NodeSeq =
      <div>
        <ul class="unstyled">
          {summaries.flatten}
        </ul>
      </div>

    var content: NodeSeq = summary ++
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

    tables.flatten.foreach(content ++= _)

    UIUtils.headerSparkPage("Stages for All Jobs", content, parent)
  }

  def summaryAndTableForStatus(
      status: StageStatus,
      request: HttpServletRequest): (Option[Elem], Option[NodeSeq]) = {
    val stages = if (status == StageStatus.FAILED) {
      allStages.filter(_.status == status).reverse
    } else {
      allStages.filter(_.status == status)
    }

    if (stages.isEmpty) {
      (None, None)
    } else {
      val killEnabled = status == StageStatus.ACTIVE && parent.killEnabled
      val isFailedStage = status == StageStatus.FAILED

      val stagesTable =
        new StageTableBase(parent.store, request, stages, tableHeaderID(status), stageTag(status),
          parent.basePath, subPath, parent.isFairScheduler, killEnabled, isFailedStage)
      val stagesSize = stages.size
      (Some(summary(status, stagesSize)), Some(table(status, stagesTable, stagesSize)))
    }
  }

  private def tableHeaderID(status: StageStatus): String = status match {
    case StageStatus.ACTIVE => "active"
    case StageStatus.COMPLETE => "completed"
    case StageStatus.FAILED => "failed"
    case StageStatus.PENDING => "pending"
    case StageStatus.SKIPPED => "skipped"
  }

  private def stageTag(status: StageStatus): String = status match {
    case StageStatus.ACTIVE => "activeStage"
    case StageStatus.COMPLETE => "completedStage"
    case StageStatus.FAILED => "failedStage"
    case StageStatus.PENDING => "pendingStage"
    case StageStatus.SKIPPED => "skippedStage"
  }

  private def headerDescription(status: StageStatus): String = status match {
    case StageStatus.ACTIVE => "Active"
    case StageStatus.COMPLETE => "Completed"
    case StageStatus.FAILED => "Failed"
    case StageStatus.PENDING => "Pending"
    case StageStatus.SKIPPED => "Skipped"
  }

  private def classSuffix(status: StageStatus): String = status match {
    case StageStatus.ACTIVE => "ActiveStages"
    case StageStatus.COMPLETE => "CompletedStages"
    case StageStatus.FAILED => "FailedStages"
    case StageStatus.PENDING => "PendingStages"
    case StageStatus.SKIPPED => "SkippedStages"
  }

  private def summaryContent(status: StageStatus, size: Int): String = {
    if (status == StageStatus.COMPLETE
        && appSummary.numCompletedStages != size) {
      s"${appSummary.numCompletedStages}, only showing $size"
    } else {
      s"$size"
    }
  }

  private def summary(status: StageStatus, size: Int): Elem = {
    val summary =
      <li>
        <a href={s"#${tableHeaderID(status)}"}>
          <strong>{headerDescription(status)} Stages:</strong>
        </a>
        {summaryContent(status, size)}
      </li>

    if (status == StageStatus.COMPLETE) {
      summary % Attribute(None, "id", Text("completed-summary"), Null)
    } else {
      summary
    }
  }

  private def table(status: StageStatus, stagesTable: StageTableBase, size: Int): NodeSeq = {
    val classSuffixStatus = classSuffix(status)
    <span id={tableHeaderID(status)}
          class={s"collapse-aggregated-all$classSuffixStatus collapse-table"}
          onClick={s"collapseTable('collapse-aggregated-all$classSuffixStatus'," +
            s" 'aggregated-all$classSuffixStatus')"}>
      <h4>
        <span class="collapse-table-arrow arrow-open"></span>
        <a>{headerDescription(status)} Stages ({summaryContent(status, size)})</a>
      </h4>
    </span> ++
      <div class={s"aggregated-all$classSuffixStatus collapsible-table"}>
        {stagesTable.toNodeSeq}
      </div>
  }
}
