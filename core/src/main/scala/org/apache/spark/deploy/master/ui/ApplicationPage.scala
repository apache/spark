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

package org.apache.spark.deploy.master.ui

import scala.xml.Node

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.deploy.DeployMessages.{MasterStateResponse, RequestMasterState}
import org.apache.spark.deploy.ExecutorState
import org.apache.spark.deploy.StandaloneResourceUtils.{formatResourceRequirements, formatResourcesAddresses}
import org.apache.spark.deploy.master.ExecutorDesc
import org.apache.spark.ui.{ToolTips, UIUtils, WebUIPage}
import org.apache.spark.util.Utils

private[ui] class ApplicationPage(parent: MasterWebUI) extends WebUIPage("app") {

  private val master = parent.masterEndpointRef

  /** Executor details for a particular application */
  def render(request: HttpServletRequest): Seq[Node] = {
    val appId = request.getParameter("appId")
    val state = master.askSync[MasterStateResponse](RequestMasterState)
    val app = state.activeApps.find(_.id == appId)
      .getOrElse(state.completedApps.find(_.id == appId).orNull)
    if (app == null) {
      val msg = <div class="row">No running application with ID {appId}</div>
      return UIUtils.basicSparkPage(request, msg, "Not Found")
    }

    val executorHeaders = Seq("ExecutorID", "Worker", "Cores", "Memory", "Resource Profile Id",
      "Resources", "State", "Logs")
    val allExecutors = (app.executors.values ++ app.removedExecutors).toSet.toSeq
    // This includes executors that are either still running or have exited cleanly
    val executors = allExecutors.filter { exec =>
      !ExecutorState.isFinished(exec.state) || exec.state == ExecutorState.EXITED
    }
    val removedExecutors = allExecutors.diff(executors)
    val executorsTable = UIUtils.listingTable(executorHeaders, executorRow, executors)
    val removedExecutorsTable = UIUtils.listingTable(executorHeaders, executorRow, removedExecutors)

    val content =
      <div class="row">
        <div class="col-12">
          <ul class="list-unstyled">
            <li><strong>ID:</strong> {app.id}</li>
            <li><strong>Name:</strong> {app.desc.name}</li>
            <li><strong>User:</strong> {app.desc.user}</li>
            <li><strong>Cores:</strong>
            {
              if (app.desc.maxCores.isEmpty) {
                "Unlimited (%s granted)".format(app.coresGranted)
              } else {
                "%s (%s granted, %s left)".format(
                  app.desc.maxCores.get, app.coresGranted, app.coresLeft)
              }
            }
            </li>
            <li>
              <span data-toggle="tooltip" title={ToolTips.APPLICATION_EXECUTOR_LIMIT}
                    data-placement="top">
                <strong>Executor Limit: </strong>
                {
                  if (app.getExecutorLimit == Int.MaxValue) "Unlimited" else app.getExecutorLimit
                }
                ({app.executors.size} granted)
              </span>
            </li>
            <li>
              <strong>Executor Memory - Default Resource Profile:</strong>
              {Utils.megabytesToString(app.desc.memoryPerExecutorMB)}
            </li>
            <li>
              <strong>Executor Resources - Default Resource Profile:</strong>
              {formatResourceRequirements(app.desc.resourceReqsPerExecutor)}
            </li>
            <li><strong>Submit Date:</strong> {UIUtils.formatDate(app.submitDate)}</li>
            <li><strong>Duration:</strong> {UIUtils.formatDuration(app.duration)}</li>
            <li><strong>State:</strong> {app.state}</li>
            {
              if (!app.isFinished) {
                if (app.desc.appUiUrl.isBlank()) {
                  <li><strong>Application UI:</strong> Disabled</li>
                } else {
                  <li><strong>
                      <a href={UIUtils.makeHref(parent.master.reverseProxy,
                        app.id, app.desc.appUiUrl)}>Application Detail UI</a>
                  </strong></li>
                }
              } else if (parent.master.historyServerUrl.nonEmpty) {
                <li><strong>
                    <a href={s"${parent.master.historyServerUrl.get}/history/${app.id}"}>
                      Application History UI</a>
                </strong></li>
              }
            }
          </ul>
        </div>
      </div>

      <div class="row"> <!-- Executors -->
        <div class="col-12">
          <span class="collapse-aggregated-executors collapse-table"
              onClick="collapseTable('collapse-aggregated-executors','aggregated-executors')">
            <h4>
              <span class="collapse-table-arrow arrow-open"></span>
              <a>Executor Summary ({allExecutors.length})</a>
            </h4>
          </span>
          <div class="aggregated-executors collapsible-table">
            {executorsTable}
          </div>
          {
            if (removedExecutors.nonEmpty) {
              <span class="collapse-aggregated-removedExecutors collapse-table"
                  onClick="collapseTable('collapse-aggregated-removedExecutors',
                  'aggregated-removedExecutors')">
                <h4>
                  <span class="collapse-table-arrow arrow-open"></span>
                  <a>Removed Executors ({removedExecutors.length})</a>
                </h4>
              </span> ++
              <div class="aggregated-removedExecutors collapsible-table">
                {removedExecutorsTable}
              </div>
            }
          }
        </div>
      </div>;
    UIUtils.basicSparkPage(request, content, "Application: " + app.desc.name)
  }

  private def executorRow(executor: ExecutorDesc): Seq[Node] = {
    val workerUrlRef = UIUtils.makeHref(parent.master.reverseProxy,
      executor.worker.id, executor.worker.webUiAddress)
    <tr>
      <td>{executor.id}</td>
      <td>
        <a href={workerUrlRef}>{executor.worker.id}</a>
      </td>
      <td>{executor.cores}</td>
      <td>{executor.memory}</td>
      <td>{executor.rpId}</td>
      <td>{formatResourcesAddresses(executor.resources)}</td>
      <td>{executor.state}</td>
      <td>
        <a href={s"$workerUrlRef/logPage/?appId=${executor.application.id}&executorId=${executor.
          id}&logType=stdout"}>stdout</a>
        <a href={s"$workerUrlRef/logPage/?appId=${executor.application.id}&executorId=${executor.
          id}&logType=stderr"}>stderr</a>
      </td>
    </tr>
  }
}
