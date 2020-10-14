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

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

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

    val executorHeaders = Seq("ExecutorID", "Worker", "Cores", "Memory", "Resources",
      "State", "Logs")
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
                  if (app.executorLimit == Int.MaxValue) "Unlimited" else app.executorLimit
                }
                ({app.executors.size} granted)
              </span>
            </li>
            <li>
              <strong>Executor Memory:</strong>
              {Utils.megabytesToString(app.desc.memoryPerExecutorMB)}
            </li>
            <li>
              <strong>Executor Resources:</strong>
              {formatResourceRequirements(app.desc.resourceReqsPerExecutor)}
            </li>
            <li><strong>Submit Date:</strong> {UIUtils.formatDate(app.submitDate)}</li>
            <li><strong>State:</strong> {app.state}</li>
            {
              if (!app.isFinished) {
                <li><strong>
                    <a href={UIUtils.makeHref(parent.master.reverseProxy,
                      app.id, app.desc.appUiUrl)}>Application Detail UI</a>
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
      <td>{formatResourcesAddresses(executor.resources)}</td>
      <td>{executor.state}</td>
      <td>
        <a href={s"$workerUrlRef/logPage?appId=${executor.application.id}&executorId=${executor.
          id}&logType=stdout"}>stdout</a>
        <a href={s"$workerUrlRef/logPage?appId=${executor.application.id}&executorId=${executor.
          id}&logType=stderr"}>stderr</a>
      </td>
    </tr>
  }
}
