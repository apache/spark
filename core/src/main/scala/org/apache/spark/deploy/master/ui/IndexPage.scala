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

import scala.concurrent.Await
import scala.xml.Node

import akka.pattern.ask
import javax.servlet.http.HttpServletRequest
import net.liftweb.json.JsonAST.JValue

import org.apache.spark.deploy.{DeployWebUI, JsonProtocol}
import org.apache.spark.deploy.DeployMessages.{MasterStateResponse, RequestMasterState}
import org.apache.spark.deploy.master.{ApplicationInfo, WorkerInfo}
import org.apache.spark.ui.UIUtils
import org.apache.spark.util.Utils

private[spark] class IndexPage(parent: MasterWebUI) {
  val master = parent.masterActorRef
  val timeout = parent.timeout

  def renderJson(request: HttpServletRequest): JValue = {
    val stateFuture = (master ? RequestMasterState)(timeout).mapTo[MasterStateResponse]
    val state = Await.result(stateFuture, timeout)
    JsonProtocol.writeMasterState(state)
  }

  /** Index view listing applications and executors */
  def render(request: HttpServletRequest): Seq[Node] = {
    val stateFuture = (master ? RequestMasterState)(timeout).mapTo[MasterStateResponse]
    val state = Await.result(stateFuture, timeout)

    val workerHeaders = Seq("Id", "Address", "State", "Cores", "Memory")
    val workers = state.workers.sortBy(_.id)
    val workerTable = UIUtils.listingTable(workerHeaders, workerRow, workers)

    val appHeaders = Seq("ID", "Name", "Cores", "Memory per Node", "Submitted Time", "User",
      "State", "Duration")
    val activeApps = state.activeApps.sortBy(_.startTime).reverse
    val activeAppsTable = UIUtils.listingTable(appHeaders, appRow, activeApps)
    val completedApps = state.completedApps.sortBy(_.endTime).reverse
    val completedAppsTable = UIUtils.listingTable(appHeaders, appRow, completedApps)

    val content =
        <div class="row-fluid">
          <div class="span12">
            <ul class="unstyled">
              <li><strong>URL:</strong> {state.uri}</li>
              <li><strong>Workers:</strong> {state.workers.size}</li>
              <li><strong>Cores:</strong> {state.workers.map(_.cores).sum} Total,
                {state.workers.map(_.coresUsed).sum} Used</li>
              <li><strong>Memory:</strong>
                {Utils.megabytesToString(state.workers.map(_.memory).sum)} Total,
                {Utils.megabytesToString(state.workers.map(_.memoryUsed).sum)} Used</li>
              <li><strong>Applications:</strong>
                {state.activeApps.size} Running,
                {state.completedApps.size} Completed </li>
            </ul>
          </div>
        </div>

        <div class="row-fluid">
          <div class="span12">
            <h4> Workers </h4>
            {workerTable}
          </div>
        </div>

        <div class="row-fluid">
          <div class="span12">
            <h4> Running Applications </h4>

            {activeAppsTable}
          </div>
        </div>

        <div class="row-fluid">
          <div class="span12">
            <h4> Completed Applications </h4>
            {completedAppsTable}
          </div>
        </div>;
    UIUtils.basicSparkPage(content, "Spark Master at " + state.uri)
  }

  def workerRow(worker: WorkerInfo): Seq[Node] = {
    <tr>
      <td>
        <a href={worker.webUiAddress}>{worker.id}</a>
      </td>
      <td>{worker.host}:{worker.port}</td>
      <td>{worker.state}</td>
      <td>{worker.cores} ({worker.coresUsed} Used)</td>
      <td sorttable_customkey={"%s.%s".format(worker.memory, worker.memoryUsed)}>
        {Utils.megabytesToString(worker.memory)}
        ({Utils.megabytesToString(worker.memoryUsed)} Used)
      </td>
    </tr>
  }


  def appRow(app: ApplicationInfo): Seq[Node] = {
    <tr>
      <td>
        <a href={"app?appId=" + app.id}>{app.id}</a>
      </td>
      <td>
        <a href={app.appUiUrl}>{app.desc.name}</a>
      </td>
      <td>
        {app.coresGranted}
      </td>
      <td sorttable_customkey={app.desc.memoryPerSlave.toString}>
        {Utils.megabytesToString(app.desc.memoryPerSlave)}
      </td>
      <td>{DeployWebUI.formatDate(app.submitDate)}</td>
      <td>{app.desc.user}</td>
      <td>{app.state.toString}</td>
      <td>{DeployWebUI.formatDuration(app.duration)}</td>
    </tr>
  }
}
