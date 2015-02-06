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

import scala.concurrent.Await
import scala.xml.Node

import akka.pattern.ask
import org.json4s.JValue

import org.apache.spark.deploy.JsonProtocol
import org.apache.spark.deploy.DeployMessages.{MasterStateResponse, RequestMasterState}
import org.apache.spark.deploy.master.{ApplicationInfo, DriverInfo, WorkerInfo}
import org.apache.spark.ui.{WebUIPage, UIUtils}
import org.apache.spark.util.Utils

private[spark] class MasterPage(parent: MasterWebUI) extends WebUIPage("") {
  private val master = parent.masterActorRef
  private val timeout = parent.timeout

  override def renderJson(request: HttpServletRequest): JValue = {
    val stateFuture = (master ? RequestMasterState)(timeout).mapTo[MasterStateResponse]
    val state = Await.result(stateFuture, timeout)
    JsonProtocol.writeMasterState(state)
  }

  /** Index view listing applications and executors */
  def render(request: HttpServletRequest): Seq[Node] = {
    val stateFuture = (master ? RequestMasterState)(timeout).mapTo[MasterStateResponse]
    val state = Await.result(stateFuture, timeout)

    val workerHeaders = Seq("Worker Id", "Address", "State", "Cores", "Memory")
    val workers = state.workers.sortBy(_.id)
    val workerTable = UIUtils.listingTable(workerHeaders, workerRow, workers)

    val appHeaders = Seq("Application ID", "Name", "Cores", "Memory per Node", "Submitted Time",
      "User", "State", "Duration")
    val activeApps = state.activeApps.sortBy(_.startTime).reverse
    val activeAppsTable = UIUtils.listingTable(appHeaders, appRow, activeApps)
    val completedApps = state.completedApps.sortBy(_.endTime).reverse
    val completedAppsTable = UIUtils.listingTable(appHeaders, appRow, completedApps)

    val driverHeaders = Seq("Submission ID", "Submitted Time", "Worker", "State", "Cores",
      "Memory", "Main Class")
    val activeDrivers = state.activeDrivers.sortBy(_.startTime).reverse
    val activeDriversTable = UIUtils.listingTable(driverHeaders, driverRow, activeDrivers)
    val completedDrivers = state.completedDrivers.sortBy(_.startTime).reverse
    val completedDriversTable = UIUtils.listingTable(driverHeaders, driverRow, completedDrivers)

    // For now we only show driver information if the user has submitted drivers to the cluster.
    // This is until we integrate the notion of drivers and applications in the UI.
    def hasDrivers = activeDrivers.length > 0 || completedDrivers.length > 0

    val content =
        <div class="row-fluid">
          <div class="span12">
            <ul class="unstyled">
              <li><strong>URL:</strong> {state.uri}</li>
              {
                state.restUri.map { uri =>
                  <li>
                    <strong>REST URL:</strong> {uri}
                    <span class="rest-uri"> (cluster mode)</span>
                  </li>
                }.getOrElse { Seq.empty }
              }
              <li><strong>Workers:</strong> {state.workers.size}</li>
              <li><strong>Cores:</strong> {state.workers.map(_.cores).sum} Total,
                {state.workers.map(_.coresUsed).sum} Used</li>
              <li><strong>Memory:</strong>
                {Utils.megabytesToString(state.workers.map(_.memory).sum)} Total,
                {Utils.megabytesToString(state.workers.map(_.memoryUsed).sum)} Used</li>
              <li><strong>Applications:</strong>
                {state.activeApps.size} Running,
                {state.completedApps.size} Completed </li>
              <li><strong>Drivers:</strong>
                {state.activeDrivers.size} Running,
                {state.completedDrivers.size} Completed </li>
              <li><strong>Status:</strong> {state.status}</li>
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

        <div>
          {if (hasDrivers) {
             <div class="row-fluid">
               <div class="span12">
                 <h4> Running Drivers </h4>
                 {activeDriversTable}
               </div>
             </div>
           }
          }
        </div>

        <div class="row-fluid">
          <div class="span12">
            <h4> Completed Applications </h4>
            {completedAppsTable}
          </div>
        </div>

        <div>
          {
            if (hasDrivers) {
              <div class="row-fluid">
                <div class="span12">
                  <h4> Completed Drivers </h4>
                  {completedDriversTable}
                </div>
              </div>
            }
          }
        </div>;

    UIUtils.basicSparkPage(content, "Spark Master at " + state.uri)
  }

  private def workerRow(worker: WorkerInfo): Seq[Node] = {
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

  private def appRow(app: ApplicationInfo): Seq[Node] = {
    <tr>
      <td>
        <a href={"app?appId=" + app.id}>{app.id}</a>
      </td>
      <td>
        <a href={app.desc.appUiUrl}>{app.desc.name}</a>
      </td>
      <td>
        {app.coresGranted}
      </td>
      <td sorttable_customkey={app.desc.memoryPerSlave.toString}>
        {Utils.megabytesToString(app.desc.memoryPerSlave)}
      </td>
      <td>{UIUtils.formatDate(app.submitDate)}</td>
      <td>{app.desc.user}</td>
      <td>{app.state.toString}</td>
      <td>{UIUtils.formatDuration(app.duration)}</td>
    </tr>
  }

  private def driverRow(driver: DriverInfo): Seq[Node] = {
    <tr>
      <td>{driver.id} </td>
      <td>{driver.submitDate}</td>
      <td>{driver.worker.map(w => <a href={w.webUiAddress}>{w.id.toString}</a>).getOrElse("None")}
      </td>
      <td>{driver.state}</td>
      <td sorttable_customkey={driver.desc.cores.toString}>
        {driver.desc.cores}
      </td>
      <td sorttable_customkey={driver.desc.mem.toString}>
        {Utils.megabytesToString(driver.desc.mem.toLong)}
      </td>
      <td>{driver.desc.command.arguments(1)}</td>
    </tr>
  }
}
