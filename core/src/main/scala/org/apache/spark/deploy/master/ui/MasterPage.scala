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
import org.json4s.JValue

import org.apache.spark.deploy.DeployMessages.{KillDriverResponse, MasterStateResponse, RequestKillDriver, RequestMasterState}
import org.apache.spark.deploy.JsonProtocol
import org.apache.spark.deploy.StandaloneResourceUtils._
import org.apache.spark.deploy.master._
import org.apache.spark.internal.config.UI.MASTER_UI_TITLE
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.util.Utils

private[ui] class MasterPage(parent: MasterWebUI) extends WebUIPage("") {
  private val master = parent.masterEndpointRef
  private val title = parent.master.conf.get(MASTER_UI_TITLE)
  private val jsonFieldPattern = "/json/([a-zA-Z]+).*".r

  def getMasterState: MasterStateResponse = {
    master.askSync[MasterStateResponse](RequestMasterState)
  }

  override def renderJson(request: HttpServletRequest): JValue = {
    jsonFieldPattern.findFirstMatchIn(request.getRequestURI()) match {
      case None => JsonProtocol.writeMasterState(getMasterState)
      case Some(m) if m.group(1) == "clusterutilization" =>
        JsonProtocol.writeClusterUtilization(getMasterState)
      case Some(m) => JsonProtocol.writeMasterState(getMasterState, Some(m.group(1)))
    }
  }

  def handleAppKillRequest(request: HttpServletRequest): Unit = {
    handleKillRequest(request, id => {
      parent.master.idToApp.get(id).foreach { app =>
        parent.master.removeApplication(app, ApplicationState.KILLED)
      }
    })
  }

  def handleDriverKillRequest(request: HttpServletRequest): Unit = {
    handleKillRequest(request, id => {
      master.ask[KillDriverResponse](RequestKillDriver(id))
    })
  }

  private def handleKillRequest(request: HttpServletRequest, action: String => Unit): Unit = {
    if (parent.killEnabled &&
        parent.master.securityMgr.checkModifyPermissions(request.getRemoteUser)) {
      val killFlag = Option(request.getParameter("terminate")).getOrElse("false").toBoolean
      val id = Option(request.getParameter("id"))
      if (id.isDefined && killFlag) {
        action(id.get)
      }

      Thread.sleep(100)
    }
  }

  private def formatWorkerResourcesDetails(worker: WorkerInfo): String = {
    val usedInfo = worker.resourcesInfoUsed
    val freeInfo = worker.resourcesInfoFree
    formatResourcesDetails(usedInfo, freeInfo)
  }

  private def formatMasterResourcesInUse(aliveWorkers: Array[WorkerInfo]): String = {
    val totalInfo = aliveWorkers.map(_.resourcesInfo)
      .flatMap(_.iterator)
      .groupBy(_._1) // group by resource name
      .map { case (rName, rInfoArr) =>
      rName -> rInfoArr.map(_._2.addresses.length).sum
    }
    val usedInfo = aliveWorkers.map(_.resourcesInfoUsed)
      .flatMap(_.iterator)
      .groupBy(_._1) // group by resource name
      .map { case (rName, rInfoArr) =>
      rName -> rInfoArr.map(_._2.addresses.length).sum
    }
    formatResourcesUsed(totalInfo, usedInfo)
  }

  /** Index view listing applications and executors */
  def render(request: HttpServletRequest): Seq[Node] = {
    val state = getMasterState

    val showResourceColumn = state.workers.filter(_.resourcesInfoUsed.nonEmpty).nonEmpty
    val workerHeaders = if (showResourceColumn) {
      Seq("Worker Id", "Address", "State", "Cores", "Memory", "Resources")
    } else {
      Seq("Worker Id", "Address", "State", "Cores", "Memory")
    }
    val workers = state.workers.sortBy(_.id)
    val aliveWorkers = state.workers.filter(_.state == WorkerState.ALIVE)
    val workerTable = UIUtils.listingTable(workerHeaders, workerRow(showResourceColumn), workers)

    val appHeaders = Seq("Application ID", "Name", "Cores", "Memory per Executor",
      "Resources Per Executor", "Submitted Time", "User", "State", "Duration")
    val activeApps = state.activeApps.sortBy(_.startTime).reverse
    val activeAppsTable = UIUtils.listingTable(appHeaders, appRow, activeApps)
    val completedApps = state.completedApps.sortBy(_.endTime).reverse
    val completedAppsTable = UIUtils.listingTable(appHeaders, appRow, completedApps)

    val activeDriverHeaders = Seq("Submission ID", "Submitted Time", "Worker", "State", "Cores",
      "Memory", "Resources", "Main Class", "Duration")
    val activeDrivers = state.activeDrivers.sortBy(_.startTime).reverse
    val activeDriversTable =
      UIUtils.listingTable(activeDriverHeaders, activeDriverRow, activeDrivers)

    val completedDriverHeaders = Seq("Submission ID", "Submitted Time", "Worker", "State", "Cores",
      "Memory", "Resources", "Main Class")
    val completedDrivers = state.completedDrivers.sortBy(_.startTime).reverse
    val completedDriversTable =
      UIUtils.listingTable(completedDriverHeaders, completedDriverRow, completedDrivers)

    // For now we only show driver information if the user has submitted drivers to the cluster.
    // This is until we integrate the notion of drivers and applications in the UI.
    def hasDrivers: Boolean = activeDrivers.length > 0 || completedDrivers.length > 0

    val content =
        <div class="row">
          <div class="col-12">
            <ul class="list-unstyled">
              <li><strong>URL:</strong> {state.uri}</li>
              {
                state.restUri.map { uri =>
                  <li>
                    <strong>REST URL:</strong> {uri}
                    <span class="rest-uri"> (cluster mode)</span>
                  </li>
                }.getOrElse { Seq.empty }
              }
              <li><strong>Workers:</strong> {aliveWorkers.length} Alive,
                {workers.count(_.state == WorkerState.DEAD)} Dead,
                {workers.count(_.state == WorkerState.DECOMMISSIONED)} Decommissioned,
                {workers.count(_.state == WorkerState.UNKNOWN)} Unknown
              </li>
              <li><strong>Cores in use:</strong> {aliveWorkers.map(_.cores).sum} Total,
                {aliveWorkers.map(_.coresUsed).sum} Used</li>
              <li><strong>Memory in use:</strong>
                {Utils.megabytesToString(aliveWorkers.map(_.memory).sum)} Total,
                {Utils.megabytesToString(aliveWorkers.map(_.memoryUsed).sum)} Used</li>
              <li><strong>Resources in use:</strong>
                {formatMasterResourcesInUse(aliveWorkers)}</li>
              <li><strong>Applications:</strong>
                {state.activeApps.length} <a href="#running-app">Running</a>,
                {state.completedApps.length} <a href="#completed-app">Completed</a> </li>
              <li><strong>Drivers:</strong>
                {state.activeDrivers.length} Running
                ({state.activeDrivers.count(_.state == DriverState.SUBMITTED)} Waiting),
                {state.completedDrivers.length} Completed
                ({state.completedDrivers.count(_.state == DriverState.KILLED)} Killed,
                {state.completedDrivers.count(_.state == DriverState.FAILED)} Failed,
                {state.completedDrivers.count(_.state == DriverState.ERROR)} Error,
                {state.completedDrivers.count(_.state == DriverState.RELAUNCHING)} Relaunching)
              </li>
              <li><strong>Status:</strong> {state.status}
                (<a href={"/environment/"}>Environment</a>,
                <a href={"/logPage/?self&logType=out"}>Log</a>)
              </li>
            </ul>
          </div>
        </div>

        <div class="row">
          <div class="col-12">
            <span class="collapse-aggregated-workers collapse-table"
                onClick="collapseTable('collapse-aggregated-workers','aggregated-workers')">
              <h4>
                <span class="collapse-table-arrow arrow-open"></span>
                <a>Workers ({workers.length})</a>
              </h4>
            </span>
            <div class="aggregated-workers collapsible-table">
              {workerTable}
            </div>
          </div>
        </div>

        <div class="row">
          <div class="col-12">
            <span id="running-app" class="collapse-aggregated-activeApps collapse-table"
                onClick="collapseTable('collapse-aggregated-activeApps','aggregated-activeApps')">
              <h4>
                <span class="collapse-table-arrow arrow-open"></span>
                <a>Running Applications ({activeApps.length})</a>
              </h4>
            </span>
            <div class="aggregated-activeApps collapsible-table">
              {activeAppsTable}
            </div>
          </div>
        </div>

        <div>
          {if (hasDrivers) {
             <div class="row">
               <div class="col-12">
                 <span class="collapse-aggregated-activeDrivers collapse-table"
                     onClick="collapseTable('collapse-aggregated-activeDrivers',
                     'aggregated-activeDrivers')">
                   <h4>
                     <span class="collapse-table-arrow arrow-open"></span>
                     <a>Running Drivers ({activeDrivers.length})</a>
                   </h4>
                 </span>
                 <div class="aggregated-activeDrivers collapsible-table">
                   {activeDriversTable}
                 </div>
               </div>
             </div>
           }
          }
        </div>

        <div class="row">
          <div class="col-12">
            <span id="completed-app" class="collapse-aggregated-completedApps collapse-table"
                onClick="collapseTable('collapse-aggregated-completedApps',
                'aggregated-completedApps')">
              <h4>
                <span class="collapse-table-arrow arrow-open"></span>
                <a>Completed Applications ({completedApps.length})</a>
              </h4>
            </span>
            <div class="aggregated-completedApps collapsible-table">
              {completedAppsTable}
            </div>
          </div>
        </div>

        <div>
          {
            if (hasDrivers) {
              <div class="row">
                <div class="col-12">
                  <span class="collapse-aggregated-completedDrivers collapse-table"
                      onClick="collapseTable('collapse-aggregated-completedDrivers',
                      'aggregated-completedDrivers')">
                    <h4>
                      <span class="collapse-table-arrow arrow-open"></span>
                      <a>Completed Drivers ({completedDrivers.length})</a>
                    </h4>
                  </span>
                  <div class="aggregated-completedDrivers collapsible-table">
                    {completedDriversTable}
                  </div>
                </div>
              </div>
            }
          }
        </div>;

    UIUtils.basicSparkPage(request, content, title.getOrElse("Spark Master at " + state.uri))
  }

  private def workerRow(showResourceColumn: Boolean): WorkerInfo => Seq[Node] = worker => {
    <tr>
      <td>
        {
          if (worker.isAlive()) {
            <a href={UIUtils.makeHref(parent.master.reverseProxy, worker.id, worker.webUiAddress)}>
              {worker.id}
            </a>
          } else {
            worker.id
          }
        }
      </td>
      <td>{worker.host}:{worker.port}</td>
      <td>{worker.state}</td>
      <td>{worker.cores} ({worker.coresUsed} Used)</td>
      <td sorttable_customkey={"%s.%s".format(worker.memory, worker.memoryUsed)}>
        {Utils.megabytesToString(worker.memory)}
        ({Utils.megabytesToString(worker.memoryUsed)} Used)
      </td>
      {if (showResourceColumn) {
        <td>{formatWorkerResourcesDetails(worker)}</td>
      }}
    </tr>
  }

  private def appRow(app: ApplicationInfo): Seq[Node] = {
    val killLink = if (parent.killEnabled &&
      (app.state == ApplicationState.RUNNING || app.state == ApplicationState.WAITING)) {
      val confirm =
        s"if (window.confirm('Are you sure you want to kill application ${app.id} ?')) " +
          "{ this.parentNode.submit(); return true; } else { return false; }"
      <form action="app/kill/" method="POST" style="display:inline">
        <input type="hidden" name="id" value={app.id}/>
        <input type="hidden" name="terminate" value="true"/>
        <a href="#" onclick={confirm} class="kill-link">(kill)</a>
      </form>
    }
    <tr>
      <td>
        <a href={"app/?appId=" + app.id}>{app.id}</a>
        {killLink}
      </td>
      <td>
        {
          if (app.isFinished || app.desc.appUiUrl.isBlank()) {
            app.desc.name
          } else {
            <a href={UIUtils.makeHref(parent.master.reverseProxy,
              app.id, app.desc.appUiUrl)}>{app.desc.name}</a>
          }
        }
      </td>
      <td>
        {app.coresGranted}
      </td>
      <td sorttable_customkey={app.desc.memoryPerExecutorMB.toString}>
        {Utils.megabytesToString(app.desc.memoryPerExecutorMB)}
      </td>
      <td>
        {formatResourceRequirements(app.desc.resourceReqsPerExecutor)}
      </td>
      <td>{UIUtils.formatDate(app.submitDate)}</td>
      <td>{app.desc.user}</td>
      <td>{app.state.toString}</td>
      <td sorttable_customkey={app.duration.toString}>
        {UIUtils.formatDuration(app.duration)}
      </td>
    </tr>
  }

  private def activeDriverRow(driver: DriverInfo) = driverRow(driver, showDuration = true)

  private def completedDriverRow(driver: DriverInfo) = driverRow(driver, showDuration = false)

  private def driverRow(driver: DriverInfo, showDuration: Boolean): Seq[Node] = {
    val killLink = if (parent.killEnabled &&
      (driver.state == DriverState.RUNNING ||
        driver.state == DriverState.SUBMITTED)) {
      val confirm =
        s"if (window.confirm('Are you sure you want to kill driver ${driver.id} ?')) " +
          "{ this.parentNode.submit(); return true; } else { return false; }"
      <form action="driver/kill/" method="POST" style="display:inline">
        <input type="hidden" name="id" value={driver.id}/>
        <input type="hidden" name="terminate" value="true"/>
        <a href="#" onclick={confirm} class="kill-link">(kill)</a>
      </form>
    }
    <tr>
      <td>{driver.id} {killLink}</td>
      <td>{UIUtils.formatDate(driver.submitDate)}</td>
      <td>{driver.worker.map(w =>
        if (w.isAlive()) {
          <a href={UIUtils.makeHref(parent.master.reverseProxy, w.id, w.webUiAddress)}>
            {w.id}
          </a>
        } else {
          w.id
        }).getOrElse("None")}
      </td>
      <td>{driver.state}</td>
      <td sorttable_customkey={driver.desc.cores.toString}>
        {driver.desc.cores}
      </td>
      <td sorttable_customkey={driver.desc.mem.toString}>
        {Utils.megabytesToString(driver.desc.mem.toLong)}
      </td>
      <td>{formatResourcesAddresses(driver.resources)}</td>
      <td>{driver.desc.command.arguments(2)}</td>
      {if (showDuration) {
        <td sorttable_customkey={(-driver.startTime).toString}>
          {UIUtils.formatDuration(System.currentTimeMillis() - driver.startTime)}
        </td>
      }}
    </tr>
  }
}
