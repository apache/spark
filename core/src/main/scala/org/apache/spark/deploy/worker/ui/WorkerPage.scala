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

package org.apache.spark.deploy.worker.ui

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.json4s.JValue

import org.apache.spark.deploy.{ExecutorState, JsonProtocol}
import org.apache.spark.deploy.DeployMessages.{RequestWorkerState, WorkerStateResponse}
import org.apache.spark.deploy.master.DriverState
import org.apache.spark.deploy.worker.{DriverRunner, ExecutorRunner}
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.util.Utils

private[ui] class WorkerPage(parent: WorkerWebUI) extends WebUIPage("") {
  private val workerEndpoint = parent.worker.self

  override def renderJson(request: HttpServletRequest): JValue = {
    val workerState = workerEndpoint.askSync[WorkerStateResponse](RequestWorkerState)
    JsonProtocol.writeWorkerState(workerState)
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    val workerState = workerEndpoint.askSync[WorkerStateResponse](RequestWorkerState)

    val executorHeaders = Seq("ExecutorID", "Cores", "State", "Memory", "Job Details", "Logs")
    val runningExecutors = workerState.executors
    val runningExecutorTable =
      UIUtils.listingTable(executorHeaders, executorRow, runningExecutors)
    val finishedExecutors = workerState.finishedExecutors
    val finishedExecutorTable =
      UIUtils.listingTable(executorHeaders, executorRow, finishedExecutors)

    val driverHeaders = Seq("DriverID", "Main Class", "State", "Cores", "Memory", "Logs", "Notes")
    val runningDrivers = workerState.drivers.sortBy(_.driverId).reverse
    val runningDriverTable = UIUtils.listingTable[DriverRunner](driverHeaders,
      driverRow(workerState.workerId, _), runningDrivers)
    val finishedDrivers = workerState.finishedDrivers.sortBy(_.driverId).reverse
    val finishedDriverTable = UIUtils.listingTable[DriverRunner](driverHeaders,
      driverRow(workerState.workerId, _), finishedDrivers)

    // For now we only show driver information if the user has submitted drivers to the cluster.
    // This is until we integrate the notion of drivers and applications in the UI.

    val content =
      <div class="row-fluid"> <!-- Worker Details -->
        <div class="span12">
          <ul class="unstyled">
            <li><strong>ID:</strong> {workerState.workerId}</li>
            <li><strong>
              Master URL:</strong> {workerState.masterUrl}
            </li>
            <li><strong>Cores:</strong> {workerState.cores} ({workerState.coresUsed} Used)</li>
            <li><strong>Memory:</strong> {Utils.megabytesToString(workerState.memory)}
              ({Utils.megabytesToString(workerState.memoryUsed)} Used)</li>
          </ul>
          <p><a href={workerState.masterWebUiUrl}>Back to Master</a></p>
        </div>
      </div>
      <div class="row-fluid"> <!-- Executors and Drivers -->
        <div class="span12">
          <span class="collapse-aggregated-runningExecutors collapse-table"
              onClick="collapseTable('collapse-aggregated-runningExecutors',
              'aggregated-runningExecutors')">
            <h4>
              <span class="collapse-table-arrow arrow-open"></span>
              <a>Running Executors ({runningExecutors.size})</a>
            </h4>
          </span>
          <div class="aggregated-runningExecutors collapsible-table">
            {runningExecutorTable}
          </div>
          {
            if (runningDrivers.nonEmpty) {
              <span class="collapse-aggregated-runningDrivers collapse-table"
                  onClick="collapseTable('collapse-aggregated-runningDrivers',
                  'aggregated-runningDrivers')">
                <h4>
                  <span class="collapse-table-arrow arrow-open"></span>
                  <a>Running Drivers ({runningDrivers.size})</a>
                </h4>
              </span> ++
              <div class="aggregated-runningDrivers collapsible-table">
                {runningDriverTable}
              </div>
            }
          }
          {
            if (finishedExecutors.nonEmpty) {
              <span class="collapse-aggregated-finishedExecutors collapse-table"
                  onClick="collapseTable('collapse-aggregated-finishedExecutors',
                  'aggregated-finishedExecutors')">
                <h4>
                  <span class="collapse-table-arrow arrow-open"></span>
                  <a>Finished Executors ({finishedExecutors.size})</a>
                </h4>
              </span> ++
              <div class="aggregated-finishedExecutors collapsible-table">
                {finishedExecutorTable}
              </div>
            }
          }
          {
            if (finishedDrivers.nonEmpty) {
              <span class="collapse-aggregated-finishedDrivers collapse-table"
                  onClick="collapseTable('collapse-aggregated-finishedDrivers',
                  'aggregated-finishedDrivers')">
                <h4>
                  <span class="collapse-table-arrow arrow-open"></span>
                  <a>Finished Drivers ({finishedDrivers.size})</a>
                </h4>
              </span> ++
              <div class="aggregated-finishedDrivers collapsible-table">
                {finishedDriverTable}
              </div>
            }
          }
        </div>
      </div>;
    UIUtils.basicSparkPage(request, content, "Spark Worker at %s:%s".format(
      workerState.host, workerState.port))
  }

  def executorRow(executor: ExecutorRunner): Seq[Node] = {
    val workerUrlRef = UIUtils.makeHref(parent.worker.reverseProxy, executor.workerId,
      parent.webUrl)
    val appUrlRef = UIUtils.makeHref(parent.worker.reverseProxy, executor.appId,
      executor.appDesc.appUiUrl)

    <tr>
      <td>{executor.execId}</td>
      <td>{executor.cores}</td>
      <td>{executor.state}</td>
      <td sorttable_customkey={executor.memory.toString}>
        {Utils.megabytesToString(executor.memory)}
      </td>
      <td>
        <ul class="unstyled">
          <li><strong>ID:</strong> {executor.appId}</li>
          <li><strong>Name:</strong>
          {
            if ({executor.state == ExecutorState.RUNNING} && executor.appDesc.appUiUrl.nonEmpty) {
              <a href={appUrlRef}> {executor.appDesc.name}</a>
            } else {
              {executor.appDesc.name}
            }
          }
          </li>
          <li><strong>User:</strong> {executor.appDesc.user}</li>
        </ul>
      </td>
      <td>
        <a href={s"$workerUrlRef/logPage?appId=${executor
          .appId}&executorId=${executor.execId}&logType=stdout"}>stdout</a>
        <a href={s"$workerUrlRef/logPage?appId=${executor
          .appId}&executorId=${executor.execId}&logType=stderr"}>stderr</a>
      </td>
    </tr>

  }

  def driverRow(workerId: String, driver: DriverRunner): Seq[Node] = {
    val workerUrlRef = UIUtils.makeHref(parent.worker.reverseProxy, workerId, parent.webUrl)
    <tr>
      <td>{driver.driverId}</td>
      <td>{driver.driverDesc.command.arguments(2)}</td>
      <td>{driver.finalState.getOrElse(DriverState.RUNNING)}</td>
      <td sorttable_customkey={driver.driverDesc.cores.toString}>
        {driver.driverDesc.cores.toString}
      </td>
      <td sorttable_customkey={driver.driverDesc.mem.toString}>
        {Utils.megabytesToString(driver.driverDesc.mem)}
      </td>
      <td>
        <a href={s"$workerUrlRef/logPage?driverId=${driver.driverId}&logType=stdout"}>stdout</a>
        <a href={s"$workerUrlRef/logPage?driverId=${driver.driverId}&logType=stderr"}>stderr</a>
      </td>
      <td>
        {driver.finalException.getOrElse("")}
      </td>
    </tr>
  }
}
