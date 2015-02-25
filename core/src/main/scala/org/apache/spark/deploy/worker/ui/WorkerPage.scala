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

import scala.concurrent.Await
import scala.xml.Node

import akka.pattern.ask
import javax.servlet.http.HttpServletRequest
import org.json4s.JValue

import org.apache.spark.deploy.JsonProtocol
import org.apache.spark.deploy.DeployMessages.{RequestWorkerState, WorkerStateResponse}
import org.apache.spark.deploy.master.DriverState
import org.apache.spark.deploy.worker.{DriverRunner, ExecutorRunner}
import org.apache.spark.ui.{WebUIPage, UIUtils}
import org.apache.spark.util.Utils

private[spark] class WorkerPage(parent: WorkerWebUI) extends WebUIPage("") {
  val workerActor = parent.worker.self
  val worker = parent.worker
  val timeout = parent.timeout

  override def renderJson(request: HttpServletRequest): JValue = {
    val stateFuture = (workerActor ? RequestWorkerState)(timeout).mapTo[WorkerStateResponse]
    val workerState = Await.result(stateFuture, timeout)
    JsonProtocol.writeWorkerState(workerState)
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    val stateFuture = (workerActor ? RequestWorkerState)(timeout).mapTo[WorkerStateResponse]
    val workerState = Await.result(stateFuture, timeout)

    val executorHeaders = Seq("ExecutorID", "Cores", "State", "Memory", "Job Details", "Logs")
    val runningExecutors = workerState.executors
    val runningExecutorTable =
      UIUtils.listingTable(executorHeaders, executorRow, runningExecutors)
    val finishedExecutors = workerState.finishedExecutors
    val finishedExecutorTable =
      UIUtils.listingTable(executorHeaders, executorRow, finishedExecutors)

    val driverHeaders = Seq("DriverID", "Main Class", "State", "Cores", "Memory", "Logs", "Notes")
    val runningDrivers = workerState.drivers.sortBy(_.driverId).reverse
    val runningDriverTable = UIUtils.listingTable(driverHeaders, driverRow, runningDrivers)
    val finishedDrivers = workerState.finishedDrivers.sortBy(_.driverId).reverse
    val finishedDriverTable = UIUtils.listingTable(driverHeaders, driverRow, finishedDrivers)

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
          <h4> Running Executors ({runningExecutors.size}) </h4>
          {runningExecutorTable}
          {
            if (runningDrivers.nonEmpty) {
              <h4> Running Drivers ({runningDrivers.size}) </h4> ++
              runningDriverTable
            }
          }
          {
            if (finishedExecutors.nonEmpty) {
              <h4>Finished Executors ({finishedExecutors.size}) </h4> ++
              finishedExecutorTable
            }
          }
          {
            if (finishedDrivers.nonEmpty) {
              <h4> Finished Drivers ({finishedDrivers.size}) </h4> ++
              finishedDriverTable
            }
          }
        </div>
      </div>;
    UIUtils.basicSparkPage(content, "Spark Worker at %s:%s".format(
      workerState.host, workerState.port))
  }

  def executorRow(executor: ExecutorRunner): Seq[Node] = {
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
          <li><strong>Name:</strong> {executor.appDesc.name}</li>
          <li><strong>User:</strong> {executor.appDesc.user}</li>
        </ul>
      </td>
      <td>
     <a href={"logPage?appId=%s&executorId=%s&logType=stdout"
        .format(executor.appId, executor.execId)}>stdout</a>
     <a href={"logPage?appId=%s&executorId=%s&logType=stderr"
        .format(executor.appId, executor.execId)}>stderr</a>
      </td>
    </tr>

  }

  def driverRow(driver: DriverRunner): Seq[Node] = {
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
        <a href={s"logPage?driverId=${driver.driverId}&logType=stdout"}>stdout</a>
        <a href={s"logPage?driverId=${driver.driverId}&logType=stderr"}>stderr</a>
      </td>
      <td>
        {driver.finalException.getOrElse("")}
      </td>
    </tr>
  }
}
