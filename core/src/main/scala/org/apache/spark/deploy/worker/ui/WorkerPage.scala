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
import org.apache.spark.ui.{UITable, UITableBuilder, WebUIPage, UIUtils}
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

  private val executorTable: UITable[ExecutorRunner] = {
    val t = new UITableBuilder[ExecutorRunner]()
    t.intCol("Executor ID") { _.execId }
    t.intCol("Cores") { _.cores }
    t.col("State") { _.state.toString }
    t.memCol("Memory") { _.memory }
    t.customCol("Job Details") { executor =>
      <ul class="unstyled">
        <li><strong>ID:</strong> {executor.appId}</li>
        <li><strong>Name:</strong> {executor.appDesc.name}</li>
        <li><strong>User:</strong> {executor.appDesc.user}</li>
      </ul>
    }
    t.customCol("Logs") { executor =>
      <a href={"logPage?appId=%s&executorId=%s&logType=stdout"
        .format(executor.appId, executor.execId)}>stdout</a>
      <a href={"logPage?appId=%s&executorId=%s&logType=stderr"
        .format(executor.appId, executor.execId)}>stderr</a>
    }
    t.build()
  }

  private val driverTable: UITable[DriverRunner] = {
    val t = new UITableBuilder[DriverRunner]()
    t.col("Driver ID") { _.driverId }
    t.col("Main Class") { _.driverDesc.command.arguments(1) }
    t.col("State") { _.finalState.getOrElse(DriverState.RUNNING).toString }
    t.intCol("Cores") { _.driverDesc.cores }
    t.memCol("Memory") { _.driverDesc.mem }
    t.customCol("Logs") { driver =>
      <a href={s"logPage?driverId=${driver.driverId}&logType=stdout"}>stdout</a>
      <a href={s"logPage?driverId=${driver.driverId}&logType=stderr"}>stderr</a>
    }
    t.col("Notes") { _.finalException.getOrElse("").toString }
    t.build()
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    val stateFuture = (workerActor ? RequestWorkerState)(timeout).mapTo[WorkerStateResponse]
    val workerState = Await.result(stateFuture, timeout)

    val runningExecutors = workerState.executors
    val runningExecutorTable = executorTable.render(runningExecutors)
    val finishedExecutors = workerState.finishedExecutors
    val finishedExecutorTable = executorTable.render(finishedExecutors)

    val runningDrivers = workerState.drivers.sortBy(_.driverId).reverse
    val runningDriverTable = driverTable.render(runningDrivers)
    val finishedDrivers = workerState.finishedDrivers.sortBy(_.driverId).reverse
    val finishedDriverTable = driverTable.render(finishedDrivers)

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
}
