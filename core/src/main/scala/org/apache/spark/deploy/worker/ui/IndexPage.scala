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

import scala.concurrent.duration._
import scala.concurrent.Await

import akka.pattern.ask

import net.liftweb.json.JsonAST.JValue

import org.apache.spark.deploy.JsonProtocol
import org.apache.spark.deploy.DeployMessages.{RequestWorkerState, WorkerStateResponse}
import org.apache.spark.deploy.worker.ExecutorRunner
import org.apache.spark.ui.UIUtils
import org.apache.spark.util.Utils


private[spark] class IndexPage(parent: WorkerWebUI) {
  val workerActor = parent.worker.self
  val worker = parent.worker
  val timeout = parent.timeout

  def renderJson(request: HttpServletRequest): JValue = {
    val stateFuture = (workerActor ? RequestWorkerState)(timeout).mapTo[WorkerStateResponse]
    val workerState = Await.result(stateFuture, timeout)
    JsonProtocol.writeWorkerState(workerState)
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    val stateFuture = (workerActor ? RequestWorkerState)(timeout).mapTo[WorkerStateResponse]
    val workerState = Await.result(stateFuture, timeout)

    val executorHeaders = Seq("ExecutorID", "Cores", "Memory", "Job Details", "Logs")
    val runningExecutorTable =
      UIUtils.listingTable(executorHeaders, executorRow, workerState.executors)
    val finishedExecutorTable =
      UIUtils.listingTable(executorHeaders, executorRow, workerState.finishedExecutors)

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

        <div class="row-fluid"> <!-- Running Executors -->
          <div class="span12">
            <h4> Running Executors {workerState.executors.size} </h4>
            {runningExecutorTable}
          </div>
        </div>

        <div class="row-fluid"> <!-- Finished Executors  -->
          <div class="span12">
            <h4> Finished Executors </h4>
            {finishedExecutorTable}
          </div>
        </div>;

    UIUtils.basicSparkPage(content, "Spark Worker at %s:%s".format(
      workerState.host, workerState.port))
  }

  def executorRow(executor: ExecutorRunner): Seq[Node] = {
    <tr>
      <td>{executor.execId}</td>
      <td>{executor.cores}</td>
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

}
