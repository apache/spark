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

package org.apache.spark.deploy.mesos.ui

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.mesos.Protos.TaskStatus

import org.apache.spark.deploy.mesos.MesosDriverDescription
import org.apache.spark.scheduler.cluster.mesos.{RetryState, MesosClusterTaskState}
import org.apache.spark.ui.{UIUtils, WebUIPage}

private[mesos] class MesosClusterPage(parent: MesosClusterUI) extends WebUIPage("") {
  def render(request: HttpServletRequest): Seq[Node] = {
    val state = parent.scheduler.getState()
    val queuedHeaders = Seq("DriverID", "Submit Date", "Description")
    val driverHeaders = queuedHeaders ++
      Seq("Start Date", "Mesos Slave ID", "State")
    val retryHeaders = Seq("DriverID", "Submit Date", "Description") ++
      Seq("Last Failed Status", "Next Retry Time", "Attempt Count")
    val queuedTable = UIUtils.listingTable(queuedHeaders, queuedRow, state.queuedDrivers)
    val launchedTable = UIUtils.listingTable(driverHeaders, driverRow, state.launchedDrivers)
    val finishedTable = UIUtils.listingTable(driverHeaders, driverRow, state.finishedDrivers)
    val retryTable = UIUtils.listingTable(retryHeaders, retryRow, state.retryList)
    val content =
      <p>Mesos Framework ID: {state.frameworkId}</p>
      <div class="row-fluid">
        <div class="span12">
          <h4>Queued Drivers:</h4>
          {queuedTable}
          <h4>Launched Drivers:</h4>
          {launchedTable}
          <h4>Finished Drivers:</h4>
          {finishedTable}
          <h4>Supervise drivers waiting for retry:</h4>
          {retryTable}
        </div>
      </div>;
    UIUtils.basicSparkPage(content, "Spark Drivers for Mesos cluster")
  }

  private def queuedRow(submission: MesosDriverDescription): Seq[Node] = {
    <tr>
      <td>{submission.submissionId}</td>
      <td>{submission.submissionDate}</td>
      <td>{submission.command.mainClass}</td>
    </tr>
  }

  private def driverRow(state: MesosClusterTaskState): Seq[Node] = {
    <tr>
      <td>{state.submission.submissionId}</td>
      <td>{state.submission.submissionDate}</td>
      <td>{state.submission.command.mainClass}</td>
      <td>{state.startDate}</td>
      <td>{state.slaveId.getValue}</td>
      <td>{stateString(state.taskState)}</td>
    </tr>
  }

  private def retryRow(state: RetryState): Seq[Node] = {
    <tr>
      <td>{state.submission.submissionId}</td>
      <td>{state.submission.submissionDate}</td>
      <td>{state.submission.command.mainClass}</td>
      <td>{state.lastFailureStatus}</td>
      <td>{state.nextRetry}</td>
      <td>{state.retries}</td>
    </tr>
  }

  private def stateString(status: Option[TaskStatus]): String = {
    if (status.isEmpty) {
      return ""
    }
    val sb = new StringBuilder
    sb.append(s"State: ${status.get.getState}")
    if (status.get.hasMessage) {
      sb.append(s", Message: ${status.get.getMessage}")
    }
    if (status.get.hasHealthy) {
      sb.append(s", Healthy: ${status.get.getHealthy}")
    }
    if (status.get.hasSource) {
      sb.append(s", Source: ${status.get.getSource}")
    }
    if (status.get.hasReason) {
      sb.append(s", Reason: ${status.get.getReason}")
    }
    if (status.get.hasTimestamp) {
      sb.append(s", Time: ${status.get.getTimestamp}")
    }
    sb.toString()
  }
}
