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

import org.apache.spark.ui.{UIUtils, WebUIPage}
import javax.servlet.http.HttpServletRequest
import scala.xml.Node
import org.apache.spark.scheduler.cluster.mesos.{ClusterTaskState, DriverSubmission}
import org.apache.mesos.Protos.TaskStatus

class DriverOutputPage(parent: MesosClusterUI) extends WebUIPage("") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val state = parent.scheduler.getState()

    val queuedHeaders = Seq("DriverID", "Submit Date", "Description")
    val driverHeaders = queuedHeaders ++
      Seq("Start Date", "Mesos Slave ID", "Mesos Task ID", "State")

    val queuedTable = UIUtils.listingTable(queuedHeaders, queuedRow, state.queuedDrivers)
    val launchedTable = UIUtils.listingTable(driverHeaders, driverRow, state.launchedDrivers)
    val finishedTable = UIUtils.listingTable(driverHeaders, driverRow, state.finishedDrivers)
    val content =
      <div class="row-fluid">
        <div class="span12">
          <h4>Queued Drivers:</h4>
          {queuedTable}
          <h4>Launched Drivers:</h4>
          {launchedTable}
          <h4>Finished Drivers:</h4>
          {finishedTable}
        </div>
      </div>;
    UIUtils.basicSparkPage(content, "Spark Drivers for Mesos cluster")
  }

  def queuedRow(submission: DriverSubmission): Seq[Node] = {
    <tr>
      <td>{submission.submissionId}</td>
      <td>{submission.submitDate}</td>
      <td>{submission.req.desc.command.mainClass}</td>
    </tr>
  }

  def driverRow(state: ClusterTaskState): Seq[Node] = {
    <tr>
      <td>{state.submission.submissionId}</td>
      <td>{state.submission.submitDate}</td>
      <td>{state.submission.req.desc.command.mainClass}</td>
      <td>{state.startDate}</td>
      <td>{state.slaveId.getValue}</td>
      <td>{state.taskId.getValue}</td>
      <td>{stateString(state.taskState)}</td>
    </tr>
  }

  def stateString(status: Option[TaskStatus]): String = {
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
