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
import org.apache.spark.deploy.master.DriverInfo
import org.apache.spark.scheduler.cluster.mesos.{ClusterTaskState, DriverSubmission, ClusterScheduler}

class DriverOutputPage(parent: MesosClusterUI) extends WebUIPage("") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val state = parent.scheduler.getState()

    val queuedHeaders = Seq("DriverID", "Submit Date", "Description")
    val driverHeaders = queuedHeaders ++
      Seq("Start Date", "Mesos Slave ID", "Mesos Task ID", "State", "Logs")

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

    null
  }

  def queuedRow(submission: DriverSubmission): Seq[Node] = {
    <tr>
      <td>{submission.submissionId}</td>
      <td>{submission.submitDate}</td>
      <td>{submission.desc.command.mainClass}</td>
    </tr>
  }

  def driverRow(state: ClusterTaskState): Seq[Node] = {
    <tr>
      <td>{state.submission.submissionId}</td>
      <td>{state.submission.submitDate}</td>
      <td>{state.submission.desc.command.mainClass}</td>
      <td>{state.startDate}</td>
      <td>{state.slaveId}</td>
      <td>{state.taskId}</td>
      <td>{state.taskState}</td>
      <td>
        <a href={""}>stdout</a>,
        <a href={""}>stderr</a>
      </td>
    </tr>
  }
}
