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

import org.apache.spark.deploy.Command
import org.apache.spark.deploy.mesos.MesosDriverDescription
import org.apache.spark.scheduler.cluster.mesos.{MesosClusterSubmissionState, MesosClusterRetryState}
import org.apache.spark.ui.{UIUtils, WebUIPage}


private[ui] class DriverPage(parent: MesosClusterUI) extends WebUIPage("driver") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val driverId = request.getParameter("id")
    require(driverId != null && driverId.nonEmpty, "Missing id parameter")

    val state = parent.scheduler.getDriverState(driverId)
    if (state.isEmpty) {
      val content =
        <div>
          <p>Cannot find driver {driverId}</p>
        </div>
      return UIUtils.basicSparkPage(content, s"Details for Job $driverId")
    }
    val driverState = state.get
    val driverHeaders = Seq("Driver property", "Value")
    val schedulerHeaders = Seq("Scheduler property", "Value")
    val commandEnvHeaders = Seq("Command environment variable", "Value")
    val launchedHeaders = Seq("Launched property", "Value")
    val commandHeaders = Seq("Comamnd property", "Value")
    val retryHeaders = Seq("Last failed status", "Next retry time", "Retry count")
    val driverDescription = Iterable.apply(driverState.description)
    val submissionState = Iterable.apply(driverState.submissionState)
    val command = Iterable.apply(driverState.description.command)
    val schedulerProperties = Iterable.apply(driverState.description.schedulerProperties)
    val commandEnv = Iterable.apply(driverState.description.command.environment)
    val driverTable =
      UIUtils.listingTable(driverHeaders, driverRow, driverDescription)
    val commandTable =
      UIUtils.listingTable(commandHeaders, commandRow, command)
    val commandEnvTable =
      UIUtils.listingTable(commandEnvHeaders, propertiesRow, commandEnv)
    val schedulerTable =
      UIUtils.listingTable(schedulerHeaders, propertiesRow, schedulerProperties)
    val launchedTable =
      UIUtils.listingTable(launchedHeaders, launchedRow, submissionState)
    val retryTable =
      UIUtils.listingTable(
        retryHeaders, retryRow, Iterable.apply(driverState.description.retryState))
    val content =
      <p>Driver state information for driver id {driverId}</p>
        <a href="/">Back to Drivers</a>
        <div class="row-fluid">
          <div class="span12">
            <h4>Driver state: {driverState.state}</h4>
            <h4>Driver properties</h4>
            {driverTable}
            <h4>Driver command</h4>
            {commandTable}
            <h4>Driver command environment</h4>
            {commandEnvTable}
            <h4>Scheduler properties</h4>
            {schedulerTable}
            <h4>Launched state</h4>
            {launchedTable}
            <h4>Retry state</h4>
            {retryTable}
          </div>
        </div>;

    UIUtils.basicSparkPage(content, s"Details for Job $driverId")
  }

  private def launchedRow(submissionState: Option[MesosClusterSubmissionState]): Seq[Node] = {
    submissionState.map { state =>
      <tr>
        <td>Mesos Slave ID</td>
        <td>{state.slaveId.getValue}</td>
      </tr>
      <tr>
        <td>Mesos Task ID</td>
        <td>{state.taskId.getValue}</td>
      </tr>
      <tr>
        <td>Launch Time</td>
        <td>{state.startDate}</td>
      </tr>
      <tr>
        <td>Finish Time</td>
        <td>{state.finishDate.map(_.toString).getOrElse("")}</td>
      </tr>
      <tr>
        <td>Last Task Status</td>
        <td>{state.mesosTaskStatus.map(_.toString).getOrElse("")}</td>
      </tr>
    }.getOrElse(Seq[Node]())
  }

  private def propertiesRow(properties: collection.Map[String, String]): Seq[Node] = {
    properties.map { case (k, v) =>
      <tr>
        <td>{k}</td><td>{v}</td>
      </tr>
    }.toSeq
  }

  private def commandRow(command: Command): Seq[Node] = {
    <tr>
      <td>Main class</td><td>{command.mainClass}</td>
    </tr>
    <tr>
      <td>Arguments</td><td>{command.arguments.mkString(" ")}</td>
    </tr>
    <tr>
      <td>Class path entries</td><td>{command.classPathEntries.mkString(" ")}</td>
    </tr>
    <tr>
      <td>Java options</td><td>{command.javaOpts.mkString((" "))}</td>
    </tr>
    <tr>
      <td>Library path entries</td><td>{command.libraryPathEntries.mkString((" "))}</td>
    </tr>
  }

  private def driverRow(driver: MesosDriverDescription): Seq[Node] = {
    <tr>
      <td>Name</td><td>{driver.name}</td>
    </tr>
    <tr>
      <td>Id</td><td>{driver.submissionId}</td>
    </tr>
    <tr>
      <td>Cores</td><td>{driver.cores}</td>
    </tr>
    <tr>
      <td>Memory</td><td>{driver.mem}</td>
    </tr>
    <tr>
      <td>Submitted</td><td>{driver.submissionDate}</td>
    </tr>
    <tr>
      <td>Supervise</td><td>{driver.supervise}</td>
    </tr>
  }

  private def retryRow(retryState: Option[MesosClusterRetryState]): Seq[Node] = {
    retryState.map { state =>
      <tr>
        <td>
          {state.lastFailureStatus}
        </td>
        <td>
          {state.nextRetry}
        </td>
        <td>
          {state.retries}
        </td>
      </tr>
    }.getOrElse(Seq[Node]())
  }
}
