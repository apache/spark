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

import java.io.PrintWriter
import java.nio.{ByteBuffer, ByteOrder}
import javax.servlet.http.HttpServletRequest

import scala.io.Source
import scala.xml.Node

import org.apache.mesos.Protos.TaskStatus
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

import org.apache.spark.deploy.mesos.{MesosClusterDispatcher, MesosDriverDescription}
import org.apache.spark.scheduler.cluster.mesos.MesosClusterSubmissionState
import org.apache.spark.ui.{UIUtils, WebUIPage}

private[mesos] class MesosClusterPage(parent: MesosClusterUI) extends WebUIPage("") {
  def render(request: HttpServletRequest): Seq[Node] = {
    val state = parent.scheduler.getSchedulerState()
    val sandboxHeader = Seq("Sandbox")
    val queuedHeaders = Seq("Driver ID", "Submit Date", "Main Class", "Driver Resources")
    val driverHeaders = queuedHeaders ++
      Seq("Start Date", "Mesos Slave ID", "State") ++ sandboxHeader
    val retryHeaders = Seq("Driver ID", "Submit Date", "Description") ++
      Seq("Last Failed Status", "Next Retry Time", "Attempt Count")
    val queuedTable = UIUtils.listingTable(queuedHeaders, queuedRow, state.queuedDrivers)
    val launchedTable = UIUtils.listingTable(driverHeaders, driverRow, state.launchedDrivers)
    val finishedTable = UIUtils.listingTable(driverHeaders, driverRow, state.finishedDrivers)
    val retryTable = UIUtils.listingTable(retryHeaders, retryRow, state.pendingRetryDrivers)
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
    val id = submission.submissionId
    <tr>
      <td><a href={s"driver?id=$id"}>{id}</a></td>
      <td>{submission.submissionDate}</td>
      <td>{submission.command.mainClass}</td>
      <td>cpus: {submission.cores}, mem: {submission.mem}</td>
    </tr>
  }

  private def driverRow(state: MesosClusterSubmissionState): Seq[Node] = {
    val id = state.driverDescription.submissionId
    val masterInfo = parent.scheduler.getSchedulerMasterInfo()
    val schedulerFwId = parent.scheduler.getSchedulerFrameworkId()
    val sandboxCol = if (masterInfo.isDefined && schedulerFwId.isDefined) {

      val masterUri = masterInfo.map{info => s"http://${getIp4(info.getIp)}:${info.getPort}"}.get
      val directory = getTaskDirectory(masterUri, id, state.slaveId.getValue)

      if(directory.isDefined) {
        val sandBoxUri = s"$masterUri" +
          s"/#/slaves/${state.slaveId.getValue}" +
          s"/browse?path=${directory.get}"
          <a href={sandBoxUri}>Sandbox</a>
      } else {
        " " // just 1 space - could use &nbsp;
      }
    } else {
     " " // just 1 space - could use &nbsp;
    }

    <tr>
      <td><a href={s"driver?id=$id"}>{id}</a></td>
      <td>{state.driverDescription.submissionDate}</td>
      <td>{state.driverDescription.command.mainClass}</td>
      <td>cpus: {state.driverDescription.cores}, mem: {state.driverDescription.mem}</td>
      <td>{state.startDate}</td>
      <td>{state.slaveId.getValue}</td>
      <td>{stateString(state.mesosTaskStatus)}</td>
      <td>{sandboxCol}</td>
    </tr>
  }

  private def retryRow(submission: MesosDriverDescription): Seq[Node] = {
    val id = submission.submissionId
    <tr>
      <td><a href={s"driver?id=$id"}>{id}</a></td>
      <td>{submission.submissionDate}</td>
      <td>{submission.command.mainClass}</td>
      <td>{submission.retryState.get.lastFailureStatus}</td>
      <td>{submission.retryState.get.nextRetry}</td>
      <td>{submission.retryState.get.retries}</td>
    </tr>
  }

  private def stateString(status: Option[TaskStatus]): String = {
    if (status.isEmpty) {
      return ""
    }
    val sb = new StringBuilder
    val s = status.get
    sb.append(s"State: ${s.getState}")
    if (status.get.hasMessage) {
      sb.append(s", Message: ${s.getMessage}")
    }
    if (status.get.hasHealthy) {
      sb.append(s", Healthy: ${s.getHealthy}")
    }
    if (status.get.hasSource) {
      sb.append(s", Source: ${s.getSource}")
    }
    if (status.get.hasReason) {
      sb.append(s", Reason: ${s.getReason}")
    }
    if (status.get.hasTimestamp) {
      sb.append(s", Time: ${s.getTimestamp}")
    }
    sb.toString()
  }

  private def getIp4(ip: Int): String = {
    val buffer = ByteBuffer.allocate(4)
    buffer.putInt(ip)
    // we need to check about that because protocolbuf changes the order
    // which by mesos api is considered to be network order (big endian).
    val result = if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
      buffer.array.toList.reverse
    } else {
      buffer.array.toList
    }
    result.map{byte => byte & 0xFF}.mkString(".")
  }

  private def getListFromJson(value: JValue): List[Map[String, Any]] = {
    value.values.asInstanceOf[List[Map[String, Any]]]
  }

  private def getTaskDirectory(masterUri: String, driverFwId: String, slaveId: String):
      Option[String] = {

    val stateSummaryValues = getListFromJson(parse(Source.fromURL(s"$masterUri" +
      "/state-summary").mkString) \ "slaves")

    // get slave ip:port from state-summary in the form slave(1)@ip:port
    val slaveUriArray = stateSummaryValues.filter(x => x.get("id").get == slaveId)
      .map{x => x.get("pid").get}.head.toString.split("@")

    if(slaveUriArray.length !=2) {
      return None
    }

    val slavesStateJson = Source.fromURL("http://" + slaveUriArray(1) + "/slave(1)/state").mkString

    val completedFrameworksValues = getListFromJson(parse(slavesStateJson) \ "completed_frameworks")

    val frameworksValues = getListFromJson(parse(slavesStateJson) \ "frameworks")

    val clusterSchedulerId = parent.scheduler.getSchedulerState().frameworkId

    val clusterSchedulerInfo = (completedFrameworksValues ++ frameworksValues)
      .filter(x => x.get("id").get == clusterSchedulerId)

    val executorsInfoAll = clusterSchedulerInfo.
      flatMap{x => x.get("completed_executors").get.asInstanceOf[List[Map[String, Any]]]} ++
    clusterSchedulerInfo.
      flatMap{x => x.get("executors").get.asInstanceOf[List[Map[String, Any]]]}

    val executorsInfoDriver = executorsInfoAll.filter{x => x.get("id").get.toString == driverFwId}
    if (executorsInfoDriver.size != 1) {
      return None
    }
    executorsInfoDriver.head.get("directory").asInstanceOf[Option[String]]
  }
}
