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
import org.apache.spark.deploy.mesos.Messages.{DispatcherStateResponse, RequestDispatcherState}
import scala.concurrent.Await
import org.apache.spark.deploy.master.DriverInfo

class DriverOutputPage(parent: MesosClusterUI) extends WebUIPage("") {
  private val dispatcher = parent.dispatcherActorRef
  private val timeout = parent.timeout

  def render(request: HttpServletRequest): Seq[Node] = {
    val stateFuture = (dispatcher ? RequestDispatcherState)(timeout).mapTo[DispatcherStateResponse]
    val state = Await.result(stateFuture, timeout)

    val driverHeaders = Seq("DriverID", "Submit Date", "Start Date", "Logs")
    val completedDriverHeaders = driverHeaders ++ Seq("State", "Exception")
    val driverTable = UIUtils.listingTable(driverHeaders, driverRow, state.activeDrivers)
    val completedDriverTable =
      UIUtils.listingTable(completedDriverHeaders, completedDriverRow, state.completedDrivers)
    val content =
      <div class="row-fluid">
        <div class="span12">
          <h4>Running Drivers</h4>
          {driverTable}
          <h4>Finished Drivers</h4>
          {completedDriverTable}
        </div>
      </div>;
    UIUtils.basicSparkPage(content, "Spark Drivers for Mesos cluster")
  }

  def driverRow(info: DriverInfo): Seq[Node] = {
    <tr>
      <td>{info.id}</td>
      <td>{info.submitDate}</td>
      <td>{info.startTime}</td>
      <td>
        <a href={"logPage?driverId=%s&logType=stdout"
          .format(info.id)}>stdout</a>,
        <a href={"logPage?driverId=%s&logType=stderr"
          .format(info.id)}>stderr</a>
      </td>
    </tr>
  }

  def completedDriverRow(info: DriverInfo): Seq[Node] = {
    <tr>
      <td>{info.id}</td>
      <td>{info.submitDate}</td>
      <td>{info.startTime}</td>
      <td>
        <a href={"logPage?driverId=%s&logType=stdout"
          .format(info.id)}>stdout</a>,
        <a href={"logPage?driverId=%s&logType=stderr"
          .format(info.id)}>stderr</a>
      </td>
      <td>{info.state}</td>
      <td>{info.exception}</td>
    </tr>
  }
}
