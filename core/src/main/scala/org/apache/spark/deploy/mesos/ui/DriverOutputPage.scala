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
    val content =
      <div class="row-fluid">
        <div class="span12">
          <h3>Active drivers</h3>
          {state.activeDrivers.map(d => driverContent(d)).flatten}
          <h3>Completed drivers</h3>
          {state.completedDrivers.map(d => completedDriverContent(d)).flatten}
        </div>
      </div>;
    UIUtils.basicSparkPage(content, "Spark Drivers for Mesos cluster")
  }

  def driverContent(info: DriverInfo): Seq[Node] = {
    <ul class="unstyled">
      <li><strong>ID:</strong> {info.id}</li>
      <li><strong>Submit Date:</strong> {info.submitDate}</li>
      <li><strong>Start Date:</strong> {info.startTime}</li>
      <li><strong>Output:</strong>
        <a href={"logPage?driverId=%s&logType=stdout"
          .format(info.id)}>stdout</a>
        <a href={"logPage?driverId=%s&logType=stderr"
          .format(info.id)}>stderr</a>
      </li>
    </ul>
  }

  def completedDriverContent(info: DriverInfo): Seq[Node] = {
    <ul class="unstyled">
      <li><strong>ID:</strong> {info.id}</li>
      <li><strong>Submit Date:</strong> {info.submitDate}</li>
      <li><strong>Start Date:</strong> {info.startTime}</li>
      <li><strong>Output:</strong>
        <a href={"logPage?driverId=%s&logType=stdout"
          .format(info.id)}>stdout</a>
        <a href={"logPage?driverId=%s&logType=stderr"
          .format(info.id)}>stderr</a>
      </li>
      <li><strong>State:</strong>{info.state}</li>
      <li><strong>Exception:</strong>{info.exception}</li>
    </ul>
  }
}
