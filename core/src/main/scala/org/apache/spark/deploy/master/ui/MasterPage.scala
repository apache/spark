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

package org.apache.spark.deploy.master.ui

import javax.servlet.http.HttpServletRequest

import scala.concurrent.Await
import scala.xml.{Text, Node}

import akka.pattern.ask
import org.json4s.JValue

import org.apache.spark.deploy.JsonProtocol
import org.apache.spark.deploy.DeployMessages.{MasterStateResponse, RequestMasterState}
import org.apache.spark.deploy.master.{WorkerInfo, ApplicationInfo, DriverInfo}
import org.apache.spark.ui.{UITable, UITableBuilder, WebUIPage, UIUtils}
import org.apache.spark.util.Utils

private[spark] class MasterPage(parent: MasterWebUI) extends WebUIPage("") {
  private val master = parent.masterActorRef
  private val timeout = parent.timeout

  override def renderJson(request: HttpServletRequest): JValue = {
    val stateFuture = (master ? RequestMasterState)(timeout).mapTo[MasterStateResponse]
    val state = Await.result(stateFuture, timeout)
    JsonProtocol.writeMasterState(state)
  }

  private val workerTable: UITable[WorkerInfo] = {
    val t = new UITableBuilder[WorkerInfo]()
    t.col("ID") (identity) withMarkup  { worker =>
      <a href={worker.webUiAddress}>{worker.id}</a>
    }
    t.col("Address") { worker => s"${worker.host}:${worker.port}"}
    t.col("State") { _.state.toString }
    t.col("Cores") { _.coresUsed } formatWith { c: Int => s"$c Used" }
    t.col("Memory") (identity) sortBy { worker =>
      s"${worker.memory}:${worker.memoryUsed}"
    } withMarkup { worker =>
      Text(Utils.megabytesToString(worker.memory)) ++
      Text(Utils.megabytesToString(worker.memoryUsed))
    }
    t.build()
  }

  private val appTable: UITable[ApplicationInfo] = {
    val t = new UITableBuilder[ApplicationInfo]()
    t.col("ID") (_.id) withMarkup { id =>
      <a href={"app?appId=" + id}>{id}</a>
    }
    t.col("Name") { _.id }
    t.col("Cores") { _.coresGranted }
    t.sizeCol("Memory per Node") { _.desc.memoryPerSlave }
    t.dateCol("Submitted Time") { _.submitDate }
    t.col("User") { _.desc.user }
    t.col("State") { _.state.toString }
    t.durationCol("Duration") { _.duration }
    t.build()
  }

  private val driverTable: UITable[DriverInfo] = {
    val t = new UITableBuilder[DriverInfo]()
    t.col("ID") { _.id }
    t.dateCol("Submitted Time") { _.submitDate }
    t.col("Worker") (identity) withMarkup { driver =>
      driver.worker.map(w => <a href={w.webUiAddress}>{w.id.toString}</a>).getOrElse(Text("None"))
    }
    t.col("State") { _.state.toString }
    t.col("Cores") { _.desc.cores }
    t.sizeCol("Memory") { _.desc.mem.toLong }
    t.col("Main Class") { _.desc.command.arguments(1) }
    t.build()
  }

  /** Index view listing applications and executors */
  def render(request: HttpServletRequest): Seq[Node] = {
    val stateFuture = (master ? RequestMasterState)(timeout).mapTo[MasterStateResponse]
    val state = Await.result(stateFuture, timeout)

    val allWorkersTable = workerTable.render(state.workers.sortBy(_.id))

    val activeAppsTable = appTable.render(state.activeApps.sortBy(_.startTime).reverse)
    val completedAppsTable = appTable.render(state.completedApps.sortBy(_.endTime).reverse)

    val activeDriversTable = driverTable.render(state.activeDrivers.sortBy(_.startTime).reverse)
    val completedDriversTable =
      driverTable.render(state.completedDrivers.sortBy(_.startTime).reverse)

    // For now we only show driver information if the user has submitted drivers to the cluster.
    // This is until we integrate the notion of drivers and applications in the UI.
    def hasDrivers = state.activeDrivers.length > 0 || state.completedDrivers.length > 0

    val content =
        <div class="row-fluid">
          <div class="span12">
            <ul class="unstyled">
              <li><strong>URL:</strong> {state.uri}</li>
              <li><strong>Workers:</strong> {state.workers.size}</li>
              <li><strong>Cores:</strong> {state.workers.map(_.cores).sum} Total,
                {state.workers.map(_.coresUsed).sum} Used</li>
              <li><strong>Memory:</strong>
                {Utils.megabytesToString(state.workers.map(_.memory).sum)} Total,
                {Utils.megabytesToString(state.workers.map(_.memoryUsed).sum)} Used</li>
              <li><strong>Applications:</strong>
                {state.activeApps.size} Running,
                {state.completedApps.size} Completed </li>
              <li><strong>Drivers:</strong>
                {state.activeDrivers.size} Running,
                {state.completedDrivers.size} Completed </li>
              <li><strong>Status:</strong> {state.status}</li>
            </ul>
          </div>
        </div>

        <div class="row-fluid">
          <div class="span12">
            <h4> Workers </h4>
            {allWorkersTable}
          </div>
        </div>

        <div class="row-fluid">
          <div class="span12">
            <h4> Running Applications </h4>
            {activeAppsTable}
          </div>
        </div>

        <div>
          {if (hasDrivers) {
             <div class="row-fluid">
               <div class="span12">
                 <h4> Running Drivers </h4>
                 {activeDriversTable}
               </div>
             </div>
           }
          }
        </div>

        <div class="row-fluid">
          <div class="span12">
            <h4> Completed Applications </h4>
            {completedAppsTable}
          </div>
        </div>

        <div>
          {
            if (hasDrivers) {
              <div class="row-fluid">
                <div class="span12">
                  <h4> Completed Drivers </h4>
                  {completedDriversTable}
                </div>
              </div>
            }
          }
        </div>;

    UIUtils.basicSparkPage(content, "Spark Master at " + state.uri)
  }
}
