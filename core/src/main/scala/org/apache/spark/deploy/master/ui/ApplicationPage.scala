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

import scala.concurrent.Await
import scala.xml.Node

import akka.pattern.ask
import javax.servlet.http.HttpServletRequest
import net.liftweb.json.JsonAST.JValue

import org.apache.spark.deploy.JsonProtocol
import org.apache.spark.deploy.DeployMessages.{MasterStateResponse, RequestMasterState}
import org.apache.spark.deploy.master.ExecutorInfo
import org.apache.spark.ui.UIUtils
import org.apache.spark.util.Utils

private[spark] class ApplicationPage(parent: MasterWebUI) {
  val master = parent.masterActorRef
  val timeout = parent.timeout

  /** Executor details for a particular application */
  def renderJson(request: HttpServletRequest): JValue = {
    val appId = request.getParameter("appId")
    val stateFuture = (master ? RequestMasterState)(timeout).mapTo[MasterStateResponse]
    val state = Await.result(stateFuture, timeout)
    val app = state.activeApps.find(_.id == appId).getOrElse({
      state.completedApps.find(_.id == appId).getOrElse(null)
    })
    JsonProtocol.writeApplicationInfo(app)
  }

  /** Executor details for a particular application */
  def render(request: HttpServletRequest): Seq[Node] = {
    val appId = request.getParameter("appId")
    val stateFuture = (master ? RequestMasterState)(timeout).mapTo[MasterStateResponse]
    val state = Await.result(stateFuture, timeout)
    val app = state.activeApps.find(_.id == appId).getOrElse({
      state.completedApps.find(_.id == appId).getOrElse(null)
    })

    val executorHeaders = Seq("ExecutorID", "Worker", "Cores", "Memory", "State", "Logs")
    val executors = app.executors.values.toSeq
    val executorTable = UIUtils.listingTable(executorHeaders, executorRow, executors)

    val content =
        <div class="row-fluid">
          <div class="span12">
            <ul class="unstyled">
              <li><strong>ID:</strong> {app.id}</li>
              <li><strong>Name:</strong> {app.desc.name}</li>
              <li><strong>User:</strong> {app.desc.user}</li>
              <li><strong>Cores:</strong>
                {
                if (app.desc.maxCores == None) {
                  "Unlimited (%s granted)".format(app.coresGranted)
                } else {
                  "%s (%s granted, %s left)".format(
                    app.desc.maxCores.get, app.coresGranted, app.coresLeft)
                }
                }
              </li>
              <li>
                <strong>Executor Memory:</strong>
                {Utils.megabytesToString(app.desc.memoryPerSlave)}
              </li>
              <li><strong>Submit Date:</strong> {app.submitDate}</li>
              <li><strong>State:</strong> {app.state}</li>
              <li><strong><a href={app.appUiUrl}>Application Detail UI</a></strong></li>
            </ul>
          </div>
        </div>

        <div class="row-fluid"> <!-- Executors -->
          <div class="span12">
            <h4> Executor Summary </h4>
            {executorTable}
          </div>
        </div>;
    UIUtils.basicSparkPage(content, "Application: " + app.desc.name)
  }

  def executorRow(executor: ExecutorInfo): Seq[Node] = {
    <tr>
      <td>{executor.id}</td>
      <td>
        <a href={executor.worker.webUiAddress}>{executor.worker.id}</a>
      </td>
      <td>{executor.cores}</td>
      <td>{executor.memory}</td>
      <td>{executor.state}</td>
      <td>
        <a href={"%s/logPage?appId=%s&executorId=%s&logType=stdout"
          .format(executor.worker.webUiAddress, executor.application.id, executor.id)}>stdout</a>
        <a href={"%s/logPage?appId=%s&executorId=%s&logType=stderr"
          .format(executor.worker.webUiAddress, executor.application.id, executor.id)}>stderr</a>
      </td>
    </tr>
  }
}
