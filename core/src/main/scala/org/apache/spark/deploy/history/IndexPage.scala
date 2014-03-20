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

package org.apache.spark.deploy.history

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.deploy.DeployWebUI
import org.apache.spark.deploy.master.ApplicationInfo
import org.apache.spark.ui.UIUtils
import org.apache.spark.util.Utils

private[spark] class IndexPage(parent: HistoryServer) {

  def render(request: HttpServletRequest): Seq[Node] = {
    val content =
      <div class="row-fluid">
        <div class="span12">
          <ul class="unstyled">
            <li>
              <strong>Welcome to the Fastest and Furious-est HistoryServer in the World!</strong>
            </li>
            {
              parent.logPathToUI.map { case (path, ui) =>
                <li>{path} at {ui.basePath}</li>
              }
            }
          </ul>
        </div>
      </div>

    UIUtils.basicSparkPage(content, "History Server")
  }

  def appRow(app: ApplicationInfo): Seq[Node] = {
    <tr>
      <td>
        <a href={"app?appId=" + app.id}>{app.id}</a>
      </td>
      <td>
        <a href={app.desc.appUiUrl}>{app.desc.name}</a>
      </td>
      <td>
        {app.coresGranted}
      </td>
      <td sorttable_customkey={app.desc.memoryPerSlave.toString}>
        {Utils.megabytesToString(app.desc.memoryPerSlave)}
      </td>
      <td>{DeployWebUI.formatDate(app.submitDate)}</td>
      <td>{app.desc.user}</td>
      <td>{app.state.toString}</td>
      <td>{DeployWebUI.formatDuration(app.duration)}</td>
    </tr>
  }
}
