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

import scala.xml.Node

import org.apache.spark.ui.{UIUtils, WebUIPage}

private[spark] class HistoryNotFoundPage(parent: MasterWebUI)
  extends WebUIPage("history/not-found") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val content =
      <div class="row-fluid">
        <div class="span12" style="font-size:14px;font-weight:bold">
          No event logs were found for this application. Did you forget to set
          <a href="http://spark.apache.org/docs/latest/monitoring.html">spark.eventLog.enabled</a>?
        </div>
      </div>
    UIUtils.basicSparkPage(content, "Application history not found")
  }
}
