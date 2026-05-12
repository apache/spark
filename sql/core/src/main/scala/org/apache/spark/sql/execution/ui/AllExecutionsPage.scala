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

package org.apache.spark.sql.execution.ui

import scala.xml.Node

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.internal.config.UI.UI_SQL_GROUP_SUB_EXECUTION_ENABLED
import org.apache.spark.ui.{UIUtils, WebUIPage}

private[ui] class AllExecutionsPage(parent: SQLTab) extends WebUIPage("") {

  private val spinner =
    <div class="text-center p-3">
      <div class="spinner-border text-primary" role="status">
        <span class="visually-hidden">Loading...</span>
      </div>
    </div>

  override def render(request: HttpServletRequest): Seq[Node] = {
    val groupSubExec = parent.conf.get(UI_SQL_GROUP_SUB_EXECUTION_ENABLED)
    val content =
      <span>
        <div id="group-sub-exec-config" style="display:none"
             data-value={groupSubExec.toString}></div>
        <div id="sql-executions-table">
          {spinner}
        </div>
        <script src={UIUtils.prependBaseUri(
          request, "/static/sql/allexecutionspage.js")}></script>
      </span>

    UIUtils.headerSparkPage(
      request, "SQL / DataFrame", content, parent, useDataTables = true)
  }
}
