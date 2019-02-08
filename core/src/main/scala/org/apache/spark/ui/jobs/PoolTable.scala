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

package org.apache.spark.ui.jobs

import java.net.URLEncoder
import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.scheduler.Schedulable
import org.apache.spark.status.PoolData
import org.apache.spark.ui.UIUtils

/** Table showing list of pools */
private[ui] class PoolTable(pools: Map[Schedulable, PoolData], parent: StagesTab) {

  def toNodeSeq(request: HttpServletRequest): Seq[Node] = {
    <table class="table table-bordered table-striped table-condensed sortable table-fixed">
      <thead>
        <th>Pool Name</th>
        <th>Minimum Share</th>
        <th>Pool Weight</th>
        <th>Active Stages</th>
        <th>Running Tasks</th>
        <th>SchedulingMode</th>
      </thead>
      <tbody>
        {pools.map { case (s, p) => poolRow(request, s, p) }}
      </tbody>
    </table>
  }

  private def poolRow(request: HttpServletRequest, s: Schedulable, p: PoolData): Seq[Node] = {
    val activeStages = p.stageIds.size
    val href = "%s/stages/pool?poolname=%s"
      .format(UIUtils.prependBaseUri(request, parent.basePath), URLEncoder.encode(p.name, "UTF-8"))
    <tr>
      <td>
        <a href={href}>{p.name}</a>
      </td>
      <td>{s.minShare}</td>
      <td>{s.weight}</td>
      <td>{activeStages}</td>
      <td>{s.runningTasks}</td>
      <td>{s.schedulingMode}</td>
    </tr>
  }
}

