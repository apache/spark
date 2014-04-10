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

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.scheduler.{Schedulable, StageInfo}
import org.apache.spark.ui.Page._
import org.apache.spark.ui.UIUtils

/** Page showing specific pool details */
private[ui] class PoolPage(parent: JobProgressUI) {
  private val appName = parent.appName
  private val basePath = parent.basePath
  private val live = parent.live
  private val sc = parent.sc
  private lazy val listener = parent.listener

  def render(request: HttpServletRequest): Seq[Node] = {
    listener.synchronized {
      val poolName = request.getParameter("poolname")
      val poolToActiveStages = listener.poolToActiveStages
      val activeStages = poolToActiveStages.get(poolName) match {
        case Some(s) => s.values.toSeq
        case None => Seq[StageInfo]()
      }

      val mostRecentlySubmitted = (si: StageInfo) => -si.submissionTime.getOrElse(0L)
      val activeStagesTable = new StageTable(activeStages.sortBy(mostRecentlySubmitted), parent)

      // For now, pool information is only accessible in live UIs
      val pools = if (live) Seq(sc.getPoolForName(poolName).get) else Seq[Schedulable]()
      val poolTable = new PoolTable(pools, parent)

      val content =
        <h4>Summary </h4> ++ poolTable.toNodeSeq ++
        <h4>{activeStages.size} Active Stages</h4> ++ activeStagesTable.toNodeSeq

      UIUtils.headerSparkPage(
        content, basePath, appName, "Fair Scheduler Pool: " + poolName, Stages)
    }
  }
}
