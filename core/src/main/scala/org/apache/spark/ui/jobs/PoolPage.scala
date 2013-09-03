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

import scala.xml.{NodeSeq, Node}
import scala.collection.mutable.HashSet

import org.apache.spark.scheduler.Stage
import org.apache.spark.ui.UIUtils._
import org.apache.spark.ui.Page._

/** Page showing specific pool details */
private[spark] class PoolPage(parent: JobProgressUI) {
  def listener = parent.listener

  def render(request: HttpServletRequest): Seq[Node] = {
    listener.synchronized {
      val poolName = request.getParameter("poolname")
      val poolToActiveStages = listener.poolToActiveStages
      val activeStages = poolToActiveStages.get(poolName).toSeq.flatten
      val activeStagesTable = new StageTable(activeStages.sortBy(_.submissionTime).reverse, parent)

      val pool = listener.sc.getPoolForName(poolName).get
      val poolTable = new PoolTable(Seq(pool), listener)

      val content = <h4>Summary </h4> ++ poolTable.toNodeSeq() ++
                    <h4>{activeStages.size} Active Stages</h4> ++ activeStagesTable.toNodeSeq()

      headerSparkPage(content, parent.sc, "Fair Scheduler Pool: " + poolName, Stages)
    }
  }
}
