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


import scala.xml.Node

import org.apache.spark.scheduler.SchedulingMode


/** Page showing executor summary */
private[spark] class ExecutorTable(val parent: JobProgressUI) {

  val listener = parent.listener
  val dateFmt = parent.dateFmt
  val isFairScheduler = listener.sc.getSchedulingMode == SchedulingMode.FAIR

  def toNodeSeq(): Seq[Node] = {
    listener.synchronized {
      executorTable()
    }
  }

  /** Special table which merges two header cells. */
  private def executorTable[T](): Seq[Node] = {
    <table class="table table-bordered table-striped table-condensed sortable">
      <thead>
        <th>Executor ID</th>
        <th>Duration</th>
        <th>#Tasks</th>
        <th>#Failed Tasks</th>
        <th>#Succeed Tasks</th>
        <th>Shuffle Read</th>
        <th>Shuffle Write</th>
      </thead>
      <tbody>
        {createExecutorTable()}
      </tbody>
    </table>
  }

  private def createExecutorTable() : Seq[Node] = {
    val executorIdToSummary = listener.executorIdToSummary
    executorIdToSummary.toSeq.sortBy(_._1).map{
      case (k,v) => {
      <tr>
        <td>{k}</td>
        <td>{v.duration} ms</td>
        <td>{v.totalTasks}</td>
        <td>{v.failedTasks}</td>
        <td>{v.succeedTasks}</td>
        <td>{v.shuffleRead}</td>
        <td>{v.shuffleWrite}</td>
      </tr>
      }
    }
  }
}
