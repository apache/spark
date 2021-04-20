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

package org.apache.spark.ui.accm

import javax.servlet.http.HttpServletRequest

import scala.collection.immutable.SortedMap
import scala.xml.Node

import org.apache.spark.status._
import org.apache.spark.ui._

/** Page showing table with a list of tasks from a single accumulator across a single stage* */
private[ui] class AccumulatorStagePage(parent: AccumulatorsTab, store: AppStatusStore)
  extends WebUIPage(prefix = "stage") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val parameterAccumulatorName = UIUtils.stripXSS(request.getParameter("accumulator_name"))
    require(parameterAccumulatorName != null && parameterAccumulatorName.nonEmpty,
      "Missing accumulator name parameter")
    val parameterAttempt = UIUtils.stripXSS(request.getParameter("attempt"))
    require(parameterAttempt != null && parameterAttempt.nonEmpty, "Missing attempt parameter")
    val parameterId = UIUtils.stripXSS(request.getParameter("id"))
    val accumulatorName = parameterAccumulatorName
    val attemptId = parameterAttempt.toInt
    val stageId = parameterId.toInt

    val accumulatorHeader =
      s"Details for Stage ${stageId} (Attempt ${attemptId})"

    def createAccumulatorTaskTable(tempMap: SortedMap[Long, AccumulatorTaskInfo] ): Seq[Node] = {
      if (tempMap.nonEmpty) {
        val accumulableHeaders: Seq[String] = Seq("Task Id", "Task Index", "Update")

        def accumulableRow(acc: (Long, AccumulatorTaskInfo)): Seq[Node] = {
          <tr>
            <td>{acc._2.taskId}</td>
            <td>{acc._2.taskIndex}</td>
            <td>{acc._2.update}</td>
          </tr>
        }

        val singleAccTable = UIUtils.listingTable(
          accumulableHeaders,
          accumulableRow,
          tempMap
        )

        singleAccTable
      } else {
        Nil
      }
    }

    val taskD = store.taskList(stageId, attemptId, Int.MaxValue)
    var tempMap = SortedMap[Long, AccumulatorTaskInfo]()
    // SortedMap[TaskId ->
    // NamedAccumulatorInfoDetails(task.id, task.index, accumulator.update)]

    taskD.foreach(task => {
      task.accumulatorUpdates.filter(_.name == accumulatorName).map(ac => {
          tempMap+=(task.taskId ->
            AccumulatorTaskInfo(
              task.taskId,
              task.index,
              ac.update.get))
      })
    })

    val singleAccumulatorSingleStageTable = createAccumulatorTaskTable(tempMap)

    val content = {
      <h4> Accumulator {accumulatorName} all tasks</h4> ++
        singleAccumulatorSingleStageTable
    }
    UIUtils.headerSparkPage(request, accumulatorHeader, content, parent)
  }

  case class AccumulatorTaskInfo(taskId: Long, taskIndex: Long, update: String)
}
