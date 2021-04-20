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


/** Page showing table with single accumulator across all stages and tasks* */
private[ui] class AccumulatorPage (parent: AccumulatorsTab, store: AppStatusStore)
  extends WebUIPage(prefix = "accumulator") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val parameterAccumulatorName = UIUtils.stripXSS(request.getParameter("accumulator_name"))
    require(parameterAccumulatorName != null && parameterAccumulatorName.nonEmpty,
      "Missing accumulator name parameter")
    val parameterAttempt = UIUtils.stripXSS(request.getParameter("attempt"))
    require(parameterAttempt != null && parameterAttempt.nonEmpty, "Missing attempt parameter")
    val accumulatorName = parameterAccumulatorName
    val attemptId = parameterAttempt.toInt

    val accumulatorHeader = s"Details for all Stages (Attempt ${attemptId})"

    val allStages = parent.store.stageList(null)
    val basePathUri = UIUtils.prependBaseUri(request, parent.basePath)

    def createSingleAccumulatorTable(tempMap: SortedMap[Long, NamedAccumulatorInfo]):
    Seq[Node] = {
      if (tempMap.nonEmpty) {
        val accumulableHeaders: Seq[String] = Seq("Stage", "Task Id", "Task Index", "Value")

        def accumulableRow(acc: (Long, NamedAccumulatorInfo)): Seq[Node] = {
          <tr>
            <td>{acc._2.stage}</td>
            <td>{acc._2.taskId}</td>
            <td>{acc._2.taskIndex}</td>
            <td>{acc._2.value}</td>
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

    def createStagesAccumulatorTable(tempStageMap: SortedMap[Int, String]): Seq[Node] = {
      val accumulableStageHeaders: Seq[String] = Seq("Stage", "Value")

      def accumulableStagesRow(acc: (Int, String)): Seq[Node] = {
          <tr>
            <td>{acc._1}</td>
            <td>{acc._2}</td>
          </tr>
      }

      val singleAccTable = UIUtils.listingTable(
        accumulableStageHeaders,
        accumulableStagesRow,
        tempStageMap
      )

      val maybeTable : Seq[Node] = if (tempStageMap.nonEmpty) singleAccTable else Nil
      maybeTable
    }

    var tempStageMap = SortedMap[Int, String]()// SortedMap[StageId -> accumulator.update]
    var tempMapStagesAndTasks = SortedMap[Long, NamedAccumulatorInfo]()
    // SortedMap[TaskId ->
    // NamedAccumulatorInfoDetails(stage, task.id, task.index, accumulator.update)]

    allStages.foreach(stage => {
      val taskD = store.taskList(stage.stageId, stage.attemptId, Int.MaxValue)
      taskD.foreach(task => {
        task.accumulatorUpdates.filter(_.name == accumulatorName).foreach(ac => {
          val stageLinkUri =
            s"$basePathUri/accumulators/stage/?id=${stage.stageId}&attempt=${stage.attemptId}"
          tempMapStagesAndTasks+=(
            task.taskId ->
              NamedAccumulatorInfo(
                stage.stageId,
                task.taskId,
                task.index,
                ac.update.get))
        })
      })
      val stageData = parent.store.asOption(
        parent.store.stageAttempt(stage.stageId, stage.attemptId, details = false)).get
      val targetAcc = stageData.accumulatorUpdates.filter(_.name == accumulatorName)
      if (targetAcc.nonEmpty) tempStageMap+=(stage.stageId -> targetAcc.head.value)
    })

    val accummulatorsTableAllTasks = createSingleAccumulatorTable(tempMapStagesAndTasks)
    val AccStageTable = createStagesAccumulatorTable(tempStageMap)

    val content = {
      <h4>Accumulator {accumulatorName}</h4>
        <span>
          <h4>Per Stage ({tempStageMap.size})</h4>
          {AccStageTable}
        </span>
        <span>
          <h4>Per Task ({tempMapStagesAndTasks.size})</h4>
          {accummulatorsTableAllTasks}
        </span>
    }
    UIUtils.headerSparkPage(request, accumulatorHeader, content, parent)
  }

  case class NamedAccumulatorInfo(
        stage: Int,
        taskId: Long,
        taskIndex: Long,
        value: String)
}
