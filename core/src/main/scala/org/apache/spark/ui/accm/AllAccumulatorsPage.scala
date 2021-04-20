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
import scala.collection.mutable.ListBuffer
import scala.xml.Node

import org.apache.spark.status._
import org.apache.spark.ui._

/** Page showing table with a list of all accumulators across all finished stages* */
private[ui] class AllAccumulatorsPage(parent: AccumulatorsTab, store: AppStatusStore)
  extends WebUIPage("") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val allStages = parent.store.stageList(null)
    val basePathUri = UIUtils.prependBaseUri(request, parent.basePath)
    val accumulatorHeader = s"Accumulators across all Stages"

    def generateAccumulatorsTable(
      tempMap: SortedMap[String, ListBuffer[AccumulatorInfo]]): Seq[Node] = {
      if (tempMap.nonEmpty) {
        val accumulableHeaders: Seq[String] = Seq("Accumulable", "Stage", "Value")
        def accumulableRow(acc: (String, ListBuffer[AccumulatorInfo])): Seq[Node] = {
          acc._2.map(accDetails => {
            <tr>
              <td>
                <a href={accDetails.accNameLink} class="name-link">{accDetails.name}</a>
              </td>
              <td>
                <a href={accDetails.stageLink} class="name-link">{accDetails.stageId}</a>
              </td>
              <td>{accDetails.value}</td>
            </tr>
          })
        }
        val accumulableTable = UIUtils.listingTable(
          accumulableHeaders,
          accumulableRow,
          tempMap
        )

        accumulableTable.toSeq
      } else {
        Nil
      }
    }

    var tempMap = SortedMap[String, ListBuffer[AccumulatorInfo]]()
      // SortedMap[accumulator.name ->
      // ListBuffer[
      // AccumulatorInfoDetails(
       // stage,
       // accumulator.name,
       // accumulator.value,
       // stageLink,
       // accumulatorNameLink)]]

    allStages.map( stage => {
      val stageData = parent.store
        .asOption(parent.store.stageAttempt(stage.stageId, stage.attemptId, details = false))
        .getOrElse {
          val content =
            <div id="no-info">
            </div>
          return UIUtils.headerSparkPage(request, "", content, parent)
        }
      val sd = stageData.accumulatorUpdates.toSeq

      sd.map(acc => {
        val accNameLinkUri =
          s"$basePathUri/accumulators/accumulator/?accumulator_name=" +
            s"${acc.name}&attempt=${stage.attemptId}"

        val stageLinkUri =
          s"$basePathUri/accumulators/stage/?accumulator_name=" +
            s"${acc.name}&id=${stageData.stageId}&attempt=${stageData.attemptId}"

        def addAccToMap(): ListBuffer[AccumulatorInfo] = {
          tempMap += (acc.name -> new ListBuffer[AccumulatorInfo]())
          tempMap(acc.name)
        }

        val newList = tempMap.getOrElse(acc.name, addAccToMap()):+
          AccumulatorInfo(stage.stageId, acc.name, acc.value, stageLinkUri, accNameLinkUri)
        tempMap+=(acc.name -> newList)
      })
    })

    val accumulatorsTable = generateAccumulatorsTable(tempMap)

    val content =
      <div id="tables-info-acc">
        <h4>Accumulators</h4>
        {accumulatorsTable}
      </div>

    UIUtils.headerSparkPage(request, accumulatorHeader, content, parent, showVisualization = true)
  }

  case class AccumulatorInfo(
     stageId: Int,
     name: String,
     value: String,
     stageLink: String,
     accNameLink: String)
}
