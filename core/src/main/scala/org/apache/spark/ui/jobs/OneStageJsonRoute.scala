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

import org.apache.spark.SparkException
import org.apache.spark.status.api.StageData
import org.apache.spark.status.{JsonRequestHandler, StatusJsonRoute}

class OneStageJsonRoute(parent: JsonRequestHandler) extends StatusJsonRoute[StageData] {

  override def renderJson(route: HttpServletRequest): StageData = {
    parent.withSparkUI(route){case(ui, route) =>
      val listener = ui.stagesTab.listener
      val stageIdOpt = JsonRequestHandler.extractStageId(route.getPathInfo)
      stageIdOpt match {
        case Some(stageId) =>
          val stageAndStatus = AllStagesJsonRoute.stagesAndStatus(ui)
          val oneStage = stageAndStatus.flatMap{case (status, stages) =>
            val matched = stages.find{_.stageId == stageId}
            matched.map{status -> _}
          }.headOption
          oneStage match {
            case Some((status,stageInfo)) =>
              val stageUiData = listener.synchronized{
                listener.stageIdToData.get((stageInfo.stageId, stageInfo.attemptId)).
                  getOrElse{ throw new SparkException("failed to get full stage data for stage: " + stageInfo.stageId +
                    ":" + stageInfo.attemptId)
                }
              }
              AllStagesJsonRoute.stageUiToStageData(status, stageInfo, stageUiData, includeDetails = true)
            case None =>
              throw new IllegalArgumentException("unknown stage: " + stageId)
          }

        case None =>
          throw new IllegalArgumentException("no valid stageId in path")
      }
    }
  }
}