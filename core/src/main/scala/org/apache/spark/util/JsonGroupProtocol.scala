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

package org.apache.spark.util

import org.apache.spark.scheduler._
import org.json4s.JsonAST._
import org.json4s.JsonDSL._

private[spark] object JsonGroupProtocol {
  def stageSubmittedToJson(stageSubmitted: SparkListenerStageSubmitted): (JValue, Int) = {
    val stageJson = stageInfoToJson(stageSubmitted.stageInfo)
    val properties = JsonProtocol.propertiesToJson(stageSubmitted.properties)
    val json = ("Event" -> Utils.getFormattedClassName(stageSubmitted)) ~
      ("Stage Info" -> stageJson._1) ~
      ("Properties" -> properties)

    (json, stageJson._2)
  }

  def stageCompletedToJson(stageCompleted: SparkListenerStageCompleted): (JValue, Int) = {
    val stageJson = stageInfoToJson(stageCompleted.stageInfo)
    val json = ("Event" -> Utils.getFormattedClassName(stageCompleted)) ~
      ("Stage Info" -> stageJson._1)

    (json, stageJson._2)
  }

  def taskStartToJson(taskStart: SparkListenerTaskStart): (JValue, Int) = {
    (JsonProtocol.taskStartToJson(taskStart), taskStart.stageId)
  }

  def taskGettingResultToJson(taskGettingResult: SparkListenerTaskGettingResult): (JValue, Int) = {
    (JsonProtocol.taskGettingResultToJson(taskGettingResult), taskGettingResult.taskInfo.stageId)
  }

  def taskEndToJson(taskEnd: SparkListenerTaskEnd): (JValue, Int) = {
    (JsonProtocol.taskEndToJson(taskEnd), taskEnd.stageId)
  }

  def jobStartToJson(jobStart: SparkListenerJobStart): (JValue, Int, Set[Int]) = {
    (JsonProtocol.jobStartToJson(jobStart), jobStart.jobId, jobStart.stageIds.toSet)
  }

  def jobEndToJson(jobEnd: SparkListenerJobEnd): (JValue, Int) = {
    (JsonProtocol.jobEndToJson(jobEnd), jobEnd.jobId)
  }

  def stageInfoToJson(stageInfo: StageInfo): (JValue, Int) = {
    (JsonProtocol.stageInfoToJson(stageInfo), stageInfo.stageId)
  }
}
