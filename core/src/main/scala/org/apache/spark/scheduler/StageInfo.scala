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

package org.apache.spark.scheduler

import scala.collection.mutable

import org.apache.spark.executor.TaskMetrics

import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonDSL._
import org.apache.spark.util.Utils
import net.liftweb.json.DefaultFormats

/**
 * Stores information about a stage to pass from the scheduler to SparkListeners. Also stores the
 * metrics for all tasks that have completed, including redundant, speculated tasks.
 */
class StageInfo(
    val stageId: Int,
    val name: String,
    val rddName: String,
    val numPartitions: Int,
    val numTasks: Int,
    val taskInfos: mutable.Buffer[(TaskInfo, TaskMetrics)] =
      mutable.Buffer[(TaskInfo, TaskMetrics)]()
  ) extends JsonSerializable {

  /**
   * When this stage was submitted from the DAGScheduler to a TaskScheduler.
   */
  var submissionTime: Option[Long] = None
  var completionTime: Option[Long] = None
  var emittedTaskSizeWarning = false

  override def toJson = {
    val (taskInfoList, taskMetricsList) = taskInfos.toList.unzip
    val taskInfoListJson = JArray(taskInfoList.map(_.toJson))
    val taskMetricsListJson = JArray(taskMetricsList.map(_.toJson))
    val submissionTimeJson = submissionTime.map(JInt(_)).getOrElse(JNothing)
    val completionTimeJson = completionTime.map(JInt(_)).getOrElse(JNothing)
    ("Stage ID" -> stageId) ~
    ("Stage Name" -> name) ~
    ("RDD Name" -> rddName) ~
    ("Number of Partitions" -> numPartitions) ~
    ("Number of Tasks" -> numTasks) ~
    ("Task Info List" -> taskInfoListJson) ~
    ("Task Metrics List" -> taskMetricsListJson) ~
    ("Submission Time" -> submissionTimeJson) ~
    ("Completion Time" -> completionTimeJson) ~
    ("Emitted Task Size Warning" -> emittedTaskSizeWarning)
  }
}

object StageInfo {
  def fromStage(stage: Stage): StageInfo = {
    new StageInfo(
      stage.id,
      stage.name,
      stage.rdd.name,
      stage.numPartitions,
      stage.numTasks)
  }

  def fromJson(json: JValue): StageInfo = {
    implicit val format = DefaultFormats
    val taskInfoListJson = (json \ "Task Info List").extract[List[JValue]]
    val taskMetricsListJson = (json \ "Task Metrics List").extract[List[JValue]]
    val taskInfo = taskInfoListJson.zip(taskMetricsListJson).map { case (info, metrics) =>
      (TaskInfo.fromJson(info), TaskMetrics.fromJson(metrics))
    }.toBuffer

    val metrics = new StageInfo(
      (json \ "Stage ID").extract[Int],
      (json \ "Stage Name").extract[String],
      (json \ "RDD Name").extract[String],
      (json \ "Number of Partitions").extract[Int],
      (json \ "Number of Tasks").extract[Int],
      taskInfo)

    metrics.submissionTime =
      json \ "Submission Time" match {
        case JNothing => None
        case value: JValue => Some(value.extract[Long])
      }
    metrics.completionTime =
      json \ "Completion Time" match {
        case JNothing => None
        case value: JValue => Some(value.extract[Long])
      }
    metrics.emittedTaskSizeWarning = (json \ "Emitted Task Size Warning").extract[Boolean]
    metrics
  }
}
