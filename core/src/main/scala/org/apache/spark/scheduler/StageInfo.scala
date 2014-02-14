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

import scala.collection._

import org.apache.spark.executor.TaskMetrics

import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonDSL._

/**
 * Stores information about a stage to pass from the scheduler to SparkListeners.
 */
class StageInfo(stage: Stage) extends JsonSerializable {
  val stageId = stage.id

  /**
   * Store the metrics for all tasks that have completed, including redundant, speculated tasks.
   */
  val taskInfos = mutable.Buffer[(TaskInfo, TaskMetrics)]()

  /**
   * When this stage was submitted from the DAGScheduler to a TaskScheduler.
   */
  var submissionTime: Option[Long] = None

  var completionTime: Option[Long] = None
  val rddName = stage.rdd.name
  val name = stage.name
  val numPartitions = stage.numPartitions
  val numTasks = stage.numTasks
  var emittedTaskSizeWarning = false

  override def toJson = {
    val (taskInfoList, taskMetricsList) = taskInfos.toList.unzip
    val taskInfoJson = JArray(taskInfoList.map(_.toJson))
    val taskMetricsJson = JArray(taskMetricsList.map(_.toJson))
    ("Stage ID" -> stage.id) ~
    ("Submission Time" -> submissionTime.getOrElse(0L)) ~
    ("Completion Time" -> completionTime.getOrElse(0L)) ~
    ("RDD Name" -> rddName) ~
    ("Stage Name" -> name) ~
    ("Number of Partitions" -> numPartitions) ~
    ("Number of Tasks" -> numTasks) ~
    ("Task Info" -> taskInfoJson) ~
    ("Task Metrics" -> taskMetricsJson)
  }
}
