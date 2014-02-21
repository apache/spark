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

/**
 * Stores information about a stage to pass from the scheduler to SparkListeners. Also
 * stores the metrics for all tasks that have completed, including redundant, speculated tasks.
 */
private[spark]
class StageInfo(
    val stageId: Int,
    val name: String,
    val rddName: String,
    val numPartitions: Int,
    val numTasks: Int,
    val taskInfos: mutable.Buffer[(TaskInfo, TaskMetrics)] =
      mutable.Buffer[(TaskInfo, TaskMetrics)]()) {

  /** When this stage was submitted from the DAGScheduler to a TaskScheduler. */
  var submissionTime: Option[Long] = None
  var completionTime: Option[Long] = None
  var emittedTaskSizeWarning = false
}

private[spark]
object StageInfo {
  def fromStage(stage: Stage): StageInfo = {
    new StageInfo(stage.id, stage.name, stage.rdd.name, stage.numPartitions, stage.numTasks)
  }
}
