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

class StageInfo(
    stage: Stage,
    val taskInfos: mutable.Buffer[(TaskInfo, TaskMetrics)] = mutable.Buffer[(TaskInfo, TaskMetrics)]()
) {
  val stageId = stage.id
  /** When this stage was submitted from the DAGScheduler to a TaskScheduler. */
  var submissionTime: Option[Long] = None
  var completionTime: Option[Long] = None
  val rddName = stage.rdd.name
  val name = stage.name
  val numPartitions = stage.numPartitions
  val numTasks = stage.numTasks
  var emittedTaskSizeWarning = false
}
