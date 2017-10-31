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

package org.apache.spark.status

import com.fasterxml.jackson.annotation.JsonIgnore

import org.apache.spark.status.KVUtils._
import org.apache.spark.status.api.v1._
import org.apache.spark.util.kvstore.KVIndex

private[spark] class ApplicationInfoWrapper(val info: ApplicationInfo) {

  @JsonIgnore @KVIndex
  def id: String = info.id

}

private[spark] class ExecutorSummaryWrapper(val info: ExecutorSummary) {

  @JsonIgnore @KVIndex
  private[this] val id: String = info.id

  @JsonIgnore @KVIndex("active")
  private[this] val active: Boolean = info.isActive

  @JsonIgnore @KVIndex("host")
  val host: String = info.hostPort.split(":")(0)

}

/**
 * Keep track of the existing stages when the job was submitted, and those that were
 * completed during the job's execution. This allows a more accurate acounting of how
 * many tasks were skipped for the job.
 */
private[spark] class JobDataWrapper(
    val info: JobData,
    val skippedStages: Set[Int]) {

  @JsonIgnore @KVIndex
  private[this] val id: Int = info.jobId

}

private[spark] class StageDataWrapper(
    val info: StageData,
    val jobIds: Set[Int]) {

  @JsonIgnore @KVIndex
  def id: Array[Int] = Array(info.stageId, info.attemptId)

}

private[spark] class TaskDataWrapper(val info: TaskData) {

  @JsonIgnore @KVIndex
  def id: Long = info.taskId

}

private[spark] class RDDStorageInfoWrapper(val info: RDDStorageInfo) {

  @JsonIgnore @KVIndex
  def id: Int = info.id

  @JsonIgnore @KVIndex("cached")
  def cached: Boolean = info.numCachedPartitions > 0

}

private[spark] class ExecutorStageSummaryWrapper(
    val stageId: Int,
    val stageAttemptId: Int,
    val executorId: String,
    val info: ExecutorStageSummary) {

  @JsonIgnore @KVIndex
  val id: Array[Any] = Array(stageId, stageAttemptId, executorId)

  @JsonIgnore @KVIndex("stage")
  private[this] val stage: Array[Int] = Array(stageId, stageAttemptId)

}
