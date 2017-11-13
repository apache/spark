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

import java.lang.{Integer => JInteger, Long => JLong}

import com.fasterxml.jackson.annotation.JsonIgnore

import org.apache.spark.status.KVUtils._
import org.apache.spark.status.api.v1._
import org.apache.spark.util.kvstore.KVIndex

private[spark] case class AppStatusStoreMetadata(version: Long)

private[spark] class ApplicationInfoWrapper(val info: ApplicationInfo) {

  @JsonIgnore @KVIndex
  def id: String = info.id

}

private[spark] class ApplicationEnvironmentInfoWrapper(val info: ApplicationEnvironmentInfo) {

  /**
   * There's always a single ApplicationEnvironmentInfo object per application, so this
   * ID doesn't need to be dynamic. But the KVStore API requires an ID.
   */
  @JsonIgnore @KVIndex
  def id: String = classOf[ApplicationEnvironmentInfoWrapper].getName()

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

  @JsonIgnore @KVIndex("stageId")
  def stageId: Int = info.stageId

}

/**
 * The task information is always indexed with the stage ID, since that is how the UI and API
 * consume it. That means every indexed value has the stage ID and attempt ID included, aside
 * from the actual data being indexed.
 */
private[spark] class TaskDataWrapper(
    val info: TaskData,
    val stageId: Int,
    val stageAttemptId: Int) {

  @JsonIgnore @KVIndex
  def id: Long = info.taskId

  @JsonIgnore @KVIndex("stage")
  def stage: Array[Int] = Array(stageId, stageAttemptId)

  @JsonIgnore @KVIndex("runtime")
  def runtime: Array[AnyRef] = {
    val _runtime = info.taskMetrics.map(_.executorRunTime).getOrElse(-1L)
    Array(stageId: JInteger, stageAttemptId: JInteger, _runtime: JLong)
  }

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

private[spark] class StreamBlockData(
  val name: String,
  val executorId: String,
  val hostPort: String,
  val storageLevel: String,
  val useMemory: Boolean,
  val useDisk: Boolean,
  val deserialized: Boolean,
  val memSize: Long,
  val diskSize: Long) {

  @JsonIgnore @KVIndex
  def key: Array[String] = Array(name, executorId)

}
