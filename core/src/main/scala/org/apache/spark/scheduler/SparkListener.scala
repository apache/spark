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

import java.util.Properties
import javax.annotation.Nullable

import scala.collection.Map

import com.fasterxml.jackson.annotation.JsonTypeInfo

import org.apache.spark.{SparkConf, TaskEndReason}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.{ExecutorMetrics, TaskMetrics}
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.storage.{BlockManagerId, BlockUpdatedInfo}
import org.apache.spark.ui.SparkUI

@DeveloperApi
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "Event")
trait SparkListenerEvent {
  /* Whether output this event to the event log */
  protected[spark] def logEvent: Boolean = true
}

@DeveloperApi
case class SparkListenerStageSubmitted(stageInfo: StageInfo, properties: Properties = null)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerStageCompleted(stageInfo: StageInfo) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerTaskStart(stageId: Int, stageAttemptId: Int, taskInfo: TaskInfo)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerTaskGettingResult(taskInfo: TaskInfo) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerSpeculativeTaskSubmitted(stageId: Int) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerTaskEnd(
    stageId: Int,
    stageAttemptId: Int,
    taskType: String,
    reason: TaskEndReason,
    taskInfo: TaskInfo,
    // may be null if the task has failed
    @Nullable taskMetrics: TaskMetrics)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerJobStart(
    jobId: Int,
    time: Long,
    stageInfos: Seq[StageInfo],
    properties: Properties = null)
  extends SparkListenerEvent {
  // Note: this is here for backwards-compatibility with older versions of this event which
  // only stored stageIds and not StageInfos:
  val stageIds: Seq[Int] = stageInfos.map(_.stageId)
}

@DeveloperApi
case class SparkListenerJobEnd(
    jobId: Int,
    time: Long,
    jobResult: JobResult)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerEnvironmentUpdate(environmentDetails: Map[String, Seq[(String, String)]])
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerBlockManagerAdded(
    time: Long,
    blockManagerId: BlockManagerId,
    maxMem: Long,
    maxOnHeapMem: Option[Long] = None,
    maxOffHeapMem: Option[Long] = None) extends SparkListenerEvent {
}

@DeveloperApi
case class SparkListenerBlockManagerRemoved(time: Long, blockManagerId: BlockManagerId)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerUnpersistRDD(rddId: Int) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerExecutorAdded(time: Long, executorId: String, executorInfo: ExecutorInfo)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerExecutorRemoved(time: Long, executorId: String, reason: String)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerExecutorBlacklisted(
    time: Long,
    executorId: String,
    taskFailures: Int)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerExecutorBlacklistedForStage(
    time: Long,
    executorId: String,
    taskFailures: Int,
    stageId: Int,
    stageAttemptId: Int)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerNodeBlacklistedForStage(
    time: Long,
    hostId: String,
    executorFailures: Int,
    stageId: Int,
    stageAttemptId: Int)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerExecutorUnblacklisted(time: Long, executorId: String)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerNodeBlacklisted(
    time: Long,
    hostId: String,
    executorFailures: Int)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerNodeUnblacklisted(time: Long, hostId: String)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerBlockUpdated(blockUpdatedInfo: BlockUpdatedInfo) extends SparkListenerEvent

/**
 * Periodic updates from executors.
 * @param execId executor id
 * @param accumUpdates sequence of (taskId, stageId, stageAttemptId, accumUpdates)
 * @param executorUpdates executor level metrics updates
 */
@DeveloperApi
case class SparkListenerExecutorMetricsUpdate(
    execId: String,
    accumUpdates: Seq[(Long, Int, Int, Seq[AccumulableInfo])],
    executorUpdates: Option[ExecutorMetrics] = None)
  extends SparkListenerEvent

/**
 * Peak metric values for the executor for the stage, written to the history log at stage
 * completion.
 * @param execId executor id
 * @param stageId stage id
 * @param stageAttemptId stage attempt
 * @param executorMetrics executor level metrics, indexed by ExecutorMetricType.values
 */
@DeveloperApi
case class SparkListenerStageExecutorMetrics(
    execId: String,
    stageId: Int,
    stageAttemptId: Int,
    executorMetrics: ExecutorMetrics)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerApplicationStart(
    appName: String,
    appId: Option[String],
    time: Long,
    sparkUser: String,
    appAttemptId: Option[String],
    driverLogs: Option[Map[String, String]] = None,
    driverAttributes: Option[Map[String, String]] = None) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerApplicationEnd(time: Long) extends SparkListenerEvent

/**
 * An internal class that describes the metadata of an event log.
 */
@DeveloperApi
case class SparkListenerLogStart(sparkVersion: String) extends SparkListenerEvent

/**
 * Interface for listening to events from the Spark scheduler. Most applications should probably
 * extend SparkListener or SparkFirehoseListener directly, rather than implementing this class.
 *
 * Note that this is an internal interface which might change in different Spark releases.
 */
private[spark] trait SparkListenerInterface {

  /**
   * Called when a stage completes successfully or fails, with information on the completed stage.
   */
  def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit

  /**
   * Called when a stage is submitted
   */
  def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit

  /**
   * Called when a task starts
   */
  def onTaskStart(taskStart: SparkListenerTaskStart): Unit

  /**
   * Called when a task begins remotely fetching its result (will not be called for tasks that do
   * not need to fetch the result remotely).
   */
  def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit

  /**
   * Called when a task ends
   */
  def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit

  /**
   * Called when a job starts
   */
  def onJobStart(jobStart: SparkListenerJobStart): Unit

  /**
   * Called when a job ends
   */
  def onJobEnd(jobEnd: SparkListenerJobEnd): Unit

  /**
   * Called when environment properties have been updated
   */
  def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit

  /**
   * Called when a new block manager has joined
   */
  def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit

  /**
   * Called when an existing block manager has been removed
   */
  def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit

  /**
   * Called when an RDD is manually unpersisted by the application
   */
  def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit

  /**
   * Called when the application starts
   */
  def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit

  /**
   * Called when the application ends
   */
  def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit

  /**
   * Called when the driver receives task metrics from an executor in a heartbeat.
   */
  def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit

  /**
   * Called with the peak memory metrics for a given (executor, stage) combination. Note that this
   * is only present when reading from the event log (as in the history server), and is never
   * called in a live application.
   */
  def onStageExecutorMetrics(executorMetrics: SparkListenerStageExecutorMetrics): Unit

  /**
   * Called when the driver registers a new executor.
   */
  def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit

  /**
   * Called when the driver removes an executor.
   */
  def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit

  /**
   * Called when the driver blacklists an executor for a Spark application.
   */
  def onExecutorBlacklisted(executorBlacklisted: SparkListenerExecutorBlacklisted): Unit

  /**
   * Called when the driver blacklists an executor for a stage.
   */
  def onExecutorBlacklistedForStage(
      executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage): Unit

  /**
   * Called when the driver blacklists a node for a stage.
   */
  def onNodeBlacklistedForStage(nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage): Unit

  /**
   * Called when the driver re-enables a previously blacklisted executor.
   */
  def onExecutorUnblacklisted(executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit

  /**
   * Called when the driver blacklists a node for a Spark application.
   */
  def onNodeBlacklisted(nodeBlacklisted: SparkListenerNodeBlacklisted): Unit

  /**
   * Called when the driver re-enables a previously blacklisted node.
   */
  def onNodeUnblacklisted(nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit

  /**
   * Called when the driver receives a block update info.
   */
  def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit

  /**
   * Called when a speculative task is submitted
   */
  def onSpeculativeTaskSubmitted(speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit

  /**
   * Called when other events like SQL-specific events are posted.
   */
  def onOtherEvent(event: SparkListenerEvent): Unit
}


/**
 * :: DeveloperApi ::
 * A default implementation for `SparkListenerInterface` that has no-op implementations for
 * all callbacks.
 *
 * Note that this is an internal interface which might change in different Spark releases.
 */
@DeveloperApi
abstract class SparkListener extends SparkListenerInterface {
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = { }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = { }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = { }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = { }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = { }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = { }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = { }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = { }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = { }

  override def onBlockManagerRemoved(
      blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = { }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = { }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = { }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = { }

  override def onExecutorMetricsUpdate(
      executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = { }

  override def onStageExecutorMetrics(
      executorMetrics: SparkListenerStageExecutorMetrics): Unit = { }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = { }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = { }

  override def onExecutorBlacklisted(
      executorBlacklisted: SparkListenerExecutorBlacklisted): Unit = { }

  def onExecutorBlacklistedForStage(
      executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage): Unit = { }

  def onNodeBlacklistedForStage(
      nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage): Unit = { }

  override def onExecutorUnblacklisted(
      executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit = { }

  override def onNodeBlacklisted(
      nodeBlacklisted: SparkListenerNodeBlacklisted): Unit = { }

  override def onNodeUnblacklisted(
      nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit = { }

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = { }

  override def onSpeculativeTaskSubmitted(
      speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit = { }

  override def onOtherEvent(event: SparkListenerEvent): Unit = { }
}
