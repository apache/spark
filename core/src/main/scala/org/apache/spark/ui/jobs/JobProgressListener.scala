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

import scala.collection.mutable.{HashMap, ListBuffer}

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.storage.BlockManagerId

/**
 * :: DeveloperApi ::
 * Tracks task-level information to be displayed in the UI.
 *
 * All access to the data structures in this class must be synchronized on the
 * class, since the UI thread and the EventBus loop may otherwise be reading and
 * updating the internal data structures concurrently.
 */
@DeveloperApi
class JobProgressListener(conf: SparkConf) extends SparkListener {

  import JobProgressListener._

  // How many stages to remember
  val retainedStages = conf.getInt("spark.ui.retainedStages", DEFAULT_RETAINED_STAGES)

  val activeStages = HashMap[Int, StageInfo]()
  val completedStages = ListBuffer[StageInfo]()
  val failedStages = ListBuffer[StageInfo]()

  // TODO: Should probably consolidate all following into a single hash map.
  val stageIdToTime = HashMap[Int, Long]()
  val stageIdToInputBytes = HashMap[Int, Long]()
  val stageIdToShuffleRead = HashMap[Int, Long]()
  val stageIdToShuffleWrite = HashMap[Int, Long]()
  val stageIdToMemoryBytesSpilled = HashMap[Int, Long]()
  val stageIdToDiskBytesSpilled = HashMap[Int, Long]()
  val stageIdToTasksActive = HashMap[Int, HashMap[Long, TaskInfo]]()
  val stageIdToTasksComplete = HashMap[Int, Int]()
  val stageIdToTasksFailed = HashMap[Int, Int]()
  val stageIdToTaskData = HashMap[Int, HashMap[Long, TaskUIData]]()
  val stageIdToExecutorSummaries = HashMap[Int, HashMap[String, ExecutorSummary]]()
  val stageIdToPool = HashMap[Int, String]()
  val stageIdToDescription = HashMap[Int, String]()
  val poolToActiveStages = HashMap[String, HashMap[Int, StageInfo]]()

  val executorIdToBlockManagerId = HashMap[String, BlockManagerId]()

  var schedulingMode: Option[SchedulingMode] = None

  def blockManagerIds = executorIdToBlockManagerId.values.toSeq

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) = synchronized {
    val stage = stageCompleted.stageInfo
    val stageId = stage.stageId
    // Remove by stageId, rather than by StageInfo, in case the StageInfo is from storage
    poolToActiveStages(stageIdToPool(stageId)).remove(stageId)
    activeStages.remove(stageId)
    if (stage.failureReason.isEmpty) {
      completedStages += stage
      trimIfNecessary(completedStages)
    } else {
      failedStages += stage
      trimIfNecessary(failedStages)
    }
  }

  /** If stages is too large, remove and garbage collect old stages */
  private def trimIfNecessary(stages: ListBuffer[StageInfo]) = synchronized {
    if (stages.size > retainedStages) {
      val toRemove = math.max(retainedStages / 10, 1)
      stages.take(toRemove).foreach { s =>
        stageIdToTime.remove(s.stageId)
        stageIdToInputBytes.remove(s.stageId)
        stageIdToShuffleRead.remove(s.stageId)
        stageIdToShuffleWrite.remove(s.stageId)
        stageIdToMemoryBytesSpilled.remove(s.stageId)
        stageIdToDiskBytesSpilled.remove(s.stageId)
        stageIdToTasksActive.remove(s.stageId)
        stageIdToTasksComplete.remove(s.stageId)
        stageIdToTasksFailed.remove(s.stageId)
        stageIdToTaskData.remove(s.stageId)
        stageIdToExecutorSummaries.remove(s.stageId)
        stageIdToPool.remove(s.stageId)
        stageIdToDescription.remove(s.stageId)
      }
      stages.trimStart(toRemove)
    }
  }

  /** For FIFO, all stages are contained by "default" pool but "default" pool here is meaningless */
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) = synchronized {
    val stage = stageSubmitted.stageInfo
    activeStages(stage.stageId) = stage

    val poolName = Option(stageSubmitted.properties).map {
      p => p.getProperty("spark.scheduler.pool", DEFAULT_POOL_NAME)
    }.getOrElse(DEFAULT_POOL_NAME)
    stageIdToPool(stage.stageId) = poolName

    val description = Option(stageSubmitted.properties).flatMap {
      p => Option(p.getProperty(SparkContext.SPARK_JOB_DESCRIPTION))
    }
    description.map(d => stageIdToDescription(stage.stageId) = d)

    val stages = poolToActiveStages.getOrElseUpdate(poolName, new HashMap[Int, StageInfo]())
    stages(stage.stageId) = stage
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart) = synchronized {
    val sid = taskStart.stageId
    val taskInfo = taskStart.taskInfo
    if (taskInfo != null) {
      val tasksActive = stageIdToTasksActive.getOrElseUpdate(sid, new HashMap[Long, TaskInfo]())
      tasksActive(taskInfo.taskId) = taskInfo
      val taskMap = stageIdToTaskData.getOrElse(sid, HashMap[Long, TaskUIData]())
      taskMap(taskInfo.taskId) = new TaskUIData(taskInfo)
      stageIdToTaskData(sid) = taskMap
    }
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) {
    // Do nothing: because we don't do a deep copy of the TaskInfo, the TaskInfo in
    // stageToTaskInfos already has the updated status.
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) = synchronized {
    val sid = taskEnd.stageId
    val info = taskEnd.taskInfo

    if (info != null) {
      // create executor summary map if necessary
      val executorSummaryMap = stageIdToExecutorSummaries.getOrElseUpdate(key = sid,
        op = new HashMap[String, ExecutorSummary]())
      executorSummaryMap.getOrElseUpdate(key = info.executorId, op = new ExecutorSummary)

      val executorSummary = executorSummaryMap.get(info.executorId)
      executorSummary match {
        case Some(y) => {
          // first update failed-task, succeed-task
          taskEnd.reason match {
            case Success =>
              y.succeededTasks += 1
            case _ =>
              y.failedTasks += 1
          }

          // update duration
          y.taskTime += info.duration

          val metrics = taskEnd.taskMetrics
          if (metrics != null) {
            metrics.inputMetrics.foreach { y.inputBytes += _.bytesRead }
            metrics.shuffleReadMetrics.foreach { y.shuffleRead += _.remoteBytesRead }
            metrics.shuffleWriteMetrics.foreach { y.shuffleWrite += _.shuffleBytesWritten }
            y.memoryBytesSpilled += metrics.memoryBytesSpilled
            y.diskBytesSpilled += metrics.diskBytesSpilled
          }
        }
        case _ => {}
      }

      val tasksActive = stageIdToTasksActive.getOrElseUpdate(sid, new HashMap[Long, TaskInfo]())
      // Remove by taskId, rather than by TaskInfo, in case the TaskInfo is from storage
      tasksActive.remove(info.taskId)

      val (errorMessage, metrics): (Option[String], Option[TaskMetrics]) =
        taskEnd.reason match {
          case org.apache.spark.Success =>
            stageIdToTasksComplete(sid) = stageIdToTasksComplete.getOrElse(sid, 0) + 1
            (None, Option(taskEnd.taskMetrics))
          case e: ExceptionFailure =>  // Handle ExceptionFailure because we might have metrics
            stageIdToTasksFailed(sid) = stageIdToTasksFailed.getOrElse(sid, 0) + 1
            (Some(e.toErrorString), e.metrics)
          case e: TaskFailedReason =>  // All other failure cases
            stageIdToTasksFailed(sid) = stageIdToTasksFailed.getOrElse(sid, 0) + 1
            (Some(e.toErrorString), None)
        }

      stageIdToTime.getOrElseUpdate(sid, 0L)
      val time = metrics.map(_.executorRunTime).getOrElse(0L)
      stageIdToTime(sid) += time

      stageIdToInputBytes.getOrElseUpdate(sid, 0L)
      val inputBytes = metrics.flatMap(_.inputMetrics).map(_.bytesRead).getOrElse(0L)
      stageIdToInputBytes(sid) += inputBytes

      stageIdToShuffleRead.getOrElseUpdate(sid, 0L)
      val shuffleRead = metrics.flatMap(_.shuffleReadMetrics).map(_.remoteBytesRead).getOrElse(0L)
      stageIdToShuffleRead(sid) += shuffleRead

      stageIdToShuffleWrite.getOrElseUpdate(sid, 0L)
      val shuffleWrite =
        metrics.flatMap(_.shuffleWriteMetrics).map(_.shuffleBytesWritten).getOrElse(0L)
      stageIdToShuffleWrite(sid) += shuffleWrite

      stageIdToMemoryBytesSpilled.getOrElseUpdate(sid, 0L)
      val memoryBytesSpilled = metrics.map(_.memoryBytesSpilled).getOrElse(0L)
      stageIdToMemoryBytesSpilled(sid) += memoryBytesSpilled

      stageIdToDiskBytesSpilled.getOrElseUpdate(sid, 0L)
      val diskBytesSpilled = metrics.map(_.diskBytesSpilled).getOrElse(0L)
      stageIdToDiskBytesSpilled(sid) += diskBytesSpilled

      val taskMap = stageIdToTaskData.getOrElse(sid, HashMap[Long, TaskUIData]())
      taskMap(info.taskId) = new TaskUIData(info, metrics, errorMessage)
      stageIdToTaskData(sid) = taskMap
    }
  }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate) {
    synchronized {
      schedulingMode = environmentUpdate
        .environmentDetails("Spark Properties").toMap
        .get("spark.scheduler.mode")
        .map(SchedulingMode.withName)
    }
  }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded) {
    synchronized {
      val blockManagerId = blockManagerAdded.blockManagerId
      val executorId = blockManagerId.executorId
      executorIdToBlockManagerId(executorId) = blockManagerId
    }
  }

  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved) {
    synchronized {
      val executorId = blockManagerRemoved.blockManagerId.executorId
      executorIdToBlockManagerId.remove(executorId)
    }
  }

}

@DeveloperApi
case class TaskUIData(
    taskInfo: TaskInfo,
    taskMetrics: Option[TaskMetrics] = None,
    errorMessage: Option[String] = None)

private object JobProgressListener {
  val DEFAULT_POOL_NAME = "default"
  val DEFAULT_RETAINED_STAGES = 1000
}
