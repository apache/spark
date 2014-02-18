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

import scala.collection.mutable.{ListBuffer, HashMap}

import org.apache.spark.{ExceptionFailure, SparkContext, Success}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.ui.UISparkListener

/**
 * Tracks task-level information to be displayed in the UI.
 *
 * All access to the data structures in this class must be synchronized on the
 * class, since the UI thread and the DAGScheduler event loop may otherwise
 * be reading/updating the internal data structures concurrently.
 */
private[spark] class JobProgressListener(sc: SparkContext, fromDisk: Boolean = false)
  extends UISparkListener("job-progress-ui", fromDisk) {

  // How many stages to remember
  val RETAINED_STAGES = sc.conf.getInt("spark.ui.retainedStages", 1000)
  val DEFAULT_POOL_NAME = "default"

  val stageIdToPool = new HashMap[Int, String]()
  val stageIdToDescription = new HashMap[Int, String]()
  val poolToActiveStages = new HashMap[String, HashMap[Int, StageInfo]]()

  val activeStages = HashMap[Int, StageInfo]()
  val completedStages = ListBuffer[StageInfo]()
  val failedStages = ListBuffer[StageInfo]()

  // Total metrics reflect metrics only for completed tasks
  var totalTime = 0L
  var totalShuffleRead = 0L
  var totalShuffleWrite = 0L

  val stageIdToTime = HashMap[Int, Long]()
  val stageIdToShuffleRead = HashMap[Int, Long]()
  val stageIdToShuffleWrite = HashMap[Int, Long]()
  val stageIdToMemoryBytesSpilled = HashMap[Int, Long]()
  val stageIdToDiskBytesSpilled = HashMap[Int, Long]()
  val stageIdToTasksActive = HashMap[Int, HashMap[Long, TaskInfo]]()
  val stageIdToTasksComplete = HashMap[Int, Int]()
  val stageIdToTasksFailed = HashMap[Int, Int]()
  val stageIdToTaskInfos = HashMap[Int, HashMap[Long, TaskUIData]]()
  val stageIdToExecutorSummaries = HashMap[Int, HashMap[String, ExecutorSummary]]()

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) = synchronized {
    val stage = stageCompleted.stageInfo
    val stageId = stage.stageId
    // Remove by stageId, rather than by StageInfo, in case the StageInfo is from disk
    poolToActiveStages(stageIdToPool(stageId)).remove(stageId)
    activeStages.remove(stageId)
    completedStages += stage
    trimIfNecessary(completedStages)
    logEvent(stageCompleted)
  }

  /** If stages is too large, remove and garbage collect old stages */
  private def trimIfNecessary(stages: ListBuffer[StageInfo]) = synchronized {
    if (stages.size > RETAINED_STAGES) {
      val toRemove = RETAINED_STAGES / 10
      stages.takeRight(toRemove).foreach( s => {
        stageIdToTaskInfos.remove(s.stageId)
        stageIdToTime.remove(s.stageId)
        stageIdToShuffleRead.remove(s.stageId)
        stageIdToShuffleWrite.remove(s.stageId)
        stageIdToMemoryBytesSpilled.remove(s.stageId)
        stageIdToDiskBytesSpilled.remove(s.stageId)
        stageIdToTasksActive.remove(s.stageId)
        stageIdToTasksComplete.remove(s.stageId)
        stageIdToTasksFailed.remove(s.stageId)
        stageIdToPool.remove(s.stageId)
        if (stageIdToDescription.contains(s.stageId)) {stageIdToDescription.remove(s.stageId)}
      })
      stages.trimEnd(toRemove)
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
    logEvent(stageSubmitted)
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart) = synchronized {
    val sid = taskStart.stageId
    val taskInfo = taskStart.taskInfo
    val tasksActive = stageIdToTasksActive.getOrElseUpdate(sid, new HashMap[Long, TaskInfo]())
    tasksActive(taskInfo.taskId) = taskInfo
    val taskMap = stageIdToTaskInfos.getOrElse(sid, HashMap[Long, TaskUIData]())
    taskMap(taskInfo.taskId) = new TaskUIData(taskInfo)
    stageIdToTaskInfos(sid) = taskMap
    logEvent(taskStart)
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult)
      = synchronized {
    // Do nothing: because we don't do a deep copy of the TaskInfo, the TaskInfo in
    // stageToTaskInfos already has the updated status.
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) = synchronized {
    val sid = taskEnd.stageId

    // create executor summary map if necessary
    val executorSummaryMap = stageIdToExecutorSummaries.getOrElseUpdate(key = sid,
      op = new HashMap[String, ExecutorSummary]())
    executorSummaryMap.getOrElseUpdate(key = taskEnd.taskInfo.executorId,
      op = new ExecutorSummary())

    val executorSummary = executorSummaryMap.get(taskEnd.taskInfo.executorId)
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
        y.taskTime += taskEnd.taskInfo.duration

        Option(taskEnd.taskMetrics).foreach { taskMetrics =>
          taskMetrics.shuffleReadMetrics.foreach { y.shuffleRead += _.remoteBytesRead }
          taskMetrics.shuffleWriteMetrics.foreach { y.shuffleWrite += _.shuffleBytesWritten }
          y.memoryBytesSpilled += taskMetrics.memoryBytesSpilled
          y.diskBytesSpilled += taskMetrics.diskBytesSpilled
        }
      }
      case _ => {}
    }

    val tasksActive = stageIdToTasksActive.getOrElseUpdate(sid, new HashMap[Long, TaskInfo]())
    // Remove by taskId, rather than by TaskInfo, in case the TaskInfo is from disk
    tasksActive.remove(taskEnd.taskInfo.taskId)

    val (failureInfo, metrics): (Option[ExceptionFailure], Option[TaskMetrics]) =
      taskEnd.reason match {
        case e: ExceptionFailure =>
          stageIdToTasksFailed(sid) = stageIdToTasksFailed.getOrElse(sid, 0) + 1
          (Some(e), e.metrics)
        case _ =>
          stageIdToTasksComplete(sid) = stageIdToTasksComplete.getOrElse(sid, 0) + 1
          (None, Option(taskEnd.taskMetrics))
      }

    stageIdToTime.getOrElseUpdate(sid, 0L)
    val time = metrics.map(m => m.executorRunTime).getOrElse(0L)
    stageIdToTime(sid) += time
    totalTime += time

    stageIdToShuffleRead.getOrElseUpdate(sid, 0L)
    val shuffleRead = metrics.flatMap(m => m.shuffleReadMetrics).map(s =>
      s.remoteBytesRead).getOrElse(0L)
    stageIdToShuffleRead(sid) += shuffleRead
    totalShuffleRead += shuffleRead

    stageIdToShuffleWrite.getOrElseUpdate(sid, 0L)
    val shuffleWrite = metrics.flatMap(m => m.shuffleWriteMetrics).map(s =>
      s.shuffleBytesWritten).getOrElse(0L)
    stageIdToShuffleWrite(sid) += shuffleWrite
    totalShuffleWrite += shuffleWrite

    stageIdToMemoryBytesSpilled.getOrElseUpdate(sid, 0L)
    val memoryBytesSpilled = metrics.map(m => m.memoryBytesSpilled).getOrElse(0L)
    stageIdToMemoryBytesSpilled(sid) += memoryBytesSpilled

    stageIdToDiskBytesSpilled.getOrElseUpdate(sid, 0L)
    val diskBytesSpilled = metrics.map(m => m.diskBytesSpilled).getOrElse(0L)
    stageIdToDiskBytesSpilled(sid) += diskBytesSpilled

    val taskMap = stageIdToTaskInfos.getOrElse(sid, HashMap[Long, TaskUIData]())
    val taskInfo = taskEnd.taskInfo
    taskMap(taskInfo.taskId) = new TaskUIData(taskInfo, metrics, failureInfo)
    stageIdToTaskInfos(sid) = taskMap
    logEvent(taskEnd)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) = synchronized {
    jobEnd.jobResult match {
      case JobFailed(_, stageId) =>
        activeStages.get(stageId).foreach { s =>
          // Remove by stageId, rather than by StageInfo, in case the StageInfo is from disk
          activeStages.remove(s.stageId)
          poolToActiveStages(stageIdToPool(stageId)).remove(s.stageId)
          failedStages += s
          trimIfNecessary(failedStages)
        }
      case _ =>
    }
    logEvent(jobEnd)
    logger.foreach(_.close())
  }
}

private[spark] case class TaskUIData(
  taskInfo: TaskInfo,
  taskMetrics: Option[TaskMetrics] = None,
  exception: Option[ExceptionFailure] = None)
