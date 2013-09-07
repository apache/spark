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

import scala.Seq
import scala.collection.mutable.{ListBuffer, HashMap, HashSet}

import org.apache.spark.{ExceptionFailure, SparkContext, Success}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.TaskInfo
import org.apache.spark.executor.TaskMetrics
import collection.mutable

/**
 * Tracks task-level information to be displayed in the UI.
 *
 * All access to the data structures in this class must be synchronized on the
 * class, since the UI thread and the DAGScheduler event loop may otherwise
 * be reading/updating the internal data structures concurrently.
 */
private[spark] class JobProgressListener(val sc: SparkContext) extends SparkListener {
  // How many stages to remember
  val RETAINED_STAGES = System.getProperty("spark.ui.retained_stages", "1000").toInt
  val DEFAULT_POOL_NAME = "default"

  val stageToPool = new HashMap[Stage, String]()
  val stageToDescription = new HashMap[Stage, String]()
  val poolToActiveStages = new HashMap[String, HashSet[Stage]]()

  val activeStages = HashSet[Stage]()
  val completedStages = ListBuffer[Stage]()
  val failedStages = ListBuffer[Stage]()

  // Total metrics reflect metrics only for completed tasks
  var totalTime = 0L
  var totalShuffleRead = 0L
  var totalShuffleWrite = 0L

  val stageToTime = HashMap[Int, Long]()
  val stageToShuffleRead = HashMap[Int, Long]()
  val stageToShuffleWrite = HashMap[Int, Long]()
  val stageToTasksActive = HashMap[Int, HashSet[TaskInfo]]()
  val stageToTasksComplete = HashMap[Int, Int]()
  val stageToTasksFailed = HashMap[Int, Int]()
  val stageToTaskInfos =
    HashMap[Int, HashSet[(TaskInfo, Option[TaskMetrics], Option[ExceptionFailure])]]()

  override def onJobStart(jobStart: SparkListenerJobStart) {}

  override def onStageCompleted(stageCompleted: StageCompleted) = synchronized {
    val stage = stageCompleted.stageInfo.stage
    poolToActiveStages(stageToPool(stage)) -= stage
    activeStages -= stage
    completedStages += stage
    trimIfNecessary(completedStages)
  }

  /** If stages is too large, remove and garbage collect old stages */
  def trimIfNecessary(stages: ListBuffer[Stage]) = synchronized {
    if (stages.size > RETAINED_STAGES) {
      val toRemove = RETAINED_STAGES / 10
      stages.takeRight(toRemove).foreach( s => {
        stageToTaskInfos.remove(s.id)
        stageToTime.remove(s.id)
        stageToShuffleRead.remove(s.id)
        stageToShuffleWrite.remove(s.id)
        stageToTasksActive.remove(s.id)
        stageToTasksComplete.remove(s.id)
        stageToTasksFailed.remove(s.id)
        stageToPool.remove(s)
        if (stageToDescription.contains(s)) {stageToDescription.remove(s)}
      })
      stages.trimEnd(toRemove)
    }
  }

  /** For FIFO, all stages are contained by "default" pool but "default" pool here is meaningless */
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) = synchronized {
    val stage = stageSubmitted.stage
    activeStages += stage

    val poolName = Option(stageSubmitted.properties).map {
      p => p.getProperty("spark.scheduler.pool", DEFAULT_POOL_NAME)
    }.getOrElse(DEFAULT_POOL_NAME)
    stageToPool(stage) = poolName

    val description = Option(stageSubmitted.properties).flatMap {
      p => Option(p.getProperty(SparkContext.SPARK_JOB_DESCRIPTION))
    }
    description.map(d => stageToDescription(stage) = d)

    val stages = poolToActiveStages.getOrElseUpdate(poolName, new HashSet[Stage]())
    stages += stage
  }
  
  override def onTaskStart(taskStart: SparkListenerTaskStart) = synchronized {
    val sid = taskStart.task.stageId
    val tasksActive = stageToTasksActive.getOrElseUpdate(sid, new HashSet[TaskInfo]())
    tasksActive += taskStart.taskInfo
    val taskList = stageToTaskInfos.getOrElse(
      sid, HashSet[(TaskInfo, Option[TaskMetrics], Option[ExceptionFailure])]())
    taskList += ((taskStart.taskInfo, None, None))
    stageToTaskInfos(sid) = taskList
  }
 
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) = synchronized {
    val sid = taskEnd.task.stageId
    val tasksActive = stageToTasksActive.getOrElseUpdate(sid, new HashSet[TaskInfo]())
    tasksActive -= taskEnd.taskInfo
    val (failureInfo, metrics): (Option[ExceptionFailure], Option[TaskMetrics]) =
      taskEnd.reason match {
        case e: ExceptionFailure =>
          stageToTasksFailed(sid) = stageToTasksFailed.getOrElse(sid, 0) + 1
          (Some(e), e.metrics)
        case _ =>
          stageToTasksComplete(sid) = stageToTasksComplete.getOrElse(sid, 0) + 1
          (None, Option(taskEnd.taskMetrics))
      }

    stageToTime.getOrElseUpdate(sid, 0L)
    val time = metrics.map(m => m.executorRunTime).getOrElse(0)
    stageToTime(sid) += time
    totalTime += time

    stageToShuffleRead.getOrElseUpdate(sid, 0L)
    val shuffleRead = metrics.flatMap(m => m.shuffleReadMetrics).map(s =>
      s.remoteBytesRead).getOrElse(0L)
    stageToShuffleRead(sid) += shuffleRead
    totalShuffleRead += shuffleRead

    stageToShuffleWrite.getOrElseUpdate(sid, 0L)
    val shuffleWrite = metrics.flatMap(m => m.shuffleWriteMetrics).map(s =>
      s.shuffleBytesWritten).getOrElse(0L)
    stageToShuffleWrite(sid) += shuffleWrite
    totalShuffleWrite += shuffleWrite

    val taskList = stageToTaskInfos.getOrElse(
      sid, HashSet[(TaskInfo, Option[TaskMetrics], Option[ExceptionFailure])]())
    taskList -= ((taskEnd.taskInfo, None, None))
    taskList += ((taskEnd.taskInfo, metrics, failureInfo))
    stageToTaskInfos(sid) = taskList
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) = synchronized {
    jobEnd match {
      case end: SparkListenerJobEnd =>
        end.jobResult match {
          case JobFailed(ex, Some(stage)) =>
            activeStages -= stage
            poolToActiveStages(stageToPool(stage)) -= stage
            failedStages += stage
            trimIfNecessary(failedStages)
          case _ =>
        }
      case _ =>
    }
  }
}
