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

package org.apache.spark.deploy.history

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._

/**
 * This class tracks both live jobs and live executors, and pass the list to the
 * [[BasicEventFilter]] to help BasicEventFilter to filter out finished jobs
 * (+ stages/tasks/RDDs) and dead executors.
 */
private[spark] class BasicEventFilterBuilder extends SparkListener with EventFilterBuilder {
  private val _liveJobToStages = new mutable.HashMap[Int, Seq[Int]]
  private val _stageToTasks = new mutable.HashMap[Int, mutable.Set[Long]]
  private val _stageToRDDs = new mutable.HashMap[Int, Seq[Int]]
  private val _liveExecutors = new mutable.HashSet[String]

  def liveJobToStages: Map[Int, Seq[Int]] = _liveJobToStages.toMap
  def stageToTasks: Map[Int, Set[Long]] = _stageToTasks.mapValues(_.toSet).toMap
  def stageToRDDs: Map[Int, Seq[Int]] = _stageToRDDs.toMap
  def liveExecutors: Set[String] = _liveExecutors.toSet

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    _liveJobToStages += jobStart.jobId -> jobStart.stageIds
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val stages = _liveJobToStages.getOrElse(jobEnd.jobId, Seq.empty[Int])
    _liveJobToStages -= jobEnd.jobId
    _stageToTasks --= stages
    _stageToRDDs --= stages
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    _stageToRDDs.getOrElseUpdate(stageSubmitted.stageInfo.stageId,
      stageSubmitted.stageInfo.rddInfos.map(_.id))
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    val curTasks = _stageToTasks.getOrElseUpdate(taskStart.stageId,
      mutable.HashSet[Long]())
    curTasks += taskStart.taskInfo.taskId
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    _liveExecutors += executorAdded.executorId
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    _liveExecutors -= executorRemoved.executorId
  }

  override def createFilter(): EventFilter = BasicEventFilter(this)
}

/**
 * This class provides the functionality to filter out events which are related to the finished
 * jobs based on the given information. This class only deals with job related events, and returns
 * either Some(true) or Some(false) - successors should override the methods if they don't want to
 * return Some(false) for finished jobs and related events.
 */
private[spark] abstract class JobEventFilter(
    jobToStages: Map[Int, Seq[Int]],
    stageToTasks: Map[Int, Set[Long]],
    stageToRDDs: Map[Int, Seq[Int]]) extends EventFilter with Logging {

  private val liveTasks: Set[Long] = stageToTasks.values match {
    case xs if xs.isEmpty => Set.empty[Long]
    case xs => xs.reduce(_ ++ _).toSet
  }

  private val liveRDDs: Set[Int] = stageToRDDs.values match {
    case xs if xs.isEmpty => Set.empty[Int]
    case xs => xs.reduce(_ ++ _).toSet
  }

  logDebug(s"jobs : ${jobToStages.keySet}")
  logDebug(s"stages in jobs : ${jobToStages.values.flatten}")
  logDebug(s"stages : ${stageToTasks.keySet}")
  logDebug(s"tasks in stages : ${stageToTasks.values.flatten}")
  logDebug(s"RDDs in stages : ${stageToRDDs.values.flatten}")

  override def filterStageCompleted(event: SparkListenerStageCompleted): Option[Boolean] = {
    Some(stageToTasks.contains(event.stageInfo.stageId))
  }

  override def filterStageSubmitted(event: SparkListenerStageSubmitted): Option[Boolean] = {
    Some(stageToTasks.contains(event.stageInfo.stageId))
  }

  override def filterTaskStart(event: SparkListenerTaskStart): Option[Boolean] = {
    Some(liveTasks.contains(event.taskInfo.taskId))
  }

  override def filterTaskGettingResult(event: SparkListenerTaskGettingResult): Option[Boolean] = {
    Some(liveTasks.contains(event.taskInfo.taskId))
  }

  override def filterTaskEnd(event: SparkListenerTaskEnd): Option[Boolean] = {
    Some(liveTasks.contains(event.taskInfo.taskId))
  }

  override def filterJobStart(event: SparkListenerJobStart): Option[Boolean] = {
    Some(jobToStages.contains(event.jobId))
  }

  override def filterJobEnd(event: SparkListenerJobEnd): Option[Boolean] = {
    Some(jobToStages.contains(event.jobId))
  }

  override def filterUnpersistRDD(event: SparkListenerUnpersistRDD): Option[Boolean] = {
    Some(liveRDDs.contains(event.rddId))
  }

  override def filterExecutorMetricsUpdate(
      event: SparkListenerExecutorMetricsUpdate): Option[Boolean] = {
    Some(event.accumUpdates.exists { case (_, stageId, _, _) =>
      stageToTasks.contains(stageId)
    })
  }

  override def filterSpeculativeTaskSubmitted(
      event: SparkListenerSpeculativeTaskSubmitted): Option[Boolean] = {
    Some(stageToTasks.contains(event.stageId))
  }
}

/**
 * This class filters out events which are related to the finished jobs or dead executors,
 * based on the given information. The events which are not related to the job and executor
 * will be considered as "Don't mind".
 */
private[spark] class BasicEventFilter(
    _liveJobToStages: Map[Int, Seq[Int]],
    _stageToTasks: Map[Int, Set[Long]],
    _stageToRDDs: Map[Int, Seq[Int]],
    liveExecutors: Set[String])
  extends JobEventFilter(_liveJobToStages, _stageToTasks, _stageToRDDs) with Logging {

  logDebug(s"live executors : $liveExecutors")

  override def filterExecutorAdded(event: SparkListenerExecutorAdded): Option[Boolean] = {
    Some(liveExecutors.contains(event.executorId))
  }

  override def filterExecutorRemoved(event: SparkListenerExecutorRemoved): Option[Boolean] = {
    Some(liveExecutors.contains(event.executorId))
  }

  override def filterExecutorBlacklisted(
      event: SparkListenerExecutorBlacklisted): Option[Boolean] = {
    Some(liveExecutors.contains(event.executorId))
  }

  override def filterExecutorUnblacklisted(
      event: SparkListenerExecutorUnblacklisted): Option[Boolean] = {
    Some(liveExecutors.contains(event.executorId))
  }

  override def filterStageExecutorMetrics(
      event: SparkListenerStageExecutorMetrics): Option[Boolean] = {
    Some(liveExecutors.contains(event.execId))
  }
}

private[spark] object BasicEventFilter {
  def apply(builder: BasicEventFilterBuilder): BasicEventFilter = {
    new BasicEventFilter(
      builder.liveJobToStages,
      builder.stageToTasks,
      builder.stageToRDDs,
      builder.liveExecutors)
  }
}
