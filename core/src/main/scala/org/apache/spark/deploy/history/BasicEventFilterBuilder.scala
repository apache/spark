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

import org.apache.spark.deploy.history.EventFilter.FilterStatistic
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._

/**
 * This class tracks both live jobs and live executors, and pass the list to the
 * [[BasicEventFilter]] to help BasicEventFilter to reject finished jobs (+ stages/tasks/RDDs)
 * and dead executors.
 */
private[spark] class BasicEventFilterBuilder extends SparkListener with EventFilterBuilder {
  private val _liveJobToStages = new mutable.HashMap[Int, Seq[Int]]
  private val _stageToTasks = new mutable.HashMap[Int, mutable.Set[Long]]
  private val _stageToRDDs = new mutable.HashMap[Int, Seq[Int]]
  private val _liveExecutors = new mutable.HashSet[String]

  private var totalJobs: Long = 0L
  private var totalStages: Long = 0L
  private var totalTasks: Long = 0L

  def liveJobToStages: Map[Int, Seq[Int]] = _liveJobToStages.toMap
  def stageToTasks: Map[Int, Set[Long]] = _stageToTasks.mapValues(_.toSet).toMap
  def stageToRDDs: Map[Int, Seq[Int]] = _stageToRDDs.toMap
  def liveExecutors: Set[String] = _liveExecutors.toSet

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    totalJobs += 1
    totalStages += jobStart.stageIds.length
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
    totalTasks += 1
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

  override def createFilter(): EventFilter = new BasicEventFilter(this)

  def statistic(): FilterStatistic = {
    FilterStatistic(totalJobs, liveJobToStages.size, totalStages,
      liveJobToStages.map(_._2.size).sum, totalTasks, _stageToTasks.map(_._2.size).sum)
  }
}

/**
 * This class provides the functionality to reject events which are related to the finished
 * jobs based on the given information. This class only deals with job related events, and provides
 * a PartialFunction which returns false for rejected events for finished jobs, returns true
 * otherwise.
 */
private[spark] abstract class JobEventFilter(
    stats: Option[FilterStatistic],
    jobToStages: Map[Int, Seq[Int]],
    stageToTasks: Map[Int, Set[Long]],
    stageToRDDs: Map[Int, Seq[Int]]) extends EventFilter with Logging {

  private val liveTasks: Set[Long] = stageToTasks.values.flatten.toSet
  private val liveRDDs: Set[Int] = stageToRDDs.values.flatten.toSet

  logDebug(s"jobs : ${jobToStages.keySet}")
  logDebug(s"stages in jobs : ${jobToStages.values.flatten}")
  logDebug(s"stages : ${stageToTasks.keySet}")
  logDebug(s"tasks in stages : ${stageToTasks.values.flatten}")
  logDebug(s"RDDs in stages : ${stageToRDDs.values.flatten}")

  override def statistic(): Option[FilterStatistic] = stats

  protected val acceptFnForJobEvents: PartialFunction[SparkListenerEvent, Boolean] = {
    case e: SparkListenerStageCompleted =>
      stageToTasks.contains(e.stageInfo.stageId)

    case e: SparkListenerStageSubmitted =>
      stageToTasks.contains(e.stageInfo.stageId)

    case e: SparkListenerTaskStart =>
      liveTasks.contains(e.taskInfo.taskId)

    case e: SparkListenerTaskGettingResult =>
      liveTasks.contains(e.taskInfo.taskId)

    case e: SparkListenerTaskEnd =>
      liveTasks.contains(e.taskInfo.taskId)

    case e: SparkListenerJobStart =>
      jobToStages.contains(e.jobId)

    case e: SparkListenerJobEnd =>
      jobToStages.contains(e.jobId)

    case e: SparkListenerUnpersistRDD =>
      liveRDDs.contains(e.rddId)

    case e: SparkListenerExecutorMetricsUpdate =>
      e.accumUpdates.exists { case (_, stageId, _, _) =>
        stageToTasks.contains(stageId)
      }

    case e: SparkListenerSpeculativeTaskSubmitted =>
      stageToTasks.contains(e.stageId)
  }
}

/**
 * This class rejects events which are related to the finished jobs or dead executors,
 * based on the given information. The events which are not related to the job and executor
 * will be considered as "Don't mind".
 */
private[spark] class BasicEventFilter(
    _stats: FilterStatistic,
    _liveJobToStages: Map[Int, Seq[Int]],
    _stageToTasks: Map[Int, Set[Long]],
    _stageToRDDs: Map[Int, Seq[Int]],
    liveExecutors: Set[String])
  extends JobEventFilter(Some(_stats), _liveJobToStages, _stageToTasks, _stageToRDDs) with Logging {

  def this(builder: BasicEventFilterBuilder) = {
    this(builder.statistic(), builder.liveJobToStages, builder.stageToTasks, builder.stageToRDDs,
      builder.liveExecutors)
  }

  logDebug(s"live executors : $liveExecutors")

  private val _acceptFn: PartialFunction[SparkListenerEvent, Boolean] = {
    case e: SparkListenerExecutorAdded => liveExecutors.contains(e.executorId)
    case e: SparkListenerExecutorRemoved => liveExecutors.contains(e.executorId)
    case e: SparkListenerExecutorBlacklisted => liveExecutors.contains(e.executorId)
    case e: SparkListenerExecutorUnblacklisted => liveExecutors.contains(e.executorId)
    case e: SparkListenerStageExecutorMetrics => liveExecutors.contains(e.execId)
  }

  override def acceptFn(): PartialFunction[SparkListenerEvent, Boolean] = {
    _acceptFn.orElse(acceptFnForJobEvents)
  }
}
