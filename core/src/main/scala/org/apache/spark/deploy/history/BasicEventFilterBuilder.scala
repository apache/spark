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

class BasicEventFilterBuilder extends SparkListener with EventFilterBuilder {
  val liveJobToStages = new mutable.HashMap[Int, Seq[Int]]
  val stageToTasks = new mutable.HashMap[Int, mutable.Set[Long]]
  val stageToRDDs = new mutable.HashMap[Int, mutable.Set[Int]]
  val liveExecutors = new mutable.HashSet[String]

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    liveJobToStages += jobStart.jobId -> jobStart.stageIds
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val stages = liveJobToStages.getOrElse(jobEnd.jobId, Seq.empty[Int])
    liveJobToStages -= jobEnd.jobId
    stages.foreach { stage =>
      stageToTasks -= stage
      stageToRDDs -= stage
    }
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    val curTasks = stageToTasks.getOrElseUpdate(taskStart.stageId,
      mutable.HashSet[Long]())
    curTasks += taskStart.taskInfo.taskId
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    liveExecutors += executorAdded.executorId
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    liveExecutors -= executorRemoved.executorId
  }

  override def createFilter(): EventFilter = new BasicEventFilter(this)
}

class BasicEventFilter(trackListener: BasicEventFilterBuilder) extends EventFilter with Logging {

  private val liveTasks: Set[Long] = trackListener.stageToTasks.values match {
    case xs if xs.isEmpty => Set.empty[Long]
    case xs => xs.reduce(_ ++ _).toSet
  }

  private val liveRDDs: Set[Int] = trackListener.stageToRDDs.values match {
    case xs if xs.isEmpty => Set.empty[Int]
    case xs => xs.reduce(_ ++ _).toSet
  }

  if (log.isDebugEnabled) {
    logDebug(s"live jobs : ${trackListener.liveJobToStages.keySet}")
    logDebug(s"stages in jobs : ${trackListener.liveJobToStages.values.flatten}")
    logDebug(s"stages : ${trackListener.stageToTasks.keySet}")
    logDebug(s"tasks in stages : ${trackListener.stageToTasks.values.flatten}")
  }

  override def filterStageCompleted(event: SparkListenerStageCompleted): Option[Boolean] = {
    Some(trackListener.stageToTasks.contains(event.stageInfo.stageId))
  }

  override def filterStageSubmitted(event: SparkListenerStageSubmitted): Option[Boolean] = {
    Some(trackListener.stageToTasks.contains(event.stageInfo.stageId))
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
    Some(trackListener.liveJobToStages.contains(event.jobId))
  }

  override def filterJobEnd(event: SparkListenerJobEnd): Option[Boolean] = {
    Some(trackListener.liveJobToStages.contains(event.jobId))
  }

  override def filterUnpersistRDD(event: SparkListenerUnpersistRDD): Option[Boolean] = {
    Some(liveRDDs.contains(event.rddId))
  }

  override def filterStageExecutorMetrics(
      event: SparkListenerStageExecutorMetrics): Option[Boolean] = {
    Some(trackListener.liveExecutors.contains(event.execId))
  }

  override def filterExecutorAdded(event: SparkListenerExecutorAdded): Option[Boolean] = {
    Some(trackListener.liveExecutors.contains(event.executorId))
  }

  override def filterExecutorRemoved(event: SparkListenerExecutorRemoved): Option[Boolean] = {
    Some(trackListener.liveExecutors.contains(event.executorId))
  }

  override def filterExecutorBlacklisted(
      event: SparkListenerExecutorBlacklisted): Option[Boolean] = {
    Some(trackListener.liveExecutors.contains(event.executorId))
  }

  override def filterExecutorUnblacklisted(
      event: SparkListenerExecutorUnblacklisted): Option[Boolean] = {
    Some(trackListener.liveExecutors.contains(event.executorId))
  }

  override def filterSpeculativeTaskSubmitted(
      event: SparkListenerSpeculativeTaskSubmitted): Option[Boolean] = {
    Some(trackListener.stageToTasks.contains(event.stageId))
  }
}
