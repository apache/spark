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

package org.apache.spark.ui

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.json4s.jackson.JsonMethods._

import org.apache.spark.scheduler._
import org.apache.spark.storage._
import org.apache.spark.util.FileLogger
import org.apache.spark.util.JsonProtocol

private[ui] trait UISparkListener extends SparkListener

/**
 * A SparkListener that serves as an entry point for all events posted to the UI.
 *
 * GatewayUISparkListener achieves two functions:
 *
 *  (1) If the UI is live, GatewayUISparkListener posts each event to all attached listeners
 *      then logs it as JSON. This centralizes event logging and avoids having all attached
 *      listeners log the events on their own. By default, GatewayUISparkListener logs one
 *      file per job, though this needs not be the case.
 *
 *  (2) If the UI is rendered from disk, GatewayUISparkListener replays each event deserialized
 *      from the event logs to all attached listeners.
 */
private[ui] class GatewayUISparkListener(parent: SparkUI, live: Boolean) extends SparkListener {

  // Log events only if the UI is live
  private val logger: Option[FileLogger] = if (live) Some(new FileLogger()) else None

  // Children listeners for which this gateway is responsible
  private val listeners = ArrayBuffer[UISparkListener]()

  def registerSparkListener(listener: UISparkListener) = listeners += listener

  /** Log the event as JSON */
  private def logEvent(event: SparkListenerEvent, flushLogger: Boolean = false) {
    val eventJson = compact(render(JsonProtocol.sparkEventToJson(event)))
    logger.foreach(_.logLine(eventJson))
    if (flushLogger) {
      logger.foreach(_.flush())
    }
  }

  private def closeLogger() = logger.foreach(_.close())
  private def restartLogger() = logger.foreach(_.start())

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
    listeners.foreach(_.onStageSubmitted(stageSubmitted))
    logEvent(stageSubmitted, flushLogger = true)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    listeners.foreach(_.onStageCompleted(stageCompleted))
    logEvent(stageCompleted, flushLogger = true)
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart) {
    listeners.foreach(_.onTaskStart(taskStart))
    logEvent(taskStart)
  }
  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) {
    listeners.foreach(_.onTaskGettingResult(taskGettingResult))
    logEvent(taskGettingResult)
  }
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    listeners.foreach(_.onTaskEnd(taskEnd))
    logEvent(taskEnd)
  }

  override def onJobStart(jobStart: SparkListenerJobStart) {
    listeners.foreach(_.onJobStart(jobStart))
    restartLogger()
    logEvent(jobStart)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    listeners.foreach(_.onJobEnd(jobEnd))
    logEvent(jobEnd)
    closeLogger()
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
    // For live UI's, this should be equivalent to sc.appName
    parent.setAppName(applicationStart.appName)
    listeners.foreach(_.onApplicationStart(applicationStart))
    logEvent(applicationStart, flushLogger = true)
  }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate) {
    listeners.foreach(_.onEnvironmentUpdate(environmentUpdate))
    logEvent(environmentUpdate)
  }

  override def onExecutorsStateChange(executorsStateChange: SparkListenerExecutorsStateChange) {
    listeners.foreach(_.onExecutorsStateChange(executorsStateChange))
    logEvent(executorsStateChange, flushLogger = true)
  }
}

/**
 * A UISparkListener that maintains executor storage status
 */
private[ui] class StorageStatusSparkListener extends UISparkListener {
  var storageStatusList = Seq[StorageStatus]()

  /** Update storage status list to reflect updated block statuses */
  def updateStorageStatus(execId: String, updatedBlocks: Seq[(BlockId, BlockStatus)]) {
    val filteredStatus = storageStatusList.find(_.blockManagerId.executorId == execId)
    filteredStatus.foreach { storageStatus =>
      updatedBlocks.foreach { case (blockId, updatedStatus) =>
        storageStatus.blocks(blockId) = updatedStatus
      }
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    val execId = taskEnd.taskInfo.executorId
    val updatedBlocks = taskEnd.taskMetrics.updatedBlocks.getOrElse(Seq())
    if (updatedBlocks.length > 0) {
      updateStorageStatus(execId, updatedBlocks)
    }
  }

  override def onExecutorsStateChange(executorsStateChange: SparkListenerExecutorsStateChange) {
    storageStatusList = executorsStateChange.storageStatusList
  }
}

/**
 * A UISparkListener that maintains RDD information
 */
private[ui] class RDDInfoSparkListener extends StorageStatusSparkListener {
  private val _rddInfoMap = mutable.Map[Int, RDDInfo]()

  /** Filter RDD info to include only those with cached partitions */
  def rddInfoList = _rddInfoMap.values.filter(_.numCachedPartitions > 0).toSeq

  /** Update each RDD's info to reflect any updates to the RDD's storage status */
  private def updateRDDInfo() {
    val updatedRDDInfoList = StorageUtils.rddInfoFromStorageStatus(storageStatusList, _rddInfoMap)
    updatedRDDInfoList.foreach { info => _rddInfoMap(info.id) = info }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    super.onTaskEnd(taskEnd)
    if (taskEnd.taskMetrics.updatedBlocks.isDefined) {
      updateRDDInfo()
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
    val rddInfo = stageSubmitted.stageInfo.rddInfo
    _rddInfoMap(rddInfo.id) = rddInfo
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    // Remove all partitions that are no longer cached
    // TODO(aor): Handle unpersist
    _rddInfoMap.retain { case (id, info) => info.numCachedPartitions > 0 }
  }
}
