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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.scheduler._
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageStatus
import org.apache.spark.util.FileLogger

import net.liftweb.json.JsonAST._

private[spark] trait UISparkListener extends SparkListener

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
private[spark] class GatewayUISparkListener(parent: SparkUI, live: Boolean) extends SparkListener {

  // Log events only if the UI is live
  private val logger: Option[FileLogger] = if (live) Some(new FileLogger()) else None

  // Children listeners for which this gateway is responsible
  private val listeners = ArrayBuffer[UISparkListener]()

  def registerSparkListener(listener: UISparkListener) = {
    listeners += listener
  }

  /** Log the event as JSON */
  private def logEvent(event: SparkListenerEvent) {
    logger.foreach(_.logLine(compactRender(event.toJson)))
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
    listeners.foreach(_.onStageSubmitted(stageSubmitted))
    logEvent(stageSubmitted)
    logger.foreach(_.flush())
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    listeners.foreach(_.onStageCompleted(stageCompleted))
    logEvent(stageCompleted)
    logger.foreach(_.flush())
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
    logger.foreach(_.start())
    logEvent(jobStart)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    listeners.foreach(_.onJobEnd(jobEnd))
    logEvent(jobEnd)
    logger.foreach(_.close())
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
    // Retrieve app name from the application start event
    // For live UI's, this should be equivalent to sc.appName
    val sparkProperties = applicationStart.environmentDetails("Spark Properties").toMap
    val appName = sparkProperties.get("spark.app.name")
    appName.foreach(parent.setAppName)

    listeners.foreach(_.onApplicationStart(applicationStart))
    logEvent(applicationStart)
    logger.foreach(_.flush())
  }

  override def onStorageStatusFetch(storageStatusFetch: SparkListenerStorageStatusFetch) {
    listeners.foreach(_.onStorageStatusFetch(storageStatusFetch))
    logEvent(storageStatusFetch)
    logger.foreach(_.flush())
  }

  override def onGetRDDInfo(getRDDInfo: SparkListenerGetRDDInfo) {
    listeners.foreach(_.onGetRDDInfo(getRDDInfo))
    logEvent(getRDDInfo)
    logger.foreach(_.flush())
  }
}

/**
 * A SparkListener that fetches storage information from SparkEnv.
 *
 * The frequency at which this occurs is by default every time a stage event is triggered.
 * This needs not be the case, however; a stage can be arbitrarily long, so any failure
 * in the middle of a stage causes the storage status for that stage to be lost.
 */
private[spark] class StorageStatusFetchSparkListener(
    sc: SparkContext,
    gateway: GatewayUISparkListener,
    live: Boolean)
  extends UISparkListener {
  var storageStatusList: Seq[StorageStatus] = Seq()

  /**
   * Fetch storage information from SparkEnv, which involves a query to the driver. This is
   * expensive and should be invoked sparingly.
   */
  def fetchStorageStatus() {
    if (live) {
      // Fetch only this is a live UI
      val storageStatus = sc.getExecutorStorageStatus
      val event = new SparkListenerStorageStatusFetch(storageStatus)
      gateway.onStorageStatusFetch(event)
    }
  }

  override def onStorageStatusFetch(storageStatusFetch: SparkListenerStorageStatusFetch) {
    storageStatusList = storageStatusFetch.storageStatusList
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) = fetchStorageStatus()
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) = fetchStorageStatus()
}
