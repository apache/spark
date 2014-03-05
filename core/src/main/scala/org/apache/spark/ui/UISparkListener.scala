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

import org.apache.spark.scheduler._
import org.apache.spark.storage._

private[ui] trait UISparkListener extends SparkListener

/**
 * A SparkListener that listens only for application start events to set the app name for the UI.
 */
private[ui] class AppNameListener(parent: SparkUI) extends UISparkListener {
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
    val appName = applicationStart.appName
    parent.setAppName(appName)
  }
}

/**
 * A SparkListener that maintains executor storage status
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

  /** Update storage status list to reflect the removal of an RDD from the cache */
  def updateStorageStatus(unpersistedRDDId: Int) {
    storageStatusList.foreach { storageStatus =>
      val unpersistedBlocksIds = storageStatus.rddBlocks.keys.filter(_.rddId == unpersistedRDDId)
      unpersistedBlocksIds.foreach { blockId =>
        storageStatus.blocks(blockId) = BlockStatus(StorageLevel.NONE, 0L, 0L)
      }
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    val info = taskEnd.taskInfo
    if (info != null) {
      val execId = formatExecutorId(info.executorId)
      val metrics = taskEnd.taskMetrics
      if (metrics != null) {
        val updatedBlocks = metrics.updatedBlocks.getOrElse(Seq())
        if (updatedBlocks.length > 0) {
          updateStorageStatus(execId, updatedBlocks)
        }
      }
    }
  }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD) {
    updateStorageStatus(unpersistRDD.rddId)
  }

  override def onExecutorsStateChange(executorsStateChange: SparkListenerExecutorsStateChange) {
    storageStatusList = executorsStateChange.storageStatusList
  }

  /**
   * In the local mode, there is a discrepancy between the executor ID according to the
   * task ("localhost") and that according to SparkEnv ("<driver>"). This results in
   * duplicate rows for the same executor. Thus, in this mode, we aggregate these two
   * rows and use the executor ID of "<driver>" to be consistent.
   */
  protected def formatExecutorId(execId: String): String = {
    if (execId == "localhost") "<driver>" else execId
  }
}

/**
 * A SparkListener that maintains RDD information
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
    val metrics = taskEnd.taskMetrics
    if (metrics != null && metrics.updatedBlocks.isDefined) {
      updateRDDInfo()
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
    val rddInfo = stageSubmitted.stageInfo.rddInfo
    _rddInfoMap(rddInfo.id) = rddInfo
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    // Remove all partitions that are no longer cached
    _rddInfoMap.retain { case (id, info) => info.numCachedPartitions > 0 }
  }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD) {
    super.onUnpersistRDD(unpersistRDD)
    updateRDDInfo()
  }
}
