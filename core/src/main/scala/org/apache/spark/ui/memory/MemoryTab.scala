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

package org.apache.spark.ui.memory

import scala.collection.mutable.HashMap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.{ExecutorMetrics, TransportMetrics}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.ui.{SparkUI, SparkUITab}

private[ui] class MemoryTab(parent: SparkUI) extends SparkUITab(parent, "memory") {
  val memoryListener = parent.memoryListener
  val progressListener = parent.jobProgressListener
  attachPage(new MemoryPage(this))
  attachPage(new StageMemoryPage(this))
}

/**
 * :: DeveloperApi ::
 * A SparkListener that prepares information to be displayed on the MemoryTab.
 */
@DeveloperApi
class MemoryListener extends SparkListener {
  type ExecutorId = String
  val activeExecutorIdToMem = new HashMap[ExecutorId, MemoryUIInfo]
  // TODO This may use too much memory.
  // There may be many removed executors (e.g. in Dynamic Allocation Mode).
  val removedExecutorIdToMem = new HashMap[ExecutorId, MemoryUIInfo]
  // A map that maintains the latest metrics of each active executor.
  val latestExecIdToExecMetrics = new HashMap[ExecutorId, ExecutorMetrics]
  // A map that maintains all executors memory information of each stage.
  // [(stageId, attemptId), Seq[(executorId, MemoryUIInfo)]
  val activeStagesToMem = new HashMap[(Int, Int), HashMap[ExecutorId, MemoryUIInfo]]
  // TODO Get conf of the retained stages so that we don't need to handle them all.
  // There may be many completed stages.
  val completedStagesToMem = new HashMap[(Int, Int), HashMap[ExecutorId, MemoryUIInfo]]

  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = {
    val executorId = event.execId
    val executorMetrics = event.executorMetrics
    activeExecutorIdToMem
      .getOrElseUpdate(executorId, new MemoryUIInfo)
      .updateMemUiInfo(executorMetrics.get)
    activeStagesToMem.foreach { case (_, stageMemMetrics) =>
      // If executor is added in the stage running time, we also update the metrics for the
      // executor in {{activeStagesToMem}}
      if (!stageMemMetrics.contains(executorId)) {
        stageMemMetrics(executorId) = new MemoryUIInfo
      }
      stageMemMetrics(executorId).updateMemUiInfo(executorMetrics.get)
    }
    latestExecIdToExecMetrics(executorId) = executorMetrics.get
  }

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
    val executorId = event.executorId
    activeExecutorIdToMem.put(executorId, new MemoryUIInfo(event.executorInfo))
  }

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
    val executorId = event.executorId
    val info = activeExecutorIdToMem.remove(executorId)
    latestExecIdToExecMetrics.remove(executorId)
    removedExecutorIdToMem.getOrElseUpdate(executorId, info.getOrElse(new MemoryUIInfo))
  }

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    val stage = (event.stageInfo.stageId, event.stageInfo.attemptId)
    val memInfoMap = new HashMap[ExecutorId, MemoryUIInfo]
    activeExecutorIdToMem.map { case (id, _) =>
      memInfoMap(id) = new MemoryUIInfo
      val latestExecMetrics = latestExecIdToExecMetrics.get(id)
      latestExecMetrics match {
        case None => // Do nothing
        case Some(metrics) =>
          memInfoMap(id).updateMemUiInfo(metrics)
      }
    }
    activeStagesToMem(stage) = memInfoMap
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    val stage = (event.stageInfo.stageId, event.stageInfo.attemptId)
    // We need to refresh {{activeStagesToMem}} with {{activeExecutorIdToMem}} in case the
    // executor is added in the stage running time and no {{SparkListenerExecutorMetricsUpdate}}
    // event is updated in this stage.
    activeStagesToMem.get(stage).map { memInfoMap =>
      activeExecutorIdToMem.foreach { case (executorId, memUiInfo) =>
        if (!memInfoMap.contains(executorId)) {
          memInfoMap(executorId) = new MemoryUIInfo
          memInfoMap(executorId).copyMemUiInfo(memUiInfo)
        }
      }
    }
    completedStagesToMem.put(stage, activeStagesToMem.remove(stage).get)
  }
}

@DeveloperApi
class MemoryUIInfo {
  var executorAddress: String = _
  var transportInfo: Option[TransportMemSize] = None

  def this(execInfo: ExecutorInfo) = {
    this()
    executorAddress = execInfo.executorHost
  }

  def updateMemUiInfo(execMetrics: ExecutorMetrics): Unit = {
    transportInfo = transportInfo match {
      case Some(transportMemSize) => transportInfo
      case _ => Some(new TransportMemSize)
    }
    executorAddress = execMetrics.hostPort
    transportInfo.get.updateTransMemSize(execMetrics.transportMetrics)
  }

  def copyMemUiInfo(memUiInfo: MemoryUIInfo): Unit = {
    executorAddress = memUiInfo.executorAddress
    transportInfo.foreach(_.copyTransMemSize(memUiInfo.transportInfo.get))
  }
}

@DeveloperApi
class TransportMemSize {
  var onHeapSize: Long = _
  var offHeapSize: Long = _
  var peakOnHeapSizeTime: MemTime = new MemTime()
  var peakOffHeapSizeTime: MemTime = new MemTime()

  def updateTransMemSize(transportMetrics: TransportMetrics): Unit = {
    val updatedOnHeapSize = transportMetrics.onHeapSize
    val updatedOffHeapSize = transportMetrics.offHeapSize
    val updateTime: Long = transportMetrics.timeStamp
    onHeapSize = updatedOnHeapSize
    offHeapSize = updatedOffHeapSize
    if (updatedOnHeapSize >= peakOnHeapSizeTime.memorySize) {
      peakOnHeapSizeTime = MemTime(updatedOnHeapSize, updateTime)
    }
    if (updatedOffHeapSize >= peakOffHeapSizeTime.memorySize) {
      peakOffHeapSizeTime = MemTime(updatedOffHeapSize, updateTime)
    }
  }

  def copyTransMemSize(transMemSize: TransportMemSize): Unit = {
    onHeapSize = transMemSize.onHeapSize
    offHeapSize = transMemSize.offHeapSize
    peakOnHeapSizeTime = MemTime(transMemSize.peakOnHeapSizeTime.memorySize,
      transMemSize.peakOnHeapSizeTime.timeStamp)
    peakOffHeapSizeTime = MemTime(transMemSize.peakOffHeapSizeTime.memorySize,
      transMemSize.peakOffHeapSizeTime.timeStamp)
  }
}

@DeveloperApi
case class MemTime(memorySize: Long = 0L, timeStamp: Long = System.currentTimeMillis)
