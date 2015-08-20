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
import org.apache.spark.executor.{TransportMetrics, ExecutorMetrics}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.ui.{SparkUITab, SparkUI}

private[ui] class MemoryTab(parent: SparkUI) extends SparkUITab(parent, "memory") {
  val memoryListener = parent.memoryListener
  val progressListener = parent.jobProgressListener
  attachPage(new MemoryPage(this))
  attachPage(new StageMemoryPage(this))
}

/**
 * :: DeveloperApi ::
 * A SparkListener that prepares information to be displayed on the MemoryTab
 */
@DeveloperApi
class MemoryListener extends SparkListener {
  type ExecutorId = String
  val activeExecutorIdToMem = new HashMap[ExecutorId, MemoryUIInfo]
  val removedExecutorIdToMem = new HashMap[ExecutorId, MemoryUIInfo]
  // latestExecIdToExecMetrics include all executors that is active and removed.
  // this may consume a lot of memory when executors are changing frequently, e.g. in dynamical
  // allocation mode.
  val latestExecIdToExecMetrics = new HashMap[ExecutorId, ExecutorMetrics]
  // stagesIdToMem a map maintains all executors memory information of each stage,
  // the Map type is [(stageId, attemptId), Seq[(executorId, MemoryUIInfo)]
  val activeStagesToMem = new HashMap[(Int, Int), HashMap[ExecutorId, MemoryUIInfo]]
  val completedStagesToMem = new HashMap[(Int, Int), HashMap[ExecutorId, MemoryUIInfo]]

  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = {
    val executorId = event.execId
    val executorMetrics = event.executorMetrics
    val memoryInfo = activeExecutorIdToMem.getOrElseUpdate(executorId, new MemoryUIInfo)
    memoryInfo.updateExecutorMetrics(executorMetrics)
    activeStagesToMem.map {stageToMem =>
      if (stageToMem._2.contains(executorId)) {
        val memInfo = stageToMem._2.get(executorId).get
        memInfo.updateExecutorMetrics(executorMetrics)
      }
    }
    latestExecIdToExecMetrics.update(executorId, executorMetrics)
  }

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
    val executorId = event.executorId
    activeExecutorIdToMem.put(executorId, new MemoryUIInfo(event.executorInfo))
  }

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
    val executorId = event.executorId
    val info = activeExecutorIdToMem.remove(executorId)
    removedExecutorIdToMem.getOrElseUpdate(executorId, info.getOrElse(new MemoryUIInfo))
  }

  override def onBlockManagerRemoved(event: SparkListenerBlockManagerRemoved): Unit = {
    val executorId = event.blockManagerId.executorId
    val info = activeExecutorIdToMem.remove(executorId)
    removedExecutorIdToMem.getOrElseUpdate(executorId, info.getOrElse(new MemoryUIInfo))
  }

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    val stage = (event.stageInfo.stageId, event.stageInfo.attemptId)
    val memInfoMap = new HashMap[ExecutorId, MemoryUIInfo]
    activeExecutorIdToMem.map(idToMem => memInfoMap.update(idToMem._1, new MemoryUIInfo))
    activeStagesToMem.update(stage, memInfoMap)
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    val stage = (event.stageInfo.stageId, event.stageInfo.attemptId)
    val memInfoMap = activeStagesToMem.get(stage)
    if (memInfoMap.isDefined) {
      activeExecutorIdToMem.map { idToMem =>
        val executorId = idToMem._1
        val memInfo = memInfoMap.get.getOrElse(executorId, new MemoryUIInfo)
        if (latestExecIdToExecMetrics.contains(executorId)) {
          memInfo.updateExecutorMetrics(latestExecIdToExecMetrics.get(executorId).get)
        }
        memInfoMap.get.update(executorId, memInfo)
      }
      completedStagesToMem.put(stage, activeStagesToMem.remove(stage).get)
    }
  }
}

class MemoryUIInfo {
  var executorAddress: String = _
  var transportInfo: Option[transportMemSize] = None

  def this(execInfo: ExecutorInfo) = {
    this()
    executorAddress = execInfo.executorHost
  }

  def updateExecutorMetrics(execMetrics: ExecutorMetrics): Unit = {
    if (execMetrics.transportMetrics.isDefined) {
      transportInfo = transportInfo match {
        case Some(transportMemSize) => transportInfo
        case _ => Some(new transportMemSize)
      }
      executorAddress = execMetrics.hostPort
      if (execMetrics.transportMetrics.isDefined) {
        transportInfo.get.updateTransport(execMetrics.transportMetrics.get)
      }
    }
  }
}

class transportMemSize {
  var onheapSize: Long = _
  var directheapSize: Long = _
  var peakOnheapSizeTime: MemTime = new MemTime()
  var peakDirectheapSizeTime: MemTime = new MemTime()

  def updateTransport(transportMetrics: TransportMetrics): Unit = {
    val updatedOnheapSize = transportMetrics.clientOnheapSize +
      transportMetrics.serverOnheapSize
    val updatedDirectheapSize = transportMetrics.clientDirectheapSize +
      transportMetrics.serverDirectheapSize
    val updateTime: Long = transportMetrics.timeStamp
    onheapSize = updatedOnheapSize
    directheapSize = updatedDirectheapSize
    if (updatedOnheapSize >= peakOnheapSizeTime.memorySize) {
      peakOnheapSizeTime = MemTime(updatedOnheapSize, updateTime)
    }
    if (updatedDirectheapSize >= peakDirectheapSizeTime.memorySize) {
      peakDirectheapSizeTime = MemTime(updatedDirectheapSize, updateTime)
    }
  }
}

case class MemTime(memorySize: Long = 0L, timeStamp: Long = 0L)
