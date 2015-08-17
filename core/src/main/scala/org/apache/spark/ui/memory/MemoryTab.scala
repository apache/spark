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
import org.apache.spark.scheduler.{SparkListenerExecutorMetricsUpdate, SparkListener}
import org.apache.spark.ui.{SparkUITab, SparkUI}

private[ui] class MemoryTab(parent: SparkUI) extends SparkUITab(parent, "memory") {
  val listener = parent.memoryListener
  attachPage(new MemoryPage(this))
}

/**
 * :: DeveloperApi ::
 * A SparkListener that prepares information to be displayed on the MemoryTab
 */
@DeveloperApi
class MemoryListener extends SparkListener {
  type ExecutorId = String
  val executorIdToMem = new HashMap[ExecutorId, MemoryUIInfo]

  override def onExecutorMetricsUpdate(
      executorMetricsUpdate: SparkListenerExecutorMetricsUpdate) {
    val executorId = executorMetricsUpdate.execId
    val executorMetrics = executorMetricsUpdate.executorMetrics
    val memoryInfo = executorIdToMem.getOrElseUpdate(executorId, new MemoryUIInfo)
    memoryInfo.updateExecutorMetrics(executorMetrics)
  }
}

class MemoryUIInfo {
  var executorAddress: String = _
  var transportInfo: Option[transportMemSize] = None

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