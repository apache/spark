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
package org.apache.spark.metrics

import java.lang.management.{BufferPoolMXBean, ManagementFactory}
import java.util
import javax.management.ObjectName

import org.apache.spark.executor.ProcfsBasedSystems
import org.apache.spark.memory.MemoryManager

import scala.collection.mutable

/**
 * Executor metric types for executor-level metrics stored in ExecutorMetrics.
 */
sealed trait ExecutorMetricType {
  private[spark] def getMetricValue(memoryManager: MemoryManager): Long = 0
  private[spark] def getMetricSet(memoryManager: MemoryManager): Array[Long] = {
    new Array[Long](0)
  }
  private[spark] def names = Seq(getClass().getName().stripSuffix("$").split("""\.""").last)
}

private[spark] abstract class MemoryManagerExecutorMetricType(
    f: MemoryManager => Long) extends ExecutorMetricType {
  override private[spark] def getMetricSet(memoryManager: MemoryManager): Array[Long] = {
    val metricAsSet = new Array[Long](names.length)
    (0 until names.length ).foreach { idx =>
      metricAsSet(idx) = (f(memoryManager))
    }
    metricAsSet
  }
  override private[spark] def getMetricValue(memoryManager: MemoryManager): Long = {
    f(memoryManager)
  }
}

private[spark] abstract class MBeanExecutorMetricType(mBeanName: String)
  extends ExecutorMetricType {
  private val bean = ManagementFactory.newPlatformMXBeanProxy(
    ManagementFactory.getPlatformMBeanServer,
    new ObjectName(mBeanName).toString, classOf[BufferPoolMXBean])

  override private[spark] def getMetricSet(memoryManager: MemoryManager): Array[Long] = {
    val metricAsSet = new Array[Long](1)
    metricAsSet(0) = bean.getMemoryUsed
    metricAsSet
  }

  override private[spark] def getMetricValue(memoryManager: MemoryManager): Long = {
    bean.getMemoryUsed
  }
}

case object JVMHeapMemory extends ExecutorMetricType {

  override private[spark] def getMetricSet(memoryManager: MemoryManager): Array[Long] = {
    val metricAsSet = new Array[Long](1)
    metricAsSet(0) = ( ManagementFactory.getMemoryMXBean.getHeapMemoryUsage().getUsed())
    metricAsSet
  }
  override private[spark] def getMetricValue(memoryManager: MemoryManager): Long = {
    ManagementFactory.getMemoryMXBean.getHeapMemoryUsage().getUsed()
  }
}

case object JVMOffHeapMemory extends ExecutorMetricType {
  override private[spark] def getMetricSet(memoryManager: MemoryManager): Array[Long] = {
    val metricAsSet = new Array[ Long](1)
    metricAsSet(0) = ( ManagementFactory.getMemoryMXBean.getNonHeapMemoryUsage().getUsed())
    metricAsSet
  }
  override private[spark] def getMetricValue(memoryManager: MemoryManager): Long = {
    ManagementFactory.getMemoryMXBean.getNonHeapMemoryUsage().getUsed()
  }
}

case object ProcessTreeMetrics extends ExecutorMetricType {
  override val names = Seq(
    "ProcessTreeJVMVMemory",
    "ProcessTreeJVMRSSMemory",
    "ProcessTreePythonVMemory",
    "ProcessTreePythonRSSMemory",
    "ProcessTreeOtherVMemory",
    "ProcessTreeOtherRSSMemory")
  override private[spark] def getMetricSet(memoryManager: MemoryManager): Array[Long] = {
    val allMetrics = ExecutorMetricType.pTreeInfo.computeAllMetrics()
    val processTreeMetrics = new Array[Long](names.length)
    processTreeMetrics(0) = allMetrics.jvmVmemTotal
    processTreeMetrics(1) = allMetrics.jvmRSSTotal
    processTreeMetrics(2) = allMetrics.pythonVmemTotal
    processTreeMetrics(3) = allMetrics.pythonRSSTotal
    processTreeMetrics(4) = allMetrics.otherVmemTotal
    processTreeMetrics(5) = allMetrics.otherRSSTotal
    processTreeMetrics
  }
}

case object OnHeapExecutionMemory extends MemoryManagerExecutorMetricType(
  _.onHeapExecutionMemoryUsed)

case object OffHeapExecutionMemory extends MemoryManagerExecutorMetricType(
  _.offHeapExecutionMemoryUsed)

case object OnHeapStorageMemory extends MemoryManagerExecutorMetricType(
  _.onHeapStorageMemoryUsed)

case object OffHeapStorageMemory extends MemoryManagerExecutorMetricType(
  _.offHeapStorageMemoryUsed)

case object OnHeapUnifiedMemory extends MemoryManagerExecutorMetricType(
  (m => m.onHeapExecutionMemoryUsed + m.onHeapStorageMemoryUsed))

case object OffHeapUnifiedMemory extends MemoryManagerExecutorMetricType(
  (m => m.offHeapExecutionMemoryUsed + m.offHeapStorageMemoryUsed))

case object DirectPoolMemory extends MBeanExecutorMetricType(
  "java.nio:type=BufferPool,name=direct")

case object MappedPoolMemory extends MBeanExecutorMetricType(
  "java.nio:type=BufferPool,name=mapped")

private[spark] object ExecutorMetricType {
  final val pTreeInfo = new ProcfsBasedSystems

  // List of all executor metric getters
  val metricGetters = IndexedSeq(
    JVMHeapMemory,
    JVMOffHeapMemory,
    OnHeapExecutionMemory,
    OffHeapExecutionMemory,
    OnHeapStorageMemory,
    OffHeapStorageMemory,
    OnHeapUnifiedMemory,
    OffHeapUnifiedMemory,
    DirectPoolMemory,
    MappedPoolMemory,
    ProcessTreeMetrics
  )

  var definedMetricsAndOffset = mutable.LinkedHashMap.empty[String, Int]
  var numberOfMetrics = 0
  metricGetters.foreach { m =>
    var metricInSet = 0
    while (metricInSet < m.names.length) {
      definedMetricsAndOffset += (m.names(metricInSet) -> (metricInSet + numberOfMetrics) )
      metricInSet += 1
    }
    numberOfMetrics += m.names.length
  }
}
