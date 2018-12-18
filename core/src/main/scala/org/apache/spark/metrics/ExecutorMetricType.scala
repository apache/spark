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
import javax.management.ObjectName

import scala.collection.mutable

import org.apache.spark.SparkEnv
import org.apache.spark.executor.ProcfsMetricsGetter
import org.apache.spark.internal.config
import org.apache.spark.memory.MemoryManager


/**
 * Executor metric types for executor-level metrics stored in ExecutorMetrics.
 */
sealed trait ExecutorMetricType {
  private[spark] def getMetricValues(memoryManager: MemoryManager): Array[Long]
  private[spark] def names: Seq[String]
}

sealed trait SingleValueExecutorMetricType extends ExecutorMetricType {
  override private[spark] def names = {
    Seq(getClass().getName().
      stripSuffix("$").split("""\.""").last)
  }

  override private[spark] def getMetricValues(memoryManager: MemoryManager): Array[Long] = {
    val metrics = new Array[Long](1)
    metrics(0) = getMetricValue(memoryManager)
    metrics
  }

  private[spark] def getMetricValue(memoryManager: MemoryManager): Long
}

private[spark] abstract class MemoryManagerExecutorMetricType(
    f: MemoryManager => Long) extends SingleValueExecutorMetricType {
  override private[spark] def getMetricValue(memoryManager: MemoryManager): Long = {
    f(memoryManager)
  }
}

private[spark] abstract class MBeanExecutorMetricType(mBeanName: String)
  extends SingleValueExecutorMetricType {
  private val bean = ManagementFactory.newPlatformMXBeanProxy(
    ManagementFactory.getPlatformMBeanServer,
    new ObjectName(mBeanName).toString, classOf[BufferPoolMXBean])

  override private[spark] def getMetricValue(memoryManager: MemoryManager): Long = {
    bean.getMemoryUsed
  }
}

case object JVMHeapMemory extends SingleValueExecutorMetricType {
  override private[spark] def getMetricValue(memoryManager: MemoryManager): Long = {
    ManagementFactory.getMemoryMXBean.getHeapMemoryUsage().getUsed()
  }
}

case object JVMOffHeapMemory extends SingleValueExecutorMetricType {
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
  val shouldLogStageExecutorMetrics =
    SparkEnv.get.conf.get(config.EVENT_LOG_STAGE_EXECUTOR_METRICS)
  val shouldLogStageExecutorProcessTreeMetrics =
    SparkEnv.get.conf.get(config.EVENT_LOG_PROCESS_TREE_METRICS)
  override private[spark] def getMetricValues(memoryManager: MemoryManager): Array[Long] = {
    val processTreeMetrics = new Array[Long](names.length)
    if (shouldLogStageExecutorMetrics && shouldLogStageExecutorProcessTreeMetrics) {
      val allMetrics = ProcfsMetricsGetter.pTreeInfo.computeAllMetrics()
      processTreeMetrics(0) = allMetrics.jvmVmemTotal
      processTreeMetrics(1) = allMetrics.jvmRSSTotal
      processTreeMetrics(2) = allMetrics.pythonVmemTotal
      processTreeMetrics(3) = allMetrics.pythonRSSTotal
      processTreeMetrics(4) = allMetrics.otherVmemTotal
      processTreeMetrics(5) = allMetrics.otherRSSTotal
    }
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

  val (metricToOffset, numMetrics) = {
    var numberOfMetrics = 0
    val definedMetricsAndOffset = mutable.LinkedHashMap.empty[String, Int]
    metricGetters.foreach { m =>
      var metricInSet = 0
      (0 until m.names.length).foreach { idx =>
        definedMetricsAndOffset += (m.names(idx) -> (idx + numberOfMetrics))
      }
      numberOfMetrics += m.names.length
    }
    (definedMetricsAndOffset, numberOfMetrics)
  }
}
