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

import org.apache.spark.memory.MemoryManager

private[spark] sealed trait MetricGetter {
  def getMetricValue(memoryManager: MemoryManager): Long
  val name = getClass().getName().stripSuffix("$").split("""\.""").last
}

private[spark] abstract class MemoryManagerMetricGetter(
    f: MemoryManager => Long) extends MetricGetter {
  override def getMetricValue(memoryManager: MemoryManager): Long = {
    f(memoryManager)
  }
}

private[spark]abstract class MBeanMetricGetter(mBeanName: String)
  extends MetricGetter {
  val bean = ManagementFactory.newPlatformMXBeanProxy(ManagementFactory.getPlatformMBeanServer,
    new ObjectName(mBeanName).toString, classOf[BufferPoolMXBean])

  override def getMetricValue(memoryManager: MemoryManager): Long = {
    bean.getMemoryUsed
  }
}

private[spark] case object JVMHeapMemory extends MetricGetter {
  override def getMetricValue(memoryManager: MemoryManager): Long = {
    ManagementFactory.getMemoryMXBean.getHeapMemoryUsage().getUsed()
  }
}

private[spark] case object JVMOffHeapMemory extends MetricGetter {
  override def getMetricValue(memoryManager: MemoryManager): Long = {
    ManagementFactory.getMemoryMXBean.getNonHeapMemoryUsage().getUsed()
  }
}

private[spark] case object OnHeapExecutionMemory extends MemoryManagerMetricGetter(
  _.onHeapExecutionMemoryUsed)

private[spark] case object OffHeapExecutionMemory extends MemoryManagerMetricGetter(
  _.offHeapExecutionMemoryUsed)

private[spark] case object OnHeapStorageMemory extends MemoryManagerMetricGetter(
  _.onHeapStorageMemoryUsed)

private[spark] case object OffHeapStorageMemory extends MemoryManagerMetricGetter(
  _.offHeapStorageMemoryUsed)

private[spark] case object OnHeapUnifiedMemory extends MemoryManagerMetricGetter(
  (m => m.onHeapExecutionMemoryUsed + m.onHeapStorageMemoryUsed))

private[spark] case object OffHeapUnifiedMemory extends MemoryManagerMetricGetter(
  (m => m.offHeapExecutionMemoryUsed + m.offHeapStorageMemoryUsed))

private[spark] case object DirectPoolMemory extends MBeanMetricGetter(
  "java.nio:type=BufferPool,name=direct")
private[spark] case object MappedPoolMemory extends MBeanMetricGetter(
  "java.nio:type=BufferPool,name=mapped")

private[spark] object MetricGetter {
  val values = IndexedSeq(
    JVMHeapMemory,
    JVMOffHeapMemory,
    OnHeapExecutionMemory,
    OffHeapExecutionMemory,
    OnHeapStorageMemory,
    OffHeapStorageMemory,
    OnHeapUnifiedMemory,
    OffHeapUnifiedMemory,
    DirectPoolMemory,
    MappedPoolMemory
  )

  val idxAndValues = values.zipWithIndex.map(_.swap)
}
