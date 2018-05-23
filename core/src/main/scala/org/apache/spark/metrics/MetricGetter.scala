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

sealed trait MetricGetter {
  def getMetricValue(memoryManager: MemoryManager): Long
  val name = getClass().getName().stripSuffix("$")
}

abstract class MemoryManagerMetricGetter(f: MemoryManager => Long) extends MetricGetter {
  override def getMetricValue(memoryManager: MemoryManager): Long = {
    f(memoryManager)
  }
}

abstract class MBeanMetricGetter(mBeanName: String) extends MetricGetter {
  val bean = ManagementFactory.newPlatformMXBeanProxy(ManagementFactory.getPlatformMBeanServer,
    new ObjectName(mBeanName).toString, classOf[BufferPoolMXBean])

  override def getMetricValue(memoryManager: MemoryManager): Long = {
    bean.getMemoryUsed
  }
}

case object JVMHeapMemory extends MetricGetter {
  override def getMetricValue(memoryManager: MemoryManager): Long = {
    ManagementFactory.getMemoryMXBean.getHeapMemoryUsage().getUsed()
  }
}

case object JVMOffHeapMemory extends MetricGetter {
  override def getMetricValue(memoryManager: MemoryManager): Long = {
    ManagementFactory.getMemoryMXBean.getNonHeapMemoryUsage().getUsed()
  }
}

case object OnHeapExecution extends MemoryManagerMetricGetter(_.onHeapExecutionMemoryUsed)

case object OffHeapExecution extends MemoryManagerMetricGetter(_.offHeapExecutionMemoryUsed)

case object OnHeapStorage extends MemoryManagerMetricGetter(_.onHeapStorageMemoryUsed)

case object OffHeapStorage extends MemoryManagerMetricGetter(_.offHeapStorageMemoryUsed)

case object DirectPoolMemory extends MBeanMetricGetter("java.nio:type=BufferPool,name=direct")
case object MappedPoolMemory extends MBeanMetricGetter("java.nio:type=BufferPool,name=mapped")

object MetricGetter {
  val values = IndexedSeq(
    JVMHeapMemory,
    JVMOffHeapMemory,
    OnHeapExecution,
    OffHeapExecution,
    OnHeapStorage,
    OffHeapStorage,
    DirectPoolMemory,
    MappedPoolMemory
  )

  val idxAndValues = values.zipWithIndex.map(_.swap)
}
