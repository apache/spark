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

package org.apache.spark.storage

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.source.Source

private[spark] class BlockManagerSource(val blockManager: BlockManager)
    extends Source {
  override val metricRegistry = new MetricRegistry()
  override val sourceName = "BlockManager"

  private def registerGauge(name: String, func: BlockManagerMaster => Long): Unit = {
    metricRegistry.register(name, new Gauge[Long] {
      override def getValue: Long = func(blockManager.master) / 1024 / 1024
    })
  }

  registerGauge(MetricRegistry.name("memory", "maxMem_MB"),
    _.getStorageStatus.map(_.maxMem).sum)

  registerGauge(MetricRegistry.name("memory", "maxOnHeapMem_MB"),
    _.getStorageStatus.map(_.maxOnHeapMem.getOrElse(0L)).sum)

  registerGauge(MetricRegistry.name("memory", "maxOffHeapMem_MB"),
    _.getStorageStatus.map(_.maxOffHeapMem.getOrElse(0L)).sum)

  registerGauge(MetricRegistry.name("memory", "remainingMem_MB"),
    _.getStorageStatus.map(_.memRemaining).sum)

  registerGauge(MetricRegistry.name("memory", "remainingOnHeapMem_MB"),
    _.getStorageStatus.map(_.onHeapMemRemaining.getOrElse(0L)).sum)

  registerGauge(MetricRegistry.name("memory", "remainingOffHeapMem_MB"),
    _.getStorageStatus.map(_.offHeapMemRemaining.getOrElse(0L)).sum)

  registerGauge(MetricRegistry.name("memory", "memUsed_MB"),
    _.getStorageStatus.map(_.memUsed).sum)

  registerGauge(MetricRegistry.name("memory", "onHeapMemUsed_MB"),
    _.getStorageStatus.map(_.onHeapMemUsed.getOrElse(0L)).sum)

  registerGauge(MetricRegistry.name("memory", "offHeapMemUsed_MB"),
    _.getStorageStatus.map(_.offHeapMemUsed.getOrElse(0L)).sum)

  registerGauge(MetricRegistry.name("disk", "diskSpaceUsed_MB"),
    _.getStorageStatus.map(_.diskUsed).sum)
}
