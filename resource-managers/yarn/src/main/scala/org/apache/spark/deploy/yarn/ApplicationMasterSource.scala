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

package org.apache.spark.deploy.yarn

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.source.Source

private[spark] class ApplicationMasterSource(prefix: String, yarnAllocator: YarnAllocator)
  extends Source {

  override val sourceName: String = prefix + ".applicationMaster"
  override val metricRegistry: MetricRegistry = new MetricRegistry()

  metricRegistry.register(MetricRegistry.name("numExecutorsFailed"), new Gauge[Int] {
    override def getValue: Int = yarnAllocator.getNumExecutorsFailed
  })

  metricRegistry.register(MetricRegistry.name("numExecutorsRunning"), new Gauge[Int] {
    override def getValue: Int = yarnAllocator.getNumExecutorsRunning
  })

  metricRegistry.register(MetricRegistry.name("numReleasedContainers"), new Gauge[Int] {
    override def getValue: Int = yarnAllocator.getNumReleasedContainers
  })

  metricRegistry.register(MetricRegistry.name("numLocalityAwareTasks"), new Gauge[Int] {
    override def getValue: Int = yarnAllocator.getNumLocalityAwareTasks
  })

  metricRegistry.register(MetricRegistry.name("numContainersPendingAllocate"), new Gauge[Int] {
    override def getValue: Int = yarnAllocator.getNumContainersPendingAllocate
  })

}
