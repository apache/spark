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

package org.apache.spark.scheduler.cluster.mesos

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.source.Source

private[mesos] class MesosClusterSchedulerSource(scheduler: MesosClusterScheduler)
  extends Source {

  override val sourceName: String = "mesos_cluster"
  override val metricRegistry: MetricRegistry = new MetricRegistry()

  metricRegistry.register(MetricRegistry.name("waitingDrivers"), new Gauge[Int] {
    override def getValue: Int = scheduler.getQueuedDriversSize
  })

  metricRegistry.register(MetricRegistry.name("launchedDrivers"), new Gauge[Int] {
    override def getValue: Int = scheduler.getLaunchedDriversSize
  })

  metricRegistry.register(MetricRegistry.name("retryDrivers"), new Gauge[Int] {
    override def getValue: Int = scheduler.getPendingRetryDriversSize
  })
}
