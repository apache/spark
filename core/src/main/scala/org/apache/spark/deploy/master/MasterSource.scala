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

package org.apache.spark.deploy.master

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.source.Source

private[spark] class MasterSource(val master: Master) extends Source {
  override val metricRegistry = new MetricRegistry()
  override val sourceName = "master"

  // Gauge for worker numbers in cluster
  metricRegistry.register(MetricRegistry.name("workers"), new Gauge[Int] {
    override def getValue: Int = master.workers.size
  })

  // Gauge for application numbers in cluster
  metricRegistry.register(MetricRegistry.name("apps"), new Gauge[Int] {
    override def getValue: Int = master.apps.size
  })

  // Gauge for waiting application numbers in cluster
  metricRegistry.register(MetricRegistry.name("waitingApps"), new Gauge[Int] {
    override def getValue: Int = master.waitingApps.size
  })
}
