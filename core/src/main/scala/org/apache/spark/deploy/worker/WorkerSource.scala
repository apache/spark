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

package org.apache.spark.deploy.worker

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.source.Source

private[worker] class WorkerSource(val worker: Worker) extends Source {
  override val sourceName = "worker"
  override val metricRegistry = new MetricRegistry()

  metricRegistry.register(MetricRegistry.name("executors"), new Gauge[Int] {
    override def getValue: Int = worker.executors.size
  })

  // Gauge for cores used of this worker
  metricRegistry.register(MetricRegistry.name("coresUsed"), new Gauge[Int] {
    override def getValue: Int = worker.coresUsed
  })

  // Gauge for memory used of this worker
  metricRegistry.register(MetricRegistry.name("memUsed_MB"), new Gauge[Int] {
    override def getValue: Int = worker.memoryUsed
  })

  // Gauge for cores free of this worker
  metricRegistry.register(MetricRegistry.name("coresFree"), new Gauge[Int] {
    override def getValue: Int = worker.coresFree
  })

  // Gauge for memory free of this worker
  metricRegistry.register(MetricRegistry.name("memFree_MB"), new Gauge[Int] {
    override def getValue: Int = worker.memoryFree
  })
}
