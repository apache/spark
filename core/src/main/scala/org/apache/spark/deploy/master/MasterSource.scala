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

  // Gauge for alive worker numbers in cluster
  metricRegistry.register(MetricRegistry.name("aliveWorkers"), new Gauge[Int]{
    override def getValue: Int = master.workers.count(_.state == WorkerState.ALIVE)
  })

  // Gauge for dead worker numbers in cluster
  metricRegistry.register(MetricRegistry.name("deadWorkers"), new Gauge[Int]{
    override def getValue: Int = master.workers.count(_.state == WorkerState.DEAD)
  })

  // Gauge for decommissioned worker numbers in cluster
  metricRegistry.register(MetricRegistry.name("decommissionedWorkers"), new Gauge[Int]{
    override def getValue: Int = master.workers.count(_.state == WorkerState.DECOMMISSIONED)
  })

  // Gauge for unknown worker numbers in cluster
  metricRegistry.register(MetricRegistry.name("unknownWorkers"), new Gauge[Int]{
    override def getValue: Int = master.workers.count(_.state == WorkerState.UNKNOWN)
  })

  // Gauge for application numbers in cluster
  metricRegistry.register(MetricRegistry.name("apps"), new Gauge[Int] {
    override def getValue: Int = master.apps.size
  })

  // Gauge for waiting application numbers in cluster
  metricRegistry.register(MetricRegistry.name("waitingApps"), new Gauge[Int] {
    override def getValue: Int = master.apps.count(_.state == ApplicationState.WAITING)
  })

  // Gauge for running application numbers in cluster
  metricRegistry.register(MetricRegistry.name("runningApps"), new Gauge[Int] {
    override def getValue: Int = master.apps.count(_.state == ApplicationState.RUNNING)
  })

  // Gauge for finished application numbers in cluster
  metricRegistry.register(MetricRegistry.name("finishedApps"), new Gauge[Int] {
    override def getValue: Int = master.apps.count(_.state == ApplicationState.FINISHED)
  })

  // Gauge for failed application numbers in cluster
  metricRegistry.register(MetricRegistry.name("failedApps"), new Gauge[Int] {
    override def getValue: Int = master.apps.count(_.state == ApplicationState.FAILED)
  })

  // Gauge for killed application numbers in cluster
  metricRegistry.register(MetricRegistry.name("killedApps"), new Gauge[Int] {
    override def getValue: Int = master.apps.count(_.state == ApplicationState.KILLED)
  })

  // Gauge for unknown application numbers in cluster
  metricRegistry.register(MetricRegistry.name("unknownApps"), new Gauge[Int] {
    override def getValue: Int = master.apps.count(_.state == ApplicationState.UNKNOWN)
  })

  // Gauge for submitted driver numbers in cluster
  metricRegistry.register(MetricRegistry.name("submittedDrivers"), new Gauge[Int] {
    override def getValue: Int = master.drivers.count(_.state == DriverState.SUBMITTED)
  })

  // Gauge for running driver numbers in cluster
  metricRegistry.register(MetricRegistry.name("runningDrivers"), new Gauge[Int] {
    override def getValue: Int = master.drivers.count(_.state == DriverState.RUNNING)
  })

  // Gauge for finished driver numbers in cluster
  metricRegistry.register(MetricRegistry.name("finishedDrivers"), new Gauge[Int] {
    override def getValue: Int = master.drivers.count(_.state == DriverState.FINISHED)
  })

  // Gauge for relaunching driver numbers in cluster
  metricRegistry.register(MetricRegistry.name("relaunchingDrivers"), new Gauge[Int] {
    override def getValue: Int = master.drivers.count(_.state == DriverState.RELAUNCHING)
  })

  // Gauge for unknown driver numbers in cluster
  metricRegistry.register(MetricRegistry.name("unknownDrivers"), new Gauge[Int] {
    override def getValue: Int = master.drivers.count(_.state == DriverState.UNKNOWN)
  })

  // Gauge for killed driver numbers in cluster
  metricRegistry.register(MetricRegistry.name("killedDrivers"), new Gauge[Int] {
    override def getValue: Int = master.drivers.count(_.state == DriverState.KILLED)
  })

  // Gauge for failed driver numbers in cluster
  metricRegistry.register(MetricRegistry.name("failedDrivers"), new Gauge[Int] {
    override def getValue: Int = master.drivers.count(_.state == DriverState.FAILED)
  })

  // Gauge for error driver numbers in cluster
  metricRegistry.register(MetricRegistry.name("errorDrivers"), new Gauge[Int] {
    override def getValue: Int = master.drivers.count(_.state == DriverState.ERROR)
  })
}
