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

package org.apache.spark.metrics.sink

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.ganglia.GangliaReporter
import info.ganglia.gmetric4j.gmetric.GMetric
import info.ganglia.gmetric4j.gmetric.GMetric.UDPAddressingMode

import org.apache.spark.SecurityManager

class GangliaSink(
  override val properties: Properties,
  val registry: MetricRegistry,
  securityMgr: SecurityManager
) extends Sink with HasPollingPeriod {

  val MODE_KEY = "mode"
  val DEFAULT_MODE = GMetric.UDPAddressingMode.MULTICAST

  // TTL for multicast messages. If listeners are X hops away in network, must be at least X.
  val TTL_KEY = "ttl"
  val DEFAULT_TTL = 1

  val HOST_KEY = "host"
  val PORT_KEY = "port"

  def propertyToOption(prop: String): Option[String] = Option(properties.getProperty(prop))

  require(propertyToOption(HOST_KEY).isDefined, "Ganglia sink requires 'host' property.")
  require(propertyToOption(PORT_KEY).isDefined, "Ganglia sink requires 'port' property.")

  val host = propertyToOption(HOST_KEY).get
  val port = propertyToOption(PORT_KEY).get.toInt
  val ttl = propertyToOption(TTL_KEY).map(_.toInt).getOrElse(DEFAULT_TTL)
  val mode: UDPAddressingMode = propertyToOption(MODE_KEY)
    .map(u => GMetric.UDPAddressingMode.valueOf(u.toUpperCase)).getOrElse(DEFAULT_MODE)

  val ganglia = new GMetric(host, port, mode, ttl)
  val reporter: GangliaReporter = GangliaReporter.forRegistry(registry)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .build(ganglia)

  override def start() {
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop() {
    reporter.stop()
  }

  override def report() {
    reporter.report()
  }
}

