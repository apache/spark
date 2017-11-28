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

import java.util.{Locale, Properties}
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.graphite.{Graphite, GraphiteReporter, GraphiteUDP}

import org.apache.spark.SecurityManager

/**
 * A metrics [[Sink]] which will output registered metrics to Graphite.
 *
 * @param property [[GraphiteSink]] specific properties
 * @param registry A [[MetricRegistry]] can this sink to register
 * @param securityMgr A [[SecurityManager]] to check security related stuffs.
 */
private[spark] class GraphiteSink(
    property: Properties,
    registry: MetricRegistry,
    securityMgr: SecurityManager) extends Sink(property, registry) {
  val GRAPHITE_DEFAULT_PERIOD = 10
  val GRAPHITE_DEFAULT_UNIT = "SECONDS"
  val GRAPHITE_DEFAULT_PREFIX = ""

  val GRAPHITE_KEY_HOST = "host"
  val GRAPHITE_KEY_PORT = "port"
  val GRAPHITE_KEY_PERIOD = "period"
  val GRAPHITE_KEY_UNIT = "unit"
  val GRAPHITE_KEY_PREFIX = "prefix"
  val GRAPHITE_KEY_PROTOCOL = "protocol"

  def propertyToOption(prop: String): Option[String] = Option(property.getProperty(prop))

  if (!propertyToOption(GRAPHITE_KEY_HOST).isDefined) {
    throw new Exception("Graphite sink requires 'host' property.")
  }

  if (!propertyToOption(GRAPHITE_KEY_PORT).isDefined) {
    throw new Exception("Graphite sink requires 'port' property.")
  }

  private val host = propertyToOption(GRAPHITE_KEY_HOST).get
  private val port = propertyToOption(GRAPHITE_KEY_PORT).get.toInt

  private val prefix = propertyToOption(GRAPHITE_KEY_PREFIX).getOrElse(GRAPHITE_DEFAULT_PREFIX)

  val graphite = propertyToOption(GRAPHITE_KEY_PROTOCOL).map(_.toLowerCase(Locale.ROOT)) match {
    case Some("udp") => new GraphiteUDP(host, port)
    case Some("tcp") | None => new Graphite(host, port)
    case Some(p) => throw new Exception(s"Invalid Graphite protocol: $p")
  }

  private val reporter: GraphiteReporter = GraphiteReporter.forRegistry(registry)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .convertRatesTo(TimeUnit.SECONDS)
    .prefixedWith(prefix)
    .build(graphite)

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
