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

import org.apache.spark.SecurityManager
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.MetricsSystem

private[spark] object StatsdSink {
  val STATSD_KEY_HOST = "host"
  val STATSD_KEY_PORT = "port"
  val STATSD_KEY_PERIOD = "period"
  val STATSD_KEY_UNIT = "unit"
  val STATSD_KEY_PREFIX = "prefix"

  val STATSD_DEFAULT_HOST = "127.0.0.1"
  val STATSD_DEFAULT_PORT = "8125"
  val STATSD_DEFAULT_PERIOD = "10"
  val STATSD_DEFAULT_UNIT = "SECONDS"
  val STATSD_DEFAULT_PREFIX = ""
}

private[spark] class StatsdSink(
    val property: Properties,
    val registry: MetricRegistry,
    securityMgr: SecurityManager)
  extends Sink with Logging {
  import StatsdSink._

  val host = property.getProperty(STATSD_KEY_HOST, STATSD_DEFAULT_HOST)
  val port = property.getProperty(STATSD_KEY_PORT, STATSD_DEFAULT_PORT).toInt

  val pollPeriod = property.getProperty(STATSD_KEY_PERIOD, STATSD_DEFAULT_PERIOD).toInt
  val pollUnit =
    TimeUnit.valueOf(property.getProperty(STATSD_KEY_UNIT, STATSD_DEFAULT_UNIT).toUpperCase)

  val prefix = property.getProperty(STATSD_KEY_PREFIX, STATSD_DEFAULT_PREFIX)

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  val reporter = new StatsdReporter(registry, host, port, prefix)

  override def start(): Unit = {
    reporter.start(pollPeriod, pollUnit)
    logInfo(s"StatsdSink started with prefix: '$prefix'")
  }

  override def stop(): Unit = {
    reporter.stop()
    logInfo("StatsdSink stopped.")
  }

  override def report(): Unit = reporter.report()
}

