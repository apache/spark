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

package org.apache.spark.sql.kafka010

import com.codahale.metrics.MetricRegistry
import java.util.{Locale, Properties}
import java.util.concurrent.TimeUnit

import org.apache.spark.SecurityManager
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.sink.Sink

class KafkaSinkMetrics(
                        val properties: Properties,
                        val registry: MetricRegistry,
                        securityMgr: SecurityManager) extends Sink with Logging {
  val KAFKA_SINK_DEFAULT_PERIOD = "10"
  val KAFKA_SINK_DEFAULT_UNIT = "SECONDS"
  private def getProp(prop: String): Option[String] = Option(properties.getProperty(prop))
  val broker = getProp("broker").get
  val topic = getProp("topic").get
  val reporter = new KafkaReporter(registry, broker, topic, properties)

  def start(): Unit = {
    logInfo(s"Starting Kafka metric reporter at $broker, topic $topic")
    val period = getProp("period").getOrElse(KAFKA_SINK_DEFAULT_PERIOD).toLong
    val timeUnit = TimeUnit.valueOf(getProp("unit")
      .getOrElse("KAFKA_SINK_DEFAULT_UNIT")
      .toUpperCase(Locale.ROOT))
    val MINIMAL_POLL_UNIT = TimeUnit.SECONDS
    val MINIMAL_POLL_PERIOD = 1
    val periodt = MINIMAL_POLL_UNIT.convert(period, timeUnit)
    if (periodt < MINIMAL_POLL_PERIOD) {
      throw new IllegalArgumentException("Polling period " + period + " " + timeUnit +
        " below than minimal polling period ")
    }
    reporter.start(period, timeUnit)
  }

  def stop(): Unit = {
    logInfo(s"Stopping Kafka metric reporter at $broker, topic $topic")
    reporter.stop()
  }

  def report(): Unit = {
    logInfo(s"Reporting metrics to Kafka reporter at $broker, topic $topic")
    reporter.report()
  }
}
