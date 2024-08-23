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

package org.apache.spark.metrics.sink.opentelemetry

import java.util.{Locale, Properties}
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import org.apache.commons.lang3.StringUtils

import org.apache.spark.internal.Logging
import org.apache.spark.metrics.sink.Sink

private[spark] object OpenTelemetryPushSink {
  private def fetchMapFromProperties(
      properties: Properties,
      keyPrefix: String): Map[String, String] = {
    val propertiesMap = scala.collection.mutable.Map[String, String]()
    val valueEnumeration = properties.propertyNames
    val dotCount = keyPrefix.count(_ == '.')
    while (valueEnumeration.hasMoreElements) {
      val key = valueEnumeration.nextElement.asInstanceOf[String]
      if (key.startsWith(keyPrefix)) {
        val dotIndex = StringUtils.ordinalIndexOf(key, ".", dotCount + 1)
        val mapKey = key.substring(dotIndex + 1)
        propertiesMap(mapKey) = properties.getProperty(key)
      }
    }
    propertiesMap.toMap
  }
}

private[spark] class OpenTelemetryPushSink(val property: Properties, val registry: MetricRegistry)
    extends Sink
    with Logging {

  val OPEN_TELEMETRY_KEY_PERIOD = "period"
  val OPEN_TELEMETRY_KEY_UNIT = "unit"
  val OPEN_TELEMETRY_DEFAULT_PERIOD = "10"
  val OPEN_TELEMETRY_DEFAULT_UNIT = "SECONDS"
  val OPEN_TELEMETRY_KEY_HOST = "host"
  val OPEN_TELEMETRY_KEY_PORT = "port"
  val GRPC_METRIC_EXPORTER_HEADER_KEY = "grpc.metric.exporter.header"
  val GRPC_METRIC_EXPORTER_ATTRIBUTES_KEY = "grpc.metric.exporter.attributes"
  val TRUSTED_CERTIFICATE_PATH = "trusted.certificate.path"
  val PRIVATE_KEY_PEM_PATH = "private.key.pem.path"
  val CERTIFICATE_PEM_PATH = "certificate.pem.path"

  val pollPeriod = property
    .getProperty(OPEN_TELEMETRY_KEY_PERIOD, OPEN_TELEMETRY_DEFAULT_PERIOD)
    .toInt

  val pollUnit = TimeUnit.valueOf(
    property
      .getProperty(OPEN_TELEMETRY_KEY_UNIT, OPEN_TELEMETRY_DEFAULT_UNIT)
      .toUpperCase(Locale.ROOT))

  val host = property.getProperty(OPEN_TELEMETRY_KEY_HOST)
  val port = property.getProperty(OPEN_TELEMETRY_KEY_PORT)

  val headersMap =
    OpenTelemetryPushSink.fetchMapFromProperties(property, GRPC_METRIC_EXPORTER_HEADER_KEY)
  val attributesMap =
    OpenTelemetryPushSink.fetchMapFromProperties(property, GRPC_METRIC_EXPORTER_ATTRIBUTES_KEY)

  val trustedCertificatesPath: String =
    property.getProperty(TRUSTED_CERTIFICATE_PATH)

  val privateKeyPemPath: String = property.getProperty(PRIVATE_KEY_PEM_PATH)

  val certificatePemPath: String = property.getProperty(CERTIFICATE_PEM_PATH)

  val reporter = new OpenTelemetryPushReporter(
    registry,
    pollInterval = pollPeriod,
    pollUnit,
    host,
    port,
    headersMap,
    attributesMap,
    trustedCertificatesPath,
    privateKeyPemPath,
    certificatePemPath)

  registry.addListener(reporter)

  override def start(): Unit = {
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop(): Unit = {
    reporter.stop()
  }

  override def report(): Unit = {
    reporter.report()
  }
}
