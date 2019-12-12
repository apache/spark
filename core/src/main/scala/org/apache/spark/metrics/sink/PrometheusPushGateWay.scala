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

import java.util
import java.util.Properties

import com.codahale.metrics.MetricRegistry
import io.prometheus.client.{CollectorRegistry, Gauge}
import io.prometheus.client.exporter.PushGateway
import org.apache.commons.lang3.StringUtils

import org.apache.spark.SecurityManager

/**
 * PrometheusPushGateWay that exports Metric Metrics via Prometheus PushGateway.
 */
private[spark] class PrometheusPushGateWay(
    val property: Properties,
    val registry: MetricRegistry,
    securityMgr: SecurityManager)
  extends Sink {
  val PROMETHEUS_DEFAULT_PREFIX = ""
  val PROMETHEUS_DEFAULT_GROUP_KEY = "job=spark;instance=spark"
  val PROMETHEUS_DEFAULT_ON_SHUTDOWN = "true"

  val PROMETHEUS_KEY_JOBNAME = "job"
  val PROMETHEUS_KEY_HOST = "host"
  val PROMETHEUS_KEY_PORT = "port"
  val PROMETHEUS_KEY_PREFIX = "prefix"
  val PROMETHEUS_KEY_GROUP_KEY = "groupKey"
  val PROMETHEUS_KEY_DELETE_ON_SHUTDOWN = "deleteOnShutdown"

  var groupingKey: util.Map[String, String] = _
  var pushGateway: PushGateway = _

  def propertyToOption(prop: String): Option[String] = Option(property.getProperty(prop))

  if (propertyToOption(PROMETHEUS_KEY_JOBNAME).isEmpty) {
    throw new Exception("Prometheus sink requires 'job' property.")
  }

  if (propertyToOption(PROMETHEUS_KEY_HOST).isEmpty) {
    throw new Exception("Prometheus sink requires 'host' property.")
  }

  if (propertyToOption(PROMETHEUS_KEY_PORT).isEmpty) {
    throw new Exception("Prometheus sink requires 'port' property.")
  }

  val jobName: String = propertyToOption(PROMETHEUS_KEY_JOBNAME).get
  val host: String = propertyToOption(PROMETHEUS_KEY_HOST).get
  val port: Int = propertyToOption(PROMETHEUS_KEY_PORT).get.toInt
  val prefix: String =
    propertyToOption(PROMETHEUS_KEY_PREFIX).getOrElse(PROMETHEUS_DEFAULT_PREFIX)
  val deleteOnShutdown: Boolean = propertyToOption(PROMETHEUS_KEY_DELETE_ON_SHUTDOWN)
    .getOrElse(PROMETHEUS_DEFAULT_ON_SHUTDOWN).toBoolean

  groupingKey = parseGroupingKey(
    propertyToOption(PROMETHEUS_KEY_GROUP_KEY).getOrElse(PROMETHEUS_DEFAULT_GROUP_KEY))

  pushGateway = new PushGateway(host + ":" + port)

  def parseGroupingKey(groupingKeyConfig: String): util.Map[String, String] = {
    val groupingKey = new util.HashMap[String, String]
    if (!groupingKeyConfig.isEmpty) {
      val kvs = groupingKeyConfig.split(";")
      for (kv <- kvs) {
        val idx = kv.indexOf("=")
        if (idx > 0) {
          val labelKey = kv.substring(0, idx)
          val labelValue = kv.substring(idx + 1)
          if (StringUtils.isNotBlank(labelKey) || StringUtils.isNotBlank(labelValue)) {
            groupingKey.put(labelKey, labelValue)
          }
        }
      }
    }
    groupingKey
  }

  def getMetricsSnapshot(): CollectorRegistry = {
    import scala.collection.JavaConverters._

    val metricRegistry = new CollectorRegistry
    val map: util.HashMap[String, Double] = new util.HashMap[String, Double]()
    registry.getGauges.asScala.foreach { case (k, v) =>
      if (!v.getValue.isInstanceOf[String]) {
        v.getValue match {
          case d: Int =>
            map.put(s"${normalizeKey(k)}Value", d)
          case d: Long =>
            map.put(s"${normalizeKey(k)}Value", d)
          case d: Double =>
            map.put(s"${normalizeKey(k)}Value", d)
        }
      }
    }
    registry.getCounters.asScala.foreach { case (k, v) =>
      map.put(s"${normalizeKey(k)}Count", v.getCount)
    }
    registry.getHistograms.asScala.foreach { case (k, h) =>
      val snapshot = h.getSnapshot
      val prefix = normalizeKey(k)
      map.put(s"${prefix}Count", h.getCount)
      map.put(s"${prefix}Max", snapshot.getMax)
      map.put(s"${prefix}Mean", snapshot.getMean)
      map.put(s"${prefix}Min", snapshot.getMin)
      map.put(s"${prefix}50thPercentile", snapshot.getMedian)
      map.put(s"${prefix}75thPercentile", snapshot.get75thPercentile)
      map.put(s"${prefix}95thPercentile", snapshot.get95thPercentile)
      map.put(s"${prefix}98thPercentile", snapshot.get98thPercentile)
      map.put(s"${prefix}99thPercentile", snapshot.get99thPercentile)
      map.put(s"${prefix}999thPercentile", snapshot.get999thPercentile)
      map.put(s"${prefix}StdDev", snapshot.getStdDev)
    }
    registry.getMeters.entrySet.iterator.asScala.foreach { kv =>
      val meter = kv.getValue
      val prefix = normalizeKey(kv.getKey)
      map.put(s"${prefix}Count", meter.getCount)
      map.put(s"${prefix}MeanRate", meter.getMeanRate)
      map.put(s"${prefix}OneMinuteRate", meter.getOneMinuteRate)
      map.put(s"${prefix}FiveMinuteRate", meter.getFiveMinuteRate)
      map.put(s"${prefix}FifteenMinuteRate", meter.getFifteenMinuteRate)
    }
    registry.getTimers.entrySet.iterator.asScala.foreach { kv =>
      val prefix = normalizeKey(kv.getKey)
      val timer = kv.getValue
      val snapshot = timer.getSnapshot
      map.put(s"${prefix}Count", timer.getCount)
      map.put(s"${prefix}Max", snapshot.getMax)
      map.put(s"${prefix}Mean", snapshot.getMean)
      map.put(s"${prefix}Min", snapshot.getMin)
      map.put(s"${prefix}50thPercentile", snapshot.getMedian)
      map.put(s"${prefix}75thPercentile", snapshot.get75thPercentile)
      map.put(s"${prefix}98thPercentile", snapshot.get95thPercentile)
      map.put(s"${prefix}99thPercentile", snapshot.get98thPercentile)
      map.put(s"${prefix}999thPercentile", snapshot.get99thPercentile)
      map.put(s"${prefix}StdDev", snapshot.get999thPercentile)
      map.put(s"${prefix}FifteenMinuteRate", snapshot.getStdDev)
      map.put(s"${prefix}FiveMinuteRate", timer.getFifteenMinuteRate)
      map.put(s"${prefix}OneMinuteRate", timer.getOneMinuteRate)
      map.put(s"${prefix}MeanRate", timer.getMeanRate)
    }
    map.asScala.foreach(e => Gauge.build().name(e._1).help(e._1).register(metricRegistry).set(e._2))
    metricRegistry
  }

  private def normalizeKey(key: String): String = {
    s"metrics_${key.replaceAll("[^a-zA-Z0-9]", "_")}_"
  }

  override def start(): Unit = {}

  override def stop(): Unit = {
    if (deleteOnShutdown && pushGateway != null) {
      if (groupingKey.isEmpty) {
        pushGateway.delete(jobName)
      } else {
        pushGateway.delete(jobName, groupingKey)
      }
    }
  }

  override def report(): Unit = {
    val metricRegistry: CollectorRegistry = getMetricsSnapshot()
    if (pushGateway != null) {
      if (groupingKey.isEmpty) {
        pushGateway.push(metricRegistry, jobName)
      } else {
        pushGateway.push(metricRegistry, jobName, groupingKey)
      }
    }
  }
}
