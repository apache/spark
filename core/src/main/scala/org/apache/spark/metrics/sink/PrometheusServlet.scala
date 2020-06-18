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
import javax.servlet.http.HttpServletRequest

import com.codahale.metrics.MetricRegistry
import org.eclipse.jetty.servlet.ServletContextHandler

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.annotation.Experimental
import org.apache.spark.ui.JettyUtils._

/**
 * :: Experimental ::
 * This exposes the metrics of the given registry with Prometheus format.
 *
 * The output is consistent with /metrics/json result in terms of item ordering
 * and with the previous result of Spark JMX Sink + Prometheus JMX Converter combination
 * in terms of key string format.
 */
@Experimental
private[spark] class PrometheusServlet(
    val property: Properties,
    val registry: MetricRegistry,
    securityMgr: SecurityManager)
  extends Sink {

  val SERVLET_KEY_PATH = "path"
  val DRIVER_SUBSTRING = ".driver."

  val servletPath = property.getProperty(SERVLET_KEY_PATH)

  def getHandlers(conf: SparkConf): Array[ServletContextHandler] = {
    Array[ServletContextHandler](
      createServletHandler(servletPath,
        new ServletParams(request => getMetricsSnapshot(request), "text/plain"), conf)
    )
  }

  def getMetricsSnapshot(request: HttpServletRequest): String = {
    import scala.collection.JavaConverters._

    def gaugesLabel(applicationId: String) =
      s"""{application_id="${applicationId}", type="gauges"}"""
    def countersLabel(applicationId: String) =
      s"""{application_id="${applicationId}", type="counters"}"""
    def metersLabel(applicationId: String) = countersLabel(applicationId)
    def histogramslabels(applicationId: String) =
      s"""{application_id="${applicationId}", type="histograms"}"""
    def timersLabels(applicationId: String) =
      s"""{application_id="${applicationId}", type="timers"}"""

    val sb = new StringBuilder()
    registry.getGauges.asScala.foreach { case (k, v) =>
      if (!v.getValue.isInstanceOf[String]) {
        val (applicationId, prefix) = normalizeKey(k)
        val label = gaugesLabel(applicationId)
        sb.append(s"${prefix}Number$label ${v.getValue}\n")
        sb.append(s"${prefix}Value$label ${v.getValue}\n")
      }
    }
    registry.getCounters.asScala.foreach { case (k, v) =>
      val (applicationId, prefix) = normalizeKey(k)
      val label = countersLabel(applicationId)
      sb.append(s"${prefix}Count$label ${v.getCount}\n")
    }
    registry.getHistograms.asScala.foreach { case (k, h) =>
      val snapshot = h.getSnapshot
      val (applicationId, prefix) = normalizeKey(k)
      val label = histogramslabels(applicationId)
      sb.append(s"${prefix}Count$label ${h.getCount}\n")
      sb.append(s"${prefix}Max$label ${snapshot.getMax}\n")
      sb.append(s"${prefix}Mean$label ${snapshot.getMean}\n")
      sb.append(s"${prefix}Min$label ${snapshot.getMin}\n")
      sb.append(s"${prefix}50thPercentile$label ${snapshot.getMedian}\n")
      sb.append(s"${prefix}75thPercentile$label ${snapshot.get75thPercentile}\n")
      sb.append(s"${prefix}95thPercentile$label ${snapshot.get95thPercentile}\n")
      sb.append(s"${prefix}98thPercentile$label ${snapshot.get98thPercentile}\n")
      sb.append(s"${prefix}99thPercentile$label ${snapshot.get99thPercentile}\n")
      sb.append(s"${prefix}999thPercentile$label ${snapshot.get999thPercentile}\n")
      sb.append(s"${prefix}StdDev$label ${snapshot.getStdDev}\n")
    }
    registry.getMeters.entrySet.iterator.asScala.foreach { kv =>
      val (applicationId, prefix) = normalizeKey(kv.getKey)
      val label = metersLabel(applicationId)
      val meter = kv.getValue
      sb.append(s"${prefix}Count$label ${meter.getCount}\n")
      sb.append(s"${prefix}MeanRate$label ${meter.getMeanRate}\n")
      sb.append(s"${prefix}OneMinuteRate$label ${meter.getOneMinuteRate}\n")
      sb.append(s"${prefix}FiveMinuteRate$label ${meter.getFiveMinuteRate}\n")
      sb.append(s"${prefix}FifteenMinuteRate$label ${meter.getFifteenMinuteRate}\n")
    }
    registry.getTimers.entrySet.iterator.asScala.foreach { kv =>
      val (applicationId, prefix) = normalizeKey(kv.getKey)
      val label = timersLabels(applicationId)
      val timer = kv.getValue
      val snapshot = timer.getSnapshot
      sb.append(s"${prefix}Count$label ${timer.getCount}\n")
      sb.append(s"${prefix}Max$label ${snapshot.getMax}\n")
      sb.append(s"${prefix}Mean$label ${snapshot.getMax}\n")
      sb.append(s"${prefix}Min$label ${snapshot.getMin}\n")
      sb.append(s"${prefix}50thPercentile$label ${snapshot.getMedian}\n")
      sb.append(s"${prefix}75thPercentile$label ${snapshot.get75thPercentile}\n")
      sb.append(s"${prefix}95thPercentile$label ${snapshot.get95thPercentile}\n")
      sb.append(s"${prefix}98thPercentile$label ${snapshot.get98thPercentile}\n")
      sb.append(s"${prefix}99thPercentile$label ${snapshot.get99thPercentile}\n")
      sb.append(s"${prefix}999thPercentile$label ${snapshot.get999thPercentile}\n")
      sb.append(s"${prefix}StdDev$label ${snapshot.getStdDev}\n")
      sb.append(s"${prefix}FifteenMinuteRate$label ${timer.getFifteenMinuteRate}\n")
      sb.append(s"${prefix}FiveMinuteRate$label ${timer.getFiveMinuteRate}\n")
      sb.append(s"${prefix}OneMinuteRate$label ${timer.getOneMinuteRate}\n")
      sb.append(s"${prefix}MeanRate$label ${timer.getMeanRate}\n")
    }
    sb.toString()
  }

  private def normalizeKey(key: String): (String, String) = {
    val applicationId = key.substring(0, key.indexOf(DRIVER_SUBSTRING))
    val suffix = key.substring(key.indexOf(DRIVER_SUBSTRING), key.length)
    (applicationId, s"metrics${suffix.replaceAll("[^a-zA-Z0-9]", "_")}_")
  }

  override def start(): Unit = { }

  override def stop(): Unit = { }

  override def report(): Unit = { }
}
