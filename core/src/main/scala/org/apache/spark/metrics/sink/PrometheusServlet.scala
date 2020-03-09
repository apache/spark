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
import org.apache.spark.ui.JettyUtils._

/**
 * This exposes the metrics of the given registry with Prometheus format.
 *
 * The output is consistent with /metrics/json result in terms of item ordering
 * and with the previous result of Spark JMX Sink + Prometheus JMX Converter combination
 * in terms of key string format.
 */
private[spark] class PrometheusServlet(
    val property: Properties,
    val registry: MetricRegistry,
    securityMgr: SecurityManager)
  extends Sink {

  val SERVLET_KEY_PATH = "path"

  val servletPath = property.getProperty(SERVLET_KEY_PATH)

  def getHandlers(conf: SparkConf): Array[ServletContextHandler] = {
    Array[ServletContextHandler](
      createServletHandler(servletPath,
        new ServletParams(request => getMetricsSnapshot(request), "text/plain"), conf)
    )
  }

  def getMetricsSnapshot(request: HttpServletRequest): String = {
    import scala.collection.JavaConverters._

    val sb = new StringBuilder()
    registry.getGauges.asScala.foreach { case (k, v) =>
      if (!v.getValue.isInstanceOf[String]) {
        sb.append(s"${normalizeKey(k)}Value ${v.getValue}\n")
      }
    }
    registry.getCounters.asScala.foreach { case (k, v) =>
      sb.append(s"${normalizeKey(k)}Count ${v.getCount}\n")
    }
    registry.getHistograms.asScala.foreach { case (k, h) =>
      val snapshot = h.getSnapshot
      val prefix = normalizeKey(k)
      sb.append(s"${prefix}Count ${h.getCount}\n")
      sb.append(s"${prefix}Max ${snapshot.getMax}\n")
      sb.append(s"${prefix}Mean ${snapshot.getMean}\n")
      sb.append(s"${prefix}Min ${snapshot.getMin}\n")
      sb.append(s"${prefix}50thPercentile ${snapshot.getMedian}\n")
      sb.append(s"${prefix}75thPercentile ${snapshot.get75thPercentile}\n")
      sb.append(s"${prefix}95thPercentile ${snapshot.get95thPercentile}\n")
      sb.append(s"${prefix}98thPercentile ${snapshot.get98thPercentile}\n")
      sb.append(s"${prefix}99thPercentile ${snapshot.get99thPercentile}\n")
      sb.append(s"${prefix}999thPercentile ${snapshot.get999thPercentile}\n")
      sb.append(s"${prefix}StdDev ${snapshot.getStdDev}\n")
    }
    registry.getMeters.entrySet.iterator.asScala.foreach { kv =>
      val prefix = normalizeKey(kv.getKey)
      val meter = kv.getValue
      sb.append(s"${prefix}Count ${meter.getCount}\n")
      sb.append(s"${prefix}MeanRate ${meter.getMeanRate}\n")
      sb.append(s"${prefix}OneMinuteRate ${meter.getOneMinuteRate}\n")
      sb.append(s"${prefix}FiveMinuteRate ${meter.getFiveMinuteRate}\n")
      sb.append(s"${prefix}FifteenMinuteRate ${meter.getFifteenMinuteRate}\n")
    }
    registry.getTimers.entrySet.iterator.asScala.foreach { kv =>
      val prefix = normalizeKey(kv.getKey)
      val timer = kv.getValue
      val snapshot = timer.getSnapshot
      sb.append(s"${prefix}Count ${timer.getCount}\n")
      sb.append(s"${prefix}Max ${snapshot.getMax}\n")
      sb.append(s"${prefix}Mean ${snapshot.getMax}\n")
      sb.append(s"${prefix}Min ${snapshot.getMin}\n")
      sb.append(s"${prefix}50thPercentile ${snapshot.getMedian}\n")
      sb.append(s"${prefix}75thPercentile ${snapshot.get75thPercentile}\n")
      sb.append(s"${prefix}95thPercentile ${snapshot.get95thPercentile}\n")
      sb.append(s"${prefix}98thPercentile ${snapshot.get98thPercentile}\n")
      sb.append(s"${prefix}99thPercentile ${snapshot.get99thPercentile}\n")
      sb.append(s"${prefix}999thPercentile ${snapshot.get999thPercentile}\n")
      sb.append(s"${prefix}StdDev ${snapshot.getStdDev}\n")
      sb.append(s"${prefix}FifteenMinuteRate ${timer.getFifteenMinuteRate}\n")
      sb.append(s"${prefix}FiveMinuteRate ${timer.getFiveMinuteRate}\n")
      sb.append(s"${prefix}OneMinuteRate ${timer.getOneMinuteRate}\n")
      sb.append(s"${prefix}MeanRate ${timer.getMeanRate}\n")
    }
    sb.toString()
  }

  private def normalizeKey(key: String): String = {
    s"metrics_${key.replaceAll("[^a-zA-Z0-9]", "_")}_"
  }

  override def start(): Unit = { }

  override def stop(): Unit = { }

  override def report(): Unit = { }
}
