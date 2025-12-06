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

import com.codahale.metrics.MetricRegistry
import jakarta.servlet.http.HttpServletRequest
import org.eclipse.jetty.servlet.ServletContextHandler

import org.apache.spark.SparkConf
import org.apache.spark.annotation.{DeveloperApi, Since, Unstable}
import org.apache.spark.ui.JettyUtils._

/**
 * :: DeveloperApi ::
 * This exposes the metrics of the given registry with Prometheus format.
 *
 * The output is consistent with /metrics/json result in terms of item ordering
 * and with the previous result of Spark JMX Sink + Prometheus JMX Converter combination
 * in terms of key string format.
 *
 * This is used by Spark MetricsSystem internally and Spark K8s operator.
 */
@Unstable
@DeveloperApi
private[spark] class PrometheusServlet(
    val property: Properties, val registry: MetricRegistry) extends Sink {

  val SERVLET_KEY_PATH = "path"

  val servletPath = property.getProperty(SERVLET_KEY_PATH)

  def getHandlers(conf: SparkConf): Array[ServletContextHandler] = {
    Array[ServletContextHandler](
      createServletHandler(servletPath,
        new ServletParams(request => getMetricsSnapshot(request), "text/plain"), conf)
    )
  }

  def getMetricsSnapshot(request: HttpServletRequest): String = getMetricsSnapshot()

  @Since("4.0.0")
  def getMetricsSnapshot(): String = {
    import scala.jdk.CollectionConverters._

    val PERCENTILE_P50 = "0.5"
    val PERCENTILE_P75 = "0.75"
    val PERCENTILE_P95 = "0.95"
    val PERCENTILE_P98 = "0.98"
    val PERCENTILE_P99 = "0.99"
    val PERCENTILE_P999 = "0.999"

    val sb = new StringBuilder()
    registry.getGauges.asScala.foreach { case (k, v) =>
      v.getValue match {
        case n: Number =>
          sb.append(s"# HELP ${normalizeKey(k)} Gauge metric\n")
          sb.append(s"# TYPE ${normalizeKey(k)} gauge\n")
          sb.append(s"${normalizeKey(k)} ${n.doubleValue()}\n")
        case _ => // non-numeric gauges
      }
    }
    registry.getCounters.asScala.foreach { case (k, v) =>
      val name = s"${normalizeKey(k)}_total"
      sb.append(s"# HELP ${name} Counter metric\n")
      sb.append(s"# TYPE ${name} counter\n")
      sb.append(s"${name} ${v.getCount}\n")
    }
    registry.getHistograms.asScala.foreach { case (k, h) =>
      val snapshot = h.getSnapshot
      val values = snapshot.getValues.map(_.toDouble)
      val prefix = normalizeKey(k)
      sb.append(s"# HELP ${prefix} Histogram metric\n")
      sb.append(s"# TYPE ${prefix} summary\n")
      sb.append(s"${prefix}{quantile=\"${PERCENTILE_P50}\"} ${snapshot.getMedian}\n")
      sb.append(s"${prefix}{quantile=\"${PERCENTILE_P75}\"} ${snapshot.get75thPercentile}\n")
      sb.append(s"${prefix}{quantile=\"${PERCENTILE_P95}\"} ${snapshot.get95thPercentile}\n")
      sb.append(s"${prefix}{quantile=\"${PERCENTILE_P98}\"} ${snapshot.get98thPercentile}\n")
      sb.append(s"${prefix}{quantile=\"${PERCENTILE_P99}\"} ${snapshot.get99thPercentile}\n")
      sb.append(s"${prefix}{quantile=\"${PERCENTILE_P999}\"} ${snapshot.get999thPercentile}\n")
      sb.append(s"${prefix}_count ${h.getCount}\n")
      sb.append(s"${prefix}_sum ${values.sum}\n")
      sb.append(s"# HELP ${prefix}_min Minimum value\n")
      sb.append(s"# TYPE ${prefix}_min gauge\n")
      sb.append(s"${prefix}_min ${snapshot.getMin}\n")
      sb.append(s"# HELP ${prefix}_max Maximal value\n")
      sb.append(s"# TYPE ${prefix}_max gauge\n")
      sb.append(s"${prefix}_max ${snapshot.getMax}\n")
      sb.append(s"# HELP ${prefix}_mean Mean value\n")
      sb.append(s"# TYPE ${prefix}_mean gauge\n")
      sb.append(s"${prefix}_mean ${snapshot.getMedian}\n")
      sb.append(s"# HELP ${prefix}_stddev Standard deviation value\n")
      sb.append(s"# TYPE ${prefix}_stddev gauge\n")
      sb.append(s"${prefix}_stddev ${snapshot.getStdDev}\n")
    }
    registry.getMeters.entrySet.iterator.asScala.foreach { kv =>
      val prefix = normalizeKey(kv.getKey)
      val meter = kv.getValue
      sb.append(s"# HELP ${prefix}_count_cumulative Meter counts metric\n")
      sb.append(s"# TYPE ${prefix}_count_cumulative gauge\n")
      sb.append(s"${prefix}_count_cumulative ${meter.getCount}\n")
      sb.append(s"# HELP ${prefix}_mean_rate total counts metric\n")
      sb.append(s"# TYPE ${prefix}_mean_rate gauge\n")
      sb.append(s"${prefix}_mean_rate ${meter.getMeanRate}\n")
      sb.append(s"# HELP ${prefix}_m1_rate 1-min moving avg metric\n")
      sb.append(s"# TYPE ${prefix}_m1_rate gauge\n")
      sb.append(s"${prefix}_m1_rate ${meter.getOneMinuteRate}\n")
      sb.append(s"# HELP ${prefix}_m5_rate 5-min moving avg metric\n")
      sb.append(s"# TYPE ${prefix}_m5_rate gauge\n")
      sb.append(s"${prefix}_m5_rate ${meter.getFiveMinuteRate}\n")
      sb.append(s"# HELP ${prefix}_m15_rate 15-min moving avg metric\n")
      sb.append(s"# TYPE ${prefix}_m15_rate gauge\n")
      sb.append(s"${prefix}_m15_rate ${meter.getFifteenMinuteRate}\n")
    }

    registry.getTimers.entrySet.iterator.asScala.foreach { kv =>
      val prefix = normalizeKey(kv.getKey)
      val timer = kv.getValue
      val snapshot = timer.getSnapshot
      val NANOS_TO_SECONDS_UNIT = 1e9
      def nanosToSeconds(n: Double): Double = n / NANOS_TO_SECONDS_UNIT
      val medianValue = nanosToSeconds(snapshot.getMedian)
      val p75Value = nanosToSeconds(snapshot.get75thPercentile)
      val p95Value = nanosToSeconds(snapshot.get95thPercentile)
      val p98Value = nanosToSeconds(snapshot.get98thPercentile)
      val p99Value = nanosToSeconds(snapshot.get99thPercentile)
      val p999Value = nanosToSeconds(snapshot.get999thPercentile)

      val durationSecondsName = s"${prefix}_duration_seconds"
      sb.append(s"# HELP $durationSecondsName Timer summary metric\n")
      sb.append(s"# TYPE $durationSecondsName summary\n")
      sb.append(s"${durationSecondsName}{quantile=\"${PERCENTILE_P50}\"} ${medianValue}\n")
      sb.append(s"${durationSecondsName}{quantile=\"${PERCENTILE_P75}\"} ${p75Value}\n")
      sb.append(s"${durationSecondsName}{quantile=\"${PERCENTILE_P95}\"} ${p95Value}\n")
      sb.append(s"${durationSecondsName}{quantile=\"${PERCENTILE_P98}\"} ${p98Value}\n")
      sb.append(s"${durationSecondsName}{quantile=\"${PERCENTILE_P99}\"} ${p99Value}\n")
      sb.append(s"${durationSecondsName}{quantile=\"${PERCENTILE_P999}\"} ${p999Value}\n")
      sb.append(s"${durationSecondsName}_count ${timer.getCount}\n")
      sb.append(s"${durationSecondsName}_sum " +
        s"${snapshot.getValues.map(_.toDouble / NANOS_TO_SECONDS_UNIT).sum}\n")
      sb.append(s"# HELP ${prefix}_m1_rate Timer rate 1-min moving avg metric\n")
      sb.append(s"# TYPE ${prefix}_m1_rate gauge\n")
      sb.append(s"${prefix}_m1_rate ${timer.getOneMinuteRate}\n")
      sb.append(s"# HELP ${prefix}_m5_rate Timer rate 5-min moving avg metric\n")
      sb.append(s"# TYPE ${prefix}_m5_rate gauge\n")
      sb.append(s"${prefix}_m5_rate ${timer.getFiveMinuteRate}\n")
      sb.append(s"# HELP ${prefix}_m15_rate Timer rate 15-min moving avg metric\n")
      sb.append(s"# TYPE ${prefix}_m15_rate gauge\n")
      sb.append(s"${prefix}_m15_rate ${timer.getFifteenMinuteRate}\n")
    }
    sb.toString()
  }

  private def normalizeKey(key: String): String = {
    s"metrics_${key.replaceAll("[^a-zA-Z0-9]", "_")}"
  }

  override def start(): Unit = { }

  override def stop(): Unit = { }

  override def report(): Unit = { }
}
