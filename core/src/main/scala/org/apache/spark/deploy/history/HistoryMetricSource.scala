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

package org.apache.spark.deploy.history

import scala.collection.JavaConverters._

import com.codahale.metrics.{Counter, Gauge, Metric, MetricFilter, MetricRegistry, Timer}

import org.apache.spark.metrics.source.Source
import org.apache.spark.util.Clock

/**
 * An abstract implementation of the metrics [[Source]] trait with some common operations for
 * retrieving entries; the `toString()` operation dumps all counters and gauges.
 */
private[history] abstract class HistoryMetricSource(val prefix: String) extends Source {

  override val metricRegistry = new MetricRegistry()

  /**
   * Register a sequence of metrics
   * @param metrics sequence of metrics to register
   */
  def register(metrics: Seq[(String, Metric)]): Unit = {
    metrics.foreach { case (name, metric) =>
      metricRegistry.register(fullname(name), metric)
    }
  }

  /**
   * Create the full name of a metric by prepending the prefix to the name
   * @param name short name
   * @return the full name to use in registration
   */
  def fullname(name: String): String = {
    MetricRegistry.name(prefix, name)
  }

  /**
   * Dump the counters and gauges.
   * @return a string for logging and diagnostics -not for parsing by machines.
   */
  override def toString: String = {
    val sb = new StringBuilder(s"Metrics for $sourceName:\n")
    def robustAppend[T](s : => T) = {
      try {
        sb.append(s)
      } catch {
        case e: Exception =>
          sb.append(s"(exception: $e)")
      }
    }

    sb.append("  Counters\n")

    metricRegistry.getCounters.asScala.foreach { case (name, counter) =>
      sb.append("    ").append(name).append(" = ")
        .append(counter.getCount).append('\n')
    }
    sb.append("  Gauges\n")
    metricRegistry.getGauges.asScala.foreach { case (name, gauge) =>
      sb.append("    ").append(name).append(" = ")
      robustAppend(gauge.getValue)
      sb.append('\n')
    }
    sb.toString()
  }

  /**
   * Get a named counter.
   * @param counterName name of the counter
   * @return the counter, if found
   */
  def getCounter(counterName: String): Option[Counter] = {
    val full = fullname(counterName)
    Option(metricRegistry.getCounters(new MetricByName(full)).get(full))
  }

  /**
   * Get a gauge of an unknown numeric type.
   * @param gaugeName name of the gauge
   * @return gauge, if found
   */
  def getGauge(gaugeName: String): Option[Gauge[_]] = {
    val full = fullname(gaugeName)
    Option(metricRegistry.getGauges(new MetricByName(full)).get(full))
  }

  /**
   * Get a Long gauge.
   * @param gaugeName name of the gauge
   * @return gauge, if found
   * @throws ClassCastException if the gauge is found but of the wrong type
   */
  def getLongGauge(gaugeName: String): Option[Gauge[Long]] = {
    getGauge(gaugeName).asInstanceOf[Option[Gauge[Long]]]
  }

  /**
   * Get a timer.
   * @param timerName name of the timer
   * @return the timer, if found.
   */
  def getTimer(timerName: String): Option[Timer] = {
    val full = fullname(timerName)
    Option(metricRegistry.getTimers(new MetricByName(full)).get(full))
  }

  /**
   * A filter for metrics by name; include the prefix in the name.
   * @param fullname full name of metric
   */
  private class MetricByName(fullname: String) extends MetricFilter {
    def matches(metricName: String, metric: Metric): Boolean = metricName == fullname
  }
}

/**
 * A Long gauge from a lambda expression; the expression is evaluated
 * whenever the metrics are queried.
 * @param expression expression which generates the value.
 */
private[history] class LambdaLongGauge(expression: (() => Long)) extends Gauge[Long] {
  override def getValue: Long = expression()
}

/**
 * A timestamp is a gauge which is set to a point in time
 * as measured in millseconds since the epoch began.
 */
private[history] class TimestampGauge(clock: Clock) extends Gauge[Long] {
  var time = 0L

  /** Current value. */
  override def getValue: Long = time

  /** Set a new value. */
  def setValue(t: Long): Unit = {
    time = t
  }

  /** Set to the current system time. */
  def touch(): Unit = {
    setValue(clock.getTimeMillis())
  }
}
