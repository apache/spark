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

import scala.concurrent.duration.MILLISECONDS
import scala.jdk.CollectionConverters._

import com.codahale.metrics.{Counter, Gauge, MetricRegistry, Timer}
import org.scalatest.PrivateMethodTester

import org.apache.spark.SparkFunSuite

class PrometheusServletSuite extends SparkFunSuite with PrivateMethodTester {

  test("register metrics") {
    val sink = createPrometheusServlet()

    val gauge = new Gauge[Double] {
      override def getValue: Double = 5.0
    }

    val counter = new Counter
    counter.inc(10)

    sink.registry.register("gauge1", gauge)
    sink.registry.register("gauge2", gauge)
    sink.registry.register("counter1", counter)

    val metricGaugeKeys = sink.registry.getGauges.keySet.asScala
    assert(metricGaugeKeys.equals(Set("gauge1", "gauge2")),
      "Should contain 2 gauges metrics registered")

    val metricCounterKeys = sink.registry.getCounters.keySet.asScala
    assert(metricCounterKeys.equals(Set("counter1")),
      "Should contain 1 counter metric registered")

    val gaugeValues = sink.registry.getGauges.values.asScala
    assert(gaugeValues.size == 2)
    gaugeValues.foreach(gauge => assert(gauge.getValue == 5.0))

    val counterValues = sink.registry.getCounters.values.asScala
    assert(counterValues.size == 1)
    counterValues.foreach(counter => assert(counter.getCount == 10))
  }

  test("normalize key") {
    val key = "local-1592132938718.driver.LiveListenerBus." +
      "listenerProcessingTime.org.apache.spark.HeartbeatReceiver"
    val sink = createPrometheusServlet()
    val suffix = sink invokePrivate PrivateMethod[String](Symbol("normalizeKey"))(key)
    assert(suffix == "metrics_local_1592132938718_driver_LiveListenerBus_" +
      "listenerProcessingTime_org_apache_spark_HeartbeatReceiver")
  }

  test("Counter should emit Prometheus counter") {
    val sink = createPrometheusServlet()
    val counter = new Counter
    sink.registry.register("counter1", counter)
    counter.inc(42)

    val snapshot = sink.getMetricsSnapshot()

    assert(snapshot.contains("metrics_counter1_total 42"))
    assert(snapshot.contains("# TYPE metrics_counter1_total counter"))
  }

  test("Gauge should emit Prometheus gauge") {
    val sink = createPrometheusServlet()
    val gauge = new Gauge[Double] {
      override def getValue: Double = 5.123
    }
    sink.registry.register("gauge1", gauge)

    val snapshot = sink.getMetricsSnapshot()
    assert(snapshot.contains("metrics_gauge1 5.123"))
    assert(snapshot.contains("# TYPE metrics_gauge1 gauge"))
    validateNumericLinesFormat(snapshot)
  }

  test("Timer should emit summary and rates") {
    val sink = createPrometheusServlet()
    val timer = new Timer
    timer.update(500, MILLISECONDS)
    timer.update(1500, MILLISECONDS)
    sink.registry.register("test.timer", timer)

    val snapshot = sink.getMetricsSnapshot()

    // Summary
    assert(snapshot.contains("metrics_test_timer_duration_seconds_count 2"))
    assert(snapshot.contains("metrics_test_timer_duration_seconds_sum"))
    assert(snapshot.contains("""metrics_test_timer_duration_seconds{quantile="0.5"}"""))

    // Rate
    assert(snapshot.contains("metrics_test_timer_m1_rate"))
    assert(snapshot.contains("metrics_test_timer_m5_rate"))
    assert(snapshot.contains("metrics_test_timer_m15_rate"))

    validateNumericLinesFormat(snapshot)
  }

  test("Histogram should emit summary") {
    val sink = createPrometheusServlet()
    val histogram = sink.registry.histogram("test.hist")

    histogram.update(25)
    histogram.update(75)
    histogram.update(150)

    val snapshot = sink.getMetricsSnapshot()

    assert(snapshot.contains("metrics_test_hist_count 3"))
    assert(snapshot.contains("metrics_test_hist_sum"))
    assert(snapshot.contains("""metrics_test_hist{quantile="0.5"}"""))
    validateNumericLinesFormat(snapshot)
  }

  test("Meter should emit count and rates") {
    val sink = createPrometheusServlet()
    val meter = sink.registry.meter("test.meter")
    meter.mark(5)

    val snapshot = sink.getMetricsSnapshot()

    assert(snapshot.contains("metrics_test_meter_count_cumulative 5"))
    assert(snapshot.contains("metrics_test_meter_m1_rate"))
    assert(snapshot.contains("metrics_test_meter_m5_rate"))
    assert(snapshot.contains("metrics_test_meter_m15_rate"))
    validateNumericLinesFormat(snapshot)
  }

  private def createPrometheusServlet(): PrometheusServlet =
    new PrometheusServlet(new Properties, new MetricRegistry)

  private def validateNumericLinesFormat(formattedOutput: String): Unit = {
    val numericLinePattern = """^metrics_.*\s+([-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?)$""".r
    val lines = formattedOutput.linesIterator.filterNot(_.startsWith("#")).toList
    lines.foreach {
      case numericLinePattern(_, _) => // valid
      case badLine => fail(s"Invalid metric value format: $badLine")
    }
  }
}
