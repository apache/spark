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

import scala.jdk.CollectionConverters._

import com.codahale.metrics.{Counter, Gauge, MetricRegistry}
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
      "listenerProcessingTime_org_apache_spark_HeartbeatReceiver_")
  }

  private def createPrometheusServlet(): PrometheusServlet =
    new PrometheusServlet(new Properties, new MetricRegistry)
}
