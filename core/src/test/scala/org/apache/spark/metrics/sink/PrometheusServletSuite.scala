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

import scala.collection.JavaConverters._
import com.codahale.metrics.{Gauge, MetricRegistry}
import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.scalatest.PrivateMethodTester

class PrometheusServletSuite extends SparkFunSuite with PrivateMethodTester {

  test("PrometheusServletSuite default test") {
    val props = new Properties
    props.put("host", "127.0.0.1")
    props.put("port", "12340")
    val registry = new MetricRegistry
    val securityMgr = new SecurityManager(new SparkConf(false))

    val sink = new PrometheusServlet(props, registry, securityMgr)
    val gauge = new Gauge[Double] {
      override def getValue: Double = 1.23
    }
    sink.registry.register("gauge", gauge)
    sink.registry.register("anothergauge", gauge)
    sink.registry.register("streaminggauge", gauge)

    val metricKeys = sink.registry.getGauges().keySet.asScala

    assert(metricKeys.equals(Set("gauge", "anothergauge", "streaminggauge")),
      "Should contain all metrics registered")
  }

  test("PrometheusServletSuite private function test") {
    val key = "local-1592132938718.driver.LiveListenerBus.listenerProcessingTime.org.apache.spark.HeartbeatReceiver"
    val props = new Properties
    props.put("host", "127.0.0.1")
    props.put("port", "12340")
    val registry = new MetricRegistry
    val securityMgr = new SecurityManager(new SparkConf(false))

    val sink = new PrometheusServlet(props, registry, securityMgr)
    val (applicationId, suffix) = sink invokePrivate PrivateMethod[(String, String)]('normalizeKey)(key)
    assert(applicationId == "local-1592132938718")
    assert(suffix == "metrics_driver_LiveListenerBus_listenerProcessingTime_org_apache_spark_HeartbeatReceiver_")
  }

}
