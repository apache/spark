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

import com.codahale.metrics._

import org.apache.spark.{SparkException, SparkFunSuite}

class GraphiteSinkSuite extends SparkFunSuite {

  test("GraphiteSink with default MetricsFilter") {
    val props = new Properties
    props.put("host", "127.0.0.1")
    props.put("port", "54321")
    val registry = new MetricRegistry

    val sink = new GraphiteSink(props, registry)

    val gauge = new Gauge[Double] {
      override def getValue: Double = 1.23
    }
    sink.registry.register("gauge", gauge)
    sink.registry.register("anothergauge", gauge)
    sink.registry.register("streaminggauge", gauge)

    val metricKeys = sink.registry.getGauges(sink.filter).keySet.asScala

    assert(metricKeys.equals(Set("gauge", "anothergauge", "streaminggauge")),
      "Should contain all metrics registered")
  }

  test("GraphiteSink with regex MetricsFilter") {
    val props = new Properties
    props.put("host", "127.0.0.1")
    props.put("port", "54321")
    props.put("regex", "local-[0-9]+.driver.(CodeGenerator|BlockManager)")
    val registry = new MetricRegistry

    val sink = new GraphiteSink(props, registry)

    val gauge = new Gauge[Double] {
      override def getValue: Double = 1.23
    }
    sink.registry.register("gauge", gauge)
    sink.registry.register("anothergauge", gauge)
    sink.registry.register("streaminggauge", gauge)
    sink.registry.register("local-1563838109260.driver.CodeGenerator.generatedMethodSize", gauge)
    sink.registry.register("local-1563838109260.driver.BlockManager.disk.diskSpaceUsed_MB", gauge)
    sink.registry.register("local-1563813796998.driver.spark.streaming.nicklocal.latency", gauge)
    sink.registry.register("myapp.driver.CodeGenerator.generatedMethodSize", gauge)
    sink.registry.register("myapp.driver.BlockManager.disk.diskSpaceUsed_MB", gauge)

    val metricKeys = sink.registry.getGauges(sink.filter).keySet.asScala

    val filteredMetricKeys = Set(
      "local-1563838109260.driver.CodeGenerator.generatedMethodSize",
      "local-1563838109260.driver.BlockManager.disk.diskSpaceUsed_MB"
    )

    assert(metricKeys.equals(filteredMetricKeys),
      "Should contain only metrics matches regex filter")
  }

  test("GraphiteSink without host") {
    val props = new Properties
    props.put("port", "54321")
    val registry = new MetricRegistry

    val e = intercept[SparkException] {
      new GraphiteSink(props, registry)
    }
    assert(e.getErrorClass === "GRAPHITE_SINK_PROPERTY_MISSING")
    assert(e.getMessage ===
      "[GRAPHITE_SINK_PROPERTY_MISSING] Graphite sink requires 'host' property.")
  }

  test("GraphiteSink without port") {
    val props = new Properties
    props.put("host", "127.0.0.1")
    val registry = new MetricRegistry

    val e = intercept[SparkException] {
      new GraphiteSink(props, registry)
    }
    assert(e.getErrorClass === "GRAPHITE_SINK_PROPERTY_MISSING")
    assert(e.getMessage ===
      "[GRAPHITE_SINK_PROPERTY_MISSING] Graphite sink requires 'port' property.")
  }

  test("GraphiteSink with invalid protocol") {
    val props = new Properties
    props.put("host", "127.0.0.1")
    props.put("port", "54321")
    props.put("protocol", "http")
    val registry = new MetricRegistry

    val e = intercept[SparkException] {
      new GraphiteSink(props, registry)
    }
    assert(e.getErrorClass === "GRAPHITE_SINK_INVALID_PROTOCOL")
    assert(e.getMessage ===
      "[GRAPHITE_SINK_INVALID_PROTOCOL] Invalid Graphite protocol: http")
  }
}
