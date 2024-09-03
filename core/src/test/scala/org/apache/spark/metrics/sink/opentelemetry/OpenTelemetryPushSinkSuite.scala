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

import java.util.Properties

import com.codahale.metrics._
import org.junit.jupiter.api.Assertions.assertEquals
import org.scalatest.PrivateMethodTester

import org.apache.spark.SparkFunSuite

class OpenTelemetryPushSinkSuite
  extends SparkFunSuite with PrivateMethodTester {
  test("fetch properties map") {
    val properties = new Properties
    properties.put("foo1.foo2.foo3.foo4.header.key1.key2.key3", "value1")
    properties.put("foo1.foo2.foo3.foo4.header.key2", "value2")
    val keyPrefix = "foo1.foo2.foo3.foo4.header"
    val propertiesMap: Map[String, String] = OpenTelemetryPushSink invokePrivate
      PrivateMethod[Map[String, String]](Symbol("fetchMapFromProperties"))(
        properties,
        keyPrefix
      )

    assert("value1".equals(propertiesMap("key1.key2.key3")))
    assert("value2".equals(propertiesMap("key2")))
  }

  test("OpenTelemetry sink with one counter added") {
    val props = new Properties
    props.put("endpoint", "http://127.0.0.1:10086")
    val registry = new MetricRegistry
    val sink = new OpenTelemetryPushSink(props, registry)
    sink.start()
    val reporter = sink.reporter
    val counter = registry.counter("test-counter")
    assertEquals(reporter.openTelemetryCounters.size, 1)
  }
}
