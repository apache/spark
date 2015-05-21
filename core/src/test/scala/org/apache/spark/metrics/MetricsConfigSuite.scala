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

package org.apache.spark.metrics

import org.scalatest.{BeforeAndAfter, FunSuite}

class MetricsConfigSuite extends FunSuite with BeforeAndAfter {
  var filePath: String = _

  before {
    filePath = getClass.getClassLoader.getResource("test_metrics_config.properties").getFile()
  }

  test("MetricsConfig with default properties") {
    val conf = new MetricsConfig(None)
    conf.initialize()

    assert(conf.properties.size() === 4)
    assert(conf.properties.getProperty("test-for-dummy") === null)

    val property = conf.getInstance("random")
    assert(property.size() === 2)
    assert(property.getProperty("sink.servlet.class") ===
      "org.apache.spark.metrics.sink.MetricsServlet")
    assert(property.getProperty("sink.servlet.path") === "/metrics/json")
  }

  test("MetricsConfig with properties set") {
    val conf = new MetricsConfig(Option(filePath))
    conf.initialize()

    val masterProp = conf.getInstance("master")
    assert(masterProp.size() === 5)
    assert(masterProp.getProperty("sink.console.period") === "20")
    assert(masterProp.getProperty("sink.console.unit") === "minutes")
    assert(masterProp.getProperty("source.jvm.class") ===
      "org.apache.spark.metrics.source.JvmSource")
    assert(masterProp.getProperty("sink.servlet.class") ===
      "org.apache.spark.metrics.sink.MetricsServlet")
    assert(masterProp.getProperty("sink.servlet.path") === "/metrics/master/json")

    val workerProp = conf.getInstance("worker")
    assert(workerProp.size() === 5)
    assert(workerProp.getProperty("sink.console.period") === "10")
    assert(workerProp.getProperty("sink.console.unit") === "seconds")
    assert(workerProp.getProperty("source.jvm.class") ===
      "org.apache.spark.metrics.source.JvmSource")
    assert(workerProp.getProperty("sink.servlet.class") ===
      "org.apache.spark.metrics.sink.MetricsServlet")
    assert(workerProp.getProperty("sink.servlet.path") === "/metrics/json")
  }

  test("MetricsConfig with subProperties") {
    val conf = new MetricsConfig(Option(filePath))
    conf.initialize()

    val propCategories = conf.propertyCategories
    assert(propCategories.size === 3)

    val masterProp = conf.getInstance("master")
    val sourceProps = conf.subProperties(masterProp, MetricsSystem.SOURCE_REGEX)
    assert(sourceProps.size === 1)
    assert(sourceProps("jvm").getProperty("class") === "org.apache.spark.metrics.source.JvmSource")

    val sinkProps = conf.subProperties(masterProp, MetricsSystem.SINK_REGEX)
    assert(sinkProps.size === 2)
    assert(sinkProps.contains("console"))
    assert(sinkProps.contains("servlet"))

    val consoleProps = sinkProps("console")
    assert(consoleProps.size() === 2)

    val servletProps = sinkProps("servlet")
    assert(servletProps.size() === 2)
  }
}
