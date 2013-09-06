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
import org.apache.spark.deploy.master.MasterSource

class MetricsSystemSuite extends FunSuite with BeforeAndAfter {
  var filePath: String = _

  before {
    filePath = getClass.getClassLoader.getResource("test_metrics_system.properties").getFile()
    System.setProperty("spark.metrics.conf", filePath)
  }

  test("MetricsSystem with default config") {
    val metricsSystem = MetricsSystem.createMetricsSystem("default")
    val sources = metricsSystem.sources
    val sinks = metricsSystem.sinks

    assert(sources.length === 0)
    assert(sinks.length === 0)
    assert(!metricsSystem.getServletHandlers.isEmpty)
  }

  test("MetricsSystem with sources add") {
    val metricsSystem = MetricsSystem.createMetricsSystem("test")
    val sources = metricsSystem.sources
    val sinks = metricsSystem.sinks

    assert(sources.length === 0)
    assert(sinks.length === 1)
    assert(!metricsSystem.getServletHandlers.isEmpty)

    val source = new MasterSource(null)
    metricsSystem.registerSource(source)
    assert(sources.length === 1)
  }
}
