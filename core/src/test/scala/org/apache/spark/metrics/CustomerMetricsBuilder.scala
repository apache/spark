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

// This file is placed in different package to make sure all of these components work well
// when they are outside of org.apache.spark.
package other.metrics

import java.util.Properties

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.sink.Sink
import org.apache.spark.metrics.source.Source

class CustomMetricsSource extends Source {
  val sourceName = "fake"

  private val registry = new MetricRegistry

  registry.register(MetricRegistry.name("test1"), new Gauge[Int] {
    override def getValue: Int = 1
  })

  registry.register(MetricRegistry.name("test2"), new Gauge[Int] {
    override def getValue: Int = 2
  })

  override def metricRegistry: MetricRegistry = registry
}

class CustomMetricsSink(properties: Properties, metricRegistry: MetricRegistry)
    extends Sink(properties, metricRegistry) {

  private val prop1 = properties.getProperty("prop1")
  private val prop2 = properties.getProperty("prop2")

  assert(prop1 != null)
  assert(prop2 != null)
  assert(metricRegistry.getGauges.keySet().contains("fake.test1"))
  assert(metricRegistry.getGauges.keySet().contains("fake.test2"))

  def start(): Unit = { }

  def stop(): Unit = { }

  def report(): Unit = { }
}


