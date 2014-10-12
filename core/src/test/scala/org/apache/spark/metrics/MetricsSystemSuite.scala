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

import org.scalatest.{BeforeAndAfter, FunSuite, PrivateMethodTester}

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.master.MasterSource
import org.apache.spark.metrics.source.Source

import com.codahale.metrics.MetricRegistry

import scala.collection.mutable.ArrayBuffer

class MetricsSystemSuite extends FunSuite with BeforeAndAfter with PrivateMethodTester{
  var filePath: String = _
  var conf: SparkConf = null
  var securityMgr: SecurityManager = null

  before {
    filePath = getClass.getClassLoader.getResource("test_metrics_system.properties").getFile
    conf = new SparkConf(false).set("spark.metrics.conf", filePath)
    securityMgr = new SecurityManager(conf)
  }

  test("MetricsSystem with default config") {
    val metricsSystem = MetricsSystem.createMetricsSystem("default", conf, securityMgr)
    metricsSystem.start()
    val sources = PrivateMethod[ArrayBuffer[Source]]('sources)
    val sinks = PrivateMethod[ArrayBuffer[Source]]('sinks)

    assert(metricsSystem.invokePrivate(sources()).length === 0)
    assert(metricsSystem.invokePrivate(sinks()).length === 0)
    assert(metricsSystem.getServletHandlers.nonEmpty)
  }

  test("MetricsSystem with sources add") {
    val metricsSystem = MetricsSystem.createMetricsSystem("test", conf, securityMgr)
    metricsSystem.start()
    val sources = PrivateMethod[ArrayBuffer[Source]]('sources)
    val sinks = PrivateMethod[ArrayBuffer[Source]]('sinks)

    assert(metricsSystem.invokePrivate(sources()).length === 0)
    assert(metricsSystem.invokePrivate(sinks()).length === 1)
    assert(metricsSystem.getServletHandlers.nonEmpty)

    val source = new MasterSource(null)
    metricsSystem.registerSource(source)
    assert(metricsSystem.invokePrivate(sources()).length === 1)
  }

  test("MetricsSystem with Driver instance") {
    val source = new Source {
      override val sourceName = "dummySource"
      override val metricRegistry = new MetricRegistry()
    }

    val appId = "testId"
    val executorId = "driver"
    conf.set("spark.app.id", appId)
    conf.set("spark.executor.id", executorId)

    val instanceName = "driver"
    val driverMetricsSystem = MetricsSystem.createMetricsSystem(instanceName, conf, securityMgr)

    val metricName = driverMetricsSystem.buildRegistryName(source)
    assert(metricName === s"$appId.$executorId.${source.sourceName}")
  }

  test("MetricsSystem with Driver instance and spark.app.id is not set") {
    val source = new Source {
      override val sourceName = "dummySource"
      override val metricRegistry = new MetricRegistry()
    }

    val executorId = "driver"
    conf.set("spark.executor.id", executorId)

    val instanceName = "driver"
    val driverMetricsSystem = MetricsSystem.createMetricsSystem(instanceName, conf, securityMgr)

    val metricName = driverMetricsSystem.buildRegistryName(source)
    assert(metricName === source.sourceName)
  }

  test("MetricsSystem with Driver instance and spark.executor.id is not set") {
    val source = new Source {
      override val sourceName = "dummySource"
      override val metricRegistry = new MetricRegistry()
    }

    val appId = "testId"
    conf.set("spark.app.id", appId)

    val instanceName = "driver"
    val driverMetricsSystem = MetricsSystem.createMetricsSystem(instanceName, conf, securityMgr)

    val metricName = driverMetricsSystem.buildRegistryName(source)
    assert(metricName === source.sourceName)
  }

  test("MetricsSystem with Executor instance") {
    val source = new Source {
      override val sourceName = "dummySource"
      override val metricRegistry = new MetricRegistry()
    }

    val appId = "testId"
    val executorId = "executor.1"
    conf.set("spark.app.id", appId)
    conf.set("spark.executor.id", executorId)

    val instanceName = "executor"
    val driverMetricsSystem = MetricsSystem.createMetricsSystem(instanceName, conf, securityMgr)

    val metricName = driverMetricsSystem.buildRegistryName(source)
    assert(metricName === s"$appId.$executorId.${source.sourceName}")
  }

  test("MetricsSystem with Executor instance and spark.app.id is not set") {
    val source = new Source {
      override val sourceName = "dummySource"
      override val metricRegistry = new MetricRegistry()
    }

    val executorId = "executor.1"
    conf.set("spark.executor.id", executorId)

    val instanceName = "executor"
    val driverMetricsSystem = MetricsSystem.createMetricsSystem(instanceName, conf, securityMgr)

    val metricName = driverMetricsSystem.buildRegistryName(source)
    assert(metricName === source.sourceName)
  }

  test("MetricsSystem with Executor instance and spark.executor.id is not set") {
    val source = new Source {
      override val sourceName = "dummySource"
      override val metricRegistry = new MetricRegistry()
    }

    val appId = "testId"
    conf.set("spark.app.id", appId)

    val instanceName = "executor"
    val driverMetricsSystem = MetricsSystem.createMetricsSystem(instanceName, conf, securityMgr)

    val metricName = driverMetricsSystem.buildRegistryName(source)
    assert(metricName === source.sourceName)
  }

  test("MetricsSystem with instance which is neither Driver nor Executor") {
    val source = new Source {
      override val sourceName = "dummySource"
      override val metricRegistry = new MetricRegistry()
    }

    val appId = "testId"
    val executorId = "dummyExecutorId"
    conf.set("spark.app.id", appId)
    conf.set("spark.executor.id", executorId)

    val instanceName = "testInstance"
    val driverMetricsSystem = MetricsSystem.createMetricsSystem(instanceName, conf, securityMgr)

    val metricName = driverMetricsSystem.buildRegistryName(source)

    // Even if spark.app.id and spark.executor.id are set, they are not used for the metric name.
    assert(metricName != s"$appId.$executorId.${source.sourceName}")
    assert(metricName === source.sourceName)
  }
}
