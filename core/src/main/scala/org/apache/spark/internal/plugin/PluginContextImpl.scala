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

package org.apache.spark.internal.plugin

import java.util

import com.codahale.metrics.MetricRegistry

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.metrics.source.Source
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.util.RpcUtils

private class PluginContextImpl(
    pluginName: String,
    rpcEnv: RpcEnv,
    metricsSystem: MetricsSystem,
    override val conf: SparkConf,
    override val executorID: String,
    override val resources: util.Map[String, ResourceInformation])
  extends PluginContext with Logging {

  override def hostname(): String = rpcEnv.address.hostPort.split(":")(0)

  private val registry = new MetricRegistry()

  private lazy val driverEndpoint = try {
    RpcUtils.makeDriverRef(classOf[PluginEndpoint].getName(), conf, rpcEnv)
  } catch {
    case e: Exception =>
      logWarning(s"Failed to create driver plugin endpoint ref.", e)
      null
  }

  override def metricRegistry(): MetricRegistry = registry

  override def send(message: AnyRef): Unit = {
    if (driverEndpoint == null) {
      throw new IllegalStateException("Driver endpoint is not known.")
    }
    driverEndpoint.send(PluginMessage(pluginName, message))
  }

  override def ask(message: AnyRef): AnyRef = {
    try {
      if (driverEndpoint != null) {
        driverEndpoint.askSync[AnyRef](PluginMessage(pluginName, message))
      } else {
        throw new IllegalStateException("Driver endpoint is not known.")
      }
    } catch {
      case e: SparkException if e.getCause() != null =>
        throw e.getCause()
    }
  }

  def registerMetrics(): Unit = {
    if (!registry.getMetrics().isEmpty()) {
      val src = new PluginMetricsSource(s"plugin.$pluginName", registry)
      metricsSystem.registerSource(src)
    }
  }

  class PluginMetricsSource(
      override val sourceName: String,
      override val metricRegistry: MetricRegistry)
    extends Source

}
