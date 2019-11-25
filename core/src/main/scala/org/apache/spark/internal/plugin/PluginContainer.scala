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

import scala.collection.JavaConverters._
import scala.util.{Either, Left, Right}

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.api.plugin._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.shuffle.api._
import org.apache.spark.util.Utils

class DriverPluginContainer(sc: SparkContext) extends Logging {

  private val plugins: Seq[(String, DriverPlugin, PluginContextImpl)] =
    PluginContainer.loadPlugins(sc.conf).flatMap { p =>
      val driverPlugin = p.driverPlugin()
      if (driverPlugin != null) {
        val name = p match {
          case shuffle: ShuffleDataIOAdapter => shuffle.shufflePluginClass
          case _ => p.getClass().getName()
        }

        val ctx = new PluginContextImpl(name, sc.env.rpcEnv, sc.env.metricsSystem, sc.conf,
          sc.env.executorId)

        val extraConf = driverPlugin.init(sc, ctx)
        if (extraConf != null) {
          extraConf.asScala.foreach { case (k, v) =>
            sc.conf.set(s"${PluginContainer.EXTRA_CONF_PREFIX}$name.$k", v)
          }
        }
        logInfo(s"Initialized driver component for plugin $name.")
        Some((p.getClass().getName(), driverPlugin, ctx))
      } else {
        None
      }
    }

  private val _shufflePlugin: ShuffleDriverComponents = {
    plugins.find { case (_, plugin, _) => plugin.isInstanceOf[ShuffleDriverComponents] }
      .map { case (_, plugin, _) => plugin }
      .getOrElse(throw new IllegalStateException("Could not find shuffle driver component."))
      .asInstanceOf[ShuffleDriverComponents]
  }


  // Initialize the RPC endpoint only if at least one plugin needs it.
  {
    val endpoints = plugins.flatMap { case (name, plugin, _) =>
      val pluginEndpoint = plugin.rpcEndpoint()
      if (pluginEndpoint != null) {
        Some((name, pluginEndpoint))
      } else {
        None
      }
    }.toMap
    if (endpoints.nonEmpty) {
      sc.env.rpcEnv.setupEndpoint(classOf[PluginEndpoint].getName(),
        new PluginEndpoint(endpoints, sc.env.rpcEnv))
    }
  }

  def shufflePlugin: ShuffleDriverComponents = _shufflePlugin

  def registerMetrics(appId: String): Unit = {
    plugins.foreach { case (_, plugin, ctx) =>
      plugin.registerMetrics(appId, ctx)
      ctx.registerMetrics()
    }
  }

  def shutdown(): Unit = {
    plugins.foreach { case (name, plugin, _) =>
      try {
        logDebug(s"Stopping plugin $name.")
        plugin.shutdown()
      } catch {
        case t: Throwable =>
          logInfo(s"Exception while shutting down plugin $name.", t)
      }
    }
  }

}

class ExecutorPluginContainer(env: SparkEnv) extends Logging {

  private val plugins: Seq[(String, ExecutorPlugin)] = {
    val allExtraConf = env.conf.getAllWithPrefix(PluginContainer.EXTRA_CONF_PREFIX).toMap
    PluginContainer.loadPlugins(env.conf).flatMap { p =>
      val executorPlugin = p.executorPlugin()
      if (executorPlugin != null) {
        val name = p match {
          case shuffle: ShuffleDataIOAdapter => shuffle.shufflePluginClass
          case _ => p.getClass().getName()
        }
        val prefix = name + "."
        val extraConf = allExtraConf
          .filter { case (k, v) => k.startsWith(prefix) }
          .map { case (k, v) => k.substring(prefix.length()) -> v }
          .toMap
          .asJava
        val ctx = new PluginContextImpl(name, env.rpcEnv, env.metricsSystem, env.conf,
          env.executorId)
        executorPlugin.init(ctx, extraConf)
        ctx.registerMetrics()

        logInfo(s"Initialized executor component for plugin $name.")
        Some(p.getClass().getName() -> executorPlugin)
      } else {
        None
      }
    }
  }

  private val _shufflePlugin: ShuffleExecutorComponents = {
    plugins.find { case (_, plugin) => plugin.isInstanceOf[ShuffleExecutorComponents] }
      .map { case (_, plugin) => plugin }
      .getOrElse(throw new IllegalStateException("Could not find shuffle executor component."))
      .asInstanceOf[ShuffleExecutorComponents]
  }

  def shufflePlugin: ShuffleExecutorComponents = _shufflePlugin

  def shutdown(): Unit = {
    plugins.foreach { case (name, plugin) =>
      try {
        logDebug(s"Stopping plugin $name.")
        plugin.shutdown()
      } catch {
        case t: Throwable =>
          logInfo(s"Exception while shutting down plugin $name.", t)
      }
    }
  }
}

private class ShuffleDataIOAdapter(conf: SparkConf) extends SparkPlugin {

  val shufflePluginClass = conf.get(SHUFFLE_IO_PLUGIN_CLASS)

  override def driverPlugin(): DriverPlugin = {
    loadShuffleDataIO().driver()
  }

  override def executorPlugin(): ExecutorPlugin = {
    loadShuffleDataIO().executor()
  }

  private def loadShuffleDataIO(): ShuffleDataIO = {
    val maybeIO = Utils.loadExtensions(classOf[ShuffleDataIO], Seq(shufflePluginClass), conf)
    require(maybeIO.nonEmpty, s"A valid shuffle plugin must be specified by config " +
      s"${SHUFFLE_IO_PLUGIN_CLASS.key}, but $shufflePluginClass resulted in zero valid " +
      s"plugins.")
    maybeIO.head
  }

}

private object PluginContainer {

  val EXTRA_CONF_PREFIX = "spark.plugins.internal.conf."

  def loadPlugins(conf: SparkConf): Seq[SparkPlugin] = {
    val pluginClasses = conf.get(PLUGINS) ++ Seq(classOf[ShuffleDataIOAdapter].getName())
    Utils.loadExtensions(classOf[SparkPlugin], pluginClasses.distinct, conf)
  }
}
