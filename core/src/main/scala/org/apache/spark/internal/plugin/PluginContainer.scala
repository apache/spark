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

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.api.plugin._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils

sealed abstract class PluginContainer {

  def shutdown(): Unit

}

private class DriverPluginContainer(sc: SparkContext, plugins: Seq[SparkPlugin])
  extends PluginContainer with Logging {

  private val driverPlugins: Seq[(String, DriverPlugin)] = plugins.flatMap { p =>
    val driverPlugin = p.driverPlugin()
    if (driverPlugin != null) {
      val name = p.getClass().getName()
      val ctx = new PluginContextImpl(name, sc.env.rpcEnv, sc.env.metricsSystem, sc.conf,
        sc.env.executorId)

      val extraConf = driverPlugin.init(sc, ctx)
      if (extraConf != null) {
        extraConf.asScala.foreach { case (k, v) =>
          sc.conf.set(s"${PluginContainer.EXTRA_CONF_PREFIX}$name.$k", v)
        }
      }
      ctx.registerMetrics()
      logInfo(s"Initialized driver component for plugin $name.")
      Some(p.getClass().getName() -> driverPlugin)
    } else {
      None
    }
  }

  if (driverPlugins.nonEmpty) {
    sc.env.rpcEnv.setupEndpoint(classOf[PluginEndpoint].getName(),
      new PluginEndpoint(driverPlugins.toMap, sc.env.rpcEnv))
  }

  override def shutdown(): Unit = {
    driverPlugins.foreach { case (name, plugin) =>
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

private class ExecutorPluginContainer(env: SparkEnv, plugins: Seq[SparkPlugin])
  extends PluginContainer with Logging {

  private val executorPlugins: Seq[(String, ExecutorPlugin)] = {
    val allExtraConf = env.conf.getAllWithPrefix(PluginContainer.EXTRA_CONF_PREFIX)

    plugins.flatMap { p =>
      val executorPlugin = p.executorPlugin()
      if (executorPlugin != null) {
        val name = p.getClass().getName()
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

  override def shutdown(): Unit = {
    executorPlugins.foreach { case (name, plugin) =>
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

object PluginContainer {

  val EXTRA_CONF_PREFIX = "spark.plugins.internal."

  def apply(sc: SparkContext): Option[PluginContainer] = PluginContainer(Left(sc))

  def apply(env: SparkEnv): Option[PluginContainer] = PluginContainer(Right(env))

  private def apply(ctx: Either[SparkContext, SparkEnv]): Option[PluginContainer] = {
    val conf = ctx.fold(_.conf, _.conf)
    val plugins = Utils.loadExtensions(classOf[SparkPlugin], conf.get(PLUGINS).distinct, conf)
    if (plugins.nonEmpty) {
      ctx match {
        case Left(sc) => Some(new DriverPluginContainer(sc, plugins))
        case Right(env) => Some(new ExecutorPluginContainer(env, plugins))
      }
    } else {
      None
    }
  }
}
