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
package org.apache.spark.scheduler.cluster.k8s

import java.util.{Map => JMap}
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.deploy.k8s.Config.EXECUTOR_ROLL_INTERVAL
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.DECOMMISSION_ENABLED
import org.apache.spark.scheduler.ExecutorDecommissionInfo
import org.apache.spark.util.ThreadUtils

/**
 * Spark plugin to roll executor pods periodically.
 * This is independent from ExecutorPodsAllocator and aims to decommission executors
 * one by one in both static and dynamic allocation.
 *
 * To use this plugin, we assume that a user has the required maximum number of executors + 1
 * in both static and dynamic allocation configurations.
 */
class ExecutorRollPlugin extends SparkPlugin with Logging {
  override def driverPlugin(): DriverPlugin = {
    new DriverPlugin() {
      private var sparkContext: SparkContext = _

      private val periodicService: ScheduledExecutorService =
        ThreadUtils.newDaemonSingleThreadScheduledExecutor("executor-roller")

      override def init(sc: SparkContext, ctx: PluginContext): JMap[String, String] = {
        val interval = sc.conf.get(EXECUTOR_ROLL_INTERVAL)
        if (interval <= 0) {
          logWarning(s"Disabled due to invalid interval value, '$interval'")
        } else if (!sc.conf.get(DECOMMISSION_ENABLED)) {
          logWarning(s"Disabled because ${DECOMMISSION_ENABLED.key} is false.")
        } else {
          // Scheduler is not created yet
          sparkContext = sc

          periodicService.scheduleAtFixedRate(() => {
            try {
              sparkContext.schedulerBackend match {
                case scheduler: KubernetesClusterSchedulerBackend =>
                  // Roughly assume that the smallest ID executor is the most long-lived one.
                  val smallestID = scheduler
                    .getExecutorIds()
                    .filterNot(_.equals(SparkContext.DRIVER_IDENTIFIER))
                    .map(_.toInt)
                    .sorted
                    .headOption
                  smallestID match {
                    case Some(id) =>
                      // Use decommission to be safe.
                      logInfo(s"Ask to decommission executor $id")
                      val now = System.currentTimeMillis()
                      scheduler.decommissionExecutor(
                        id.toString,
                        ExecutorDecommissionInfo(s"Rolling at $now"),
                        adjustTargetNumExecutors = false)
                    case _ =>
                      logInfo("There is nothing to roll.")
                  }
                case _ =>
                  logWarning("This plugin expects " +
                    s"${classOf[KubernetesClusterSchedulerBackend].getSimpleName}.")
              }
            } catch {
              case e: Throwable => logError("Error in rolling thread", e)
            }
          }, interval, interval, TimeUnit.SECONDS)
        }
        Map.empty[String, String].asJava
      }

      override def shutdown(): Unit = periodicService.shutdown()
    }
  }

  // No-op
  override def executorPlugin(): ExecutorPlugin = null
}
