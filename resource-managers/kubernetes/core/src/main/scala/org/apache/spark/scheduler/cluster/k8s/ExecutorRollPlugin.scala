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

import java.lang.Math.sqrt
import java.util.{Map => JMap}
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.deploy.k8s.Config.{EXECUTOR_ROLL_INTERVAL, EXECUTOR_ROLL_POLICY, ExecutorRollPolicy, MINIMUM_TASKS_PER_EXECUTOR_BEFORE_ROLLING}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.DECOMMISSION_ENABLED
import org.apache.spark.scheduler.ExecutorDecommissionInfo
import org.apache.spark.status.api.v1
import org.apache.spark.util.ThreadUtils

/**
 * Spark plugin to roll executor pods periodically.
 * This is independent from ExecutorPodsAllocator and aims to decommission executors
 * one by one in both static and dynamic allocation.
 *
 * To use this plugin, we assume that a user has the required maximum number of executors + 1
 * in both static and dynamic allocation configurations.
 */
class ExecutorRollPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new ExecutorRollDriverPlugin()

  // No-op
  override def executorPlugin(): ExecutorPlugin = null
}

class ExecutorRollDriverPlugin extends DriverPlugin with Logging {
  private var sparkContext: SparkContext = _

  private val periodicService: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("executor-roller")

  private[k8s] var minTasks: Int = 0

  override def init(sc: SparkContext, ctx: PluginContext): JMap[String, String] = {
    val interval = sc.conf.get(EXECUTOR_ROLL_INTERVAL)
    if (interval <= 0) {
      logWarning(s"Disabled due to invalid interval value, '$interval'")
    } else if (!sc.conf.get(DECOMMISSION_ENABLED)) {
      logWarning(s"Disabled because ${DECOMMISSION_ENABLED.key} is false.")
    } else {
      minTasks = sc.conf.get(MINIMUM_TASKS_PER_EXECUTOR_BEFORE_ROLLING)
      // Scheduler is not created yet
      sparkContext = sc

      val policy = ExecutorRollPolicy.withName(sc.conf.get(EXECUTOR_ROLL_POLICY))
      periodicService.scheduleAtFixedRate(() => {
        try {
          sparkContext.schedulerBackend match {
            case scheduler: KubernetesClusterSchedulerBackend =>
              val executorSummaryList = sparkContext
                .statusStore
                .executorList(true)
              choose(executorSummaryList, policy) match {
                case Some(id) =>
                  // Use decommission to be safe.
                  logInfo(s"Ask to decommission executor $id")
                  val now = System.currentTimeMillis()
                  scheduler.decommissionExecutor(
                    id,
                    ExecutorDecommissionInfo(s"Rolling via $policy at $now"),
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

  private def choose(list: Seq[v1.ExecutorSummary], policy: ExecutorRollPolicy.Value)
      : Option[String] = {
    val listWithoutDriver = list
      .filterNot(_.id.equals(SparkContext.DRIVER_IDENTIFIER))
      .filter(_.totalTasks >= minTasks)
    val sortedList = policy match {
      case ExecutorRollPolicy.ID =>
        listWithoutDriver.sortBy(_.id)
      case ExecutorRollPolicy.ADD_TIME =>
        listWithoutDriver.sortBy(_.addTime)
      case ExecutorRollPolicy.TOTAL_GC_TIME =>
        listWithoutDriver.sortBy(_.totalGCTime).reverse
      case ExecutorRollPolicy.TOTAL_DURATION =>
        listWithoutDriver.sortBy(_.totalDuration).reverse
      case ExecutorRollPolicy.AVERAGE_DURATION =>
        listWithoutDriver.sortBy(e => e.totalDuration.toFloat / Math.max(1, e.totalTasks)).reverse
      case ExecutorRollPolicy.FAILED_TASKS =>
        listWithoutDriver.sortBy(_.failedTasks).reverse
      case ExecutorRollPolicy.OUTLIER =>
        // We build multiple outlier lists and concat in the following importance order to find
        // outliers in various perspective:
        //   AVERAGE_DURATION > TOTAL_DURATION > TOTAL_GC_TIME > FAILED_TASKS
        // Since we will choose only first item, the duplication is okay. If there is no outlier,
        // We fallback to TOTAL_DURATION policy.
        outliers(listWithoutDriver.filter(_.totalTasks > 0), e => e.totalDuration / e.totalTasks) ++
          outliers(listWithoutDriver, e => e.totalDuration) ++
          outliers(listWithoutDriver, e => e.totalGCTime) ++
          outliers(listWithoutDriver, e => e.failedTasks) ++
          listWithoutDriver.sortBy(_.totalDuration).reverse
    }
    sortedList.headOption.map(_.id)
  }

  /**
   * Return executors whose metrics is outstanding, '(value - mean) > 2-sigma'. This is
   * a best-effort approach because the snapshot of ExecutorSummary is not a normal distribution.
   * Outliers can be defined in several ways (https://en.wikipedia.org/wiki/Outlier).
   * Here, we borrowed 2-sigma idea from https://en.wikipedia.org/wiki/68-95-99.7_rule.
   * In case of normal distribution, this is known to be 2.5 percent roughly.
   */
  private def outliers(
      list: Seq[v1.ExecutorSummary],
      get: v1.ExecutorSummary => Float): Seq[v1.ExecutorSummary] = {
    if (list.isEmpty) {
      list
    } else {
      val size = list.size
      val values = list.map(get)
      val mean = values.sum / size
      val sd = sqrt(values.map(v => (v - mean) * (v - mean)).sum / size)
      list
        .filter(e => (get(e) - mean) > 2 * sd)
        .sortBy(e => get(e))
        .reverse
    }
  }
}
