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

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.deploy.k8s.Config.{EXECUTOR_ROLL_INTERVAL, EXECUTOR_ROLL_POLICY, ExecutorRollPolicy, MINIMUM_TASKS_PER_EXECUTOR_BEFORE_ROLLING}
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.{CLASS_NAME, CONFIG, INTERVAL}
import org.apache.spark.internal.config.DECOMMISSION_ENABLED
import org.apache.spark.scheduler.ExecutorDecommissionInfo
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MILLIS_PER_SECOND
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
  lazy val EMPTY_METRICS = new ExecutorMetrics(Array.emptyLongArray)

  private var sparkContext: SparkContext = _

  private val periodicService: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("executor-roller")

  private[k8s] var minTasks: Int = 0

  override def init(sc: SparkContext, ctx: PluginContext): JMap[String, String] = {
    val interval = sc.conf.get(EXECUTOR_ROLL_INTERVAL)
    if (interval <= 0) {
      logWarning(log"Disabled due to invalid interval value, " +
        log"'${MDC(INTERVAL, interval * MILLIS_PER_SECOND)}'")
    } else if (!sc.conf.get(DECOMMISSION_ENABLED)) {
      logWarning(log"Disabled because ${MDC(CONFIG, DECOMMISSION_ENABLED.key)} is false.")
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
                log"${MDC(CLASS_NAME, classOf[KubernetesClusterSchedulerBackend].getSimpleName)}.")
          }
        } catch {
          case e: Throwable => logError("Error in rolling thread", e)
        }
      }, interval, interval, TimeUnit.SECONDS)
    }
    Map.empty[String, String].asJava
  }

  override def shutdown(): Unit = periodicService.shutdown()

  private def getPeakMetrics(summary: v1.ExecutorSummary, name: String): Long =
    summary.peakMemoryMetrics.getOrElse(EMPTY_METRICS).getMetricValue(name)

  private def choose(list: Seq[v1.ExecutorSummary], policy: ExecutorRollPolicy.Value)
      : Option[String] = {
    val listWithoutDriver = list
      .filterNot(_.id.equals(SparkContext.DRIVER_IDENTIFIER))
      .filter(_.totalTasks >= minTasks)
    val sortedList = policy match {
      case ExecutorRollPolicy.ID =>
        // We can convert to integer because EXECUTOR_ID_COUNTER uses AtomicInteger.
        listWithoutDriver.sortBy(_.id.toInt)
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
      case ExecutorRollPolicy.PEAK_JVM_ONHEAP_MEMORY =>
        listWithoutDriver.sortBy(getPeakMetrics(_, "JVMHeapMemory")).reverse
      case ExecutorRollPolicy.PEAK_JVM_OFFHEAP_MEMORY =>
        listWithoutDriver.sortBy(getPeakMetrics(_, "JVMOffHeapMemory")).reverse
      case ExecutorRollPolicy.TOTAL_SHUFFLE_WRITE =>
        listWithoutDriver.sortBy(_.totalShuffleWrite).reverse
      case ExecutorRollPolicy.DISK_USED =>
        listWithoutDriver.sortBy(_.diskUsed).reverse
      case ExecutorRollPolicy.OUTLIER =>
        // If there is no outlier we fallback to TOTAL_DURATION policy.
        outliersFromMultipleDimensions(listWithoutDriver) ++
          listWithoutDriver.sortBy(_.totalDuration).reverse
      case ExecutorRollPolicy.OUTLIER_NO_FALLBACK =>
        outliersFromMultipleDimensions(listWithoutDriver)
    }
    sortedList.headOption.map(_.id)
  }

  /**
   * We build multiple outlier lists and concat in the following importance order to find
   * outliers in various perspective:
   *   AVERAGE_DURATION > TOTAL_DURATION > TOTAL_GC_TIME > FAILED_TASKS >
   *     PEAK_JVM_ONHEAP_MEMORY > PEAK_JVM_OFFHEAP_MEMORY > TOTAL_SHUFFLE_WRITE > DISK_USED
   * Since we will choose only first item, the duplication is okay.
   */
  private def outliersFromMultipleDimensions(listWithoutDriver: Seq[v1.ExecutorSummary]) =
    outliers(listWithoutDriver.filter(_.totalTasks > 0),
      e => (e.totalDuration / e.totalTasks).toFloat) ++
      outliers(listWithoutDriver, e => e.totalDuration.toFloat) ++
      outliers(listWithoutDriver, e => e.totalGCTime.toFloat) ++
      outliers(listWithoutDriver, e => e.failedTasks.toFloat) ++
      outliers(listWithoutDriver, e => getPeakMetrics(e, "JVMHeapMemory").toFloat) ++
      outliers(listWithoutDriver, e => getPeakMetrics(e, "JVMOffHeapMemory").toFloat) ++
      outliers(listWithoutDriver, e => e.totalShuffleWrite.toFloat) ++
      outliers(listWithoutDriver, e => e.diskUsed.toFloat)

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
