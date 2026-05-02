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
import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService, TimeUnit}

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.{PersistentVolumeClaimBuilder, Pod, Quantity}
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.base.PatchContext
import io.fabric8.kubernetes.client.dsl.base.PatchType

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{CONFIG, CONFIG2, CURRENT_DISK_SIZE, ORIGINAL_DISK_SIZE, PVC_METADATA_NAME}
import org.apache.spark.util.ThreadUtils

/**
 * Spark plugin to monitor executor PVC disk usage and grow the PVC storage request
 * when the usage exceeds a configurable threshold.
 *
 * Executors measure their own local-directory usage (via DiskBlockManager) and report
 * the maximum filesystem usage ratio to the driver through the plugin RPC channel.
 * When the ratio exceeds the threshold, the driver patches every `spark-local-dir-*`
 * PVC mounted by the reporting executor's pod to grow its
 * `spec.resources.requests.storage`. The underlying StorageClass must have
 * `allowVolumeExpansion: true`.
 */
class ExecutorPVCResizePlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new ExecutorPVCResizeDriverPlugin()

  override def executorPlugin(): ExecutorPlugin = new ExecutorPVCResizeExecutorPlugin()
}

/**
 * Message sent from each executor to the driver with the maximum filesystem usage
 * ratio (used / total) across the executor's SPARK_LOCAL_DIRS. The driver applies
 * this ratio to every PVC mounted by the reporting executor's pod.
 */
private[k8s] case class PVCDiskUsageReport(
    executorId: String,
    ratio: Double)

class ExecutorPVCResizeDriverPlugin extends DriverPlugin with Logging {
  private var sparkContext: SparkContext = _
  private var namespace: String = _
  private var threshold: Double = _
  private var factor: Double = _

  private val latestReports = new ConcurrentHashMap[String, PVCDiskUsageReport]()
  private val failedPvcs = ConcurrentHashMap.newKeySet[String]()
  private val requestedSizes = new ConcurrentHashMap[String, Long]()

  private val periodicService: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("pvc-resize-plugin")

  override def init(sc: SparkContext, ctx: PluginContext): JMap[String, String] = {
    val allocator = sc.conf.get(KUBERNETES_ALLOCATION_PODS_ALLOCATOR)
    if (allocator != "direct") {
      logWarning(log"ExecutorPVCResizePlugin requires the 'direct' pods allocator; " +
        log"${MDC(CONFIG, KUBERNETES_ALLOCATION_PODS_ALLOCATOR.key)} is " +
        log"${MDC(CONFIG2, allocator)}. Plugin will not start.")
      return Map.empty[String, String].asJava
    }
    val interval = sc.conf.get(PVC_RESIZE_INTERVAL)
    if (interval <= 0) {
      logInfo("PVCResizePlugin disabled (interval <= 0).")
      return Map.empty[String, String].asJava
    }
    threshold = sc.conf.get(PVC_RESIZE_THRESHOLD)
    factor = sc.conf.get(PVC_RESIZE_FACTOR)
    namespace = sc.conf.get(KUBERNETES_NAMESPACE)
    sparkContext = sc

    periodicService.scheduleAtFixedRate(() => if (!sparkContext.isStopped) {
      try {
        checkAndResizePVCs()
      } catch {
        case e: Throwable => logError("Error in PVC resize thread", e)
      }
    }, interval, interval, TimeUnit.MINUTES)
    logInfo("ExecutorPVCResizeDriverPlugin is scheduled")

    // Propagate the interval to executors so they report at the same cadence.
    Map(PVC_RESIZE_INTERVAL.key -> interval.toString).asJava
  }

  override def receive(message: Any): AnyRef = message match {
    case r: PVCDiskUsageReport =>
      latestReports.put(r.executorId, r)
      null
    case _ =>
      null
  }

  override def shutdown(): Unit = {
    periodicService.shutdown()
  }

  private[k8s] def checkAndResizePVCs(): Unit = {
    logInfo(s"Latest PVC usage reports: $latestReports")
    val appId = sparkContext.applicationId

    sparkContext.schedulerBackend match {
      case b: KubernetesClusterSchedulerBackend =>
        val client = b.kubernetesClient
        val pods = client.pods()
          .inNamespace(namespace)
          .withLabel(SPARK_APP_ID_LABEL, appId)
          .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
          .list()
          .getItems.asScala

        val podByExecId = pods.flatMap { p =>
          Option(p.getMetadata.getLabels.get(SPARK_EXECUTOR_ID_LABEL)).map(_ -> p)
        }.toMap

        latestReports.values().asScala.foreach { report =>
          podByExecId.get(report.executorId).foreach { pod =>
            pvcsOf(pod).foreach { pvcName =>
              if (!failedPvcs.contains(pvcName)) {
                tryResize(client, pvcName, report.ratio, report.executorId)
              }
            }
          }
        }
      case _ =>
        logWarning("Skipping PVC resize: schedulerBackend is not " +
          "KubernetesClusterSchedulerBackend.")
    }
  }

  private[k8s] def pvcsOf(pod: Pod): Set[String] = {
    val volNameToPvc = pod.getSpec.getVolumes.asScala
      .filter(_.getPersistentVolumeClaim != null)
      .filter(_.getName.startsWith("spark-local-dir-"))
      .map(v => v.getName -> v.getPersistentVolumeClaim.getClaimName)
      .toMap
    pod.getSpec.getContainers.asScala
      .find(_.getName == DEFAULT_EXECUTOR_CONTAINER_NAME)
      .orElse(pod.getSpec.getContainers.asScala.headOption)
      .toSeq
      .flatMap(_.getVolumeMounts.asScala)
      .flatMap(m => volNameToPvc.get(m.getName))
      .toSet
  }

  private def tryResize(
      client: KubernetesClient, pvcName: String, ratio: Double, execId: String): Unit = {
    logInfo(s"Try to resize executor $execId PVC $pvcName with ratio $ratio " +
      s"(threshold $threshold).")
    if (ratio <= threshold) return
    try {
      val pvc = client.persistentVolumeClaims()
        .inNamespace(namespace)
        .withName(pvcName)
        .get()
      if (pvc == null) return
      val current = Quantity.getAmountInBytes(
        pvc.getSpec.getResources.getRequests.get("storage")).longValue()
      val capacity = Option(pvc.getStatus)
        .flatMap(s => Option(s.getCapacity))
        .flatMap(c => Option(c.get("storage")))
        .map(q => Quantity.getAmountInBytes(q).longValue())
        .getOrElse(current)
      if (current > capacity) {
        logInfo(s"PVC $pvcName resize is in progress or failed " +
          s"(spec=$current, status=$capacity); skip.")
        return
      }
      val newSize = (current * (1.0 + factor)).toLong
      if (requestedSizes.get(pvcName) == newSize) return
      logInfo(log"Increase PVC ${MDC(PVC_METADATA_NAME, pvcName)} storage " +
        log"from ${MDC(ORIGINAL_DISK_SIZE, current)} to " +
        log"${MDC(CURRENT_DISK_SIZE, newSize)} as usage ratio exceeded threshold.")
      val patch = new PersistentVolumeClaimBuilder()
        .withNewSpec()
          .withNewResources()
            .addToRequests("storage", new Quantity(newSize.toString))
          .endResources()
        .endSpec()
        .build()
      client.persistentVolumeClaims()
        .inNamespace(namespace)
        .withName(pvcName)
        .patch(PatchContext.of(PatchType.STRATEGIC_MERGE), patch)
      requestedSizes.put(pvcName, newSize)
    } catch {
      case e: Throwable =>
        failedPvcs.add(pvcName)
        logInfo(log"Failed to expand PVC ${MDC(PVC_METADATA_NAME, pvcName)}; " +
          log"will skip subsequent attempts.", e)
    }
  }
}

class ExecutorPVCResizeExecutorPlugin extends ExecutorPlugin with Logging {
  private var pluginContext: PluginContext = _
  private var periodicService: ScheduledExecutorService = _

  override def init(ctx: PluginContext, extraConf: JMap[String, String]): Unit = {
    val intervalStr = extraConf.get(PVC_RESIZE_INTERVAL.key)
    if (intervalStr == null) {
      // Driver disabled the plugin; do nothing.
      return
    }
    val interval = intervalStr.toLong
    if (interval <= 0) return

    pluginContext = ctx
    periodicService =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("pvc-resize-reporter")
    periodicService.scheduleAtFixedRate(() => {
      try {
        report()
      } catch {
        case e: Throwable => logDebug("Failed to report PVC usage", e)
      }
    }, interval, interval, TimeUnit.MINUTES)
  }

  override def shutdown(): Unit = {
    if (periodicService != null) periodicService.shutdown()
  }

  private def report(): Unit = {
    val env = SparkEnv.get
    if (env == null) return
    val dirs = env.blockManager.diskBlockManager.localDirs
    if (dirs == null || dirs.isEmpty) return
    val maxRatio = dirs.iterator.flatMap { d =>
      try {
        // Skip if total is 0 (e.g. dir unmounted, statvfs failed) to avoid divide-by-zero.
        val total = d.getTotalSpace
        if (total > 0) Some((total - d.getUsableSpace).toDouble / total) else None
      } catch { case _: Throwable => None }
    }.maxOption
    maxRatio.foreach { ratio =>
      logInfo(s"Reporting max PVC disk usage ratio for executor ${env.executorId}: $ratio")
      pluginContext.send(PVCDiskUsageReport(env.executorId, ratio))
    }
  }
}
