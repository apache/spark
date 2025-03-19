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

package org.apache.spark.sql.execution.streaming.state

import java.util.UUID

import scala.collection.mutable

import org.apache.spark.SparkEnv
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.RpcUtils

/** Trait representing all messages to [[StateStoreCoordinator]] */
private sealed trait StateStoreCoordinatorMessage extends Serializable

/** Classes representing messages */

/**
 * This message is used to report active instance of a state store provider
 * to [[StateStoreCoordinator]]. This message also carries other loaded state
 * store providers on the same executor. [[StateStoreCoordinator]] will check
 * if these providers are inactive now. Inactive providers will be returned
 * back to the sender of the message for unloading.
 */
private case class ReportActiveInstance(
    storeId: StateStoreProviderId,
    host: String,
    executorId: String,
    providerIdsToCheck: Seq[StateStoreProviderId])
  extends StateStoreCoordinatorMessage

private case class VerifyIfInstanceActive(storeId: StateStoreProviderId, executorId: String)
  extends StateStoreCoordinatorMessage

private case class GetLocation(storeId: StateStoreProviderId)
  extends StateStoreCoordinatorMessage

private case class DeactivateInstances(runId: UUID)
  extends StateStoreCoordinatorMessage

/**
 * This message is used to report a state store instance has just finished uploading a snapshot,
 * along with the timestamp in milliseconds and the snapshot version.
 */
private case class ReportSnapshotUploaded(
    storeId: StateStoreProviderId,
    version: Long,
    timestamp: Long)
  extends StateStoreCoordinatorMessage

/**
 * This message is used for the coordinator to look for all state stores that are lagging behind
 * in snapshot uploads. The coordinator will then log a warning message for each lagging instance.
 */
private case class ConstructLaggingInstanceReport(
    queryRunId: UUID,
    latestVersion: Long,
    timestamp: Long)
  extends StateStoreCoordinatorMessage

/**
 * Message used for testing.
 * This message is used to retrieve the latest snapshot version reported for upload from a
 * specific state store instance.
 */
private case class GetLatestSnapshotVersionForTesting(storeId: StateStoreProviderId)
  extends StateStoreCoordinatorMessage

/**
 * Message used for testing.
 * This message is used to retrieve the all active state store instance falling behind in
 * snapshot uploads, using version and time criterias.
 */
private case class GetLaggingStoresForTesting(
    queryRunId: UUID,
    latestVersion: Long,
    timestamp: Long)
  extends StateStoreCoordinatorMessage

private object StopCoordinator
  extends StateStoreCoordinatorMessage

/** Helper object used to create reference to [[StateStoreCoordinator]]. */
object StateStoreCoordinatorRef extends Logging {

  private val endpointName = "StateStoreCoordinator"

  /**
   * Create a reference to a [[StateStoreCoordinator]]
   */
  def forDriver(env: SparkEnv, sqlConf: SQLConf): StateStoreCoordinatorRef = synchronized {
    try {
      val coordinator = new StateStoreCoordinator(env.rpcEnv, sqlConf)
      val coordinatorRef = env.rpcEnv.setupEndpoint(endpointName, coordinator)
      logInfo("Registered StateStoreCoordinator endpoint")
      new StateStoreCoordinatorRef(coordinatorRef)
    } catch {
      case e: IllegalArgumentException =>
        val rpcEndpointRef = RpcUtils.makeDriverRef(endpointName, env.conf, env.rpcEnv)
        logDebug("Retrieved existing StateStoreCoordinator endpoint")
        new StateStoreCoordinatorRef(rpcEndpointRef)
    }
  }

  def forExecutor(env: SparkEnv): StateStoreCoordinatorRef = synchronized {
    val rpcEndpointRef = RpcUtils.makeDriverRef(endpointName, env.conf, env.rpcEnv)
    logDebug("Retrieved existing StateStoreCoordinator endpoint")
    new StateStoreCoordinatorRef(rpcEndpointRef)
  }
}

/**
 * Reference to a [[StateStoreCoordinator]] that can be used to coordinate instances of
 * [[StateStore]]s across all the executors, and get their locations for job scheduling.
 */
class StateStoreCoordinatorRef private(rpcEndpointRef: RpcEndpointRef) {

  private[sql] def reportActiveInstance(
      stateStoreProviderId: StateStoreProviderId,
      host: String,
      executorId: String,
      otherProviderIds: Seq[StateStoreProviderId]): Seq[StateStoreProviderId] = {
    rpcEndpointRef.askSync[Seq[StateStoreProviderId]](
      ReportActiveInstance(stateStoreProviderId, host, executorId, otherProviderIds))
  }

  /** Verify whether the given executor has the active instance of a state store */
  private[sql] def verifyIfInstanceActive(
      stateStoreProviderId: StateStoreProviderId,
      executorId: String): Boolean = {
    rpcEndpointRef.askSync[Boolean](VerifyIfInstanceActive(stateStoreProviderId, executorId))
  }

  /** Get the location of the state store */
  private[sql] def getLocation(stateStoreProviderId: StateStoreProviderId): Option[String] = {
    rpcEndpointRef.askSync[Option[String]](GetLocation(stateStoreProviderId))
  }

  /** Deactivate instances related to a query */
  private[sql] def deactivateInstances(runId: UUID): Unit = {
    rpcEndpointRef.askSync[Boolean](DeactivateInstances(runId))
  }

  /** Inform that an executor has uploaded a snapshot */
  private[sql] def snapshotUploaded(
      storeProviderId: StateStoreProviderId,
      version: Long,
      timestamp: Long): Unit = {
    rpcEndpointRef.askSync[Boolean](ReportSnapshotUploaded(storeProviderId, version, timestamp))
  }

  /** Ask the coordinator to find all state store instances that are lagging behind in uploads */
  private[sql] def constructLaggingInstanceReport(
      queryRunId: UUID,
      latestVersion: Long,
      timestamp: Long): Unit = {
    rpcEndpointRef.askSync[Boolean](
      ConstructLaggingInstanceReport(queryRunId, latestVersion, timestamp)
    )
  }

  /**
   * Endpoint used for testing.
   * Get the latest snapshot version uploaded for a state store.
   */
  private[state] def getLatestSnapshotVersionForTesting(
      stateStoreProviderId: StateStoreProviderId): Option[Long] = {
    rpcEndpointRef.askSync[Option[Long]](GetLatestSnapshotVersionForTesting(stateStoreProviderId))
  }

  /**
   * Endpoint used for testing.
   * Get the state store instances that are falling behind in snapshot uploads for a particular
   * query run.
   */
  private[state] def getLaggingStoresForTesting(
      queryRunId: UUID,
      latestVersion: Long,
      timestamp: Long): Seq[StateStoreProviderId] = {
    rpcEndpointRef.askSync[Seq[StateStoreProviderId]](
      GetLaggingStoresForTesting(queryRunId, latestVersion, timestamp)
    )
  }

  private[state] def stop(): Unit = {
    rpcEndpointRef.askSync[Boolean](StopCoordinator)
  }
}


/**
 * Class for coordinating instances of [[StateStore]]s loaded in executors across the cluster,
 * and get their locations for job scheduling.
 */
private class StateStoreCoordinator(
    override val rpcEnv: RpcEnv,
    val sqlConf: SQLConf)
  extends ThreadSafeRpcEndpoint
  with Logging {
  private val instances = new mutable.HashMap[StateStoreProviderId, ExecutorCacheTaskLocation]

  // Stores the latest snapshot upload event for a specific state store provider instance
  private val stateStoreLatestUploadedSnapshot =
    new mutable.HashMap[StateStoreProviderId, SnapshotUploadEvent]

  // Default snapshot upload event to use when a provider has never uploaded a snapshot
  private val defaultSnapshotUploadEvent = SnapshotUploadEvent(-1, 0)

  // Stores the last timestamp in milliseconds where the coordinator did a full report on
  // instances lagging behind on snapshot uploads. The initial timestamp is defaulted to
  // 0 milliseconds.
  private var lastFullSnapshotLagReport = 0L

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case ReportActiveInstance(id, host, executorId, providerIdsToCheck) =>
      logDebug(s"Reported state store $id is active at $executorId")
      val taskLocation = ExecutorCacheTaskLocation(host, executorId)
      instances.put(id, taskLocation)

      // Check if any loaded provider id is already loaded in other executor.
      val providerIdsToUnload = providerIdsToCheck.filter { providerId =>
        val providerLoc = instances.get(providerId)
        // This provider is is already loaded in other executor. Marked it to unload.
        providerLoc.map(_ != taskLocation).getOrElse(false)
      }
      context.reply(providerIdsToUnload)

    case VerifyIfInstanceActive(id, execId) =>
      val response = instances.get(id) match {
        case Some(location) => location.executorId == execId
        case None => false
      }
      logDebug(s"Verified that state store $id is active: $response")
      context.reply(response)

    case GetLocation(id) =>
      val executorId = instances.get(id).map(_.toString)
      logDebug(s"Got location of the state store $id: $executorId")
      context.reply(executorId)

    case DeactivateInstances(runId) =>
      val storeIdsToRemove =
        instances.keys.filter(_.queryRunId == runId).toSeq
      instances --= storeIdsToRemove
      logDebug(s"Deactivating instances related to checkpoint location $runId: " +
        storeIdsToRemove.mkString(", "))
      context.reply(true)

    case ReportSnapshotUploaded(providerId, version, timestamp) =>
      // Ignore this upload event if the registered latest version for the provider is more recent,
      // since it's possible that an older version gets uploaded after a new executor uploads for
      // the same provider but with a newer snapshot.
      logDebug(s"Snapshot version $version was uploaded for provider $providerId")
      if (!stateStoreLatestUploadedSnapshot.get(providerId).exists(_.version >= version)) {
        stateStoreLatestUploadedSnapshot.put(providerId, SnapshotUploadEvent(version, timestamp))
      }
      context.reply(true)

    case ConstructLaggingInstanceReport(queryRunId, latestVersion, endOfBatchTimestamp) =>
      // Only log lagging instances if the snapshot report upload is enabled,
      // otherwise all instances will be considered lagging.
      if (sqlConf.getConf(SQLConf.STATE_STORE_COORDINATOR_REPORT_UPLOAD_ENABLED)) {
        val laggingStores = findLaggingStores(queryRunId, latestVersion, endOfBatchTimestamp)
        logWarning(
          log"StateStoreCoordinator Snapshot Lag Report for " +
          log"queryRunId=${MDC(LogKeys.QUERY_RUN_ID, queryRunId)} - " +
          log"Number of state stores falling behind: " +
          log"${MDC(LogKeys.NUM_LAGGING_STORES, laggingStores.size)}"
        )
        // Report all stores that are behind in snapshot uploads.
        // Only report the full list of providers lagging behind if the last reported time
        // is not recent. The lag report interval denotes the minimum time between these
        // full reports.
        val coordinatorLagReportInterval =
          sqlConf.getConf(SQLConf.STATE_STORE_COORDINATOR_SNAPSHOT_LAG_REPORT_INTERVAL)
        val currentTimestamp = System.currentTimeMillis()
        if (laggingStores.nonEmpty &&
          currentTimestamp - lastFullSnapshotLagReport > coordinatorLagReportInterval) {
          // Mark timestamp of the full report and log the lagging instances
          lastFullSnapshotLagReport = currentTimestamp
          laggingStores.foreach { providerId =>
            val logMessage = stateStoreLatestUploadedSnapshot.get(providerId) match {
              case Some(snapshotEvent) =>
                val versionDelta = latestVersion - snapshotEvent.version
                val timeDelta = endOfBatchTimestamp - snapshotEvent.timestamp

                log"StateStoreCoordinator Snapshot Lag Detected for " +
                log"queryRunId=${MDC(LogKeys.QUERY_RUN_ID, queryRunId)} - " +
                log"Provider: ${MDC(LogKeys.STATE_STORE_PROVIDER_ID, providerId)} " +
                log"(Latest batch ID: ${MDC(LogKeys.BATCH_ID, latestVersion)}, " +
                log"latest snapshot: ${MDC(LogKeys.SNAPSHOT_EVENT, snapshotEvent)}, " +
                log"version delta: ${MDC(LogKeys.SNAPSHOT_EVENT_VERSION_DELTA, versionDelta)}, " +
                log"time delta: ${MDC(LogKeys.SNAPSHOT_EVENT_TIME_DELTA, timeDelta)}ms)"
              case None =>
                log"StateStoreCoordinator Snapshot Lag Detected for " +
                log"queryRunId=${MDC(LogKeys.QUERY_RUN_ID, queryRunId)} - " +
                log"Provider: ${MDC(LogKeys.STATE_STORE_PROVIDER_ID, providerId)} " +
                log"(Latest batch ID: ${MDC(LogKeys.BATCH_ID, latestVersion)}, " +
                log"latest snapshot: never uploaded)"
            }
            logWarning(logMessage)
          }
        } else if (laggingStores.nonEmpty) {
          logInfo(log"StateStoreCoordinator Snapshot Lag Report - last full report was too recent")
        }
      }
      context.reply(true)

    case GetLatestSnapshotVersionForTesting(providerId) =>
      val version = stateStoreLatestUploadedSnapshot.get(providerId).map(_.version)
      logDebug(s"Got latest snapshot version of the state store $providerId: $version")
      context.reply(version)

    case GetLaggingStoresForTesting(queryRunId, latestVersion, timestamp) =>
      val laggingStores = findLaggingStores(queryRunId, latestVersion, timestamp)
      logDebug(s"Got lagging state stores: ${laggingStores.mkString(", ")}")
      context.reply(laggingStores)

    case StopCoordinator =>
      stop() // Stop before replying to ensure that endpoint name has been deregistered
      logInfo("StateStoreCoordinator stopped")
      context.reply(true)
  }

  case class SnapshotUploadEvent(
      version: Long,
      timestamp: Long
  ) extends Ordered[SnapshotUploadEvent] {

    def isLagging(latestVersion: Long, latestTimestamp: Long): Boolean = {
      val versionDelta = latestVersion - version
      val timeDelta = latestTimestamp - timestamp

      // Determine alert thresholds from configurations for both time and version differences.
      val snapshotVersionDeltaMultiplier = sqlConf.getConf(
        SQLConf.STATE_STORE_COORDINATOR_SNAPSHOT_DELTA_MULTIPLIER_FOR_MIN_VERSION_DELTA_TO_LOG)
      val maintenanceIntervalMultiplier = sqlConf.getConf(
        SQLConf.STATE_STORE_COORDINATOR_MAINTENANCE_MULTIPLIER_FOR_MIN_TIME_DELTA_TO_LOG)
      val minDeltasForSnapshot = sqlConf.getConf(SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT)
      val maintenanceInterval = sqlConf.getConf(SQLConf.STREAMING_MAINTENANCE_INTERVAL)

      // Use the configured multipliers to determine the proper alert thresholds
      val minVersionDeltaForLogging = snapshotVersionDeltaMultiplier * minDeltasForSnapshot
      val minTimeDeltaForLogging = maintenanceIntervalMultiplier * maintenanceInterval

      // Mark a state store as lagging if it is behind in both version and time.
      // In the case that a snapshot was never uploaded, we treat version -1 as the preceding
      // version of 0, and only rely on the version delta condition.
      // Time requirement will be automatically satisfied as the initial timestamp is 0.
      versionDelta >= minVersionDeltaForLogging && timeDelta > minTimeDeltaForLogging
    }

    override def compare(otherEvent: SnapshotUploadEvent): Int = {
      // Compare by version first, then by timestamp as tiebreaker
      val versionCompare = this.version.compare(otherEvent.version)
      if (versionCompare == 0) {
        this.timestamp.compare(otherEvent.timestamp)
      } else {
        versionCompare
      }
    }

    override def toString(): String = {
      s"SnapshotUploadEvent(version=$version, timestamp=$timestamp)"
    }
  }

  private def findLaggingStores(
      queryRunId: UUID,
      referenceVersion: Long,
      referenceTimestamp: Long): Seq[StateStoreProviderId] = {
    // Do not report any instance as lagging if the snapshot report upload is disabled,
    // since it will treat all active instances as stores that have never uploaded.
    if (!sqlConf.getConf(SQLConf.STATE_STORE_COORDINATOR_REPORT_UPLOAD_ENABLED)) {
      return Seq.empty
    }
    // Look for instances that are lagging behind in snapshot uploads
    instances.keys.filter { storeProviderId =>
      // Only consider instances that are part of this specific query run
      storeProviderId.queryRunId == queryRunId &&
        stateStoreLatestUploadedSnapshot
          .getOrElse(storeProviderId, defaultSnapshotUploadEvent)
          .isLagging(referenceVersion, referenceTimestamp)
    }.toSeq
  }
}
