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
 * This message is used to report a state store has just finished uploading a snapshot,
 * along with the timestamp in milliseconds and the snapshot version.
 */
private case class ReportSnapshotUploaded(
    providerId: StateStoreProviderId,
    version: Long,
    timestamp: Long)
  extends StateStoreCoordinatorMessage

/**
 * This message is used for the coordinator to look for all state stores that are lagging behind
 * in snapshot uploads. The coordinator will then log a warning message for each lagging instance.
 */
private case class LogLaggingStateStores(
    queryRunId: UUID,
    latestVersion: Long,
    isTerminatingTrigger: Boolean)
  extends StateStoreCoordinatorMessage

/**
 * Message used for testing.
 * This message is used to retrieve the latest snapshot version reported for upload from a
 * specific state store.
 */
private case class GetLatestSnapshotVersionForTesting(providerId: StateStoreProviderId)
  extends StateStoreCoordinatorMessage

/**
 * Message used for testing.
 * This message is used to retrieve all active state store instances falling behind in
 * snapshot uploads, using version and time criteria.
 */
private case class GetLaggingStoresForTesting(
    queryRunId: UUID,
    latestVersion: Long,
    isTerminatingTrigger: Boolean)
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
      providerId: StateStoreProviderId,
      version: Long,
      timestamp: Long): Boolean = {
    rpcEndpointRef.askSync[Boolean](ReportSnapshotUploaded(providerId, version, timestamp))
  }

  /** Ask the coordinator to log all state store instances that are lagging behind in uploads */
  private[sql] def logLaggingStateStores(
      queryRunId: UUID,
      latestVersion: Long,
      isTerminatingTrigger: Boolean): Boolean = {
    rpcEndpointRef.askSync[Boolean](
      LogLaggingStateStores(queryRunId, latestVersion, isTerminatingTrigger))
  }

  /**
   * Endpoint used for testing.
   * Get the latest snapshot version uploaded for a state store.
   */
  private[state] def getLatestSnapshotVersionForTesting(
      providerId: StateStoreProviderId): Option[Long] = {
    rpcEndpointRef.askSync[Option[Long]](GetLatestSnapshotVersionForTesting(providerId))
  }

  /**
   * Endpoint used for testing.
   * Get the state store instances that are falling behind in snapshot uploads for a particular
   * query run.
   */
  private[state] def getLaggingStoresForTesting(
      queryRunId: UUID,
      latestVersion: Long,
      isTerminatingTrigger: Boolean = false): Seq[StateStoreProviderId] = {
    rpcEndpointRef.askSync[Seq[StateStoreProviderId]](
      GetLaggingStoresForTesting(queryRunId, latestVersion, isTerminatingTrigger)
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
  extends ThreadSafeRpcEndpoint with Logging {
  private val instances = new mutable.HashMap[StateStoreProviderId, ExecutorCacheTaskLocation]

  // Stores the latest snapshot upload event for a specific state store
  private val stateStoreLatestUploadedSnapshot =
    new mutable.HashMap[StateStoreProviderId, SnapshotUploadEvent]

  // Default snapshot upload event to use when a provider has never uploaded a snapshot
  private val defaultSnapshotUploadEvent = SnapshotUploadEvent(0, 0)

  // Stores the last timestamp in milliseconds for each queryRunId indicating when the
  // coordinator did a report on instances lagging behind on snapshot uploads.
  // The initial timestamp is defaulted to 0 milliseconds.
  private val lastFullSnapshotLagReportTimeMs = new mutable.HashMap[UUID, Long]

  private def shouldCoordinatorReportSnapshotLag: Boolean =
    sqlConf.stateStoreCoordinatorReportSnapshotUploadLag

  private def coordinatorLagReportInterval: Long =
    sqlConf.stateStoreCoordinatorSnapshotLagReportInterval

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
      // Also remove these instances from snapshot upload event tracking
      stateStoreLatestUploadedSnapshot --= storeIdsToRemove
      // Remove the corresponding run id entries for report time and starting time
      lastFullSnapshotLagReportTimeMs -= runId
      logDebug(s"Deactivating instances related to checkpoint location $runId: " +
        storeIdsToRemove.mkString(", "))
      context.reply(true)

    case ReportSnapshotUploaded(providerId, version, timestamp) =>
      // Ignore this upload event if the registered latest version for the store is more recent,
      // since it's possible that an older version gets uploaded after a new executor uploads for
      // the same state store but with a newer snapshot.
      logDebug(s"Snapshot version $version was uploaded for state store $providerId")
      if (!stateStoreLatestUploadedSnapshot.get(providerId).exists(_.version >= version)) {
        stateStoreLatestUploadedSnapshot.put(providerId, SnapshotUploadEvent(version, timestamp))
      }
      context.reply(true)

    case LogLaggingStateStores(queryRunId, latestVersion, isTerminatingTrigger) =>
      val currentTimestamp = System.currentTimeMillis()
      // Only log lagging instances if snapshot lag reporting and uploading is enabled,
      // otherwise all instances will be considered lagging.
      if (shouldCoordinatorReportSnapshotLag) {
        val laggingStores =
          findLaggingStores(queryRunId, latestVersion, currentTimestamp, isTerminatingTrigger)
        if (laggingStores.nonEmpty) {
          logWarning(
            log"StateStoreCoordinator Snapshot Lag Report for " +
            log"queryRunId=${MDC(LogKeys.QUERY_RUN_ID, queryRunId)} - " +
            log"Number of state stores falling behind: " +
            log"${MDC(LogKeys.NUM_LAGGING_STORES, laggingStores.size)}"
          )
          // Report all stores that are behind in snapshot uploads.
          // Only report the list of providers lagging behind if the last reported time
          // is not recent for this query run. The lag report interval denotes the minimum
          // time between these full reports.
          val timeSinceLastReport =
            currentTimestamp - lastFullSnapshotLagReportTimeMs.getOrElse(queryRunId, 0L)
          if (timeSinceLastReport > coordinatorLagReportInterval) {
            // Mark timestamp of the report and log the lagging instances
            lastFullSnapshotLagReportTimeMs.put(queryRunId, currentTimestamp)
            // Only report the stores that are lagging the most behind in snapshot uploads.
            laggingStores
              .sortBy(stateStoreLatestUploadedSnapshot.getOrElse(_, defaultSnapshotUploadEvent))
              .take(sqlConf.stateStoreCoordinatorMaxLaggingStoresToReport)
              .foreach { providerId =>
                val baseLogMessage =
                  log"StateStoreCoordinator Snapshot Lag Detected for " +
                  log"queryRunId=${MDC(LogKeys.QUERY_RUN_ID, queryRunId)} - " +
                  log"Store ID: ${MDC(LogKeys.STATE_STORE_ID, providerId.storeId)} " +
                  log"(Latest batch ID: ${MDC(LogKeys.BATCH_ID, latestVersion)}"

                val logMessage = stateStoreLatestUploadedSnapshot.get(providerId) match {
                  case Some(snapshotEvent) =>
                    val versionDelta = latestVersion - snapshotEvent.version
                    val timeDelta = currentTimestamp - snapshotEvent.timestamp

                    baseLogMessage + log", " +
                    log"latest snapshot: ${MDC(LogKeys.SNAPSHOT_EVENT, snapshotEvent)}, " +
                    log"version delta: " +
                    log"${MDC(LogKeys.SNAPSHOT_EVENT_VERSION_DELTA, versionDelta)}, " +
                    log"time delta: ${MDC(LogKeys.SNAPSHOT_EVENT_TIME_DELTA, timeDelta)}ms)"
                  case None =>
                    baseLogMessage + log", latest snapshot: no upload for query run)"
                }
                logWarning(logMessage)
              }
          }
        }
      }
      context.reply(true)

    case GetLatestSnapshotVersionForTesting(providerId) =>
      val version = stateStoreLatestUploadedSnapshot.get(providerId).map(_.version)
      logDebug(s"Got latest snapshot version of the state store $providerId: $version")
      context.reply(version)

    case GetLaggingStoresForTesting(queryRunId, latestVersion, isTerminatingTrigger) =>
      val currentTimestamp = System.currentTimeMillis()
      // Only report if snapshot lag reporting is enabled
      if (shouldCoordinatorReportSnapshotLag) {
        val laggingStores =
          findLaggingStores(queryRunId, latestVersion, currentTimestamp, isTerminatingTrigger)
        logDebug(s"Got lagging state stores: ${laggingStores.mkString(", ")}")
        context.reply(laggingStores)
      } else {
        context.reply(Seq.empty)
      }

    case StopCoordinator =>
      stop() // Stop before replying to ensure that endpoint name has been deregistered
      logInfo("StateStoreCoordinator stopped")
      context.reply(true)
  }

  private def findLaggingStores(
      queryRunId: UUID,
      referenceVersion: Long,
      referenceTimestamp: Long,
      isTerminatingTrigger: Boolean): Seq[StateStoreProviderId] = {
    // Determine alert thresholds from configurations for both time and version differences.
    val snapshotVersionDeltaMultiplier =
      sqlConf.stateStoreCoordinatorMultiplierForMinVersionDiffToLog
    val maintenanceIntervalMultiplier = sqlConf.stateStoreCoordinatorMultiplierForMinTimeDiffToLog
    val minDeltasForSnapshot = sqlConf.stateStoreMinDeltasForSnapshot
    val maintenanceInterval = sqlConf.streamingMaintenanceInterval

    // Use the configured multipliers multiplierForMinVersionDiffToLog and
    // multiplierForMinTimeDiffToLog to determine the proper alert thresholds.
    val minVersionDeltaForLogging = snapshotVersionDeltaMultiplier * minDeltasForSnapshot
    val minTimeDeltaForLogging = maintenanceIntervalMultiplier * maintenanceInterval

    // Look for active state store providers that are lagging behind in snapshot uploads.
    // The coordinator should only consider providers that are part of this specific query run.
    instances.view.keys
      .filter(_.queryRunId == queryRunId)
      .filter { storeProviderId =>
        // Stores that didn't upload a snapshot will be treated as a store with a snapshot of
        // version 0 and timestamp 0ms.
        val latestSnapshot = stateStoreLatestUploadedSnapshot.getOrElse(
          storeProviderId,
          defaultSnapshotUploadEvent
        )
        // Mark a state store as lagging if it's behind in both version and time.
        // A state store is considered lagging if it's behind in both version and time according
        // to the configured thresholds.
        val isBehindOnVersions =
          referenceVersion - latestSnapshot.version > minVersionDeltaForLogging
        val isBehindOnTime =
          referenceTimestamp - latestSnapshot.timestamp > minTimeDeltaForLogging
        // If the query is using a trigger that self-terminates like OneTimeTrigger
        // and AvailableNowTrigger, we ignore the time threshold check as the upload frequency
        // is not fully dependent on the maintenance interval.
        isBehindOnVersions && (isTerminatingTrigger || isBehindOnTime)
      }.toSeq
  }
}

case class SnapshotUploadEvent(
    version: Long,
    timestamp: Long
) extends Ordered[SnapshotUploadEvent] {

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
