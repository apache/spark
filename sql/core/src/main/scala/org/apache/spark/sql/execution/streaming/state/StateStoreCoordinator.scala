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
import org.apache.spark.internal.Logging
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

private case class SnapshotUploaded(storeId: StateStoreProviderId, version: Long, timestamp: Long)
  extends StateStoreCoordinatorMessage

private case class GetLatestSnapshotVersion(storeId: StateStoreProviderId)
  extends StateStoreCoordinatorMessage

private case class GetLaggingStores()
  extends StateStoreCoordinatorMessage

private object StopCoordinator
  extends StateStoreCoordinatorMessage

/** Helper object used to create reference to [[StateStoreCoordinator]]. */
object StateStoreCoordinatorRef extends Logging {

  private val endpointName = "StateStoreCoordinator"

  /**
   * Create a reference to a [[StateStoreCoordinator]]
   */
  def forDriver(env: SparkEnv, conf: SQLConf): StateStoreCoordinatorRef = synchronized {
    try {
      val coordinator = new StateStoreCoordinator(env.rpcEnv, conf)
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
    rpcEndpointRef.askSync[Boolean](SnapshotUploaded(storeProviderId, version, timestamp))
  }

  /** Get the latest snapshot version uploaded for a state store */
  private[sql] def getLatestSnapshotVersion(
      stateStoreProviderId: StateStoreProviderId): Option[Long] = {
    rpcEndpointRef.askSync[Option[Long]](GetLatestSnapshotVersion(stateStoreProviderId))
  }

  /** Get the state store instances that are falling behind in snapshot uploads */
  private[sql] def getLaggingStores(): Seq[StateStoreProviderId] = {
    rpcEndpointRef.askSync[Seq[StateStoreProviderId]](GetLaggingStores)
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

  // Stores the latest snapshot version of a specific state store provider instance
  private val stateStoreSnapshotVersions =
    new mutable.HashMap[StateStoreProviderId, SnapshotUploadEvent]

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

    case SnapshotUploaded(providerId, version, timestamp) =>
      stateStoreSnapshotVersions.put(providerId, SnapshotUploadEvent(version, timestamp))
      logDebug(s"Snapshot uploaded at ${providerId} with version ${version}")
      // Report all stores that are behind in snapshot uploads
      val (laggingStores, latestSnapshot) = findLaggingStores()
      if (laggingStores.nonEmpty) {
        logWarning(s"Number of state stores falling behind: ${laggingStores.size}")
        laggingStores.foreach { storeProviderId =>
          val snapshotEvent =
            stateStoreSnapshotVersions.getOrElse(storeProviderId, SnapshotUploadEvent(-1, 0))
          logWarning(
            s"State store falling behind $storeProviderId " +
            s"(current: $snapshotEvent, latest: $latestSnapshot)"
          )
        }
      }
      context.reply(true)

    case GetLatestSnapshotVersion(providerId) =>
      val version = stateStoreSnapshotVersions.get(providerId).map(_.version)
      logDebug(s"Got latest snapshot version of the state store $providerId: $version")
      context.reply(version)

    case GetLaggingStores =>
      val (laggingStores, _) = findLaggingStores()
      logDebug(s"Got lagging state stores ${laggingStores
        .map(
          id =>
            s"StateStoreId(operatorId=${id.storeId.operatorId}, " +
            s"partitionId=${id.storeId.partitionId}, " +
            s"storeName=${id.storeId.storeName})"
        )
        .mkString(", ")}")
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
    def isLagging(latest: SnapshotUploadEvent): Boolean = {
      val versionDelta = latest.version - version
      val timeDelta = latest.timestamp - timestamp
      val minVersionDeltaForLogging =
        sqlConf.getConf(SQLConf.STATE_STORE_COORDINATOR_MIN_SNAPSHOT_VERSION_DELTA_TO_LOG)
      // Use 10 times the maintenance interval as the minimum time delta for logging
      val minTimeDeltaForLogging = 10 * sqlConf.getConf(SQLConf.STREAMING_MAINTENANCE_INTERVAL)

      versionDelta >= minVersionDeltaForLogging ||
      (version >= 0 && timeDelta > minTimeDeltaForLogging)
    }

    override def compare(that: SnapshotUploadEvent): Int = {
      this.version.compare(that.version)
    }

    override def toString(): String = {
      s"SnapshotUploadEvent(version=$version, timestamp=$timestamp)"
    }
  }

  private def findLaggingStores(): (Seq[StateStoreProviderId], SnapshotUploadEvent) = {
    // Find the most updated instance to use as reference point
    val latestSnapshot = instances
      .map(
        instance => stateStoreSnapshotVersions.getOrElse(instance._1, SnapshotUploadEvent(-1, 0))
      )
      .max
    // Look for instances that are lagging behind in snapshot uploads
    val laggingStores = instances.keys.filter { storeProviderId =>
      stateStoreSnapshotVersions
        .getOrElse(storeProviderId, SnapshotUploadEvent(-1, 0))
        .isLagging(latestSnapshot)
    }.toSeq
    (laggingStores, latestSnapshot)
  }
}
