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

private object StopCoordinator
  extends StateStoreCoordinatorMessage

/** Helper object used to create reference to [[StateStoreCoordinator]]. */
object StateStoreCoordinatorRef extends Logging {

  private val endpointName = "StateStoreCoordinator"

  /**
   * Create a reference to a [[StateStoreCoordinator]]
   */
  def forDriver(env: SparkEnv): StateStoreCoordinatorRef = synchronized {
    try {
      val coordinator = new StateStoreCoordinator(env.rpcEnv)
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

  private[state] def stop(): Unit = {
    rpcEndpointRef.askSync[Boolean](StopCoordinator)
  }
}


/**
 * Class for coordinating instances of [[StateStore]]s loaded in executors across the cluster,
 * and get their locations for job scheduling.
 */
private class StateStoreCoordinator(override val rpcEnv: RpcEnv)
    extends ThreadSafeRpcEndpoint with Logging {
  private val instances = new mutable.HashMap[StateStoreProviderId, ExecutorCacheTaskLocation]

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

    case StopCoordinator =>
      stop() // Stop before replying to ensure that endpoint name has been deregistered
      logInfo("StateStoreCoordinator stopped")
      context.reply(true)
  }
}
