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

import scala.collection.mutable

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.util.RpcUtils

/** Trait representing all messages to [[StateStoreCoordinator]] */
private sealed trait StateStoreCoordinatorMessage extends Serializable

private case class ReportActiveInstance(storeId: StateStoreId, host: String, executorId: String)
  extends StateStoreCoordinatorMessage

private case class VerifyIfInstanceActive(storeId: StateStoreId, executorId: String)
  extends StateStoreCoordinatorMessage

private case class GetLocation(storeId: StateStoreId)
  extends StateStoreCoordinatorMessage

private case class DeactivateInstances(storeRootLocation: String)
  extends StateStoreCoordinatorMessage

private object StopCoordinator
  extends StateStoreCoordinatorMessage


private[sql] object StateStoreCoordinatorRef extends Logging {

  private val endpointName = "StateStoreCoordinator"

  def apply(env: SparkEnv): StateStoreCoordinatorRef = synchronized {
    try {
      val coordinator = new StateStoreCoordinator()
      val endpoint = new RpcEndpoint {
        override val rpcEnv: RpcEnv = env.rpcEnv

        override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
          case StopCoordinator =>
            stop() // Stop before replying to ensure that endpoint name has been deregistered
            context.reply(true)
          case message: StateStoreCoordinatorMessage =>
            context.reply(coordinator.process(message))
        }
      }

      val coordinatorRef = env.rpcEnv.setupEndpoint(endpointName, endpoint)
      logInfo("Registered StateStoreCoordinator endpoint")
      new StateStoreCoordinatorRef(coordinatorRef)
    } catch {
      case e: Exception =>
        logDebug("Retrieving exitsing StateStoreCoordinator endpoint")
        val rpcEndpointRef = RpcUtils.makeDriverRef(endpointName, env.conf, env.rpcEnv)
        new StateStoreCoordinatorRef(rpcEndpointRef)
    }
  }
}

private[sql] class StateStoreCoordinatorRef private(rpcEndpointRef: RpcEndpointRef) {

  private[state] def reportActiveInstance(
      storeId: StateStoreId,
      host: String,
      executorId: String): Boolean = {
    rpcEndpointRef.askWithRetry[Boolean](ReportActiveInstance(storeId, host, executorId))
  }

  /** Verify whether the given executor has the active instance of a state store */
  private[state] def verifyIfInstanceActive(storeId: StateStoreId, executorId: String): Boolean = {
    rpcEndpointRef.askWithRetry[Boolean](VerifyIfInstanceActive(storeId, executorId))
  }

  /** Get the location of the state store */
  private[state] def getLocation(storeId: StateStoreId): Option[String] = {
    rpcEndpointRef.askWithRetry[Option[String]](GetLocation(storeId))
  }

  /** Deactivate instances related to a set of operator */
  private[state] def deactivateInstances(storeRootLocation: String): Unit = {
    rpcEndpointRef.askWithRetry[Boolean](DeactivateInstances(storeRootLocation))
  }

  private[state] def stop(): Unit = {
    rpcEndpointRef.askWithRetry[Boolean](StopCoordinator)
  }
}


/** Class for coordinating instances of [[StateStore]]s loaded in executors across the cluster */
private class StateStoreCoordinator {
  private val instances = new mutable.HashMap[StateStoreId, ExecutorCacheTaskLocation]

  def process(message: StateStoreCoordinatorMessage): Any = {
    message match {
      case ReportActiveInstance(id, host, executorId) =>
        instances.put(id, ExecutorCacheTaskLocation(host, executorId))
        true

      case VerifyIfInstanceActive(id, execId) =>
        instances.get(id) match {
          case Some(location) => location.executorId == execId
          case None => false
        }

      case GetLocation(id) =>
        instances.get(id).map(_.toString)

      case DeactivateInstances(loc) =>
        val storeIdsToRemove =
          instances.keys.filter(_.rootLocation == loc).toSeq
        instances --= storeIdsToRemove
        true

      case _ =>
        throw new IllegalArgumentException("Cannot iden")
    }
  }
}


