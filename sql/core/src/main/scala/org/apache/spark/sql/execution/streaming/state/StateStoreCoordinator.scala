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

import org.apache.spark.{SparkEnv, Logging}
import org.apache.spark.util.RpcUtils
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEnv}
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql.execution.streaming.state.StateStoreCoordinator.StateStoreCoordinatorEndpoint

/** Trait representing all messages to [[StateStoreCoordinator]] */
private sealed trait StateStoreCoordinatorMessage extends Serializable

private case class ReportActiveInstance(storeId: StateStoreId, host: String, executorId: String)
  extends StateStoreCoordinatorMessage
private case class VerifyIfInstanceActive(storeId: StateStoreId, executorId: String)
  extends StateStoreCoordinatorMessage
private object StopCoordinator extends StateStoreCoordinatorMessage


/** Class for coordinating instances of [[StateStore]]s loaded in the cluster */
class StateStoreCoordinator(rpcEnv: RpcEnv) {
  private val coordinatorRef = rpcEnv.setupEndpoint(
    StateStoreCoordinator.endpointName, new StateStoreCoordinatorEndpoint(rpcEnv, this))
  private val instances = new mutable.HashMap[StateStoreId, ExecutorCacheTaskLocation]

  /** Report active instance of a state store in an executor */
  def reportActiveInstance(storeId: StateStoreId, host: String, executorId: String): Boolean = {
    instances.synchronized { instances.put(storeId, ExecutorCacheTaskLocation(host, executorId)) }
    true
  }

  /** Verify whether the given executor has the active instance of a state store */
  def verifyIfInstanceActive(storeId: StateStoreId, executorId: String): Boolean = {
    instances.synchronized {
      instances.get(storeId) match {
        case Some(location) => location.executorId == executorId
        case None => false
      }
    }
  }

  /** Get the location of the state store */
  def getLocation(storeId: StateStoreId): Option[String] = {
    instances.synchronized { instances.get(storeId).map(_.toString) }
  }

  /** Deactivate instances related to a set of operator */
  def deactivateInstances(operatorIds: Set[Long]): Unit = {
    instances.synchronized {
      val storeIdsToRemove =
        instances.keys.filter(id => operatorIds.contains(id.operatorId)).toSeq
      instances --= storeIdsToRemove
    }
  }

  def stop(): Unit = {
    coordinatorRef.askWithRetry[Boolean](StopCoordinator)
  }
}


private[sql] object StateStoreCoordinator {

  private val endpointName = "StateStoreCoordinator"

  private class StateStoreCoordinatorEndpoint(
    override val rpcEnv: RpcEnv, coordinator: StateStoreCoordinator)
    extends RpcEndpoint with Logging {

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case ReportActiveInstance(id, host, executorId) =>
        context.reply(coordinator.reportActiveInstance(id, host, executorId))
      case VerifyIfInstanceActive(id, executor) =>
        context.reply(coordinator.verifyIfInstanceActive(id, executor))
      case StopCoordinator =>
        // Stop before replying to ensure that endpoint name has been deregistered
        stop()
        context.reply(true)
    }
  }

  def ask(message: StateStoreCoordinatorMessage): Option[Boolean] = {
    val env = SparkEnv.get
    if (env != null) {
      val coordinatorRef = RpcUtils.makeDriverRef(endpointName, env.conf, env.rpcEnv)
      Some(coordinatorRef.askWithRetry[Boolean](message))
    } else {
      None
    }
  }
}


