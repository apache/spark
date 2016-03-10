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

import org.apache.spark.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEnv}
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql.execution.streaming.state.StateStoreCoordinator.StateStoreCoordinatorEndpoint

private sealed trait StateStoreCoordinatorMessage extends Serializable
private case class ReportActiveInstance(storeId: StateStoreId, host: String, executorId: String)
  extends StateStoreCoordinatorMessage
private case class VerifyIfInstanceActive(storeId: StateStoreId, executorId: String)
  extends StateStoreCoordinatorMessage
private object StopCoordinator extends StateStoreCoordinatorMessage


class StateStoreCoordinator(rpcEnv: RpcEnv) {
  private val coordinatorRef = rpcEnv.setupEndpoint(
    "StateStoreCoordinator", new StateStoreCoordinatorEndpoint(rpcEnv, this))
  private val instances = new mutable.HashMap[StateStoreId, ExecutorCacheTaskLocation]

  def reportActiveInstance(storeId: StateStoreId, host: String, executorId: String): Unit = {
    instances.synchronized { instances.put(storeId, ExecutorCacheTaskLocation(host, executorId)) }
  }

  def verifyIfInstanceActive(storeId: StateStoreId, executorId: String): Boolean = {
    instances.synchronized {
      instances.get(storeId).forall(_.executorId == executorId)
    }
  }

  def getLocation(storeId: StateStoreId): Option[String] = {
    instances.synchronized { instances.get(storeId).map(_.toString) }
  }

  def makeInstancesInactive(operatorIds: Set[Long]): Unit = {
    instances.synchronized {
      val instancesToRemove =
        instances.keys.filter(id => operatorIds.contains(id.operatorId)).toSeq
      instances --= instancesToRemove
    }
  }
}

private[spark] object StateStoreCoordinator {

  private[spark] class StateStoreCoordinatorEndpoint(
    override val rpcEnv: RpcEnv, coordinator: StateStoreCoordinator)
    extends RpcEndpoint with Logging {

    override def receive: PartialFunction[Any, Unit] = {
      case StopCoordinator =>
        logInfo("StateStoreCoordinator stopped!")
        stop()
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case ReportActiveInstance(id, host, executorId) =>
        coordinator.reportActiveInstance(id, host, executorId)
      case VerifyIfInstanceActive(id, executor) =>
        context.reply(coordinator.verifyIfInstanceActive(id, executor))
    }
  }
}


