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

package org.apache.spark.storage

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.storage.BlockManagerMessages.{BlockManagerHeartbeat, RegisterBlockManager, RemoveExecutor, StopBlockManagerMaster, UpdateBlockInfo}

/**
 * Separate heartbeat out of BlockManagerMasterEndpoint due to performance consideration.
 */
private[spark] class BlockManagerMasterHeartbeatEndpoint(
    override val rpcEnv: RpcEnv,
    isLocal: Boolean)
  extends ThreadSafeRpcEndpoint with Logging {

  // Mapping from block manager id to the block manager's information.
  private val blockManagerLastSeen = new mutable.HashMap[BlockManagerId, Long]

  // Mapping from executor ID to block manager ID.
  private val blockManagerIdByExecutor = new mutable.HashMap[String, BlockManagerId]

  // Use BlockManagerId -> Long to manage the heartbeat last seen, so the events which to handle
  // in this class due to whether or not the block manager id and last seen will be changed.
  // `RegisterBlockManager` and `RemoveExecutor` updates the BlockManagerId
  // and `UpdateBlockInfo` and `BlockManagerHeartbeat` updates the last seen time.
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterBlockManager(blockManagerId, _, _, _, _) =>
      updateLastSeenMs(blockManagerId)
      blockManagerIdByExecutor(blockManagerId.executorId) = blockManagerId
      context.reply(true)

    case UpdateBlockInfo(blockManagerId, _, _, _, _) =>
      updateLastSeenMs(blockManagerId)
      context.reply(true)

    case RemoveExecutor(execId) =>
      blockManagerIdByExecutor.get(execId).foreach(blockManagerLastSeen.remove)
      blockManagerIdByExecutor -= execId
      context.reply(true)

    case BlockManagerHeartbeat(blockManagerId) =>
      context.reply(heartbeatReceived(blockManagerId))

    case StopBlockManagerMaster =>
      context.reply(true)
      stop()

    case _ => // do nothing for unexpected events
  }

  /**
   * Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  private def heartbeatReceived(blockManagerId: BlockManagerId): Boolean = {
    if (!blockManagerLastSeen.contains(blockManagerId)) {
      blockManagerId.isDriver && !isLocal
    } else {
      updateLastSeenMs(blockManagerId)
      true
    }
  }

  def updateLastSeenMs(blockManagerId: BlockManagerId) {
    blockManagerLastSeen(blockManagerId) = System.currentTimeMillis()
  }
}
