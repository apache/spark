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

import scala.concurrent.{ExecutionContext, Future}

import org.apache.spark.{MapOutputTracker, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{IsolatedThreadSafeRpcEndpoint, RpcCallContext, RpcEnv}
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * An RpcEndpoint to take commands from the master to execute options. For example,
 * this is used to remove blocks from the storage endpoint's BlockManager.
 */
private[storage]
class BlockManagerStorageEndpoint(
    override val rpcEnv: RpcEnv,
    blockManager: BlockManager,
    mapOutputTracker: MapOutputTracker)
  extends IsolatedThreadSafeRpcEndpoint with Logging {

  private val asyncThreadPool =
    ThreadUtils.newDaemonCachedThreadPool("block-manager-storage-async-thread-pool", 100)
  private implicit val asyncExecutionContext = ExecutionContext.fromExecutorService(asyncThreadPool)

  // Operations that involve removing blocks may be slow and should be done asynchronously
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RemoveBlock(blockId) =>
      doAsync[Boolean]("removing block " + blockId, context) {
        blockManager.removeBlock(blockId)
        true
      }

    case RemoveRdd(rddId) =>
      doAsync[Int]("removing RDD " + rddId, context) {
        blockManager.removeRdd(rddId)
      }

    case RemoveShuffle(shuffleId) =>
      doAsync[Boolean]("removing shuffle " + shuffleId, context) {
        if (mapOutputTracker != null) {
          mapOutputTracker.unregisterShuffle(shuffleId)
        }
        SparkEnv.get.shuffleManager.unregisterShuffle(shuffleId)
      }

    case DecommissionBlockManager =>
      context.reply(blockManager.decommissionSelf())

    case RemoveBroadcast(broadcastId, _) =>
      doAsync[Int]("removing broadcast " + broadcastId, context) {
        blockManager.removeBroadcast(broadcastId, tellMaster = true)
      }

    case GetBlockStatus(blockId, _) =>
      context.reply(blockManager.getStatus(blockId))

    case GetMatchingBlockIds(filter, _) =>
      context.reply(blockManager.getMatchingBlockIds(filter))

    case TriggerThreadDump =>
      context.reply(Utils.getThreadDump())

    case ReplicateBlock(blockId, replicas, maxReplicas) =>
      context.reply(blockManager.replicateBlock(blockId, replicas.toSet, maxReplicas))

  }

  private def doAsync[T](actionMessage: String, context: RpcCallContext)(body: => T): Unit = {
    val future = Future {
      logDebug(actionMessage)
      body
    }
    future.foreach { response =>
      logDebug(s"Done $actionMessage, response is $response")
      context.reply(response)
      logDebug(s"Sent response: $response to ${context.senderAddress}")
    }
    future.failed.foreach { t =>
      logError(s"Error in $actionMessage", t)
      context.sendFailure(t)
    }
  }

  override def onStop(): Unit = {
    asyncThreadPool.shutdownNow()
  }
}
