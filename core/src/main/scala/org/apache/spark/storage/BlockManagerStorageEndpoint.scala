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

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}

import org.apache.spark.{MapOutputTracker, SparkEnv}
import org.apache.spark.internal.{Logging, MDC, MessageWithContext}
import org.apache.spark.internal.LogKeys.{BLOCK_ID, BROADCAST_ID, RDD_ID, SHUFFLE_ID}
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
  private implicit val asyncExecutionContext: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(asyncThreadPool)

  // Operations that involve removing blocks may be slow and should be done asynchronously
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RemoveBlock(blockId) =>
      doAsync[Boolean](log"removing block ${MDC(BLOCK_ID, blockId)}", context) {
        blockManager.removeBlock(blockId)
        true
      }

    case RemoveRdd(rddId) =>
      doAsync[Int](log"removing RDD ${MDC(RDD_ID, rddId)}", context) {
        blockManager.removeRdd(rddId)
      }

    case RemoveShuffle(shuffleId) =>
      doAsync[Boolean](log"removing shuffle ${MDC(SHUFFLE_ID, shuffleId)}", context) {
        if (mapOutputTracker != null) {
          mapOutputTracker.unregisterShuffle(shuffleId)
        }
        SparkEnv.get.shuffleManager.unregisterShuffle(shuffleId)
      }

    case DecommissionBlockManager =>
      context.reply(blockManager.decommissionSelf())

    case RemoveBroadcast(broadcastId, _) =>
      doAsync[Int](log"removing broadcast ${MDC(BROADCAST_ID, broadcastId)}", context) {
        blockManager.removeBroadcast(broadcastId, tellMaster = true)
      }

    case GetBlockStatus(blockId, _) =>
      context.reply(blockManager.getStatus(blockId))

    case GetMatchingBlockIds(filter, _) =>
      context.reply(blockManager.getMatchingBlockIds(filter))

    case TriggerThreadDump =>
      context.reply(Utils.getThreadDump())

    case TriggerHeapHistogram =>
      context.reply(Utils.getHeapHistogram())

    case ReplicateBlock(blockId, replicas, maxReplicas) =>
      context.reply(blockManager.replicateBlock(blockId, replicas.toSet, maxReplicas))

    case MarkRDDBlockAsVisible(blockId) =>
      // The message is sent from driver to ask the block manager to mark the rdd block with
      // `blockId` to be visible now. This happens in 2 scenarios:
      // 1. A task computing/caching the rdd block finished successfully and the rdd block can be
      //    turned to be visible. Driver will ask all block managers hosting the rdd block to mark
      //    the block as visible.
      // 2. Once a replica of a visible block is cached and reported, driver will also ask the
      //    the block manager to mark the block as visible immediately.
      context.reply(blockManager.blockInfoManager.tryMarkBlockAsVisible(blockId))
  }

  private def doAsync[T](
      actionMessage: MessageWithContext,
      context: RpcCallContext)(body: => T): Unit = {
    val future = Future {
      logDebug(actionMessage.message)
      body
    }
    future.foreach { response =>
      logDebug(s"Done ${actionMessage.message}, response is $response")
      context.reply(response)
      logDebug(s"Sent response: $response to ${context.senderAddress}")
    }
    future.failed.foreach { t =>
      logError(log"Error in " + actionMessage, t)
      context.sendFailure(t)
    }
  }

  override def onStop(): Unit = {
    asyncThreadPool.shutdownNow()
  }
}
