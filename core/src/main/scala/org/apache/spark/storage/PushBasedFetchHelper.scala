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

import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}

import org.roaringbitmap.RoaringBitmap

import org.apache.spark.MapOutputTracker
import org.apache.spark.MapOutputTracker.SHUFFLE_PUSH_MAP_ID
import org.apache.spark.internal.Logging
import org.apache.spark.network.shuffle.{BlockStoreClient, MergedBlockMeta, MergedBlocksMetaListener}
import org.apache.spark.storage.BlockManagerId.SHUFFLE_MERGER_IDENTIFIER
import org.apache.spark.storage.ShuffleBlockFetcherIterator._

/**
 * Helper class for [[ShuffleBlockFetcherIterator]] that encapsulates all the push-based
 * functionality to fetch push-merged block meta and shuffle chunks.
 * A push-merged block contains multiple shuffle chunks where each shuffle chunk contains multiple
 * shuffle blocks that belong to the common reduce partition and were merged by the
 * external shuffle service to that chunk.
 */
private class PushBasedFetchHelper(
   private val iterator: ShuffleBlockFetcherIterator,
   private val shuffleClient: BlockStoreClient,
   private val blockManager: BlockManager,
   private val mapOutputTracker: MapOutputTracker) extends Logging {

  private[this] val startTimeNs = System.nanoTime()

  private[storage] val localShuffleMergerBlockMgrId = BlockManagerId(
    SHUFFLE_MERGER_IDENTIFIER, blockManager.blockManagerId.host,
    blockManager.blockManagerId.port, blockManager.blockManagerId.topologyInfo)

  /**
   * A map for storing shuffle chunk bitmap.
   */
  private[this] val chunksMetaMap = new mutable.HashMap[ShuffleBlockChunkId, RoaringBitmap]()

  /**
   * Returns true if the address is for a push-merged block.
   */
  def isPushMergedShuffleBlockAddress(address: BlockManagerId): Boolean = {
    SHUFFLE_MERGER_IDENTIFIER == address.executorId
  }

  /**
   * Returns true if the address is of a remote push-merged block. false otherwise.
   */
  def isRemotePushMergedBlockAddress(address: BlockManagerId): Boolean = {
    isPushMergedShuffleBlockAddress(address) && address.host != blockManager.blockManagerId.host
  }

  /**
   * Returns true if the address is of a push-merged-local block. false otherwise.
   */
  def isLocalPushMergedBlockAddress(address: BlockManagerId): Boolean = {
    isPushMergedShuffleBlockAddress(address) && address.host == blockManager.blockManagerId.host
  }

  /**
   * This is executed by the task thread when the `iterator.next()` is invoked and the iterator
   * processes a response of type [[ShuffleBlockFetcherIterator.SuccessFetchResult]].
   *
   * @param blockId shuffle chunk id.
   */
  def removeChunk(blockId: ShuffleBlockChunkId): Unit = {
    chunksMetaMap.remove(blockId)
  }

  /**
   * This is executed by the task thread when the `iterator.next()` is invoked and the iterator
   * processes a response of type [[ShuffleBlockFetcherIterator.PushMergedLocalMetaFetchResult]].
   *
   * @param blockId shuffle chunk id.
   */
  def addChunk(blockId: ShuffleBlockChunkId, chunkMeta: RoaringBitmap): Unit = {
    chunksMetaMap(blockId) = chunkMeta
  }

  /**
   * This is executed by the task thread when the `iterator.next()` is invoked and the iterator
   * processes a response of type [[ShuffleBlockFetcherIterator.PushMergedRemoteMetaFetchResult]].
   *
   * @param shuffleId shuffle id.
   * @param reduceId  reduce id.
   * @param blockSize size of the push-merged block.
   * @param bitmaps   chunk bitmaps, where each bitmap contains all the mapIds that were merged
   *                  to that chunk.
   * @return  shuffle chunks to fetch.
   */
  def createChunkBlockInfosFromMetaResponse(
      shuffleId: Int,
      reduceId: Int,
      blockSize: Long,
      bitmaps: Array[RoaringBitmap]): ArrayBuffer[(BlockId, Long, Int)] = {
    val approxChunkSize = blockSize / bitmaps.length
    val blocksToFetch = new ArrayBuffer[(BlockId, Long, Int)]()
    for (i <- bitmaps.indices) {
      val blockChunkId = ShuffleBlockChunkId(shuffleId, reduceId, i)
      chunksMetaMap.put(blockChunkId, bitmaps(i))
      logDebug(s"adding block chunk $blockChunkId of size $approxChunkSize")
      blocksToFetch += ((blockChunkId, approxChunkSize, SHUFFLE_PUSH_MAP_ID))
    }
    blocksToFetch
  }

  /**
   * This is executed by the task thread when the iterator is initialized and only if it has
   * push-merged blocks for which it needs to fetch the metadata.
   *
   * @param req [[ShuffleBlockFetcherIterator.FetchRequest]] that only contains requests to fetch
   *            metadata of push-merged blocks.
   */
  def sendFetchMergedStatusRequest(req: FetchRequest): Unit = {
    val sizeMap = req.blocks.map {
      case FetchBlockInfo(blockId, size, _) =>
        val shuffleBlockId = blockId.asInstanceOf[ShuffleBlockId]
        ((shuffleBlockId.shuffleId, shuffleBlockId.reduceId), size)
    }.toMap
    val address = req.address
    val mergedBlocksMetaListener = new MergedBlocksMetaListener {
      override def onSuccess(shuffleId: Int, reduceId: Int, meta: MergedBlockMeta): Unit = {
        logInfo(s"Received the meta of push-merged block for ($shuffleId, $reduceId)  " +
          s"from ${req.address.host}:${req.address.port}")
        try {
          iterator.addToResultsQueue(PushMergedRemoteMetaFetchResult(shuffleId, reduceId,
            sizeMap((shuffleId, reduceId)), meta.readChunkBitmaps(), address))
        } catch {
          case exception: Exception =>
            logError(s"Failed to parse the meta of push-merged block for ($shuffleId, " +
              s"$reduceId) from ${req.address.host}:${req.address.port}", exception)
            iterator.addToResultsQueue(
              PushMergedRemoteMetaFailedFetchResult(shuffleId, reduceId, address))
        }
      }

      override def onFailure(shuffleId: Int, reduceId: Int, exception: Throwable): Unit = {
        logError(s"Failed to get the meta of push-merged block for ($shuffleId, $reduceId) " +
          s"from ${req.address.host}:${req.address.port}", exception)
        iterator.addToResultsQueue(
          PushMergedRemoteMetaFailedFetchResult(shuffleId, reduceId, address))
      }
    }
    req.blocks.foreach { block =>
      val shuffleBlockId = block.blockId.asInstanceOf[ShuffleBlockId]
      shuffleClient.getMergedBlockMeta(address.host, address.port, shuffleBlockId.shuffleId,
        shuffleBlockId.reduceId, mergedBlocksMetaListener)
    }
  }

  /**
   * This is executed by the task thread when the iterator is initialized. It fetches all the
   * outstanding push-merged local blocks.
   * @param pushMergedLocalBlocks set of identified merged local blocks and their sizes.
   */
  def fetchAllPushMergedLocalBlocks(
      pushMergedLocalBlocks: mutable.LinkedHashSet[BlockId]): Unit = {
    if (pushMergedLocalBlocks.nonEmpty) {
      blockManager.hostLocalDirManager.foreach(fetchPushMergedLocalBlocks(_, pushMergedLocalBlocks))
    }
  }

  /**
   * Fetch the push-merged blocks dirs if they are not in the cache and eventually fetch push-merged
   * local blocks.
   */
  private def fetchPushMergedLocalBlocks(
      hostLocalDirManager: HostLocalDirManager,
      pushMergedLocalBlocks: mutable.LinkedHashSet[BlockId]): Unit = {
    val cachedPushedMergedDirs = hostLocalDirManager.getCachedHostLocalDirsFor(
      SHUFFLE_MERGER_IDENTIFIER)
    if (cachedPushedMergedDirs.isDefined) {
      logDebug(s"Fetch the push-merged-local blocks with cached merged dirs: " +
        s"${cachedPushedMergedDirs.get.mkString(", ")}")
      pushMergedLocalBlocks.foreach { blockId =>
        fetchPushMergedLocalBlock(blockId, cachedPushedMergedDirs.get,
          localShuffleMergerBlockMgrId)
      }
    } else {
      // Push-based shuffle is only enabled when the external shuffle service is enabled. If the
      // external shuffle service is not enabled, then there will not be any push-merged blocks
      // for the iterator to fetch.
      logDebug(s"Asynchronous fetch the push-merged-local blocks without cached merged " +
        s"dirs from the external shuffle service")
      hostLocalDirManager.getHostLocalDirs(blockManager.blockManagerId.host,
        blockManager.externalShuffleServicePort, Array(SHUFFLE_MERGER_IDENTIFIER)) {
        case Success(dirs) =>
          logDebug(s"Fetched merged dirs in " +
            s"${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)} ms")
          pushMergedLocalBlocks.foreach {
            blockId =>
              logDebug(s"Successfully fetched local dirs: " +
                s"${dirs.get(SHUFFLE_MERGER_IDENTIFIER).mkString(", ")}")
              fetchPushMergedLocalBlock(blockId, dirs(SHUFFLE_MERGER_IDENTIFIER),
                localShuffleMergerBlockMgrId)
          }
        case Failure(throwable) =>
          // If we see an exception with getting the local dirs for push-merged-local blocks,
          // we fallback to fetch the original blocks. We do not report block fetch failure.
          logWarning(s"Error while fetching the merged dirs for push-merged-local " +
            s"blocks: ${pushMergedLocalBlocks.mkString(", ")}. Fetch the original blocks instead",
            throwable)
          pushMergedLocalBlocks.foreach {
            blockId =>
              iterator.addToResultsQueue(FallbackOnPushMergedFailureResult(
                blockId, localShuffleMergerBlockMgrId, 0, isNetworkReqDone = false))
          }
      }
    }
  }

  /**
   * Fetch a single push-merged-local block generated. This can also be executed by the task thread
   * as well as the netty thread.
   * @param blockId ShuffleBlockId to be fetched
   * @param localDirs Local directories where the push-merged shuffle files are stored
   * @param blockManagerId BlockManagerId
   */
  private[this] def fetchPushMergedLocalBlock(
      blockId: BlockId,
      localDirs: Array[String],
      blockManagerId: BlockManagerId): Unit = {
    try {
      val shuffleBlockId = blockId.asInstanceOf[ShuffleBlockId]
      val chunksMeta = blockManager.getLocalMergedBlockMeta(shuffleBlockId, localDirs)
      iterator.addToResultsQueue(PushMergedLocalMetaFetchResult(
        shuffleBlockId.shuffleId, shuffleBlockId.reduceId, chunksMeta.readChunkBitmaps(),
        localDirs))
    } catch {
      case e: Exception =>
        // If we see an exception with reading a push-merged-local meta, we fallback to
        // fetch the original blocks. We do not report block fetch failure
        // and will continue with the remaining local block read.
        logWarning(s"Error occurred while fetching push-merged-local meta, " +
          s"prepare to fetch the original blocks", e)
        iterator.addToResultsQueue(
          FallbackOnPushMergedFailureResult(blockId, blockManagerId, 0, isNetworkReqDone = false))
    }
  }

  /**
   * This is executed by the task thread when the `iterator.next()` is invoked and the iterator
   * processes a response of type:
   * 1) [[ShuffleBlockFetcherIterator.SuccessFetchResult]]
   * 2) [[ShuffleBlockFetcherIterator.FallbackOnPushMergedFailureResult]]
   * 3) [[ShuffleBlockFetcherIterator.PushMergedRemoteMetaFailedFetchResult]]
   *
   * This initiates fetching fallback blocks for a push-merged block or a shuffle chunk that
   * failed to fetch.
   * It makes a call to the map output tracker to get the list of original blocks for the
   * given push-merged block/shuffle chunk, split them into remote and local blocks, and process
   * them accordingly.
   * It also updates the numberOfBlocksToFetch in the iterator as it processes failed response and
   * finds more push-merged requests to remote and again updates it with additional requests for
   * original blocks.
   * The fallback happens when:
   * 1. There is an exception while creating shuffle chunks from push-merged-local shuffle block.
   *    See fetchLocalBlock.
   * 2. There is a failure when fetching remote shuffle chunks.
   * 3. There is a failure when processing SuccessFetchResult which is for a shuffle chunk
   *    (local or remote).
   */
  def initiateFallbackFetchForPushMergedBlock(
      blockId: BlockId,
      address: BlockManagerId): Unit = {
    assert(blockId.isInstanceOf[ShuffleBlockId] || blockId.isInstanceOf[ShuffleBlockChunkId])
    logWarning(s"Falling back to fetch the original blocks for push-merged block $blockId")
    // Increase the blocks processed since we will process another block in the next iteration of
    // the while loop in ShuffleBlockFetcherIterator.next().
    val fallbackBlocksByAddr: Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])] =
      blockId match {
        case shuffleBlockId: ShuffleBlockId =>
          iterator.decreaseNumBlocksToFetch(1)
          mapOutputTracker.getMapSizesForMergeResult(
            shuffleBlockId.shuffleId, shuffleBlockId.reduceId)
        case _ =>
          val shuffleChunkId = blockId.asInstanceOf[ShuffleBlockChunkId]
          val chunkBitmap: RoaringBitmap = chunksMetaMap.remove(shuffleChunkId).get
          var blocksProcessed = 1
          // When there is a failure to fetch a remote shuffle chunk, then we try to
          // fallback not only for that particular remote shuffle chunk but also for all the
          // pending chunks that belong to the same host. The reason for doing so is that it
          // is very likely that the subsequent requests for shuffle chunks from this host will
          // fail as well. Since, push-based shuffle is best effort and we try not to increase the
          // delay of the fetches, we immediately fallback for all the pending shuffle chunks in the
          // fetchRequests queue.
          if (isRemotePushMergedBlockAddress(address)) {
            // Fallback for all the pending fetch requests
            val pendingShuffleChunks = iterator.removePendingChunks(shuffleChunkId, address)
            pendingShuffleChunks.foreach { pendingBlockId =>
              logInfo(s"Falling back immediately for shuffle chunk $pendingBlockId")
              val bitmapOfPendingChunk: RoaringBitmap = chunksMetaMap.remove(pendingBlockId).get
              chunkBitmap.or(bitmapOfPendingChunk)
            }
            // These blocks were added to numBlocksToFetch so we increment numBlocksProcessed
            blocksProcessed += pendingShuffleChunks.size
          }
          iterator.decreaseNumBlocksToFetch(blocksProcessed)
          mapOutputTracker.getMapSizesForMergeResult(
            shuffleChunkId.shuffleId, shuffleChunkId.reduceId, chunkBitmap)
      }
    iterator.fallbackFetch(fallbackBlocksByAddr)
  }
}
