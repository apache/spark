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
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.{BlockStoreClient, MergedBlockMeta, MergedBlocksMetaListener}
import org.apache.spark.storage.BlockManagerId.SHUFFLE_MERGER_IDENTIFIER
import org.apache.spark.storage.ShuffleBlockFetcherIterator._

/**
 * Helper class for [[ShuffleBlockFetcherIterator]] that encapsulates all the push-based
 * functionality to fetch merged block meta and merged shuffle block chunks.
 */
private class PushBasedFetchHelper(
  private val iterator: ShuffleBlockFetcherIterator,
  private val shuffleClient: BlockStoreClient,
  private val blockManager: BlockManager,
  private val mapOutputTracker: MapOutputTracker) extends Logging {

  private[this] val startTimeNs = System.nanoTime()

  private[this] val localShuffleMergerBlockMgrId = BlockManagerId(
    SHUFFLE_MERGER_IDENTIFIER, blockManager.blockManagerId.host,
    blockManager.blockManagerId.port, blockManager.blockManagerId.topologyInfo)

  /** A map for storing merged block shuffle chunk bitmap */
  private[this] val chunksMetaMap = new mutable.HashMap[ShuffleBlockChunkId, RoaringBitmap]()

  /**
   * Returns true if the address is for a push-merged block.
   */
  def isMergedShuffleBlockAddress(address: BlockManagerId): Boolean = {
    SHUFFLE_MERGER_IDENTIFIER.equals(address.executorId)
  }

  /**
   * Returns true if the address is not of executor local or merged local block. false otherwise.
   */
  def isNotExecutorOrMergedLocal(address: BlockManagerId): Boolean = {
    (isMergedShuffleBlockAddress(address) && address.host != blockManager.blockManagerId.host) ||
      (!isMergedShuffleBlockAddress(address) && address != blockManager.blockManagerId)
  }

  /**
   * Returns true if the address if of merged local block. false otherwise.
   */
  def isMergedLocal(address: BlockManagerId): Boolean = {
    isMergedShuffleBlockAddress(address) && address.host == blockManager.blockManagerId.host
  }

  def getNumberOfBlocksInChunk(blockId : ShuffleBlockChunkId): Int = {
    chunksMetaMap(blockId).getCardinality
  }

  def removeChunk(blockId: ShuffleBlockChunkId): Unit = {
    chunksMetaMap.remove(blockId)
  }

  def createChunkBlockInfosFromMetaResponse(
      shuffleId: Int,
      reduceId: Int,
      blockSize: Long,
      numChunks: Int,
    bitmaps: Array[RoaringBitmap]): ArrayBuffer[(BlockId, Long, Int)] = {
    val approxChunkSize = blockSize / numChunks
    val blocksToFetch = new ArrayBuffer[(BlockId, Long, Int)]()
    for (i <- 0 until numChunks) {
      val blockChunkId = ShuffleBlockChunkId(shuffleId, reduceId, i)
      chunksMetaMap.put(blockChunkId, bitmaps(i))
      logDebug(s"adding block chunk $blockChunkId of size $approxChunkSize")
      blocksToFetch += ((blockChunkId, approxChunkSize, SHUFFLE_PUSH_MAP_ID))
    }
    blocksToFetch
  }

  def sendFetchMergedStatusRequest(req: FetchRequest): Unit = {
    val sizeMap = req.blocks.map {
      case FetchBlockInfo(blockId, size, _) =>
        val shuffleBlockId = blockId.asInstanceOf[ShuffleBlockId]
        ((shuffleBlockId.shuffleId, shuffleBlockId.reduceId), size)}.toMap
    val address = req.address
    val mergedBlocksMetaListener = new MergedBlocksMetaListener {
      override def onSuccess(shuffleId: Int, reduceId: Int, meta: MergedBlockMeta): Unit = {
        logInfo(s"Received the meta of merged block for ($shuffleId, $reduceId)  " +
          s"from ${req.address.host}:${req.address.port}")
        try {
          iterator.addToResultsQueue(MergedBlocksMetaFetchResult(shuffleId, reduceId,
            sizeMap((shuffleId, reduceId)), meta.getNumChunks, meta.readChunkBitmaps(), address))
        } catch {
          case exception: Throwable =>
            logError(s"Failed to parse the meta of merged block for ($shuffleId, $reduceId) " +
              s"from ${req.address.host}:${req.address.port}", exception)
            iterator.addToResultsQueue(
              MergedBlocksMetaFailedFetchResult(shuffleId, reduceId, address))
        }
      }

      override def onFailure(shuffleId: Int, reduceId: Int, exception: Throwable): Unit = {
        logError(s"Failed to get the meta of merged block for ($shuffleId, $reduceId) " +
          s"from ${req.address.host}:${req.address.port}", exception)
        iterator.addToResultsQueue(MergedBlocksMetaFailedFetchResult(shuffleId, reduceId, address))
      }
    }
    req.blocks.foreach { block =>
      val shuffleBlockId = block.blockId.asInstanceOf[ShuffleBlockId]
      shuffleClient.getMergedBlockMeta(address.host, address.port, shuffleBlockId.shuffleId,
        shuffleBlockId.reduceId, mergedBlocksMetaListener)
    }
  }

  // Fetch all outstanding merged local blocks
  def fetchAllMergedLocalBlocks(
    mergedLocalBlocks: mutable.LinkedHashSet[BlockId]): Unit = {
    if (mergedLocalBlocks.nonEmpty) {
      blockManager.hostLocalDirManager.foreach(fetchMergedLocalBlocks(_, mergedLocalBlocks))
    }
  }

  /**
   * Fetch the merged blocks dirs if they are not in the cache and eventually fetch merged local
   * blocks.
   */
  private def fetchMergedLocalBlocks(
      hostLocalDirManager: HostLocalDirManager,
      mergedLocalBlocks: mutable.LinkedHashSet[BlockId]): Unit = {
    val cachedMergerDirs = hostLocalDirManager.getCachedHostLocalDirs.get(
      SHUFFLE_MERGER_IDENTIFIER)
    if (cachedMergerDirs.isDefined) {
      logDebug(s"Fetching local merged blocks with cached executors dir: " +
        s"${cachedMergerDirs.get.mkString(", ")}")
      mergedLocalBlocks.foreach(blockId =>
        fetchMergedLocalBlock(blockId, cachedMergerDirs.get, localShuffleMergerBlockMgrId))
    } else {
      logDebug(s"Asynchronous fetching local merged blocks without cached executors dir")
      hostLocalDirManager.getHostLocalDirs(localShuffleMergerBlockMgrId.host,
        localShuffleMergerBlockMgrId.port, Array(SHUFFLE_MERGER_IDENTIFIER)) {
        case Success(dirs) =>
          mergedLocalBlocks.takeWhile {
            blockId =>
              logDebug(s"Successfully fetched local dirs: " +
                s"${dirs.get(SHUFFLE_MERGER_IDENTIFIER).mkString(", ")}")
              fetchMergedLocalBlock(blockId, dirs(SHUFFLE_MERGER_IDENTIFIER),
                localShuffleMergerBlockMgrId)
          }
          logDebug(s"Got local merged blocks (without cached executors' dir) in " +
            s"${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)} ms")
        case Failure(throwable) =>
          // If we see an exception with getting the local dirs for local merged blocks,
          // we fallback to fetch the original unmerged blocks. We do not report block fetch
          // failure.
          logWarning(s"Error occurred while getting the local dirs for local merged " +
            s"blocks: ${mergedLocalBlocks.mkString(", ")}. Fetch the original blocks instead",
            throwable)
          mergedLocalBlocks.foreach(
            blockId => iterator.addToResultsQueue(
              IgnoreFetchResult(blockId, localShuffleMergerBlockMgrId, 0, isNetworkReqDone = false))
          )
      }
    }
  }

  /**
   * Fetch a single local merged block generated.
   * @param blockId ShuffleBlockId to be fetched
   * @param localDirs Local directories where the merged shuffle files are stored
   * @param blockManagerId BlockManagerId
   * @return Boolean represents successful or failed fetch
   */
  private[this] def fetchMergedLocalBlock(
      blockId: BlockId,
      localDirs: Array[String],
      blockManagerId: BlockManagerId): Boolean = {
    try {
      val shuffleBlockId = blockId.asInstanceOf[ShuffleBlockId]
      val chunksMeta = blockManager.getMergedBlockMeta(shuffleBlockId, localDirs)
        .readChunkBitmaps()
      // Fetch local merged shuffle block data as multiple chunks
      val bufs: Seq[ManagedBuffer] = blockManager.getMergedBlockData(shuffleBlockId, localDirs)
      // Update total number of blocks to fetch, reflecting the multiple local chunks
      iterator.foundMoreBlocksToFetch(bufs.size - 1)
      for (chunkId <- bufs.indices) {
        val buf = bufs(chunkId)
        buf.retain()
        val shuffleChunkId = ShuffleBlockChunkId(shuffleBlockId.shuffleId,
          shuffleBlockId.reduceId, chunkId)
        iterator.addToResultsQueue(
          SuccessFetchResult(shuffleChunkId, SHUFFLE_PUSH_MAP_ID, blockManagerId, buf.size(), buf,
            isNetworkReqDone = false))
        chunksMetaMap.put(shuffleChunkId, chunksMeta(chunkId))
      }
      true
    } catch {
      case e: Exception =>
        // If we see an exception with reading a local merged block, we fallback to
        // fetch the original unmerged blocks. We do not report block fetch failure
        // and will continue with the remaining local block read.
        logWarning(s"Error occurred while fetching local merged block, " +
          s"prepare to fetch the original blocks", e)
        iterator.addToResultsQueue(
          IgnoreFetchResult(blockId, blockManagerId, 0, isNetworkReqDone = false))
        false
    }
  }

  /**
   * Initiate fetching fallback blocks for a merged block (or a merged block chunk) that's failed
   * to fetch.
   * It calls out to map output tracker to get the list of original blocks for the
   * given merged blocks, split them into remote and local blocks, and process them
   * accordingly.
   * The fallback happens when:
   * 1. There is an exception while creating shuffle block chunk from local merged shuffle block.
   *    See fetchLocalBlock.
   * 2. There is a failure when fetching remote shuffle block chunks.
   * 3. There is a failure when processing SuccessFetchResult which is for a shuffle chunk
   *    (local or remote).
   *
   * @return number of blocks processed
   */
  def initiateFallbackBlockFetchForMergedBlock(
      blockId: BlockId,
      address: BlockManagerId): Int = {
    logWarning(s"Falling back to fetch the original unmerged blocks for merged block $blockId")
    // Increase the blocks processed since we will process another block in the next iteration of
    // the while loop in ShuffleBlockFetcherIterator.next().
    var blocksProcessed = 1
    val fallbackBlocksByAddr: Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])] =
      if (blockId.isShuffle) {
        val shuffleBlockId = blockId.asInstanceOf[ShuffleBlockId]
        mapOutputTracker.getMapSizesForMergeResult(
          shuffleBlockId.shuffleId, shuffleBlockId.reduceId)
      } else {
        val shuffleChunkId = blockId.asInstanceOf[ShuffleBlockChunkId]
        val chunkBitmap: RoaringBitmap = chunksMetaMap.remove(shuffleChunkId).orNull
        // When there is a failure to fetch a remote merged shuffle block chunk, then we try to
        // fallback not only for that particular remote shuffle block chunk but also for all the
        // pending block chunks that belong to the same host. The reason for doing so is that it is
        // very likely that the subsequent requests for merged block chunks from this host will fail
        // as well. Since, push-based shuffle is best effort and we try not to increase the delay
        // of the fetches, we immediately fallback for all the pending shuffle chunks in the
        // fetchRequests queue.
        if (isNotExecutorOrMergedLocal(address)) {
          // Fallback for all the pending fetch requests
          val pendingShuffleChunks = iterator.removePendingChunks(shuffleChunkId, address)
          if (pendingShuffleChunks.nonEmpty) {
            pendingShuffleChunks.foreach { pendingBlockId =>
              logWarning(s"Falling back immediately for merged block $pendingBlockId")
              val bitmapOfPendingChunk: RoaringBitmap =
                chunksMetaMap.remove(pendingBlockId).orNull
              assert(bitmapOfPendingChunk != null)
              chunkBitmap.or(bitmapOfPendingChunk)
            }
            // These blocks were added to numBlocksToFetch so we increment numBlocksProcessed
            blocksProcessed += pendingShuffleChunks.size
          }
        }
        mapOutputTracker.getMapSizesForMergeResult(
          shuffleChunkId.shuffleId, shuffleChunkId.reduceId, chunkBitmap)
      }
    iterator.fetchFallbackBlocks(fallbackBlocksByAddr)
    blocksProcessed
  }
}
