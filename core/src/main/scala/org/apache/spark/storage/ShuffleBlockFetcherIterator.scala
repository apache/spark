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

import java.util.concurrent.LinkedBlockingQueue

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet
import scala.collection.mutable.Queue

import org.apache.spark.{TaskContext, Logging}
import org.apache.spark.network.{ManagedBuffer, BlockFetchingListener, BlockTransferService}
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.Utils


/**
 * An iterator that fetches multiple blocks. For local blocks, it fetches from the local block
 * manager. For remote blocks, it fetches them using the provided BlockTransferService.
 *
 * This creates an iterator of (BlockID, values) tuples so the caller can handle blocks in a
 * pipelined fashion as they are received.
 *
 * The implementation throttles the remote fetches to they don't exceed maxBytesInFlight to avoid
 * using too much memory.
 *
 * @param context [[TaskContext]], used for metrics update
 * @param blockTransferService [[BlockTransferService]] for fetching remote blocks
 * @param blockManager  [[BlockManager]] for reading local blocks
 * @param blocksByAddress list of blocks to fetch grouped by the [[BlockManagerId]].
 *                        For each block we also require the size (in bytes as a long field) in
 *                        order to throttle the memory usage.
 * @param serializer serializer used to deserialize the data.
 * @param maxBytesInFlight max size (in bytes) of remote blocks to fetch at any given point.
 */
private[spark]
final class ShuffleBlockFetcherIterator(
    context: TaskContext,
    blockTransferService: BlockTransferService,
    blockManager: BlockManager,
    blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])],
    serializer: Serializer,
    maxBytesInFlight: Long)
  extends Iterator[(BlockId, Option[Iterator[Any]])] with Logging {

  import ShuffleBlockFetcherIterator._

  /**
   * Total number of blocks to fetch. This can be smaller than the total number of blocks
   * in [[blocksByAddress]] because we filter out zero-sized blocks in [[initialize]].
   *
   * This should equal localBlocks.size + remoteBlocks.size.
   */
  private[this] var numBlocksToFetch = 0

  /**
   * The number of blocks proccessed by the caller. The iterator is exhausted when
   * [[numBlocksProcessed]] == [[numBlocksToFetch]].
   */
  private[this] var numBlocksProcessed = 0

  private[this] val startTime = System.currentTimeMillis

  /** Local blocks to fetch, excluding zero-sized blocks. */
  private[this] val localBlocks = new ArrayBuffer[BlockId]()

  /** Remote blocks to fetch, excluding zero-sized blocks. */
  private[this] val remoteBlocks = new HashSet[BlockId]()

  /**
   * A queue to hold our results. This turns the asynchronous model provided by
   * [[BlockTransferService]] into a synchronous model (iterator).
   */
  private[this] val results = new LinkedBlockingQueue[FetchResult]

  // Queue of fetch requests to issue; we'll pull requests off this gradually to make sure that
  // the number of bytes in flight is limited to maxBytesInFlight
  private[this] val fetchRequests = new Queue[FetchRequest]

  // Current bytes in flight from our requests
  private[this] var bytesInFlight = 0L

  private[this] val shuffleMetrics = context.taskMetrics.createShuffleReadMetricsForDependency()

  initialize()

  private[this] def sendRequest(req: FetchRequest) {
    logDebug("Sending request for %d blocks (%s) from %s".format(
      req.blocks.size, Utils.bytesToString(req.size), req.address.hostPort))
    bytesInFlight += req.size

    // so we can look up the size of each blockID
    val sizeMap = req.blocks.map { case (blockId, size) => (blockId.toString, size) }.toMap
    val blockIds = req.blocks.map(_._1.toString)

    blockTransferService.fetchBlocks(req.address.host, req.address.port, blockIds,
      new BlockFetchingListener {
        override def onBlockFetchSuccess(blockId: String, data: ManagedBuffer): Unit = {
          results.put(new FetchResult(BlockId(blockId), sizeMap(blockId),
            () => serializer.newInstance().deserializeStream(
              blockManager.wrapForCompression(BlockId(blockId), data.inputStream())).asIterator
          ))
          shuffleMetrics.remoteBytesRead += data.size
          shuffleMetrics.remoteBlocksFetched += 1
          logDebug("Got remote block " + blockId + " after " + Utils.getUsedTimeMs(startTime))
        }

        override def onBlockFetchFailure(e: Throwable): Unit = {
          logError("Failed to get block(s) from ${req.address.host}:${req.address.port}", e)
          // Note that there is a chance that some blocks have been fetched successfully, but we
          // still add them to the failed queue. This is fine because when the caller see a
          // FetchFailedException, it is going to fail the entire task anyway.
          for ((blockId, size) <- req.blocks) {
            results.put(new FetchResult(blockId, -1, null))
          }
        }
      }
    )
  }

  private[this] def splitLocalRemoteBlocks(): ArrayBuffer[FetchRequest] = {
    // Make remote requests at most maxBytesInFlight / 5 in length; the reason to keep them
    // smaller than maxBytesInFlight is to allow multiple, parallel fetches from up to 5
    // nodes, rather than blocking on reading output from one node.
    val targetRequestSize = math.max(maxBytesInFlight / 5, 1L)
    logInfo("maxBytesInFlight: " + maxBytesInFlight + ", targetRequestSize: " + targetRequestSize)

    // Split local and remote blocks. Remote blocks are further split into FetchRequests of size
    // at most maxBytesInFlight in order to limit the amount of data in flight.
    val remoteRequests = new ArrayBuffer[FetchRequest]

    // Tracks total number of blocks (including zero sized blocks)
    var totalBlocks = 0
    for ((address, blockInfos) <- blocksByAddress) {
      totalBlocks += blockInfos.size
      if (address == blockManager.blockManagerId) {
        // Filter out zero-sized blocks
        localBlocks ++= blockInfos.filter(_._2 != 0).map(_._1)
        numBlocksToFetch += localBlocks.size
      } else {
        val iterator = blockInfos.iterator
        var curRequestSize = 0L
        var curBlocks = new ArrayBuffer[(BlockId, Long)]
        while (iterator.hasNext) {
          val (blockId, size) = iterator.next()
          // Skip empty blocks
          if (size > 0) {
            curBlocks += ((blockId, size))
            remoteBlocks += blockId
            numBlocksToFetch += 1
            curRequestSize += size
          } else if (size < 0) {
            throw new BlockException(blockId, "Negative block size " + size)
          }
          if (curRequestSize >= targetRequestSize) {
            // Add this FetchRequest
            remoteRequests += new FetchRequest(address, curBlocks)
            curBlocks = new ArrayBuffer[(BlockId, Long)]
            logDebug(s"Creating fetch request of $curRequestSize at $address")
            curRequestSize = 0
          }
        }
        // Add in the final request
        if (curBlocks.nonEmpty) {
          remoteRequests += new FetchRequest(address, curBlocks)
        }
      }
    }
    logInfo(s"Getting $numBlocksToFetch non-empty blocks out of $totalBlocks blocks")
    remoteRequests
  }

  private[this] def fetchLocalBlocks() {
    // Get the local blocks while remote blocks are being fetched. Note that it's okay to do
    // these all at once because they will just memory-map some files, so they won't consume
    // any memory that might exceed our maxBytesInFlight
    for (id <- localBlocks) {
      try {
        shuffleMetrics.localBlocksFetched += 1
        results.put(new FetchResult(
          id, 0, () => blockManager.getLocalShuffleFromDisk(id, serializer).get))
        logDebug("Got local block " + id)
      } catch {
        case e: Exception =>
          logError(s"Error occurred while fetching local blocks", e)
          results.put(new FetchResult(id, -1, null))
          return
      }
    }
  }

  private[this] def initialize(): Unit = {
    // Split local and remote blocks.
    val remoteRequests = splitLocalRemoteBlocks()
    // Add the remote requests into our queue in a random order
    fetchRequests ++= Utils.randomize(remoteRequests)

    // Send out initial requests for blocks, up to our maxBytesInFlight
    while (fetchRequests.nonEmpty &&
      (bytesInFlight == 0 || bytesInFlight + fetchRequests.front.size <= maxBytesInFlight)) {
      sendRequest(fetchRequests.dequeue())
    }

    val numFetches = remoteRequests.size - fetchRequests.size
    logInfo("Started " + numFetches + " remote fetches in" + Utils.getUsedTimeMs(startTime))

    // Get Local Blocks
    fetchLocalBlocks()
    logDebug("Got local blocks in " + Utils.getUsedTimeMs(startTime) + " ms")
  }

  override def hasNext: Boolean = numBlocksProcessed < numBlocksToFetch

  override def next(): (BlockId, Option[Iterator[Any]]) = {
    numBlocksProcessed += 1
    val startFetchWait = System.currentTimeMillis()
    val result = results.take()
    val stopFetchWait = System.currentTimeMillis()
    shuffleMetrics.fetchWaitTime += (stopFetchWait - startFetchWait)
    if (!result.failed) {
      bytesInFlight -= result.size
    }
    // Send fetch requests up to maxBytesInFlight
    while (fetchRequests.nonEmpty &&
      (bytesInFlight == 0 || bytesInFlight + fetchRequests.front.size <= maxBytesInFlight)) {
      sendRequest(fetchRequests.dequeue())
    }
    (result.blockId, if (result.failed) None else Some(result.deserialize()))
  }
}


private[storage]
object ShuffleBlockFetcherIterator {

  /**
   * A request to fetch blocks from a remote BlockManager.
   * @param address remote BlockManager to fetch from.
   * @param blocks Sequence of tuple, where the first element is the block id,
   *               and the second element is the estimated size, used to calculate bytesInFlight.
   */
  class FetchRequest(val address: BlockManagerId, val blocks: Seq[(BlockId, Long)]) {
    val size = blocks.map(_._2).sum
  }

  /**
   * Result of a fetch from a remote block. A failure is represented as size == -1.
   * @param blockId block id
   * @param size estimated size of the block, used to calculate bytesInFlight.
   *             Note that this is NOT the exact bytes.
   * @param deserialize closure to return the result in the form of an Iterator.
   */
  class FetchResult(val blockId: BlockId, val size: Long, val deserialize: () => Iterator[Any]) {
    def failed: Boolean = size == -1
  }
}
