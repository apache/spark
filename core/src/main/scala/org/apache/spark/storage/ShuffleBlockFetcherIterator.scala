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

import java.io.{InputStream, IOException}
import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.CheckedInputStream
import javax.annotation.concurrent.GuardedBy

import scala.collection
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Queue}
import scala.util.{Failure, Success}

import io.netty.util.internal.OutOfDirectMemoryError
import org.apache.commons.io.IOUtils
import org.roaringbitmap.RoaringBitmap

import org.apache.spark.{MapOutputTracker, SparkException, TaskContext}
import org.apache.spark.MapOutputTracker.SHUFFLE_PUSH_MAP_ID
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.SHUFFLE_LOCAL_READ_ENABLE
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.shuffle._
import org.apache.spark.network.shuffle.checksum.{Cause, ShuffleChecksumHelper}
import org.apache.spark.network.util.{NettyUtils, TransportConf}
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.util.{Clock, CompletionIterator, SystemClock, TaskCompletionListener, Utils}

/**
 * An iterator that fetches multiple blocks. For local blocks, it fetches from the local block
 * manager. For remote blocks, it fetches them using the provided BlockTransferService.
 *
 * This creates an iterator of (BlockID, InputStream) tuples so the caller can handle blocks
 * in a pipelined fashion as they are received.
 *
 * The implementation throttles the remote fetches so they don't exceed maxBytesInFlight to avoid
 * using too much memory.
 *
 * @param context [[TaskContext]], used for metrics update
 * @param shuffleClient [[BlockStoreClient]] for fetching remote blocks
 * @param blockManager [[BlockManager]] for reading local blocks
 * @param blocksByAddress list of blocks to fetch grouped by the [[BlockManagerId]].
 *                        For each block we also require two info: 1. the size (in bytes as a long
 *                        field) in order to throttle the memory usage; 2. the mapIndex for this
 *                        block, which indicate the index in the map stage.
 *                        Note that zero-sized blocks are already excluded, which happened in
 *                        [[org.apache.spark.MapOutputTracker.convertMapStatuses]].
 * @param mapOutputTracker [[MapOutputTracker]] for falling back to fetching the original blocks if
 *                         we fail to fetch shuffle chunks when push based shuffle is enabled.
 * @param streamWrapper A function to wrap the returned input stream.
 * @param maxBytesInFlight max size (in bytes) of remote blocks to fetch at any given point.
 * @param maxReqsInFlight max number of remote requests to fetch blocks at any given point.
 * @param maxBlocksInFlightPerAddress max number of shuffle blocks being fetched at any given point
 *                                    for a given remote host:port.
 * @param maxReqSizeShuffleToMem max size (in bytes) of a request that can be shuffled to memory.
 * @param maxAttemptsOnNettyOOM The max number of a block could retry due to Netty OOM before
 *                              throwing the fetch failure.
 * @param detectCorrupt         whether to detect any corruption in fetched blocks.
 * @param checksumEnabled whether the shuffle checksum is enabled. When enabled, Spark will try to
 *                        diagnose the cause of the block corruption.
 * @param checksumAlgorithm the checksum algorithm that is used when calculating the checksum value
 *                         for the block data.
 * @param shuffleMetrics used to report shuffle metrics.
 * @param doBatchFetch fetch continuous shuffle blocks from same executor in batch if the server
 *                     side supports.
 */
private[spark]
final class ShuffleBlockFetcherIterator(
    context: TaskContext,
    shuffleClient: BlockStoreClient,
    blockManager: BlockManager,
    mapOutputTracker: MapOutputTracker,
    blocksByAddress: Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])],
    streamWrapper: (BlockId, InputStream) => InputStream,
    maxBytesInFlight: Long,
    maxReqsInFlight: Int,
    maxBlocksInFlightPerAddress: Int,
    val maxReqSizeShuffleToMem: Long,
    maxAttemptsOnNettyOOM: Int,
    detectCorrupt: Boolean,
    detectCorruptUseExtraMemory: Boolean,
    checksumEnabled: Boolean,
    checksumAlgorithm: String,
    shuffleMetrics: ShuffleReadMetricsReporter,
    doBatchFetch: Boolean,
  clock: Clock = new SystemClock())
  extends Iterator[(BlockId, InputStream)] with DownloadFileManager with Logging {

  import ShuffleBlockFetcherIterator._

  // Make remote requests at most maxBytesInFlight / 5 in length; the reason to keep them
  // smaller than maxBytesInFlight is to allow multiple, parallel fetches from up to 5
  // nodes, rather than blocking on reading output from one node.
  private val targetRemoteRequestSize = math.max(maxBytesInFlight / 5, 1L)

  /**
   * Total number of blocks to fetch.
   */
  private[this] var numBlocksToFetch = 0

  /**
   * The number of blocks processed by the caller. The iterator is exhausted when
   * [[numBlocksProcessed]] == [[numBlocksToFetch]].
   */
  private[this] var numBlocksProcessed = 0

  private[this] val startTimeNs = System.nanoTime()

  /** Host local blocks to fetch, excluding zero-sized blocks. */
  private[this] val hostLocalBlocks = scala.collection.mutable.LinkedHashSet[(BlockId, Int)]()

  /**
   * A queue to hold our results. This turns the asynchronous model provided by
   * [[org.apache.spark.network.BlockTransferService]] into a synchronous model (iterator).
   */
  private[this] val results = new LinkedBlockingQueue[FetchResult]

  /**
   * Current [[FetchResult]] being processed. We track this so we can release the current buffer
   * in case of a runtime exception when processing the current buffer.
   */
  @volatile private[this] var currentResult: SuccessFetchResult = null

  /**
   * Queue of fetch requests to issue; we'll pull requests off this gradually to make sure that
   * the number of bytes in flight is limited to maxBytesInFlight.
   */
  private[this] val fetchRequests = new Queue[FetchRequest]

  /**
   * Queue of fetch requests which could not be issued the first time they were dequeued. These
   * requests are tried again when the fetch constraints are satisfied.
   */
  private[this] val deferredFetchRequests = new HashMap[BlockManagerId, Queue[FetchRequest]]()

  /** Current bytes in flight from our requests */
  private[this] var bytesInFlight = 0L

  /** Current number of requests in flight */
  private[this] var reqsInFlight = 0

  /** Current number of blocks in flight per host:port */
  private[this] val numBlocksInFlightPerAddress = new HashMap[BlockManagerId, Int]()

  /**
   * Count the retry times for the blocks due to Netty OOM. The block will stop retry if
   * retry times has exceeded the [[maxAttemptsOnNettyOOM]].
   */
  private[this] val blockOOMRetryCounts = new HashMap[String, Int]

  /**
   * The blocks that can't be decompressed successfully, it is used to guarantee that we retry
   * at most once for those corrupted blocks.
   */
  private[this] val corruptedBlocks = mutable.HashSet[BlockId]()

  /**
   * Whether the iterator is still active. If isZombie is true, the callback interface will no
   * longer place fetched blocks into [[results]].
   */
  @GuardedBy("this")
  private[this] var isZombie = false

  /**
   * A set to store the files used for shuffling remote huge blocks. Files in this set will be
   * deleted when cleanup. This is a layer of defensiveness against disk file leaks.
   */
  @GuardedBy("this")
  private[this] val shuffleFilesSet = mutable.HashSet[DownloadFile]()

  private[this] val onCompleteCallback = new ShuffleFetchCompletionListener(this)

  private[this] val pushBasedFetchHelper = new PushBasedFetchHelper(
    this, shuffleClient, blockManager, mapOutputTracker, shuffleMetrics)

  initialize()

  // Decrements the buffer reference count.
  // The currentResult is set to null to prevent releasing the buffer again on cleanup()
  private[storage] def releaseCurrentResultBuffer(): Unit = {
    // Release the current buffer if necessary
    if (currentResult != null) {
      currentResult.buf.release()
    }
    currentResult = null
  }

  override def createTempFile(transportConf: TransportConf): DownloadFile = {
    // we never need to do any encryption or decryption here, regardless of configs, because that
    // is handled at another layer in the code.  When encryption is enabled, shuffle data is written
    // to disk encrypted in the first place, and sent over the network still encrypted.
    new SimpleDownloadFile(
      blockManager.diskBlockManager.createTempLocalBlock()._2, transportConf)
  }

  override def registerTempFileToClean(file: DownloadFile): Boolean = synchronized {
    if (isZombie) {
      false
    } else {
      shuffleFilesSet += file
      true
    }
  }

  /**
   * Mark the iterator as zombie, and release all buffers that haven't been deserialized yet.
   */
  private[storage] def cleanup(): Unit = {
    synchronized {
      isZombie = true
    }
    releaseCurrentResultBuffer()
    // Release buffers in the results queue
    val iter = results.iterator()
    while (iter.hasNext) {
      val result = iter.next()
      result match {
        case SuccessFetchResult(blockId, mapIndex, address, _, buf, _) =>
          if (address != blockManager.blockManagerId) {
            if (pushBasedFetchHelper.isLocalPushMergedBlockAddress(address) ||
              hostLocalBlocks.contains(blockId -> mapIndex)) {
              shuffleMetricsUpdate(blockId, buf, local = true)
            } else {
              shuffleMetricsUpdate(blockId, buf, local = false)
            }
          }
          buf.release()
        case _ =>
      }
    }
    shuffleFilesSet.foreach { file =>
      if (!file.delete()) {
        logWarning(log"Failed to cleanup shuffle fetch temp file ${MDC(PATH, file.path())}")
      }
    }
  }

  private[this] def sendRequest(req: FetchRequest): Unit = {
    logDebug("Sending request for %d blocks (%s) from %s".format(
      req.blocks.size, Utils.bytesToString(req.size), req.address.hostPort))
    bytesInFlight += req.size
    reqsInFlight += 1

    // so we can look up the block info of each blockID
    val infoMap = req.blocks.map {
      case FetchBlockInfo(blockId, size, mapIndex) => (blockId.toString, (size, mapIndex))
    }.toMap
    val remainingBlocks = new HashSet[String]() ++= infoMap.keys
    val deferredBlocks = new ArrayBuffer[String]()
    val blockIds = req.blocks.map(_.blockId.toString)
    val address = req.address
    val requestStartTime = clock.nanoTime()

    @inline def enqueueDeferredFetchRequestIfNecessary(): Unit = {
      if (remainingBlocks.isEmpty && deferredBlocks.nonEmpty) {
        val blocks = deferredBlocks.map { blockId =>
          val (size, mapIndex) = infoMap(blockId)
          FetchBlockInfo(BlockId(blockId), size, mapIndex)
        }
        results.put(DeferFetchRequestResult(FetchRequest(address, blocks)))
        deferredBlocks.clear()
      }
    }

    @inline def updateMergedReqsDuration(wasReqForMergedChunks: Boolean = false): Unit = {
      if (remainingBlocks.isEmpty) {
        val durationMs = TimeUnit.NANOSECONDS.toMillis(clock.nanoTime() - requestStartTime)
        if (wasReqForMergedChunks) {
          shuffleMetrics.incRemoteMergedReqsDuration(durationMs)
        }
        shuffleMetrics.incRemoteReqsDuration(durationMs)
      }
    }

    val blockFetchingListener = new BlockFetchingListener {
      override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
        // Only add the buffer to results queue if the iterator is not zombie,
        // i.e. cleanup() has not been called yet.
        ShuffleBlockFetcherIterator.this.synchronized {
          if (!isZombie) {
            // Increment the ref count because we need to pass this to a different thread.
            // This needs to be released after use.
            buf.retain()
            remainingBlocks -= blockId
            blockOOMRetryCounts.remove(blockId)
            updateMergedReqsDuration(BlockId(blockId).isShuffleChunk)
            results.put(SuccessFetchResult(BlockId(blockId), infoMap(blockId)._2,
              address, infoMap(blockId)._1, buf, remainingBlocks.isEmpty))
            logDebug("remainingBlocks: " + remainingBlocks)
            enqueueDeferredFetchRequestIfNecessary()
          }
        }
        logTrace(s"Got remote block $blockId after ${Utils.getUsedTimeNs(startTimeNs)}")
      }

      override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
        ShuffleBlockFetcherIterator.this.synchronized {
          logError(log"Failed to get block(s) from " +
            log"${MDC(HOST, req.address.host)}:${MDC(PORT, req.address.port)}", e)
          e match {
            // SPARK-27991: Catch the Netty OOM and set the flag `isNettyOOMOnShuffle` (shared among
            // tasks) to true as early as possible. The pending fetch requests won't be sent
            // afterwards until the flag is set to false on:
            // 1) the Netty free memory >= maxReqSizeShuffleToMem
            //    - we'll check this whenever there's a fetch request succeeds.
            // 2) the number of in-flight requests becomes 0
            //    - we'll check this in `fetchUpToMaxBytes` whenever it's invoked.
            // Although Netty memory is shared across multiple modules, e.g., shuffle, rpc, the flag
            // only takes effect for the shuffle due to the implementation simplicity concern.
            // And we'll buffer the consecutive block failures caused by the OOM error until there's
            // no remaining blocks in the current request. Then, we'll package these blocks into
            // a same fetch request for the retry later. In this way, instead of creating the fetch
            // request per block, it would help reduce the concurrent connections and data loads
            // pressure at remote server.
            // Note that catching OOM and do something based on it is only a workaround for
            // handling the Netty OOM issue, which is not the best way towards memory management.
            // We can get rid of it when we find a way to manage Netty's memory precisely.
            case _: OutOfDirectMemoryError
                if blockOOMRetryCounts.getOrElseUpdate(blockId, 0) < maxAttemptsOnNettyOOM =>
              if (!isZombie) {
                val failureTimes = blockOOMRetryCounts(blockId)
                blockOOMRetryCounts(blockId) += 1
                if (isNettyOOMOnShuffle.compareAndSet(false, true)) {
                  // The fetcher can fail remaining blocks in batch for the same error. So we only
                  // log the warning once to avoid flooding the logs.
                  logInfo(log"Block ${MDC(BLOCK_ID, blockId)} has failed " +
                    log"${MDC(FAILURES, failureTimes)} times due to Netty OOM, will retry")
                }
                remainingBlocks -= blockId
                deferredBlocks += blockId
                enqueueDeferredFetchRequestIfNecessary()
              }

            case _ =>
              val block = BlockId(blockId)
              if (block.isShuffleChunk) {
                remainingBlocks -= blockId
                updateMergedReqsDuration(wasReqForMergedChunks = true)
                results.put(FallbackOnPushMergedFailureResult(
                  block, address, infoMap(blockId)._1, remainingBlocks.isEmpty))
              } else {
                results.put(FailureFetchResult(block, infoMap(blockId)._2, address, e))
              }
          }
        }
      }
    }

    // Fetch remote shuffle blocks to disk when the request is too large. Since the shuffle data is
    // already encrypted and compressed over the wire(w.r.t. the related configs), we can just fetch
    // the data and write it to file directly.
    if (req.size > maxReqSizeShuffleToMem) {
      shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
        blockFetchingListener, this)
    } else {
      shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
        blockFetchingListener, null)
    }
  }

  /**
   * This is called from initialize and also from the fallback which is triggered from
   * [[PushBasedFetchHelper]].
   */
  private[this] def partitionBlocksByFetchMode(
      blocksByAddress: Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])],
      localBlocks: mutable.LinkedHashSet[(BlockId, Int)],
      hostLocalBlocksByExecutor:
        mutable.LinkedHashMap[BlockManagerId, collection.Seq[(BlockId, Long, Int)]],
      pushMergedLocalBlocks: mutable.LinkedHashSet[BlockId]): ArrayBuffer[FetchRequest] = {
    logDebug(s"maxBytesInFlight: $maxBytesInFlight, targetRemoteRequestSize: "
      + s"$targetRemoteRequestSize, maxBlocksInFlightPerAddress: $maxBlocksInFlightPerAddress")

    // Partition to local, host-local, push-merged-local, remote (includes push-merged-remote)
    // blocks.Remote blocks are further split into FetchRequests of size at most maxBytesInFlight
    // in order to limit the amount of data in flight

    val localReadEnable = blockManager.conf.get(SHUFFLE_LOCAL_READ_ENABLE)
    val collectedRemoteRequests = new ArrayBuffer[FetchRequest]
    var localBlockBytes = 0L
    var hostLocalBlockBytes = 0L
    var numHostLocalBlocks = 0
    var pushMergedLocalBlockBytes = 0L
    val prevNumBlocksToFetch = numBlocksToFetch

    val fallback = FallbackStorage.FALLBACK_BLOCK_MANAGER_ID.executorId
    val localExecIds = Set(blockManager.blockManagerId.executorId, fallback)
    for ((address, blockInfos) <- blocksByAddress) {
      checkBlockSizes(blockInfos)
      if (pushBasedFetchHelper.isPushMergedShuffleBlockAddress(address)) {
        // These are push-merged blocks or shuffle chunks of these blocks.
        if (address.host == blockManager.blockManagerId.host) {
          numBlocksToFetch += blockInfos.size
          pushMergedLocalBlocks ++= blockInfos.map(_._1)
          pushMergedLocalBlockBytes += blockInfos.map(_._2).sum
        } else {
          collectFetchRequests(address, blockInfos, collectedRemoteRequests)
        }
      } else if (localExecIds.contains(address.executorId)) {
        val mergedBlockInfos = mergeContinuousShuffleBlockIdsIfNeeded(
          blockInfos.map(info => FetchBlockInfo(info._1, info._2, info._3)), doBatchFetch)
        numBlocksToFetch += mergedBlockInfos.size
        localBlocks ++= mergedBlockInfos.map(info => (info.blockId, info.mapIndex))
        localBlockBytes += mergedBlockInfos.map(_.size).sum
      } else if (blockManager.hostLocalDirManager.isDefined &&
        address.host == blockManager.blockManagerId.host && localReadEnable) {
        val mergedBlockInfos = mergeContinuousShuffleBlockIdsIfNeeded(
          blockInfos.map(info => FetchBlockInfo(info._1, info._2, info._3)), doBatchFetch)
        numBlocksToFetch += mergedBlockInfos.size
        val blocksForAddress =
          mergedBlockInfos.map(info => (info.blockId, info.size, info.mapIndex))
        hostLocalBlocksByExecutor += address -> blocksForAddress
        numHostLocalBlocks += blocksForAddress.size
        hostLocalBlockBytes += mergedBlockInfos.map(_.size).sum
      } else {
        val (_, timeCost) = Utils.timeTakenMs[Unit] {
          collectFetchRequests(address, blockInfos, collectedRemoteRequests)
        }
        logDebug(s"Collected remote fetch requests for $address in $timeCost ms")
      }
    }
    val (remoteBlockBytes, numRemoteBlocks) =
      collectedRemoteRequests.foldLeft((0L, 0))((x, y) => (x._1 + y.size, x._2 + y.blocks.size))
    val totalBytes = localBlockBytes + remoteBlockBytes + hostLocalBlockBytes +
      pushMergedLocalBlockBytes
    val blocksToFetchCurrentIteration = numBlocksToFetch - prevNumBlocksToFetch
    assert(blocksToFetchCurrentIteration == localBlocks.size +
      numHostLocalBlocks + numRemoteBlocks + pushMergedLocalBlocks.size,
        s"The number of non-empty blocks $blocksToFetchCurrentIteration doesn't equal to the sum " +
        s"of the number of local blocks ${localBlocks.size} + " +
        s"the number of host-local blocks ${numHostLocalBlocks} " +
        s"the number of push-merged-local blocks ${pushMergedLocalBlocks.size} " +
        s"+ the number of remote blocks ${numRemoteBlocks} ")
    logInfo(
      log"Getting ${MDC(NUM_BLOCKS, blocksToFetchCurrentIteration)} " +
      log"(${MDC(TOTAL_SIZE, Utils.bytesToString(totalBytes))}) non-empty blocks including " +
      log"${MDC(NUM_LOCAL_BLOCKS, localBlocks.size)} " +
      log"(${MDC(LOCAL_BLOCKS_SIZE, Utils.bytesToString(localBlockBytes))}) local and " +
      log"${MDC(NUM_HOST_LOCAL_BLOCKS, numHostLocalBlocks)} " +
      log"(${MDC(HOST_LOCAL_BLOCKS_SIZE, Utils.bytesToString(hostLocalBlockBytes))}) " +
      log"host-local and ${MDC(NUM_PUSH_MERGED_LOCAL_BLOCKS, pushMergedLocalBlocks.size)} " +
      log"(${MDC(PUSH_MERGED_LOCAL_BLOCKS_SIZE, Utils.bytesToString(pushMergedLocalBlockBytes))})" +
      log" push-merged-local and ${MDC(NUM_REMOTE_BLOCKS, numRemoteBlocks)} " +
      log"(${MDC(REMOTE_BLOCKS_SIZE, Utils.bytesToString(remoteBlockBytes))}) remote blocks")
    this.hostLocalBlocks ++= hostLocalBlocksByExecutor.values
      .flatMap { infos => infos.map(info => (info._1, info._3)) }
    collectedRemoteRequests
  }

  private def createFetchRequest(
      blocks: collection.Seq[FetchBlockInfo],
      address: BlockManagerId,
      forMergedMetas: Boolean): FetchRequest = {
    logDebug(s"Creating fetch request of ${blocks.map(_.size).sum} at $address "
      + s"with ${blocks.size} blocks")
    FetchRequest(address, blocks, forMergedMetas)
  }

  private def createFetchRequests(
      curBlocks: collection.Seq[FetchBlockInfo],
      address: BlockManagerId,
      isLast: Boolean,
      collectedRemoteRequests: ArrayBuffer[FetchRequest],
      enableBatchFetch: Boolean,
      forMergedMetas: Boolean = false): ArrayBuffer[FetchBlockInfo] = {
    val mergedBlocks = mergeContinuousShuffleBlockIdsIfNeeded(curBlocks, enableBatchFetch)
    numBlocksToFetch += mergedBlocks.size
    val retBlocks = new ArrayBuffer[FetchBlockInfo]
    if (mergedBlocks.length <= maxBlocksInFlightPerAddress) {
      collectedRemoteRequests += createFetchRequest(mergedBlocks, address, forMergedMetas)
    } else {
      mergedBlocks.grouped(maxBlocksInFlightPerAddress).foreach { blocks =>
        if (blocks.length == maxBlocksInFlightPerAddress || isLast) {
          collectedRemoteRequests += createFetchRequest(blocks, address, forMergedMetas)
        } else {
          // The last group does not exceed `maxBlocksInFlightPerAddress`. Put it back
          // to `curBlocks`.
          retBlocks ++= blocks
          numBlocksToFetch -= blocks.size
        }
      }
    }
    retBlocks
  }

  private def collectFetchRequests(
      address: BlockManagerId,
      blockInfos: collection.Seq[(BlockId, Long, Int)],
      collectedRemoteRequests: ArrayBuffer[FetchRequest]): Unit = {
    val iterator = blockInfos.iterator
    var curRequestSize = 0L
    var curBlocks = new ArrayBuffer[FetchBlockInfo]()

    while (iterator.hasNext) {
      val (blockId, size, mapIndex) = iterator.next()
      curBlocks += FetchBlockInfo(blockId, size, mapIndex)
      curRequestSize += size
      blockId match {
        // Either all blocks are push-merged blocks, shuffle chunks, or original blocks.
        // Based on these types, we decide to do batch fetch and create FetchRequests with
        // forMergedMetas set.
        case ShuffleBlockChunkId(_, _, _, _) =>
          if (curRequestSize >= targetRemoteRequestSize ||
            curBlocks.size >= maxBlocksInFlightPerAddress) {
            curBlocks = createFetchRequests(curBlocks, address, isLast = false,
              collectedRemoteRequests, enableBatchFetch = false)
            curRequestSize = curBlocks.map(_.size).sum
          }
        case ShuffleMergedBlockId(_, _, _) =>
          if (curBlocks.size >= maxBlocksInFlightPerAddress) {
            curBlocks = createFetchRequests(curBlocks, address, isLast = false,
              collectedRemoteRequests, enableBatchFetch = false, forMergedMetas = true)
          }
        case _ =>
          // For batch fetch, the actual block in flight should count for merged block.
          val mayExceedsMaxBlocks = !doBatchFetch && curBlocks.size >= maxBlocksInFlightPerAddress
          if (curRequestSize >= targetRemoteRequestSize || mayExceedsMaxBlocks) {
            curBlocks = createFetchRequests(curBlocks, address, isLast = false,
              collectedRemoteRequests, doBatchFetch)
            curRequestSize = curBlocks.map(_.size).sum
          }
      }
    }
    // Add in the final request
    if (curBlocks.nonEmpty) {
      val (enableBatchFetch, forMergedMetas) = {
        curBlocks.head.blockId match {
          case ShuffleBlockChunkId(_, _, _, _) => (false, false)
          case ShuffleMergedBlockId(_, _, _) => (false, true)
          case _ => (doBatchFetch, false)
        }
      }
      createFetchRequests(curBlocks, address, isLast = true, collectedRemoteRequests,
        enableBatchFetch = enableBatchFetch, forMergedMetas = forMergedMetas)
    }
  }

  private def assertPositiveBlockSize(blockId: BlockId, blockSize: Long): Unit = {
    if (blockSize < 0) {
      throw BlockException(blockId, "Negative block size " + size)
    } else if (blockSize == 0) {
      throw BlockException(blockId, "Zero-sized blocks should be excluded.")
    }
  }

  private def checkBlockSizes(blockInfos: collection.Seq[(BlockId, Long, Int)]): Unit = {
    blockInfos.foreach { case (blockId, size, _) => assertPositiveBlockSize(blockId, size) }
  }

  /**
   * Fetch the local blocks while we are fetching remote blocks. This is ok because
   * `ManagedBuffer`'s memory is allocated lazily when we create the input stream, so all we
   * track in-memory are the ManagedBuffer references themselves.
   */
  private[this] def fetchLocalBlocks(
      localBlocks: mutable.LinkedHashSet[(BlockId, Int)]): Unit = {
    logDebug(s"Start fetching local blocks: ${localBlocks.mkString(", ")}")
    val iter = localBlocks.iterator
    while (iter.hasNext) {
      val (blockId, mapIndex) = iter.next()
      try {
        val buf = blockManager.getLocalBlockData(blockId)
        shuffleMetrics.incLocalBlocksFetched(1)
        shuffleMetrics.incLocalBytesRead(buf.size)
        buf.retain()
        results.put(SuccessFetchResult(blockId, mapIndex, blockManager.blockManagerId,
          buf.size(), buf, false))
      } catch {
        // If we see an exception, stop immediately.
        case e: Exception =>
          e match {
            // ClosedByInterruptException is an excepted exception when kill task,
            // don't log the exception stack trace to avoid confusing users.
            // See: SPARK-28340
            case ce: ClosedByInterruptException =>
              logError(
                log"Error occurred while fetching local blocks, ${MDC(ERROR, ce.getMessage)}")
            case ex: Exception => logError("Error occurred while fetching local blocks", ex)
          }
          results.put(FailureFetchResult(blockId, mapIndex, blockManager.blockManagerId, e))
          return
      }
    }
  }

  private[this] def fetchHostLocalBlock(
      blockId: BlockId,
      mapIndex: Int,
      localDirs: Array[String],
      blockManagerId: BlockManagerId): Boolean = {
    try {
      val buf = blockManager.getHostLocalShuffleData(blockId, localDirs)
      buf.retain()
      results.put(SuccessFetchResult(blockId, mapIndex, blockManagerId, buf.size(), buf,
        isNetworkReqDone = false))
      true
    } catch {
      case e: Exception =>
        // If we see an exception, stop immediately.
        logError(s"Error occurred while fetching local blocks", e)
        results.put(FailureFetchResult(blockId, mapIndex, blockManagerId, e))
        false
    }
  }

  /**
   * Fetch the host-local blocks while we are fetching remote blocks. This is ok because
   * `ManagedBuffer`'s memory is allocated lazily when we create the input stream, so all we
   * track in-memory are the ManagedBuffer references themselves.
   */
  private[this] def fetchHostLocalBlocks(
      hostLocalDirManager: HostLocalDirManager,
      hostLocalBlocksByExecutor:
        mutable.LinkedHashMap[BlockManagerId, collection.Seq[(BlockId, Long, Int)]]):
    Unit = {
    val cachedDirsByExec = hostLocalDirManager.getCachedHostLocalDirs
    val (hostLocalBlocksWithCachedDirs, hostLocalBlocksWithMissingDirs) = {
      val (hasCache, noCache) = hostLocalBlocksByExecutor.partition { case (hostLocalBmId, _) =>
        cachedDirsByExec.contains(hostLocalBmId.executorId)
      }
      (hasCache.toMap, noCache.toMap)
    }

    if (hostLocalBlocksWithMissingDirs.nonEmpty) {
      logDebug(s"Asynchronous fetching host-local blocks without cached executors' dir: " +
        s"${hostLocalBlocksWithMissingDirs.mkString(", ")}")

      // If the external shuffle service is enabled, we'll fetch the local directories for
      // multiple executors from the external shuffle service, which located at the same host
      // with the executors, in once. Otherwise, we'll fetch the local directories from those
      // executors directly one by one. The fetch requests won't be too much since one host is
      // almost impossible to have many executors at the same time practically.
      val dirFetchRequests = if (blockManager.externalShuffleServiceEnabled) {
        val host = blockManager.blockManagerId.host
        val port = blockManager.externalShuffleServicePort
        Seq((host, port, hostLocalBlocksWithMissingDirs.keys.toArray))
      } else {
        hostLocalBlocksWithMissingDirs.keys.map(bmId => (bmId.host, bmId.port, Array(bmId))).toSeq
      }

      dirFetchRequests.foreach { case (host, port, bmIds) =>
        hostLocalDirManager.getHostLocalDirs(host, port, bmIds.map(_.executorId)) {
          case Success(dirsByExecId) =>
            fetchMultipleHostLocalBlocks(
              hostLocalBlocksWithMissingDirs.filter { case (k, _) => bmIds.contains(k) },
              dirsByExecId,
              cached = false)

          case Failure(throwable) =>
            logError("Error occurred while fetching host local blocks", throwable)
            val bmId = bmIds.head
            val blockInfoSeq = hostLocalBlocksWithMissingDirs(bmId)
            val (blockId, _, mapIndex) = blockInfoSeq.head
            results.put(FailureFetchResult(blockId, mapIndex, bmId, throwable))
        }
      }
    }

    if (hostLocalBlocksWithCachedDirs.nonEmpty) {
      logDebug(s"Synchronous fetching host-local blocks with cached executors' dir: " +
          s"${hostLocalBlocksWithCachedDirs.mkString(", ")}")
      fetchMultipleHostLocalBlocks(hostLocalBlocksWithCachedDirs, cachedDirsByExec, cached = true)
    }
  }

  private def fetchMultipleHostLocalBlocks(
      bmIdToBlocks: Map[BlockManagerId, collection.Seq[(BlockId, Long, Int)]],
      localDirsByExecId: Map[String, Array[String]],
      cached: Boolean): Unit = {
    // We use `forall` because once there's a failed block fetch, `fetchHostLocalBlock` will put
    // a `FailureFetchResult` immediately to the `results`. So there's no reason to fetch the
    // remaining blocks.
    val allFetchSucceeded = bmIdToBlocks.forall { case (bmId, blockInfos) =>
      blockInfos.forall { case (blockId, _, mapIndex) =>
        fetchHostLocalBlock(blockId, mapIndex, localDirsByExecId(bmId.executorId), bmId)
      }
    }
    if (allFetchSucceeded) {
      logDebug(s"Got host-local blocks from ${bmIdToBlocks.keys.mkString(", ")} " +
        s"(${if (cached) "with" else "without"} cached executors' dir) " +
        s"in ${Utils.getUsedTimeNs(startTimeNs)}")
    }
  }

  private[this] def initialize(): Unit = {
    // Add a task completion callback (called in both success case and failure case) to cleanup.
    context.addTaskCompletionListener(onCompleteCallback)
    // Local blocks to fetch, excluding zero-sized blocks.
    val localBlocks = mutable.LinkedHashSet[(BlockId, Int)]()
    val hostLocalBlocksByExecutor =
      mutable.LinkedHashMap[BlockManagerId, collection.Seq[(BlockId, Long, Int)]]()
    val pushMergedLocalBlocks = mutable.LinkedHashSet[BlockId]()
    // Partition blocks by the different fetch modes: local, host-local, push-merged-local and
    // remote blocks.
    val remoteRequests = partitionBlocksByFetchMode(
      blocksByAddress, localBlocks, hostLocalBlocksByExecutor, pushMergedLocalBlocks)
    // Add the remote requests into our queue in a random order
    fetchRequests ++= Utils.randomize(remoteRequests)
    assert ((0 == reqsInFlight) == (0 == bytesInFlight),
      "expected reqsInFlight = 0 but found reqsInFlight = " + reqsInFlight +
      ", expected bytesInFlight = 0 but found bytesInFlight = " + bytesInFlight)

    // Send out initial requests for blocks, up to our maxBytesInFlight
    fetchUpToMaxBytes()

    val numDeferredRequest = deferredFetchRequests.values.map(_.size).sum
    val numFetches = remoteRequests.size - fetchRequests.size - numDeferredRequest
    logInfo(log"Started ${MDC(COUNT, numFetches)} remote fetches in " +
      log"${MDC(DURATION, Utils.getUsedTimeNs(startTimeNs))}" +
      (if (numDeferredRequest > 0) log", deferred ${MDC(NUM_REQUESTS, numDeferredRequest)} requests"
      else log""))

    // Get Local Blocks
    fetchLocalBlocks(localBlocks)
    logDebug(s"Got local blocks in ${Utils.getUsedTimeNs(startTimeNs)}")
    // Get host local blocks if any
    fetchAllHostLocalBlocks(hostLocalBlocksByExecutor)
    pushBasedFetchHelper.fetchAllPushMergedLocalBlocks(pushMergedLocalBlocks)
  }

  private def fetchAllHostLocalBlocks(
      hostLocalBlocksByExecutor:
        mutable.LinkedHashMap[BlockManagerId, collection.Seq[(BlockId, Long, Int)]]):
    Unit = {
    if (hostLocalBlocksByExecutor.nonEmpty) {
      blockManager.hostLocalDirManager.foreach(fetchHostLocalBlocks(_, hostLocalBlocksByExecutor))
    }
  }

  private def shuffleMetricsUpdate(
      blockId: BlockId,
      buf: ManagedBuffer,
      local: Boolean): Unit = {
    if (local) {
      shuffleLocalMetricsUpdate(blockId, buf)
    } else {
      shuffleRemoteMetricsUpdate(blockId, buf)
    }
  }

  private def shuffleLocalMetricsUpdate(blockId: BlockId, buf: ManagedBuffer): Unit = {
    blockId match {
      case chunkId: ShuffleBlockChunkId =>
        val chunkCardinality = pushBasedFetchHelper.getShuffleChunkCardinality(chunkId)
        shuffleMetrics.incLocalMergedChunksFetched(1)
        shuffleMetrics.incLocalMergedBlocksFetched(chunkCardinality)
        shuffleMetrics.incLocalMergedBytesRead(buf.size)
        shuffleMetrics.incLocalBlocksFetched(chunkCardinality)
      case _ =>
        shuffleMetrics.incLocalBlocksFetched(1)
    }
    shuffleMetrics.incLocalBytesRead(buf.size)
  }

  private def shuffleRemoteMetricsUpdate(blockId: BlockId, buf: ManagedBuffer): Unit = {
    blockId match {
      case chunkId: ShuffleBlockChunkId =>
        val chunkCardinality = pushBasedFetchHelper.getShuffleChunkCardinality(chunkId)
        shuffleMetrics.incRemoteMergedChunksFetched(1)
        shuffleMetrics.incRemoteMergedBlocksFetched(chunkCardinality)
        shuffleMetrics.incRemoteMergedBytesRead(buf.size)
        shuffleMetrics.incRemoteBlocksFetched(chunkCardinality)
      case _ =>
        shuffleMetrics.incRemoteBlocksFetched(1)
    }
    shuffleMetrics.incRemoteBytesRead(buf.size)
    if (buf.isInstanceOf[FileSegmentManagedBuffer]) {
      shuffleMetrics.incRemoteBytesReadToDisk(buf.size)
    }
  }

  override def hasNext: Boolean = numBlocksProcessed < numBlocksToFetch

  /**
   * Fetches the next (BlockId, InputStream). If a task fails, the ManagedBuffers
   * underlying each InputStream will be freed by the cleanup() method registered with the
   * TaskCompletionListener. However, callers should close() these InputStreams
   * as soon as they are no longer needed, in order to release memory as early as possible.
   *
   * Throws a FetchFailedException if the next block could not be fetched.
   */
  override def next(): (BlockId, InputStream) = {
    if (!hasNext) {
      throw SparkCoreErrors.noSuchElementError()
    }

    numBlocksProcessed += 1

    var result: FetchResult = null
    var input: InputStream = null
    // This's only initialized when shuffle checksum is enabled.
    var checkedIn: CheckedInputStream = null
    var streamCompressedOrEncrypted: Boolean = false
    // Take the next fetched result and try to decompress it to detect data corruption,
    // then fetch it one more time if it's corrupt, throw FailureFetchResult if the second fetch
    // is also corrupt, so the previous stage could be retried.
    // For local shuffle block, throw FailureFetchResult for the first IOException.
    while (result == null) {
      val startFetchWait = System.nanoTime()
      result = results.take()
      val fetchWaitTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startFetchWait)
      shuffleMetrics.incFetchWaitTime(fetchWaitTime)

      result match {
        case SuccessFetchResult(blockId, mapIndex, address, size, buf, isNetworkReqDone) =>
          if (address != blockManager.blockManagerId) {
            if (hostLocalBlocks.contains(blockId -> mapIndex) ||
              pushBasedFetchHelper.isLocalPushMergedBlockAddress(address)) {
              // It is a host local block or a local shuffle chunk
              shuffleMetricsUpdate(blockId, buf, local = true)
            } else {
              numBlocksInFlightPerAddress(address) -= 1
              shuffleMetricsUpdate(blockId, buf, local = false)
              bytesInFlight -= size
            }
          }
          if (isNetworkReqDone) {
            reqsInFlight -= 1
            resetNettyOOMFlagIfPossible(maxReqSizeShuffleToMem)
            logDebug("Number of requests in flight " + reqsInFlight)
          }

          val in = if (buf.size == 0) {
            // We will never legitimately receive a zero-size block. All blocks with zero records
            // have zero size and all zero-size blocks have no records (and hence should never
            // have been requested in the first place). This statement relies on behaviors of the
            // shuffle writers, which are guaranteed by the following test cases:
            //
            // - BypassMergeSortShuffleWriterSuite: "write with some empty partitions"
            // - UnsafeShuffleWriterSuite: "writeEmptyIterator"
            // - DiskBlockObjectWriterSuite: "commit() and close() without ever opening or writing"
            //
            // There is not an explicit test for SortShuffleWriter but the underlying APIs that
            // uses are shared by the UnsafeShuffleWriter (both writers use DiskBlockObjectWriter
            // which returns a zero-size from commitAndGet() in case no records were written
            // since the last call.
            val msg = log"Received a zero-size buffer for block ${MDC(BLOCK_ID, blockId)} " +
              log"from ${MDC(URI, address)} " +
              log"(expectedApproxSize = ${MDC(NUM_BYTES, size)}, " +
              log"isNetworkReqDone=${MDC(IS_NETWORK_REQUEST_DONE, isNetworkReqDone)})"
            if (blockId.isShuffleChunk) {
              // Zero-size block may come from nodes with hardware failures, For shuffle chunks,
              // the original shuffle blocks that belong to that zero-size shuffle chunk is
              // available and we can opt to fallback immediately.
              logWarning(msg)
              pushBasedFetchHelper.initiateFallbackFetchForPushMergedBlock(blockId, address)
              shuffleMetrics.incCorruptMergedBlockChunks(1)
              // Set result to null to trigger another iteration of the while loop to get either.
              result = null
              null
            } else {
              throwFetchFailedException(blockId, mapIndex, address, new IOException(msg.message))
            }
          } else {
            try {
              val bufIn = buf.createInputStream()
              if (checksumEnabled) {
                val checksum = ShuffleChecksumHelper.getChecksumByAlgorithm(checksumAlgorithm)
                checkedIn = new CheckedInputStream(bufIn, checksum)
                checkedIn
              } else {
                bufIn
              }
            } catch {
              // The exception could only be throwed by local shuffle block
              case e: IOException =>
                assert(buf.isInstanceOf[FileSegmentManagedBuffer])
                e match {
                  case ce: ClosedByInterruptException =>
                    lazy val error = MDC(ERROR, ce.getMessage)
                    logError(log"Failed to create input stream from local block, $error")
                  case e: IOException =>
                    logError("Failed to create input stream from local block", e)
                }
                buf.release()
                if (blockId.isShuffleChunk) {
                  pushBasedFetchHelper.initiateFallbackFetchForPushMergedBlock(blockId, address)
                  // Set result to null to trigger another iteration of the while loop to get
                  // either.
                  result = null
                  null
                } else {
                  throwFetchFailedException(blockId, mapIndex, address, e)
                }
            }
          }

          if (in != null) {
            try {
              input = streamWrapper(blockId, in)
              // If the stream is compressed or wrapped, then we optionally decompress/unwrap the
              // first maxBytesInFlight/3 bytes into memory, to check for corruption in that portion
              // of the data. But even if 'detectCorruptUseExtraMemory' configuration is off, or if
              // the corruption is later, we'll still detect the corruption later in the stream.
              streamCompressedOrEncrypted = !input.eq(in)
              if (streamCompressedOrEncrypted && detectCorruptUseExtraMemory) {
                // TODO: manage the memory used here, and spill it into disk in case of OOM.
                input = Utils.copyStreamUpTo(input, maxBytesInFlight / 3)
              }
            } catch {
              case e: IOException =>
                // When shuffle checksum is enabled, for a block that is corrupted twice,
                // we'd calculate the checksum of the block by consuming the remaining data
                // in the buf. So, we should release the buf later.
                if (!(checksumEnabled && corruptedBlocks.contains(blockId))) {
                  buf.release()
                }

                if (blockId.isShuffleChunk) {
                  shuffleMetrics.incCorruptMergedBlockChunks(1)
                  // TODO (SPARK-36284): Add shuffle checksum support for push-based shuffle
                  // Retrying a corrupt block may result again in a corrupt block. For shuffle
                  // chunks, we opt to fallback on the original shuffle blocks that belong to that
                  // corrupt shuffle chunk immediately instead of retrying to fetch the corrupt
                  // chunk. This also makes the code simpler because the chunkMeta corresponding to
                  // a shuffle chunk is always removed from chunksMetaMap whenever a shuffle chunk
                  // gets processed. If we try to re-fetch a corrupt shuffle chunk, then it has to
                  // be added back to the chunksMetaMap.
                  pushBasedFetchHelper.initiateFallbackFetchForPushMergedBlock(blockId, address)
                  // Set result to null to trigger another iteration of the while loop.
                  result = null
                } else if (buf.isInstanceOf[FileSegmentManagedBuffer]) {
                  throwFetchFailedException(blockId, mapIndex, address, e)
                } else if (corruptedBlocks.contains(blockId)) {
                  // It's the second time this block is detected corrupted
                  if (checksumEnabled) {
                    // Diagnose the cause of data corruption if shuffle checksum is enabled
                    val diagnosisResponse = diagnoseCorruption(checkedIn, address, blockId)
                    buf.release()
                    logError(diagnosisResponse)
                    throwFetchFailedException(
                      blockId, mapIndex, address, e, Some(diagnosisResponse))
                  } else {
                    throwFetchFailedException(blockId, mapIndex, address, e)
                  }
                } else {
                  // It's the first time this block is detected corrupted
                  logWarning(log"got an corrupted block ${MDC(BLOCK_ID, blockId)} " +
                    log"from ${MDC(URI, address)}, fetch again", e)
                  corruptedBlocks += blockId
                  fetchRequests += FetchRequest(
                    address, Array(FetchBlockInfo(blockId, size, mapIndex)))
                  result = null
                }
            } finally {
              if (blockId.isShuffleChunk) {
                pushBasedFetchHelper.removeChunk(blockId.asInstanceOf[ShuffleBlockChunkId])
              }
              // TODO: release the buf here to free memory earlier
              if (input == null) {
                // Close the underlying stream if there was an issue in wrapping the stream using
                // streamWrapper
                in.close()
              }
            }
          }

        case FailureFetchResult(blockId, mapIndex, address, e) =>
          var errorMsg: String = null
          if (e.isInstanceOf[OutOfDirectMemoryError]) {
            val logMessage = log"Block ${MDC(BLOCK_ID, blockId)} fetch failed after " +
              log"${MDC(MAX_ATTEMPTS, maxAttemptsOnNettyOOM)} retries due to Netty OOM"
            logError(logMessage)
            errorMsg = logMessage.message
          }
          throwFetchFailedException(blockId, mapIndex, address, e, Some(errorMsg))

        case DeferFetchRequestResult(request) =>
          val address = request.address
          numBlocksInFlightPerAddress(address) -= request.blocks.size
          bytesInFlight -= request.size
          reqsInFlight -= 1
          logDebug("Number of requests in flight " + reqsInFlight)
          val defReqQueue =
            deferredFetchRequests.getOrElseUpdate(address, new Queue[FetchRequest]())
          defReqQueue.enqueue(request)
          result = null

        case FallbackOnPushMergedFailureResult(blockId, address, size, isNetworkReqDone) =>
          // We get this result in 3 cases:
          // 1. Failure to fetch the data of a remote shuffle chunk. In this case, the
          //    blockId is a ShuffleBlockChunkId.
          // 2. Failure to read the push-merged-local meta. In this case, the blockId is
          //    ShuffleBlockId.
          // 3. Failure to get the push-merged-local directories from the external shuffle service.
          //    In this case, the blockId is ShuffleBlockId.
          if (pushBasedFetchHelper.isRemotePushMergedBlockAddress(address)) {
            numBlocksInFlightPerAddress(address) -= 1
            bytesInFlight -= size
          }
          if (isNetworkReqDone) {
            reqsInFlight -= 1
            logDebug("Number of requests in flight " + reqsInFlight)
          }
          pushBasedFetchHelper.initiateFallbackFetchForPushMergedBlock(blockId, address)
          // Set result to null to trigger another iteration of the while loop to get either
          // a SuccessFetchResult or a FailureFetchResult.
          result = null

          case PushMergedLocalMetaFetchResult(
            shuffleId, shuffleMergeId, reduceId, bitmaps, localDirs) =>
            // Fetch push-merged-local shuffle block data as multiple shuffle chunks
            val shuffleBlockId = ShuffleMergedBlockId(shuffleId, shuffleMergeId, reduceId)
            try {
              val bufs: Seq[ManagedBuffer] = blockManager.getLocalMergedBlockData(shuffleBlockId,
                localDirs)
              // Since the request for local block meta completed successfully, numBlocksToFetch
              // is decremented.
              numBlocksToFetch -= 1
              // Update total number of blocks to fetch, reflecting the multiple local shuffle
              // chunks.
              numBlocksToFetch += bufs.size
              bufs.zipWithIndex.foreach { case (buf, chunkId) =>
                buf.retain()
                val shuffleChunkId = ShuffleBlockChunkId(shuffleId, shuffleMergeId, reduceId,
                  chunkId)
                pushBasedFetchHelper.addChunk(shuffleChunkId, bitmaps(chunkId))
                results.put(SuccessFetchResult(shuffleChunkId, SHUFFLE_PUSH_MAP_ID,
                  pushBasedFetchHelper.localShuffleMergerBlockMgrId, buf.size(), buf,
                  isNetworkReqDone = false))
              }
            } catch {
              case e: Exception =>
                // If we see an exception with reading push-merged-local index file, we fallback
                // to fetch the original blocks. We do not report block fetch failure
                // and will continue with the remaining local block read.
                logWarning("Error occurred while reading push-merged-local index, " +
                  "prepare to fetch the original blocks", e)
                pushBasedFetchHelper.initiateFallbackFetchForPushMergedBlock(
                  shuffleBlockId, pushBasedFetchHelper.localShuffleMergerBlockMgrId)
            }
            result = null

        case PushMergedRemoteMetaFetchResult(
          shuffleId, shuffleMergeId, reduceId, blockSize, bitmaps, address) =>
          // The original meta request is processed so we decrease numBlocksToFetch and
          // numBlocksInFlightPerAddress by 1. We will collect new shuffle chunks request and the
          // count of this is added to numBlocksToFetch in collectFetchReqsFromMergedBlocks.
          numBlocksInFlightPerAddress(address) -= 1
          numBlocksToFetch -= 1
          val blocksToFetch = pushBasedFetchHelper.createChunkBlockInfosFromMetaResponse(
            shuffleId, shuffleMergeId, reduceId, blockSize, bitmaps)
          val additionalRemoteReqs = new ArrayBuffer[FetchRequest]
          collectFetchRequests(address, blocksToFetch.toSeq, additionalRemoteReqs)
          fetchRequests ++= additionalRemoteReqs
          // Set result to null to force another iteration.
          result = null

        case PushMergedRemoteMetaFailedFetchResult(
          shuffleId, shuffleMergeId, reduceId, address) =>
          // The original meta request failed so we decrease numBlocksInFlightPerAddress by 1.
          numBlocksInFlightPerAddress(address) -= 1
          // If we fail to fetch the meta of a push-merged block, we fall back to fetching the
          // original blocks.
          pushBasedFetchHelper.initiateFallbackFetchForPushMergedBlock(
            ShuffleMergedBlockId(shuffleId, shuffleMergeId, reduceId), address)
          // Set result to null to force another iteration.
          result = null
      }

      // Send fetch requests up to maxBytesInFlight
      fetchUpToMaxBytes()
    }

    currentResult = result.asInstanceOf[SuccessFetchResult]
    (currentResult.blockId,
      new BufferReleasingInputStream(
        input,
        this,
        currentResult.blockId,
        currentResult.mapIndex,
        currentResult.address,
        detectCorrupt && streamCompressedOrEncrypted,
        currentResult.isNetworkReqDone,
        Option(checkedIn)))
  }

  /**
   * Get the suspect corruption cause for the corrupted block. It should be only invoked
   * when checksum is enabled and corruption was detected at least once.
   *
   * This will firstly consume the rest of stream of the corrupted block to calculate the
   * checksum of the block. Then, it will raise a synchronized RPC call along with the
   * checksum to ask the server(where the corrupted block is fetched from) to diagnose the
   * cause of corruption and return it.
   *
   * Any exception raised during the process will result in the [[Cause.UNKNOWN_ISSUE]] of the
   * corruption cause since corruption diagnosis is only a best effort.
   *
   * @param checkedIn the [[CheckedInputStream]] which is used to calculate the checksum.
   * @param address the address where the corrupted block is fetched from.
   * @param blockId the blockId of the corrupted block.
   * @return The corruption diagnosis response for different causes.
   */
  private[storage] def diagnoseCorruption(
      checkedIn: CheckedInputStream,
      address: BlockManagerId,
      blockId: BlockId): String = {
    logInfo("Start corruption diagnosis.")
    blockId match {
      case shuffleBlock: ShuffleBlockId =>
        val startTimeNs = System.nanoTime()
        val buffer = new Array[Byte](ShuffleChecksumHelper.CHECKSUM_CALCULATION_BUFFER)
        // consume the remaining data to calculate the checksum
        var cause: Cause = null
        try {
          while (checkedIn.read(buffer) != -1) {}
          val checksum = checkedIn.getChecksum.getValue
          cause = shuffleClient.diagnoseCorruption(address.host, address.port, address.executorId,
            shuffleBlock.shuffleId, shuffleBlock.mapId, shuffleBlock.reduceId, checksum,
            checksumAlgorithm)
        } catch {
          case e: Exception =>
            logWarning("Unable to diagnose the corruption cause of the corrupted block", e)
            cause = Cause.UNKNOWN_ISSUE
        }
        val duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)
        val diagnosisResponse = cause match {
          case Cause.UNSUPPORTED_CHECKSUM_ALGORITHM =>
            s"Block $blockId is corrupted but corruption diagnosis failed due to " +
              s"unsupported checksum algorithm: $checksumAlgorithm"

          case Cause.CHECKSUM_VERIFY_PASS =>
            s"Block $blockId is corrupted but checksum verification passed"

          case Cause.UNKNOWN_ISSUE =>
            s"Block $blockId is corrupted but the cause is unknown"

          case otherCause =>
            s"Block $blockId is corrupted due to $otherCause"
        }
        logInfo(log"Finished corruption diagnosis in ${MDC(DURATION, duration)} ms. " +
          log"${MDC(STATUS, diagnosisResponse)}")
        diagnosisResponse
      case shuffleBlockChunk: ShuffleBlockChunkId =>
        // TODO SPARK-36284 Add shuffle checksum support for push-based shuffle
        logWarning(log"BlockChunk ${MDC(SHUFFLE_BLOCK_INFO, shuffleBlockChunk)} " +
          log"is corrupted but corruption diagnosis is skipped due to lack of shuffle " +
          log"checksum support for push-based shuffle.")
        s"BlockChunk $shuffleBlockChunk is corrupted but corruption " +
          s"diagnosis is skipped due to lack of shuffle checksum support for push-based shuffle."
      case shuffleBlockBatch: ShuffleBlockBatchId =>
        logWarning(log"BlockBatch ${MDC(SHUFFLE_BLOCK_INFO, shuffleBlockBatch)} is corrupted " +
          log"but corruption diagnosis is skipped due to lack of shuffle checksum support for " +
          log"ShuffleBlockBatchId")
        s"BlockBatch $shuffleBlockBatch is corrupted but corruption " +
          s"diagnosis is skipped due to lack of shuffle checksum support for ShuffleBlockBatchId"
      case unexpected: BlockId =>
        throw SparkException.internalError(
          s"Unexpected type of BlockId, $unexpected", category = "STORAGE")
    }
  }

  def toCompletionIterator: Iterator[(BlockId, InputStream)] = {
    CompletionIterator[(BlockId, InputStream), this.type](this,
      onCompleteCallback.onComplete(context))
  }

  private def fetchUpToMaxBytes(): Unit = {
    if (isNettyOOMOnShuffle.get()) {
      if (reqsInFlight > 0) {
        // Return immediately if Netty is still OOMed and there're ongoing fetch requests
        return
      } else {
        resetNettyOOMFlagIfPossible(0)
      }
    }

    // Send fetch requests up to maxBytesInFlight. If you cannot fetch from a remote host
    // immediately, defer the request until the next time it can be processed.

    // Process any outstanding deferred fetch requests if possible.
    if (deferredFetchRequests.nonEmpty) {
      for ((remoteAddress, defReqQueue) <- deferredFetchRequests) {
        while (isRemoteBlockFetchable(defReqQueue) &&
            !isRemoteAddressMaxedOut(remoteAddress, defReqQueue.front)) {
          val request = defReqQueue.dequeue()
          logDebug(s"Processing deferred fetch request for $remoteAddress with "
            + s"${request.blocks.length} blocks")
          send(remoteAddress, request)
          if (defReqQueue.isEmpty) {
            deferredFetchRequests -= remoteAddress
          }
        }
      }
    }

    // Process any regular fetch requests if possible.
    while (isRemoteBlockFetchable(fetchRequests)) {
      val request = fetchRequests.dequeue()
      val remoteAddress = request.address
      if (isRemoteAddressMaxedOut(remoteAddress, request)) {
        logDebug(s"Deferring fetch request for $remoteAddress with ${request.blocks.size} blocks")
        val defReqQueue = deferredFetchRequests.getOrElse(remoteAddress, new Queue[FetchRequest]())
        defReqQueue.enqueue(request)
        deferredFetchRequests(remoteAddress) = defReqQueue
      } else {
        send(remoteAddress, request)
      }
    }

    def send(remoteAddress: BlockManagerId, request: FetchRequest): Unit = {
      if (request.forMergedMetas) {
        pushBasedFetchHelper.sendFetchMergedStatusRequest(request)
      } else {
        sendRequest(request)
      }
      numBlocksInFlightPerAddress(remoteAddress) =
        numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size
    }

    def isRemoteBlockFetchable(fetchReqQueue: Queue[FetchRequest]): Boolean = {
      fetchReqQueue.nonEmpty &&
        (bytesInFlight == 0 ||
          (reqsInFlight + 1 <= maxReqsInFlight &&
            bytesInFlight + fetchReqQueue.front.size <= maxBytesInFlight))
    }

    // Checks if sending a new fetch request will exceed the max no. of blocks being fetched from a
    // given remote address.
    def isRemoteAddressMaxedOut(remoteAddress: BlockManagerId, request: FetchRequest): Boolean = {
      numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size >
        maxBlocksInFlightPerAddress
    }
  }

  private[storage] def throwFetchFailedException(
      blockId: BlockId,
      mapIndex: Int,
      address: BlockManagerId,
      e: Throwable,
      message: Option[String] = None) = {
    val msg = message.getOrElse(e.getMessage)
    blockId match {
      case ShuffleBlockId(shufId, mapId, reduceId) =>
        throw SparkCoreErrors.fetchFailedError(address, shufId, mapId, mapIndex, reduceId, msg, e)
      case ShuffleBlockBatchId(shuffleId, mapId, startReduceId, _) =>
        throw SparkCoreErrors.fetchFailedError(address, shuffleId, mapId, mapIndex, startReduceId,
          msg, e)
      case ShuffleBlockChunkId(shuffleId, _, reduceId, _) =>
        throw SparkCoreErrors.fetchFailedError(address, shuffleId,
          SHUFFLE_PUSH_MAP_ID.toLong, SHUFFLE_PUSH_MAP_ID, reduceId, msg, e)
      case _ => throw SparkCoreErrors.failToGetNonShuffleBlockError(blockId, e)
    }
  }

  /**
   * All the below methods are used by [[PushBasedFetchHelper]] to communicate with the iterator
   */
  private[storage] def addToResultsQueue(result: FetchResult): Unit = {
    results.put(result)
  }

  private[storage] def decreaseNumBlocksToFetch(blocksFetched: Int): Unit = {
    numBlocksToFetch -= blocksFetched
  }

  /**
   * Currently used by [[PushBasedFetchHelper]] to fetch fallback blocks when there is a fetch
   * failure related to a push-merged block or shuffle chunk.
   * This is executed by the task thread when the `iterator.next()` is invoked and if that initiates
   * fallback.
   */
  private[storage] def fallbackFetch(
      originalBlocksByAddr:
        Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])]): Unit = {
    val originalLocalBlocks = mutable.LinkedHashSet[(BlockId, Int)]()
    val originalHostLocalBlocksByExecutor =
      mutable.LinkedHashMap[BlockManagerId, collection.Seq[(BlockId, Long, Int)]]()
    val originalMergedLocalBlocks = mutable.LinkedHashSet[BlockId]()
    val originalRemoteReqs = partitionBlocksByFetchMode(originalBlocksByAddr,
      originalLocalBlocks, originalHostLocalBlocksByExecutor, originalMergedLocalBlocks)
    // Add the remote requests into our queue in a random order
    fetchRequests ++= Utils.randomize(originalRemoteReqs)
    logInfo(log"Created ${MDC(NUM_REQUESTS, originalRemoteReqs.size)} fallback remote requests " +
      log"for push-merged")
    // fetch all the fallback blocks that are local.
    fetchLocalBlocks(originalLocalBlocks)
    // Merged local blocks should be empty during fallback
    assert(originalMergedLocalBlocks.isEmpty,
      "There should be zero push-merged blocks during fallback")
    // Some of the fallback local blocks could be host local blocks
    fetchAllHostLocalBlocks(originalHostLocalBlocksByExecutor)
  }

  /**
   * Removes all the pending shuffle chunks that are on the same host and have the same reduceId as
   * the current chunk that had a fetch failure.
   * This is executed by the task thread when the `iterator.next()` is invoked and if that initiates
   * fallback.
   *
   * @return set of all the removed shuffle chunk Ids.
   */
  private[storage] def removePendingChunks(
      failedBlockId: ShuffleBlockChunkId,
      address: BlockManagerId): mutable.HashSet[ShuffleBlockChunkId] = {
    val removedChunkIds = new mutable.HashSet[ShuffleBlockChunkId]()

    def sameShuffleReducePartition(block: BlockId): Boolean = {
      val chunkId = block.asInstanceOf[ShuffleBlockChunkId]
      chunkId.shuffleId == failedBlockId.shuffleId && chunkId.reduceId == failedBlockId.reduceId
    }

    def filterRequests(queue: mutable.Queue[FetchRequest]): Unit = {
      val fetchRequestsToRemove = new mutable.Queue[FetchRequest]()
      fetchRequestsToRemove ++= queue.dequeueAll { req =>
        val firstBlock = req.blocks.head
        firstBlock.blockId.isShuffleChunk && req.address.equals(address) &&
          sameShuffleReducePartition(firstBlock.blockId)
      }
      fetchRequestsToRemove.foreach { _ =>
        removedChunkIds ++=
          fetchRequestsToRemove.flatMap(_.blocks.map(_.blockId.asInstanceOf[ShuffleBlockChunkId]))
      }
    }

    filterRequests(fetchRequests)
    deferredFetchRequests.get(address).foreach { defRequests =>
      filterRequests(defRequests)
      if (defRequests.isEmpty) deferredFetchRequests.remove(address)
    }
    removedChunkIds
  }
}

/**
 * Helper class that ensures a ManagedBuffer is released upon InputStream.close() and
 * also detects stream corruption if streamCompressedOrEncrypted is true
 */
private class BufferReleasingInputStream(
    // This is visible for testing
    private[storage] val delegate: InputStream,
    private val iterator: ShuffleBlockFetcherIterator,
    private val blockId: BlockId,
    private val mapIndex: Int,
    private val address: BlockManagerId,
    private val detectCorruption: Boolean,
    private val isNetworkReqDone: Boolean,
    private val checkedInOpt: Option[CheckedInputStream])
  extends InputStream {
  private[this] var closed = false

  override def read(): Int =
    tryOrFetchFailedException(delegate.read())

  override def close(): Unit = {
    if (!closed) {
      try {
        delegate.close()
        iterator.releaseCurrentResultBuffer()
      } finally {
        // Unset the flag when a remote request finished and free memory is fairly enough.
        if (isNetworkReqDone) {
          ShuffleBlockFetcherIterator.resetNettyOOMFlagIfPossible(iterator.maxReqSizeShuffleToMem)
        }
        closed = true
      }
    }
  }

  override def available(): Int =
    tryOrFetchFailedException(delegate.available())

  override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

  override def skip(n: Long): Long =
    tryOrFetchFailedException(delegate.skip(n))

  override def markSupported(): Boolean = delegate.markSupported()

  override def read(b: Array[Byte]): Int =
    tryOrFetchFailedException(delegate.read(b))

  override def read(b: Array[Byte], off: Int, len: Int): Int =
    tryOrFetchFailedException(delegate.read(b, off, len))

  override def reset(): Unit = tryOrFetchFailedException(delegate.reset())

  /**
   * Execute a block of code that returns a value, close this stream quietly and re-throwing
   * IOException as FetchFailedException when detectCorruption is true. This method is only
   * used by the `available`, `read` and `skip` methods inside `BufferReleasingInputStream`
   * currently.
   */
  private def tryOrFetchFailedException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException if detectCorruption =>
        val diagnosisResponse = checkedInOpt.map { checkedIn =>
          iterator.diagnoseCorruption(checkedIn, address, blockId)
        }
        IOUtils.closeQuietly(this)
        // We'd never retry the block whatever the cause is since the block has been
        // partially consumed by downstream RDDs.
        iterator.throwFetchFailedException(blockId, mapIndex, address, e, diagnosisResponse)
    }
  }
}

/**
 * A listener to be called at the completion of the ShuffleBlockFetcherIterator
 * @param data the ShuffleBlockFetcherIterator to process
 */
private class ShuffleFetchCompletionListener(var data: ShuffleBlockFetcherIterator)
  extends TaskCompletionListener {

  override def onTaskCompletion(context: TaskContext): Unit = {
    if (data != null) {
      data.cleanup()
      // Null out the referent here to make sure we don't keep a reference to this
      // ShuffleBlockFetcherIterator, after we're done reading from it, to let it be
      // collected during GC. Otherwise we can hold metadata on block locations(blocksByAddress)
      data = null
    }
  }

  // Just an alias for onTaskCompletion to avoid confusing
  def onComplete(context: TaskContext): Unit = this.onTaskCompletion(context)
}

private[storage]
object ShuffleBlockFetcherIterator {

  /**
   * A flag which indicates whether the Netty OOM error has raised during shuffle.
   * If true, unless there's no in-flight fetch requests, all the pending shuffle
   * fetch requests will be deferred until the flag is unset (whenever there's a
   * complete fetch request).
   */
  val isNettyOOMOnShuffle = new AtomicBoolean(false)

  def resetNettyOOMFlagIfPossible(freeMemoryLowerBound: Long): Unit = {
    if (isNettyOOMOnShuffle.get() && NettyUtils.freeDirectMemory() >= freeMemoryLowerBound) {
      isNettyOOMOnShuffle.compareAndSet(true, false)
    }
  }

  /**
   * This function is used to merged blocks when doBatchFetch is true. Blocks which have the
   * same `mapId` can be merged into one block batch. The block batch is specified by a range
   * of reduceId, which implies the continuous shuffle blocks that we can fetch in a batch.
   * For example, input blocks like (shuffle_0_0_0, shuffle_0_0_1, shuffle_0_1_0) can be
   * merged into (shuffle_0_0_0_2, shuffle_0_1_0_1), and input blocks like (shuffle_0_0_0_2,
   * shuffle_0_0_2, shuffle_0_0_3) can be merged into (shuffle_0_0_0_4).
   *
   * @param blocks blocks to be merged if possible. May contains already merged blocks.
   * @param doBatchFetch whether to merge blocks.
   * @return the input blocks if doBatchFetch=false, or the merged blocks if doBatchFetch=true.
   */
  def mergeContinuousShuffleBlockIdsIfNeeded(
      blocks: collection.Seq[FetchBlockInfo],
      doBatchFetch: Boolean): collection.Seq[FetchBlockInfo] = {
    val result = if (doBatchFetch) {
      val curBlocks = new ArrayBuffer[FetchBlockInfo]
      val mergedBlockInfo = new ArrayBuffer[FetchBlockInfo]

      def mergeFetchBlockInfo(toBeMerged: ArrayBuffer[FetchBlockInfo]): FetchBlockInfo = {
        val startBlockId = toBeMerged.head.blockId.asInstanceOf[ShuffleBlockId]

        // The last merged block may comes from the input, and we can merge more blocks
        // into it, if the map id is the same.
        def shouldMergeIntoPreviousBatchBlockId =
          mergedBlockInfo.last.blockId.asInstanceOf[ShuffleBlockBatchId].mapId == startBlockId.mapId

        val (startReduceId, size) =
          if (mergedBlockInfo.nonEmpty && shouldMergeIntoPreviousBatchBlockId) {
            // Remove the previous batch block id as we will add a new one to replace it.
            val removed = mergedBlockInfo.remove(mergedBlockInfo.length - 1)
            (removed.blockId.asInstanceOf[ShuffleBlockBatchId].startReduceId,
              removed.size + toBeMerged.map(_.size).sum)
          } else {
            (startBlockId.reduceId, toBeMerged.map(_.size).sum)
          }

        FetchBlockInfo(
          ShuffleBlockBatchId(
            startBlockId.shuffleId,
            startBlockId.mapId,
            startReduceId,
            toBeMerged.last.blockId.asInstanceOf[ShuffleBlockId].reduceId + 1),
          size,
          toBeMerged.head.mapIndex)
      }

      val iter = blocks.iterator
      while (iter.hasNext) {
        val info = iter.next()
        // It's possible that the input block id is already a batch ID. For example, we merge some
        // blocks, and then make fetch requests with the merged blocks according to "max blocks per
        // request". The last fetch request may be too small, and we give up and put the remaining
        // merged blocks back to the input list.
        if (info.blockId.isInstanceOf[ShuffleBlockBatchId]) {
          mergedBlockInfo += info
        } else {
          if (curBlocks.isEmpty) {
            curBlocks += info
          } else {
            val curBlockId = info.blockId.asInstanceOf[ShuffleBlockId]
            val currentMapId = curBlocks.head.blockId.asInstanceOf[ShuffleBlockId].mapId
            if (curBlockId.mapId != currentMapId) {
              mergedBlockInfo += mergeFetchBlockInfo(curBlocks)
              curBlocks.clear()
            }
            curBlocks += info
          }
        }
      }
      if (curBlocks.nonEmpty) {
        mergedBlockInfo += mergeFetchBlockInfo(curBlocks)
      }
      mergedBlockInfo
    } else {
      blocks
    }
    result
  }

  /**
   * The block information to fetch used in FetchRequest.
   * @param blockId block id
   * @param size estimated size of the block. Note that this is NOT the exact bytes.
   *             Size of remote block is used to calculate bytesInFlight.
   * @param mapIndex the mapIndex for this block, which indicate the index in the map stage.
   */
  private[storage] case class FetchBlockInfo(
    blockId: BlockId,
    size: Long,
    mapIndex: Int)

  /**
   * A request to fetch blocks from a remote BlockManager.
   * @param address remote BlockManager to fetch from.
   * @param blocks Sequence of the information for blocks to fetch from the same address.
   * @param forMergedMetas true if this request is for requesting push-merged meta information;
   *                       false if it is for regular or shuffle chunks.
   */
  case class FetchRequest(
      address: BlockManagerId,
      blocks: collection.Seq[FetchBlockInfo],
      forMergedMetas: Boolean = false) {
    val size = blocks.map(_.size).sum
  }

  /**
   * Result of a fetch from a remote block.
   */
  private[storage] sealed trait FetchResult

  /**
   * Result of a fetch from a remote block successfully.
   * @param blockId block id
   * @param mapIndex the mapIndex for this block, which indicate the index in the map stage.
   * @param address BlockManager that the block was fetched from.
   * @param size estimated size of the block. Note that this is NOT the exact bytes.
   *             Size of remote block is used to calculate bytesInFlight.
   * @param buf `ManagedBuffer` for the content.
   * @param isNetworkReqDone Is this the last network request for this host in this fetch request.
   */
  private[storage] case class SuccessFetchResult(
      blockId: BlockId,
      mapIndex: Int,
      address: BlockManagerId,
      size: Long,
      buf: ManagedBuffer,
      isNetworkReqDone: Boolean) extends FetchResult {
    require(buf != null)
    require(size >= 0)
  }

  /**
   * Result of a fetch from a remote block unsuccessfully.
   * @param blockId block id
   * @param mapIndex the mapIndex for this block, which indicate the index in the map stage
   * @param address BlockManager that the block was attempted to be fetched from
   * @param e the failure exception
   */
  private[storage] case class FailureFetchResult(
      blockId: BlockId,
      mapIndex: Int,
      address: BlockManagerId,
      e: Throwable)
    extends FetchResult

  /**
   * Result of a fetch request that should be deferred for some reasons, e.g., Netty OOM
   */
  private[storage]
  case class DeferFetchRequestResult(fetchRequest: FetchRequest) extends FetchResult

  /**
   * Result of an un-successful fetch of either of these:
   * 1) Remote shuffle chunk.
   * 2) Local push-merged block.
   *
   * Instead of treating this as a [[FailureFetchResult]], we fallback to fetch the original blocks.
   *
   * @param blockId block id
   * @param address BlockManager that the push-merged block was attempted to be fetched from
   * @param size size of the block, used to update bytesInFlight.
   * @param isNetworkReqDone Is this the last network request for this host in this fetch
   *                         request. Used to update reqsInFlight.
   */
  private[storage] case class FallbackOnPushMergedFailureResult(blockId: BlockId,
      address: BlockManagerId,
      size: Long,
      isNetworkReqDone: Boolean) extends FetchResult

  /**
   * Result of a successful fetch of meta information for a remote push-merged block.
   *
   * @param shuffleId shuffle id.
   * @param shuffleMergeId shuffleMergeId is used to uniquely identify merging process
   *                       of shuffle by an indeterminate stage attempt.
   * @param reduceId reduce id.
   * @param blockSize size of each push-merged block.
   * @param bitmaps bitmaps for every chunk.
   * @param address BlockManager that the meta was fetched from.
   */
  private[storage] case class PushMergedRemoteMetaFetchResult(
      shuffleId: Int,
      shuffleMergeId: Int,
      reduceId: Int,
      blockSize: Long,
      bitmaps: Array[RoaringBitmap],
      address: BlockManagerId) extends FetchResult

  /**
   * Result of a failure while fetching the meta information for a remote push-merged block.
   *
   * @param shuffleId shuffle id.
   * @param shuffleMergeId shuffleMergeId is used to uniquely identify merging process
   *                       of shuffle by an indeterminate stage attempt.
   * @param reduceId reduce id.
   * @param address BlockManager that the meta was fetched from.
   */
  private[storage] case class PushMergedRemoteMetaFailedFetchResult(
      shuffleId: Int,
      shuffleMergeId: Int,
      reduceId: Int,
      address: BlockManagerId) extends FetchResult

  /**
   * Result of a successful fetch of meta information for a push-merged-local block.
   *
   * @param shuffleId shuffle id.
   * @param shuffleMergeId shuffleMergeId is used to uniquely identify merging process
   *                       of shuffle by an indeterminate stage attempt.
   * @param reduceId reduce id.
   * @param bitmaps bitmaps for every chunk.
   * @param localDirs local directories where the push-merged shuffle files are storedl
   */
  private[storage] case class PushMergedLocalMetaFetchResult(
      shuffleId: Int,
      shuffleMergeId: Int,
      reduceId: Int,
      bitmaps: Array[RoaringBitmap],
      localDirs: Array[String]) extends FetchResult
}
