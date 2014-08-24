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
import org.apache.spark.network.netty.client.{BlockClientListener, LazyInitIterator, ReferenceCountedBuffer}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet
import scala.collection.mutable.Queue
import scala.util.{Failure, Success}

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.executor.ShuffleReadMetrics
import org.apache.spark.network.BufferMessage
import org.apache.spark.network.ConnectionManagerId
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.Utils

/**
 * A block fetcher iterator interface. There are two implementations:
 *
 * BasicBlockFetcherIterator: uses a custom-built NIO communication layer.
 * NettyBlockFetcherIterator: uses Netty (OIO) as the communication layer.
 *
 * Eventually we would like the two to converge and use a single NIO-based communication layer,
 * but extensive tests show that under some circumstances (e.g. large shuffles with lots of cores),
 * NIO would perform poorly and thus the need for the Netty OIO one.
 */

private[storage]
trait BlockFetcherIterator extends Iterator[(BlockId, Option[Iterator[Any]])] with Logging {
  def initialize()
}


private[storage]
object BlockFetcherIterator {

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

  // TODO: Refactor this whole thing to make code more reusable.
  class BasicBlockFetcherIterator(
      private val blockManager: BlockManager,
      val blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])],
      serializer: Serializer,
      readMetrics: ShuffleReadMetrics)
    extends BlockFetcherIterator {

    import blockManager._

    if (blocksByAddress == null) {
      throw new IllegalArgumentException("BlocksByAddress is null")
    }

    // Total number blocks fetched (local + remote). Also number of FetchResults expected
    protected var _numBlocksToFetch = 0

    protected var startTime = System.currentTimeMillis

    // BlockIds for local blocks that need to be fetched. Excludes zero-sized blocks
    protected val localBlocksToFetch = new ArrayBuffer[BlockId]()

    // BlockIds for remote blocks that need to be fetched. Excludes zero-sized blocks
    protected val remoteBlocksToFetch = new HashSet[BlockId]()

    // A queue to hold our results.
    protected val results = new LinkedBlockingQueue[FetchResult]

    // Queue of fetch requests to issue; we'll pull requests off this gradually to make sure that
    // the number of bytes in flight is limited to maxBytesInFlight
    protected val fetchRequests = new Queue[FetchRequest]

    // Current bytes in flight from our requests
    protected var bytesInFlight = 0L

    protected def sendRequest(req: FetchRequest) {
      logDebug("Sending request for %d blocks (%s) from %s".format(
        req.blocks.size, Utils.bytesToString(req.size), req.address.hostPort))
      val cmId = new ConnectionManagerId(req.address.host, req.address.port)
      val blockMessageArray = new BlockMessageArray(req.blocks.map {
        case (blockId, size) => BlockMessage.fromGetBlock(GetBlock(blockId))
      })
      bytesInFlight += req.size
      val sizeMap = req.blocks.toMap  // so we can look up the size of each blockID
      val future = connectionManager.sendMessageReliably(cmId, blockMessageArray.toBufferMessage)
      future.onComplete {
        case Success(message) => {
          val bufferMessage = message.asInstanceOf[BufferMessage]
          val blockMessageArray = BlockMessageArray.fromBufferMessage(bufferMessage)
          for (blockMessage <- blockMessageArray) {
            if (blockMessage.getType != BlockMessage.TYPE_GOT_BLOCK) {
              throw new SparkException(
                "Unexpected message " + blockMessage.getType + " received from " + cmId)
            }
            val blockId = blockMessage.getId
            val networkSize = blockMessage.getData.limit()
            results.put(new FetchResult(blockId, sizeMap(blockId),
              () => dataDeserialize(blockId, blockMessage.getData, serializer)))
            // TODO: NettyBlockFetcherIterator has some race conditions where multiple threads can
            // be incrementing bytes read at the same time (SPARK-2625).
            readMetrics.remoteBytesRead += networkSize
            readMetrics.remoteBlocksFetched += 1
            logDebug("Got remote block " + blockId + " after " + Utils.getUsedTimeMs(startTime))
          }
        }
        case Failure(exception) => {
          logError("Could not get block(s) from " + cmId, exception)
          for ((blockId, size) <- req.blocks) {
            results.put(new FetchResult(blockId, -1, null))
          }
        }
      }
    }

    protected def splitLocalRemoteBlocks(): ArrayBuffer[FetchRequest] = {
      // Make remote requests at most maxBytesInFlight / 5 in length; the reason to keep them
      // smaller than maxBytesInFlight is to allow multiple, parallel fetches from up to 5
      // nodes, rather than blocking on reading output from one node.
      val targetRequestSize = math.max(maxBytesInFlight / 5, 1L)
      logInfo("maxBytesInFlight: " + maxBytesInFlight + ", targetRequestSize: " + targetRequestSize)

      // Split local and remote blocks. Remote blocks are further split into FetchRequests of size
      // at most maxBytesInFlight in order to limit the amount of data in flight.
      val remoteRequests = new ArrayBuffer[FetchRequest]
      var totalBlocks = 0
      for ((address, blockInfos) <- blocksByAddress) {
        totalBlocks += blockInfos.size
        if (address == blockManagerId) {
          // Filter out zero-sized blocks
          localBlocksToFetch ++= blockInfos.filter(_._2 != 0).map(_._1)
          _numBlocksToFetch += localBlocksToFetch.size
        } else {
          val iterator = blockInfos.iterator
          var curRequestSize = 0L
          var curBlocks = new ArrayBuffer[(BlockId, Long)]
          while (iterator.hasNext) {
            val (blockId, size) = iterator.next()
            // Skip empty blocks
            if (size > 0) {
              curBlocks += ((blockId, size))
              remoteBlocksToFetch += blockId
              _numBlocksToFetch += 1
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
          if (!curBlocks.isEmpty) {
            remoteRequests += new FetchRequest(address, curBlocks)
          }
        }
      }
      logInfo("Getting " + _numBlocksToFetch + " non-empty blocks out of " +
        totalBlocks + " blocks")
      remoteRequests
    }

    protected def getLocalBlocks() {
      // Get the local blocks while remote blocks are being fetched. Note that it's okay to do
      // these all at once because they will just memory-map some files, so they won't consume
      // any memory that might exceed our maxBytesInFlight
      for (id <- localBlocksToFetch) {
        try {
          // getLocalFromDisk never return None but throws BlockException
          val iter = getLocalFromDisk(id, serializer).get
          // Pass 0 as size since it's not in flight
          readMetrics.localBlocksFetched += 1
          results.put(new FetchResult(id, 0, () => iter))
          logDebug("Got local block " + id)
        } catch {
          case e: Exception => {
            logError(s"Error occurred while fetching local blocks", e)
            results.put(new FetchResult(id, -1, null))
            return
          }
        }
      }
    }

    override def initialize() {
      // Split local and remote blocks.
      val remoteRequests = splitLocalRemoteBlocks()
      // Add the remote requests into our queue in a random order
      fetchRequests ++= Utils.randomize(remoteRequests)

      // Send out initial requests for blocks, up to our maxBytesInFlight
      while (!fetchRequests.isEmpty &&
        (bytesInFlight == 0 || bytesInFlight + fetchRequests.front.size <= maxBytesInFlight)) {
        sendRequest(fetchRequests.dequeue())
      }

      val numFetches = remoteRequests.size - fetchRequests.size
      logInfo("Started " + numFetches + " remote fetches in" + Utils.getUsedTimeMs(startTime))

      // Get Local Blocks
      startTime = System.currentTimeMillis
      getLocalBlocks()
      logDebug("Got local blocks in " + Utils.getUsedTimeMs(startTime) + " ms")
    }

    // Implementing the Iterator methods with an iterator that reads fetched blocks off the queue
    // as they arrive.
    @volatile protected var resultsGotten = 0

    override def hasNext: Boolean = resultsGotten < _numBlocksToFetch

    override def next(): (BlockId, Option[Iterator[Any]]) = {
      resultsGotten += 1
      val startFetchWait = System.currentTimeMillis()
      val result = results.take()
      val stopFetchWait = System.currentTimeMillis()
      readMetrics.fetchWaitTime += (stopFetchWait - startFetchWait)
      if (! result.failed) bytesInFlight -= result.size
      while (!fetchRequests.isEmpty &&
        (bytesInFlight == 0 || bytesInFlight + fetchRequests.front.size <= maxBytesInFlight)) {
        sendRequest(fetchRequests.dequeue())
      }
      (result.blockId, if (result.failed) None else Some(result.deserialize()))
    }
  }
  // End of BasicBlockFetcherIterator

  class NettyBlockFetcherIterator(
      blockManager: BlockManager,
      blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])],
      serializer: Serializer,
      readMetrics: ShuffleReadMetrics)
    extends BasicBlockFetcherIterator(blockManager, blocksByAddress, serializer, readMetrics) {

    override protected def sendRequest(req: FetchRequest) {
      logDebug("Sending request for %d blocks (%s) from %s".format(
        req.blocks.size, Utils.bytesToString(req.size), req.address.hostPort))
      val cmId = new ConnectionManagerId(req.address.host, req.address.port)

      bytesInFlight += req.size
      val sizeMap = req.blocks.toMap // so we can look up the size of each blockID

      // This could throw a TimeoutException. In that case we will just retry the task.
      val client = blockManager.nettyBlockClientFactory.createClient(
        cmId.host, req.address.nettyPort)
      val blocks = req.blocks.map(_._1.toString)

      client.fetchBlocks(
        blocks,
        new BlockClientListener {
          override def onFetchFailure(blockId: String, errorMsg: String): Unit = {
            logError(s"Could not get block(s) from $cmId with error: $errorMsg")
            for ((blockId, size) <- req.blocks) {
              results.put(new FetchResult(blockId, -1, null))
            }
          }

          override def onFetchSuccess(blockId: String, data: ReferenceCountedBuffer): Unit = {
            // Increment the reference count so the buffer won't be recycled.
            // TODO: This could result in memory leaks when the task is stopped due to exception
            // before the iterator is exhausted.
            data.retain()
            val buf = data.byteBuffer()
            val blockSize = buf.remaining()
            val bid = BlockId(blockId)

            // TODO: remove code duplication between here and BlockManager.dataDeserialization.
            results.put(new FetchResult(bid, sizeMap(bid), () => {
              def createIterator: Iterator[Any] = {
                val stream = blockManager.wrapForCompression(bid, data.inputStream())
                serializer.newInstance().deserializeStream(stream).asIterator
              }
              new LazyInitIterator(createIterator) {
                // Release the buffer when we are done traversing it.
                override def close(): Unit = data.release()
              }
            }))

            readMetrics.synchronized {
              readMetrics.remoteBytesRead += blockSize
              readMetrics.remoteBlocksFetched += 1
            }
            logDebug("Got remote block " + blockId + " after " + Utils.getUsedTimeMs(startTime))
          }
        }
      )
    }
  }
  // End of NettyBlockFetcherIterator
}
