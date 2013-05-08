package spark.storage

import java.nio.ByteBuffer
import java.util.concurrent.LinkedBlockingQueue

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet
import scala.collection.mutable.Queue

import io.netty.buffer.ByteBuf

import spark.Logging
import spark.Utils
import spark.SparkException
import spark.network.BufferMessage
import spark.network.ConnectionManagerId
import spark.network.netty.ShuffleCopier
import spark.serializer.Serializer


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
trait BlockFetcherIterator extends Iterator[(String, Option[Iterator[Any]])]
  with Logging with BlockFetchTracker {
  def initialize()
}


private[storage]
object BlockFetcherIterator {

  // A request to fetch one or more blocks, complete with their sizes
  class FetchRequest(val address: BlockManagerId, val blocks: Seq[(String, Long)]) {
    val size = blocks.map(_._2).sum
  }

  // A result of a fetch. Includes the block ID, size in bytes, and a function to deserialize
  // the block (since we want all deserializaton to happen in the calling thread); can also
  // represent a fetch failure if size == -1.
  class FetchResult(val blockId: String, val size: Long, val deserialize: () => Iterator[Any]) {
    def failed: Boolean = size == -1
  }

  class BasicBlockFetcherIterator(
      private val blockManager: BlockManager,
      val blocksByAddress: Seq[(BlockManagerId, Seq[(String, Long)])],
      serializer: Serializer)
    extends BlockFetcherIterator {

    import blockManager._

    private var _remoteBytesRead = 0l
    private var _remoteFetchTime = 0l
    private var _fetchWaitTime = 0l

    if (blocksByAddress == null) {
      throw new IllegalArgumentException("BlocksByAddress is null")
    }

    protected var _totalBlocks = blocksByAddress.map(_._2.size).sum
    logDebug("Getting " + _totalBlocks + " blocks")
    protected var startTime = System.currentTimeMillis
    protected val localBlockIds = new ArrayBuffer[String]()
    protected val remoteBlockIds = new HashSet[String]()

    // A queue to hold our results.
    protected val results = new LinkedBlockingQueue[FetchResult]

    // Queue of fetch requests to issue; we'll pull requests off this gradually to make sure that
    // the number of bytes in flight is limited to maxBytesInFlight
    private val fetchRequests = new Queue[FetchRequest]

    // Current bytes in flight from our requests
    private var bytesInFlight = 0L

    protected def sendRequest(req: FetchRequest) {
      logDebug("Sending request for %d blocks (%s) from %s".format(
        req.blocks.size, Utils.memoryBytesToString(req.size), req.address.hostPort))
      val cmId = new ConnectionManagerId(req.address.host, req.address.port)
      val blockMessageArray = new BlockMessageArray(req.blocks.map {
        case (blockId, size) => BlockMessage.fromGetBlock(GetBlock(blockId))
      })
      bytesInFlight += req.size
      val sizeMap = req.blocks.toMap  // so we can look up the size of each blockID
      val fetchStart = System.currentTimeMillis()
      val future = connectionManager.sendMessageReliably(cmId, blockMessageArray.toBufferMessage)
      future.onSuccess {
        case Some(message) => {
          val fetchDone = System.currentTimeMillis()
          _remoteFetchTime += fetchDone - fetchStart
          val bufferMessage = message.asInstanceOf[BufferMessage]
          val blockMessageArray = BlockMessageArray.fromBufferMessage(bufferMessage)
          for (blockMessage <- blockMessageArray) {
            if (blockMessage.getType != BlockMessage.TYPE_GOT_BLOCK) {
              throw new SparkException(
                "Unexpected message " + blockMessage.getType + " received from " + cmId)
            }
            val blockId = blockMessage.getId
            results.put(new FetchResult(blockId, sizeMap(blockId),
              () => dataDeserialize(blockId, blockMessage.getData, serializer)))
            _remoteBytesRead += req.size
            logDebug("Got remote block " + blockId + " after " + Utils.getUsedTimeMs(startTime))
          }
        }
        case None => {
          logError("Could not get block(s) from " + cmId)
          for ((blockId, size) <- req.blocks) {
            results.put(new FetchResult(blockId, -1, null))
          }
        }
      }
    }

    protected def splitLocalRemoteBlocks(): ArrayBuffer[FetchRequest] = {
      // Split local and remote blocks. Remote blocks are further split into FetchRequests of size
      // at most maxBytesInFlight in order to limit the amount of data in flight.
      val remoteRequests = new ArrayBuffer[FetchRequest]
      for ((address, blockInfos) <- blocksByAddress) {
        if (address == blockManagerId) {
          localBlockIds ++= blockInfos.map(_._1)
        } else {
          remoteBlockIds ++= blockInfos.map(_._1)
          // Make our requests at least maxBytesInFlight / 5 in length; the reason to keep them
          // smaller than maxBytesInFlight is to allow multiple, parallel fetches from up to 5
          // nodes, rather than blocking on reading output from one node.
          val minRequestSize = math.max(maxBytesInFlight / 5, 1L)
          logInfo("maxBytesInFlight: " + maxBytesInFlight + ", minRequest: " + minRequestSize)
          val iterator = blockInfos.iterator
          var curRequestSize = 0L
          var curBlocks = new ArrayBuffer[(String, Long)]
          while (iterator.hasNext) {
            val (blockId, size) = iterator.next()
            curBlocks += ((blockId, size))
            curRequestSize += size
            if (curRequestSize >= minRequestSize) {
              // Add this FetchRequest
              remoteRequests += new FetchRequest(address, curBlocks)
              curRequestSize = 0
              curBlocks = new ArrayBuffer[(String, Long)]
            }
          }
          // Add in the final request
          if (!curBlocks.isEmpty) {
            remoteRequests += new FetchRequest(address, curBlocks)
          }
        }
      }
      remoteRequests
    }

    protected def getLocalBlocks() {
      // Get the local blocks while remote blocks are being fetched. Note that it's okay to do
      // these all at once because they will just memory-map some files, so they won't consume
      // any memory that might exceed our maxBytesInFlight
      for (id <- localBlockIds) {
        getLocal(id) match {
          case Some(iter) => {
            // Pass 0 as size since it's not in flight
            results.put(new FetchResult(id, 0, () => iter))
            logDebug("Got local block " + id)
          }
          case None => {
            throw new BlockException(id, "Could not get block " + id + " from local machine")
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

      val numGets = remoteBlockIds.size - fetchRequests.size
      logInfo("Started " + numGets + " remote gets in " + Utils.getUsedTimeMs(startTime))

      // Get Local Blocks
      startTime = System.currentTimeMillis
      getLocalBlocks()
      logDebug("Got local blocks in " + Utils.getUsedTimeMs(startTime) + " ms")
    }

    //an iterator that will read fetched blocks off the queue as they arrive.
    @volatile protected var resultsGotten = 0

    override def hasNext: Boolean = resultsGotten < _totalBlocks

    override def next(): (String, Option[Iterator[Any]]) = {
      resultsGotten += 1
      val startFetchWait = System.currentTimeMillis()
      val result = results.take()
      val stopFetchWait = System.currentTimeMillis()
      _fetchWaitTime += (stopFetchWait - startFetchWait)
      if (! result.failed) bytesInFlight -= result.size
      while (!fetchRequests.isEmpty &&
        (bytesInFlight == 0 || bytesInFlight + fetchRequests.front.size <= maxBytesInFlight)) {
        sendRequest(fetchRequests.dequeue())
      }
      (result.blockId, if (result.failed) None else Some(result.deserialize()))
    }

    // Implementing BlockFetchTracker trait.
    override def totalBlocks: Int = _totalBlocks
    override def numLocalBlocks: Int = localBlockIds.size
    override def numRemoteBlocks: Int = remoteBlockIds.size
    override def remoteFetchTime: Long = _remoteFetchTime
    override def fetchWaitTime: Long = _fetchWaitTime
    override def remoteBytesRead: Long = _remoteBytesRead
  }
  // End of BasicBlockFetcherIterator

  class NettyBlockFetcherIterator(
      blockManager: BlockManager,
      blocksByAddress: Seq[(BlockManagerId, Seq[(String, Long)])],
      serializer: Serializer)
    extends BasicBlockFetcherIterator(blockManager, blocksByAddress, serializer) {

    import blockManager._

    val fetchRequestsSync = new LinkedBlockingQueue[FetchRequest]

    private def startCopiers(numCopiers: Int): List[_ <: Thread] = {
      (for ( i <- Range(0,numCopiers) ) yield {
        val copier = new Thread {
          override def run(){
            try {
              while(!isInterrupted && !fetchRequestsSync.isEmpty) {
                sendRequest(fetchRequestsSync.take())
              }
            } catch {
              case x: InterruptedException => logInfo("Copier Interrupted")
              //case _ => throw new SparkException("Exception Throw in Shuffle Copier")
            }
          }
        }
        copier.start
        copier
      }).toList
    }

    //keep this to interrupt the threads when necessary
    private def stopCopiers() {
      for (copier <- copiers) {
        copier.interrupt()
      }
    }

    override protected def sendRequest(req: FetchRequest) {

      def putResult(blockId: String, blockSize: Long, blockData: ByteBuf) {
        val fetchResult = new FetchResult(blockId, blockSize,
          () => dataDeserialize(blockId, blockData.nioBuffer, serializer))
        results.put(fetchResult)
      }

      logDebug("Sending request for %d blocks (%s) from %s".format(
        req.blocks.size, Utils.memoryBytesToString(req.size), req.address.host))
      val cmId = new ConnectionManagerId(
        req.address.host, System.getProperty("spark.shuffle.sender.port", "6653").toInt)
      val cpier = new ShuffleCopier
      cpier.getBlocks(cmId, req.blocks, putResult)
      logDebug("Sent request for remote blocks " + req.blocks + " from " + req.address.host )
    }

    override protected def splitLocalRemoteBlocks(): ArrayBuffer[FetchRequest] = {
      // Split local and remote blocks. Remote blocks are further split into FetchRequests of size
      // at most maxBytesInFlight in order to limit the amount of data in flight.
      val originalTotalBlocks = _totalBlocks;
      val remoteRequests = new ArrayBuffer[FetchRequest]
      for ((address, blockInfos) <- blocksByAddress) {
        if (address == blockManagerId) {
          localBlockIds ++= blockInfos.map(_._1)
        } else {
          remoteBlockIds ++= blockInfos.map(_._1)
          // Make our requests at least maxBytesInFlight / 5 in length; the reason to keep them
          // smaller than maxBytesInFlight is to allow multiple, parallel fetches from up to 5
          // nodes, rather than blocking on reading output from one node.
          val minRequestSize = math.max(maxBytesInFlight / 5, 1L)
          logInfo("maxBytesInFlight: " + maxBytesInFlight + ", minRequest: " + minRequestSize)
          val iterator = blockInfos.iterator
          var curRequestSize = 0L
          var curBlocks = new ArrayBuffer[(String, Long)]
          while (iterator.hasNext) {
            val (blockId, size) = iterator.next()
            if (size > 0) {
              curBlocks += ((blockId, size))
              curRequestSize += size
            } else if (size == 0) {
              //here we changes the totalBlocks
              _totalBlocks -= 1
            } else {
              throw new BlockException(blockId, "Negative block size " + size)
            }
            if (curRequestSize >= minRequestSize) {
              // Add this FetchRequest
              remoteRequests += new FetchRequest(address, curBlocks)
              curRequestSize = 0
              curBlocks = new ArrayBuffer[(String, Long)]
            }
          }
          // Add in the final request
          if (!curBlocks.isEmpty) {
            remoteRequests += new FetchRequest(address, curBlocks)
          }
        }
      }
      logInfo("Getting " + _totalBlocks + " non-zero-bytes blocks out of " +
        originalTotalBlocks + " blocks")
      remoteRequests
    }

    private var copiers: List[_ <: Thread] = null

    override def initialize() {
      // Split Local Remote Blocks and adjust totalBlocks to include only the non 0-byte blocks
      val remoteRequests = splitLocalRemoteBlocks()
      // Add the remote requests into our queue in a random order
      for (request <- Utils.randomize(remoteRequests)) {
        fetchRequestsSync.put(request)
      }

      copiers = startCopiers(System.getProperty("spark.shuffle.copier.threads", "6").toInt)
      logInfo("Started " + fetchRequestsSync.size + " remote gets in " +
        Utils.getUsedTimeMs(startTime))

      // Get Local Blocks
      startTime = System.currentTimeMillis
      getLocalBlocks()
      logDebug("Got local blocks in " + Utils.getUsedTimeMs(startTime) + " ms")
    }

    override def next(): (String, Option[Iterator[Any]]) = {
      resultsGotten += 1
      val result = results.take()
      // if all the results has been retrieved, shutdown the copiers
      if (resultsGotten == _totalBlocks && copiers != null) {
        stopCopiers()
      }
      (result.blockId, if (result.failed) None else Some(result.deserialize()))
    }
  }
  // End of NettyBlockFetcherIterator
}
