package spark.storage

import akka.dispatch.{Await, Future}
import akka.util.Duration

import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream

import java.io.{InputStream, OutputStream, Externalizable, ObjectInput, ObjectOutput}
import java.nio.{MappedByteBuffer, ByteBuffer}
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Queue}
import scala.collection.JavaConversions._

import spark.{CacheTracker, Logging, Serializer, SizeEstimator, SparkException, Utils}
import spark.network._
import spark.util.ByteBufferInputStream
import com.ning.compress.lzf.{LZFInputStream, LZFOutputStream}
import sun.nio.ch.DirectBuffer


private[spark] class BlockManagerId(var ip: String, var port: Int) extends Externalizable {
  def this() = this(null, 0)

  override def writeExternal(out: ObjectOutput) {
    out.writeUTF(ip)
    out.writeInt(port)
  }

  override def readExternal(in: ObjectInput) {
    ip = in.readUTF()
    port = in.readInt()
  }

  override def toString = "BlockManagerId(" + ip + ", " + port + ")"

  override def hashCode = ip.hashCode * 41 + port

  override def equals(that: Any) = that match {
    case id: BlockManagerId => port == id.port && ip == id.ip
    case _ => false
  }
}


private[spark] 
case class BlockException(blockId: String, message: String, ex: Exception = null)
extends Exception(message)


private[spark] class BlockLocker(numLockers: Int) {
  private val hashLocker = Array.fill(numLockers)(new Object())

  def getLock(blockId: String): Object = {
    return hashLocker(math.abs(blockId.hashCode % numLockers))
  }
}


private[spark]
class BlockManager(val master: BlockManagerMaster, val serializer: Serializer, maxMemory: Long)
  extends Logging {

  class BlockInfo(val level: StorageLevel, val tellMaster: Boolean) {
    var pending: Boolean = true
    var size: Long = -1L

    /** Wait for this BlockInfo to be marked as ready (i.e. block is finished writing) */
    def waitForReady() {
      if (pending) {
        synchronized {
          while (pending) this.wait()
        }
      }
    }

    /** Mark this BlockInfo as ready (i.e. block is finished writing) */
    def markReady(sizeInBytes: Long) {
      pending = false
      size = sizeInBytes
      synchronized {
        this.notifyAll()
      }
    }
  }

  private val NUM_LOCKS = 337
  private val locker = new BlockLocker(NUM_LOCKS)

  private val blockInfo = new ConcurrentHashMap[String, BlockInfo]()

  private[storage] val memoryStore: BlockStore = new MemoryStore(this, maxMemory)
  private[storage] val diskStore: BlockStore =
    new DiskStore(this, System.getProperty("spark.local.dir", System.getProperty("java.io.tmpdir")))

  val connectionManager = new ConnectionManager(0)
  implicit val futureExecContext = connectionManager.futureExecContext

  val connectionManagerId = connectionManager.id
  val blockManagerId = new BlockManagerId(connectionManagerId.host, connectionManagerId.port)

  // TODO: This will be removed after cacheTracker is removed from the code base.
  var cacheTracker: CacheTracker = null

  val numParallelFetches = BlockManager.getNumParallelFetchesFromSystemProperties
  val compress = System.getProperty("spark.blockManager.compress", "false").toBoolean
  val host = System.getProperty("spark.hostname", Utils.localHostName())

  initialize()

  /**
   * Construct a BlockManager with a memory limit set based on system properties.
   */
  def this(master: BlockManagerMaster, serializer: Serializer) = {
    this(master, serializer, BlockManager.getMaxMemoryFromSystemProperties)
  }

  /**
   * Initialize the BlockManager. Register to the BlockManagerMaster, and start the
   * BlockManagerWorker actor.
   */
  private def initialize() {
    master.mustRegisterBlockManager(
      RegisterBlockManager(blockManagerId, maxMemory))
    BlockManagerWorker.startBlockManagerWorker(this)
  }

  /**
   * Get storage level of local block. If no info exists for the block, then returns null.
   */
  def getLevel(blockId: String): StorageLevel = {
    val info = blockInfo.get(blockId)
    if (info != null) info.level else null
  }

  /**
   * Tell the master about the current storage status of a block. This will send a heartbeat
   * message reflecting the current status, *not* the desired storage level in its block info.
   * For example, a block with MEMORY_AND_DISK set might have fallen out to be only on disk.
   */
  def reportBlockStatus(blockId: String) {
    locker.getLock(blockId).synchronized {
      val curLevel = blockInfo.get(blockId) match {
        case null =>
          StorageLevel.NONE
        case info =>
          info.level match {
            case null =>
              StorageLevel.NONE
            case level =>
              val inMem = level.useMemory && memoryStore.contains(blockId)
              val onDisk = level.useDisk && diskStore.contains(blockId)
              new StorageLevel(onDisk, inMem, level.deserialized, level.replication)
          }
      }
      master.mustHeartBeat(HeartBeat(
        blockManagerId,
        blockId,
        curLevel,
        if (curLevel.useMemory) memoryStore.getSize(blockId) else 0L,
        if (curLevel.useDisk) diskStore.getSize(blockId) else 0L))
      logDebug("Told master about block " + blockId)
    }
  }

  /**
   * Get locations of the block.
   */
  def getLocations(blockId: String): Seq[String] = {
    val startTimeMs = System.currentTimeMillis
    var managers = master.mustGetLocations(GetLocations(blockId))
    val locations = managers.map(_.ip)
    logDebug("Get block locations in " + Utils.getUsedTimeMs(startTimeMs))
    return locations
  }

  /**
   * Get locations of an array of blocks.
   */
  def getLocations(blockIds: Array[String]): Array[Seq[String]] = {
    val startTimeMs = System.currentTimeMillis
    val locations = master.mustGetLocationsMultipleBlockIds(
      GetLocationsMultipleBlockIds(blockIds)).map(_.map(_.ip).toSeq).toArray
    logDebug("Get multiple block location in " + Utils.getUsedTimeMs(startTimeMs))
    return locations
  }

  /**
   * Get block from local block manager.
   */
  def getLocal(blockId: String): Option[Iterator[Any]] = {
    logDebug("Getting local block " + blockId)
    locker.getLock(blockId).synchronized {
      val info = blockInfo.get(blockId)
      if (info != null) {
        info.waitForReady() // In case the block is still being put() by another thread
        val level = info.level
        logDebug("Level for block " + blockId + " is " + level)

        // Look for the block in memory
        if (level.useMemory) {
          logDebug("Getting block " + blockId + " from memory")
          memoryStore.getValues(blockId) match {
            case Some(iterator) =>
              return Some(iterator)
            case None =>
              logDebug("Block " + blockId + " not found in memory")
          }
        }

        // Look for block on disk, potentially loading it back into memory if required
        if (level.useDisk) {
          logDebug("Getting block " + blockId + " from disk")
          if (level.useMemory && level.deserialized) {
            diskStore.getValues(blockId) match {
              case Some(iterator) =>
                // Put the block back in memory before returning it
                memoryStore.putValues(blockId, iterator, level, true).data match {
                  case Left(iterator2) =>
                    return Some(iterator2)
                  case _ =>
                    throw new Exception("Memory store did not return back an iterator")
                }
              case None =>
                throw new Exception("Block " + blockId + " not found on disk, though it should be")
            }
          } else if (level.useMemory && !level.deserialized) {
            // Read it as a byte buffer into memory first, then return it
            diskStore.getBytes(blockId) match {
              case Some(bytes) =>
                // Put a copy of the block back in memory before returning it. Note that we can't
                // put the ByteBuffer returned by the disk store as that's a memory-mapped file.
                val copyForMemory = ByteBuffer.allocate(bytes.limit)
                copyForMemory.put(bytes)
                memoryStore.putBytes(blockId, copyForMemory, level)
                bytes.rewind()
                return Some(dataDeserialize(bytes))
              case None =>
                throw new Exception("Block " + blockId + " not found on disk, though it should be")
            }
          } else {
            diskStore.getValues(blockId) match {
              case Some(iterator) =>
                return Some(iterator)
              case None =>
                throw new Exception("Block " + blockId + " not found on disk, though it should be")
            }
          }
        }
      } else {
        logDebug("Block " + blockId + " not registered locally")
      }
    }
    return None
  }

  /**
   * Get block from the local block manager as serialized bytes.
   */
  def getLocalBytes(blockId: String): Option[ByteBuffer] = {
    // TODO: This whole thing is very similar to getLocal; we need to refactor it somehow
    logDebug("Getting local block " + blockId + " as bytes")
    locker.getLock(blockId).synchronized {
      val info = blockInfo.get(blockId)
      if (info != null) {
        info.waitForReady() // In case the block is still being put() by another thread
        val level = info.level
        logDebug("Level for block " + blockId + " is " + level)

        // Look for the block in memory
        if (level.useMemory) {
          logDebug("Getting block " + blockId + " from memory")
          memoryStore.getBytes(blockId) match {
            case Some(bytes) =>
              return Some(bytes)
            case None =>
              logDebug("Block " + blockId + " not found in memory")
          }
        }

        // Look for block on disk
        if (level.useDisk) {
          // Read it as a byte buffer into memory first, then return it
          diskStore.getBytes(blockId) match {
            case Some(bytes) =>
              if (level.useMemory) {
                if (level.deserialized) {
                  memoryStore.putBytes(blockId, bytes, level)
                } else {
                  // The memory store will hang onto the ByteBuffer, so give it a copy instead of
                  // the memory-mapped file buffer we got from the disk store
                  val copyForMemory = ByteBuffer.allocate(bytes.limit)
                  copyForMemory.put(bytes)
                  memoryStore.putBytes(blockId, copyForMemory, level)
                }
              }
              bytes.rewind()
              return Some(bytes)
            case None =>
              throw new Exception("Block " + blockId + " not found on disk, though it should be")
          }
        }
      } else {
        logDebug("Block " + blockId + " not registered locally")
      }
    }
    return None
  }

  /**
   * Get block from remote block managers.
   */
  def getRemote(blockId: String): Option[Iterator[Any]] = {
    if (blockId == null) {
      throw new IllegalArgumentException("Block Id is null")
    }
    logDebug("Getting remote block " + blockId)
    // Get locations of block
    val locations = master.mustGetLocations(GetLocations(blockId))

    // Get block from remote locations
    for (loc <- locations) {
      logDebug("Getting remote block " + blockId + " from " + loc)
      val data = BlockManagerWorker.syncGetBlock(
          GetBlock(blockId), ConnectionManagerId(loc.ip, loc.port))
      if (data != null) {
        logDebug("Data is not null: " + data)
        return Some(dataDeserialize(data))
      }
      logDebug("Data is null")
    }
    logDebug("Data not found")
    return None
  }

  /**
   * Get a block from the block manager (either local or remote).
   */
  def get(blockId: String): Option[Iterator[Any]] = {
    getLocal(blockId).orElse(getRemote(blockId))
  }

  /**
   * Get multiple blocks from local and remote block manager using their BlockManagerIds. Returns
   * an Iterator of (block ID, value) pairs so that clients may handle blocks in a pipelined
   * fashion as they're received.
   */
  def getMultiple(blocksByAddress: Seq[(BlockManagerId, Seq[String])])
      : Iterator[(String, Option[Iterator[Any]])] = {

    if (blocksByAddress == null) {
      throw new IllegalArgumentException("BlocksByAddress is null")
    }
    val totalBlocks = blocksByAddress.map(_._2.size).sum
    logDebug("Getting " + totalBlocks + " blocks")
    var startTime = System.currentTimeMillis
    val localBlockIds = new ArrayBuffer[String]()
    val remoteBlockIds = new HashSet[String]()

    // A queue to hold our results. Because we want all the deserializing the happen in the
    // caller's thread, this will actually hold functions to produce the Iterator for each block.
    // For local blocks we'll have an iterator already, while for remote ones we'll deserialize.
    val results = new LinkedBlockingQueue[(String, Option[() => Iterator[Any]])]

    // Bound the number and memory usage of fetched remote blocks.
    val blocksToRequest = new Queue[(BlockManagerId, BlockMessage)]

    def sendRequest(bmId: BlockManagerId, blockMessages: Seq[BlockMessage]) {
      val cmId = new ConnectionManagerId(bmId.ip, bmId.port)
      val blockMessageArray = new BlockMessageArray(blockMessages)
      val future = connectionManager.sendMessageReliably(cmId, blockMessageArray.toBufferMessage)
      future.onSuccess {
        case Some(message) => {
          val bufferMessage = message.asInstanceOf[BufferMessage]
          val blockMessageArray = BlockMessageArray.fromBufferMessage(bufferMessage)
          for (blockMessage <- blockMessageArray) {
            if (blockMessage.getType != BlockMessage.TYPE_GOT_BLOCK) {
              throw new SparkException(
                "Unexpected message " + blockMessage.getType + " received from " + cmId)
            }
            val blockId = blockMessage.getId
            results.put((blockId, Some(() => dataDeserialize(blockMessage.getData))))
            logDebug("Got remote block " + blockId + " after " + Utils.getUsedTimeMs(startTime))
          }
        }
        case None => {
          logError("Could not get block(s) from " + cmId)
          for (blockMessage <- blockMessages) {
            results.put((blockMessage.getId, None))
          }
        }
      }
    }

    // Split local and remote blocks. Remote blocks are further split into ones that will
    // be requested initially and ones that will be added to a queue of blocks to request.
    val initialRequestBlocks = new HashMap[BlockManagerId, ArrayBuffer[BlockMessage]]()
    var initialRequests = 0
    val blocksToGetLater = new ArrayBuffer[(BlockManagerId, BlockMessage)]
    for ((address, blockIds) <- Utils.randomize(blocksByAddress)) {
      if (address == blockManagerId) {
        localBlockIds ++= blockIds
      } else {
        remoteBlockIds ++= blockIds
        for (blockId <- blockIds) {
          val blockMessage = BlockMessage.fromGetBlock(GetBlock(blockId))
          if (initialRequests < numParallelFetches) {
            initialRequestBlocks.getOrElseUpdate(address, new ArrayBuffer[BlockMessage])
              .append(blockMessage)
            initialRequests += 1
          } else {
            blocksToGetLater.append((address, blockMessage))
          }
        }
      }
    }
    // Add the remaining blocks into a queue to pull later in a random order
    blocksToRequest ++= Utils.randomize(blocksToGetLater)

    // Send out initial request(s) for 'numParallelFetches' blocks.
    for ((bmId, blockMessages) <- initialRequestBlocks) {
      sendRequest(bmId, blockMessages)
    }

    logDebug("Started remote gets for " + numParallelFetches + " blocks in " +
      Utils.getUsedTimeMs(startTime) + " ms")

    // Get the local blocks while remote blocks are being fetched.
    startTime = System.currentTimeMillis
    for (id <- localBlockIds) {
      getLocal(id) match {
        case Some(block) => {
          results.put((id, Some(() => block)))
          logDebug("Got local block " + id)
        }
        case None => {
          throw new BlockException(id, "Could not get block " + id + " from local machine")
        }
      }
    }
    logDebug("Got local blocks in " + Utils.getUsedTimeMs(startTime) + " ms")

    // Return an iterator that will read fetched blocks off the queue as they arrive.
    return new Iterator[(String, Option[Iterator[Any]])] {
      var resultsGotten = 0

      def hasNext: Boolean = resultsGotten < totalBlocks

      def next(): (String, Option[Iterator[Any]]) = {
        resultsGotten += 1
        val (blockId, functionOption) = results.take()
        if (remoteBlockIds.contains(blockId) && !blocksToRequest.isEmpty) {
          val (bmId, blockMessage) = blocksToRequest.dequeue()
          sendRequest(bmId, Seq(blockMessage))
        }
        (blockId, functionOption.map(_.apply()))
      }
    }
  }

  /**
   * Put a new block of values to the block manager. Returns its (estimated) size in bytes.
   */
  def put(blockId: String, values: Iterator[Any], level: StorageLevel, tellMaster: Boolean = true)
    : Long = {

    if (blockId == null) {
      throw new IllegalArgumentException("Block Id is null")
    }
    if (values == null) {
      throw new IllegalArgumentException("Values is null")
    }
    if (level == null || !level.isValid) {
      throw new IllegalArgumentException("Storage level is null or invalid")
    }

    val oldBlock = blockInfo.get(blockId)
    if (oldBlock != null) {
      logWarning("Block " + blockId + " already exists on this machine; not re-adding it")
      oldBlock.waitForReady()
      return oldBlock.size
    }

    // Remember the block's storage level so that we can correctly drop it to disk if it needs
    // to be dropped right after it got put into memory. Note, however, that other threads will
    // not be able to get() this block until we call markReady on its BlockInfo.
    val myInfo = new BlockInfo(level, tellMaster)
    blockInfo.put(blockId, myInfo)

    val startTimeMs = System.currentTimeMillis

    // If we need to replicate the data, we'll want access to the values, but because our
    // put will read the whole iterator, there will be no values left. For the case where
    // the put serializes data, we'll remember the bytes, above; but for the case where it
    // doesn't, such as deserialized storage, let's rely on the put returning an Iterator.
    var valuesAfterPut: Iterator[Any] = null

    // Ditto for the bytes after the put
    var bytesAfterPut: ByteBuffer = null

    // Size of the block in bytes (to return to caller)
    var size = 0L

    locker.getLock(blockId).synchronized {
      logDebug("Put for block " + blockId + " took " + Utils.getUsedTimeMs(startTimeMs)
        + " to get into synchronized block")

      if (level.useMemory) {
        // Save it just to memory first, even if it also has useDisk set to true; we will later
        // drop it to disk if the memory store can't hold it.
        val res = memoryStore.putValues(blockId, values, level, true)
        size = res.size
        res.data match {
          case Right(newBytes) => bytesAfterPut = newBytes
          case Left(newIterator) => valuesAfterPut = newIterator
        }
      } else {
        // Save directly to disk.
        val askForBytes = level.replication > 1 // Don't get back the bytes unless we replicate them
        val res = diskStore.putValues(blockId, values, level, askForBytes)
        size = res.size
        res.data match {
          case Right(newBytes) => bytesAfterPut = newBytes
          case _ =>
        }
      }

      // Now that the block is in either the memory or disk store, let other threads read it,
      // and tell the master about it.
      myInfo.markReady(size)
      if (tellMaster) {
        reportBlockStatus(blockId)
      }
    }
    logDebug("Put block " + blockId + " locally took " + Utils.getUsedTimeMs(startTimeMs))

    // Replicate block if required
    if (level.replication > 1) {
      // Serialize the block if not already done
      if (bytesAfterPut == null) {
        if (valuesAfterPut == null) {
          throw new SparkException(
            "Underlying put returned neither an Iterator nor bytes! This shouldn't happen.")
        }
        bytesAfterPut = dataSerialize(valuesAfterPut)
      }
      replicate(blockId, bytesAfterPut, level)
    }

    BlockManager.dispose(bytesAfterPut)

    // TODO: This code will be removed when CacheTracker is gone.
    if (blockId.startsWith("rdd")) {
      notifyCacheTracker(blockId)
    }
    logDebug("Put block " + blockId + " took " + Utils.getUsedTimeMs(startTimeMs))

    return size
  }


  /**
   * Put a new block of serialized bytes to the block manager.
   */
  def putBytes(
    blockId: String, bytes: ByteBuffer, level: StorageLevel, tellMaster: Boolean = true) {

    if (blockId == null) {
      throw new IllegalArgumentException("Block Id is null")
    }
    if (bytes == null) {
      throw new IllegalArgumentException("Bytes is null")
    }
    if (level == null || !level.isValid) {
      throw new IllegalArgumentException("Storage level is null or invalid")
    }

    if (blockInfo.containsKey(blockId)) {
      logWarning("Block " + blockId + " already exists on this machine; not re-adding it")
      return
    }

    // Remember the block's storage level so that we can correctly drop it to disk if it needs
    // to be dropped right after it got put into memory. Note, however, that other threads will
    // not be able to get() this block until we call markReady on its BlockInfo.
    val myInfo = new BlockInfo(level, tellMaster)
    blockInfo.put(blockId, myInfo)

    val startTimeMs = System.currentTimeMillis

    // Initiate the replication before storing it locally. This is faster as
    // data is already serialized and ready for sending
    val replicationFuture = if (level.replication > 1) {
      val bufferView = bytes.duplicate() // Doesn't copy the bytes, just creates a wrapper
      Future {
        replicate(blockId, bufferView, level)
      }
    } else {
      null
    }

    locker.getLock(blockId).synchronized {
      logDebug("PutBytes for block " + blockId + " took " + Utils.getUsedTimeMs(startTimeMs)
        + " to get into synchronized block")

      if (level.useMemory) {
        // Store it only in memory at first, even if useDisk is also set to true
        bytes.rewind()
        memoryStore.putBytes(blockId, bytes, level)
      } else {
        bytes.rewind()
        diskStore.putBytes(blockId, bytes, level)
      }

      // Now that the block is in either the memory or disk store, let other threads read it,
      // and tell the master about it.
      myInfo.markReady(bytes.limit)
      if (tellMaster) {
        reportBlockStatus(blockId)
      }
    }

    // TODO: This code will be removed when CacheTracker is gone.
    if (blockId.startsWith("rdd")) {
      notifyCacheTracker(blockId)
    }

    // If replication had started, then wait for it to finish
    if (level.replication > 1) {
      if (replicationFuture == null) {
        throw new Exception("Unexpected")
      }
      Await.ready(replicationFuture, Duration.Inf)
    }

    if (level.replication > 1) {
      logDebug("PutBytes for block " + blockId + " with replication took " +
        Utils.getUsedTimeMs(startTimeMs))
    } else {
      logDebug("PutBytes for block " + blockId + " without replication took " +
        Utils.getUsedTimeMs(startTimeMs))
    }
  }

  /**
   * Replicate block to another node.
   */
  var cachedPeers: Seq[BlockManagerId] = null
  private def replicate(blockId: String, data: ByteBuffer, level: StorageLevel) {
    val tLevel: StorageLevel =
      new StorageLevel(level.useDisk, level.useMemory, level.deserialized, 1)
    if (cachedPeers == null) {
      cachedPeers = master.mustGetPeers(GetPeers(blockManagerId, level.replication - 1))
    }
    for (peer: BlockManagerId <- cachedPeers) {
      val start = System.nanoTime
      data.rewind()
      logDebug("Try to replicate BlockId " + blockId + " once; The size of the data is "
        + data.limit() + " Bytes. To node: " + peer)
      if (!BlockManagerWorker.syncPutBlock(PutBlock(blockId, data, tLevel),
        new ConnectionManagerId(peer.ip, peer.port))) {
        logError("Failed to call syncPutBlock to " + peer)
      }
      logDebug("Replicated BlockId " + blockId + " once used " +
        (System.nanoTime - start) / 1e6 + " s; The size of the data is " +
        data.limit() + " bytes.")
    }
  }

  // TODO: This code will be removed when CacheTracker is gone.
  private def notifyCacheTracker(key: String) {
    if (cacheTracker != null) {
      val rddInfo = key.split("_")
      val rddId: Int = rddInfo(1).toInt
      val partition: Int = rddInfo(2).toInt
      cacheTracker.notifyFromBlockManager(spark.AddedToCache(rddId, partition, host))
    }
  }

  /**
   * Read a block consisting of a single object.
   */
  def getSingle(blockId: String): Option[Any] = {
    get(blockId).map(_.next())
  }

  /**
   * Write a block consisting of a single object.
   */
  def putSingle(blockId: String, value: Any, level: StorageLevel, tellMaster: Boolean = true) {
    put(blockId, Iterator(value), level, tellMaster)
  }

  /**
   * Drop a block from memory, possibly putting it on disk if applicable. Called when the memory
   * store reaches its limit and needs to free up space.
   */
  def dropFromMemory(blockId: String, data: Either[Iterator[_], ByteBuffer]) {
    logInfo("Dropping block " + blockId + " from memory")
    locker.getLock(blockId).synchronized {
      val info = blockInfo.get(blockId)
      val level = info.level
      if (level.useDisk && !diskStore.contains(blockId)) {
        logInfo("Writing block " + blockId + " to disk")
        data match {
          case Left(iterator) =>
            diskStore.putValues(blockId, iterator, level, false)
          case Right(bytes) =>
            diskStore.putBytes(blockId, bytes, level)
        }
      }
      memoryStore.remove(blockId)
      if (info.tellMaster) {
        reportBlockStatus(blockId)
      }
      if (!level.useDisk) {
        // The block is completely gone from this node; forget it so we can put() it again later.
        blockInfo.remove(blockId)
      }
    }
  }

  /**
   * Wrap an output stream for compression if block compression is enabled
   */
  def wrapForCompression(s: OutputStream): OutputStream = {
    if (compress) new LZFOutputStream(s) else s
  }

  /**
   * Wrap an input stream for compression if block compression is enabled
   */
  def wrapForCompression(s: InputStream): InputStream = {
    if (compress) new LZFInputStream(s) else s
  }

  def dataSerialize(values: Iterator[Any]): ByteBuffer = {
    val byteStream = new FastByteArrayOutputStream(4096)
    val ser = serializer.newInstance()
    ser.serializeStream(wrapForCompression(byteStream)).writeAll(values).close()
    byteStream.trim()
    ByteBuffer.wrap(byteStream.array)
  }

  /**
   * Deserializes a ByteBuffer into an iterator of values and disposes of it when the end of
   * the iterator is reached.
   */
  def dataDeserialize(bytes: ByteBuffer): Iterator[Any] = {
    bytes.rewind()
    val ser = serializer.newInstance()
    ser.deserializeStream(wrapForCompression(new ByteBufferInputStream(bytes, true))).asIterator
  }

  def stop() {
    connectionManager.stop()
    blockInfo.clear()
    memoryStore.clear()
    diskStore.clear()
    logInfo("BlockManager stopped")
  }
}

private[spark]
object BlockManager extends Logging {
  def getNumParallelFetchesFromSystemProperties: Int = {
    System.getProperty("spark.blockManager.parallelFetches", "4").toInt
  }

  def getMaxMemoryFromSystemProperties: Long = {
    val memoryFraction = System.getProperty("spark.storage.memoryFraction", "0.66").toDouble
    (Runtime.getRuntime.maxMemory * memoryFraction).toLong
  }

  /**
   * Attempt to clean up a ByteBuffer if it is memory-mapped. This uses an *unsafe* Sun API that
   * might cause errors if one attempts to read from the unmapped buffer, but it's better than
   * waiting for the GC to find it because that could lead to huge numbers of open files. There's
   * unfortunately no standard API to do this.
   */
  def dispose(buffer: ByteBuffer) {
    if (buffer != null && buffer.isInstanceOf[MappedByteBuffer]) {
      logDebug("Unmapping " + buffer)
      if (buffer.asInstanceOf[DirectBuffer].cleaner() != null) {
        buffer.asInstanceOf[DirectBuffer].cleaner().clean()
      }
    }
  }
}
