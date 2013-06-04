package spark.storage

import java.io.{InputStream, OutputStream}
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Queue}
import scala.collection.JavaConversions._

import akka.actor.{ActorSystem, Cancellable, Props}
import akka.dispatch.{Await, Future}
import akka.util.Duration
import akka.util.duration._

import com.ning.compress.lzf.{LZFInputStream, LZFOutputStream}

import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream

import spark.{Logging, SizeEstimator, SparkEnv, SparkException, Utils}
import spark.network._
import spark.serializer.Serializer
import spark.util.{ByteBufferInputStream, IdGenerator, MetadataCleaner, TimeStampedHashMap}

import sun.nio.ch.DirectBuffer


private[spark]
case class BlockException(blockId: String, message: String, ex: Exception = null)
extends Exception(message)

private[spark]
class BlockManager(
    executorId: String,
    actorSystem: ActorSystem,
    val master: BlockManagerMaster,
    val serializer: Serializer,
    maxMemory: Long)
  extends Logging {

  class BlockInfo(val level: StorageLevel, val tellMaster: Boolean) {
    var pending: Boolean = true
    var size: Long = -1L
    var failed: Boolean = false

    /**
     * Wait for this BlockInfo to be marked as ready (i.e. block is finished writing).
     * Return true if the block is available, false otherwise.
     */
    def waitForReady(): Boolean = {
      if (pending) {
        synchronized {
          while (pending) this.wait()
        }
      }
      !failed
    }

    /** Mark this BlockInfo as ready (i.e. block is finished writing) */
    def markReady(sizeInBytes: Long) {
      synchronized {
        pending = false
        failed = false
        size = sizeInBytes
        this.notifyAll()
      }
    }

    /** Mark this BlockInfo as ready but failed */
    def markFailure() {
      synchronized {
        failed = true
        pending = false
        this.notifyAll()
      }
    }
  }

  private val blockInfo = new TimeStampedHashMap[String, BlockInfo]

  private[storage] val memoryStore: BlockStore = new MemoryStore(this, maxMemory)
  private[storage] val diskStore: BlockStore =
    new DiskStore(this, System.getProperty("spark.local.dir", System.getProperty("java.io.tmpdir")))

  val connectionManager = new ConnectionManager(0)
  implicit val futureExecContext = connectionManager.futureExecContext

  val blockManagerId = BlockManagerId(
    executorId, connectionManager.id.host, connectionManager.id.port)

  // Max megabytes of data to keep in flight per reducer (to avoid over-allocating memory
  // for receiving shuffle outputs)
  val maxBytesInFlight =
    System.getProperty("spark.reducer.maxMbInFlight", "48").toLong * 1024 * 1024

  // Whether to compress broadcast variables that are stored
  val compressBroadcast = System.getProperty("spark.broadcast.compress", "true").toBoolean
  // Whether to compress shuffle output that are stored
  val compressShuffle = System.getProperty("spark.shuffle.compress", "true").toBoolean
  // Whether to compress RDD partitions that are stored serialized
  val compressRdds = System.getProperty("spark.rdd.compress", "false").toBoolean

  val heartBeatFrequency = BlockManager.getHeartBeatFrequencyFromSystemProperties

  val host = System.getProperty("spark.hostname", Utils.localHostName())

  val slaveActor = actorSystem.actorOf(Props(new BlockManagerSlaveActor(this)),
    name = "BlockManagerActor" + BlockManager.ID_GENERATOR.next)

  // Pending reregistration action being executed asynchronously or null if none
  // is pending. Accesses should synchronize on asyncReregisterLock.
  var asyncReregisterTask: Future[Unit] = null
  val asyncReregisterLock = new Object

  private def heartBeat() {
    if (!master.sendHeartBeat(blockManagerId)) {
      reregister()
    }
  }

  var heartBeatTask: Cancellable = null

  val metadataCleaner = new MetadataCleaner("BlockManager", this.dropOldBlocks)
  initialize()

  /**
   * Construct a BlockManager with a memory limit set based on system properties.
   */
  def this(execId: String, actorSystem: ActorSystem, master: BlockManagerMaster,
           serializer: Serializer) = {
    this(execId, actorSystem, master, serializer, BlockManager.getMaxMemoryFromSystemProperties)
  }

  /**
   * Initialize the BlockManager. Register to the BlockManagerMaster, and start the
   * BlockManagerWorker actor.
   */
  private def initialize() {
    master.registerBlockManager(blockManagerId, maxMemory, slaveActor)
    BlockManagerWorker.startBlockManagerWorker(this)
    if (!BlockManager.getDisableHeartBeatsForTesting) {
      heartBeatTask = actorSystem.scheduler.schedule(0.seconds, heartBeatFrequency.milliseconds) {
        heartBeat()
      }
    }
  }

  /**
   * Report all blocks to the BlockManager again. This may be necessary if we are dropped
   * by the BlockManager and come back or if we become capable of recovering blocks on disk after
   * an executor crash.
   *
   * This function deliberately fails silently if the master returns false (indicating that
   * the slave needs to reregister). The error condition will be detected again by the next
   * heart beat attempt or new block registration and another try to reregister all blocks
   * will be made then.
   */
  private def reportAllBlocks() {
    logInfo("Reporting " + blockInfo.size + " blocks to the master.")
    for ((blockId, info) <- blockInfo) {
      if (!tryToReportBlockStatus(blockId, info)) {
        logError("Failed to report " + blockId + " to master; giving up.")
        return
      }
    }
  }

  /**
   * Reregister with the master and report all blocks to it. This will be called by the heart beat
   * thread if our heartbeat to the block amnager indicates that we were not registered.
   *
   * Note that this method must be called without any BlockInfo locks held.
   */
  def reregister() {
    // TODO: We might need to rate limit reregistering.
    logInfo("BlockManager reregistering with master")
    master.registerBlockManager(blockManagerId, maxMemory, slaveActor)
    reportAllBlocks()
  }

  /**
   * Reregister with the master sometime soon.
   */
  def asyncReregister() {
    asyncReregisterLock.synchronized {
      if (asyncReregisterTask == null) {
        asyncReregisterTask = Future[Unit] {
          reregister()
          asyncReregisterLock.synchronized {
            asyncReregisterTask = null
          }
        }
      }
    }
  }

  /**
   * For testing. Wait for any pending asynchronous reregistration; otherwise, do nothing.
   */
  def waitForAsyncReregister() {
    val task = asyncReregisterTask
    if (task != null) {
      Await.ready(task, Duration.Inf)
    }
  }

  /**
   * Get storage level of local block. If no info exists for the block, then returns null.
   */
  def getLevel(blockId: String): StorageLevel = blockInfo.get(blockId).map(_.level).orNull

  /**
   * Tell the master about the current storage status of a block. This will send a block update
   * message reflecting the current status, *not* the desired storage level in its block info.
   * For example, a block with MEMORY_AND_DISK set might have fallen out to be only on disk.
   */
  def reportBlockStatus(blockId: String, info: BlockInfo) {
    val needReregister = !tryToReportBlockStatus(blockId, info)
    if (needReregister) {
      logInfo("Got told to reregister updating block " + blockId)
      // Reregistering will report our new block for free.
      asyncReregister()
    }
    logDebug("Told master about block " + blockId)
  }

  /**
   * Actually send a UpdateBlockInfo message. Returns the mater's response,
   * which will be true if the block was successfully recorded and false if
   * the slave needs to re-register.
   */
  private def tryToReportBlockStatus(blockId: String, info: BlockInfo): Boolean = {
    val (curLevel, inMemSize, onDiskSize, tellMaster) = info.synchronized {
      info.level match {
        case null =>
          (StorageLevel.NONE, 0L, 0L, false)
        case level =>
          val inMem = level.useMemory && memoryStore.contains(blockId)
          val onDisk = level.useDisk && diskStore.contains(blockId)
          val storageLevel = StorageLevel(onDisk, inMem, level.deserialized, level.replication)
          val memSize = if (inMem) memoryStore.getSize(blockId) else 0L
          val diskSize = if (onDisk) diskStore.getSize(blockId) else 0L
          (storageLevel, memSize, diskSize, info.tellMaster)
      }
    }

    if (tellMaster) {
      master.updateBlockInfo(blockManagerId, blockId, curLevel, inMemSize, onDiskSize)
    } else {
      true
    }
  }


  /**
   * Get locations of the block.
   */
  def getLocations(blockId: String): Seq[String] = {
    val startTimeMs = System.currentTimeMillis
    var managers = master.getLocations(blockId)
    val locations = managers.map(_.ip)
    logDebug("Got block locations in " + Utils.getUsedTimeMs(startTimeMs))
    return locations
  }

  /**
   * Get locations of an array of blocks.
   */
  def getLocations(blockIds: Array[String]): Array[Seq[String]] = {
    val startTimeMs = System.currentTimeMillis
    val locations = master.getLocations(blockIds).map(_.map(_.ip).toSeq).toArray
    logDebug("Got multiple block location in " + Utils.getUsedTimeMs(startTimeMs))
    return locations
  }

  /**
   * Get block from local block manager.
   */
  def getLocal(blockId: String): Option[Iterator[Any]] = {
    logDebug("Getting local block " + blockId)

    // As an optimization for map output fetches, if the block is for a shuffle, return it
    // without acquiring a lock; the disk store never deletes (recent) items so this should work
    if (blockId.startsWith("shuffle_")) {
      return diskStore.getValues(blockId) match {
        case Some(iterator) =>
          Some(iterator)
        case None =>
          throw new Exception("Block " + blockId + " not found on disk, though it should be")
      }
    }

    val info = blockInfo.get(blockId).orNull
    if (info != null) {
      info.synchronized {

        // In the another thread is writing the block, wait for it to become ready.
        if (!info.waitForReady()) {
          // If we get here, the block write failed.
          logWarning("Block " + blockId + " was marked as failure.")
          return None
        }

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
                // TODO: Consider creating a putValues that also takes in a iterator ?
                val elements = new ArrayBuffer[Any]
                elements ++= iterator
                memoryStore.putValues(blockId, elements, level, true).data match {
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
                return Some(dataDeserialize(blockId, bytes))
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
      }
    } else {
      logDebug("Block " + blockId + " not registered locally")
    }
    return None
  }

  /**
   * Get block from the local block manager as serialized bytes.
   */
  def getLocalBytes(blockId: String): Option[ByteBuffer] = {
    // TODO: This whole thing is very similar to getLocal; we need to refactor it somehow
    logDebug("Getting local block " + blockId + " as bytes")

    // As an optimization for map output fetches, if the block is for a shuffle, return it
    // without acquiring a lock; the disk store never deletes (recent) items so this should work
    if (blockId.startsWith("shuffle_")) {
      return diskStore.getBytes(blockId) match {
        case Some(bytes) =>
          Some(bytes)
        case None =>
          throw new Exception("Block " + blockId + " not found on disk, though it should be")
      }
    }

    val info = blockInfo.get(blockId).orNull
    if (info != null) {
      info.synchronized {

        // In the another thread is writing the block, wait for it to become ready.
        if (!info.waitForReady()) {
          // If we get here, the block write failed.
          logWarning("Block " + blockId + " was marked as failure.")
          return None
        }

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
      }
    } else {
      logDebug("Block " + blockId + " not registered locally")
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
    val locations = master.getLocations(blockId)

    // Get block from remote locations
    for (loc <- locations) {
      logDebug("Getting remote block " + blockId + " from " + loc)
      val data = BlockManagerWorker.syncGetBlock(
          GetBlock(blockId), ConnectionManagerId(loc.ip, loc.port))
      if (data != null) {
        return Some(dataDeserialize(blockId, data))
      }
      logDebug("The value of block " + blockId + " is null")
    }
    logDebug("Block " + blockId + " not found")
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
   * fashion as they're received. Expects a size in bytes to be provided for each block fetched,
   * so that we can control the maxMegabytesInFlight for the fetch.
   */
  def getMultiple(blocksByAddress: Seq[(BlockManagerId, Seq[(String, Long)])])
      : BlockFetcherIterator = {
    return new BlockFetcherIterator(this, blocksByAddress)
  }

  def put(blockId: String, values: Iterator[Any], level: StorageLevel, tellMaster: Boolean)
    : Long = {
    val elements = new ArrayBuffer[Any]
    elements ++= values
    put(blockId, elements, level, tellMaster)
  }

  /**
   * Put a new block of values to the block manager. Returns its (estimated) size in bytes.
   */
  def put(blockId: String, values: ArrayBuffer[Any], level: StorageLevel,
    tellMaster: Boolean = true) : Long = {

    if (blockId == null) {
      throw new IllegalArgumentException("Block Id is null")
    }
    if (values == null) {
      throw new IllegalArgumentException("Values is null")
    }
    if (level == null || !level.isValid) {
      throw new IllegalArgumentException("Storage level is null or invalid")
    }

    val oldBlock = blockInfo.get(blockId).orNull
    if (oldBlock != null && oldBlock.waitForReady()) {
      logWarning("Block " + blockId + " already exists on this machine; not re-adding it")
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

    myInfo.synchronized {
      logTrace("Put for block " + blockId + " took " + Utils.getUsedTimeMs(startTimeMs)
        + " to get into synchronized block")

      try {
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
          // Don't get back the bytes unless we replicate them.
          val askForBytes = level.replication > 1
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
          reportBlockStatus(blockId, myInfo)
        }
      } catch {
        // If we failed at putting the block to memory/disk, notify other possible readers
        // that it has failed, and then remove it from the block info map.
        case e: Exception => {
          // Note that the remove must happen before markFailure otherwise another thread
          // could've inserted a new BlockInfo before we remove it.
          blockInfo.remove(blockId)
          myInfo.markFailure()
          logWarning("Putting block " + blockId + " failed", e)
          throw e
        }
      }
    }
    logDebug("Put block " + blockId + " locally took " + Utils.getUsedTimeMs(startTimeMs))


    // Replicate block if required
    if (level.replication > 1) {
      val remoteStartTime = System.currentTimeMillis
      // Serialize the block if not already done
      if (bytesAfterPut == null) {
        if (valuesAfterPut == null) {
          throw new SparkException(
            "Underlying put returned neither an Iterator nor bytes! This shouldn't happen.")
        }
        bytesAfterPut = dataSerialize(blockId, valuesAfterPut)
      }
      replicate(blockId, bytesAfterPut, level)
      logDebug("Put block " + blockId + " remotely took " + Utils.getUsedTimeMs(remoteStartTime))
    }
    BlockManager.dispose(bytesAfterPut)

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

    if (blockInfo.contains(blockId)) {
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

    myInfo.synchronized {
      logDebug("PutBytes for block " + blockId + " took " + Utils.getUsedTimeMs(startTimeMs)
        + " to get into synchronized block")

      try {
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
          reportBlockStatus(blockId, myInfo)
        }
      } catch {
        // If we failed at putting the block to memory/disk, notify other possible readers
        // that it has failed, and then remove it from the block info map.
        case e: Exception => {
          // Note that the remove must happen before markFailure otherwise another thread
          // could've inserted a new BlockInfo before we remove it.
          blockInfo.remove(blockId)
          myInfo.markFailure()
          logWarning("Putting block " + blockId + " failed", e)
          throw e
        }
      }
    }

    // If replication had started, then wait for it to finish
    if (level.replication > 1) {
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
    val tLevel = StorageLevel(level.useDisk, level.useMemory, level.deserialized, 1)
    if (cachedPeers == null) {
      cachedPeers = master.getPeers(blockManagerId, level.replication - 1)
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
  def dropFromMemory(blockId: String, data: Either[ArrayBuffer[Any], ByteBuffer]) {
    logInfo("Dropping block " + blockId + " from memory")
    val info = blockInfo.get(blockId).orNull
    if (info != null)  {
      info.synchronized {
        val level = info.level
        if (level.useDisk && !diskStore.contains(blockId)) {
          logInfo("Writing block " + blockId + " to disk")
          data match {
            case Left(elements) =>
              diskStore.putValues(blockId, elements, level, false)
            case Right(bytes) =>
              diskStore.putBytes(blockId, bytes, level)
          }
        }
        val blockWasRemoved = memoryStore.remove(blockId)
        if (!blockWasRemoved) {
          logWarning("Block " + blockId + " could not be dropped from memory as it does not exist")
        }
        if (info.tellMaster) {
          reportBlockStatus(blockId, info)
        }
        if (!level.useDisk) {
          // The block is completely gone from this node; forget it so we can put() it again later.
          blockInfo.remove(blockId)
        }
      }
    } else {
      // The block has already been dropped
    }
  }

  /**
   * Remove a block from both memory and disk.
   */
  def removeBlock(blockId: String) {
    logInfo("Removing block " + blockId)
    val info = blockInfo.get(blockId).orNull
    if (info != null) info.synchronized {
      // Removals are idempotent in disk store and memory store. At worst, we get a warning.
      val removedFromMemory = memoryStore.remove(blockId)
      val removedFromDisk = diskStore.remove(blockId)
      if (!removedFromMemory && !removedFromDisk) {
        logWarning("Block " + blockId + " could not be removed as it was not found in either " +
          "the disk or memory store")
      }
      blockInfo.remove(blockId)
      if (info.tellMaster) {
        reportBlockStatus(blockId, info)
      }
    } else {
      // The block has already been removed; do nothing.
      logWarning("Asked to remove block " + blockId + ", which does not exist")
    }
  }

  def dropOldBlocks(cleanupTime: Long) {
    logInfo("Dropping blocks older than " + cleanupTime)
    val iterator = blockInfo.internalMap.entrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      val (id, info, time) = (entry.getKey, entry.getValue._1, entry.getValue._2)
      if (time < cleanupTime) {
        info.synchronized {
          val level = info.level
          if (level.useMemory) {
            memoryStore.remove(id)
          }
          if (level.useDisk) {
            diskStore.remove(id)
          }
          iterator.remove()
          logInfo("Dropped block " + id)
        }
        reportBlockStatus(id, info)
      }
    }
  }

  def shouldCompress(blockId: String): Boolean = {
    if (blockId.startsWith("shuffle_")) {
      compressShuffle
    } else if (blockId.startsWith("broadcast_")) {
      compressBroadcast
    } else if (blockId.startsWith("rdd_")) {
      compressRdds
    } else {
      false    // Won't happen in a real cluster, but it can in tests
    }
  }

  /**
   * Wrap an output stream for compression if block compression is enabled for its block type
   */
  def wrapForCompression(blockId: String, s: OutputStream): OutputStream = {
    if (shouldCompress(blockId)) new LZFOutputStream(s) else s
  }

  /**
   * Wrap an input stream for compression if block compression is enabled for its block type
   */
  def wrapForCompression(blockId: String, s: InputStream): InputStream = {
    if (shouldCompress(blockId)) new LZFInputStream(s) else s
  }

  def dataSerialize(blockId: String, values: Iterator[Any]): ByteBuffer = {
    val byteStream = new FastByteArrayOutputStream(4096)
    val ser = serializer.newInstance()
    ser.serializeStream(wrapForCompression(blockId, byteStream)).writeAll(values).close()
    byteStream.trim()
    ByteBuffer.wrap(byteStream.array)
  }

  /**
   * Deserializes a ByteBuffer into an iterator of values and disposes of it when the end of
   * the iterator is reached.
   */
  def dataDeserialize(blockId: String, bytes: ByteBuffer): Iterator[Any] = {
    bytes.rewind()
    val stream = wrapForCompression(blockId, new ByteBufferInputStream(bytes, true))
    serializer.newInstance().deserializeStream(stream).asIterator
  }

  def stop() {
    if (heartBeatTask != null) {
      heartBeatTask.cancel()
    }
    connectionManager.stop()
    actorSystem.stop(slaveActor)
    blockInfo.clear()
    memoryStore.clear()
    diskStore.clear()
    metadataCleaner.cancel()
    logInfo("BlockManager stopped")
  }
}

private[spark]
object BlockManager extends Logging {

  val ID_GENERATOR = new IdGenerator

  def getMaxMemoryFromSystemProperties: Long = {
    val memoryFraction = System.getProperty("spark.storage.memoryFraction", "0.66").toDouble
    (Runtime.getRuntime.maxMemory * memoryFraction).toLong
  }

  def getHeartBeatFrequencyFromSystemProperties: Long =
    System.getProperty("spark.storage.blockManagerTimeoutIntervalMs", "60000").toLong / 4

  def getDisableHeartBeatsForTesting: Boolean =
    System.getProperty("spark.test.disableBlockManagerHeartBeat", "false").toBoolean

  /**
   * Attempt to clean up a ByteBuffer if it is memory-mapped. This uses an *unsafe* Sun API that
   * might cause errors if one attempts to read from the unmapped buffer, but it's better than
   * waiting for the GC to find it because that could lead to huge numbers of open files. There's
   * unfortunately no standard API to do this.
   */
  def dispose(buffer: ByteBuffer) {
    if (buffer != null && buffer.isInstanceOf[MappedByteBuffer]) {
      logTrace("Unmapping " + buffer)
      if (buffer.asInstanceOf[DirectBuffer].cleaner() != null) {
        buffer.asInstanceOf[DirectBuffer].cleaner().clean()
      }
    }
  }
}

class BlockFetcherIterator(
    private val blockManager: BlockManager,
    val blocksByAddress: Seq[(BlockManagerId, Seq[(String, Long)])]
) extends Iterator[(String, Option[Iterator[Any]])] with Logging with BlockFetchTracker {

  import blockManager._

  private var _remoteBytesRead = 0l
  private var _remoteFetchTime = 0l
  private var _fetchWaitTime = 0l

  if (blocksByAddress == null) {
    throw new IllegalArgumentException("BlocksByAddress is null")
  }
  val totalBlocks = blocksByAddress.map(_._2.size).sum
  logDebug("Getting " + totalBlocks + " blocks")
  var startTime = System.currentTimeMillis
  val localBlockIds = new ArrayBuffer[String]()
  val remoteBlockIds = new HashSet[String]()

  // A result of a fetch. Includes the block ID, size in bytes, and a function to deserialize
  // the block (since we want all deserializaton to happen in the calling thread); can also
  // represent a fetch failure if size == -1.
  class FetchResult(val blockId: String, val size: Long, val deserialize: () => Iterator[Any]) {
    def failed: Boolean = size == -1
  }

  // A queue to hold our results.
  val results = new LinkedBlockingQueue[FetchResult]

  // A request to fetch one or more blocks, complete with their sizes
  class FetchRequest(val address: BlockManagerId, val blocks: Seq[(String, Long)]) {
    val size = blocks.map(_._2).sum
  }

  // Queue of fetch requests to issue; we'll pull requests off this gradually to make sure that
  // the number of bytes in flight is limited to maxBytesInFlight
  val fetchRequests = new Queue[FetchRequest]

  // Current bytes in flight from our requests
  var bytesInFlight = 0L

  def sendRequest(req: FetchRequest) {
    logDebug("Sending request for %d blocks (%s) from %s".format(
      req.blocks.size, Utils.memoryBytesToString(req.size), req.address.ip))
    val cmId = new ConnectionManagerId(req.address.ip, req.address.port)
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
          results.put(new FetchResult(
            blockId, sizeMap(blockId), () => dataDeserialize(blockId, blockMessage.getData)))
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
  // Add the remote requests into our queue in a random order
  fetchRequests ++= Utils.randomize(remoteRequests)

  // Send out initial requests for blocks, up to our maxBytesInFlight
  while (!fetchRequests.isEmpty &&
    (bytesInFlight == 0 || bytesInFlight + fetchRequests.front.size <= maxBytesInFlight)) {
    sendRequest(fetchRequests.dequeue())
  }

  val numGets = remoteBlockIds.size - fetchRequests.size
  logInfo("Started " + numGets + " remote gets in " + Utils.getUsedTimeMs(startTime))

  // Get the local blocks while remote blocks are being fetched. Note that it's okay to do
  // these all at once because they will just memory-map some files, so they won't consume
  // any memory that might exceed our maxBytesInFlight
  startTime = System.currentTimeMillis
  for (id <- localBlockIds) {
    getLocal(id) match {
      case Some(iter) => {
        results.put(new FetchResult(id, 0, () => iter)) // Pass 0 as size since it's not in flight
        logDebug("Got local block " + id)
      }
      case None => {
        throw new BlockException(id, "Could not get block " + id + " from local machine")
      }
    }
  }
  logDebug("Got local blocks in " + Utils.getUsedTimeMs(startTime) + " ms")

  //an iterator that will read fetched blocks off the queue as they arrive.
  var resultsGotten = 0

  def hasNext: Boolean = resultsGotten < totalBlocks

  def next(): (String, Option[Iterator[Any]]) = {
    resultsGotten += 1
    val startFetchWait = System.currentTimeMillis()
    val result = results.take()
    val stopFetchWait = System.currentTimeMillis()
    _fetchWaitTime += (stopFetchWait - startFetchWait)
    bytesInFlight -= result.size
    while (!fetchRequests.isEmpty &&
      (bytesInFlight == 0 || bytesInFlight + fetchRequests.front.size <= maxBytesInFlight)) {
      sendRequest(fetchRequests.dequeue())
    }
    (result.blockId, if (result.failed) None else Some(result.deserialize()))
  }


  //methods to profile the block fetching
  def numLocalBlocks = localBlockIds.size
  def numRemoteBlocks = remoteBlockIds.size

  def remoteFetchTime = _remoteFetchTime
  def fetchWaitTime = _fetchWaitTime

  def remoteBytesRead = _remoteBytesRead

}
