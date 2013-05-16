package spark.storage

import java.io.{InputStream, OutputStream}
import java.nio.{ByteBuffer, MappedByteBuffer}

import scala.collection.mutable.{HashMap, ArrayBuffer, HashSet, Queue}
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


private[spark] class BlockManager(
    executorId: String,
    actorSystem: ActorSystem,
    val master: BlockManagerMaster,
    val defaultSerializer: Serializer,
    maxMemory: Long)
  extends Logging {

  private class BlockInfo(val level: StorageLevel, val tellMaster: Boolean) {
    @volatile var pending: Boolean = true
    @volatile var size: Long = -1L
    @volatile var initThread: Thread = null
    @volatile var failed = false

    setInitThread()

    private def setInitThread() {
      // Set current thread as init thread - waitForReady will not block this thread
      // (in case there is non trivial initialization which ends up calling waitForReady as part of
      // initialization itself)
      this.initThread = Thread.currentThread()
    }

    /**
     * Wait for this BlockInfo to be marked as ready (i.e. block is finished writing).
     * Return true if the block is available, false otherwise.
     */
    def waitForReady(): Boolean = {
      if (initThread != Thread.currentThread() && pending) {
        synchronized {
          while (pending) this.wait()
        }
      }
      !failed
    }

    /** Mark this BlockInfo as ready (i.e. block is finished writing) */
    def markReady(sizeInBytes: Long) {
      assert (pending)
      size = sizeInBytes
      initThread = null
      failed = false
      initThread = null
      pending = false
      synchronized {
        this.notifyAll()
      }
    }

    /** Mark this BlockInfo as ready but failed */
    def markFailure() {
      assert (pending)
      size = 0
      initThread = null
      failed = true
      initThread = null
      pending = false
      synchronized {
        this.notifyAll()
      }
    }
  }

  val shuffleBlockManager = new ShuffleBlockManager(this)

  private val blockInfo = new TimeStampedHashMap[String, BlockInfo]

  private[storage] val memoryStore: BlockStore = new MemoryStore(this, maxMemory)
  private[storage] val diskStore: DiskStore =
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

  val hostPort = Utils.localHostPort()

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
   *
   * droppedMemorySize exists to account for when block is dropped from memory to disk (so it is still valid).
   * This ensures that update in master will compensate for the increase in memory on slave.
   */
  def reportBlockStatus(blockId: String, info: BlockInfo, droppedMemorySize: Long = 0L) {
    val needReregister = !tryToReportBlockStatus(blockId, info, droppedMemorySize)
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
  private def tryToReportBlockStatus(blockId: String, info: BlockInfo, droppedMemorySize: Long = 0L): Boolean = {
    val (curLevel, inMemSize, onDiskSize, tellMaster) = info.synchronized {
      info.level match {
        case null =>
          (StorageLevel.NONE, 0L, 0L, false)
        case level =>
          val inMem = level.useMemory && memoryStore.contains(blockId)
          val onDisk = level.useDisk && diskStore.contains(blockId)
          val storageLevel = StorageLevel(onDisk, inMem, level.deserialized, level.replication)
          val memSize = if (inMem) memoryStore.getSize(blockId) else droppedMemorySize
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
   * Get locations of an array of blocks.
   */
  def getLocationBlockIds(blockIds: Array[String]): Array[Seq[BlockManagerId]] = {
    val startTimeMs = System.currentTimeMillis
    val locations = master.getLocations(blockIds).toArray
    logDebug("Got multiple block location in " + Utils.getUsedTimeMs(startTimeMs))
    return locations
  }

  /**
   * A short-circuited method to get blocks directly from disk. This is used for getting
   * shuffle blocks. It is safe to do so without a lock on block info since disk store
   * never deletes (recent) items.
   */
  def getLocalFromDisk(blockId: String, serializer: Serializer): Option[Iterator[Any]] = {
    diskStore.getValues(blockId, serializer).orElse(
      sys.error("Block " + blockId + " not found on disk, though it should be"))
  }

  /**
   * Get block from local block manager.
   */
  def getLocal(blockId: String): Option[Iterator[Any]] = {
    logDebug("Getting local block " + blockId)
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
                // The use of rewind assumes this.
                assert (0 == bytes.position())
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
    if (ShuffleBlockManager.isShuffle(blockId)) {
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
              assert (0 == bytes.position())
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
          GetBlock(blockId), ConnectionManagerId(loc.host, loc.port))
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
  def getMultiple(
    blocksByAddress: Seq[(BlockManagerId, Seq[(String, Long)])], serializer: Serializer)
      : BlockFetcherIterator = {

    val iter =
      if (System.getProperty("spark.shuffle.use.netty", "false").toBoolean) {
        new BlockFetcherIterator.NettyBlockFetcherIterator(this, blocksByAddress, serializer)
      } else {
        new BlockFetcherIterator.BasicBlockFetcherIterator(this, blocksByAddress, serializer)
      }

    iter.initialize()
    iter
  }

  def put(blockId: String, values: Iterator[Any], level: StorageLevel, tellMaster: Boolean)
    : Long = {
    val elements = new ArrayBuffer[Any]
    elements ++= values
    put(blockId, elements, level, tellMaster)
  }

  /**
   * A short circuited method to get a block writer that can write data directly to disk.
   * This is currently used for writing shuffle files out. Callers should handle error
   * cases.
   */
  def getDiskBlockWriter(blockId: String, serializer: Serializer, bufferSize: Int)
    : BlockObjectWriter = {
    val writer = diskStore.getBlockWriter(blockId, serializer, bufferSize)
    writer.registerCloseEventHandler(() => {
      val myInfo = new BlockInfo(StorageLevel.DISK_ONLY, false)
      blockInfo.put(blockId, myInfo)
      myInfo.markReady(writer.size())
    })
    writer
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

    // Remember the block's storage level so that we can correctly drop it to disk if it needs
    // to be dropped right after it got put into memory. Note, however, that other threads will
    // not be able to get() this block until we call markReady on its BlockInfo.
    val myInfo = {
      val tinfo = new BlockInfo(level, tellMaster)
      // Do atomically !
      val oldBlockOpt = blockInfo.putIfAbsent(blockId, tinfo)

      if (oldBlockOpt.isDefined) {
        if (oldBlockOpt.get.waitForReady()) {
          logWarning("Block " + blockId + " already exists on this machine; not re-adding it")
          return oldBlockOpt.get.size
        }

        // TODO: So the block info exists - but previous attempt to load it (?) failed. What do we do now ? Retry on it ?
        oldBlockOpt.get
      } else {
        tinfo
      }
    }

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

      var marked = false
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
        marked = true
        myInfo.markReady(size)
        if (tellMaster) {
          reportBlockStatus(blockId, myInfo)
        }
      } finally {
        // If we failed at putting the block to memory/disk, notify other possible readers
        // that it has failed, and then remove it from the block info map.
        if (! marked) {
          // Note that the remove must happen before markFailure otherwise another thread
          // could've inserted a new BlockInfo before we remove it.
          blockInfo.remove(blockId)
          myInfo.markFailure()
          logWarning("Putting block " + blockId + " failed")
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

    // Remember the block's storage level so that we can correctly drop it to disk if it needs
    // to be dropped right after it got put into memory. Note, however, that other threads will
    // not be able to get() this block until we call markReady on its BlockInfo.
    val myInfo = {
      val tinfo = new BlockInfo(level, tellMaster)
      // Do atomically !
      val oldBlockOpt = blockInfo.putIfAbsent(blockId, tinfo)

      if (oldBlockOpt.isDefined) {
        if (oldBlockOpt.get.waitForReady()) {
          logWarning("Block " + blockId + " already exists on this machine; not re-adding it")
          return
        }

        // TODO: So the block info exists - but previous attempt to load it (?) failed. What do we do now ? Retry on it ?
        oldBlockOpt.get
      } else {
        tinfo
      }
    }

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

      var marked = false
      try {
        if (level.useMemory) {
          // Store it only in memory at first, even if useDisk is also set to true
          bytes.rewind()
          memoryStore.putBytes(blockId, bytes, level)
        } else {
          bytes.rewind()
          diskStore.putBytes(blockId, bytes, level)
        }

        // assert (0 == bytes.position(), "" + bytes)

        // Now that the block is in either the memory or disk store, let other threads read it,
        // and tell the master about it.
        marked = true
        myInfo.markReady(bytes.limit)
        if (tellMaster) {
          reportBlockStatus(blockId, myInfo)
        }
      } finally {
        // If we failed at putting the block to memory/disk, notify other possible readers
        // that it has failed, and then remove it from the block info map.
        if (! marked) {
          // Note that the remove must happen before markFailure otherwise another thread
          // could've inserted a new BlockInfo before we remove it.
          blockInfo.remove(blockId)
          myInfo.markFailure()
          logWarning("Putting block " + blockId + " failed")
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
        new ConnectionManagerId(peer.host, peer.port))) {
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
        // required ? As of now, this will be invoked only for blocks which are ready
        // But in case this changes in future, adding for consistency sake.
        if (! info.waitForReady() ) {
          // If we get here, the block write failed.
          logWarning("Block " + blockId + " was marked as failure. Nothing to drop")
          return
        }

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
        val droppedMemorySize = if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
        val blockWasRemoved = memoryStore.remove(blockId)
        if (!blockWasRemoved) {
          logWarning("Block " + blockId + " could not be dropped from memory as it does not exist")
        }
        if (info.tellMaster) {
          reportBlockStatus(blockId, info, droppedMemorySize)
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
    if (ShuffleBlockManager.isShuffle(blockId)) {
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
    if (shouldCompress(blockId)) {
      (new LZFOutputStream(s)).setFinishBlockOnFlush(true)
    } else {
      s
    }
  }

  /**
   * Wrap an input stream for compression if block compression is enabled for its block type
   */
  def wrapForCompression(blockId: String, s: InputStream): InputStream = {
    if (shouldCompress(blockId)) new LZFInputStream(s) else s
  }

  def dataSerialize(
      blockId: String,
      values: Iterator[Any],
      serializer: Serializer = defaultSerializer): ByteBuffer = {
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
  def dataDeserialize(
      blockId: String,
      bytes: ByteBuffer,
      serializer: Serializer = defaultSerializer): Iterator[Any] = {
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


private[spark] object BlockManager extends Logging {

  val ID_GENERATOR = new IdGenerator

  def getMaxMemoryFromSystemProperties: Long = {
    val memoryFraction = System.getProperty("spark.storage.memoryFraction", "0.66").toDouble
    (Runtime.getRuntime.maxMemory * memoryFraction).toLong
  }

  def getHeartBeatFrequencyFromSystemProperties: Long =
    System.getProperty("spark.storage.blockManagerHeartBeatMs", "5000").toLong

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

  def blockIdsToExecutorLocations(blockIds: Array[String], env: SparkEnv, blockManagerMaster: BlockManagerMaster = null): HashMap[String, List[String]] = {
    // env == null and blockManagerMaster != null is used in tests
    assert (env != null || blockManagerMaster != null)
    val locationBlockIds: Seq[Seq[BlockManagerId]] =
      if (env != null) {
        val blockManager = env.blockManager
        blockManager.getLocationBlockIds(blockIds)
      } else {
        blockManagerMaster.getLocations(blockIds)
      }

    // Convert from block master locations to executor locations (we need that for task scheduling)
    val executorLocations = new HashMap[String, List[String]]()
    for (i <- 0 until blockIds.length) {
      val blockId = blockIds(i)
      val blockLocations = locationBlockIds(i)

      val executors = new HashSet[String]()

      if (env != null) {
        for (bkLocation <- blockLocations) {
          val executorHostPort = env.resolveExecutorIdToHostPort(bkLocation.executorId, bkLocation.host)
          executors += executorHostPort
          // logInfo("bkLocation = " + bkLocation + ", executorHostPort = " + executorHostPort)
        }
      } else {
        // Typically while testing, etc - revert to simply using host.
        for (bkLocation <- blockLocations) {
          executors += bkLocation.host
          // logInfo("bkLocation = " + bkLocation + ", executorHostPort = " + executorHostPort)
        }
      }

      executorLocations.put(blockId, executors.toSeq.toList)
    }

    executorLocations
  }

}

