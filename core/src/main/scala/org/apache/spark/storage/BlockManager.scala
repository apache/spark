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

import java.io.{File, OutputStream}
import java.nio.{ByteBuffer, MappedByteBuffer}

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random

import akka.actor.{ActorSystem, Props}
import sun.nio.ch.DirectBuffer

import org.apache.spark._
import org.apache.spark.executor._
import org.apache.spark.network._
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.netty.{SparkTransportConf, NettyBlockTransferService}
import org.apache.spark.network.shuffle.{ExecutorShuffleInfo, ExternalShuffleClient}
import org.apache.spark.network.util.{ConfigProvider, TransportConf}
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.shuffle.hash.HashShuffleManager
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.util._

private[spark] class BlockManager(
    executorId: String,
    actorSystem: ActorSystem,
    val master: BlockManagerMaster,
    defaultSerializer: Serializer,
    maxMemory: Long,
    val conf: SparkConf,
    mapOutputTracker: MapOutputTracker,
    shuffleManager: ShuffleManager,
    blockTransferService: BlockTransferService)
  extends BlockDataManager with Logging {

  val blockSerde = new BlockSerializer(conf, defaultSerializer)

  blockTransferService.init(this)

  val diskBlockManager = new DiskBlockManager(this, conf)

  private val blockInfo = new TimeStampedHashMap[BlockId, BlockInfo]

  // Actual storage of where blocks are kept
  private var tachyonInitialized = false
  private[spark] val memoryStore = new MemoryStore(conf, blockSerde, this, maxMemory)
  private[spark] val diskStore = new DiskStore(conf, blockSerde, diskBlockManager)
  private[spark] lazy val tachyonStore: TachyonStore = {
    val storeDir = conf.get("spark.tachyonStore.baseDir", "/tmp_spark_tachyon")
    val appFolderName = conf.get("spark.tachyonStore.folderName")
    val tachyonStorePath = s"$storeDir/$appFolderName/${this.executorId}"
    val tachyonMaster = conf.get("spark.tachyonStore.url",  "tachyon://localhost:19998")
    val tachyonBlockManager =
      new TachyonBlockManager(this, tachyonStorePath, tachyonMaster)
    tachyonInitialized = true
    new TachyonStore(blockSerde, tachyonBlockManager)
  }

  private[spark]
  val externalShuffleServiceEnabled = conf.getBoolean("spark.shuffle.service.enabled", false)
  private val externalShuffleServicePort = conf.getInt("spark.shuffle.service.port", 7337)
  // Check that we're not using external shuffle service with consolidated shuffle files.
  if (externalShuffleServiceEnabled
      && conf.getBoolean("spark.shuffle.consolidateFiles", false)
      && shuffleManager.isInstanceOf[HashShuffleManager]) {
    throw new UnsupportedOperationException("Cannot use external shuffle service with consolidated"
      + " shuffle files in hash-based shuffle. Please disable spark.shuffle.consolidateFiles or "
      + " switch to sort-based shuffle.")
  }

  val blockManagerId = BlockManagerId(
    executorId, blockTransferService.hostName, blockTransferService.port)

  // Address of the server that serves this executor's shuffle files. This is either an external
  // service, or just our own Executor's BlockManager.
  private[spark] val shuffleServerId = if (externalShuffleServiceEnabled) {
    BlockManagerId(executorId, blockTransferService.hostName, externalShuffleServicePort)
  } else {
    blockManagerId
  }

  // Client to read other executors' shuffle files. This is either an external service, or just the
  // standard BlockTranserService to directly connect to other Executors.
  private[spark] val shuffleClient = if (externalShuffleServiceEnabled) {
    val appId = conf.get("spark.app.id", "unknown-app-id")
    new ExternalShuffleClient(SparkTransportConf.fromSparkConf(conf), appId)
  } else {
    blockTransferService
  }

  private val slaveActor = actorSystem.actorOf(
    Props(new BlockManagerSlaveActor(this, mapOutputTracker)),
    name = "BlockManagerActor" + BlockManager.ID_GENERATOR.next)

  // Pending re-registration action being executed asynchronously or null if none is pending.
  // Accesses should synchronize on asyncReregisterLock.
  private var asyncReregisterTask: Future[Unit] = null
  private val asyncReregisterLock = new Object

  private val metadataCleaner = new MetadataCleaner(
    MetadataCleanerType.BLOCK_MANAGER, this.dropOldNonBroadcastBlocks, conf)
  private val broadcastCleaner = new MetadataCleaner(
    MetadataCleanerType.BROADCAST_VARS, this.dropOldBroadcastBlocks, conf)

  // Field related to peer block managers that are necessary for block replication
  @volatile private var cachedPeers: Seq[BlockManagerId] = _
  private val peerFetchLock = new Object
  private var lastPeerFetchTime = 0L

  initialize()

  /**
   * Construct a BlockManager with a memory limit set based on system properties.
   */
  def this(
      execId: String,
      actorSystem: ActorSystem,
      master: BlockManagerMaster,
      serializer: Serializer,
      conf: SparkConf,
      mapOutputTracker: MapOutputTracker,
      shuffleManager: ShuffleManager,
      blockTransferService: BlockTransferService) = {
    this(execId, actorSystem, master, serializer, BlockManager.getMaxMemory(conf),
      conf, mapOutputTracker, shuffleManager, blockTransferService)
  }

  /**
   * Initialize the BlockManager. Register to the BlockManagerMaster, and start the
   * BlockManagerWorker actor. Additionally registers with a local shuffle service if configured.
   */
  private def initialize(): Unit = {
    master.registerBlockManager(blockManagerId, maxMemory, slaveActor)

    // Register Executors' configuration with the local shuffle service, if one should exist.
    if (externalShuffleServiceEnabled && !blockManagerId.isDriver) {
      registerWithExternalShuffleServer()
    }
  }

  private def registerWithExternalShuffleServer() {
    logInfo("Registering executor with local external shuffle service.")
    val shuffleConfig = new ExecutorShuffleInfo(
      diskBlockManager.localDirs.map(_.toString),
      diskBlockManager.subDirsPerLocalDir,
      shuffleManager.getClass.getName)

    val MAX_ATTEMPTS = 3
    val SLEEP_TIME_SECS = 5

    for (i <- 1 to MAX_ATTEMPTS) {
      try {
        // Synchronous and will throw an exception if we cannot connect.
        shuffleClient.asInstanceOf[ExternalShuffleClient].registerWithShuffleServer(
          shuffleServerId.host, shuffleServerId.port, shuffleServerId.executorId, shuffleConfig)
        return
      } catch {
        case e: Exception if i < MAX_ATTEMPTS =>
          val attemptsRemaining =
          logError(s"Failed to connect to external shuffle server, will retry ${MAX_ATTEMPTS - i}}"
            + s" more times after waiting $SLEEP_TIME_SECS seconds...", e)
          Thread.sleep(SLEEP_TIME_SECS * 1000)
      }
    }
  }

  /**
   * Report all blocks to the BlockManager again. This may be necessary if we are dropped
   * by the BlockManager and come back or if we become capable of recovering blocks on disk after
   * an executor crash.
   *
   * This function deliberately fails silently if the master returns false (indicating that
   * the slave needs to re-register). The error condition will be detected again by the next
   * heart beat attempt or new block registration and another try to re-register all blocks
   * will be made then.
   */
  private def reportAllBlocks(): Unit = {
    logInfo(s"Reporting ${blockInfo.size} blocks to the master.")
    for ((blockId, info) <- blockInfo) {
      val status = getCurrentBlockStatus(blockId, info)
      if (!tryToReportBlockStatus(blockId, info, status)) {
        logError(s"Failed to report $blockId to master; giving up.")
        return
      }
    }
  }

  /**
   * Re-register with the master and report all blocks to it. This will be called by the heart beat
   * thread if our heartbeat to the block manager indicates that we were not registered.
   *
   * Note that this method must be called without any BlockInfo locks held.
   */
  def reregister(): Unit = {
    // TODO: We might need to rate limit re-registering.
    logInfo("BlockManager re-registering with master")
    master.registerBlockManager(blockManagerId, maxMemory, slaveActor)
    reportAllBlocks()
  }

  /**
   * Re-register with the master sometime soon.
   */
  private def asyncReregister(): Unit = {
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
   * For testing. Wait for any pending asynchronous re-registration; otherwise, do nothing.
   */
  def waitForAsyncReregister(): Unit = {
    val task = asyncReregisterTask
    if (task != null) {
      Await.ready(task, Duration.Inf)
    }
  }

  /**
   * Interface to get local block data. Throws an exception if the block cannot be found or
   * cannot be read successfully.
   */
  override def getBlockData(blockId: BlockId): ManagedBuffer = {
    if (blockId.isShuffle) {
      shuffleManager.shuffleBlockManager.getBlockData(blockId.asInstanceOf[ShuffleBlockId])
    } else {
      getLocal(blockId).map(block => new NioManagedBuffer(block.dataAsBytes()))
        .getOrElse(throw new BlockNotFoundException(blockId.toString))
    }
  }

  /**
   * Put the block locally, using the given storage level.
   */
  override def putBlockData(blockId: BlockId, data: ManagedBuffer, level: StorageLevel): Unit = {
    putBytes(blockId, data.nioByteBuffer(), level)
  }

  /**
   * Get the BlockStatus for the block identified by the given ID, if it exists.
   * NOTE: This is mainly for testing, and it doesn't fetch information from Tachyon.
   */
  def getStatus(blockId: BlockId): Option[BlockStatus] = {
    blockInfo.get(blockId).map { info =>
      val memSize = if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
      val diskSize = if (diskStore.contains(blockId)) diskStore.getSize(blockId) else 0L
      // Assume that block is not in Tachyon
      BlockStatus(info.level, memSize, diskSize, 0L)
    }
  }

  /**
   * Get the ids of existing blocks that match the given filter. Note that this will
   * query the blocks stored in the disk block manager (that the block manager
   * may not know of).
   */
  def getMatchingBlockIds(filter: BlockId => Boolean): Seq[BlockId] = {
    (blockInfo.keys ++ diskBlockManager.getAllBlocks()).filter(filter).toSeq
  }

  /**
   * Tell the master about the current storage status of a block. This will send a block update
   * message reflecting the current status, *not* the desired storage level in its block info.
   * For example, a block with MEMORY_AND_DISK set might have fallen out to be only on disk.
   *
   * droppedMemorySize exists to account for when the block is dropped from memory to disk (so
   * it is still valid). This ensures that update in master will compensate for the increase in
   * memory on slave.
   */
  private def reportBlockStatus(
      blockId: BlockId,
      info: BlockInfo,
      status: BlockStatus,
      droppedMemorySize: Long = 0L): Unit = {
    val needReregister = !tryToReportBlockStatus(blockId, info, status, droppedMemorySize)
    if (needReregister) {
      logInfo(s"Got told to re-register updating block $blockId")
      // Re-registering will report our new block for free.
      asyncReregister()
    }
    logDebug(s"Told master about block $blockId")
  }

  /**
   * Actually send a UpdateBlockInfo message. Returns the master's response,
   * which will be true if the block was successfully recorded and false if
   * the slave needs to re-register.
   */
  private def tryToReportBlockStatus(
      blockId: BlockId,
      info: BlockInfo,
      status: BlockStatus,
      droppedMemorySize: Long = 0L): Boolean = {
    if (info.tellMaster) {
      val storageLevel = status.storageLevel
      val inMemSize = Math.max(status.memSize, droppedMemorySize)
      val inTachyonSize = status.tachyonSize
      val onDiskSize = status.diskSize
      master.updateBlockInfo(
        blockManagerId, blockId, storageLevel, inMemSize, onDiskSize, inTachyonSize)
    } else {
      true
    }
  }

  /**
   * Return the updated storage status of the block with the given ID. More specifically, if
   * the block is dropped from memory and possibly added to disk, return the new storage level
   * and the updated in-memory and on-disk sizes.
   */
  private def getCurrentBlockStatus(blockId: BlockId, info: BlockInfo): BlockStatus = {
    info.synchronized {
      info.level match {
        case null =>
          BlockStatus(StorageLevel.NONE, 0L, 0L, 0L)
        case level =>
          val inMem = level.useMemory && memoryStore.contains(blockId)
          val inTachyon = level.useOffHeap && tachyonStore.contains(blockId)
          val onDisk = level.useDisk && diskStore.contains(blockId)
          val deserialized = if (inMem) level.deserialized else false
          val replication = if (inMem || inTachyon || onDisk) level.replication else 1
          val storageLevel = StorageLevel(onDisk, inMem, inTachyon, deserialized, replication)
          val memSize = if (inMem) memoryStore.getSize(blockId) else 0L
          val tachyonSize = if (inTachyon) tachyonStore.getSize(blockId) else 0L
          val diskSize = if (onDisk) diskStore.getSize(blockId) else 0L
          BlockStatus(storageLevel, memSize, diskSize, tachyonSize)
      }
    }
  }

  /**
   * Get locations of an array of blocks.
   */
  private def getLocationBlockIds(blockIds: Array[BlockId]): Array[Seq[BlockManagerId]] = {
    val startTimeMs = System.currentTimeMillis
    val locations = master.getLocations(blockIds).toArray
    logDebug("Got multiple block location in %s".format(Utils.getUsedTimeMs(startTimeMs)))
    locations
  }

  /**
   * Get block from local block manager.
   */
  def getLocal(blockId: BlockId): Option[BlockResult] = {
    logDebug(s"Getting local block $blockId")
    val info = blockInfo.get(blockId).orNull
    if (info != null) {
      info.synchronized {
        // Double check to make sure the block is still there. There is a small chance that the
        // block has been removed by removeBlock (which also synchronizes on the blockInfo object).
        // Note that this only checks metadata tracking. If user intentionally deleted the block
        // on disk or from off heap storage without using removeBlock, this conditional check will
        // still pass but eventually we will get an exception because we can't find the block.
        if (blockInfo.get(blockId).isEmpty) {
          logWarning(s"Block $blockId had been removed")
          return None
        }

        // If another thread is writing the block, wait for it to become ready.
        if (!info.waitForReady()) {
          // If we get here, the block write failed.
          logWarning(s"Block $blockId was marked as failure.")
          return None
        }

        val level = info.level
        logDebug(s"Level for block $blockId is $level")

        // Look for the block in memory
        if (level.useMemory) {
          logDebug(s"Getting block $blockId from memory")
          val block = memoryStore.get(blockId)
          block match {
            case Some(values) =>
              return Some(BlockResult(blockId, values, blockSerde, DataReadMethod.Memory,
                info.size))
            case None =>
              logDebug(s"Block $blockId not found in memory")
          }
        }

        // Look for the block in Tachyon
        if (level.useOffHeap) {
          logDebug(s"Getting block $blockId from tachyon")
          if (tachyonStore.contains(blockId)) {
            tachyonStore.get(blockId) match {
              case Some(block) =>
                return Some(BlockResult(blockId, block, blockSerde, DataReadMethod.Memory,
                  info.size))
              case None =>
                logDebug(s"Block $blockId not found in tachyon")
            }
          }
        }

        // Look for block on disk, potentially storing it back in memory if required
        if (level.useDisk) {
          logDebug(s"Getting block $blockId from disk")
          val block: BlockValue = diskStore.get(blockId).getOrElse(
            throw new BlockException(blockId, s"Block $blockId not found on disk"))

          if (!level.useMemory) {
            // If the block shouldn't be stored in memory, we can just return it
            return Some(BlockResult(blockId, block, blockSerde, DataReadMethod.Disk, info.size))
          } else {
            // Otherwise, we also have to store something in the memory store
            if (!level.deserialized) {
              /* We'll store the bytes in memory if the block's storage level includes
               * "memory serialized", or if it should be cached as objects in memory
               * but we only requested its serialized bytes. */
              // Why do this copy?
//              val copyForMemory = ByteBuffer.allocate(bytes.limit)
//              copyForMemory.put(bytes)
              memoryStore.put(blockId, block, level)
              return Some(BlockResult(blockId, block, blockSerde, DataReadMethod.Disk, info.size))
            } else {
              val values = block.asIterator(blockId, blockSerde)
              // Cache the values before returning them
              val putResult = memoryStore.putIterator(
                blockId, values, level, pin = true, allowPersistToDisk = false)
              // The put may or may not have succeeded, depending on whether there was enough
              // space to unroll the block. Either way, the put here should return an iterator.
              putResult match {
                case SuccessfulPut(stats) =>
                  val block = memoryStore.getPinned(blockId, unpin = true)
                  return Some(BlockResult(blockId, block, blockSerde, DataReadMethod.Disk,
                    info.size))

                case OutOfSpaceFailure(it, stats) =>
                  val block = IteratorValue(it)
                  return Some(BlockResult(blockId, block, blockSerde, DataReadMethod.Disk,
                    info.size))
              }
            }
          }
        }
      }
    } else {
      logDebug(s"Block $blockId not registered locally")
    }
    None
  }

  /**
   * Get block from remote block managers.
   */
  def getRemote(blockId: BlockId): Option[BlockResult] = {
    logDebug(s"Getting remote block $blockId")
    require(blockId != null, "BlockId is null")
    val locations = Random.shuffle(master.getLocations(blockId))
    for (loc <- locations) {
      logDebug(s"Getting remote block $blockId from $loc")
      val data = blockTransferService.fetchBlockSync(
        loc.host, loc.port, loc.executorId, blockId.toString).nioByteBuffer()

      if (data != null) {
        return Some(BlockResult(blockId, ByteBufferValue(data), blockSerde, DataReadMethod.Network,
          data.limit()))
      }
      logDebug(s"The value of block $blockId is null")
    }
    logDebug(s"Block $blockId not found")
    None
  }

  /**
   * Get a block from the block manager (either local or remote).
   */
  def get(blockId: BlockId): Option[BlockResult] = {
    val local = getLocal(blockId)
    if (local.isDefined) {
      logInfo(s"Found block $blockId locally")
      return local
    }
    val remote = getRemote(blockId)
    if (remote.isDefined) {
      logInfo(s"Found block $blockId remotely")
      return remote
    }
    None
  }

  def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      tellMaster: Boolean = true,
      effectiveStorageLevel: Option[StorageLevel] = None): Seq[(BlockId, BlockStatus)] = {
    require(values != null, "Values is null")
    doPut(blockId, IteratorValue(values), level, tellMaster, effectiveStorageLevel)
  }

  /**
   * Put a new block of values to the block manager.
   * Return a list of blocks updated as a result of this put.
   */
  def putArray(
      blockId: BlockId,
      values: Array[Any],
      level: StorageLevel,
      tellMaster: Boolean = true,
      effectiveStorageLevel: Option[StorageLevel] = None): Seq[(BlockId, BlockStatus)] = {
    require(values != null, "Values is null")
    doPut(blockId, ArrayValue(values), level, tellMaster, effectiveStorageLevel)
  }

  /**
   * Put a new block of serialized bytes to the block manager.
   * Return a list of blocks updated as a result of this put.
   */
  def putBytes(
      blockId: BlockId,
      bytes: ByteBuffer,
      level: StorageLevel,
      tellMaster: Boolean = true,
      effectiveStorageLevel: Option[StorageLevel] = None): Seq[(BlockId, BlockStatus)] = {
    require(bytes != null, "Bytes is null")
    bytes.rewind()
    doPut(blockId, ByteBufferValue(bytes), level, tellMaster, effectiveStorageLevel)
  }


  /**
   * Put the given block according to the given level in one of the block stores, replicating
   * the values if necessary.
   *
   * The effective storage level refers to the level according to which the block will actually be
   * handled. This allows the caller to specify an alternate behavior of doPut while preserving
   * the original level specified by the user.
   */
  private def doPut(
      blockId: BlockId,
      data: BlockValue,
      level: StorageLevel,
      tellMaster: Boolean = true,
      effectiveStorageLevel: Option[StorageLevel] = None): Seq[(BlockId, BlockStatus)] = {

    require(blockId != null, "BlockId is null")
    require(level != null && level.isValid, "StorageLevel is null or invalid")
    effectiveStorageLevel.foreach { level =>
      require(level != null && level.isValid, "Effective StorageLevel is null or invalid")
    }

    val startTimeMs = System.currentTimeMillis

    /* Remember the block's storage level so that we can correctly drop it to disk if it needs
     * to be dropped right after it got put into memory. Note, however, that other threads will
     * not be able to get() this block until we call markReady on its BlockInfo. */
    val putBlockInfo = obtainBlockInfoLock(blockId, new BlockInfo(level, tellMaster)) match {
      case None =>
        logWarning(s"Block $blockId already exists on this machine; not re-adding it")
        return Seq.empty
      case Some(info) =>
        info
    }

    // The level we actually use to put the block
    val putLevel = effectiveStorageLevel.getOrElse(level)

    val blockStore: BlockStore = getStoreForLevel(putLevel)

    val shouldReplicate = putLevel.replication > 1

    def doPut(pinBlock: Boolean): UpdatedBlocks = {
      doPutLocal(blockStore, putBlockInfo, blockId, data, putLevel, tellMaster, pinBlock)
    }

    val updatedBlocks: UpdatedBlocks = if (shouldReplicate) {
      doReplicateAndPut(blockId, data, blockStore, putLevel, doPut)
    } else {
      doPut(pinBlock = false)
    }

    logDebug("Putting block %s with%s replication took %s"
      .format(blockId, if (shouldReplicate) "" else "out", Utils.getUsedTimeMs(startTimeMs)))

    updatedBlocks
  }

  /** Type for a sequence of blocks that were dropped/inserted. */
  type UpdatedBlocks = Seq[(BlockId, BlockStatus)]

  /**
   * Performs the actual insertion of the given BlockValue into the local BlockStore. After a
   * successful put, this will mark the BlockInfo as "ready" so that others may start reading.
   *
   * @param tellMaster If true, we will send a message back to the BlockManagerMaster about this
   *                   block. Typically should be true unless there's a good reason not to be.
   * @param pinBlock A pinned block is guaranteed not to be evicted until getPinned(unpin = true)
   *                 is called on the associated BlockStore.
   * @return Returns the blocks that have been dropped and the single block inserted by this put.
   */
  private def doPutLocal(
      blockStore: BlockStore,
      putBlockInfo: BlockInfo,
      blockId: BlockId,
      data: BlockValue,
      putLevel: StorageLevel,
      tellMaster: Boolean,
      pinBlock: Boolean): UpdatedBlocks = {

    val startTimeMs = System.currentTimeMillis

    // Size of the block in bytes
    var size = 0L

    val updatedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]

    putBlockInfo.synchronized {
      logTrace("Put for block %s took %s to get into synchronized block"
        .format(blockId, Utils.getUsedTimeMs(startTimeMs)))

      var success = false
      var result: PutResult = null
      try {
        // Actually put the values
        result = blockStore.put(blockId, data, putLevel, pin = pinBlock)
        size = result.statistics.size

        // Keep track of which blocks are dropped
        result.statistics.droppedBlocks.foreach { updatedBlocks += _ }

        val putBlockStatus = getCurrentBlockStatus(blockId, putBlockInfo)
        if (putBlockStatus.storageLevel != StorageLevel.NONE) {
          // Now that the block is in either the memory, disk, or tachyon store,
          // let other threads read it, and tell the master about it.
          success = true
          putBlockInfo.markReady(size)
          if (tellMaster) {
            reportBlockStatus(blockId, putBlockInfo, putBlockStatus)
          }
          updatedBlocks += ((blockId, putBlockStatus))
        }
      } finally {
        // If we failed in putting the block to memory/disk, notify other possible readers
        // that it has failed, and then remove it from the block info map.
        if (!success) {
          // Note that the remove must happen before markFailure otherwise another thread
          // could've inserted a new BlockInfo before we remove it.
          blockInfo.remove(blockId)
          putBlockInfo.markFailure()
          logWarning(s"Putting block $blockId failed, put result was: $result")
        }
      }
    }
    logDebug("Put block %s locally took %s".format(blockId, Utils.getUsedTimeMs(startTimeMs)))
    updatedBlocks
  }

  /**
   * Responsible for performing replication of the BlockValue, while invoking the putBlock method.
   * This method will start replication early if the data starts out as ByteBuffer, or else will
   * perform replication after storing the block locally.
   */
  private def doReplicateAndPut(
      blockId: BlockId,
      data: BlockValue,
      blockStore: BlockStore,
      putLevel: StorageLevel,
      putBlock: (Boolean) => UpdatedBlocks): UpdatedBlocks = {
    // If we're storing bytes, then initiate the replication before storing them locally.
    // This is faster as data is already serialized and ready to send.
    val replicationFuture = data match {
      case b: ByteBufferValue =>
        // Duplicate doesn't copy the bytes, but just creates a wrapper
        val bufferView = b.buffer.duplicate()
        Future { replicate(blockId, bufferView, putLevel) }
      case _ => null
    }

    val shouldPin = replicationFuture == null
    val updatedBlocks = putBlock(shouldPin)

    // Either we're storing bytes and we asynchronously started replication, or we're storing
    // values and need to serialize and replicate them now:
    if (replicationFuture != null) {
      Await.ready(replicationFuture, Duration.Inf)
    } else {
      val remoteStartTime = System.currentTimeMillis()
      blockStore.getPinned(blockId, unpin = true) match {
        case ByteBufferValue(buffer) =>
          replicate(blockId, buffer, putLevel)
        case block =>
          // If the block was not stored as bytes inherently, turn into a ByteBuffer and dispose
          // of it afterwards.
          val buffer = block.asBytes(blockId, blockSerde)
          replicate(blockId, buffer, putLevel)
          BlockManager.dispose(buffer)
      }
      logDebug(s"Put block $blockId remotely took ${Utils.getUsedTimeMs(remoteStartTime)}")
    }
    updatedBlocks
  }

  /**
   * Returns an appropriate BlockStore for the given level, which will be the first store we'll try.
   */
  private def getStoreForLevel(level: StorageLevel): BlockStore = {
    if (level.useMemory) {
      memoryStore
    } else if (level.useOffHeap) {
      tachyonStore
    } else if (level.useDisk) {
      diskStore
    } else {
      assert(level == StorageLevel.NONE)
      null
    }
  }

  /**
   * Attempts to lock the "BlockInfo" for this block, which would allow us to have uncontested
   * access for inserting the block's data. The BlockInfo must be eventually released by calling
   * markReady() so that others may read and write it.
   * @return Returns None if the BlockInfo already exists, meaning the block is already locked, or
   *         Some(tinfo) if the caller successfully got the lock.
   */
  private def obtainBlockInfoLock(blockId: BlockId, tinfo: BlockInfo): Option[BlockInfo] = {
    // Do atomically !
    val oldBlockOpt = blockInfo.putIfAbsent(blockId, tinfo)
    if (oldBlockOpt.isDefined) {
      if (oldBlockOpt.get.waitForReady()) {
        return None
      }
      // TODO: So the block info exists - but previous attempt to load it (?) failed.
      // What do we do now ? Retry on it ?
      oldBlockOpt
    } else {
      Some(tinfo)
    }
  }

  /**
   * Get peer block managers in the system.
   */
  private def getPeers(forceFetch: Boolean): Seq[BlockManagerId] = {
    peerFetchLock.synchronized {
      val cachedPeersTtl = conf.getInt("spark.storage.cachedPeersTtl", 60 * 1000) // milliseconds
      val timeout = System.currentTimeMillis - lastPeerFetchTime > cachedPeersTtl
      if (cachedPeers == null || forceFetch || timeout) {
        cachedPeers = master.getPeers(blockManagerId).sortBy(_.hashCode)
        lastPeerFetchTime = System.currentTimeMillis
        logDebug("Fetched peers from master: " + cachedPeers.mkString("[", ",", "]"))
      }
      cachedPeers
    }
  }

  /**
   * Replicate block to another node. Not that this is a blocking call that returns after
   * the block has been replicated.
   */
  private def replicate(blockId: BlockId, data: ByteBuffer, level: StorageLevel): Unit = {
    val maxReplicationFailures = conf.getInt("spark.storage.maxReplicationFailures", 1)
    val numPeersToReplicateTo = level.replication - 1
    val peersForReplication = new ArrayBuffer[BlockManagerId]
    val peersReplicatedTo = new ArrayBuffer[BlockManagerId]
    val peersFailedToReplicateTo = new ArrayBuffer[BlockManagerId]
    val tLevel = StorageLevel(
      level.useDisk, level.useMemory, level.useOffHeap, level.deserialized, 1)
    val startTime = System.currentTimeMillis
    val random = new Random(blockId.hashCode)

    var replicationFailed = false
    var failures = 0
    var done = false

    // Get cached list of peers
    peersForReplication ++= getPeers(forceFetch = false)

    // Get a random peer. Note that this selection of a peer is deterministic on the block id.
    // So assuming the list of peers does not change and no replication failures,
    // if there are multiple attempts in the same node to replicate the same block,
    // the same set of peers will be selected.
    def getRandomPeer(): Option[BlockManagerId] = {
      // If replication had failed, then force update the cached list of peers and remove the peers
      // that have been already used
      if (replicationFailed) {
        peersForReplication.clear()
        peersForReplication ++= getPeers(forceFetch = true)
        peersForReplication --= peersReplicatedTo
        peersForReplication --= peersFailedToReplicateTo
      }
      if (!peersForReplication.isEmpty) {
        Some(peersForReplication(random.nextInt(peersForReplication.size)))
      } else {
        None
      }
    }

    // One by one choose a random peer and try uploading the block to it
    // If replication fails (e.g., target peer is down), force the list of cached peers
    // to be re-fetched from driver and then pick another random peer for replication. Also
    // temporarily black list the peer for which replication failed.
    //
    // This selection of a peer and replication is continued in a loop until one of the
    // following 3 conditions is fulfilled:
    // (i) specified number of peers have been replicated to
    // (ii) too many failures in replicating to peers
    // (iii) no peer left to replicate to
    //
    while (!done) {
      getRandomPeer() match {
        case Some(peer) =>
          try {
            val onePeerStartTime = System.currentTimeMillis
            data.rewind()
            logTrace(s"Trying to replicate $blockId of ${data.limit()} bytes to $peer")
            blockTransferService.uploadBlockSync(
              peer.host, peer.port, blockId, new NioManagedBuffer(data), tLevel)
            logTrace(s"Replicated $blockId of ${data.limit()} bytes to $peer in %s ms"
              .format(System.currentTimeMillis - onePeerStartTime))
            peersReplicatedTo += peer
            peersForReplication -= peer
            replicationFailed = false
            if (peersReplicatedTo.size == numPeersToReplicateTo) {
              done = true  // specified number of peers have been replicated to
            }
          } catch {
            case e: Exception =>
              logWarning(s"Failed to replicate $blockId to $peer, failure #$failures", e)
              failures += 1
              replicationFailed = true
              peersFailedToReplicateTo += peer
              if (failures > maxReplicationFailures) { // too many failures in replcating to peers
                done = true
              }
          }
        case None => // no peer left to replicate to
          done = true
      }
    }
    val timeTakeMs = (System.currentTimeMillis - startTime)
    logDebug(s"Replicating $blockId of ${data.limit()} bytes to " +
      s"${peersReplicatedTo.size} peer(s) took $timeTakeMs ms")
    if (peersReplicatedTo.size < numPeersToReplicateTo) {
      logWarning(s"Block $blockId replicated to only " +
        s"${peersReplicatedTo.size} peer(s) instead of $numPeersToReplicateTo peers")
    }
  }

  /**
   * Read a block consisting of a single object.
   */
  def getSingle(blockId: BlockId): Option[Any] = {
    get(blockId).map(_.dataAsIterator().next())
  }

  /**
   * Write a block consisting of a single object.
   */
  def putSingle(
      blockId: BlockId,
      value: Any,
      level: StorageLevel,
      tellMaster: Boolean = true): Seq[(BlockId, BlockStatus)] = {
    putIterator(blockId, Iterator(value), level, tellMaster)
  }

  /**
   * Drop a block from memory, possibly putting it on disk if applicable. Called when the memory
   * store reaches its limit and needs to free up space.
   *
   * Return the block status if the given block has been updated, else None.
   */
  def dropFromMemory(
      blockId: BlockId,
      data: BlockValue): Option[BlockStatus] = {

    logInfo(s"Dropping block $blockId from memory")
    val info = blockInfo.get(blockId).orNull

    // If the block has not already been dropped
    if (info != null) {
      info.synchronized {
        // required ? As of now, this will be invoked only for blocks which are ready
        // But in case this changes in future, adding for consistency sake.
        if (!info.waitForReady()) {
          // If we get here, the block write failed.
          logWarning(s"Block $blockId was marked as failure. Nothing to drop")
          return None
        }

        var blockIsUpdated = false
        val level = info.level

        // Drop to disk, if storage level requires
        if (level.useDisk && !diskStore.contains(blockId)) {
          logInfo(s"Writing block $blockId to disk")
          diskStore.put(blockId, data, level)
          blockIsUpdated = true
        }

        // Actually drop from memory store
        val droppedMemorySize =
          if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
        val blockIsRemoved = memoryStore.remove(blockId)
        if (blockIsRemoved) {
          blockIsUpdated = true
        } else {
          logWarning(s"Block $blockId could not be dropped from memory as it does not exist")
        }

        val status = getCurrentBlockStatus(blockId, info)
        if (info.tellMaster) {
          reportBlockStatus(blockId, info, status, droppedMemorySize)
        }
        if (!level.useDisk) {
          // The block is completely gone from this node; forget it so we can put() it again later.
          blockInfo.remove(blockId)
        }
        if (blockIsUpdated) {
          return Some(status)
        }
      }
    }
    None
  }

  /**
   * Remove all blocks belonging to the given RDD.
   * @return The number of blocks removed.
   */
  def removeRdd(rddId: Int): Int = {
    // TODO: Avoid a linear scan by creating another mapping of RDD.id to blocks.
    logInfo(s"Removing RDD $rddId")
    val blocksToRemove = blockInfo.keys.flatMap(_.asRDDId).filter(_.rddId == rddId)
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster = false) }
    blocksToRemove.size
  }

  /**
   * Remove all blocks belonging to the given broadcast.
   */
  def removeBroadcast(broadcastId: Long, tellMaster: Boolean): Int = {
    logInfo(s"Removing broadcast $broadcastId")
    val blocksToRemove = blockInfo.keys.collect {
      case bid @ BroadcastBlockId(`broadcastId`, _) => bid
    }
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster) }
    blocksToRemove.size
  }

  /**
   * Remove a block from both memory and disk.
   */
  def removeBlock(blockId: BlockId, tellMaster: Boolean = true): Unit = {
    logInfo(s"Removing block $blockId")
    val info = blockInfo.get(blockId).orNull
    if (info != null) {
      info.synchronized {
        // Removals are idempotent in disk store and memory store. At worst, we get a warning.
        val removedFromMemory = memoryStore.remove(blockId)
        val removedFromDisk = diskStore.remove(blockId)
        val removedFromTachyon = if (tachyonInitialized) tachyonStore.remove(blockId) else false
        if (!removedFromMemory && !removedFromDisk && !removedFromTachyon) {
          logWarning(s"Block $blockId could not be removed as it was not found in either " +
            "the disk, memory, or tachyon store")
        }
        blockInfo.remove(blockId)
        if (tellMaster && info.tellMaster) {
          val status = getCurrentBlockStatus(blockId, info)
          reportBlockStatus(blockId, info, status)
        }
      }
    } else {
      // The block has already been removed; do nothing.
      logWarning(s"Asked to remove block $blockId, which does not exist")
    }
  }

  private def dropOldNonBroadcastBlocks(cleanupTime: Long): Unit = {
    logInfo(s"Dropping non broadcast blocks older than $cleanupTime")
    dropOldBlocks(cleanupTime, !_.isBroadcast)
  }

  private def dropOldBroadcastBlocks(cleanupTime: Long): Unit = {
    logInfo(s"Dropping broadcast blocks older than $cleanupTime")
    dropOldBlocks(cleanupTime, _.isBroadcast)
  }

  private def dropOldBlocks(cleanupTime: Long, shouldDrop: (BlockId => Boolean)): Unit = {
    val iterator = blockInfo.getEntrySet.iterator
    while (iterator.hasNext) {
      val entry = iterator.next()
      val (id, info, time) = (entry.getKey, entry.getValue.value, entry.getValue.timestamp)
      if (time < cleanupTime && shouldDrop(id)) {
        info.synchronized {
          val level = info.level
          if (level.useMemory) { memoryStore.remove(id) }
          if (level.useDisk) { diskStore.remove(id) }
          if (level.useOffHeap) { tachyonStore.remove(id) }
          iterator.remove()
          logInfo(s"Dropped block $id")
        }
        val status = getCurrentBlockStatus(id, info)
        reportBlockStatus(id, info, status)
      }
    }
  }

  def stop(): Unit = {
    blockTransferService.close()
    if (shuffleClient ne blockTransferService) {
      // Closing should be idempotent, but maybe not for the NioBlockTransferService.
      shuffleClient.close()
    }
    diskBlockManager.stop()
    actorSystem.stop(slaveActor)
    blockInfo.clear()
    memoryStore.clear()
    diskStore.clear()
    if (tachyonInitialized) {
      tachyonStore.clear()
    }
    metadataCleaner.cancel()
    broadcastCleaner.cancel()
    logInfo("BlockManager stopped")
  }
}


private[spark] object BlockManager extends Logging {
  private val ID_GENERATOR = new IdGenerator

  /** Return the total amount of storage memory available. */
  private def getMaxMemory(conf: SparkConf): Long = {
    val memoryFraction = conf.getDouble("spark.storage.memoryFraction", 0.6)
    val safetyFraction = conf.getDouble("spark.storage.safetyFraction", 0.9)
    (Runtime.getRuntime.maxMemory * memoryFraction * safetyFraction).toLong
  }

  /**
   * Attempt to clean up a ByteBuffer if it is memory-mapped. This uses an *unsafe* Sun API that
   * might cause errors if one attempts to read from the unmapped buffer, but it's better than
   * waiting for the GC to find it because that could lead to huge numbers of open files. There's
   * unfortunately no standard API to do this.
   */
  def dispose(buffer: ByteBuffer): Unit = {
    if (buffer != null && buffer.isInstanceOf[MappedByteBuffer]) {
      logTrace(s"Unmapping $buffer")
      if (buffer.asInstanceOf[DirectBuffer].cleaner() != null) {
        buffer.asInstanceOf[DirectBuffer].cleaner().clean()
      }
    }
  }

  def blockIdsToBlockManagers(
      blockIds: Array[BlockId],
      env: SparkEnv,
      blockManagerMaster: BlockManagerMaster = null): Map[BlockId, Seq[BlockManagerId]] = {

    // blockManagerMaster != null is used in tests
    assert(env != null || blockManagerMaster != null)
    val blockLocations: Seq[Seq[BlockManagerId]] = if (blockManagerMaster == null) {
      env.blockManager.getLocationBlockIds(blockIds)
    } else {
      blockManagerMaster.getLocations(blockIds)
    }

    val blockManagers = new HashMap[BlockId, Seq[BlockManagerId]]
    for (i <- 0 until blockIds.length) {
      blockManagers(blockIds(i)) = blockLocations(i)
    }
    blockManagers.toMap
  }

  def blockIdsToExecutorIds(
      blockIds: Array[BlockId],
      env: SparkEnv,
      blockManagerMaster: BlockManagerMaster = null): Map[BlockId, Seq[String]] = {
    blockIdsToBlockManagers(blockIds, env, blockManagerMaster).mapValues(s => s.map(_.executorId))
  }

  def blockIdsToHosts(
      blockIds: Array[BlockId],
      env: SparkEnv,
      blockManagerMaster: BlockManagerMaster = null): Map[BlockId, Seq[String]] = {
    blockIdsToBlockManagers(blockIds, env, blockManagerMaster).mapValues(s => s.map(_.host))
  }
}
