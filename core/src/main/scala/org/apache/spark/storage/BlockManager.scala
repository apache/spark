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

import java.io.{BufferedOutputStream, ByteArrayOutputStream, File, InputStream, OutputStream}
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
import org.apache.spark.io.CompressionCodec
import org.apache.spark.network._
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.netty.{SparkTransportConf, NettyBlockTransferService}
import org.apache.spark.network.shuffle.ExternalShuffleClient
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo
import org.apache.spark.network.util.{ConfigProvider, TransportConf}
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.shuffle.hash.HashShuffleManager
import org.apache.spark.util._

private[spark] sealed trait BlockValues
private[spark] case class ByteBufferValues(buffer: ByteBuffer) extends BlockValues
private[spark] case class IteratorValues(iterator: Iterator[Any]) extends BlockValues
private[spark] case class ArrayValues(buffer: Array[Any]) extends BlockValues

/* Class for returning a fetched block and associated metrics. */
private[spark] class BlockResult(
    val data: Iterator[Any],
    readMethod: DataReadMethod.Value,
    bytes: Long) {
  val inputMetrics = new InputMetrics(readMethod)
  inputMetrics.bytesRead = bytes
}

/**
 * Manager running on every node (driver and executors) which provides interfaces for putting and
 * retrieving blocks both locally and remotely into various stores (memory, disk, and off-heap).
 *
 * Note that #initialize() must be called before the BlockManager is usable.
 */
private[spark] class BlockManager(
    executorId: String,
    actorSystem: ActorSystem,
    val master: BlockManagerMaster,
    defaultSerializer: Serializer,
    maxMemory: Long,
    val conf: SparkConf,
    mapOutputTracker: MapOutputTracker,
    shuffleManager: ShuffleManager,
    blockTransferService: BlockTransferService,
    securityManager: SecurityManager,
    numUsableCores: Int)
  extends BlockDataManager with Logging {

  val diskBlockManager = new DiskBlockManager(this, conf)

  private val blockInfo = new TimeStampedHashMap[BlockId, BlockInfo]

  // Actual storage of where blocks are kept
  private var tachyonInitialized = false
  private[spark] val memoryStore = new MemoryStore(this, maxMemory)
  private[spark] val diskStore = new DiskStore(this, diskBlockManager)
  private[spark] lazy val tachyonStore: TachyonStore = {
    val storeDir = conf.get("spark.tachyonStore.baseDir", "/tmp_spark_tachyon")
    val appFolderName = conf.get("spark.tachyonStore.folderName")
    val tachyonStorePath = s"$storeDir/$appFolderName/${this.executorId}"
    val tachyonMaster = conf.get("spark.tachyonStore.url",  "tachyon://localhost:19998")
    val tachyonBlockManager =
      new TachyonBlockManager(this, tachyonStorePath, tachyonMaster)
    tachyonInitialized = true
    new TachyonStore(this, tachyonBlockManager)
  }

  private[spark]
  val externalShuffleServiceEnabled = conf.getBoolean("spark.shuffle.service.enabled", false)

  // Port used by the external shuffle service. In Yarn mode, this may be already be
  // set through the Hadoop configuration as the server is launched in the Yarn NM.
  private val externalShuffleServicePort =
    Utils.getSparkOrYarnConfig(conf, "spark.shuffle.service.port", "7337").toInt

  // Check that we're not using external shuffle service with consolidated shuffle files.
  if (externalShuffleServiceEnabled
      && conf.getBoolean("spark.shuffle.consolidateFiles", false)
      && shuffleManager.isInstanceOf[HashShuffleManager]) {
    throw new UnsupportedOperationException("Cannot use external shuffle service with consolidated"
      + " shuffle files in hash-based shuffle. Please disable spark.shuffle.consolidateFiles or "
      + " switch to sort-based shuffle.")
  }

  var blockManagerId: BlockManagerId = _

  // Address of the server that serves this executor's shuffle files. This is either an external
  // service, or just our own Executor's BlockManager.
  private[spark] var shuffleServerId: BlockManagerId = _

  // Client to read other executors' shuffle files. This is either an external service, or just the
  // standard BlockTranserService to directly connect to other Executors.
  private[spark] val shuffleClient = if (externalShuffleServiceEnabled) {
    val transConf = SparkTransportConf.fromSparkConf(conf, numUsableCores)
    new ExternalShuffleClient(transConf, securityManager, securityManager.isAuthenticationEnabled())
  } else {
    blockTransferService
  }

  // Whether to compress broadcast variables that are stored
  private val compressBroadcast = conf.getBoolean("spark.broadcast.compress", true)
  // Whether to compress shuffle output that are stored
  private val compressShuffle = conf.getBoolean("spark.shuffle.compress", true)
  // Whether to compress RDD partitions that are stored serialized
  private val compressRdds = conf.getBoolean("spark.rdd.compress", false)
  // Whether to compress shuffle output temporarily spilled to disk
  private val compressShuffleSpill = conf.getBoolean("spark.shuffle.spill.compress", true)

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

  /* The compression codec to use. Note that the "lazy" val is necessary because we want to delay
   * the initialization of the compression codec until it is first used. The reason is that a Spark
   * program could be using a user-defined codec in a third party jar, which is loaded in
   * Executor.updateDependencies. When the BlockManager is initialized, user level jars hasn't been
   * loaded yet. */
  private lazy val compressionCodec: CompressionCodec = CompressionCodec.createCodec(conf)

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
      blockTransferService: BlockTransferService,
      securityManager: SecurityManager,
      numUsableCores: Int) = {
    this(execId, actorSystem, master, serializer, BlockManager.getMaxMemory(conf),
      conf, mapOutputTracker, shuffleManager, blockTransferService, securityManager, numUsableCores)
  }

  /**
   * Initializes the BlockManager with the given appId. This is not performed in the constructor as
   * the appId may not be known at BlockManager instantiation time (in particular for the driver,
   * where it is only learned after registration with the TaskScheduler).
   *
   * This method initializes the BlockTransferService and ShuffleClient, registers with the
   * BlockManagerMaster, starts the BlockManagerWorker actor, and registers with a local shuffle
   * service if configured.
   */
  def initialize(appId: String): Unit = {
    blockTransferService.init(this)
    shuffleClient.init(appId)

    blockManagerId = BlockManagerId(
      executorId, blockTransferService.hostName, blockTransferService.port)

    shuffleServerId = if (externalShuffleServiceEnabled) {
      BlockManagerId(executorId, blockTransferService.hostName, externalShuffleServicePort)
    } else {
      blockManagerId
    }

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
      val blockBytesOpt = doGetLocal(blockId, asBlockResult = false)
        .asInstanceOf[Option[ByteBuffer]]
      if (blockBytesOpt.isDefined) {
        val buffer = blockBytesOpt.get
        new NioManagedBuffer(buffer)
      } else {
        throw new BlockNotFoundException(blockId.toString)
      }
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
    doGetLocal(blockId, asBlockResult = true).asInstanceOf[Option[BlockResult]]
  }

  /**
   * Get block from the local block manager as serialized bytes.
   */
  def getLocalBytes(blockId: BlockId): Option[ByteBuffer] = {
    logDebug(s"Getting local block $blockId as bytes")
    // As an optimization for map output fetches, if the block is for a shuffle, return it
    // without acquiring a lock; the disk store never deletes (recent) items so this should work
    if (blockId.isShuffle) {
      val shuffleBlockManager = shuffleManager.shuffleBlockManager
      shuffleBlockManager.getBytes(blockId.asInstanceOf[ShuffleBlockId]) match {
        case Some(bytes) =>
          Some(bytes)
        case None =>
          throw new BlockException(
            blockId, s"Block $blockId not found on disk, though it should be")
      }
    } else {
      doGetLocal(blockId, asBlockResult = false).asInstanceOf[Option[ByteBuffer]]
    }
  }

  private def doGetLocal(blockId: BlockId, asBlockResult: Boolean): Option[Any] = {
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
          val result = if (asBlockResult) {
            memoryStore.getValues(blockId).map(new BlockResult(_, DataReadMethod.Memory, info.size))
          } else {
            memoryStore.getBytes(blockId)
          }
          result match {
            case Some(values) =>
              return result
            case None =>
              logDebug(s"Block $blockId not found in memory")
          }
        }

        // Look for the block in Tachyon
        if (level.useOffHeap) {
          logDebug(s"Getting block $blockId from tachyon")
          if (tachyonStore.contains(blockId)) {
            tachyonStore.getBytes(blockId) match {
              case Some(bytes) =>
                if (!asBlockResult) {
                  return Some(bytes)
                } else {
                  return Some(new BlockResult(
                    dataDeserialize(blockId, bytes), DataReadMethod.Memory, info.size))
                }
              case None =>
                logDebug(s"Block $blockId not found in tachyon")
            }
          }
        }

        // Look for block on disk, potentially storing it back in memory if required
        if (level.useDisk) {
          logDebug(s"Getting block $blockId from disk")
          val bytes: ByteBuffer = diskStore.getBytes(blockId) match {
            case Some(b) => b
            case None =>
              throw new BlockException(
                blockId, s"Block $blockId not found on disk, though it should be")
          }
          assert(0 == bytes.position())

          if (!level.useMemory) {
            // If the block shouldn't be stored in memory, we can just return it
            if (asBlockResult) {
              return Some(new BlockResult(dataDeserialize(blockId, bytes), DataReadMethod.Disk,
                info.size))
            } else {
              return Some(bytes)
            }
          } else {
            // Otherwise, we also have to store something in the memory store
            if (!level.deserialized || !asBlockResult) {
              /* We'll store the bytes in memory if the block's storage level includes
               * "memory serialized", or if it should be cached as objects in memory
               * but we only requested its serialized bytes. */
              val copyForMemory = ByteBuffer.allocate(bytes.limit)
              copyForMemory.put(bytes)
              memoryStore.putBytes(blockId, copyForMemory, level)
              bytes.rewind()
            }
            if (!asBlockResult) {
              return Some(bytes)
            } else {
              val values = dataDeserialize(blockId, bytes)
              if (level.deserialized) {
                // Cache the values before returning them
                val putResult = memoryStore.putIterator(
                  blockId, values, level, returnValues = true, allowPersistToDisk = false)
                // The put may or may not have succeeded, depending on whether there was enough
                // space to unroll the block. Either way, the put here should return an iterator.
                putResult.data match {
                  case Left(it) =>
                    return Some(new BlockResult(it, DataReadMethod.Disk, info.size))
                  case _ =>
                    // This only happens if we dropped the values back to disk (which is never)
                    throw new SparkException("Memory store did not return an iterator!")
                }
              } else {
                return Some(new BlockResult(values, DataReadMethod.Disk, info.size))
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
    doGetRemote(blockId, asBlockResult = true).asInstanceOf[Option[BlockResult]]
  }

  /**
   * Get block from remote block managers as serialized bytes.
   */
  def getRemoteBytes(blockId: BlockId): Option[ByteBuffer] = {
    logDebug(s"Getting remote block $blockId as bytes")
    doGetRemote(blockId, asBlockResult = false).asInstanceOf[Option[ByteBuffer]]
  }

  private def doGetRemote(blockId: BlockId, asBlockResult: Boolean): Option[Any] = {
    require(blockId != null, "BlockId is null")
    val locations = Random.shuffle(master.getLocations(blockId))
    for (loc <- locations) {
      logDebug(s"Getting remote block $blockId from $loc")
      val data = blockTransferService.fetchBlockSync(
        loc.host, loc.port, loc.executorId, blockId.toString).nioByteBuffer()

      if (data != null) {
        if (asBlockResult) {
          return Some(new BlockResult(
            dataDeserialize(blockId, data),
            DataReadMethod.Network,
            data.limit()))
        } else {
          return Some(data)
        }
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
    doPut(blockId, IteratorValues(values), level, tellMaster, effectiveStorageLevel)
  }

  /**
   * A short circuited method to get a block writer that can write data directly to disk.
   * The Block will be appended to the File specified by filename. Callers should handle error
   * cases.
   */
  def getDiskWriter(
      blockId: BlockId,
      file: File,
      serializer: Serializer,
      bufferSize: Int,
      writeMetrics: ShuffleWriteMetrics): BlockObjectWriter = {
    val compressStream: OutputStream => OutputStream = wrapForCompression(blockId, _)
    val syncWrites = conf.getBoolean("spark.shuffle.sync", false)
    new DiskBlockObjectWriter(blockId, file, serializer, bufferSize, compressStream, syncWrites,
      writeMetrics)
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
    doPut(blockId, ArrayValues(values), level, tellMaster, effectiveStorageLevel)
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
    doPut(blockId, ByteBufferValues(bytes), level, tellMaster, effectiveStorageLevel)
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
      data: BlockValues,
      level: StorageLevel,
      tellMaster: Boolean = true,
      effectiveStorageLevel: Option[StorageLevel] = None)
    : Seq[(BlockId, BlockStatus)] = {

    require(blockId != null, "BlockId is null")
    require(level != null && level.isValid, "StorageLevel is null or invalid")
    effectiveStorageLevel.foreach { level =>
      require(level != null && level.isValid, "Effective StorageLevel is null or invalid")
    }

    // Return value
    val updatedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]

    /* Remember the block's storage level so that we can correctly drop it to disk if it needs
     * to be dropped right after it got put into memory. Note, however, that other threads will
     * not be able to get() this block until we call markReady on its BlockInfo. */
    val putBlockInfo = {
      val tinfo = new BlockInfo(level, tellMaster)
      // Do atomically !
      val oldBlockOpt = blockInfo.putIfAbsent(blockId, tinfo)
      if (oldBlockOpt.isDefined) {
        if (oldBlockOpt.get.waitForReady()) {
          logWarning(s"Block $blockId already exists on this machine; not re-adding it")
          return updatedBlocks
        }
        // TODO: So the block info exists - but previous attempt to load it (?) failed.
        // What do we do now ? Retry on it ?
        oldBlockOpt.get
      } else {
        tinfo
      }
    }

    val startTimeMs = System.currentTimeMillis

    /* If we're storing values and we need to replicate the data, we'll want access to the values,
     * but because our put will read the whole iterator, there will be no values left. For the
     * case where the put serializes data, we'll remember the bytes, above; but for the case where
     * it doesn't, such as deserialized storage, let's rely on the put returning an Iterator. */
    var valuesAfterPut: Iterator[Any] = null

    // Ditto for the bytes after the put
    var bytesAfterPut: ByteBuffer = null

    // Size of the block in bytes
    var size = 0L

    // The level we actually use to put the block
    val putLevel = effectiveStorageLevel.getOrElse(level)

    // If we're storing bytes, then initiate the replication before storing them locally.
    // This is faster as data is already serialized and ready to send.
    val replicationFuture = data match {
      case b: ByteBufferValues if putLevel.replication > 1 =>
        // Duplicate doesn't copy the bytes, but just creates a wrapper
        val bufferView = b.buffer.duplicate()
        Future { replicate(blockId, bufferView, putLevel) }
      case _ => null
    }

    putBlockInfo.synchronized {
      logTrace("Put for block %s took %s to get into synchronized block"
        .format(blockId, Utils.getUsedTimeMs(startTimeMs)))

      var marked = false
      try {
        // returnValues - Whether to return the values put
        // blockStore - The type of storage to put these values into
        val (returnValues, blockStore: BlockStore) = {
          if (putLevel.useMemory) {
            // Put it in memory first, even if it also has useDisk set to true;
            // We will drop it to disk later if the memory store can't hold it.
            (true, memoryStore)
          } else if (putLevel.useOffHeap) {
            // Use tachyon for off-heap storage
            (false, tachyonStore)
          } else if (putLevel.useDisk) {
            // Don't get back the bytes from put unless we replicate them
            (putLevel.replication > 1, diskStore)
          } else {
            assert(putLevel == StorageLevel.NONE)
            throw new BlockException(
              blockId, s"Attempted to put block $blockId without specifying storage level!")
          }
        }

        // Actually put the values
        val result = data match {
          case IteratorValues(iterator) =>
            blockStore.putIterator(blockId, iterator, putLevel, returnValues)
          case ArrayValues(array) =>
            blockStore.putArray(blockId, array, putLevel, returnValues)
          case ByteBufferValues(bytes) =>
            bytes.rewind()
            blockStore.putBytes(blockId, bytes, putLevel)
        }
        size = result.size
        result.data match {
          case Left (newIterator) if putLevel.useMemory => valuesAfterPut = newIterator
          case Right (newBytes) => bytesAfterPut = newBytes
          case _ =>
        }

        // Keep track of which blocks are dropped from memory
        if (putLevel.useMemory) {
          result.droppedBlocks.foreach { updatedBlocks += _ }
        }

        val putBlockStatus = getCurrentBlockStatus(blockId, putBlockInfo)
        if (putBlockStatus.storageLevel != StorageLevel.NONE) {
          // Now that the block is in either the memory, tachyon, or disk store,
          // let other threads read it, and tell the master about it.
          marked = true
          putBlockInfo.markReady(size)
          if (tellMaster) {
            reportBlockStatus(blockId, putBlockInfo, putBlockStatus)
          }
          updatedBlocks += ((blockId, putBlockStatus))
        }
      } finally {
        // If we failed in putting the block to memory/disk, notify other possible readers
        // that it has failed, and then remove it from the block info map.
        if (!marked) {
          // Note that the remove must happen before markFailure otherwise another thread
          // could've inserted a new BlockInfo before we remove it.
          blockInfo.remove(blockId)
          putBlockInfo.markFailure()
          logWarning(s"Putting block $blockId failed")
        }
      }
    }
    logDebug("Put block %s locally took %s".format(blockId, Utils.getUsedTimeMs(startTimeMs)))

    // Either we're storing bytes and we asynchronously started replication, or we're storing
    // values and need to serialize and replicate them now:
    if (putLevel.replication > 1) {
      data match {
        case ByteBufferValues(bytes) =>
          if (replicationFuture != null) {
            Await.ready(replicationFuture, Duration.Inf)
          }
        case _ =>
          val remoteStartTime = System.currentTimeMillis
          // Serialize the block if not already done
          if (bytesAfterPut == null) {
            if (valuesAfterPut == null) {
              throw new SparkException(
                "Underlying put returned neither an Iterator nor bytes! This shouldn't happen.")
            }
            bytesAfterPut = dataSerialize(blockId, valuesAfterPut)
          }
          replicate(blockId, bytesAfterPut, putLevel)
          logDebug("Put block %s remotely took %s"
            .format(blockId, Utils.getUsedTimeMs(remoteStartTime)))
      }
    }

    BlockManager.dispose(bytesAfterPut)

    if (putLevel.replication > 1) {
      logDebug("Putting block %s with replication took %s"
        .format(blockId, Utils.getUsedTimeMs(startTimeMs)))
    } else {
      logDebug("Putting block %s without replication took %s"
        .format(blockId, Utils.getUsedTimeMs(startTimeMs)))
    }

    updatedBlocks
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
              peer.host, peer.port, peer.executorId, blockId, new NioManagedBuffer(data), tLevel)
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
    get(blockId).map(_.data.next())
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
      data: Either[Array[Any], ByteBuffer]): Option[BlockStatus] = {

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
        } else if (blockInfo.get(blockId).isEmpty) {
          logWarning(s"Block $blockId was already dropped.")
          return None
        }
        var blockIsUpdated = false
        val level = info.level

        // Drop to disk, if storage level requires
        if (level.useDisk && !diskStore.contains(blockId)) {
          logInfo(s"Writing block $blockId to disk")
          data match {
            case Left(elements) =>
              diskStore.putArray(blockId, elements, level, returnValues = false)
            case Right(bytes) =>
              diskStore.putBytes(blockId, bytes, level)
          }
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

  private def shouldCompress(blockId: BlockId): Boolean = {
    blockId match {
      case _: ShuffleBlockId => compressShuffle
      case _: BroadcastBlockId => compressBroadcast
      case _: RDDBlockId => compressRdds
      case _: TempLocalBlockId => compressShuffleSpill
      case _: TempShuffleBlockId => compressShuffle
      case _ => false
    }
  }

  /**
   * Wrap an output stream for compression if block compression is enabled for its block type
   */
  def wrapForCompression(blockId: BlockId, s: OutputStream): OutputStream = {
    if (shouldCompress(blockId)) compressionCodec.compressedOutputStream(s) else s
  }

  /**
   * Wrap an input stream for compression if block compression is enabled for its block type
   */
  def wrapForCompression(blockId: BlockId, s: InputStream): InputStream = {
    if (shouldCompress(blockId)) compressionCodec.compressedInputStream(s) else s
  }

  /** Serializes into a stream. */
  def dataSerializeStream(
      blockId: BlockId,
      outputStream: OutputStream,
      values: Iterator[Any],
      serializer: Serializer = defaultSerializer): Unit = {
    val byteStream = new BufferedOutputStream(outputStream)
    val ser = serializer.newInstance()
    ser.serializeStream(wrapForCompression(blockId, byteStream)).writeAll(values).close()
  }

  /** Serializes into a byte buffer. */
  def dataSerialize(
      blockId: BlockId,
      values: Iterator[Any],
      serializer: Serializer = defaultSerializer): ByteBuffer = {
    val byteStream = new ByteArrayOutputStream(4096)
    dataSerializeStream(blockId, byteStream, values, serializer)
    ByteBuffer.wrap(byteStream.toByteArray)
  }

  /**
   * Deserializes a ByteBuffer into an iterator of values and disposes of it when the end of
   * the iterator is reached.
   */
  def dataDeserialize(
      blockId: BlockId,
      bytes: ByteBuffer,
      serializer: Serializer = defaultSerializer): Iterator[Any] = {
    bytes.rewind()
    val stream = wrapForCompression(blockId, new ByteBufferInputStream(bytes, true))
    serializer.newInstance().deserializeStream(stream).asIterator
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
