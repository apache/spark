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

import java.io._
import java.nio.{ByteBuffer, MappedByteBuffer}

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Random
import scala.util.control.NonFatal

import sun.nio.ch.DirectBuffer

import org.apache.spark._
import org.apache.spark.executor.{DataReadMethod, ShuffleWriteMetrics}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.memory.MemoryManager
import org.apache.spark.network._
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.ExternalShuffleClient
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.serializer.{Serializer, SerializerInstance}
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.storage.memory._
import org.apache.spark.util._

/* Class for returning a fetched block and associated metrics. */
private[spark] class BlockResult(
    val data: Iterator[Any],
    val readMethod: DataReadMethod.Value,
    val bytes: Long)

/**
 * Manager running on every node (driver and executors) which provides interfaces for putting and
 * retrieving blocks both locally and remotely into various stores (memory, disk, and off-heap).
 *
 * Note that [[initialize()]] must be called before the BlockManager is usable.
 */
private[spark] class BlockManager(
    executorId: String,
    rpcEnv: RpcEnv,
    val master: BlockManagerMaster,
    defaultSerializer: Serializer,
    val conf: SparkConf,
    memoryManager: MemoryManager,
    mapOutputTracker: MapOutputTracker,
    shuffleManager: ShuffleManager,
    blockTransferService: BlockTransferService,
    securityManager: SecurityManager,
    numUsableCores: Int)
  extends BlockDataManager with Logging {

  private[spark] val externalShuffleServiceEnabled =
    conf.getBoolean("spark.shuffle.service.enabled", false)

  val diskBlockManager = {
    // Only perform cleanup if an external service is not serving our shuffle files.
    val deleteFilesOnStop =
      !externalShuffleServiceEnabled || executorId == SparkContext.DRIVER_IDENTIFIER
    new DiskBlockManager(conf, deleteFilesOnStop)
  }

  private[storage] val blockInfoManager = new BlockInfoManager

  private val futureExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("block-manager-future", 128))

  // Actual storage of where blocks are kept
  private[spark] val memoryStore = new MemoryStore(conf, this, memoryManager)
  private[spark] val diskStore = new DiskStore(conf, diskBlockManager)
  memoryManager.setMemoryStore(memoryStore)

  // Note: depending on the memory manager, `maxStorageMemory` may actually vary over time.
  // However, since we use this only for reporting and logging, what we actually want here is
  // the absolute maximum value that `maxStorageMemory` can ever possibly reach. We may need
  // to revisit whether reporting this value as the "max" is intuitive to the user.
  private val maxMemory = memoryManager.maxStorageMemory

  // Port used by the external shuffle service. In Yarn mode, this may be already be
  // set through the Hadoop configuration as the server is launched in the Yarn NM.
  private val externalShuffleServicePort = {
    val tmpPort = Utils.getSparkOrYarnConfig(conf, "spark.shuffle.service.port", "7337").toInt
    if (tmpPort == 0) {
      // for testing, we set "spark.shuffle.service.port" to 0 in the yarn config, so yarn finds
      // an open port.  But we still need to tell our spark apps the right port to use.  So
      // only if the yarn config has the port set to 0, we prefer the value in the spark config
      conf.get("spark.shuffle.service.port").toInt
    } else {
      tmpPort
    }
  }

  var blockManagerId: BlockManagerId = _

  // Address of the server that serves this executor's shuffle files. This is either an external
  // service, or just our own Executor's BlockManager.
  private[spark] var shuffleServerId: BlockManagerId = _

  // Client to read other executors' shuffle files. This is either an external service, or just the
  // standard BlockTransferService to directly connect to other Executors.
  private[spark] val shuffleClient = if (externalShuffleServiceEnabled) {
    val transConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numUsableCores)
    new ExternalShuffleClient(transConf, securityManager, securityManager.isAuthenticationEnabled(),
      securityManager.isSaslEncryptionEnabled())
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
  // Max number of failures before this block manager refreshes the block locations from the driver
  private val maxFailuresBeforeLocationRefresh =
    conf.getInt("spark.block.failures.beforeLocationRefresh", 5)

  private val slaveEndpoint = rpcEnv.setupEndpoint(
    "BlockManagerEndpoint" + BlockManager.ID_GENERATOR.next,
    new BlockManagerSlaveEndpoint(rpcEnv, this, mapOutputTracker))

  // Pending re-registration action being executed asynchronously or null if none is pending.
  // Accesses should synchronize on asyncReregisterLock.
  private var asyncReregisterTask: Future[Unit] = null
  private val asyncReregisterLock = new Object

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
   * Initializes the BlockManager with the given appId. This is not performed in the constructor as
   * the appId may not be known at BlockManager instantiation time (in particular for the driver,
   * where it is only learned after registration with the TaskScheduler).
   *
   * This method initializes the BlockTransferService and ShuffleClient, registers with the
   * BlockManagerMaster, starts the BlockManagerWorker endpoint, and registers with a local shuffle
   * service if configured.
   */
  def initialize(appId: String): Unit = {
    blockTransferService.init(this)
    shuffleClient.init(appId)

    blockManagerId = BlockManagerId(
      executorId, blockTransferService.hostName, blockTransferService.port)

    shuffleServerId = if (externalShuffleServiceEnabled) {
      logInfo(s"external shuffle service port = $externalShuffleServicePort")
      BlockManagerId(executorId, blockTransferService.hostName, externalShuffleServicePort)
    } else {
      blockManagerId
    }

    master.registerBlockManager(blockManagerId, maxMemory, slaveEndpoint)

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
      shuffleManager.shortName)

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
          logError(s"Failed to connect to external shuffle server, will retry ${MAX_ATTEMPTS - i}"
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
    logInfo(s"Reporting ${blockInfoManager.size} blocks to the master.")
    for ((blockId, info) <- blockInfoManager.entries) {
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
    master.registerBlockManager(blockManagerId, maxMemory, slaveEndpoint)
    reportAllBlocks()
  }

  /**
   * Re-register with the master sometime soon.
   */
  private def asyncReregister(): Unit = {
    asyncReregisterLock.synchronized {
      if (asyncReregisterTask == null) {
        asyncReregisterTask = Future[Unit] {
          // This is a blocking action and should run in futureExecutionContext which is a cached
          // thread pool
          reregister()
          asyncReregisterLock.synchronized {
            asyncReregisterTask = null
          }
        }(futureExecutionContext)
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
      shuffleManager.shuffleBlockResolver.getBlockData(blockId.asInstanceOf[ShuffleBlockId])
    } else {
      getLocalBytes(blockId) match {
        case Some(buffer) => new BlockManagerManagedBuffer(this, blockId, buffer)
        case None => throw new BlockNotFoundException(blockId.toString)
      }
    }
  }

  /**
   * Put the block locally, using the given storage level.
   */
  override def putBlockData(blockId: BlockId, data: ManagedBuffer, level: StorageLevel): Boolean = {
    putBytes(blockId, data.nioByteBuffer(), level)
  }

  /**
   * Get the BlockStatus for the block identified by the given ID, if it exists.
   * NOTE: This is mainly for testing, and it doesn't fetch information from external block store.
   */
  def getStatus(blockId: BlockId): Option[BlockStatus] = {
    blockInfoManager.get(blockId).map { info =>
      val memSize = if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
      val diskSize = if (diskStore.contains(blockId)) diskStore.getSize(blockId) else 0L
      BlockStatus(info.level, memSize = memSize, diskSize = diskSize)
    }
  }

  /**
   * Get the ids of existing blocks that match the given filter. Note that this will
   * query the blocks stored in the disk block manager (that the block manager
   * may not know of).
   */
  def getMatchingBlockIds(filter: BlockId => Boolean): Seq[BlockId] = {
    // The `toArray` is necessary here in order to force the list to be materialized so that we
    // don't try to serialize a lazy iterator when responding to client requests.
    (blockInfoManager.entries.map(_._1) ++ diskBlockManager.getAllBlocks())
      .filter(filter)
      .toArray
      .toSeq
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
      val onDiskSize = status.diskSize
      master.updateBlockInfo(blockManagerId, blockId, storageLevel, inMemSize, onDiskSize)
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
          BlockStatus(StorageLevel.NONE, memSize = 0L, diskSize = 0L)
        case level =>
          val inMem = level.useMemory && memoryStore.contains(blockId)
          val onDisk = level.useDisk && diskStore.contains(blockId)
          val deserialized = if (inMem) level.deserialized else false
          val replication = if (inMem  || onDisk) level.replication else 1
          val storageLevel =
            StorageLevel(onDisk, inMem, deserialized, replication)
          val memSize = if (inMem) memoryStore.getSize(blockId) else 0L
          val diskSize = if (onDisk) diskStore.getSize(blockId) else 0L
          BlockStatus(storageLevel, memSize, diskSize)
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
   * Get block from local block manager as an iterator of Java objects.
   */
  def getLocalValues(blockId: BlockId): Option[BlockResult] = {
    logDebug(s"Getting local block $blockId")
    blockInfoManager.lockForReading(blockId) match {
      case None =>
        logDebug(s"Block $blockId was not found")
        None
      case Some(info) =>
        val level = info.level
        logDebug(s"Level for block $blockId is $level")
        if (level.useMemory && memoryStore.contains(blockId)) {
          val iter: Iterator[Any] = if (level.deserialized) {
            memoryStore.getValues(blockId).get
          } else {
            dataDeserialize(blockId, memoryStore.getBytes(blockId).get)
          }
          val ci = CompletionIterator[Any, Iterator[Any]](iter, releaseLock(blockId))
          Some(new BlockResult(ci, DataReadMethod.Memory, info.size))
        } else if (level.useDisk && diskStore.contains(blockId)) {
          val iterToReturn: Iterator[Any] = {
            val diskBytes = diskStore.getBytes(blockId)
            if (level.deserialized) {
              val diskIterator = dataDeserialize(blockId, diskBytes)
              if (level.useMemory) {
                // Cache the values before returning them
                memoryStore.putIterator(blockId, diskIterator, level) match {
                  case Left(iter) =>
                    // The memory store put() failed, so it returned the iterator back to us:
                    iter
                  case Right(_) =>
                    // The put() succeeded, so we can read the values back:
                    memoryStore.getValues(blockId).get
                }
              } else {
                diskIterator
              }
            } else { // storage level is serialized
              if (level.useMemory) {
                // Cache the bytes back into memory to speed up subsequent reads.
                val putSucceeded = memoryStore.putBytes(blockId, diskBytes.limit(), () => {
                  // https://issues.apache.org/jira/browse/SPARK-6076
                  // If the file size is bigger than the free memory, OOM will happen. So if we
                  // cannot put it into MemoryStore, copyForMemory should not be created. That's why
                  // this action is put into a `() => ByteBuffer` and created lazily.
                  val copyForMemory = ByteBuffer.allocate(diskBytes.limit)
                  copyForMemory.put(diskBytes)
                })
                if (putSucceeded) {
                  dataDeserialize(blockId, memoryStore.getBytes(blockId).get)
                } else {
                  dataDeserialize(blockId, diskBytes)
                }
              } else {
                dataDeserialize(blockId, diskBytes)
              }
            }
          }
          val ci = CompletionIterator[Any, Iterator[Any]](iterToReturn, releaseLock(blockId))
          Some(new BlockResult(ci, DataReadMethod.Disk, info.size))
        } else {
          releaseLock(blockId)
          throw new SparkException(s"Block $blockId was not found even though it's read-locked")
        }
    }
  }

  /**
   * Get block from the local block manager as serialized bytes.
   */
  def getLocalBytes(blockId: BlockId): Option[ByteBuffer] = {
    logDebug(s"Getting local block $blockId as bytes")
    // As an optimization for map output fetches, if the block is for a shuffle, return it
    // without acquiring a lock; the disk store never deletes (recent) items so this should work
    if (blockId.isShuffle) {
      val shuffleBlockResolver = shuffleManager.shuffleBlockResolver
      // TODO: This should gracefully handle case where local block is not available. Currently
      // downstream code will throw an exception.
      Option(
        shuffleBlockResolver.getBlockData(blockId.asInstanceOf[ShuffleBlockId]).nioByteBuffer())
    } else {
      blockInfoManager.lockForReading(blockId).map { info => doGetLocalBytes(blockId, info) }
    }
  }

  /**
   * Get block from the local block manager as serialized bytes.
   *
   * Must be called while holding a read lock on the block.
   * Releases the read lock upon exception; keeps the read lock upon successful return.
   */
  private def doGetLocalBytes(blockId: BlockId, info: BlockInfo): ByteBuffer = {
    val level = info.level
    logDebug(s"Level for block $blockId is $level")
    // In order, try to read the serialized bytes from memory, then from disk, then fall back to
    // serializing in-memory objects, and, finally, throw an exception if the block does not exist.
    if (level.deserialized) {
      // Try to avoid expensive serialization by reading a pre-serialized copy from disk:
      if (level.useDisk && diskStore.contains(blockId)) {
        // Note: we purposely do not try to put the block back into memory here. Since this branch
        // handles deserialized blocks, this block may only be cached in memory as objects, not
        // serialized bytes. Because the caller only requested bytes, it doesn't make sense to
        // cache the block's deserialized objects since that caching may not have a payoff.
        diskStore.getBytes(blockId)
      } else if (level.useMemory && memoryStore.contains(blockId)) {
        // The block was not found on disk, so serialize an in-memory copy:
        dataSerialize(blockId, memoryStore.getValues(blockId).get)
      } else {
        releaseLock(blockId)
        throw new SparkException(s"Block $blockId was not found even though it's read-locked")
      }
    } else {  // storage level is serialized
      if (level.useMemory && memoryStore.contains(blockId)) {
        memoryStore.getBytes(blockId).get
      } else if (level.useDisk && diskStore.contains(blockId)) {
        val bytes = diskStore.getBytes(blockId)
        if (level.useMemory) {
          // Cache the bytes back into memory to speed up subsequent reads.
          val memoryStorePutSucceeded = memoryStore.putBytes(blockId, bytes.limit(), () => {
            // https://issues.apache.org/jira/browse/SPARK-6076
            // If the file size is bigger than the free memory, OOM will happen. So if we cannot
            // put it into MemoryStore, copyForMemory should not be created. That's why this
            // action is put into a `() => ByteBuffer` and created lazily.
            val copyForMemory = ByteBuffer.allocate(bytes.limit)
            copyForMemory.put(bytes)
          })
          if (memoryStorePutSucceeded) {
            memoryStore.getBytes(blockId).get
          } else {
            bytes.rewind()
            bytes
          }
        } else {
          bytes
        }
      } else {
        releaseLock(blockId)
        throw new SparkException(s"Block $blockId was not found even though it's read-locked")
      }
    }
  }

  /**
   * Get block from remote block managers.
   *
   * This does not acquire a lock on this block in this JVM.
   */
  def getRemoteValues(blockId: BlockId): Option[BlockResult] = {
    getRemoteBytes(blockId).map { data =>
      new BlockResult(dataDeserialize(blockId, data), DataReadMethod.Network, data.limit())
    }
  }

  /**
   * Return a list of locations for the given block, prioritizing the local machine since
   * multiple block managers can share the same host.
   */
  private def getLocations(blockId: BlockId): Seq[BlockManagerId] = {
    val locs = Random.shuffle(master.getLocations(blockId))
    val (preferredLocs, otherLocs) = locs.partition { loc => blockManagerId.host == loc.host }
    preferredLocs ++ otherLocs
  }

  /**
   * Get block from remote block managers as serialized bytes.
   */
  def getRemoteBytes(blockId: BlockId): Option[ByteBuffer] = {
    logDebug(s"Getting remote block $blockId")
    require(blockId != null, "BlockId is null")
    var runningFailureCount = 0
    var totalFailureCount = 0
    val locations = getLocations(blockId)
    val maxFetchFailures = locations.size
    var locationIterator = locations.iterator
    while (locationIterator.hasNext) {
      val loc = locationIterator.next()
      logDebug(s"Getting remote block $blockId from $loc")
      val data = try {
        blockTransferService.fetchBlockSync(
          loc.host, loc.port, loc.executorId, blockId.toString).nioByteBuffer()
      } catch {
        case NonFatal(e) =>
          runningFailureCount += 1
          totalFailureCount += 1

          if (totalFailureCount >= maxFetchFailures) {
            // Give up trying anymore locations. Either we've tried all of the original locations,
            // or we've refreshed the list of locations from the master, and have still
            // hit failures after trying locations from the refreshed list.
            throw new BlockFetchException(s"Failed to fetch block after" +
              s" ${totalFailureCount} fetch failures. Most recent failure cause:", e)
          }

          logWarning(s"Failed to fetch remote block $blockId " +
            s"from $loc (failed attempt $runningFailureCount)", e)

          // If there is a large number of executors then locations list can contain a
          // large number of stale entries causing a large number of retries that may
          // take a significant amount of time. To get rid of these stale entries
          // we refresh the block locations after a certain number of fetch failures
          if (runningFailureCount >= maxFailuresBeforeLocationRefresh) {
            locationIterator = getLocations(blockId).iterator
            logDebug(s"Refreshed locations from the driver " +
              s"after ${runningFailureCount} fetch failures.")
            runningFailureCount = 0
          }

          // This location failed, so we retry fetch from a different one by returning null here
          null
      }

      if (data != null) {
        return Some(data)
      }
      logDebug(s"The value of block $blockId is null")
    }
    logDebug(s"Block $blockId not found")
    None
  }

  /**
   * Get a block from the block manager (either local or remote).
   *
   * This acquires a read lock on the block if the block was stored locally and does not acquire
   * any locks if the block was fetched from a remote block manager. The read lock will
   * automatically be freed once the result's `data` iterator is fully consumed.
   */
  def get(blockId: BlockId): Option[BlockResult] = {
    val local = getLocalValues(blockId)
    if (local.isDefined) {
      logInfo(s"Found block $blockId locally")
      return local
    }
    val remote = getRemoteValues(blockId)
    if (remote.isDefined) {
      logInfo(s"Found block $blockId remotely")
      return remote
    }
    None
  }

  /**
   * Downgrades an exclusive write lock to a shared read lock.
   */
  def downgradeLock(blockId: BlockId): Unit = {
    blockInfoManager.downgradeLock(blockId)
  }

  /**
   * Release a lock on the given block.
   */
  def releaseLock(blockId: BlockId): Unit = {
    blockInfoManager.unlock(blockId)
  }

  /**
   * Registers a task with the BlockManager in order to initialize per-task bookkeeping structures.
   */
  def registerTask(taskAttemptId: Long): Unit = {
    blockInfoManager.registerTask(taskAttemptId)
  }

  /**
   * Release all locks for the given task.
   *
   * @return the blocks whose locks were released.
   */
  def releaseAllLocksForTask(taskAttemptId: Long): Seq[BlockId] = {
    blockInfoManager.releaseAllLocksForTask(taskAttemptId)
  }

  /**
   * Retrieve the given block if it exists, otherwise call the provided `makeIterator` method
   * to compute the block, persist it, and return its values.
   *
   * @return either a BlockResult if the block was successfully cached, or an iterator if the block
   *         could not be cached.
   */
  def getOrElseUpdate(
      blockId: BlockId,
      level: StorageLevel,
      makeIterator: () => Iterator[Any]): Either[BlockResult, Iterator[Any]] = {
    // Initially we hold no locks on this block.
    doPutIterator(blockId, makeIterator, level, keepReadLock = true) match {
      case None =>
        // doPut() didn't hand work back to us, so the block already existed or was successfully
        // stored. Therefore, we now hold a read lock on the block.
        val blockResult = getLocalValues(blockId).getOrElse {
          // Since we held a read lock between the doPut() and get() calls, the block should not
          // have been evicted, so get() not returning the block indicates some internal error.
          releaseLock(blockId)
          throw new SparkException(s"get() failed for block $blockId even though we held a lock")
        }
        // We already hold a read lock on the block from the doPut() call and getLocalValues()
        // acquires the lock again, so we need to call releaseLock() here so that the net number
        // of lock acquisitions is 1 (since the caller will only call release() once).
        releaseLock(blockId)
        Left(blockResult)
      case Some(iter) =>
        // The put failed, likely because the data was too large to fit in memory and could not be
        // dropped to disk. Therefore, we need to pass the input iterator back to the caller so
        // that they can decide what to do with the values (e.g. process them without caching).
       Right(iter)
    }
  }

  /**
   * @return true if the block was stored or false if an error occurred.
   */
  def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      tellMaster: Boolean = true): Boolean = {
    require(values != null, "Values is null")
    // If doPut() didn't hand work back to us, then block already existed or was successfully stored
    doPutIterator(blockId, () => values, level, tellMaster).isEmpty
  }

  /**
   * A short circuited method to get a block writer that can write data directly to disk.
   * The Block will be appended to the File specified by filename. Callers should handle error
   * cases.
   */
  def getDiskWriter(
      blockId: BlockId,
      file: File,
      serializerInstance: SerializerInstance,
      bufferSize: Int,
      writeMetrics: ShuffleWriteMetrics): DiskBlockObjectWriter = {
    val compressStream: OutputStream => OutputStream = wrapForCompression(blockId, _)
    val syncWrites = conf.getBoolean("spark.shuffle.sync", false)
    new DiskBlockObjectWriter(file, serializerInstance, bufferSize, compressStream,
      syncWrites, writeMetrics, blockId)
  }

  /**
   * Put a new block of serialized bytes to the block manager.
   *
   * @return true if the block was stored or false if an error occurred.
   */
  def putBytes(
      blockId: BlockId,
      bytes: ByteBuffer,
      level: StorageLevel,
      tellMaster: Boolean = true): Boolean = {
    require(bytes != null, "Bytes is null")
    doPutBytes(blockId, bytes, level, tellMaster)
  }

  /**
   * Put the given bytes according to the given level in one of the block stores, replicating
   * the values if necessary.
   *
   * If the block already exists, this method will not overwrite it.
   *
   * @param keepReadLock if true, this method will hold the read lock when it returns (even if the
   *                     block already exists). If false, this method will hold no locks when it
   *                     returns.
   * @return true if the block was already present or if the put succeeded, false otherwise.
   */
  private def doPutBytes(
      blockId: BlockId,
      bytes: ByteBuffer,
      level: StorageLevel,
      tellMaster: Boolean = true,
      keepReadLock: Boolean = false): Boolean = {
    doPut(blockId, level, tellMaster = tellMaster, keepReadLock = keepReadLock) { putBlockInfo =>
      val startTimeMs = System.currentTimeMillis
      // Since we're storing bytes, initiate the replication before storing them locally.
      // This is faster as data is already serialized and ready to send.
      val replicationFuture = if (level.replication > 1) {
        // Duplicate doesn't copy the bytes, but just creates a wrapper
        val bufferView = bytes.duplicate()
        Future {
          // This is a blocking action and should run in futureExecutionContext which is a cached
          // thread pool
          replicate(blockId, bufferView, level)
        }(futureExecutionContext)
      } else {
        null
      }

      bytes.rewind()
      val size = bytes.limit()

      if (level.useMemory) {
        // Put it in memory first, even if it also has useDisk set to true;
        // We will drop it to disk later if the memory store can't hold it.
        val putSucceeded = if (level.deserialized) {
          val values = dataDeserialize(blockId, bytes.duplicate())
          memoryStore.putIterator(blockId, values, level).isRight
        } else {
          memoryStore.putBytes(blockId, size, () => bytes)
        }
        if (!putSucceeded && level.useDisk) {
          logWarning(s"Persisting block $blockId to disk instead.")
          diskStore.putBytes(blockId, bytes)
        }
      } else if (level.useDisk) {
        diskStore.putBytes(blockId, bytes)
      }

      val putBlockStatus = getCurrentBlockStatus(blockId, putBlockInfo)
      val blockWasSuccessfullyStored = putBlockStatus.storageLevel.isValid
      if (blockWasSuccessfullyStored) {
        // Now that the block is in either the memory, externalBlockStore, or disk store,
        // tell the master about it.
        putBlockInfo.size = size
        if (tellMaster) {
          reportBlockStatus(blockId, putBlockInfo, putBlockStatus)
        }
        Option(TaskContext.get()).foreach { c =>
          c.taskMetrics().incUpdatedBlockStatuses(Seq((blockId, putBlockStatus)))
        }
      }
      logDebug("Put block %s locally took %s".format(blockId, Utils.getUsedTimeMs(startTimeMs)))
      if (level.replication > 1) {
        // Wait for asynchronous replication to finish
        Await.ready(replicationFuture, Duration.Inf)
      }
      if (blockWasSuccessfullyStored) {
        None
      } else {
        Some(bytes)
      }
    }.isEmpty
  }

  /**
   * Helper method used to abstract common code from [[doPutBytes()]] and [[doPutIterator()]].
   *
   * @param putBody a function which attempts the actual put() and returns None on success
   *                or Some on failure.
   */
  private def doPut[T](
      blockId: BlockId,
      level: StorageLevel,
      tellMaster: Boolean,
      keepReadLock: Boolean)(putBody: BlockInfo => Option[T]): Option[T] = {

    require(blockId != null, "BlockId is null")
    require(level != null && level.isValid, "StorageLevel is null or invalid")

    val putBlockInfo = {
      val newInfo = new BlockInfo(level, tellMaster)
      if (blockInfoManager.lockNewBlockForWriting(blockId, newInfo)) {
        newInfo
      } else {
        logWarning(s"Block $blockId already exists on this machine; not re-adding it")
        if (!keepReadLock) {
          // lockNewBlockForWriting returned a read lock on the existing block, so we must free it:
          releaseLock(blockId)
        }
        return None
      }
    }

    val startTimeMs = System.currentTimeMillis
    var blockWasSuccessfullyStored: Boolean = false
    val result: Option[T] = try {
      val res = putBody(putBlockInfo)
      blockWasSuccessfullyStored = res.isEmpty
      res
    } finally {
      if (blockWasSuccessfullyStored) {
        if (keepReadLock) {
          blockInfoManager.downgradeLock(blockId)
        } else {
          blockInfoManager.unlock(blockId)
        }
      } else {
        blockInfoManager.removeBlock(blockId)
        logWarning(s"Putting block $blockId failed")
      }
    }
    if (level.replication > 1) {
      logDebug("Putting block %s with replication took %s"
        .format(blockId, Utils.getUsedTimeMs(startTimeMs)))
    } else {
      logDebug("Putting block %s without replication took %s"
        .format(blockId, Utils.getUsedTimeMs(startTimeMs)))
    }
    result
  }

  /**
   * Put the given block according to the given level in one of the block stores, replicating
   * the values if necessary.
   *
   * If the block already exists, this method will not overwrite it.
   *
   * @param keepReadLock if true, this method will hold the read lock when it returns (even if the
   *                     block already exists). If false, this method will hold no locks when it
   *                     returns.
   * @return None if the block was already present or if the put succeeded, or Some(iterator)
   *         if the put failed.
   */
  private def doPutIterator(
      blockId: BlockId,
      iterator: () => Iterator[Any],
      level: StorageLevel,
      tellMaster: Boolean = true,
      keepReadLock: Boolean = false): Option[Iterator[Any]] = {
    doPut(blockId, level, tellMaster = tellMaster, keepReadLock = keepReadLock) { putBlockInfo =>
      val startTimeMs = System.currentTimeMillis
      var iteratorFromFailedMemoryStorePut: Option[Iterator[Any]] = None
      // Size of the block in bytes
      var size = 0L
      if (level.useMemory) {
        // Put it in memory first, even if it also has useDisk set to true;
        // We will drop it to disk later if the memory store can't hold it.
        memoryStore.putIterator(blockId, iterator(), level) match {
          case Right(s) =>
            size = s
          case Left(iter) =>
            // Not enough space to unroll this block; drop to disk if applicable
            if (level.useDisk) {
              logWarning(s"Persisting block $blockId to disk instead.")
              diskStore.put(blockId) { fileOutputStream =>
                dataSerializeStream(blockId, fileOutputStream, iter)
              }
              size = diskStore.getSize(blockId)
            } else {
              iteratorFromFailedMemoryStorePut = Some(iter)
            }
        }
      } else if (level.useDisk) {
        diskStore.put(blockId) { fileOutputStream =>
          dataSerializeStream(blockId, fileOutputStream, iterator())
        }
        size = diskStore.getSize(blockId)
      }

      val putBlockStatus = getCurrentBlockStatus(blockId, putBlockInfo)
      val blockWasSuccessfullyStored = putBlockStatus.storageLevel.isValid
      if (blockWasSuccessfullyStored) {
        // Now that the block is in either the memory, externalBlockStore, or disk store,
        // tell the master about it.
        putBlockInfo.size = size
        if (tellMaster) {
          reportBlockStatus(blockId, putBlockInfo, putBlockStatus)
        }
        Option(TaskContext.get()).foreach { c =>
          c.taskMetrics().incUpdatedBlockStatuses(Seq((blockId, putBlockStatus)))
        }
        logDebug("Put block %s locally took %s".format(blockId, Utils.getUsedTimeMs(startTimeMs)))
        if (level.replication > 1) {
          val remoteStartTime = System.currentTimeMillis
          val bytesToReplicate = doGetLocalBytes(blockId, putBlockInfo)
          try {
            replicate(blockId, bytesToReplicate, level)
          } finally {
            BlockManager.dispose(bytesToReplicate)
          }
          logDebug("Put block %s remotely took %s"
            .format(blockId, Utils.getUsedTimeMs(remoteStartTime)))
        }
      }
      assert(blockWasSuccessfullyStored == iteratorFromFailedMemoryStorePut.isEmpty)
      iteratorFromFailedMemoryStorePut
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
    val tLevel = StorageLevel(level.useDisk, level.useMemory, level.deserialized, 1)
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
   *
   * @return true if the block was stored or false if the block was already stored or an
   *         error occurred.
   */
  def putSingle(
      blockId: BlockId,
      value: Any,
      level: StorageLevel,
      tellMaster: Boolean = true): Boolean = {
    putIterator(blockId, Iterator(value), level, tellMaster)
  }

  /**
   * Drop a block from memory, possibly putting it on disk if applicable. Called when the memory
   * store reaches its limit and needs to free up space.
   *
   * If `data` is not put on disk, it won't be created.
   *
   * The caller of this method must hold a write lock on the block before calling this method.
   * This method does not release the write lock.
   *
   * @return the block's new effective StorageLevel.
   */
  def dropFromMemory(
      blockId: BlockId,
      data: () => Either[Array[Any], ByteBuffer]): StorageLevel = {
    logInfo(s"Dropping block $blockId from memory")
    val info = blockInfoManager.assertBlockIsLockedForWriting(blockId)
    var blockIsUpdated = false
    val level = info.level

    // Drop to disk, if storage level requires
    if (level.useDisk && !diskStore.contains(blockId)) {
      logInfo(s"Writing block $blockId to disk")
      data() match {
        case Left(elements) =>
          diskStore.put(blockId) { fileOutputStream =>
            dataSerializeStream(blockId, fileOutputStream, elements.toIterator)
          }
        case Right(bytes) =>
          diskStore.putBytes(blockId, bytes)
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
    if (blockIsUpdated) {
      Option(TaskContext.get()).foreach { c =>
        c.taskMetrics().incUpdatedBlockStatuses(Seq((blockId, status)))
      }
    }
    status.storageLevel
  }

  /**
   * Remove all blocks belonging to the given RDD.
   *
   * @return The number of blocks removed.
   */
  def removeRdd(rddId: Int): Int = {
    // TODO: Avoid a linear scan by creating another mapping of RDD.id to blocks.
    logInfo(s"Removing RDD $rddId")
    val blocksToRemove = blockInfoManager.entries.flatMap(_._1.asRDDId).filter(_.rddId == rddId)
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster = false) }
    blocksToRemove.size
  }

  /**
   * Remove all blocks belonging to the given broadcast.
   */
  def removeBroadcast(broadcastId: Long, tellMaster: Boolean): Int = {
    logDebug(s"Removing broadcast $broadcastId")
    val blocksToRemove = blockInfoManager.entries.map(_._1).collect {
      case bid @ BroadcastBlockId(`broadcastId`, _) => bid
    }
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster) }
    blocksToRemove.size
  }

  /**
   * Remove a block from both memory and disk.
   */
  def removeBlock(blockId: BlockId, tellMaster: Boolean = true): Unit = {
    logDebug(s"Removing block $blockId")
    blockInfoManager.lockForWriting(blockId) match {
      case None =>
        // The block has already been removed; do nothing.
        logWarning(s"Asked to remove block $blockId, which does not exist")
      case Some(info) =>
        // Removals are idempotent in disk store and memory store. At worst, we get a warning.
        val removedFromMemory = memoryStore.remove(blockId)
        val removedFromDisk = diskStore.remove(blockId)
        if (!removedFromMemory && !removedFromDisk) {
          logWarning(s"Block $blockId could not be removed as it was not found in either " +
            "the disk, memory, or external block store")
        }
        blockInfoManager.removeBlock(blockId)
        if (tellMaster && info.tellMaster) {
          val status = getCurrentBlockStatus(blockId, info)
          reportBlockStatus(blockId, info, status)
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
      values: Iterator[Any]): Unit = {
    val byteStream = new BufferedOutputStream(outputStream)
    val ser = defaultSerializer.newInstance()
    ser.serializeStream(wrapForCompression(blockId, byteStream)).writeAll(values).close()
  }

  /** Serializes into a byte buffer. */
  def dataSerialize(blockId: BlockId, values: Iterator[Any]): ByteBuffer = {
    val byteStream = new ByteBufferOutputStream(4096)
    dataSerializeStream(blockId, byteStream, values)
    byteStream.toByteBuffer
  }

  /**
   * Deserializes a ByteBuffer into an iterator of values and disposes of it when the end of
   * the iterator is reached.
   */
  def dataDeserialize(blockId: BlockId, bytes: ByteBuffer): Iterator[Any] = {
    bytes.rewind()
    dataDeserializeStream(blockId, new ByteBufferInputStream(bytes, true))
  }

  /**
   * Deserializes a InputStream into an iterator of values and disposes of it when the end of
   * the iterator is reached.
   */
  def dataDeserializeStream(blockId: BlockId, inputStream: InputStream): Iterator[Any] = {
    val stream = new BufferedInputStream(inputStream)
    defaultSerializer
      .newInstance()
      .deserializeStream(wrapForCompression(blockId, stream))
      .asIterator
  }

  def stop(): Unit = {
    blockTransferService.close()
    if (shuffleClient ne blockTransferService) {
      // Closing should be idempotent, but maybe not for the NioBlockTransferService.
      shuffleClient.close()
    }
    diskBlockManager.stop()
    rpcEnv.stop(slaveEndpoint)
    blockInfoManager.clear()
    memoryStore.clear()
    futureExecutionContext.shutdownNow()
    logInfo("BlockManager stopped")
  }
}


private[spark] object BlockManager extends Logging {
  private val ID_GENERATOR = new IdGenerator

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

  def blockIdsToHosts(
      blockIds: Array[BlockId],
      env: SparkEnv,
      blockManagerMaster: BlockManagerMaster = null): Map[BlockId, Seq[String]] = {

    // blockManagerMaster != null is used in tests
    assert(env != null || blockManagerMaster != null)
    val blockLocations: Seq[Seq[BlockManagerId]] = if (blockManagerMaster == null) {
      env.blockManager.getLocationBlockIds(blockIds)
    } else {
      blockManagerMaster.getLocations(blockIds)
    }

    val blockManagers = new HashMap[BlockId, Seq[String]]
    for (i <- 0 until blockIds.length) {
      blockManagers(blockIds(i)) = blockLocations(i).map(_.host)
    }
    blockManagers.toMap
  }
}
