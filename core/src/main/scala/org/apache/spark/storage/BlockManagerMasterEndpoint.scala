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

import java.io.IOException
import java.util.{HashMap => JHashMap}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.util.Random
import scala.util.control.NonFatal

import com.google.common.cache.CacheBuilder

import org.apache.spark.{MapOutputTrackerMaster, SparkConf}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.network.shuffle.ExternalBlockStoreClient
import org.apache.spark.rpc.{IsolatedRpcEndpoint, RpcCallContext, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.{CoarseGrainedClusterMessages, CoarseGrainedSchedulerBackend}
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.{RpcUtils, ThreadUtils, Utils}

/**
 * BlockManagerMasterEndpoint is an [[IsolatedRpcEndpoint]] on the master node to track statuses
 * of all the storage endpoints' block managers.
 */
private[spark]
class BlockManagerMasterEndpoint(
    override val rpcEnv: RpcEnv,
    val isLocal: Boolean,
    conf: SparkConf,
    listenerBus: LiveListenerBus,
    externalBlockStoreClient: Option[ExternalBlockStoreClient],
    blockManagerInfo: mutable.Map[BlockManagerId, BlockManagerInfo],
    mapOutputTracker: MapOutputTrackerMaster)
  extends IsolatedRpcEndpoint with Logging {

  // Mapping from executor id to the block manager's local disk directories.
  private val executorIdToLocalDirs =
    CacheBuilder
      .newBuilder()
      .maximumSize(conf.get(config.STORAGE_LOCAL_DISK_BY_EXECUTORS_CACHE_SIZE))
      .build[String, Array[String]]()

  // Mapping from external shuffle service block manager id to the block statuses.
  private val blockStatusByShuffleService =
    new mutable.HashMap[BlockManagerId, BlockStatusPerBlockId]

  // Mapping from executor ID to block manager ID.
  private val blockManagerIdByExecutor = new mutable.HashMap[String, BlockManagerId]

  // Set of block managers which are decommissioning
  private val decommissioningBlockManagerSet = new mutable.HashSet[BlockManagerId]

  // Mapping from block id to the set of block managers that have the block.
  private val blockLocations = new JHashMap[BlockId, mutable.HashSet[BlockManagerId]]

  // Mapping from host name to shuffle (mergers) services where the current app
  // registered an executor in the past. Older hosts are removed when the
  // maxRetainedMergerLocations size is reached in favor of newer locations.
  private val shuffleMergerLocations = new mutable.LinkedHashMap[String, BlockManagerId]()

  // Maximum number of merger locations to cache
  private val maxRetainedMergerLocations = conf.get(config.SHUFFLE_MERGER_MAX_RETAINED_LOCATIONS)

  private val askThreadPool =
    ThreadUtils.newDaemonCachedThreadPool("block-manager-ask-thread-pool", 100)
  private implicit val askExecutionContext = ExecutionContext.fromExecutorService(askThreadPool)

  private val topologyMapper = {
    val topologyMapperClassName = conf.get(
      config.STORAGE_REPLICATION_TOPOLOGY_MAPPER)
    val clazz = Utils.classForName(topologyMapperClassName)
    val mapper =
      clazz.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[TopologyMapper]
    logInfo(s"Using $topologyMapperClassName for getting topology information")
    mapper
  }

  private val executorTimeoutMs = Utils.executorTimeoutMs(conf)
  private val blockManagerInfoCleaner = {
    val cleaningDelay = Math.floorDiv(executorTimeoutMs, 2L)
    val executor = ThreadUtils.newDaemonSingleThreadScheduledExecutor("blockManagerInfo-cleaner")
    executor.scheduleWithFixedDelay(() => cleanBlockManagerInfo(), cleaningDelay, cleaningDelay,
      TimeUnit.MILLISECONDS)
    executor
  }

  val proactivelyReplicate = conf.get(config.STORAGE_REPLICATION_PROACTIVE)

  val defaultRpcTimeout = RpcUtils.askRpcTimeout(conf)

  private val pushBasedShuffleEnabled = Utils.isPushBasedShuffleEnabled(conf)

  logInfo("BlockManagerMasterEndpoint up")
  // same as `conf.get(config.SHUFFLE_SERVICE_ENABLED)
  //   && conf.get(config.SHUFFLE_SERVICE_FETCH_RDD_ENABLED)`
  private val externalShuffleServiceRddFetchEnabled: Boolean = externalBlockStoreClient.isDefined
  private val externalShuffleServicePort: Int = StorageUtils.externalShuffleServicePort(conf)

  private lazy val driverEndpoint =
    RpcUtils.makeDriverRef(CoarseGrainedSchedulerBackend.ENDPOINT_NAME, conf, rpcEnv)

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterBlockManager(id, localDirs, maxOnHeapMemSize, maxOffHeapMemSize, endpoint) =>
      context.reply(register(id, localDirs, maxOnHeapMemSize, maxOffHeapMemSize, endpoint))

    case _updateBlockInfo @
        UpdateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size) =>
      val isSuccess = updateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size)
      context.reply(isSuccess)
      // SPARK-30594: we should not post `SparkListenerBlockUpdated` when updateBlockInfo
      // returns false since the block info would be updated again later.
      if (isSuccess) {
        listenerBus.post(SparkListenerBlockUpdated(BlockUpdatedInfo(_updateBlockInfo)))
      }

    case GetLocations(blockId) =>
      context.reply(getLocations(blockId))

    case GetLocationsAndStatus(blockId, requesterHost) =>
      context.reply(getLocationsAndStatus(blockId, requesterHost))

    case GetLocationsMultipleBlockIds(blockIds) =>
      context.reply(getLocationsMultipleBlockIds(blockIds))

    case GetPeers(blockManagerId) =>
      context.reply(getPeers(blockManagerId))

    case GetExecutorEndpointRef(executorId) =>
      context.reply(getExecutorEndpointRef(executorId))

    case GetMemoryStatus =>
      context.reply(memoryStatus)

    case GetStorageStatus =>
      context.reply(storageStatus)

    case GetBlockStatus(blockId, askStorageEndpoints) =>
      context.reply(blockStatus(blockId, askStorageEndpoints))

    case GetShufflePushMergerLocations(numMergersNeeded, hostsToFilter) =>
      context.reply(getShufflePushMergerLocations(numMergersNeeded, hostsToFilter))

    case RemoveShufflePushMergerLocation(host) =>
      context.reply(removeShufflePushMergerLocation(host))

    case IsExecutorAlive(executorId) =>
      context.reply(blockManagerIdByExecutor.contains(executorId))

    case GetMatchingBlockIds(filter, askStorageEndpoints) =>
      context.reply(getMatchingBlockIds(filter, askStorageEndpoints))

    case RemoveRdd(rddId) =>
      context.reply(removeRdd(rddId))

    case RemoveShuffle(shuffleId) =>
      context.reply(removeShuffle(shuffleId))

    case RemoveBroadcast(broadcastId, removeFromDriver) =>
      context.reply(removeBroadcast(broadcastId, removeFromDriver))

    case RemoveBlock(blockId) =>
      removeBlockFromWorkers(blockId)
      context.reply(true)

    case RemoveExecutor(execId) =>
      removeExecutor(execId)
      context.reply(true)

    case DecommissionBlockManagers(executorIds) =>
      // Mark corresponding BlockManagers as being decommissioning by adding them to
      // decommissioningBlockManagerSet, so they won't be used to replicate or migrate blocks.
      // Note that BlockManagerStorageEndpoint will be notified about decommissioning when the
      // executor is notified(see BlockManager.decommissionSelf), so we don't need to send the
      // notification here.
      val bms = executorIds.flatMap(blockManagerIdByExecutor.get)
      logInfo(s"Mark BlockManagers (${bms.mkString(", ")}) as being decommissioning.")
      decommissioningBlockManagerSet ++= bms
      context.reply(true)

    case GetReplicateInfoForRDDBlocks(blockManagerId) =>
      context.reply(getReplicateInfoForRDDBlocks(blockManagerId))

    case StopBlockManagerMaster =>
      context.reply(true)
      stop()
  }

  /**
   * A function that used to handle the failures when removing blocks. In general, the failure
   * should be considered as non-fatal since it won't cause any correctness issue. Therefore,
   * this function would prefer to log the exception and return the default value. We only throw
   * the exception when there's a TimeoutException from an active executor, which implies the
   * unhealthy status of the executor while the driver still not be aware of it.
   * @param blockType should be one of "RDD", "shuffle", "broadcast", "block", used for log
   * @param blockId the string value of a certain block id, used for log
   * @param bmId the BlockManagerId of the BlockManager, where we're trying to remove the block
   * @param defaultValue the return value of a failure removal. e.g., 0 means no blocks are removed
   * @tparam T the generic type for defaultValue, Int or Boolean.
   * @return the defaultValue or throw exception if the executor is active but reply late.
   */
  private def handleBlockRemovalFailure[T](
      blockType: String,
      blockId: String,
      bmId: BlockManagerId,
      defaultValue: T): PartialFunction[Throwable, T] = {
    case e: IOException =>
      logWarning(s"Error trying to remove $blockType $blockId" +
        s" from block manager $bmId", e)
      defaultValue

    case t: TimeoutException =>
      val executorId = bmId.executorId
      val isAlive = try {
        driverEndpoint.askSync[Boolean](CoarseGrainedClusterMessages.IsExecutorAlive(executorId))
      } catch {
        // ignore the non-fatal error from driverEndpoint since the caller doesn't really
        // care about the return result of removing blocks. And so we could avoid breaking
        // down the whole application.
        case NonFatal(e) =>
          logError(s"Fail to know the executor $executorId is alive or not.", e)
          false
      }
      if (!isAlive) {
        logWarning(s"Error trying to remove $blockType $blockId. " +
          s"The executor $executorId may have been lost.", t)
        defaultValue
      } else {
        throw t
      }
  }

  private def removeRdd(rddId: Int): Future[Seq[Int]] = {
    // First remove the metadata for the given RDD, and then asynchronously remove the blocks
    // from the storage endpoints.

    // The message sent to the storage endpoints to remove the RDD
    val removeMsg = RemoveRdd(rddId)

    // Find all blocks for the given RDD, remove the block from both blockLocations and
    // the blockManagerInfo that is tracking the blocks and create the futures which asynchronously
    // remove the blocks from storage endpoints and gives back the number of removed blocks
    val blocks = blockLocations.asScala.keys.flatMap(_.asRDDId).filter(_.rddId == rddId)
    val blocksToDeleteByShuffleService =
      new mutable.HashMap[BlockManagerId, mutable.HashSet[RDDBlockId]]

    blocks.foreach { blockId =>
      val bms: mutable.HashSet[BlockManagerId] = blockLocations.remove(blockId)

      val (bmIdsExtShuffle, bmIdsExecutor) = bms.partition(_.port == externalShuffleServicePort)
      val liveExecutorsForBlock = bmIdsExecutor.map(_.executorId).toSet
      bmIdsExtShuffle.foreach { bmIdForShuffleService =>
        // if the original executor is already released then delete this disk block via
        // the external shuffle service
        if (!liveExecutorsForBlock.contains(bmIdForShuffleService.executorId)) {
          val blockIdsToDel = blocksToDeleteByShuffleService.getOrElseUpdate(bmIdForShuffleService,
            new mutable.HashSet[RDDBlockId]())
          blockIdsToDel += blockId
          blockStatusByShuffleService.get(bmIdForShuffleService).foreach { blockStatusForId =>
            blockStatusForId.remove(blockId)
          }
        }
      }
      bmIdsExecutor.foreach { bmId =>
        aliveBlockManagerInfo(bmId).foreach { bmInfo =>
          bmInfo.removeBlock(blockId)
        }
      }
    }
    val removeRddFromExecutorsFutures = allAliveBlockManagerInfos.map { bmInfo =>
      bmInfo.storageEndpoint.ask[Int](removeMsg).recover {
        // use 0 as default value means no blocks were removed
        handleBlockRemovalFailure("RDD", rddId.toString, bmInfo.blockManagerId, 0)
      }
    }.toSeq

    val removeRddBlockViaExtShuffleServiceFutures = externalBlockStoreClient.map { shuffleClient =>
      blocksToDeleteByShuffleService.map { case (bmId, blockIds) =>
        Future[Int] {
          val numRemovedBlocks = shuffleClient.removeBlocks(
            bmId.host,
            bmId.port,
            bmId.executorId,
            blockIds.map(_.toString).toArray)
          numRemovedBlocks.get(defaultRpcTimeout.duration.toSeconds, TimeUnit.SECONDS)
        }
      }
    }.getOrElse(Seq.empty)

    Future.sequence(removeRddFromExecutorsFutures ++ removeRddBlockViaExtShuffleServiceFutures)
  }

  private def removeShuffle(shuffleId: Int): Future[Seq[Boolean]] = {
    // Nothing to do in the BlockManagerMasterEndpoint data structures
    val removeMsg = RemoveShuffle(shuffleId)
    Future.sequence(
      allAliveBlockManagerInfos.map { bm =>
        bm.storageEndpoint.ask[Boolean](removeMsg).recover {
          // use false as default value means no shuffle data were removed
          handleBlockRemovalFailure("shuffle", shuffleId.toString, bm.blockManagerId, false)
        }
      }.toSeq
    )
  }

  /**
   * Delegate RemoveBroadcast messages to each BlockManager because the master may not notified
   * of all broadcast blocks. If removeFromDriver is false, broadcast blocks are only removed
   * from the executors, but not from the driver.
   */
  private def removeBroadcast(broadcastId: Long, removeFromDriver: Boolean): Future[Seq[Int]] = {
    val removeMsg = RemoveBroadcast(broadcastId, removeFromDriver)
    val requiredBlockManagers = allAliveBlockManagerInfos.filter { info =>
      removeFromDriver || !info.blockManagerId.isDriver
    }
    val futures = requiredBlockManagers.map { bm =>
      bm.storageEndpoint.ask[Int](removeMsg).recover {
        // use 0 as default value means no blocks were removed
        handleBlockRemovalFailure("broadcast", broadcastId.toString, bm.blockManagerId, 0)
      }
    }.toSeq

    Future.sequence(futures)
  }

  private def removeBlockManager(blockManagerId: BlockManagerId): Unit = {
    val info = blockManagerInfo(blockManagerId)

    // SPARK-35011: Not removing info from the blockManagerInfo map, but only setting the removal
    // timestamp of the executor in BlockManagerInfo. This info will be removed from
    // blockManagerInfo map by the blockManagerInfoCleaner once
    // now() - info.executorRemovalTs > executorTimeoutMs.
    //
    // We are delaying the removal of BlockManagerInfo to avoid a BlockManager reregistration
    // while a executor is shutting. This unwanted reregistration causes inconsistent bookkeeping
    // of executors in Spark.
    // Delaying this removal until blockManagerInfoCleaner decides to remove it ensures
    // BlockManagerMasterHeartbeatEndpoint does not ask the BlockManager on a recently removed
    // executor to reregister on BlockManagerHeartbeat message.
    info.setExecutorRemovalTs()

    // Remove the block manager from blockManagerIdByExecutor.
    blockManagerIdByExecutor -= blockManagerId.executorId
    decommissioningBlockManagerSet.remove(blockManagerId)

    // remove all the blocks.
    val iterator = info.blocks.keySet.iterator
    while (iterator.hasNext) {
      val blockId = iterator.next
      val locations = blockLocations.get(blockId)
      locations -= blockManagerId
      // De-register the block if none of the block managers have it. Otherwise, if pro-active
      // replication is enabled, and a block is either an RDD or a test block (the latter is used
      // for unit testing), we send a message to a randomly chosen executor location to replicate
      // the given block. Note that we ignore other block types (such as broadcast/shuffle blocks
      // etc.) as replication doesn't make much sense in that context.
      if (locations.size == 0) {
        blockLocations.remove(blockId)
        logWarning(s"No more replicas available for $blockId !")
      } else if (proactivelyReplicate && (blockId.isRDD || blockId.isInstanceOf[TestBlockId])) {
        // As a heuristic, assume single executor failure to find out the number of replicas that
        // existed before failure
        val maxReplicas = locations.size + 1
        val i = (new Random(blockId.hashCode)).nextInt(locations.size)
        val blockLocations = locations.toSeq
        val candidateBMId = blockLocations(i)
        aliveBlockManagerInfo(candidateBMId).foreach { bm =>
          val remainingLocations = locations.toSeq.filter(bm => bm != candidateBMId)
          val replicateMsg = ReplicateBlock(blockId, remainingLocations, maxReplicas)
          bm.storageEndpoint.ask[Boolean](replicateMsg)
        }
      }
    }

    listenerBus.post(SparkListenerBlockManagerRemoved(System.currentTimeMillis(), blockManagerId))
    logInfo(s"Removing block manager $blockManagerId")

  }

  private def addMergerLocation(blockManagerId: BlockManagerId): Unit = {
    if (!blockManagerId.isDriver && !shuffleMergerLocations.contains(blockManagerId.host)) {
      val shuffleServerId = BlockManagerId(BlockManagerId.SHUFFLE_MERGER_IDENTIFIER,
        blockManagerId.host, externalShuffleServicePort)
      if (shuffleMergerLocations.size >= maxRetainedMergerLocations) {
        shuffleMergerLocations -= shuffleMergerLocations.head._1
      }
      shuffleMergerLocations(shuffleServerId.host) = shuffleServerId
    }
  }

  private def removeExecutor(execId: String): Unit = {
    logInfo("Trying to remove executor " + execId + " from BlockManagerMaster.")
    blockManagerIdByExecutor.get(execId).foreach(removeBlockManager)
  }

  /**
   * Returns a Seq of ReplicateBlock for each RDD block stored by given blockManagerId
   * @param blockManagerId - block manager id for which ReplicateBlock info is needed
   * @return Seq of ReplicateBlock
   */
  private def getReplicateInfoForRDDBlocks(blockManagerId: BlockManagerId): Seq[ReplicateBlock] = {
    try {
      aliveBlockManagerInfo(blockManagerId).map { info =>
        val rddBlocks = info.blocks.keySet().asScala.filter(_.isRDD)
        rddBlocks.map { blockId =>
          val currentBlockLocations = blockLocations.get(blockId)
          val maxReplicas = currentBlockLocations.size + 1
          val remainingLocations = currentBlockLocations.toSeq.filter(bm => bm != blockManagerId)
          val replicateMsg = ReplicateBlock(blockId, remainingLocations, maxReplicas)
          replicateMsg
        }.toSeq
      }.getOrElse(Seq.empty[ReplicateBlock])
    } catch {
      // If the block manager has already exited, nothing to replicate.
      case e: java.util.NoSuchElementException =>
        Seq.empty[ReplicateBlock]
    }
  }

  // Remove a block from the workers that have it. This can only be used to remove
  // blocks that the master knows about.
  private def removeBlockFromWorkers(blockId: BlockId): Unit = {
    val locations = blockLocations.get(blockId)
    if (locations != null) {
      locations.foreach { blockManagerId: BlockManagerId =>
        aliveBlockManagerInfo(blockManagerId).foreach { bm =>
          // Remove the block from the BlockManager.
          // Doesn't actually wait for a confirmation and the message might get lost.
          // If message loss becomes frequent, we should add retry logic here.
          bm.storageEndpoint.ask[Boolean](RemoveBlock(blockId)).recover {
            // use false as default value means no blocks were removed
            handleBlockRemovalFailure("block", blockId.toString, bm.blockManagerId, false)
          }
        }
      }
    }
  }

  // Return a map from the block manager id to max memory and remaining memory.
  private def memoryStatus: Map[BlockManagerId, (Long, Long)] = {
    allAliveBlockManagerInfos.map { info =>
      (info.blockManagerId, (info.maxMem, info.remainingMem))
    }.toMap
  }

  private def storageStatus: Array[StorageStatus] = {
    allAliveBlockManagerInfos.map { info =>
      new StorageStatus(info.blockManagerId, info.maxMem, Some(info.maxOnHeapMem),
        Some(info.maxOffHeapMem), info.blocks.asScala)
    }.toArray
  }

  /**
   * Return the block's status for all block managers, if any. NOTE: This is a
   * potentially expensive operation and should only be used for testing.
   *
   * If askStorageEndpoints is true, the master queries each block manager for the most updated
   * block statuses. This is useful when the master is not informed of the given block by all block
   * managers.
   */
  private def blockStatus(
      blockId: BlockId,
      askStorageEndpoints: Boolean): Map[BlockManagerId, Future[Option[BlockStatus]]] = {
    val getBlockStatus = GetBlockStatus(blockId)
    /*
     * Rather than blocking on the block status query, master endpoint should simply return
     * Futures to avoid potential deadlocks. This can arise if there exists a block manager
     * that is also waiting for this master endpoint's response to a previous message.
     */
    allAliveBlockManagerInfos.map { info =>
      val blockStatusFuture =
        if (askStorageEndpoints) {
          info.storageEndpoint.ask[Option[BlockStatus]](getBlockStatus)
        } else {
          Future { info.getStatus(blockId) }
        }
      (info.blockManagerId, blockStatusFuture)
    }.toMap
  }

  /**
   * Return the ids of blocks present in all the block managers that match the given filter.
   * NOTE: This is a potentially expensive operation and should only be used for testing.
   *
   * If askStorageEndpoints is true, the master queries each block manager for the most updated
   * block statuses. This is useful when the master is not informed of the given block by all block
   * managers.
   */
  private def getMatchingBlockIds(
      filter: BlockId => Boolean,
      askStorageEndpoints: Boolean): Future[Seq[BlockId]] = {
    val getMatchingBlockIds = GetMatchingBlockIds(filter)
    Future.sequence(
      allAliveBlockManagerInfos.map { info =>
        val future =
          if (askStorageEndpoints) {
            info.storageEndpoint.ask[Seq[BlockId]](getMatchingBlockIds)
          } else {
            Future { info.blocks.asScala.keys.filter(filter).toSeq }
          }
        future
      }
    ).map(_.flatten.toSeq)
  }

  private def externalShuffleServiceIdOnHost(blockManagerId: BlockManagerId): BlockManagerId = {
    // we need to keep the executor ID of the original executor to let the shuffle service know
    // which local directories should be used to look for the file
    BlockManagerId(blockManagerId.executorId, blockManagerId.host, externalShuffleServicePort)
  }

  /**
   * Returns the BlockManagerId with topology information populated, if available.
   */
  private def register(
      idWithoutTopologyInfo: BlockManagerId,
      localDirs: Array[String],
      maxOnHeapMemSize: Long,
      maxOffHeapMemSize: Long,
      storageEndpoint: RpcEndpointRef): BlockManagerId = {
    // the dummy id is not expected to contain the topology information.
    // we get that info here and respond back with a more fleshed out block manager id
    val id = BlockManagerId(
      idWithoutTopologyInfo.executorId,
      idWithoutTopologyInfo.host,
      idWithoutTopologyInfo.port,
      topologyMapper.getTopologyForHost(idWithoutTopologyInfo.host))

    val time = System.currentTimeMillis()
    executorIdToLocalDirs.put(id.executorId, localDirs)
    if (!blockManagerInfo.contains(id)) {
      blockManagerIdByExecutor.get(id.executorId) match {
        case Some(oldId) =>
          // A block manager of the same executor already exists, so remove it (assumed dead)
          logError("Got two different block manager registrations on same executor - "
              + s" will replace old one $oldId with new one $id")
          removeExecutor(id.executorId)
        case None =>
      }
      logInfo("Registering block manager %s with %s RAM, %s".format(
        id.hostPort, Utils.bytesToString(maxOnHeapMemSize + maxOffHeapMemSize), id))

      blockManagerIdByExecutor(id.executorId) = id

      val externalShuffleServiceBlockStatus =
        if (externalShuffleServiceRddFetchEnabled) {
          // The blockStatusByShuffleService entries are never removed as they belong to the
          // external shuffle service instances running on the cluster nodes. To decrease its
          // memory footprint when all the disk persisted blocks are removed for a shuffle service
          // BlockStatusPerBlockId releases the backing HashMap.
          val externalShuffleServiceBlocks = blockStatusByShuffleService
            .getOrElseUpdate(externalShuffleServiceIdOnHost(id), new BlockStatusPerBlockId)
          Some(externalShuffleServiceBlocks)
        } else {
          None
        }

      blockManagerInfo(id) = new BlockManagerInfo(id, System.currentTimeMillis(),
        maxOnHeapMemSize, maxOffHeapMemSize, storageEndpoint, externalShuffleServiceBlockStatus)

      if (pushBasedShuffleEnabled) {
        addMergerLocation(id)
      }

      listenerBus.post(SparkListenerBlockManagerAdded(time, id,
        maxOnHeapMemSize + maxOffHeapMemSize, Some(maxOnHeapMemSize), Some(maxOffHeapMemSize)))
    }
    id
  }

  private def updateBlockInfo(
      blockManagerId: BlockManagerId,
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long): Boolean = {
    logDebug(s"Updating block info on master ${blockId} for ${blockManagerId}")

    if (blockId.isShuffle) {
      blockId match {
        case ShuffleIndexBlockId(shuffleId, mapId, _) =>
          // We need to update this at index file because there exists the index-only block
          logDebug(s"Received shuffle index block update for ${shuffleId} ${mapId}, updating.")
          mapOutputTracker.updateMapOutput(shuffleId, mapId, blockManagerId)
          return true
        case ShuffleDataBlockId(shuffleId: Int, mapId: Long, reduceId: Int) =>
          logDebug(s"Received shuffle data block update for ${shuffleId} ${mapId}, ignore.")
          return true
        case _ =>
          logDebug(s"Unexpected shuffle block type ${blockId}" +
            s"as ${blockId.getClass().getSimpleName()}")
          return false
      }
    }

    if (!blockManagerInfo.contains(blockManagerId)) {
      if (blockManagerId.isDriver && !isLocal) {
        // We intentionally do not register the master (except in local mode),
        // so we should not indicate failure.
        return true
      } else {
        return false
      }
    }

    if (blockId == null) {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      return true
    }

    blockManagerInfo(blockManagerId).updateBlockInfo(blockId, storageLevel, memSize, diskSize)

    var locations: mutable.HashSet[BlockManagerId] = null
    if (blockLocations.containsKey(blockId)) {
      locations = blockLocations.get(blockId)
    } else {
      locations = new mutable.HashSet[BlockManagerId]
      blockLocations.put(blockId, locations)
    }

    if (storageLevel.isValid) {
      locations.add(blockManagerId)
    } else {
      locations.remove(blockManagerId)
    }

    if (blockId.isRDD && storageLevel.useDisk && externalShuffleServiceRddFetchEnabled) {
      val externalShuffleServiceId = externalShuffleServiceIdOnHost(blockManagerId)
      if (storageLevel.isValid) {
        locations.add(externalShuffleServiceId)
      } else {
        locations.remove(externalShuffleServiceId)
      }
    }

    // Remove the block from master tracking if it has been removed on all endpoints.
    if (locations.size == 0) {
      blockLocations.remove(blockId)
    }
    true
  }

  private def getLocations(blockId: BlockId): Seq[BlockManagerId] = {
    if (blockLocations.containsKey(blockId)) blockLocations.get(blockId).toSeq else Seq.empty
  }

  private def getLocationsAndStatus(
      blockId: BlockId,
      requesterHost: String): Option[BlockLocationsAndStatus] = {
    val locations = Option(blockLocations.get(blockId)).map(_.toSeq).getOrElse(Seq.empty)
    val status = locations.headOption.flatMap { bmId =>
      if (externalShuffleServiceRddFetchEnabled && bmId.port == externalShuffleServicePort) {
        blockStatusByShuffleService.get(bmId).flatMap(m => m.get(blockId))
      } else {
        aliveBlockManagerInfo(bmId).flatMap(_.getStatus(blockId))
      }
    }

    if (locations.nonEmpty && status.isDefined) {
      val localDirs = locations.find { loc =>
        // When the external shuffle service running on the same host is found among the block
        // locations then the block must be persisted on the disk. In this case the executorId
        // can be used to access this block even when the original executor is already stopped.
        loc.host == requesterHost &&
          (loc.port == externalShuffleServicePort ||
            aliveBlockManagerInfo(loc)
              .flatMap(_.getStatus(blockId).map(_.storageLevel.useDisk))
              .getOrElse(false))
      }.flatMap { bmId => Option(executorIdToLocalDirs.getIfPresent(bmId.executorId)) }
      Some(BlockLocationsAndStatus(locations, status.get, localDirs))
    } else {
      None
    }
  }

  private def getLocationsMultipleBlockIds(
      blockIds: Array[BlockId]): IndexedSeq[Seq[BlockManagerId]] = {
    blockIds.map(blockId => getLocations(blockId))
  }

  /** Get the list of the peers of the given block manager */
  private def getPeers(blockManagerId: BlockManagerId): Seq[BlockManagerId] = {
    val blockManagerIds = allAliveBlockManagerInfos.map(_.blockManagerId).toSet
    if (blockManagerIds.contains(blockManagerId)) {
      blockManagerIds
        .filterNot { _.isDriver }
        .filterNot { _ == blockManagerId }
        .diff(decommissioningBlockManagerSet)
        .toSeq
    } else {
      Seq.empty
    }
  }

  private def getShufflePushMergerLocations(
      numMergersNeeded: Int,
      hostsToFilter: Set[String]): Seq[BlockManagerId] = {
    val blockManagerHosts = blockManagerIdByExecutor
      .filterNot(_._2.isDriver).values.map(_.host).toSet
    val filteredBlockManagerHosts = blockManagerHosts.filterNot(hostsToFilter.contains(_))
    val filteredMergersWithExecutors = filteredBlockManagerHosts.map(
      BlockManagerId(BlockManagerId.SHUFFLE_MERGER_IDENTIFIER, _, externalShuffleServicePort))
    // Enough mergers are available as part of active executors list
    if (filteredMergersWithExecutors.size >= numMergersNeeded) {
      filteredMergersWithExecutors.toSeq
    } else {
      // Delta mergers added from inactive mergers list to the active mergers list
      val filteredMergersWithExecutorsHosts = filteredMergersWithExecutors.map(_.host)
      val filteredMergersWithoutExecutors = shuffleMergerLocations.values
        .filterNot(x => hostsToFilter.contains(x.host))
        .filterNot(x => filteredMergersWithExecutorsHosts.contains(x.host))
      val randomFilteredMergersLocations =
        if (filteredMergersWithoutExecutors.size >
          numMergersNeeded - filteredMergersWithExecutors.size) {
          Utils.randomize(filteredMergersWithoutExecutors)
            .take(numMergersNeeded - filteredMergersWithExecutors.size)
        } else {
          filteredMergersWithoutExecutors
        }
      filteredMergersWithExecutors.toSeq ++ randomFilteredMergersLocations
    }
  }

  private def removeShufflePushMergerLocation(host: String): Unit = {
    if (shuffleMergerLocations.contains(host)) {
      shuffleMergerLocations.remove(host)
    }
  }

  /**
   * Returns an [[RpcEndpointRef]] of the [[BlockManagerReplicaEndpoint]] for sending RPC messages.
   */
  private def getExecutorEndpointRef(executorId: String): Option[RpcEndpointRef] = {
    for (
      blockManagerId <- blockManagerIdByExecutor.get(executorId);
      info <- aliveBlockManagerInfo(blockManagerId)
    ) yield {
      info.storageEndpoint
    }
  }

  override def onStop(): Unit = {
    askThreadPool.shutdownNow()
    blockManagerInfoCleaner.shutdownNow()
  }

  private def cleanBlockManagerInfo(): Unit = {
    logDebug("Cleaning blockManagerInfo")
    val now = System.currentTimeMillis()
    val expiredBmIds = blockManagerInfo.filter { case (_, bmInfo) =>
      // bmInfo.executorRemovalTs.get cannot be None when BM is not alive
      !bmInfo.isAlive && (now - bmInfo.executorRemovalTs.get) > executorTimeoutMs
    }.keys
    expiredBmIds.foreach { bmId =>
      logInfo(s"Cleaning expired $bmId from blockManagerInfo")
      blockManagerInfo.remove(bmId)
    }
  }

  @inline private def aliveBlockManagerInfo(bmId: BlockManagerId): Option[BlockManagerInfo] =
    blockManagerInfo.get(bmId).filter(_.isAlive)

  @inline private def allAliveBlockManagerInfos: Iterable[BlockManagerInfo] =
    blockManagerInfo.values.filter(_.isAlive)
}

@DeveloperApi
case class BlockStatus(storageLevel: StorageLevel, memSize: Long, diskSize: Long) {
  def isCached: Boolean = memSize + diskSize > 0
}

@DeveloperApi
object BlockStatus {
  def empty: BlockStatus = BlockStatus(StorageLevel.NONE, memSize = 0L, diskSize = 0L)
}

/**
 * Stores block statuses for block IDs but removes the reference to the Map which used for storing
 * the data when all the blocks are removed to avoid keeping the memory when not needed.
 */
private[spark] class BlockStatusPerBlockId {

  private var blocks: JHashMap[BlockId, BlockStatus] = _

  def get(blockId: BlockId): Option[BlockStatus] =
    if (blocks == null) None else Option(blocks.get(blockId))

  def put(blockId: BlockId, blockStatus: BlockStatus): Unit = {
    if (blocks == null) {
      blocks = new JHashMap[BlockId, BlockStatus]
    }
    blocks.put(blockId, blockStatus)
  }

  def remove(blockId: BlockId): Unit = {
    blocks.remove(blockId)
    if (blocks.isEmpty) {
      blocks = null
    }
  }

}

private[spark] class BlockManagerInfo(
    val blockManagerId: BlockManagerId,
    timeMs: Long,
    val maxOnHeapMem: Long,
    val maxOffHeapMem: Long,
    val storageEndpoint: RpcEndpointRef,
    val externalShuffleServiceBlockStatus: Option[BlockStatusPerBlockId])
  extends Logging {

  val maxMem = maxOnHeapMem + maxOffHeapMem

  private var _lastSeenMs: Long = timeMs
  private var _remainingMem: Long = maxMem
  private var _executorRemovalTs: Option[Long] = None

  // Mapping from block id to its status.
  private val _blocks = new JHashMap[BlockId, BlockStatus]

  def getStatus(blockId: BlockId): Option[BlockStatus] = Option(_blocks.get(blockId))

  def updateLastSeenMs(): Unit = {
    _lastSeenMs = System.currentTimeMillis()
  }

  def updateBlockInfo(
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long): Unit = {

    updateLastSeenMs()

    val blockExists = _blocks.containsKey(blockId)
    var originalMemSize: Long = 0
    var originalDiskSize: Long = 0
    var originalLevel: StorageLevel = StorageLevel.NONE

    if (blockExists) {
      // The block exists on the storage endpoint already.
      val blockStatus: BlockStatus = _blocks.get(blockId)
      originalLevel = blockStatus.storageLevel
      originalMemSize = blockStatus.memSize
      originalDiskSize = blockStatus.diskSize

      if (originalLevel.useMemory) {
        _remainingMem += originalMemSize
      }
    }

    if (storageLevel.isValid) {
      /* isValid means it is either stored in-memory or on-disk.
       * The memSize here indicates the data size in or dropped from memory,
       * and the diskSize here indicates the data size in or dropped to disk.
       * They can be both larger than 0, when a block is dropped from memory to disk.
       * Therefore, a safe way to set BlockStatus is to set its info in accurate modes. */
      var blockStatus: BlockStatus = null
      if (storageLevel.useMemory) {
        blockStatus = BlockStatus(storageLevel, memSize = memSize, diskSize = 0)
        _blocks.put(blockId, blockStatus)
        _remainingMem -= memSize
        if (blockExists) {
          logInfo(s"Updated $blockId in memory on ${blockManagerId.hostPort}" +
            s" (current size: ${Utils.bytesToString(memSize)}," +
            s" original size: ${Utils.bytesToString(originalMemSize)}," +
            s" free: ${Utils.bytesToString(_remainingMem)})")
        } else {
          logInfo(s"Added $blockId in memory on ${blockManagerId.hostPort}" +
            s" (size: ${Utils.bytesToString(memSize)}," +
            s" free: ${Utils.bytesToString(_remainingMem)})")
        }
      }
      if (storageLevel.useDisk) {
        blockStatus = BlockStatus(storageLevel, memSize = 0, diskSize = diskSize)
        _blocks.put(blockId, blockStatus)
        if (blockExists) {
          logInfo(s"Updated $blockId on disk on ${blockManagerId.hostPort}" +
            s" (current size: ${Utils.bytesToString(diskSize)}," +
            s" original size: ${Utils.bytesToString(originalDiskSize)})")
        } else {
          logInfo(s"Added $blockId on disk on ${blockManagerId.hostPort}" +
            s" (size: ${Utils.bytesToString(diskSize)})")
        }
      }

      externalShuffleServiceBlockStatus.foreach { shuffleServiceBlocks =>
        if (!blockId.isBroadcast && blockStatus.diskSize > 0) {
          shuffleServiceBlocks.put(blockId, blockStatus)
        }
      }
    } else if (blockExists) {
      // If isValid is not true, drop the block.
      _blocks.remove(blockId)
      externalShuffleServiceBlockStatus.foreach { blockStatus =>
        blockStatus.remove(blockId)
      }
      if (originalLevel.useMemory) {
        logInfo(s"Removed $blockId on ${blockManagerId.hostPort} in memory" +
          s" (size: ${Utils.bytesToString(originalMemSize)}," +
          s" free: ${Utils.bytesToString(_remainingMem)})")
      }
      if (originalLevel.useDisk) {
        logInfo(s"Removed $blockId on ${blockManagerId.hostPort} on disk" +
          s" (size: ${Utils.bytesToString(originalDiskSize)})")
      }
    }
  }

  def removeBlock(blockId: BlockId): Unit = {
    if (_blocks.containsKey(blockId)) {
      _remainingMem += _blocks.get(blockId).memSize
      _blocks.remove(blockId)
      externalShuffleServiceBlockStatus.foreach { blockStatus =>
        blockStatus.remove(blockId)
      }
    }
  }

  def remainingMem: Long = _remainingMem

  def lastSeenMs: Long = _lastSeenMs

  def blocks: JHashMap[BlockId, BlockStatus] = _blocks

  override def toString: String = "BlockManagerInfo " + timeMs + " " + _remainingMem

  def clear(): Unit = {
    _blocks.clear()
  }

  def executorRemovalTs: Option[Long] = _executorRemovalTs

  def isAlive: Boolean = _executorRemovalTs.isEmpty

  def setExecutorRemovalTs(): Unit = {
    if (!isAlive) {
      logWarning(s"executorRemovalTs is already set to ${_executorRemovalTs.get}")
    } else {
      _executorRemovalTs = Some(System.currentTimeMillis())
    }
  }
}
