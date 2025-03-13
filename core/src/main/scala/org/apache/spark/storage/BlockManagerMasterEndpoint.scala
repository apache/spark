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

import org.apache.spark.{MapOutputTrackerMaster, SparkConf, SparkContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.config.RDD_CACHE_VISIBILITY_TRACKING_ENABLED
import org.apache.spark.network.shuffle.{ExternalBlockStoreClient, RemoteBlockPushResolver}
import org.apache.spark.rpc.{IsolatedThreadSafeRpcEndpoint, RpcCallContext, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.{CoarseGrainedClusterMessages, CoarseGrainedSchedulerBackend}
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.{RpcUtils, ThreadUtils, Utils}

/**
 * BlockManagerMasterEndpoint is an [[IsolatedThreadSafeRpcEndpoint]] on the master node to
 * track statuses of all the storage endpoints' block managers.
 */
private[spark]
class BlockManagerMasterEndpoint(
    override val rpcEnv: RpcEnv,
    val isLocal: Boolean,
    conf: SparkConf,
    listenerBus: LiveListenerBus,
    externalBlockStoreClient: Option[ExternalBlockStoreClient],
    blockManagerInfo: mutable.Map[BlockManagerId, BlockManagerInfo],
    mapOutputTracker: MapOutputTrackerMaster,
    shuffleManager: ShuffleManager,
    isDriver: Boolean)
  extends IsolatedThreadSafeRpcEndpoint with Logging {

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

  // Mapping from task id to the set of rdd blocks which are generated from the task.
  private val tidToRddBlockIds = new mutable.HashMap[Long, mutable.HashSet[RDDBlockId]]
  // Record the RDD blocks which are not visible yet, a block will be removed from this collection
  // after at least one task generating the block finishes successfully.
  private val invisibleRDDBlocks = new mutable.HashSet[RDDBlockId]

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

  val proactivelyReplicate = conf.get(config.STORAGE_REPLICATION_PROACTIVE)

  val defaultRpcTimeout = RpcUtils.askRpcTimeout(conf)

  private val pushBasedShuffleEnabled = Utils.isPushBasedShuffleEnabled(conf, isDriver)

  logInfo("BlockManagerMasterEndpoint up")

  private val externalShuffleServiceRemoveShuffleEnabled: Boolean =
    externalBlockStoreClient.isDefined && conf.get(config.SHUFFLE_SERVICE_REMOVE_SHUFFLE_ENABLED)
  private val externalShuffleServiceRddFetchEnabled: Boolean =
    externalBlockStoreClient.isDefined && conf.get(config.SHUFFLE_SERVICE_FETCH_RDD_ENABLED)
  private val externalShuffleServicePort: Int = StorageUtils.externalShuffleServicePort(conf)

  private lazy val driverEndpoint =
    RpcUtils.makeDriverRef(CoarseGrainedSchedulerBackend.ENDPOINT_NAME, conf, rpcEnv)

  /** Whether rdd cache visibility tracking is enabled. */
  private val trackingCacheVisibility: Boolean = conf.get(RDD_CACHE_VISIBILITY_TRACKING_ENABLED)

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterBlockManager(
      id, localDirs, maxOnHeapMemSize, maxOffHeapMemSize, endpoint, isReRegister) =>
      context.reply(
        register(id, localDirs, maxOnHeapMemSize, maxOffHeapMemSize, endpoint, isReRegister))

    case _updateBlockInfo @
        UpdateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size) =>

      @inline def handleResult(success: Boolean): Unit = {
        // SPARK-30594: we should not post `SparkListenerBlockUpdated` when updateBlockInfo
        // returns false since the block info would be updated again later.
        if (success) {
          listenerBus.post(SparkListenerBlockUpdated(BlockUpdatedInfo(_updateBlockInfo)))
        }
        context.reply(success)
      }

      if (blockId.isShuffle) {
        updateShuffleBlockInfo(blockId, blockManagerId).foreach(handleResult)
      } else {
        handleResult(updateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size))
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

    case UpdateRDDBlockTaskInfo(blockId, taskId) =>
      // This is to report the information that a rdd block(with `blockId`) is computed
      // and cached by task(with `taskId`). And this happens right after the task finished
      // computing/caching the block only when the block is not visible yet. And the rdd
      // block will be marked as visible when the corresponding task finished successfully.
      context.reply(updateRDDBlockTaskInfo(blockId, taskId))

    case GetRDDBlockVisibility(blockId) =>
      // Get the visibility status of a specific rdd block.
      context.reply(isRDDBlockVisible(blockId))

    case UpdateRDDBlockVisibility(taskId, visible) =>
      // This is to report the information that whether rdd blocks computed by task(with `taskId`)
      // can be turned to be visible. This is reported by DAGScheduler right after task completes.
      // If the task finished successfully, rdd blocks can be turned to be visible, otherwise rdd
      // blocks' visibility status won't change.
      context.reply(updateRDDBlockVisibility(taskId, visible))
  }

  private def isRDDBlockVisible(blockId: RDDBlockId): Boolean = {
    if (trackingCacheVisibility) {
      blockLocations.containsKey(blockId) &&
        blockLocations.get(blockId).nonEmpty && !invisibleRDDBlocks.contains(blockId)
    } else {
      // Blocks should always be visible if the feature flag is disabled.
      true
    }
  }

  private def updateRDDBlockVisibility(taskId: Long, visible: Boolean): Unit = {
    if (!trackingCacheVisibility) {
      // Do nothing if the feature flag is disabled.
      return
    }

    // TODO: When visible is false(the task had failed), we should be asking the block managers to
    //  evict the block since the results can be inconsistent if there is any indeterminate
    //  operation computing the rdd. Besides evicting the blocks here, when a rdd block is reported
    //  we may also need to check the data with existing replicas somehow.
    //  This will be tracked with jira: https://issues.apache.org/jira/browse/SPARK-42582
    if (visible) {
      tidToRddBlockIds.get(taskId).foreach { blockIds =>
        blockIds.foreach { blockId =>
          invisibleRDDBlocks.remove(blockId)
          // Ask block managers to update the visibility status.
          val msg = MarkRDDBlockAsVisible(blockId)
          getLocations(blockId).flatMap(blockManagerInfo.get).foreach { managerInfo =>
            managerInfo.storageEndpoint.ask[Unit](msg)
          }
        }
      }
    }

    tidToRddBlockIds.remove(taskId)
  }

  private def updateRDDBlockTaskInfo(blockId: RDDBlockId, taskId: Long): Unit = {
    if (!trackingCacheVisibility) {
      // Do nothing if the feature flag is disabled.
      return
    }
    tidToRddBlockIds.getOrElseUpdate(taskId, new mutable.HashSet[RDDBlockId])
      .add(blockId)
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
      if (!SparkContext.getActive.map(_.isStopped).getOrElse(true)) {
        logWarning(s"Error trying to remove $blockType $blockId" +
          s" from block manager $bmId", e)
      }
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
      if (trackingCacheVisibility) {
        invisibleRDDBlocks.remove(blockId)
      }

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
        blockManagerInfo.get(bmId).foreach { bmInfo =>
          bmInfo.removeBlock(blockId)
        }
      }
    }
    val removeRddFromExecutorsFutures = blockManagerInfo.values.map { bmInfo =>
      bmInfo.storageEndpoint.ask[Int](removeMsg).recover {
        // use 0 as default value means no blocks were removed
        handleBlockRemovalFailure("RDD", rddId.toString, bmInfo.blockManagerId, 0)
      }
    }.toSeq

    val removeRddBlockViaExtShuffleServiceFutures = if (externalShuffleServiceRddFetchEnabled) {
      externalBlockStoreClient.map { shuffleClient =>
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
    } else {
      Seq.empty
    }

    Future.sequence(removeRddFromExecutorsFutures ++ removeRddBlockViaExtShuffleServiceFutures)
  }

  private def removeShuffle(shuffleId: Int): Future[Seq[Boolean]] = {
    // Find all shuffle blocks on executors that are no longer running
    val blocksToDeleteByShuffleService =
      new mutable.HashMap[BlockManagerId, mutable.HashSet[BlockId]]
    if (externalShuffleServiceRemoveShuffleEnabled) {
      mapOutputTracker.shuffleStatuses.get(shuffleId).foreach { shuffleStatus =>
        shuffleStatus.withMapStatuses { mapStatuses =>
          mapStatuses.foreach { mapStatus =>
            // Check if the executor has been deallocated
            if (!blockManagerIdByExecutor.contains(mapStatus.location.executorId)) {
              val blocksToDel =
                shuffleManager.shuffleBlockResolver.getBlocksForShuffle(shuffleId, mapStatus.mapId)
              if (blocksToDel.nonEmpty) {
                val blocks = blocksToDeleteByShuffleService.getOrElseUpdate(mapStatus.location,
                  new mutable.HashSet[BlockId])
                blocks ++= blocksToDel
              }
            }
          }
        }
      }
    }

    val removeShuffleFromShuffleServicesFutures =
      externalBlockStoreClient.map { shuffleClient =>
        blocksToDeleteByShuffleService.map { case (bmId, blockIds) =>
          Future[Boolean] {
            val numRemovedBlocks = shuffleClient.removeBlocks(
              bmId.host,
              bmId.port,
              bmId.executorId,
              blockIds.map(_.toString).toArray)
            numRemovedBlocks.get(defaultRpcTimeout.duration.toSeconds,
              TimeUnit.SECONDS) == blockIds.size
          }
        }
      }.getOrElse(Seq.empty)

    val removeShuffleMergeFromShuffleServicesFutures =
      externalBlockStoreClient.map { shuffleClient =>
        val mergerLocations =
          if (Utils.isPushBasedShuffleEnabled(conf, isDriver)) {
            mapOutputTracker.getShufflePushMergerLocations(shuffleId)
          } else {
            Seq.empty[BlockManagerId]
          }
        mergerLocations.map { bmId =>
          Future[Boolean] {
            shuffleClient.removeShuffleMerge(bmId.host, bmId.port, shuffleId,
              RemoteBlockPushResolver.DELETE_ALL_MERGED_SHUFFLE)
          }
        }
      }.getOrElse(Seq.empty)

    val removeMsg = RemoveShuffle(shuffleId)
    val removeShuffleFromExecutorsFutures = blockManagerInfo.values.map { bm =>
      bm.storageEndpoint.ask[Boolean](removeMsg).recover {
        // use false as default value means no shuffle data were removed
        handleBlockRemovalFailure("shuffle", shuffleId.toString, bm.blockManagerId, false)
      }
    }.toSeq
    Future.sequence(removeShuffleFromExecutorsFutures ++
      removeShuffleFromShuffleServicesFutures ++
      removeShuffleMergeFromShuffleServicesFutures)
  }

  /**
   * Delegate RemoveBroadcast messages to each BlockManager because the master may not notified
   * of all broadcast blocks. If removeFromDriver is false, broadcast blocks are only removed
   * from the executors, but not from the driver.
   */
  private def removeBroadcast(broadcastId: Long, removeFromDriver: Boolean): Future[Seq[Int]] = {
    val removeMsg = RemoveBroadcast(broadcastId, removeFromDriver)
    val requiredBlockManagers = blockManagerInfo.values.filter { info =>
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

    // Remove the block manager from blockManagerIdByExecutor.
    blockManagerIdByExecutor -= blockManagerId.executorId
    decommissioningBlockManagerSet.remove(blockManagerId)

    // Remove it from blockManagerInfo and remove all the blocks.
    blockManagerInfo.remove(blockManagerId)

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
      if (locations.isEmpty) {
        blockLocations.remove(blockId)
        logWarning(s"No more replicas available for $blockId !")
      } else if (proactivelyReplicate && (blockId.isRDD || blockId.isInstanceOf[TestBlockId])) {
        // As a heuristic, assume single executor failure to find out the number of replicas that
        // existed before failure
        val maxReplicas = locations.size + 1
        val i = (new Random(blockId.hashCode)).nextInt(locations.size)
        val blockLocations = locations.toSeq
        val candidateBMId = blockLocations(i)
        blockManagerInfo.get(candidateBMId).foreach { bm =>
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
      val info = blockManagerInfo(blockManagerId)

      val rddBlocks = info.blocks.keySet().asScala.filter(_.isRDD)
      rddBlocks.map { blockId =>
        val currentBlockLocations = blockLocations.get(blockId)
        val maxReplicas = currentBlockLocations.size + 1
        val remainingLocations = currentBlockLocations.toSeq.filter(bm => bm != blockManagerId)
        val replicateMsg = ReplicateBlock(blockId, remainingLocations, maxReplicas)
        replicateMsg
      }.toSeq
    } catch {
      // If the block manager has already exited, nothing to replicate.
      case _: java.util.NoSuchElementException =>
        Seq.empty[ReplicateBlock]
    }
  }

  // Remove a block from the workers that have it. This can only be used to remove
  // blocks that the master knows about.
  private def removeBlockFromWorkers(blockId: BlockId): Unit = {
    val locations = blockLocations.get(blockId)
    if (locations != null) {
      locations.foreach { blockManagerId: BlockManagerId =>
        val blockManager = blockManagerInfo.get(blockManagerId)
        blockManager.foreach { bm =>
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
    blockManagerInfo.map { case(blockManagerId, info) =>
      (blockManagerId, (info.maxMem, info.remainingMem))
    }.toMap
  }

  private def storageStatus: Array[StorageStatus] = {
    blockManagerInfo.map { case (blockManagerId, info) =>
      new StorageStatus(blockManagerId, info.maxMem, Some(info.maxOnHeapMem),
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
    blockManagerInfo.values.map { info =>
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
      blockManagerInfo.values.map { info =>
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
      storageEndpoint: RpcEndpointRef,
      isReRegister: Boolean): BlockManagerId = {
    // the dummy id is not expected to contain the topology information.
    // we get that info here and respond back with a more fleshed out block manager id
    val id = BlockManagerId(
      idWithoutTopologyInfo.executorId,
      idWithoutTopologyInfo.host,
      idWithoutTopologyInfo.port,
      topologyMapper.getTopologyForHost(idWithoutTopologyInfo.host))

    val time = System.currentTimeMillis()
    executorIdToLocalDirs.put(id.executorId, localDirs)
    // SPARK-41360: For the block manager re-registration, we should only allow it when
    // the executor is recognized as active by the scheduler backend. Otherwise, this kind
    // of re-registration from the terminating/stopped executor is meaningless and harmful.
    lazy val isExecutorAlive =
      driverEndpoint.askSync[Boolean](CoarseGrainedClusterMessages.IsExecutorAlive(id.executorId))
    if (!blockManagerInfo.contains(id) && (!isReRegister || isExecutorAlive)) {
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
    val updatedId = if (isReRegister && !isExecutorAlive) {
      assert(!blockManagerInfo.contains(id),
        "BlockManager re-registration shouldn't succeed when the executor is lost")

      logInfo(s"BlockManager ($id) re-registration is rejected since " +
        s"the executor (${id.executorId}) has been lost")

      // Use "invalid" as the return executor id to indicate the block manager that
      // re-registration failed. It's a bit hacky but fine since the returned block
      // manager id won't be accessed in the case of re-registration. And we'll use
      // this "invalid" executor id to print better logs and avoid blocks reporting.
      BlockManagerId(
        BlockManagerId.INVALID_EXECUTOR_ID,
        id.host,
        id.port,
        id.topologyInfo)
    } else {
      id
    }
    updatedId
  }

 private def updateShuffleBlockInfo(blockId: BlockId, blockManagerId: BlockManagerId)
    : Future[Boolean] = {
   blockId match {
     case ShuffleIndexBlockId(shuffleId, mapId, _) =>
       // SPARK-36782: Invoke `MapOutputTracker.updateMapOutput` within the thread
       // `dispatcher-BlockManagerMaster` could lead to the deadlock when
       // `MapOutputTracker.serializeOutputStatuses` broadcasts the serialized mapstatues under
       // the acquired write lock. The broadcast block would report its status to
       // `BlockManagerMasterEndpoint`, while the `BlockManagerMasterEndpoint` is occupied by
       // `updateMapOutput` since it's waiting for the write lock. Thus, we use `Future` to call
       // `updateMapOutput` in a separate thread to avoid the deadlock.
       Future {
         // We need to update this at index file because there exists the index-only block
         logDebug(s"Received shuffle index block update for ${shuffleId} ${mapId}, updating.")
         mapOutputTracker.updateMapOutput(shuffleId, mapId, blockManagerId)
         true
       }
     case ShuffleDataBlockId(shuffleId: Int, mapId: Long, _: Int) =>
       logDebug(s"Received shuffle data block update for ${shuffleId} ${mapId}, ignore.")
       Future.successful(true)
     case _ =>
       logDebug(s"Unexpected shuffle block type ${blockId}" +
         s"as ${blockId.getClass().getSimpleName()}")
       Future.successful(false)
   }
 }

  private def updateBlockInfo(
      blockManagerId: BlockManagerId,
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long): Boolean = {
    logDebug(s"Updating block info on master ${blockId} for ${blockManagerId}")

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
      val firstBlock = locations.isEmpty
      locations.add(blockManagerId)

      blockId.asRDDId.foreach { rddBlockId =>
        (trackingCacheVisibility, firstBlock) match {
          case (true, true) =>
            // Mark as invisible for the first block.
            invisibleRDDBlocks.add(rddBlockId)
          case (true, false) if !invisibleRDDBlocks.contains(rddBlockId) =>
            // If the rdd block is already visible, ask storage manager to update the visibility
            // status.
            blockManagerInfo(blockManagerId).storageEndpoint
              .ask[Unit](MarkRDDBlockAsVisible(rddBlockId))
          case _ =>
        }
      }
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
    if (locations.isEmpty) {
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
    val allLocations = Option(blockLocations.get(blockId)).map(_.toSeq).getOrElse(Seq.empty)
    val hostLocalLocations = allLocations.filter(bmId => bmId.host == requesterHost)

    val blockStatusWithBlockManagerId: Option[(BlockStatus, BlockManagerId)] =
      (if (externalShuffleServiceRddFetchEnabled) {
         // if fetching RDD is enabled from the external shuffle service then first try to find
         // the block in the external shuffle service of the same host
         val location = hostLocalLocations.find(_.port == externalShuffleServicePort)
         location
           .flatMap(blockStatusByShuffleService.get(_).flatMap(_.get(blockId)))
           .zip(location)
           .headOption
       } else {
         None
       })
        .orElse {
          // if the block is not found via the external shuffle service trying to find it in the
          // executors running on the same host and persisted on the disk
          // using flatMap on iterators makes the transformation lazy
          hostLocalLocations.iterator
            .flatMap { bmId =>
              blockManagerInfo.get(bmId).flatMap { blockInfo =>
                blockInfo.getStatus(blockId).map((_, bmId))
              }
            }
            .find(_._1.storageLevel.useDisk)
        }
        .orElse {
          // if the block cannot be found in the same host search it in all the executors
          val location = allLocations.headOption
          location
            .flatMap(blockManagerInfo.get(_))
            .flatMap(_.getStatus(blockId))
            .zip(location)
            .headOption
        }
    logDebug(s"Identified block: $blockStatusWithBlockManagerId")
    blockStatusWithBlockManagerId
      .map { case (blockStatus: BlockStatus, bmId: BlockManagerId) =>
        if (bmId.host == requesterHost && blockStatus.storageLevel.useDisk) {
          BlockLocationsAndStatus(
            allLocations,
            blockStatus,
            Option(executorIdToLocalDirs.getIfPresent(bmId.executorId)))
        } else {
          BlockLocationsAndStatus(allLocations, blockStatus, None)
        }
      }
      .orElse(None)
  }

  private def getLocationsMultipleBlockIds(
      blockIds: Array[BlockId]): IndexedSeq[Seq[BlockManagerId]] = {
    blockIds.map(blockId => getLocations(blockId))
  }

  /** Get the list of the peers of the given block manager */
  private def getPeers(blockManagerId: BlockManagerId): Seq[BlockManagerId] = {
    val blockManagerIds = blockManagerInfo.keySet
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
    val filteredBlockManagerHosts = blockManagerHosts.diff(hostsToFilter)
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
      info <- blockManagerInfo.get(blockManagerId)
    ) yield {
      info.storageEndpoint
    }
  }

  override def onStop(): Unit = {
    askThreadPool.shutdownNow()
  }
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
    if (blocks != null) {
      blocks.remove(blockId)
      if (blocks.isEmpty) {
        blocks = null
      }
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
}
