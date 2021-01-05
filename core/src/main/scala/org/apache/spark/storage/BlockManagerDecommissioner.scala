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
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.shuffle.ShuffleBlockInfo
import org.apache.spark.storage.BlockManagerMessages.ReplicateBlock
import org.apache.spark.util.ThreadUtils

/**
 * Class to handle block manager decommissioning retries.
 * It creates a Thread to retry offloading all RDD cache and Shuffle blocks
 */
private[storage] class BlockManagerDecommissioner(
    conf: SparkConf,
    bm: BlockManager) extends Logging {

  private val fallbackStorage = FallbackStorage.getFallbackStorage(conf)
  private val maxReplicationFailuresForDecommission =
    conf.get(config.STORAGE_DECOMMISSION_MAX_REPLICATION_FAILURE_PER_BLOCK)

  // Used for tracking if our migrations are complete. Readable for testing
  @volatile private[storage] var lastRDDMigrationTime: Long = 0
  @volatile private[storage] var lastShuffleMigrationTime: Long = 0
  @volatile private[storage] var rddBlocksLeft: Boolean = true
  @volatile private[storage] var shuffleBlocksLeft: Boolean = true

  /**
   * This runnable consumes any shuffle blocks in the queue for migration. This part of a
   * producer/consumer where the main migration loop updates the queue of blocks to be migrated
   * periodically. On migration failure, the current thread will reinsert the block for another
   * thread to consume. Each thread migrates blocks to a different particular executor to avoid
   * distribute the blocks as quickly as possible without overwhelming any particular executor.
   *
   * There is no preference for which peer a given block is migrated to.
   * This is notable different than the RDD cache block migration (further down in this file)
   * which uses the existing priority mechanism for determining where to replicate blocks to.
   * Generally speaking cache blocks are less impactful as they normally represent narrow
   * transformations and we normally have less cache present than shuffle data.
   *
   * The producer/consumer model is chosen for shuffle block migration to maximize
   * the chance of migrating all shuffle blocks before the executor is forced to exit.
   */
  private class ShuffleMigrationRunnable(peer: BlockManagerId) extends Runnable {
    @volatile var running = true
    override def run(): Unit = {
      var migrating: Option[(ShuffleBlockInfo, Int)] = None
      logInfo(s"Starting migration thread for ${peer}")
      // Once a block fails to transfer to an executor stop trying to transfer more blocks
      try {
        while (running && !Thread.interrupted()) {
          migrating = Option(shufflesToMigrate.poll())
          migrating match {
            case None =>
              logDebug("Nothing to migrate")
              // Nothing to do right now, but maybe a transfer will fail or a new block
              // will finish being committed.
              val SLEEP_TIME_SECS = 1
              Thread.sleep(SLEEP_TIME_SECS * 1000L)
            case Some((shuffleBlockInfo, retryCount)) =>
              if (retryCount < maxReplicationFailuresForDecommission) {
                val blocks = bm.migratableResolver.getMigrationBlocks(shuffleBlockInfo)
                if (blocks.isEmpty) {
                  logInfo(s"Ignore empty shuffle block $shuffleBlockInfo")
                } else {
                  logInfo(s"Got migration sub-blocks ${blocks}")
                  logInfo(s"Trying to migrate shuffle ${shuffleBlockInfo} to ${peer} " +
                    s"($retryCount / $maxReplicationFailuresForDecommission)")

                  // Migrate the components of the blocks.
                  try {
                    blocks.foreach { case (blockId, buffer) =>
                      logDebug(s"Migrating sub-block ${blockId}")
                      bm.blockTransferService.uploadBlockSync(
                        peer.host,
                        peer.port,
                        peer.executorId,
                        blockId,
                        buffer,
                        StorageLevel.DISK_ONLY,
                        null) // class tag, we don't need for shuffle
                      logDebug(s"Migrated sub block ${blockId}")
                    }
                    logInfo(s"Migrated ${shuffleBlockInfo} to ${peer}")
                  } catch {
                    case e: IOException =>
                      // If a block got deleted before netty opened the file handle, then trying to
                      // load the blocks now will fail. This is most likely to occur if we start
                      // migrating blocks and then the shuffle TTL cleaner kicks in. However this
                      // could also happen with manually managed shuffles or a GC event on the
                      // driver a no longer referenced RDD with shuffle files.
                      if (bm.migratableResolver.getMigrationBlocks(shuffleBlockInfo).isEmpty) {
                        logWarning(s"Skipping block ${shuffleBlockInfo}, block deleted.")
                      } else if (fallbackStorage.isDefined) {
                        fallbackStorage.foreach(_.copy(shuffleBlockInfo, bm))
                      } else {
                        throw e
                      }
                  }
                }
              } else {
                logError(s"Skipping block ${shuffleBlockInfo} because it has failed ${retryCount}")
              }
              numMigratedShuffles.incrementAndGet()
          }
        }
        // This catch is intentionally outside of the while running block.
        // if we encounter errors migrating to an executor we want to stop.
      } catch {
        case e: Exception =>
          migrating match {
            case Some((shuffleMap, retryCount)) =>
              logError(s"Error during migration, adding ${shuffleMap} back to migration queue", e)
              shufflesToMigrate.add((shuffleMap, retryCount + 1))
              running = false
            case None =>
              logError(s"Error while waiting for block to migrate", e)
          }
      }
    }
  }

  // Shuffles which are either in queue for migrations or migrated
  protected[storage] val migratingShuffles = mutable.HashSet[ShuffleBlockInfo]()

  // Shuffles which have migrated. This used to know when we are "done", being done can change
  // if a new shuffle file is created by a running task.
  private[storage] val numMigratedShuffles = new AtomicInteger(0)

  // Shuffles which are queued for migration & number of retries so far.
  // Visible in storage for testing.
  private[storage] val shufflesToMigrate =
    new java.util.concurrent.ConcurrentLinkedQueue[(ShuffleBlockInfo, Int)]()

  // Set if we encounter an error attempting to migrate and stop.
  @volatile private var stopped = false
  @volatile private var stoppedRDD =
    !conf.get(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED)
  @volatile private var stoppedShuffle =
    !conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED)

  private val migrationPeers =
    mutable.HashMap[BlockManagerId, ShuffleMigrationRunnable]()

  private lazy val rddBlockMigrationExecutor =
    ThreadUtils.newDaemonSingleThreadExecutor("block-manager-decommission-rdd")

  private val rddBlockMigrationRunnable = new Runnable {
    val sleepInterval = conf.get(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL)

    override def run(): Unit = {
      assert(conf.get(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED))
      while (!stopped && !stoppedRDD && !Thread.interrupted()) {
        logInfo("Iterating on migrating from the block manager.")
        // Validate we have peers to migrate to.
        val peers = bm.getPeers(false)
        // If we have no peers give up.
        if (peers.isEmpty) {
          stopped = true
          stoppedRDD = true
        }
        try {
          val startTime = System.nanoTime()
          logDebug("Attempting to replicate all cached RDD blocks")
          rddBlocksLeft = decommissionRddCacheBlocks()
          lastRDDMigrationTime = startTime
          logInfo("Attempt to replicate all cached blocks done")
          logInfo(s"Waiting for ${sleepInterval} before refreshing migrations.")
          Thread.sleep(sleepInterval)
        } catch {
          case e: InterruptedException =>
            logInfo("Interrupted during RDD migration, stopping")
            stoppedRDD = true
          case NonFatal(e) =>
            logError("Error occurred replicating RDD for block manager decommissioning.",
              e)
            stoppedRDD = true
        }
      }
    }
  }

  private lazy val shuffleBlockMigrationRefreshExecutor =
    ThreadUtils.newDaemonSingleThreadExecutor("block-manager-decommission-shuffle")

  private val shuffleBlockMigrationRefreshRunnable = new Runnable {
    val sleepInterval = conf.get(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL)

    override def run(): Unit = {
      assert(conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED))
      while (!stopped && !stoppedShuffle && !Thread.interrupted()) {
        try {
          logDebug("Attempting to replicate all shuffle blocks")
          val startTime = System.nanoTime()
          shuffleBlocksLeft = refreshOffloadingShuffleBlocks()
          lastShuffleMigrationTime = startTime
          logInfo("Done starting workers to migrate shuffle blocks")
          Thread.sleep(sleepInterval)
        } catch {
          case e: InterruptedException =>
            logInfo("Interrupted during migration, will not refresh migrations.")
            stoppedShuffle = true
          case NonFatal(e) =>
            logError("Error occurred while trying to replicate for block manager decommissioning.",
              e)
            stoppedShuffle = true
        }
      }
    }
  }

  lazy val shuffleMigrationPool = ThreadUtils.newDaemonCachedThreadPool(
    "migrate-shuffles",
    conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_MAX_THREADS))

  /**
   * Tries to offload all shuffle blocks that are registered with the shuffle service locally.
   * Note: this does not delete the shuffle files in-case there is an in-progress fetch
   * but rather shadows them.
   * Requires an Indexed based shuffle resolver.
   * Note: if called in testing please call stopOffloadingShuffleBlocks to avoid thread leakage.
   * Returns true if we are not done migrating shuffle blocks.
   */
  private[storage] def refreshOffloadingShuffleBlocks(): Boolean = {
    // Update the queue of shuffles to be migrated
    logInfo("Offloading shuffle blocks")
    val localShuffles = bm.migratableResolver.getStoredShuffles().toSet
    val newShufflesToMigrate = (localShuffles.diff(migratingShuffles)).toSeq
      .sortBy(b => (b.shuffleId, b.mapId))
    shufflesToMigrate.addAll(newShufflesToMigrate.map(x => (x, 0)).asJava)
    migratingShuffles ++= newShufflesToMigrate
    logInfo(s"${newShufflesToMigrate.size} of ${localShuffles.size} local shuffles " +
      s"are added. In total, ${migratingShuffles.size} shuffles are remained.")

    // Update the threads doing migrations
    val livePeerSet = bm.getPeers(false).toSet
    val currentPeerSet = migrationPeers.keys.toSet
    val deadPeers = currentPeerSet.diff(livePeerSet)
    val newPeers = livePeerSet.diff(currentPeerSet)
    migrationPeers ++= newPeers.map { peer =>
      logDebug(s"Starting thread to migrate shuffle blocks to ${peer}")
      val runnable = new ShuffleMigrationRunnable(peer)
      shuffleMigrationPool.submit(runnable)
      (peer, runnable)
    }
    // A peer may have entered a decommissioning state, don't transfer any new blocks
    deadPeers.foreach { peer =>
        migrationPeers.get(peer).foreach(_.running = false)
    }
    // If we don't have anyone to migrate to give up
    if (migrationPeers.values.find(_.running == true).isEmpty) {
      stoppedShuffle = true
    }
    // If we found any new shuffles to migrate or otherwise have not migrated everything.
    newShufflesToMigrate.nonEmpty || migratingShuffles.size > numMigratedShuffles.get()
  }

  /**
   * Stop migrating shuffle blocks.
   */
  private[storage] def stopOffloadingShuffleBlocks(): Unit = {
    logInfo("Stopping offloading shuffle blocks.")
    // Stop as gracefully as possible.
    migrationPeers.values.foreach{ _.running = false }
    shuffleMigrationPool.shutdown()
    shuffleMigrationPool.shutdownNow()
  }

  /**
   * Tries to offload all cached RDD blocks from this BlockManager to peer BlockManagers
   * Visible for testing
   * Returns true if we have not migrated all of our RDD blocks.
   */
  private[storage] def decommissionRddCacheBlocks(): Boolean = {
    val replicateBlocksInfo = bm.getMigratableRDDBlocks()
    // Refresh peers and validate we have somewhere to move blocks.

    if (replicateBlocksInfo.nonEmpty) {
      logInfo(s"Need to replicate ${replicateBlocksInfo.size} RDD blocks " +
        "for block manager decommissioning")
    } else {
      logWarning(s"Asked to decommission RDD cache blocks, but no blocks to migrate")
      return false
    }

    // TODO: We can sort these blocks based on some policy (LRU/blockSize etc)
    //   so that we end up prioritize them over each other
    val blocksFailedReplication = replicateBlocksInfo.map { replicateBlock =>
        val replicatedSuccessfully = migrateBlock(replicateBlock)
        (replicateBlock.blockId, replicatedSuccessfully)
    }.filterNot(_._2).map(_._1)
    if (blocksFailedReplication.nonEmpty) {
      logWarning("Blocks failed replication in cache decommissioning " +
        s"process: ${blocksFailedReplication.mkString(",")}")
      return true
    }
    return false
  }

  private def migrateBlock(blockToReplicate: ReplicateBlock): Boolean = {
    val replicatedSuccessfully = bm.replicateBlock(
      blockToReplicate.blockId,
      blockToReplicate.replicas.toSet,
      blockToReplicate.maxReplicas,
      maxReplicationFailures = Some(maxReplicationFailuresForDecommission))
    if (replicatedSuccessfully) {
      logInfo(s"Block ${blockToReplicate.blockId} offloaded successfully, Removing block now")
      bm.removeBlock(blockToReplicate.blockId)
      logInfo(s"Block ${blockToReplicate.blockId} removed")
    } else {
      logWarning(s"Failed to offload block ${blockToReplicate.blockId}")
    }
    replicatedSuccessfully
  }

  def start(): Unit = {
    logInfo("Starting block migration thread")
    if (conf.get(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED)) {
      rddBlockMigrationExecutor.submit(rddBlockMigrationRunnable)
    }
    if (conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED)) {
      shuffleBlockMigrationRefreshExecutor.submit(shuffleBlockMigrationRefreshRunnable)
    }
    if (!conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED) &&
      !conf.get(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED)) {
      logError(s"Storage decommissioning attempted but neither " +
        s"${config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED.key} or " +
        s"${config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED.key} is enabled ")
      stopped = true
    }
  }

  def stop(): Unit = {
    if (stopped) {
      return
    } else {
      stopped = true
    }
    try {
      rddBlockMigrationExecutor.shutdown()
    } catch {
      case e: Exception =>
        logError(s"Error during shutdown", e)
    }
    try {
      shuffleBlockMigrationRefreshExecutor.shutdown()
    } catch {
      case e: Exception =>
        logError(s"Error during shutdown", e)
    }
    try {
      stopOffloadingShuffleBlocks()
    } catch {
      case e: Exception =>
        logError(s"Error during shutdown", e)
    }
    logInfo("Forcing block migrations threads to stop")
    try {
      rddBlockMigrationExecutor.shutdownNow()
    } catch {
      case e: Exception =>
        logError(s"Error during shutdown", e)
    }
    try {
      shuffleBlockMigrationRefreshExecutor.shutdownNow()
    } catch {
      case e: Exception =>
        logError(s"Error during shutdown", e)
    }
    logInfo("Stopped storage decommissioner")
  }

  /*
   *  Returns the last migration time and a boolean for if all blocks have been migrated.
   *  The last migration time is calculated to be the minimum of the last migration of any
   *  running migration (and if there are now current running migrations it is set to current).
   *  This provides a timeStamp which, if there have been no tasks running since that time
   *  we can know that all potential blocks that can be have been migrated off.
   */
  private[storage] def lastMigrationInfo(): (Long, Boolean) = {
    if (stopped || (stoppedRDD && stoppedShuffle)) {
      // Since we don't have anything left to migrate ever (since we don't restart once
      // stopped), return that we're done with a validity timestamp that doesn't expire.
      (Long.MaxValue, true)
    } else {
      // Chose the min of the active times. See the function description for more information.
      val lastMigrationTime = if (!stoppedRDD && !stoppedShuffle) {
        Math.min(lastRDDMigrationTime, lastShuffleMigrationTime)
      } else if (!stoppedShuffle) {
        lastShuffleMigrationTime
      } else {
        lastRDDMigrationTime
      }

      // Technically we could have blocks left if we encountered an error, but those blocks will
      // never be migrated, so we don't care about them.
      val blocksMigrated = (!shuffleBlocksLeft || stoppedShuffle) && (!rddBlocksLeft || stoppedRDD)
      (lastMigrationTime, blocksMigrated)
    }
  }
}
