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

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.{config, Logging, MDC}
import org.apache.spark.internal.LogKey.SHUFFLE_BLOCK_INFO
import org.apache.spark.shuffle.ShuffleBlockInfo
import org.apache.spark.storage.BlockManagerMessages.ReplicateBlock
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Class to handle block manager decommissioning retries.
 * It creates a Thread to retry migrating all RDD cache and Shuffle blocks
 */
private[storage] class BlockManagerDecommissioner(
    conf: SparkConf,
    bm: BlockManager) extends Logging {

  private val fallbackStorage = FallbackStorage.getFallbackStorage(conf)
  private val maxReplicationFailuresForDecommission =
    conf.get(config.STORAGE_DECOMMISSION_MAX_REPLICATION_FAILURE_PER_BLOCK)
  private val blockSavedOnDecommissionedBlockManagerException =
    classOf[BlockSavedOnDecommissionedBlockManagerException].getSimpleName

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
    @volatile var keepRunning = true

    private def allowRetry(shuffleBlock: ShuffleBlockInfo, failureNum: Int): Boolean = {
      if (failureNum < maxReplicationFailuresForDecommission) {
        logInfo(s"Add $shuffleBlock back to migration queue for " +
          s"retry ($failureNum / $maxReplicationFailuresForDecommission)")
        // The block needs to retry so we should not mark it as finished
        shufflesToMigrate.add((shuffleBlock, failureNum))
      } else {
        logWarning(s"Give up migrating $shuffleBlock since it's been " +
          s"failed for $maxReplicationFailuresForDecommission times")
        false
      }
    }

    private def nextShuffleBlockToMigrate(): (ShuffleBlockInfo, Int) = {
      while (!Thread.currentThread().isInterrupted) {
        Option(shufflesToMigrate.poll()) match {
          case Some(head) => return head
          // Nothing to do right now, but maybe a transfer will fail or a new block
          // will finish being committed.
          case None => Thread.sleep(1000)
        }
      }
      throw SparkCoreErrors.interruptedError()
    }

    override def run(): Unit = {
      logInfo(s"Starting shuffle block migration thread for $peer")
      // Once a block fails to transfer to an executor stop trying to transfer more blocks
      while (keepRunning) {
        try {
          val (shuffleBlockInfo, retryCount) = nextShuffleBlockToMigrate()
          val blocks = bm.migratableResolver.getMigrationBlocks(shuffleBlockInfo)
          var isTargetDecommissioned = false
          // We only migrate a shuffle block when both index file and data file exist.
          if (blocks.isEmpty) {
            logInfo(s"Ignore deleted shuffle block $shuffleBlockInfo")
          } else {
            logInfo(s"Got migration sub-blocks $blocks. Trying to migrate $shuffleBlockInfo " +
              s"to $peer ($retryCount / $maxReplicationFailuresForDecommission)")
            // Migrate the components of the blocks.
            try {
              val startTime = System.currentTimeMillis()
              if (fallbackStorage.isDefined && peer == FallbackStorage.FALLBACK_BLOCK_MANAGER_ID) {
                fallbackStorage.foreach(_.copy(shuffleBlockInfo, bm))
              } else {
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
                  logDebug(s"Migrated sub-block $blockId")
                }
              }
              logInfo(s"Migrated $shuffleBlockInfo (" +
                s"size: ${Utils.bytesToString(blocks.map(b => b._2.size()).sum)}) to $peer " +
                s"in ${System.currentTimeMillis() - startTime} ms")
            } catch {
              case e @ ( _ : IOException | _ : SparkException) =>
                // If a block got deleted before netty opened the file handle, then trying to
                // load the blocks now will fail. This is most likely to occur if we start
                // migrating blocks and then the shuffle TTL cleaner kicks in. However this
                // could also happen with manually managed shuffles or a GC event on the
                // driver a no longer referenced RDD with shuffle files.
                if (bm.migratableResolver.getMigrationBlocks(shuffleBlockInfo).size < blocks.size) {
                  logWarning(s"Skipping block $shuffleBlockInfo, block deleted.")
                } else if (fallbackStorage.isDefined
                    // Confirm peer is not the fallback BM ID because fallbackStorage would already
                    // have been used in the try-block above so there's no point trying again
                    && peer != FallbackStorage.FALLBACK_BLOCK_MANAGER_ID) {
                  fallbackStorage.foreach(_.copy(shuffleBlockInfo, bm))
                } else if (e.getCause != null && e.getCause.getMessage != null
                  && e.getCause.getMessage
                  .contains(blockSavedOnDecommissionedBlockManagerException)) {
                  isTargetDecommissioned = true
                  keepRunning = false
                } else {
                  logError(log"Error occurred during migrating " +
                    log"${MDC(SHUFFLE_BLOCK_INFO, shuffleBlockInfo)}", e)
                  keepRunning = false
                }
              case e: Exception =>
                logError(log"Error occurred during migrating " +
                  log"${MDC(SHUFFLE_BLOCK_INFO, shuffleBlockInfo)}", e)
                keepRunning = false
            }
          }
          if (keepRunning) {
            numMigratedShuffles.incrementAndGet()
          } else {
            logWarning(s"Stop migrating shuffle blocks to $peer")

            val newRetryCount = if (isTargetDecommissioned) {
              retryCount
            } else {
              retryCount + 1
            }
            // Do not mark the block as migrated if it still needs retry
            if (!allowRetry(shuffleBlockInfo, newRetryCount)) {
              numMigratedShuffles.incrementAndGet()
            }
          }
        } catch {
          case _: InterruptedException =>
            logInfo(s"Stop shuffle block migration${if (keepRunning) " unexpectedly"}.")
            keepRunning = false
          case NonFatal(e) =>
            keepRunning = false
            logError("Error occurred during shuffle blocks migration.", e)
        }
      }
    }
  }

  // Shuffles which are either in queue for migrations or migrated
  private[storage] val migratingShuffles = mutable.HashSet[ShuffleBlockInfo]()

  // Shuffles which have migrated. This used to know when we are "done", being done can change
  // if a new shuffle file is created by a running task.
  private[storage] val numMigratedShuffles = new AtomicInteger(0)

  // Shuffles which are queued for migration & number of retries so far.
  // Visible in storage for testing.
  private[storage] val shufflesToMigrate =
    new java.util.concurrent.ConcurrentLinkedQueue[(ShuffleBlockInfo, Int)]()

  // Set if we encounter an error attempting to migrate and stop.
  @volatile private var stopped = false
  @volatile private[storage] var stoppedRDD =
    !conf.get(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED)
  @volatile private var stoppedShuffle =
    !conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED)

  private val migrationPeers =
    mutable.HashMap[BlockManagerId, ShuffleMigrationRunnable]()

  private val rddBlockMigrationExecutor =
    if (conf.get(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED)) {
      Some(ThreadUtils.newDaemonSingleThreadExecutor("block-manager-decommission-rdd"))
    } else None

  private val rddBlockMigrationRunnable = new Runnable {
    val sleepInterval = conf.get(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL)

    override def run(): Unit = {
      logInfo("Attempting to migrate all RDD blocks")
      while (!stopped && !stoppedRDD) {
        // Validate if we have peers to migrate to. Otherwise, give up migration.
        if (!bm.getPeers(false).exists(_ != FallbackStorage.FALLBACK_BLOCK_MANAGER_ID)) {
          logWarning("No available peers to receive RDD blocks, stop migration.")
          stoppedRDD = true
        } else {
          try {
            val startTime = System.nanoTime()
            logInfo("Attempting to migrate all cached RDD blocks")
            rddBlocksLeft = decommissionRddCacheBlocks()
            lastRDDMigrationTime = startTime
            logInfo(s"Finished current round RDD blocks migration, " +
              s"waiting for ${sleepInterval}ms before the next round migration.")
            Thread.sleep(sleepInterval)
          } catch {
            case _: InterruptedException =>
              logInfo(s"Stop RDD blocks migration${if (!stopped && !stoppedRDD) " unexpectedly"}.")
              stoppedRDD = true
            case NonFatal(e) =>
              logError("Error occurred during RDD blocks migration.", e)
              stoppedRDD = true
          }
        }
      }
    }
  }

  private val shuffleBlockMigrationRefreshExecutor =
    if (conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED)) {
      Some(ThreadUtils.newDaemonSingleThreadExecutor("block-manager-decommission-shuffle"))
    } else None

  private val shuffleBlockMigrationRefreshRunnable = new Runnable {
    val sleepInterval = conf.get(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL)

    override def run(): Unit = {
      logInfo("Attempting to migrate all shuffle blocks")
      while (!stopped && !stoppedShuffle) {
        try {
          val startTime = System.nanoTime()
          shuffleBlocksLeft = refreshMigratableShuffleBlocks()
          lastShuffleMigrationTime = startTime
          logInfo(s"Finished current round refreshing migratable shuffle blocks, " +
            s"waiting for ${sleepInterval}ms before the next round refreshing.")
          Thread.sleep(sleepInterval)
        } catch {
          case _: InterruptedException if stopped =>
            logInfo("Stop refreshing migratable shuffle blocks.")
          case NonFatal(e) =>
            logError("Error occurred during shuffle blocks migration.", e)
            stoppedShuffle = true
        }
      }
    }
  }

  private val shuffleMigrationPool =
    if (conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED)) {
      Some(ThreadUtils.newDaemonCachedThreadPool("migrate-shuffles",
        conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_MAX_THREADS)))
    } else None

  /**
   * Tries to migrate all shuffle blocks that are registered with the shuffle service locally.
   * Note: this does not delete the shuffle files in-case there is an in-progress fetch
   * but rather shadows them.
   * Requires an Indexed based shuffle resolver.
   * Note: if called in testing please call stopMigratingShuffleBlocks to avoid thread leakage.
   * Returns true if we are not done migrating shuffle blocks.
   */
  private[storage] def refreshMigratableShuffleBlocks(): Boolean = {
    // Update the queue of shuffles to be migrated
    logInfo("Start refreshing migratable shuffle blocks")
    val localShuffles = bm.migratableResolver.getStoredShuffles().toSet
    val newShufflesToMigrate = (localShuffles.diff(migratingShuffles)).toSeq
      .sortBy(b => (b.shuffleId, b.mapId))
    shufflesToMigrate.addAll(newShufflesToMigrate.map(x => (x, 0)).asJava)
    migratingShuffles ++= newShufflesToMigrate
    val remainedShuffles = migratingShuffles.size - numMigratedShuffles.get()
    logInfo(s"${newShufflesToMigrate.size} of ${localShuffles.size} local shuffles " +
      s"are added. In total, $remainedShuffles shuffles are remained.")

    // Update the threads doing migrations
    val livePeerSet = bm.getPeers(false).toSet
    val currentPeerSet = migrationPeers.keys.toSet
    val deadPeers = currentPeerSet.diff(livePeerSet)
    // Randomize the orders of the peers to avoid hotspot nodes.
    val newPeers = Utils.randomize(livePeerSet.diff(currentPeerSet))
    migrationPeers ++= newPeers.map { peer =>
      logDebug(s"Starting thread to migrate shuffle blocks to ${peer}")
      val runnable = new ShuffleMigrationRunnable(peer)
      shuffleMigrationPool.foreach(_.submit(runnable))
      (peer, runnable)
    }
    // A peer may have entered a decommissioning state, don't transfer any new blocks
    deadPeers.foreach(migrationPeers.get(_).foreach(_.keepRunning = false))
    // If we don't have anyone to migrate to give up
    if (!migrationPeers.values.exists(_.keepRunning)) {
      logWarning("No available peers to receive Shuffle blocks, stop migration.")
      stoppedShuffle = true
    }
    // If we found any new shuffles to migrate or otherwise have not migrated everything.
    newShufflesToMigrate.nonEmpty || migratingShuffles.size > numMigratedShuffles.get()
  }

  /**
   * Stop migrating shuffle blocks.
   */
  private[storage] def stopMigratingShuffleBlocks(): Unit = {
    shuffleMigrationPool.foreach { threadPool =>
      logInfo("Stopping migrating shuffle blocks.")
      // Stop as gracefully as possible.
      migrationPeers.values.foreach(_.keepRunning = false)
      threadPool.shutdownNow()
    }
  }

  /**
   * Tries to migrate all cached RDD blocks from this BlockManager to peer BlockManagers
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
    false
  }

  private def migrateBlock(blockToReplicate: ReplicateBlock): Boolean = {
    val replicatedSuccessfully = bm.replicateBlock(
      blockToReplicate.blockId,
      blockToReplicate.replicas.toSet,
      blockToReplicate.maxReplicas,
      maxReplicationFailures = Some(maxReplicationFailuresForDecommission))
    if (replicatedSuccessfully) {
      logInfo(s"Block ${blockToReplicate.blockId} migrated successfully, Removing block now")
      bm.removeBlock(blockToReplicate.blockId)
      logInfo(s"Block ${blockToReplicate.blockId} removed")
    } else {
      logWarning(s"Failed to migrate block ${blockToReplicate.blockId}")
    }
    replicatedSuccessfully
  }

  def start(): Unit = {
    logInfo("Starting block migration")
    rddBlockMigrationExecutor.foreach(_.submit(rddBlockMigrationRunnable))
    shuffleBlockMigrationRefreshExecutor.foreach(_.submit(shuffleBlockMigrationRefreshRunnable))
  }

  def stop(): Unit = {
    if (stopped) {
      return
    } else {
      stopped = true
    }
    try {
      rddBlockMigrationExecutor.foreach(_.shutdownNow())
    } catch {
      case NonFatal(e) =>
        logError(s"Error during shutdown RDD block migration thread", e)
    }
    try {
      shuffleBlockMigrationRefreshExecutor.foreach(_.shutdownNow())
    } catch {
      case NonFatal(e) =>
        logError(s"Error during shutdown shuffle block refreshing thread", e)
    }
    try {
      stopMigratingShuffleBlocks()
    } catch {
      case NonFatal(e) =>
        logError(s"Error during shutdown shuffle block migration thread", e)
    }
    logInfo("Stopped block migration")
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
