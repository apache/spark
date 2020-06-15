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

import java.util.concurrent.ExecutorService

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.shuffle.{MigratableResolver, ShuffleBlockInfo}
import org.apache.spark.storage.BlockManagerMessages.ReplicateBlock
import org.apache.spark.util.ThreadUtils

/**
 * Class to handle block manager decommissioning retries.
 * It creates a Thread to retry offloading all RDD cache and Shuffle blocks
 */
private[storage] class BlockManagerDecommissioner(
  conf: SparkConf,
  bm: BlockManager) extends Logging {

  private val maxReplicationFailuresForDecommission =
    conf.get(config.STORAGE_DECOMMISSION_MAX_REPLICATION_FAILURE_PER_BLOCK)

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
      var migrating: Option[ShuffleBlockInfo] = None
      logInfo(s"Starting migration thread for ${peer}")
      // Once a block fails to transfer to an executor stop trying to transfer more blocks
      try {
        while (running && !Thread.interrupted()) {
          val migrating = Option(shufflesToMigrate.poll())
          migrating match {
            case None =>
              logDebug("Nothing to migrate")
              // Nothing to do right now, but maybe a transfer will fail or a new block
              // will finish being committed.
              val SLEEP_TIME_SECS = 1
              Thread.sleep(SLEEP_TIME_SECS * 1000L)
            case Some(shuffleBlockInfo) =>
              logInfo(s"Trying to migrate shuffle ${shuffleBlockInfo} to ${peer}")
              val blocks =
                bm.migratableResolver.getMigrationBlocks(shuffleBlockInfo)
              logInfo(s"Got migration sub-blocks ${blocks}")
              blocks.foreach { case (blockId, buffer) =>
                logInfo(s"Migrating sub-block ${blockId}")
                bm.blockTransferService.uploadBlockSync(
                  peer.host,
                  peer.port,
                  peer.executorId,
                  blockId,
                  buffer,
                  StorageLevel.DISK_ONLY,
                  null)// class tag, we don't need for shuffle
                logDebug(s"Migrated sub block ${blockId}")
              }
              logInfo(s"Migrated ${shuffleBlockInfo} to ${peer}")
          }
        }
        // This catch is intentionally outside of the while running block.
        // if we encounter errors migrating to an executor we want to stop.
      } catch {
        case e: Exception =>
          migrating match {
            case Some(shuffleMap) =>
              logError(s"Error ${e} during migration, adding ${shuffleMap} back to migration queue")
              shufflesToMigrate.add(shuffleMap)
            case None =>
              logError(s"Error ${e} while waiting for block to migrate")
          }
      }
    }
  }

  // Shuffles which are either in queue for migrations or migrated
  private val migratingShuffles = mutable.HashSet[ShuffleBlockInfo]()

  // Shuffles which are queued for migration
  private[storage] val shufflesToMigrate =
    new java.util.concurrent.ConcurrentLinkedQueue[ShuffleBlockInfo]()

  @volatile private var stopped = false

  private val migrationPeers =
    mutable.HashMap[BlockManagerId, (ShuffleMigrationRunnable, ExecutorService)]()

  private lazy val blockMigrationExecutor =
    ThreadUtils.newDaemonSingleThreadExecutor("block-manager-decommission")

  private val blockMigrationRunnable = new Runnable {
    val sleepInterval = conf.get(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL)

    override def run(): Unit = {
      if (!conf.get(config.STORAGE_RDD_DECOMMISSION_ENABLED) &&
        !conf.get(config.STORAGE_SHUFFLE_DECOMMISSION_ENABLED)) {
        logWarning("Decommissioning, but no task configured set one or both:\n" +
          s"${config.STORAGE_RDD_DECOMMISSION_ENABLED.key}\n" +
          s"${config.STORAGE_SHUFFLE_DECOMMISSION_ENABLED.key}")
        stopped = true
      }
      while (!stopped && !Thread.interrupted()) {
        logInfo("Iterating on migrating from the block manager.")
        try {
          // If enabled we migrate shuffle blocks first as they are more expensive.
          if (conf.get(config.STORAGE_SHUFFLE_DECOMMISSION_ENABLED)) {
            logDebug("Attempting to replicate all shuffle blocks")
            offloadShuffleBlocks()
            logInfo("Done starting workers to migrate shuffle blocks")
          }
          if (conf.get(config.STORAGE_RDD_DECOMMISSION_ENABLED)) {
            logDebug("Attempting to replicate all cached RDD blocks")
            decommissionRddCacheBlocks()
            logInfo("Attempt to replicate all cached blocks done")
          }
          logInfo(s"Waiting for ${sleepInterval} before refreshing migrations.")
          Thread.sleep(sleepInterval)
        } catch {
          case e: InterruptedException =>
            logInfo("Interrupted during migration, will not refresh migrations.")
            stopped = true
          case NonFatal(e) =>
            logError("Error occurred while trying to replicate for block manager decommissioning.",
              e)
            stopped = true
        }
      }
    }
  }

  /**
   * Tries to offload all shuffle blocks that are registered with the shuffle service locally.
   * Note: this does not delete the shuffle files in-case there is an in-progress fetch
   * but rather shadows them.
   * Requires an Indexed based shuffle resolver.
   * Note: if called in testing please call stopOffloadingShuffleBlocks to avoid thread leakage.
   */
  private[storage] def offloadShuffleBlocks(): Unit = {
    // Update the queue of shuffles to be migrated
    logInfo("Offloading shuffle blocks")
    val localShuffles = bm.migratableResolver.getStoredShuffles()
    val newShufflesToMigrate = localShuffles.&~(migratingShuffles).toSeq
    shufflesToMigrate.addAll(newShufflesToMigrate.asJava)
    migratingShuffles ++= newShufflesToMigrate

    // Update the threads doing migrations
    // TODO: Sort & only start as many threads as min(||blocks||, ||targets||) using location pref
    val livePeerSet = bm.getPeers(false).toSet
    val currentPeerSet = migrationPeers.keys.toSet
    val deadPeers = currentPeerSet.&~(livePeerSet)
    val newPeers = livePeerSet.&~(currentPeerSet)
    migrationPeers ++= newPeers.map { peer =>
      logDebug(s"Starting thread to migrate shuffle blocks to ${peer}")
      val executor = ThreadUtils.newDaemonSingleThreadExecutor(s"migrate-shuffle-to-${peer}")
      val runnable = new ShuffleMigrationRunnable(peer)
      executor.submit(runnable)
      (peer, (runnable, executor))
    }
    // A peer may have entered a decommissioning state, don't transfer any new blocks
    deadPeers.foreach { peer =>
        migrationPeers.get(peer).foreach(_._1.running = false)
    }
  }

  /**
   * Stop migrating shuffle blocks.
   */
  private[storage] def stopOffloadingShuffleBlocks(): Unit = {
    logInfo("Stopping offloading shuffle blocks.")
    // Stop as gracefully as possible.
    migrationPeers.values.foreach{case (runnable, service) =>
      runnable.running = false}
    migrationPeers.values.foreach{case (runnable, service) =>
      service.shutdown()}
    migrationPeers.values.foreach{case (runnable, service) =>
      service.shutdownNow()}
  }

  /**
   * Tries to offload all cached RDD blocks from this BlockManager to peer BlockManagers
   * Visible for testing
   */
  private[storage] def decommissionRddCacheBlocks(): Unit = {
    val replicateBlocksInfo = bm.getMigratableRDDBlocks()

    if (replicateBlocksInfo.nonEmpty) {
      logInfo(s"Need to replicate ${replicateBlocksInfo.size} RDD blocks " +
        "for block manager decommissioning")
    } else {
      logWarning(s"Asked to decommission RDD cache blocks, but no blocks to migrate")
      return
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
    }
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
    blockMigrationExecutor.submit(blockMigrationRunnable)
  }

  def stop(): Unit = {
    if (!stopped) {
      stopped = true
    }
    try {
      blockMigrationExecutor.shutdown()
    } catch {
      case e: Exception =>
        logInfo(s"Error during shutdown ${e}")
    }
    try {
      stopOffloadingShuffleBlocks()
    } catch {
      case e: Exception =>
        logInfo(s"Error during shuffle shutdown ${e}")
    }
    logInfo("Stopping block migration thread")
    blockMigrationExecutor.shutdownNow()
  }
}
