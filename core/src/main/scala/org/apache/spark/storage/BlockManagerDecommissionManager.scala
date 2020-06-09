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
import org.apache.spark.network.BlockTransferService
import org.apache.spark.shuffle.MigratableResolver
import org.apache.spark.storage.BlockManagerMessages.ReplicateBlock
import org.apache.spark.util.ThreadUtils

/**
 * Class to handle block manager decommissioning retries.
 * It creates a Thread to retry offloading all RDD cache and Shuffle blocks
 */
private[storage] class BlockManagerDecommissionManager(
    conf: SparkConf,
    blockTransferService: BlockTransferService,
    migratableRDDResolver: MigratableRDDResolver,
    migratableShuffleBlockResolver: MigratableResolver,
    peerProvider: BlockManagerPeerProvider) extends Logging {

  // Shuffles which are either in queue for migrations or migrated
  private val migratingShuffles = mutable.HashSet[(Int, Long)]()

  // Shuffles which are queued for migration
  private val shufflesToMigrate = new java.util.concurrent.ConcurrentLinkedQueue[(Int, Long)]()

  private class ShuffleMigrationRunnable(peer: BlockManagerId) extends Runnable {
    @volatile var running = true
    override def run(): Unit = {
      var migrating: Option[(Int, Long)] = None
      logInfo(s"Starting migration thread for ${peer}")
      // Once a block fails to transfer to an executor stop trying to transfer more blocks
      try {
        while (running && !Thread.interrupted()) {
          val migrating = Option(shufflesToMigrate.poll())
          migrating match {
            case None =>
              logInfo("Nothing to migrate")
              // Nothing to do right now, but maybe a transfer will fail or a new block
              // will finish being committed.
              val SLEEP_TIME_SECS = 1
              Thread.sleep(SLEEP_TIME_SECS * 1000L)
            case Some((shuffleId, mapId)) =>
              logInfo(s"Trying to migrate shuffle ${shuffleId},${mapId} to ${peer}")
              val blocks =
                migratableShuffleBlockResolver.getMigrationBlocks(shuffleId, mapId)
              logInfo(s"Got migration sub-blocks ${blocks}")
              blocks.foreach { case (blockId, buffer) =>
                logInfo(s"Migrating sub-block ${blockId}")
                blockTransferService.uploadBlockSync(
                  peer.host,
                  peer.port,
                  peer.executorId,
                  blockId,
                  buffer,
                  StorageLevel.DISK_ONLY,
                  null)// class tag, we don't need for shuffle
                logInfo(s"Migrated sub block ${blockId}")
              }
              logInfo(s"Migrated ${shuffleId},${mapId} to ${peer}")
          }
        }
        // This catch is intentionally outside of the while running block.
        // if we encounter errors migrating to an executor we want to stop.
      } catch {
        case e: Exception =>
          migrating match {
            case Some(shuffleMap) =>
              logError(s"Error ${e} during migration, " +
                s"adding ${shuffleMap} back to migration queue")
              shufflesToMigrate.add(shuffleMap)
            case None =>
              logError(s"Error ${e} while waiting for block to migrate")
          }
      }
    }
  }

  @volatile private var stopped = false

  private val migrationPeers =
    mutable.HashMap[BlockManagerId, (ShuffleMigrationRunnable, ExecutorService)]()

  private lazy val blockMigrationExecutor =
    ThreadUtils.newDaemonSingleThreadExecutor("block-manager-decommission")

  /**
   * Tries to offload all shuffle blocks that are registered with the shuffle service locally.
   * Note: this does not delete the shuffle files in-case there is an in-progress fetch
   * but rather shadows them.
   * Requires an Indexed based shuffle resolver.
   * Note: if called in testing please call stopOffloadingShuffleBlocks to avoid thread leakage.
   */
  def offloadShuffleBlocks(): Unit = {
    // Update the queue of shuffles to be migrated
    logInfo("Offloading shuffle blocks")
    val localShuffles = migratableShuffleBlockResolver.getStoredShuffles()
    val newShufflesToMigrate = localShuffles.&~(migratingShuffles).toSeq
    shufflesToMigrate.addAll(newShufflesToMigrate.asJava)
    migratingShuffles ++= newShufflesToMigrate

    // Update the threads doing migrations
    // TODO: Sort & only start as many threads as min(||blocks||, ||targets||) using location pref
    val livePeerSet = peerProvider.getPeers(false).toSet
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
  def stopOffloadingShuffleBlocks(): Unit = {
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
  def decommissionRddCacheBlocks(): Unit = {
    val replicateBlocksInfo = migratableRDDResolver.getMigratableRDDBlocks()

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
        val replicatedSuccessfully = migratableRDDResolver.migrateBlock(replicateBlock)
        (replicateBlock.blockId, replicatedSuccessfully)
    }.filterNot(_._2).map(_._1)
    if (blocksFailedReplication.nonEmpty) {
      logWarning("Blocks failed replication in cache decommissioning " +
        s"process: ${blocksFailedReplication.mkString(",")}")
    }
  }

  private val blockMigrationRunnable = new Runnable {
    val sleepInterval = conf.get(
      config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL)

    override def run(): Unit = {
      var failures = 0
      while (!stopped
        && !Thread.interrupted()
        && failures < 20) {
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
          if (!conf.get(config.STORAGE_RDD_DECOMMISSION_ENABLED) &&
            !conf.get(config.STORAGE_SHUFFLE_DECOMMISSION_ENABLED)) {
            logWarning("Decommissioning, but no task configured set one or both:\n" +
              "spark.storage.decommission.shuffle_blocks\n" +
              "spark.storage.decommission.rdd_blocks")
          }
          logInfo(s"Waiting for ${sleepInterval} before refreshing migrations.")
          Thread.sleep(sleepInterval)
        } catch {
          case e: InterruptedException =>
            logInfo("Interrupted during migration, will not refresh migrations.")
            stopped = true
          case NonFatal(e) =>
            failures += 1
            logError("Error occurred while trying to replicate cached RDD blocks" +
              s" for block manager decommissioning (failure count: $failures)", e)
        }
      }
    }
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

