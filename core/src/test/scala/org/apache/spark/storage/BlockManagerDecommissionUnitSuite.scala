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

import java.io.FileNotFoundException

import scala.concurrent.Future
import scala.concurrent.duration._

import org.mockito.{ArgumentMatchers => mc}
import org.mockito.Mockito.{atLeast => least, mock, times, verify, when}
import org.scalatest.concurrent.Eventually._
import org.scalatest.matchers.must.Matchers

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.shuffle.{MigratableResolver, ShuffleBlockInfo}
import org.apache.spark.storage.BlockManagerMessages.ReplicateBlock

class BlockManagerDecommissionUnitSuite extends SparkFunSuite with Matchers {

  private val bmPort = 12345

  private val sparkConf = new SparkConf(false)
    .set(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED, true)
    .set(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED, true)
    // Just replicate blocks quickly during testing, as there isn't another
    // workload we need to worry about.
    .set(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL, 10L)

  private def registerShuffleBlocks(
      mockMigratableShuffleResolver: MigratableResolver,
      ids: Set[(Int, Long, Int)]): Unit = {

    when(mockMigratableShuffleResolver.getStoredShuffles())
      .thenReturn(ids.map(triple => ShuffleBlockInfo(triple._1, triple._2)).toSeq)

    ids.foreach { case (shuffleId: Int, mapId: Long, reduceId: Int) =>
      when(mockMigratableShuffleResolver.getMigrationBlocks(mc.any()))
        .thenReturn(List(
          (ShuffleIndexBlockId(shuffleId, mapId, reduceId), mock(classOf[ManagedBuffer])),
          (ShuffleDataBlockId(shuffleId, mapId, reduceId), mock(classOf[ManagedBuffer]))))
    }
  }

  /**
   * Validate a given configuration with the mocks.
   * The fail variable controls if we expect migration to fail, in which case we expect
   * a constant Long.MaxValue timestamp.
   */
  private def validateDecommissionTimestamps(conf: SparkConf, bm: BlockManager,
      fail: Boolean = false, assertDone: Boolean = true) = {
    // Verify the decommissioning manager timestamps and status
    val bmDecomManager = new BlockManagerDecommissioner(conf, bm)
    validateDecommissionTimestampsOnManager(bmDecomManager, fail, assertDone)
  }

  private def validateDecommissionTimestampsOnManager(bmDecomManager: BlockManagerDecommissioner,
      fail: Boolean = false, assertDone: Boolean = true, numShuffles: Option[Int] = None) = {
    var previousTime: Option[Long] = None
    try {
      bmDecomManager.start()
      eventually(timeout(100.second), interval(10.milliseconds)) {
        val (currentTime, done) = bmDecomManager.lastMigrationInfo()
        assert(!assertDone || done)
        // Make sure the time stamp starts moving forward.
        if (!fail) {
          previousTime match {
            case None =>
              previousTime = Some(currentTime)
              assert(false)
            case Some(t) =>
              assert(t < currentTime)
          }
        } else {
          // If we expect migration to fail we should get the max value quickly.
          assert(currentTime === Long.MaxValue)
        }
        numShuffles.foreach { s =>
          assert(bmDecomManager.numMigratedShuffles.get() === s)
        }
      }
      if (!fail) {
        // Wait 5 seconds and assert times keep moving forward.
        Thread.sleep(5000)
        val (currentTime, done) = bmDecomManager.lastMigrationInfo()
        assert((!assertDone || done) && currentTime > previousTime.get)
      }
    } finally {
      bmDecomManager.stop()
    }
  }

  test("test that with no blocks we finish migration") {
    // Set up the mocks so we return empty
    val bm = mock(classOf[BlockManager])
    val migratableShuffleBlockResolver = mock(classOf[MigratableResolver])
    when(migratableShuffleBlockResolver.getStoredShuffles())
      .thenReturn(Seq())
    when(bm.migratableResolver).thenReturn(migratableShuffleBlockResolver)
    when(bm.getMigratableRDDBlocks())
      .thenReturn(Seq())
    when(bm.getPeers(mc.any()))
      .thenReturn(Seq(BlockManagerId("exec2", "host2", 12345)))

    // Verify the decom manager handles this correctly
    validateDecommissionTimestamps(sparkConf, bm)
  }

  test("block decom manager with no migrations configured") {
    val bm = mock(classOf[BlockManager])
    val migratableShuffleBlockResolver = mock(classOf[MigratableResolver])
    registerShuffleBlocks(migratableShuffleBlockResolver, Set((1, 1L, 1)))
    when(bm.migratableResolver).thenReturn(migratableShuffleBlockResolver)
    when(bm.getMigratableRDDBlocks())
      .thenReturn(Seq())
    when(bm.getPeers(mc.any()))
      .thenReturn(Seq(BlockManagerId("exec2", "host2", 12345)))

    val badConf = new SparkConf(false)
      .set(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED, false)
      .set(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED, false)
      .set(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL, 10L)
    // Verify the decom manager handles this correctly
    validateDecommissionTimestamps(badConf, bm, fail = true)
  }

  test("block decom manager with no peers") {
    // Set up the mocks so we return one shuffle block
    val bm = mock(classOf[BlockManager])
    val migratableShuffleBlockResolver = mock(classOf[MigratableResolver])
    registerShuffleBlocks(migratableShuffleBlockResolver, Set((1, 1L, 1)))
    when(bm.migratableResolver).thenReturn(migratableShuffleBlockResolver)
    when(bm.getMigratableRDDBlocks())
      .thenReturn(Seq())
    when(bm.getPeers(mc.any()))
      .thenReturn(Seq())

    // Verify the decom manager handles this correctly
    validateDecommissionTimestamps(sparkConf, bm, fail = true)
  }


  test("block decom manager with only shuffle files time moves forward") {
    // Set up the mocks so we return one shuffle block
    val bm = mock(classOf[BlockManager])
    val migratableShuffleBlockResolver = mock(classOf[MigratableResolver])
    registerShuffleBlocks(migratableShuffleBlockResolver, Set((1, 1L, 1)))
    when(bm.migratableResolver).thenReturn(migratableShuffleBlockResolver)
    when(bm.getMigratableRDDBlocks())
      .thenReturn(Seq())
    when(bm.getPeers(mc.any()))
      .thenReturn(Seq(BlockManagerId("exec2", "host2", 12345)))

    // Verify the decom manager handles this correctly
    validateDecommissionTimestamps(sparkConf, bm)
  }

  test("block decom manager does not re-add removed shuffle files") {
    // Set up the mocks so we return one shuffle block
    val bm = mock(classOf[BlockManager])
    val migratableShuffleBlockResolver = mock(classOf[MigratableResolver])
    registerShuffleBlocks(migratableShuffleBlockResolver, Set())
    when(bm.migratableResolver).thenReturn(migratableShuffleBlockResolver)
    when(bm.getMigratableRDDBlocks())
      .thenReturn(Seq())
    when(bm.getPeers(mc.any()))
      .thenReturn(Seq(BlockManagerId("exec2", "host2", 12345)))
    val bmDecomManager = new BlockManagerDecommissioner(sparkConf, bm)
    bmDecomManager.migratingShuffles += ShuffleBlockInfo(10, 10)

    validateDecommissionTimestampsOnManager(bmDecomManager, fail = false, assertDone = false)
  }

  test("SPARK-40168: block decom manager handles shuffle file not found") {
    // Set up the mocks so we return one shuffle block
    val bm = mock(classOf[BlockManager])
    val migratableShuffleBlockResolver = mock(classOf[MigratableResolver])
    // First call get blocks, then empty list simulating a delete.
    when(migratableShuffleBlockResolver.getStoredShuffles())
      .thenReturn(Seq(ShuffleBlockInfo(1, 1)))
      .thenReturn(Seq())
    when(migratableShuffleBlockResolver.getMigrationBlocks(mc.any()))
      .thenReturn(
        List(
          (ShuffleIndexBlockId(1, 1, 1), mock(classOf[ManagedBuffer])),
          (ShuffleDataBlockId(1, 1, 1), mock(classOf[ManagedBuffer]))))
      .thenReturn(List())

    when(bm.migratableResolver).thenReturn(migratableShuffleBlockResolver)
    when(bm.getMigratableRDDBlocks())
      .thenReturn(Seq())
    when(bm.getPeers(mc.any()))
      .thenReturn(Seq(BlockManagerId("exec2", "host2", 12345)))

    val blockTransferService = mock(classOf[BlockTransferService])
    // Simulate FileNotFoundException wrap inside SparkException
    when(
      blockTransferService
        .uploadBlock(mc.any(), mc.any(), mc.any(), mc.any(), mc.any(), mc.any(), mc.isNull()))
      .thenReturn(Future.failed(
        new java.io.IOException("boop", new FileNotFoundException("file not found"))))
    when(
      blockTransferService
        .uploadBlockSync(mc.any(), mc.any(), mc.any(), mc.any(), mc.any(), mc.any(), mc.isNull()))
      .thenCallRealMethod()

    when(bm.blockTransferService).thenReturn(blockTransferService)

    // Verify the decom manager handles this correctly
    val bmDecomManager = new BlockManagerDecommissioner(sparkConf, bm)
    validateDecommissionTimestampsOnManager(
      bmDecomManager,
      numShuffles = Option(1))
  }

  test("block decom manager handles IO failures") {
    // Set up the mocks so we return one shuffle block
    val bm = mock(classOf[BlockManager])
    val migratableShuffleBlockResolver = mock(classOf[MigratableResolver])
    registerShuffleBlocks(migratableShuffleBlockResolver, Set((1, 1L, 1)))
    when(bm.migratableResolver).thenReturn(migratableShuffleBlockResolver)
    when(bm.getMigratableRDDBlocks())
      .thenReturn(Seq())
    when(bm.getPeers(mc.any()))
      .thenReturn(Seq(BlockManagerId("exec2", "host2", 12345)))

    val blockTransferService = mock(classOf[BlockTransferService])
    // Simulate an ambiguous IO error (e.g. block could be gone, connection failed, etc.)
    when(blockTransferService.uploadBlockSync(
      mc.any(), mc.any(), mc.any(), mc.any(), mc.any(), mc.any(), mc.isNull())).thenThrow(
      new java.io.IOException("boop")
    )

    when(bm.blockTransferService).thenReturn(blockTransferService)

    // Verify the decom manager handles this correctly
    val bmDecomManager = new BlockManagerDecommissioner(sparkConf, bm)
    validateDecommissionTimestampsOnManager(bmDecomManager, fail = false)
  }

  test("block decom manager short circuits removed blocks") {
    // Set up the mocks so we return one shuffle block
    val bm = mock(classOf[BlockManager])
    val migratableShuffleBlockResolver = mock(classOf[MigratableResolver])
    // First call get blocks, then empty list simulating a delete.
    when(migratableShuffleBlockResolver.getStoredShuffles())
      .thenReturn(Seq(ShuffleBlockInfo(1, 1)))
      .thenReturn(Seq())
    when(migratableShuffleBlockResolver.getMigrationBlocks(mc.any()))
      .thenReturn(List(
        (ShuffleIndexBlockId(1, 1, 1), mock(classOf[ManagedBuffer])),
        (ShuffleDataBlockId(1, 1, 1), mock(classOf[ManagedBuffer]))))
      .thenReturn(List())

    when(bm.migratableResolver).thenReturn(migratableShuffleBlockResolver)
    when(bm.getMigratableRDDBlocks())
      .thenReturn(Seq())
    when(bm.getPeers(mc.any()))
      .thenReturn(Seq(BlockManagerId("exec2", "host2", 12345)))

    val blockTransferService = mock(classOf[BlockTransferService])
    // Simulate an ambiguous IO error (e.g. block could be gone, connection failed, etc.)
    when(blockTransferService.uploadBlockSync(
      mc.any(), mc.any(), mc.any(), mc.any(), mc.any(), mc.any(), mc.isNull())).thenThrow(
      new java.io.IOException("boop")
    )

    when(bm.blockTransferService).thenReturn(blockTransferService)

    // Verify the decom manager handles this correctly
    val bmDecomManager = new BlockManagerDecommissioner(sparkConf, bm)
    validateDecommissionTimestampsOnManager(bmDecomManager, fail = false,
      numShuffles = Some(1))
  }

  test("test shuffle and cached rdd migration without any error") {
    val blockTransferService = mock(classOf[BlockTransferService])
    val bm = mock(classOf[BlockManager])

    val storedBlockId1 = RDDBlockId(0, 0)
    val storedBlock1 =
      new ReplicateBlock(storedBlockId1, Seq(BlockManagerId("replicaHolder", "host1", bmPort)), 1)

    val migratableShuffleBlockResolver = mock(classOf[MigratableResolver])
    registerShuffleBlocks(migratableShuffleBlockResolver, Set((1, 1L, 1)))
    when(bm.getPeers(mc.any()))
      .thenReturn(Seq(BlockManagerId("exec2", "host2", 12345)))

    when(bm.blockTransferService).thenReturn(blockTransferService)
    when(bm.migratableResolver).thenReturn(migratableShuffleBlockResolver)
    when(bm.getMigratableRDDBlocks())
      .thenReturn(Seq(storedBlock1))

    val bmDecomManager = new BlockManagerDecommissioner(sparkConf, bm)

    try {
      bmDecomManager.start()

      var previousRDDTime: Option[Long] = None
      var previousShuffleTime: Option[Long] = None

      // We don't check that all blocks are migrated because out mock is always returning an RDD.
      eventually(timeout(100.second), interval(10.milliseconds)) {
        assert(bmDecomManager.shufflesToMigrate.isEmpty === true)
        assert(bmDecomManager.numMigratedShuffles.get() === 1)
        verify(bm, least(1)).replicateBlock(
          mc.eq(storedBlockId1), mc.any(), mc.any(), mc.eq(Some(3)))
        verify(blockTransferService, times(2))
          .uploadBlockSync(mc.eq("host2"), mc.eq(bmPort), mc.eq("exec2"), mc.any(), mc.any(),
            mc.eq(StorageLevel.DISK_ONLY), mc.isNull())
        // Since we never "finish" the RDD blocks, make sure the time is always moving forward.
        assert(bmDecomManager.rddBlocksLeft)
        previousRDDTime match {
          case None =>
            previousRDDTime = Some(bmDecomManager.lastRDDMigrationTime)
            assert(false)
          case Some(t) =>
            assert(bmDecomManager.lastRDDMigrationTime > t)
        }
        // Since we do eventually finish the shuffle blocks make sure the shuffle blocks complete
        // and that the time keeps moving forward.
        assert(!bmDecomManager.shuffleBlocksLeft)
        previousShuffleTime match {
          case None =>
            previousShuffleTime = Some(bmDecomManager.lastShuffleMigrationTime)
            assert(false)
          case Some(t) =>
            assert(bmDecomManager.lastShuffleMigrationTime > t)
        }
      }
    } finally {
        bmDecomManager.stop()
    }
  }
}
