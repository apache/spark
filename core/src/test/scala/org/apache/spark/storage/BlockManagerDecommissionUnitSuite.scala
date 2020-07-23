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

import scala.concurrent.duration._

import org.mockito.{ArgumentMatchers => mc}
import org.mockito.Mockito.{mock, times, verify, when}
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

      eventually(timeout(5.second), interval(10.milliseconds)) {
        assert(bmDecomManager.shufflesToMigrate.isEmpty == true)
        verify(bm, times(1)).replicateBlock(
          mc.eq(storedBlockId1), mc.any(), mc.any(), mc.eq(Some(3)))
        verify(blockTransferService, times(2))
          .uploadBlockSync(mc.eq("host2"), mc.eq(bmPort), mc.eq("exec2"), mc.any(), mc.any(),
            mc.eq(StorageLevel.DISK_ONLY), mc.isNull())
      }
    } finally {
        bmDecomManager.stop()
    }
  }
}
