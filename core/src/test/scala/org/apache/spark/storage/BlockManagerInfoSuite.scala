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

import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite

class BlockManagerInfoSuite extends SparkFunSuite {

  private def testWithShuffleServiceOnOff(testName: String)
      (f: (Boolean, BlockManagerInfo) => Unit): Unit = {
    Seq(true, false).foreach { svcEnabled =>
      val bmInfo = new BlockManagerInfo(
        BlockManagerId("executor0", "host", 1234, None),
        timeMs = 300,
        maxOnHeapMem = 10000,
        maxOffHeapMem = 20000,
        storageEndpoint = null,
        if (svcEnabled) Some(new BlockStatusPerBlockId) else None)
      test(s"$testName externalShuffleServiceEnabled=$svcEnabled") {
        f(svcEnabled, bmInfo)
      }
    }
  }

  private def getEssBlockStatus(bmInfo: BlockManagerInfo, blockId: BlockId): Option[BlockStatus] = {
    assert(bmInfo.externalShuffleServiceBlockStatus.isDefined)
    val blockStatusPerBlockId = bmInfo.externalShuffleServiceBlockStatus.get
    blockStatusPerBlockId.get(blockId)
  }

  testWithShuffleServiceOnOff("broadcast block") { (_, bmInfo) =>
    val broadcastId: BlockId = BroadcastBlockId(0, "field1")
    bmInfo.updateBlockInfo(
      broadcastId, StorageLevel.MEMORY_AND_DISK, memSize = 200, diskSize = 100)
    assert(bmInfo.blocks.asScala ===
      Map(broadcastId -> BlockStatus(StorageLevel.MEMORY_AND_DISK, 0, 100)))
    assert(bmInfo.remainingMem === 29800)
  }

  testWithShuffleServiceOnOff("RDD block with MEMORY_ONLY") { (svcEnabled, bmInfo) =>
    val rddId: BlockId = RDDBlockId(0, 0)
    bmInfo.updateBlockInfo(rddId, StorageLevel.MEMORY_ONLY, memSize = 200, diskSize = 0)
    assert(bmInfo.blocks.asScala ===
      Map(rddId -> BlockStatus(StorageLevel.MEMORY_ONLY, 200, 0)))
    assert(bmInfo.remainingMem === 29800)
    if (svcEnabled) {
      assert(getEssBlockStatus(bmInfo, rddId).isEmpty)
    }
    bmInfo.updateBlockInfo(rddId, StorageLevel.NONE, memSize = 0, diskSize = 0)
    assert(bmInfo.remainingMem === 30000)
  }

  testWithShuffleServiceOnOff("RDD block with MEMORY_AND_DISK") { (svcEnabled, bmInfo) =>
    // This is the effective storage level, not the requested storage level, but MEMORY_AND_DISK
    // is still possible if it's first in memory, purged to disk, and later promoted back to memory.
    val rddId: BlockId = RDDBlockId(0, 0)
    bmInfo.updateBlockInfo(rddId, StorageLevel.MEMORY_AND_DISK, memSize = 200, diskSize = 400)
    assert(bmInfo.blocks.asScala ===
      Map(rddId -> BlockStatus(StorageLevel.MEMORY_AND_DISK, 0, 400)))
    assert(bmInfo.remainingMem === 29800)
    if (svcEnabled) {
      assert(getEssBlockStatus(bmInfo, rddId) ===
        Some(BlockStatus(StorageLevel.MEMORY_AND_DISK, 0, 400)))
    }
  }

  testWithShuffleServiceOnOff("RDD block with DISK_ONLY") { (svcEnabled, bmInfo) =>
    val rddId: BlockId = RDDBlockId(0, 0)
    bmInfo.updateBlockInfo(rddId, StorageLevel.DISK_ONLY, memSize = 0, diskSize = 200)
    assert(bmInfo.blocks.asScala ===
      Map(rddId -> BlockStatus(StorageLevel.DISK_ONLY, 0, 200)))
    val exclusiveCachedBlocksForOneMemoryOnly = if (svcEnabled) Set() else Set(rddId)
    assert(bmInfo.remainingMem === 30000)
    if (svcEnabled) {
      assert(getEssBlockStatus(bmInfo, rddId) === Some(BlockStatus(StorageLevel.DISK_ONLY, 0, 200)))
    }
  }

  testWithShuffleServiceOnOff("update from MEMORY_ONLY to DISK_ONLY") { (svcEnabled, bmInfo) =>
    // This happens if MEMORY_AND_DISK is the requested storage level, but the block gets purged
    // to disk under memory pressure.
    val rddId: BlockId = RDDBlockId(0, 0)
    bmInfo.updateBlockInfo(rddId, StorageLevel.MEMORY_ONLY, memSize = 200, 0)
    assert(bmInfo.blocks.asScala  === Map(rddId -> BlockStatus(StorageLevel.MEMORY_ONLY, 200, 0)))
    assert(bmInfo.remainingMem === 29800)
    if (svcEnabled) {
      assert(getEssBlockStatus(bmInfo, rddId).isEmpty)
    }

    bmInfo.updateBlockInfo(rddId, StorageLevel.DISK_ONLY, memSize = 0, diskSize = 200)
    assert(bmInfo.blocks.asScala === Map(rddId -> BlockStatus(StorageLevel.DISK_ONLY, 0, 200)))
    assert(bmInfo.remainingMem === 30000)
    if (svcEnabled) {
      assert(getEssBlockStatus(bmInfo, rddId) === Some(BlockStatus(StorageLevel.DISK_ONLY, 0, 200)))
    }
  }

  testWithShuffleServiceOnOff("using invalid StorageLevel") { (svcEnabled, bmInfo) =>
    val rddId: BlockId = RDDBlockId(0, 0)
    bmInfo.updateBlockInfo(rddId, StorageLevel.DISK_ONLY, memSize = 0, diskSize = 200)
    assert(bmInfo.blocks.asScala === Map(rddId -> BlockStatus(StorageLevel.DISK_ONLY, 0, 200)))
    assert(bmInfo.remainingMem === 30000)
    if (svcEnabled) {
      assert(getEssBlockStatus(bmInfo, rddId) === Some(BlockStatus(StorageLevel.DISK_ONLY, 0, 200)))
    }

    bmInfo.updateBlockInfo(rddId, StorageLevel.NONE, memSize = 0, diskSize = 200)
    assert(bmInfo.blocks.isEmpty)
    assert(bmInfo.remainingMem === 30000)
    if (svcEnabled) {
      assert(getEssBlockStatus(bmInfo, rddId).isEmpty)
    }
  }

  testWithShuffleServiceOnOff("remove block and add another one") { (svcEnabled, bmInfo) =>
    val rddId1: BlockId = RDDBlockId(0, 0)
    bmInfo.updateBlockInfo(rddId1, StorageLevel.DISK_ONLY, memSize = 0, diskSize = 200)
    assert(bmInfo.blocks.asScala === Map(rddId1 -> BlockStatus(StorageLevel.DISK_ONLY, 0, 200)))
    assert(bmInfo.remainingMem === 30000)
    if (svcEnabled) {
      assert(getEssBlockStatus(bmInfo, rddId1) ===
        Some(BlockStatus(StorageLevel.DISK_ONLY, 0, 200)))
    }

    bmInfo.removeBlock(rddId1)
    assert(bmInfo.blocks.asScala.isEmpty)
    assert(bmInfo.remainingMem === 30000)
    if (svcEnabled) {
      assert(getEssBlockStatus(bmInfo, rddId1).isEmpty)
    }
    val rddId2: BlockId = RDDBlockId(0, 1)
    bmInfo.updateBlockInfo(rddId2, StorageLevel.DISK_ONLY, memSize = 0, diskSize = 200)
    assert(bmInfo.blocks.asScala === Map(rddId2 -> BlockStatus(StorageLevel.DISK_ONLY, 0, 200)))
    assert(bmInfo.remainingMem === 30000)
    if (svcEnabled) {
      assert(getEssBlockStatus(bmInfo, rddId2) ===
        Some(BlockStatus(StorageLevel.DISK_ONLY, 0, 200)))
    }
  }
}
