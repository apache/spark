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

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite

class BlockManagerInfoSuite extends SparkFunSuite {

  def testWithShuffleServiceOnOff(testName: String)
      (f: (Boolean, BlockManagerInfo) => Unit): Unit = {
    Seq(true, false).foreach { shuffleServiceEnabled =>
      val blockManagerInfo = new BlockManagerInfo(
        BlockManagerId("executor0", "host", 1234, None),
        timeMs = 300,
        maxOnHeapMem = 10000,
        maxOffHeapMem = 20000,
        slaveEndpoint = null,
        if (shuffleServiceEnabled) Some(new JHashMap[BlockId, BlockStatus]) else None)
      test(s"$testName externalShuffleServiceEnabled=$shuffleServiceEnabled") {
        f(shuffleServiceEnabled, blockManagerInfo)
      }
    }
  }

  testWithShuffleServiceOnOff("add broadcast block") { (_, blockManagerInfo) =>
    val broadcastId: BlockId = BroadcastBlockId(0, "field1")
    blockManagerInfo.updateBlockInfo(
      broadcastId, StorageLevel.MEMORY_AND_DISK, memSize = 200, diskSize = 100)
    assert(blockManagerInfo.blocks.asScala
      === Map(broadcastId -> BlockStatus(StorageLevel.MEMORY_AND_DISK, 0, 100)))
    assert(blockManagerInfo.exclusiveCachedBlocks === Set())
    assert(blockManagerInfo.remainingMem == 29800)
  }

  testWithShuffleServiceOnOff("add RDD block with MEMORY_ONLY") {
    (shuffleServiceEnabled, blockManagerInfo) =>
    val rddId: BlockId = RDDBlockId(0, 0)
    blockManagerInfo.updateBlockInfo(rddId, StorageLevel.MEMORY_ONLY, memSize = 200, diskSize = 0)
    assert(blockManagerInfo.blocks.asScala
      === Map(rddId -> BlockStatus(StorageLevel.MEMORY_ONLY, 200, 0)))
    assert(blockManagerInfo.exclusiveCachedBlocks === Set(rddId))
    assert(blockManagerInfo.remainingMem == 29800)
    if (shuffleServiceEnabled) {
      assert(blockManagerInfo.externalShuffleServiceBlockStatus.get.asScala === Map())
    }
  }

  testWithShuffleServiceOnOff("add RDD block with MEMORY_AND_DISK") {
    (shuffleServiceEnabled, blockManagerInfo) =>
    val rddId: BlockId = RDDBlockId(0, 0)
    blockManagerInfo.updateBlockInfo(
      rddId, StorageLevel.MEMORY_AND_DISK, memSize = 200, diskSize = 400)
    assert(blockManagerInfo.blocks.asScala
      === Map(rddId -> BlockStatus(StorageLevel.MEMORY_AND_DISK, 0, 400)))
    val exclusiveCachedBlocksForOneMemoryOnly = if (shuffleServiceEnabled) Set() else Set(rddId)
    assert(blockManagerInfo.exclusiveCachedBlocks === exclusiveCachedBlocksForOneMemoryOnly)
    assert(blockManagerInfo.remainingMem == 29800)
    if (shuffleServiceEnabled) {
      assert(blockManagerInfo.externalShuffleServiceBlockStatus.get.asScala
        === Map(rddId -> BlockStatus(StorageLevel.MEMORY_AND_DISK, 0, 400)))
    }
  }

  testWithShuffleServiceOnOff("add RDD block with DISK_ONLY") {
    (shuffleServiceEnabled, blockManagerInfo) =>
    val rddId: BlockId = RDDBlockId(0, 0)
    blockManagerInfo.updateBlockInfo(rddId, StorageLevel.DISK_ONLY, memSize = 0, diskSize = 200)
    assert(blockManagerInfo.blocks.asScala
      === Map(rddId -> BlockStatus(StorageLevel.DISK_ONLY, 0, 200)))
    val exclusiveCachedBlocksForOneMemoryOnly = if (shuffleServiceEnabled) Set() else Set(rddId)
    assert(blockManagerInfo.exclusiveCachedBlocks === exclusiveCachedBlocksForOneMemoryOnly)
    assert(blockManagerInfo.remainingMem == 30000)
    if (shuffleServiceEnabled) {
      assert(blockManagerInfo.externalShuffleServiceBlockStatus.get.asScala
        === Map(rddId -> BlockStatus(StorageLevel.DISK_ONLY, 0, 200)))
    }
  }

  testWithShuffleServiceOnOff("update RDD block from MEMORY_ONLY to DISK_ONLY") {
    (shuffleServiceEnabled, blockManagerInfo) =>
    val rddId: BlockId = RDDBlockId(0, 0)
    blockManagerInfo.updateBlockInfo(rddId, StorageLevel.MEMORY_ONLY, memSize = 200, 0)
    assert(blockManagerInfo.blocks.asScala
      === Map(rddId -> BlockStatus(StorageLevel.MEMORY_ONLY, 200, 0)))
    assert(blockManagerInfo.exclusiveCachedBlocks === Set(rddId))
    assert(blockManagerInfo.remainingMem == 29800)
    if (shuffleServiceEnabled) {
      assert(blockManagerInfo.externalShuffleServiceBlockStatus.get.asScala == Map())
    }

    blockManagerInfo.updateBlockInfo(rddId, StorageLevel.DISK_ONLY, memSize = 0, diskSize = 200)
    assert(blockManagerInfo.blocks.asScala
      === Map(rddId -> BlockStatus(StorageLevel.DISK_ONLY, 0, 200)))
    val exclusiveCachedBlocksForNoMemoryOnly = if (shuffleServiceEnabled) Set() else Set(rddId)
    assert(blockManagerInfo.exclusiveCachedBlocks === exclusiveCachedBlocksForNoMemoryOnly)
    assert(blockManagerInfo.remainingMem == 30000)
    if (shuffleServiceEnabled) {
      assert(blockManagerInfo.externalShuffleServiceBlockStatus.get.asScala
        === Map(rddId -> BlockStatus(StorageLevel.DISK_ONLY, 0, 200)))
    }
  }

  testWithShuffleServiceOnOff("using invalid StorageLevel") {
    (shuffleServiceEnabled, blockManagerInfo) =>
    val rddId: BlockId = RDDBlockId(0, 0)
    blockManagerInfo.updateBlockInfo(rddId, StorageLevel.DISK_ONLY, memSize = 0, diskSize = 200)
    assert(blockManagerInfo.blocks.asScala
      === Map(rddId -> BlockStatus(StorageLevel.DISK_ONLY, 0, 200)))
    val exclusiveCachedBlocksForOneMemoryOnly = if (shuffleServiceEnabled) Set() else Set(rddId)
    assert(blockManagerInfo.exclusiveCachedBlocks === exclusiveCachedBlocksForOneMemoryOnly)
    assert(blockManagerInfo.remainingMem == 30000)
    if (shuffleServiceEnabled) {
      assert(blockManagerInfo.externalShuffleServiceBlockStatus.get.asScala
        === Map(rddId -> BlockStatus(StorageLevel.DISK_ONLY, 0, 200)))
    }

    blockManagerInfo.updateBlockInfo(rddId, StorageLevel.NONE, memSize = 0, diskSize = 200)
    assert(blockManagerInfo.blocks.asScala === Map())
    assert(blockManagerInfo.exclusiveCachedBlocks === Set())
    assert(blockManagerInfo.remainingMem == 30000)
    if (shuffleServiceEnabled) {
      assert(blockManagerInfo.externalShuffleServiceBlockStatus.get.asScala === Map())
    }
  }

  testWithShuffleServiceOnOff("remove block") {
    (shuffleServiceEnabled, blockManagerInfo) =>
    val rddId: BlockId = RDDBlockId(0, 0)
    blockManagerInfo.updateBlockInfo(rddId, StorageLevel.DISK_ONLY, memSize = 0, diskSize = 200)
    assert(blockManagerInfo.blocks.asScala
      === Map(rddId -> BlockStatus(StorageLevel.DISK_ONLY, 0, 200)))
    val exclusiveCachedBlocksForOneMemoryOnly = if (shuffleServiceEnabled) Set() else Set(rddId)
    assert(blockManagerInfo.exclusiveCachedBlocks === exclusiveCachedBlocksForOneMemoryOnly)
    assert(blockManagerInfo.remainingMem == 30000)
    if (shuffleServiceEnabled) {
      assert(blockManagerInfo.externalShuffleServiceBlockStatus.get.asScala
        === Map(rddId -> BlockStatus(StorageLevel.DISK_ONLY, 0, 200)))
    }

    blockManagerInfo.removeBlock(rddId)
    assert(blockManagerInfo.blocks.asScala === Map())
    assert(blockManagerInfo.exclusiveCachedBlocks === Set())
    assert(blockManagerInfo.remainingMem == 30000)
    if (shuffleServiceEnabled) {
      assert(blockManagerInfo.externalShuffleServiceBlockStatus.get.asScala === Map())
    }
  }
}
