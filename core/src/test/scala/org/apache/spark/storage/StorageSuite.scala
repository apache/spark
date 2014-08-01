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

import org.scalatest.FunSuite

/**
 * Test various functionalities in StorageUtils and StorageStatus.
 */
class StorageSuite extends FunSuite {
  private val memAndDisk = StorageLevel.MEMORY_AND_DISK

  // For testing add/update/removeBlock (for non-RDD blocks)
  private def storageStatus1: StorageStatus = {
    val status = new StorageStatus(BlockManagerId("big", "dog", 1, 1), 1000L)
    assert(status.blocks.isEmpty)
    assert(status.rddBlocks.isEmpty)
    assert(status.memUsed === 0)
    assert(status.memRemaining === 1000L)
    assert(status.diskUsed === 0)
    status.addBlock(TestBlockId("foo"), BlockStatus(memAndDisk, 10L, 20L, 0L))
    status.addBlock(TestBlockId("fee"), BlockStatus(memAndDisk, 10L, 20L, 0L))
    status.addBlock(TestBlockId("faa"), BlockStatus(memAndDisk, 10L, 20L, 0L))
    status
  }

  test("storage status add non-RDD blocks") {
    val status = storageStatus1
    assert(status.blocks.size === 3)
    assert(status.blocks.contains(TestBlockId("foo")))
    assert(status.blocks.contains(TestBlockId("fee")))
    assert(status.blocks.contains(TestBlockId("faa")))
    assert(status.rddBlocks.isEmpty)
    assert(status.memUsed === 30L)
    assert(status.memRemaining === 970L)
    assert(status.diskUsed === 60L)
  }

  test("storage status update non-RDD blocks") {
    val status = storageStatus1
    status.updateBlock(TestBlockId("foo"), BlockStatus(memAndDisk, 50L, 100L, 0L))
    status.updateBlock(TestBlockId("fee"), BlockStatus(memAndDisk, 100L, 20L, 0L))
    assert(status.blocks.size === 3)
    assert(status.memUsed === 160L)
    assert(status.memRemaining === 840L)
    assert(status.diskUsed === 140L)
  }

  test("storage status remove non-RDD blocks") {
    val status = storageStatus1
    status.removeBlock(TestBlockId("foo"))
    status.removeBlock(TestBlockId("faa"))
    assert(status.blocks.size === 1)
    assert(status.blocks.contains(TestBlockId("fee")))
    assert(status.memUsed === 10L)
    assert(status.memRemaining === 990L)
    assert(status.diskUsed === 20L)
  }

  // For testing add/update/remove/contains/getBlock and numBlocks
  private def storageStatus2: StorageStatus = {
    val status = new StorageStatus(BlockManagerId("big", "dog", 1, 1), 1000L)
    assert(status.rddBlocks.isEmpty)
    status.addBlock(TestBlockId("dan"), BlockStatus(memAndDisk, 10L, 20L, 0L))
    status.addBlock(TestBlockId("man"), BlockStatus(memAndDisk, 10L, 20L, 0L))
    status.addBlock(RDDBlockId(0, 0), BlockStatus(memAndDisk, 10L, 20L, 0L))
    status.addBlock(RDDBlockId(1, 1), BlockStatus(memAndDisk, 100L, 200L, 0L))
    status.addBlock(RDDBlockId(2, 2), BlockStatus(memAndDisk, 10L, 20L, 0L))
    status.addBlock(RDDBlockId(2, 3), BlockStatus(memAndDisk, 10L, 20L, 0L))
    status.addBlock(RDDBlockId(2, 4), BlockStatus(memAndDisk, 10L, 40L, 0L))
    status
  }

  test("storage status add RDD blocks") {
    val status = storageStatus2
    assert(status.blocks.size === 7)
    assert(status.rddBlocks.size === 5)
    assert(status.rddBlocks.contains(RDDBlockId(0, 0)))
    assert(status.rddBlocks.contains(RDDBlockId(1, 1)))
    assert(status.rddBlocks.contains(RDDBlockId(2, 2)))
    assert(status.rddBlocks.contains(RDDBlockId(2, 3)))
    assert(status.rddBlocks.contains(RDDBlockId(2, 4)))
    assert(status.rddBlocksById(0).size === 1)
    assert(status.rddBlocksById(0).contains(RDDBlockId(0, 0)))
    assert(status.rddBlocksById(1).size === 1)
    assert(status.rddBlocksById(1).contains(RDDBlockId(1, 1)))
    assert(status.rddBlocksById(2).size === 3)
    assert(status.rddBlocksById(2).contains(RDDBlockId(2, 2)))
    assert(status.rddBlocksById(2).contains(RDDBlockId(2, 3)))
    assert(status.rddBlocksById(2).contains(RDDBlockId(2, 4)))
    assert(status.memUsedByRDD(0) === 10L)
    assert(status.memUsedByRDD(1) === 100L)
    assert(status.memUsedByRDD(2) === 30L)
    assert(status.diskUsedByRDD(0) === 20L)
    assert(status.diskUsedByRDD(1) === 200L)
    assert(status.diskUsedByRDD(2) === 80L)
  }

  test("storage status update RDD blocks") {
    val status = storageStatus2
    status.updateBlock(TestBlockId("dan"), BlockStatus(memAndDisk, 5000L, 0L, 0L))
    status.updateBlock(RDDBlockId(0, 0), BlockStatus(memAndDisk, 0L, 0L, 0L))
    status.updateBlock(RDDBlockId(2, 2), BlockStatus(memAndDisk, 0L, 1000L, 0L))
    assert(status.blocks.size === 7)
    assert(status.rddBlocks.size === 5)
    assert(status.rddBlocksById(0).size === 1)
    assert(status.rddBlocksById(1).size === 1)
    assert(status.rddBlocksById(2).size === 3)
    assert(status.memUsedByRDD(0) === 0L)
    assert(status.memUsedByRDD(1) === 100L)
    assert(status.memUsedByRDD(2) === 20L)
    assert(status.diskUsedByRDD(0) === 0L)
    assert(status.diskUsedByRDD(1) === 200L)
    assert(status.diskUsedByRDD(2) === 1060L)
  }

  test("storage status remove RDD blocks") {
    val status = storageStatus2
    status.removeBlock(TestBlockId("man"))
    status.removeBlock(RDDBlockId(1, 1))
    status.removeBlock(RDDBlockId(2, 2))
    status.removeBlock(RDDBlockId(2, 4))
    assert(status.blocks.size === 3)
    assert(status.rddBlocks.size === 2)
    assert(status.rddBlocks.contains(RDDBlockId(0, 0)))
    assert(status.rddBlocks.contains(RDDBlockId(2, 3)))
    assert(status.rddBlocksById(0).size === 1)
    assert(status.rddBlocksById(0).contains(RDDBlockId(0, 0)))
    assert(status.rddBlocksById(1).size === 0)
    assert(status.rddBlocksById(2).size === 1)
    assert(status.rddBlocksById(2).contains(RDDBlockId(2, 3)))
    assert(status.memUsedByRDD(0) === 10L)
    assert(status.memUsedByRDD(1) === 0L)
    assert(status.memUsedByRDD(2) === 10L)
    assert(status.diskUsedByRDD(0) === 20L)
    assert(status.diskUsedByRDD(1) === 0L)
    assert(status.diskUsedByRDD(2) === 20L)
  }

  test("storage status containsBlock") {
    val status = storageStatus2
    // blocks that actually exist
    assert(status.blocks.contains(TestBlockId("dan")) === status.containsBlock(TestBlockId("dan")))
    assert(status.blocks.contains(TestBlockId("man")) === status.containsBlock(TestBlockId("man")))
    assert(status.blocks.contains(RDDBlockId(0, 0)) === status.containsBlock(RDDBlockId(0, 0)))
    assert(status.blocks.contains(RDDBlockId(1, 1)) === status.containsBlock(RDDBlockId(1, 1)))
    assert(status.blocks.contains(RDDBlockId(2, 2)) === status.containsBlock(RDDBlockId(2, 2)))
    assert(status.blocks.contains(RDDBlockId(2, 3)) === status.containsBlock(RDDBlockId(2, 3)))
    assert(status.blocks.contains(RDDBlockId(2, 4)) === status.containsBlock(RDDBlockId(2, 4)))
    // blocks that don't exist
    assert(status.blocks.contains(TestBlockId("fan")) === status.containsBlock(TestBlockId("fan")))
    assert(status.blocks.contains(RDDBlockId(100, 0)) === status.containsBlock(RDDBlockId(100, 0)))
  }

  test("storage status getBlock") {
    val status = storageStatus2
    // blocks that actually exist
    assert(status.blocks.get(TestBlockId("dan")) === status.getBlock(TestBlockId("dan")))
    assert(status.blocks.get(TestBlockId("man")) === status.getBlock(TestBlockId("man")))
    assert(status.blocks.get(RDDBlockId(0, 0)) === status.getBlock(RDDBlockId(0, 0)))
    assert(status.blocks.get(RDDBlockId(1, 1)) === status.getBlock(RDDBlockId(1, 1)))
    assert(status.blocks.get(RDDBlockId(2, 2)) === status.getBlock(RDDBlockId(2, 2)))
    assert(status.blocks.get(RDDBlockId(2, 3)) === status.getBlock(RDDBlockId(2, 3)))
    assert(status.blocks.get(RDDBlockId(2, 4)) === status.getBlock(RDDBlockId(2, 4)))
    // blocks that don't exist
    assert(status.blocks.get(TestBlockId("fan")) === status.getBlock(TestBlockId("fan")))
    assert(status.blocks.get(RDDBlockId(100, 0)) === status.getBlock(RDDBlockId(100, 0)))
  }

  test("storage status numBlocks") {
    val status = storageStatus2
    assert(status.blocks.size === status.numBlocks)
    status.addBlock(RDDBlockId(4, 4), BlockStatus(memAndDisk, 0L, 0L, 100L))
    assert(status.blocks.size === status.numBlocks)
    status.addBlock(RDDBlockId(4, 8), BlockStatus(memAndDisk, 0L, 0L, 100L))
    assert(status.blocks.size === status.numBlocks)
    status.updateBlock(RDDBlockId(0, 0), BlockStatus(memAndDisk, 0L, 0L, 100L))
    assert(status.blocks.size === status.numBlocks)
    // update a block that doesn't exist
    status.updateBlock(RDDBlockId(100, 99), BlockStatus(memAndDisk, 0L, 0L, 100L))
    assert(status.blocks.size === status.numBlocks)
    status.removeBlock(RDDBlockId(0, 0))
    assert(status.blocks.size === status.numBlocks)
    // remove a block that doesn't exist
    status.removeBlock(RDDBlockId(1000, 999))
    assert(status.blocks.size === status.numBlocks)
  }

  // For testing StorageUtils.updateRddInfo and StorageUtils.getRddBlockLocations
  private def stockStorageStatuses: Seq[StorageStatus] = {
    val status1 = new StorageStatus(BlockManagerId("big", "dog", 1, 1), 1000L)
    val status2 = new StorageStatus(BlockManagerId("fat", "duck", 2, 2), 2000L)
    val status3 = new StorageStatus(BlockManagerId("fat", "cat", 3, 3), 3000L)
    status1.addBlock(RDDBlockId(0, 0), BlockStatus(memAndDisk, 1L, 2L, 0L))
    status1.addBlock(RDDBlockId(0, 1), BlockStatus(memAndDisk, 1L, 2L, 0L))
    status2.addBlock(RDDBlockId(0, 2), BlockStatus(memAndDisk, 1L, 2L, 0L))
    status2.addBlock(RDDBlockId(0, 3), BlockStatus(memAndDisk, 1L, 2L, 0L))
    status2.addBlock(RDDBlockId(1, 0), BlockStatus(memAndDisk, 1L, 2L, 0L))
    status2.addBlock(RDDBlockId(1, 1), BlockStatus(memAndDisk, 1L, 2L, 0L))
    status3.addBlock(RDDBlockId(0, 4), BlockStatus(memAndDisk, 1L, 2L, 0L))
    status3.addBlock(RDDBlockId(1, 2), BlockStatus(memAndDisk, 1L, 2L, 0L))
    Seq(status1, status2, status3)
  }

  // For testing StorageUtils.updateRddInfo
  private def stockRDDInfos: Seq[RDDInfo] = {
    val info0 = new RDDInfo(0, "0", 10, memAndDisk)
    val info1 = new RDDInfo(1, "1", 3, memAndDisk)
    Seq(info0, info1)
  }

  test("StorageUtils.updateRddInfo") {
    val storageStatuses = stockStorageStatuses
    val rddInfos = stockRDDInfos
    StorageUtils.updateRddInfo(rddInfos, storageStatuses)
    assert(rddInfos(0).numCachedPartitions === 5)
    assert(rddInfos(0).memSize === 5L)
    assert(rddInfos(0).diskSize === 10L)
    assert(rddInfos(1).numCachedPartitions === 3)
    assert(rddInfos(1).memSize === 3L)
    assert(rddInfos(1).diskSize === 6L)
  }

  test("StorageUtils.updateRddInfo with updated blocks") {
    val storageStatuses = stockStorageStatuses
    val rddInfos = stockRDDInfos

    // Drop 3 blocks from RDD 0, and cache more of RDD 1
    val updatedBlocks1 = Seq(
      (RDDBlockId(0, 0), BlockStatus(memAndDisk, 0L, 0L, 0L)),
      (RDDBlockId(0, 1), BlockStatus(memAndDisk, 0L, 0L, 0L)),
      (RDDBlockId(0, 2), BlockStatus(memAndDisk, 0L, 0L, 0L)),
      (RDDBlockId(1, 0), BlockStatus(memAndDisk, 100L, 100L, 0L)),
      (RDDBlockId(1, 100), BlockStatus(memAndDisk, 100L, 100L, 0L))
    )
    StorageUtils.updateRddInfo(rddInfos, storageStatuses, updatedBlocks1)
    assert(rddInfos(0).numCachedPartitions === 2)
    assert(rddInfos(0).memSize === 2L)
    assert(rddInfos(0).diskSize === 4L)
    assert(rddInfos(1).numCachedPartitions === 4)
    assert(rddInfos(1).memSize === 202L)
    assert(rddInfos(1).diskSize === 204L)

    // Actually update storage statuses so we can chain the calls to StorageUtils.updateRddInfo
    updatedBlocks1.foreach { case (bid, bstatus) =>
      storageStatuses.find(_.containsBlock(bid)) match {
        case Some(s) => s.updateBlock(bid, bstatus)
        case None => storageStatuses(0).addBlock(bid, bstatus) // arbitrarily pick the first
      }
    }

    // Drop all of RDD 1, following previous updates
    val updatedBlocks2 = Seq(
      (RDDBlockId(1, 0), BlockStatus(memAndDisk, 0L, 0L, 0L)),
      (RDDBlockId(1, 1), BlockStatus(memAndDisk, 0L, 0L, 0L)),
      (RDDBlockId(1, 2), BlockStatus(memAndDisk, 0L, 0L, 0L)),
      (RDDBlockId(1, 100), BlockStatus(memAndDisk, 0L, 0L, 0L))
    )
    StorageUtils.updateRddInfo(rddInfos, storageStatuses, updatedBlocks2)
    assert(rddInfos(0).numCachedPartitions === 2)
    assert(rddInfos(0).memSize === 2L)
    assert(rddInfos(0).diskSize === 4L)
    assert(rddInfos(1).numCachedPartitions === 0)
    assert(rddInfos(1).memSize === 0L)
    assert(rddInfos(1).diskSize === 0L)
  }

  test("StorageUtils.getRddBlockLocations") {
    val storageStatuses = stockStorageStatuses
    val blockLocations0 = StorageUtils.getRddBlockLocations(storageStatuses, 0)
    val blockLocations1 = StorageUtils.getRddBlockLocations(storageStatuses, 1)
    assert(blockLocations0.size === 5)
    assert(blockLocations1.size === 3)
    assert(blockLocations0.contains(RDDBlockId(0, 0)))
    assert(blockLocations0.contains(RDDBlockId(0, 1)))
    assert(blockLocations0.contains(RDDBlockId(0, 2)))
    assert(blockLocations0.contains(RDDBlockId(0, 3)))
    assert(blockLocations0.contains(RDDBlockId(0, 4)))
    assert(blockLocations1.contains(RDDBlockId(1, 0)))
    assert(blockLocations1.contains(RDDBlockId(1, 1)))
    assert(blockLocations1.contains(RDDBlockId(1, 2)))
    assert(blockLocations0(RDDBlockId(0, 0)) === Seq("dog:1"))
    assert(blockLocations0(RDDBlockId(0, 1)) === Seq("dog:1"))
    assert(blockLocations0(RDDBlockId(0, 2)) === Seq("duck:2"))
    assert(blockLocations0(RDDBlockId(0, 3)) === Seq("duck:2"))
    assert(blockLocations0(RDDBlockId(0, 4)) === Seq("cat:3"))
    assert(blockLocations1(RDDBlockId(1, 0)) === Seq("duck:2"))
    assert(blockLocations1(RDDBlockId(1, 1)) === Seq("duck:2"))
    assert(blockLocations1(RDDBlockId(1, 2)) === Seq("cat:3"))
  }

  test("StorageUtils.getRddBlockLocations with multiple locations") {
    val storageStatuses = stockStorageStatuses
    storageStatuses(0).addBlock(RDDBlockId(1, 0), BlockStatus(memAndDisk, 1L, 2L, 0L))
    storageStatuses(0).addBlock(RDDBlockId(0, 4), BlockStatus(memAndDisk, 1L, 2L, 0L))
    storageStatuses(2).addBlock(RDDBlockId(0, 0), BlockStatus(memAndDisk, 1L, 2L, 0L))
    val blockLocations0 = StorageUtils.getRddBlockLocations(storageStatuses, 0)
    val blockLocations1 = StorageUtils.getRddBlockLocations(storageStatuses, 1)
    assert(blockLocations0.size === 5)
    assert(blockLocations1.size === 3)
    assert(blockLocations0(RDDBlockId(0, 0)) === Seq("dog:1", "cat:3"))
    assert(blockLocations0(RDDBlockId(0, 1)) === Seq("dog:1"))
    assert(blockLocations0(RDDBlockId(0, 2)) === Seq("duck:2"))
    assert(blockLocations0(RDDBlockId(0, 3)) === Seq("duck:2"))
    assert(blockLocations0(RDDBlockId(0, 4)) === Seq("dog:1", "cat:3"))
    assert(blockLocations1(RDDBlockId(1, 0)) === Seq("dog:1", "duck:2"))
    assert(blockLocations1(RDDBlockId(1, 1)) === Seq("duck:2"))
    assert(blockLocations1(RDDBlockId(1, 2)) === Seq("cat:3"))
  }

}
