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

import org.apache.spark.SparkFunSuite

/**
 * Test various functionalities in StorageUtils and StorageStatus.
 */
class StorageSuite extends SparkFunSuite {
  private val memAndDisk = StorageLevel.MEMORY_AND_DISK

  // For testing add, update, and remove (for non-RDD blocks)
  private def storageStatus1: StorageStatus = {
    val status = new StorageStatus(BlockManagerId("big", "dog", 1), 1000L)
    assert(status.blocks.isEmpty)
    assert(status.rddBlocks.isEmpty)
    assert(status.memUsed === 0L)
    assert(status.memRemaining === 1000L)
    assert(status.diskUsed === 0L)
    assert(status.offHeapUsed === 0L)
    status.addBlock(TestBlockId("foo"), BlockStatus(memAndDisk, 10L, 20L, 1L))
    status.addBlock(TestBlockId("fee"), BlockStatus(memAndDisk, 10L, 20L, 1L))
    status.addBlock(TestBlockId("faa"), BlockStatus(memAndDisk, 10L, 20L, 1L))
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
    assert(status.offHeapUsed === 3L)
  }

  test("storage status update non-RDD blocks") {
    val status = storageStatus1
    status.updateBlock(TestBlockId("foo"), BlockStatus(memAndDisk, 50L, 100L, 1L))
    status.updateBlock(TestBlockId("fee"), BlockStatus(memAndDisk, 100L, 20L, 0L))
    assert(status.blocks.size === 3)
    assert(status.memUsed === 160L)
    assert(status.memRemaining === 840L)
    assert(status.diskUsed === 140L)
    assert(status.offHeapUsed === 2L)
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
    assert(status.offHeapUsed === 1L)
  }

  // For testing add, update, remove, get, and contains etc. for both RDD and non-RDD blocks
  private def storageStatus2: StorageStatus = {
    val status = new StorageStatus(BlockManagerId("big", "dog", 1), 1000L)
    assert(status.rddBlocks.isEmpty)
    status.addBlock(TestBlockId("dan"), BlockStatus(memAndDisk, 10L, 20L, 0L))
    status.addBlock(TestBlockId("man"), BlockStatus(memAndDisk, 10L, 20L, 0L))
    status.addBlock(RDDBlockId(0, 0), BlockStatus(memAndDisk, 10L, 20L, 1L))
    status.addBlock(RDDBlockId(1, 1), BlockStatus(memAndDisk, 100L, 200L, 1L))
    status.addBlock(RDDBlockId(2, 2), BlockStatus(memAndDisk, 10L, 20L, 1L))
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
    assert(status.memUsedByRdd(0) === 10L)
    assert(status.memUsedByRdd(1) === 100L)
    assert(status.memUsedByRdd(2) === 30L)
    assert(status.diskUsedByRdd(0) === 20L)
    assert(status.diskUsedByRdd(1) === 200L)
    assert(status.diskUsedByRdd(2) === 80L)
    assert(status.offHeapUsedByRdd(0) === 1L)
    assert(status.offHeapUsedByRdd(1) === 1L)
    assert(status.offHeapUsedByRdd(2) === 1L)
    assert(status.rddStorageLevel(0) === Some(memAndDisk))
    assert(status.rddStorageLevel(1) === Some(memAndDisk))
    assert(status.rddStorageLevel(2) === Some(memAndDisk))

    // Verify default values for RDDs that don't exist
    assert(status.rddBlocksById(10).isEmpty)
    assert(status.memUsedByRdd(10) === 0L)
    assert(status.diskUsedByRdd(10) === 0L)
    assert(status.offHeapUsedByRdd(10) === 0L)
    assert(status.rddStorageLevel(10) === None)
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
    assert(status.memUsedByRdd(0) === 0L)
    assert(status.memUsedByRdd(1) === 100L)
    assert(status.memUsedByRdd(2) === 20L)
    assert(status.diskUsedByRdd(0) === 0L)
    assert(status.diskUsedByRdd(1) === 200L)
    assert(status.diskUsedByRdd(2) === 1060L)
    assert(status.offHeapUsedByRdd(0) === 0L)
    assert(status.offHeapUsedByRdd(1) === 1L)
    assert(status.offHeapUsedByRdd(2) === 0L)
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
    assert(status.memUsedByRdd(0) === 10L)
    assert(status.memUsedByRdd(1) === 0L)
    assert(status.memUsedByRdd(2) === 10L)
    assert(status.diskUsedByRdd(0) === 20L)
    assert(status.diskUsedByRdd(1) === 0L)
    assert(status.diskUsedByRdd(2) === 20L)
    assert(status.offHeapUsedByRdd(0) === 1L)
    assert(status.offHeapUsedByRdd(1) === 0L)
    assert(status.offHeapUsedByRdd(2) === 0L)
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

  test("storage status num[Rdd]Blocks") {
    val status = storageStatus2
    assert(status.blocks.size === status.numBlocks)
    assert(status.rddBlocks.size === status.numRddBlocks)
    status.addBlock(TestBlockId("Foo"), BlockStatus(memAndDisk, 0L, 0L, 100L))
    status.addBlock(RDDBlockId(4, 4), BlockStatus(memAndDisk, 0L, 0L, 100L))
    status.addBlock(RDDBlockId(4, 8), BlockStatus(memAndDisk, 0L, 0L, 100L))
    assert(status.blocks.size === status.numBlocks)
    assert(status.rddBlocks.size === status.numRddBlocks)
    assert(status.rddBlocksById(4).size === status.numRddBlocksById(4))
    assert(status.rddBlocksById(10).size === status.numRddBlocksById(10))
    status.updateBlock(TestBlockId("Foo"), BlockStatus(memAndDisk, 0L, 10L, 400L))
    status.updateBlock(RDDBlockId(4, 0), BlockStatus(memAndDisk, 0L, 0L, 100L))
    status.updateBlock(RDDBlockId(4, 8), BlockStatus(memAndDisk, 0L, 0L, 100L))
    status.updateBlock(RDDBlockId(10, 10), BlockStatus(memAndDisk, 0L, 0L, 100L))
    assert(status.blocks.size === status.numBlocks)
    assert(status.rddBlocks.size === status.numRddBlocks)
    assert(status.rddBlocksById(4).size === status.numRddBlocksById(4))
    assert(status.rddBlocksById(10).size === status.numRddBlocksById(10))
    assert(status.rddBlocksById(100).size === status.numRddBlocksById(100))
    status.removeBlock(RDDBlockId(4, 0))
    status.removeBlock(RDDBlockId(10, 10))
    assert(status.blocks.size === status.numBlocks)
    assert(status.rddBlocks.size === status.numRddBlocks)
    assert(status.rddBlocksById(4).size === status.numRddBlocksById(4))
    assert(status.rddBlocksById(10).size === status.numRddBlocksById(10))
    // remove a block that doesn't exist
    status.removeBlock(RDDBlockId(1000, 999))
    assert(status.blocks.size === status.numBlocks)
    assert(status.rddBlocks.size === status.numRddBlocks)
    assert(status.rddBlocksById(4).size === status.numRddBlocksById(4))
    assert(status.rddBlocksById(10).size === status.numRddBlocksById(10))
    assert(status.rddBlocksById(1000).size === status.numRddBlocksById(1000))
  }

  test("storage status memUsed, diskUsed, externalBlockStoreUsed") {
    val status = storageStatus2
    def actualMemUsed: Long = status.blocks.values.map(_.memSize).sum
    def actualDiskUsed: Long = status.blocks.values.map(_.diskSize).sum
    def actualOffHeapUsed: Long = status.blocks.values.map(_.externalBlockStoreSize).sum
    assert(status.memUsed === actualMemUsed)
    assert(status.diskUsed === actualDiskUsed)
    assert(status.offHeapUsed === actualOffHeapUsed)
    status.addBlock(TestBlockId("fire"), BlockStatus(memAndDisk, 4000L, 5000L, 6000L))
    status.addBlock(TestBlockId("wire"), BlockStatus(memAndDisk, 400L, 500L, 600L))
    status.addBlock(RDDBlockId(25, 25), BlockStatus(memAndDisk, 40L, 50L, 60L))
    assert(status.memUsed === actualMemUsed)
    assert(status.diskUsed === actualDiskUsed)
    assert(status.offHeapUsed === actualOffHeapUsed)
    status.updateBlock(TestBlockId("dan"), BlockStatus(memAndDisk, 4L, 5L, 6L))
    status.updateBlock(RDDBlockId(0, 0), BlockStatus(memAndDisk, 4L, 5L, 6L))
    status.updateBlock(RDDBlockId(1, 1), BlockStatus(memAndDisk, 4L, 5L, 6L))
    assert(status.memUsed === actualMemUsed)
    assert(status.diskUsed === actualDiskUsed)
    assert(status.offHeapUsed === actualOffHeapUsed)
    status.removeBlock(TestBlockId("fire"))
    status.removeBlock(TestBlockId("man"))
    status.removeBlock(RDDBlockId(2, 2))
    status.removeBlock(RDDBlockId(2, 3))
    assert(status.memUsed === actualMemUsed)
    assert(status.diskUsed === actualDiskUsed)
    assert(status.offHeapUsed === actualOffHeapUsed)
  }

  // For testing StorageUtils.updateRddInfo and StorageUtils.getRddBlockLocations
  private def stockStorageStatuses: Seq[StorageStatus] = {
    val status1 = new StorageStatus(BlockManagerId("big", "dog", 1), 1000L)
    val status2 = new StorageStatus(BlockManagerId("fat", "duck", 2), 2000L)
    val status3 = new StorageStatus(BlockManagerId("fat", "cat", 3), 3000L)
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
    val info0 = new RDDInfo(0, "0", 10, memAndDisk, Seq(3))
    val info1 = new RDDInfo(1, "1", 3, memAndDisk, Seq(4))
    Seq(info0, info1)
  }

  test("StorageUtils.updateRddInfo") {
    val storageStatuses = stockStorageStatuses
    val rddInfos = stockRDDInfos
    StorageUtils.updateRddInfo(rddInfos, storageStatuses)
    assert(rddInfos(0).storageLevel === memAndDisk)
    assert(rddInfos(0).numCachedPartitions === 5)
    assert(rddInfos(0).memSize === 5L)
    assert(rddInfos(0).diskSize === 10L)
    assert(rddInfos(0).externalBlockStoreSize === 0L)
    assert(rddInfos(1).storageLevel === memAndDisk)
    assert(rddInfos(1).numCachedPartitions === 3)
    assert(rddInfos(1).memSize === 3L)
    assert(rddInfos(1).diskSize === 6L)
    assert(rddInfos(1).externalBlockStoreSize === 0L)
  }

  test("StorageUtils.getRddBlockLocations") {
    val storageStatuses = stockStorageStatuses
    val blockLocations0 = StorageUtils.getRddBlockLocations(0, storageStatuses)
    val blockLocations1 = StorageUtils.getRddBlockLocations(1, storageStatuses)
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
    val blockLocations0 = StorageUtils.getRddBlockLocations(0, storageStatuses)
    val blockLocations1 = StorageUtils.getRddBlockLocations(1, storageStatuses)
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
