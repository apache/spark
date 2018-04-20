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
    val status = new StorageStatus(BlockManagerId("big", "dog", 1), 1000L, Some(1000L), Some(0L))
    assert(status.blocks.isEmpty)
    assert(status.rddBlocks.isEmpty)
    assert(status.memUsed === 0L)
    assert(status.memRemaining === 1000L)
    assert(status.diskUsed === 0L)
    status.addBlock(TestBlockId("foo"), BlockStatus(memAndDisk, 10L, 20L))
    status.addBlock(TestBlockId("fee"), BlockStatus(memAndDisk, 10L, 20L))
    status.addBlock(TestBlockId("faa"), BlockStatus(memAndDisk, 10L, 20L))
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

  // For testing add, update, remove, get, and contains etc. for both RDD and non-RDD blocks
  private def storageStatus2: StorageStatus = {
    val status = new StorageStatus(BlockManagerId("big", "dog", 1), 1000L, Some(1000L), Some(0L))
    assert(status.rddBlocks.isEmpty)
    status.addBlock(TestBlockId("dan"), BlockStatus(memAndDisk, 10L, 20L))
    status.addBlock(TestBlockId("man"), BlockStatus(memAndDisk, 10L, 20L))
    status.addBlock(RDDBlockId(0, 0), BlockStatus(memAndDisk, 10L, 20L))
    status.addBlock(RDDBlockId(1, 1), BlockStatus(memAndDisk, 100L, 200L))
    status.addBlock(RDDBlockId(2, 2), BlockStatus(memAndDisk, 10L, 20L))
    status.addBlock(RDDBlockId(2, 3), BlockStatus(memAndDisk, 10L, 20L))
    status.addBlock(RDDBlockId(2, 4), BlockStatus(memAndDisk, 10L, 40L))
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


  test("storage status memUsed, diskUsed, externalBlockStoreUsed") {
    val status = storageStatus2
    def actualMemUsed: Long = status.blocks.values.map(_.memSize).sum
    def actualDiskUsed: Long = status.blocks.values.map(_.diskSize).sum
    assert(status.memUsed === actualMemUsed)
    assert(status.diskUsed === actualDiskUsed)
    status.addBlock(TestBlockId("fire"), BlockStatus(memAndDisk, 4000L, 5000L))
    status.addBlock(TestBlockId("wire"), BlockStatus(memAndDisk, 400L, 500L))
    status.addBlock(RDDBlockId(25, 25), BlockStatus(memAndDisk, 40L, 50L))
    assert(status.memUsed === actualMemUsed)
    assert(status.diskUsed === actualDiskUsed)
  }

  // For testing StorageUtils.updateRddInfo and StorageUtils.getRddBlockLocations
  private def stockStorageStatuses: Seq[StorageStatus] = {
    val status1 = new StorageStatus(BlockManagerId("big", "dog", 1), 1000L, Some(1000L), Some(0L))
    val status2 = new StorageStatus(BlockManagerId("fat", "duck", 2), 2000L, Some(2000L), Some(0L))
    val status3 = new StorageStatus(BlockManagerId("fat", "cat", 3), 3000L, Some(3000L), Some(0L))
    status1.addBlock(RDDBlockId(0, 0), BlockStatus(memAndDisk, 1L, 2L))
    status1.addBlock(RDDBlockId(0, 1), BlockStatus(memAndDisk, 1L, 2L))
    status2.addBlock(RDDBlockId(0, 2), BlockStatus(memAndDisk, 1L, 2L))
    status2.addBlock(RDDBlockId(0, 3), BlockStatus(memAndDisk, 1L, 2L))
    status2.addBlock(RDDBlockId(1, 0), BlockStatus(memAndDisk, 1L, 2L))
    status2.addBlock(RDDBlockId(1, 1), BlockStatus(memAndDisk, 1L, 2L))
    status3.addBlock(RDDBlockId(0, 4), BlockStatus(memAndDisk, 1L, 2L))
    status3.addBlock(RDDBlockId(1, 2), BlockStatus(memAndDisk, 1L, 2L))
    Seq(status1, status2, status3)
  }

  // For testing StorageUtils.updateRddInfo
  private def stockRDDInfos: Seq[RDDInfo] = {
    val info0 = new RDDInfo(0, "0", 10, memAndDisk, Seq(3))
    val info1 = new RDDInfo(1, "1", 3, memAndDisk, Seq(4))
    Seq(info0, info1)
  }

  private val offheap = StorageLevel.OFF_HEAP
  // For testing add, update, remove, get, and contains etc. for both RDD and non-RDD onheap
  // and offheap blocks
  private def storageStatus3: StorageStatus = {
    val status = new StorageStatus(BlockManagerId("big", "dog", 1), 2000L, Some(1000L), Some(1000L))
    assert(status.rddBlocks.isEmpty)
    status.addBlock(TestBlockId("dan"), BlockStatus(memAndDisk, 10L, 20L))
    status.addBlock(TestBlockId("man"), BlockStatus(offheap, 10L, 0L))
    status.addBlock(RDDBlockId(0, 0), BlockStatus(offheap, 10L, 0L))
    status.addBlock(RDDBlockId(1, 1), BlockStatus(offheap, 100L, 0L))
    status.addBlock(RDDBlockId(2, 2), BlockStatus(memAndDisk, 10L, 20L))
    status.addBlock(RDDBlockId(2, 3), BlockStatus(memAndDisk, 10L, 20L))
    status.addBlock(RDDBlockId(2, 4), BlockStatus(memAndDisk, 10L, 40L))
    status
  }

  test("storage memUsed, diskUsed with on-heap and off-heap blocks") {
    val status = storageStatus3
    def actualMemUsed: Long = status.blocks.values.map(_.memSize).sum
    def actualDiskUsed: Long = status.blocks.values.map(_.diskSize).sum

    def actualOnHeapMemUsed: Long =
      status.blocks.values.filter(!_.storageLevel.useOffHeap).map(_.memSize).sum
    def actualOffHeapMemUsed: Long =
      status.blocks.values.filter(_.storageLevel.useOffHeap).map(_.memSize).sum

    assert(status.maxMem === status.maxOnHeapMem.get + status.maxOffHeapMem.get)

    assert(status.memUsed === actualMemUsed)
    assert(status.diskUsed === actualDiskUsed)
    assert(status.onHeapMemUsed.get === actualOnHeapMemUsed)
    assert(status.offHeapMemUsed.get === actualOffHeapMemUsed)

    assert(status.memRemaining === status.maxMem - actualMemUsed)
    assert(status.onHeapMemRemaining.get === status.maxOnHeapMem.get - actualOnHeapMemUsed)
    assert(status.offHeapMemRemaining.get === status.maxOffHeapMem.get - actualOffHeapMemUsed)

    status.addBlock(TestBlockId("wire"), BlockStatus(memAndDisk, 400L, 500L))
    status.addBlock(RDDBlockId(25, 25), BlockStatus(memAndDisk, 40L, 50L))
    assert(status.memUsed === actualMemUsed)
    assert(status.diskUsed === actualDiskUsed)
  }

  private def storageStatus4: StorageStatus = {
    val status = new StorageStatus(BlockManagerId("big", "dog", 1), 2000L, None, None)
    status
  }
  test("old SparkListenerBlockManagerAdded event compatible") {
    // This scenario will only be happened when replaying old event log. In this scenario there's
    // no block add or remove event replayed, so only total amount of memory is valid.
    val status = storageStatus4
    assert(status.maxMem === status.maxMemory)

    assert(status.memUsed === 0L)
    assert(status.diskUsed === 0L)
    assert(status.onHeapMemUsed === None)
    assert(status.offHeapMemUsed === None)

    assert(status.memRemaining === status.maxMem)
    assert(status.onHeapMemRemaining === None)
    assert(status.offHeapMemRemaining === None)
  }
}
