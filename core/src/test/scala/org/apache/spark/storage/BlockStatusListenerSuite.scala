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
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerMessages.UpdateBlockInfo

class BlockStatusListenerSuite extends SparkFunSuite {

  test("basic functions") {
    val blockManagerId = BlockManagerId("0", "localhost", 10000)
    val listener = new BlockStatusListener()
    listener.onBlockManagerAdded(SparkListenerBlockManagerAdded(0, blockManagerId, 0))
    listener.onBlockUpdated(SparkListenerBlockUpdated(
      UpdateBlockInfo(
        blockManagerId,
        StreamBlockId(0, 100),
        StorageLevel.MEMORY_AND_DISK,
        memSize = 100,
        diskSize = 100,
        externalBlockStoreSize = 0)))
    val blocks = listener.allBlocks
    assert(blocks.size === 1)
    assert(blocks.head.blockId === StreamBlockId(0, 100))
    assert(blocks.head.storageLevel === StorageLevel.MEMORY_AND_DISK)
    assert(blocks.head.memSize === 100)
    assert(blocks.head.diskSize === 100)
    assert(blocks.head.externalBlockStoreSize === 0)
    assert(blocks.head.locations === Set("localhost:10000"))

    // Add a new block manager
    val blockManagerId2 = BlockManagerId("1", "localhost", 10001)
    listener.onBlockManagerAdded(SparkListenerBlockManagerAdded(0, blockManagerId2, 0))

    listener.onBlockUpdated(SparkListenerBlockUpdated(
      UpdateBlockInfo(
        blockManagerId2,
        StreamBlockId(0, 100),
        StorageLevel.MEMORY_AND_DISK,
        memSize = 100,
        diskSize = 100,
        externalBlockStoreSize = 0)))

    // Adding a new replication of the same block should increase the replication
    val blocks2 = listener.allBlocks
    assert(blocks2.size === 1)
    assert(blocks2.head.blockId === StreamBlockId(0, 100))
    assert(blocks2.head.storageLevel === StorageLevel.MEMORY_AND_DISK_2)
    assert(blocks2.head.memSize === 100)
    assert(blocks2.head.diskSize === 100)
    assert(blocks2.head.externalBlockStoreSize === 0)
    assert(blocks2.head.locations === Set("localhost:10000", "localhost:10001"))

    // Removing a replication of the same block should decrease the replication
    listener.onBlockUpdated(SparkListenerBlockUpdated(
      UpdateBlockInfo(
        blockManagerId2,
        StreamBlockId(0, 100),
        StorageLevel.NONE,
        memSize = 0,
        diskSize = 0,
        externalBlockStoreSize = 0)))
    val blocks3 = listener.allBlocks
    assert(blocks3.size === 1)
    assert(blocks3.head.blockId === StreamBlockId(0, 100))
    assert(blocks3.head.storageLevel === StorageLevel.MEMORY_AND_DISK)
    assert(blocks3.head.memSize === 100)
    assert(blocks3.head.diskSize === 100)
    assert(blocks3.head.externalBlockStoreSize === 0)
    assert(blocks3.head.locations === Set("localhost:10000"))

    // Remove a block manager
    listener.onBlockManagerRemoved(SparkListenerBlockManagerRemoved(0, blockManagerId2))
    listener.onBlockUpdated(SparkListenerBlockUpdated(
      UpdateBlockInfo(
        blockManagerId2,
        StreamBlockId(0, 100),
        StorageLevel.MEMORY_AND_DISK,
        memSize = 100,
        diskSize = 100,
        externalBlockStoreSize = 0)))

    // The block manager is removed so the new block changes nothing
    val blocks4 = listener.allBlocks
    assert(blocks4.size === 1)
    assert(blocks4.head.blockId === StreamBlockId(0, 100))
    assert(blocks4.head.storageLevel === StorageLevel.MEMORY_AND_DISK)
    assert(blocks4.head.memSize === 100)
    assert(blocks4.head.diskSize === 100)
    assert(blocks4.head.externalBlockStoreSize === 0)
    assert(blocks4.head.locations === Set("localhost:10000"))

    // Remove the last block manager
    listener.onBlockManagerRemoved(SparkListenerBlockManagerRemoved(0, blockManagerId))

    // No block manager now so we should dop all blocks
    assert(listener.allBlocks.isEmpty)
  }

}
