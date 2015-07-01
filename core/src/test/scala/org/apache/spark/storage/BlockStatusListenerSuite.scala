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

class BlockStatusListenerSuite extends SparkFunSuite {

  test("basic functions") {
    val blockManagerId = BlockManagerId("0", "localhost", 10000)
    val listener = new BlockStatusListener()

    // Add a block manager and a new block status
    listener.onBlockManagerAdded(SparkListenerBlockManagerAdded(0, blockManagerId, 0))
    listener.onBlockUpdated(SparkListenerBlockUpdated(
      BlockUpdatedInfo(
        blockManagerId,
        StreamBlockId(0, 100),
        StorageLevel.MEMORY_AND_DISK,
        memSize = 100,
        diskSize = 100,
        externalBlockStoreSize = 0)))
    // The new block status should be added to the listener
    val expectedBlock = BlockUIData(
      StreamBlockId(0, 100),
      StorageLevel.MEMORY_AND_DISK,
      memSize = 100,
      diskSize = 100,
      externalBlockStoreSize = 0,
      locations = Set("localhost:10000")
    )
    assert(listener.allBlocks === Seq(expectedBlock))

    // Add the second block manager
    val blockManagerId2 = BlockManagerId("1", "localhost", 10001)
    listener.onBlockManagerAdded(SparkListenerBlockManagerAdded(0, blockManagerId2, 0))
    // Add a new replication of the same block id from the second manager
    listener.onBlockUpdated(SparkListenerBlockUpdated(
      BlockUpdatedInfo(
        blockManagerId2,
        StreamBlockId(0, 100),
        StorageLevel.MEMORY_AND_DISK,
        memSize = 100,
        diskSize = 100,
        externalBlockStoreSize = 0)))
    // Adding a new replication of the same block id should increase the replication
    val expectedBlock2 = BlockUIData(
      StreamBlockId(0, 100),
      StorageLevel.MEMORY_AND_DISK_2, // Should increase the replication
      memSize = 100,
      diskSize = 100,
      externalBlockStoreSize = 0,
      locations = Set("localhost:10000", "localhost:10001") // Should contain two block managers
    )
    assert(listener.allBlocks === Seq(expectedBlock2))

    // Remove a replication of the same block
    listener.onBlockUpdated(SparkListenerBlockUpdated(
      BlockUpdatedInfo(
        blockManagerId2,
        StreamBlockId(0, 100),
        StorageLevel.NONE, // StorageLevel.NONE means removing it
        memSize = 0,
        diskSize = 0,
        externalBlockStoreSize = 0)))
    // Removing a replication of the same block should decrease the replication
    val expectedBlock3 = BlockUIData(
      StreamBlockId(0, 100),
      StorageLevel.MEMORY_AND_DISK, // Should decrease the replication
      memSize = 100,
      diskSize = 100,
      externalBlockStoreSize = 0,
      locations = Set("localhost:10000") // Should contain only the first block manager
    )
    assert(listener.allBlocks === Seq(expectedBlock3))

    // Remove the second block manager at first but add a new block status
    // from this removed block manager
    listener.onBlockManagerRemoved(SparkListenerBlockManagerRemoved(0, blockManagerId2))
    listener.onBlockUpdated(SparkListenerBlockUpdated(
      BlockUpdatedInfo(
        blockManagerId2,
        StreamBlockId(0, 100),
        StorageLevel.MEMORY_AND_DISK,
        memSize = 100,
        diskSize = 100,
        externalBlockStoreSize = 0)))
    // The second block manager is removed so the new block status from this block manager
    // should not change anything
    assert(listener.allBlocks === Seq(expectedBlock3))

    // Remove the last block manager
    listener.onBlockManagerRemoved(SparkListenerBlockManagerRemoved(0, blockManagerId))
    // No block manager now so we should dop all blocks
    assert(listener.allBlocks.isEmpty)
  }

}
