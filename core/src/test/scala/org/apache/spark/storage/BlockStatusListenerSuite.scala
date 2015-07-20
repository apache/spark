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
      "localhost:10000",
      StorageLevel.MEMORY_AND_DISK,
      memSize = 100,
      diskSize = 100,
      externalBlockStoreSize = 0
    )
    val expectedExecutorStreamBlockStatus = Seq(
      ExecutorStreamBlockStatus("0", "localhost:10000", Seq(expectedBlock))
    )
    assert(listener.allExecutorStreamBlockStatus === expectedExecutorStreamBlockStatus)

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
    val expectedBlock2 = BlockUIData(
      StreamBlockId(0, 100),
      "localhost:10001",
      StorageLevel.MEMORY_AND_DISK,
      memSize = 100,
      diskSize = 100,
      externalBlockStoreSize = 0
    )
    // Each block manager should contain one block
    val expectedExecutorStreamBlockStatus2 = Set(
      ExecutorStreamBlockStatus("0", "localhost:10000", Seq(expectedBlock)),
      ExecutorStreamBlockStatus("1", "localhost:10001", Seq(expectedBlock2))
    )
    assert(listener.allExecutorStreamBlockStatus.toSet === expectedExecutorStreamBlockStatus2)

    // Remove a replication of the same block
    listener.onBlockUpdated(SparkListenerBlockUpdated(
      BlockUpdatedInfo(
        blockManagerId2,
        StreamBlockId(0, 100),
        StorageLevel.NONE, // StorageLevel.NONE means removing it
        memSize = 0,
        diskSize = 0,
        externalBlockStoreSize = 0)))
    // Only the first block manager contains a block
    val expectedExecutorStreamBlockStatus3 = Set(
      ExecutorStreamBlockStatus("0", "localhost:10000", Seq(expectedBlock)),
      ExecutorStreamBlockStatus("1", "localhost:10001", Seq.empty)
    )
    assert(listener.allExecutorStreamBlockStatus.toSet === expectedExecutorStreamBlockStatus3)

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
    // The second block manager is removed so we should not see the new block
    val expectedExecutorStreamBlockStatus4 = Seq(
      ExecutorStreamBlockStatus("0", "localhost:10000", Seq(expectedBlock))
    )
    assert(listener.allExecutorStreamBlockStatus === expectedExecutorStreamBlockStatus4)

    // Remove the last block manager
    listener.onBlockManagerRemoved(SparkListenerBlockManagerRemoved(0, blockManagerId))
    // No block manager now so we should dop all block managers
    assert(listener.allExecutorStreamBlockStatus.isEmpty)
  }

}
