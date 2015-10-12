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

package org.apache.spark.memory

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.storage.{BlockId, BlockStatus, MemoryStore, TestBlockId}


class UnifiedMemoryManagerSuite extends MemoryManagerSuite {
  private val storageFraction = 0.5
  private val conf = new SparkConf().set("spark.memory.storageFraction", storageFraction.toString)
  private val dummyBlock = TestBlockId("shining shimmering splendid")
  private val evictedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]

  /**
   * Make a [[UnifiedMemoryManager]] and a [[MemoryStore]] with limited class dependencies.
   */
  private def makeThings(maxMemory: Long): (UnifiedMemoryManager, MemoryStore) = {
    val mm = new UnifiedMemoryManager(conf, maxMemory)
    val ms = makeMemoryStore(mm)
    (mm, ms)
  }

  test("basic execution memory") {
    val maxMemory = 1000L
    val (mm, _) = makeThings(maxMemory)
    assert(mm.executionMemoryUsed === 0L)
    assert(mm.acquireExecutionMemory(10L, evictedBlocks) === 10L)
    assert(mm.executionMemoryUsed === 10L)
    assert(mm.acquireExecutionMemory(100L, evictedBlocks) === 100L)
    // Acquire up to the max
    assert(mm.acquireExecutionMemory(1000L, evictedBlocks) === 890L)
    assert(mm.executionMemoryUsed === maxMemory)
    assert(mm.acquireExecutionMemory(1L, evictedBlocks) === 0L)
    assert(mm.executionMemoryUsed === maxMemory)
    mm.releaseExecutionMemory(800L)
    assert(mm.executionMemoryUsed === 200L)
    // Acquire after release
    assert(mm.acquireExecutionMemory(1L, evictedBlocks) === 1L)
    assert(mm.executionMemoryUsed === 201L)
    // Release beyond what was acquired
    mm.releaseExecutionMemory(maxMemory)
    assert(mm.executionMemoryUsed === 0L)
  }

  test("basic storage memory") {
    val maxMemory = 1000L
    val (mm, ms) = makeThings(maxMemory)
    assert(mm.storageMemoryUsed === 0L)
    assert(mm.acquireStorageMemory(dummyBlock, 10L, evictedBlocks))
    // `ensureFreeSpace` should be called with the number of bytes requested
    assertEnsureFreeSpaceCalled(ms, 10L)
    assert(mm.storageMemoryUsed === 10L)
    assert(mm.acquireStorageMemory(dummyBlock, 100L, evictedBlocks))
    assertEnsureFreeSpaceCalled(ms, 100L)
    assert(mm.storageMemoryUsed === 110L)
    // Acquire more than the max, not granted
    assert(!mm.acquireStorageMemory(dummyBlock, maxMemory + 1L, evictedBlocks))
    assertEnsureFreeSpaceCalled(ms, maxMemory + 1L)
    assert(mm.storageMemoryUsed === 110L)
    // Acquire up to the max, requests after this are still granted due to LRU eviction
    assert(mm.acquireStorageMemory(dummyBlock, maxMemory, evictedBlocks))
    assertEnsureFreeSpaceCalled(ms, 1000L)
    assert(mm.storageMemoryUsed === 1000L)
    assert(mm.acquireStorageMemory(dummyBlock, 1L, evictedBlocks))
    assertEnsureFreeSpaceCalled(ms, 1L)
    assert(mm.storageMemoryUsed === 1000L)
    mm.releaseStorageMemory(800L)
    assert(mm.storageMemoryUsed === 200L)
    // Acquire after release
    assert(mm.acquireStorageMemory(dummyBlock, 1L, evictedBlocks))
    assertEnsureFreeSpaceCalled(ms, 1L)
    assert(mm.storageMemoryUsed === 201L)
    mm.releaseStorageMemory()
    assert(mm.storageMemoryUsed === 0L)
    assert(mm.acquireStorageMemory(dummyBlock, 1L, evictedBlocks))
    assertEnsureFreeSpaceCalled(ms, 1L)
    assert(mm.storageMemoryUsed === 1L)
    // Release beyond what was acquired
    mm.releaseStorageMemory(100L)
    assert(mm.storageMemoryUsed === 0L)
  }

  test("execution evicts storage") {
    val maxMemory = 1000L
    val (mm, ms) = makeThings(maxMemory)
    assert(mm.acquireStorageMemory(dummyBlock, 750L, evictedBlocks))
    assertEnsureFreeSpaceCalled(ms, 750L)
    assert(mm.executionMemoryUsed === 0L)
    assert(mm.storageMemoryUsed === 750L)
    // Needed 250 bytes to evict storage memory, but acquire only 100
    assert(mm.acquireExecutionMemory(100L, evictedBlocks) === 100L)
    assert(mm.executionMemoryUsed === 100L)
    assert(mm.storageMemoryUsed === 750L)
    assertEnsureFreeSpaceNotCalled(ms)
    // Execution wants 200 bytes but there are only 150 left, so it evicts storage
    assert(mm.acquireExecutionMemory(200L, evictedBlocks) === 200L)
    assertEnsureFreeSpaceCalled(ms, 200L)
    assert(mm.executionMemoryUsed === 300L)
    mm.releaseStorageMemory()
    // Acquire some storage memory again, but keep it below the storage fraction
    val storageRegionSize = maxMemory * storageFraction
    val executionRegionSize = maxMemory - storageRegionSize
    require(storageRegionSize === 500L, "bad test: storage region should be 500 bytes")
    require(executionRegionSize === 500L, "bad test: execution region should be 500 bytes")
    require(executionRegionSize > mm.executionMemoryUsed,
      s"bad test: execution memory used should be within $executionRegionSize bytes")
    require(mm.storageMemoryUsed === 0, "bad test: storage memory used should be 0")
    assert(mm.acquireStorageMemory(dummyBlock, 400L, evictedBlocks))
    assertEnsureFreeSpaceCalled(ms, 400L)
    require(storageRegionSize > mm.storageMemoryUsed,
      s"bad test: storage memory used should be within $storageRegionSize bytes")
    // Execution cannot evict storage because the latter is within the storage fraction
    // Execution already had 300 bytes and storage 400, so grant only 300
    assert(mm.acquireExecutionMemory(400L, evictedBlocks) === 300L)
    assert(mm.executionMemoryUsed === 600L)
    assert(mm.storageMemoryUsed === 400L)
    assertEnsureFreeSpaceNotCalled(ms)
  }

  // TODO: test a few more cases

}