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

import org.scalatest.PrivateMethodTester

import org.apache.spark.SparkConf
import org.apache.spark.storage.{BlockId, BlockStatus, MemoryStore, TestBlockId}


class UnifiedMemoryManagerSuite extends MemoryManagerSuite with PrivateMethodTester {
  private val conf = new SparkConf().set("spark.memory.storageFraction", "0.5")
  private val dummyBlock = TestBlockId("--")
  private val evictedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]

  /**
   * Make a [[UnifiedMemoryManager]] and a [[MemoryStore]] with limited class dependencies.
   */
  private def makeThings(maxMemory: Long): (UnifiedMemoryManager, MemoryStore) = {
    val mm = new UnifiedMemoryManager(conf, maxMemory)
    val ms = makeMemoryStore(mm)
    (mm, ms)
  }

  private def getStorageRegionSize(mm: UnifiedMemoryManager): Long = {
    mm invokePrivate PrivateMethod[Long]('storageRegionSize)()
  }

  test("storage region size") {
    val maxMemory = 1000L
    val (mm, _) = makeThings(maxMemory)
    val storageFraction = conf.get("spark.memory.storageFraction").toDouble
    val expectedStorageRegionSize = maxMemory * storageFraction
    val actualStorageRegionSize = getStorageRegionSize(mm)
    assert(expectedStorageRegionSize === actualStorageRegionSize)
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
    mm.releaseAllStorageMemory()
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
    // First, ensure the test classes are set up as expected
    val expectedStorageRegionSize = 500L
    val expectedExecutionRegionSize = 500L
    val storageRegionSize = getStorageRegionSize(mm)
    val executionRegionSize = maxMemory - expectedStorageRegionSize
    require(storageRegionSize === expectedStorageRegionSize,
      "bad test: storage region size is unexpected")
    require(executionRegionSize === expectedExecutionRegionSize,
      "bad test: storage region size is unexpected")
    // Acquire enough storage memory to exceed the storage region
    assert(mm.acquireStorageMemory(dummyBlock, 750L, evictedBlocks))
    assertEnsureFreeSpaceCalled(ms, 750L)
    assert(mm.executionMemoryUsed === 0L)
    assert(mm.storageMemoryUsed === 750L)
    require(mm.storageMemoryUsed > storageRegionSize,
      s"bad test: storage memory used should exceed the storage region")
    // Execution needs to request 250 bytes to evict storage memory
    assert(mm.acquireExecutionMemory(100L, evictedBlocks) === 100L)
    assert(mm.executionMemoryUsed === 100L)
    assert(mm.storageMemoryUsed === 750L)
    assertEnsureFreeSpaceNotCalled(ms)
    // Execution wants 200 bytes but only 150 are free, so storage is evicted
    assert(mm.acquireExecutionMemory(200L, evictedBlocks) === 200L)
    assertEnsureFreeSpaceCalled(ms, 200L)
    assert(mm.executionMemoryUsed === 300L)
    mm.releaseAllStorageMemory()
    require(mm.executionMemoryUsed < executionRegionSize,
      s"bad test: execution memory used should be within the execution region")
    require(mm.storageMemoryUsed === 0, "bad test: all storage memory should have been released")
    // Acquire some storage memory again, but this time keep it within the storage region
    assert(mm.acquireStorageMemory(dummyBlock, 400L, evictedBlocks))
    assertEnsureFreeSpaceCalled(ms, 400L)
    require(mm.storageMemoryUsed < storageRegionSize,
      s"bad test: storage memory used should be within the storage region")
    // Execution cannot evict storage because the latter is within the storage fraction,
    // so grant only what's remaining without evicting anything, i.e. 1000 - 300 - 400 = 300
    assert(mm.acquireExecutionMemory(400L, evictedBlocks) === 300L)
    assert(mm.executionMemoryUsed === 600L)
    assert(mm.storageMemoryUsed === 400L)
    assertEnsureFreeSpaceNotCalled(ms)
  }

  test("storage does not evict execution") {
    val maxMemory = 1000L
    val (mm, ms) = makeThings(maxMemory)
    // First, ensure the test classes are set up as expected
    val expectedStorageRegionSize = 500L
    val expectedExecutionRegionSize = 500L
    val storageRegionSize = getStorageRegionSize(mm)
    val executionRegionSize = maxMemory - expectedStorageRegionSize
    require(storageRegionSize === expectedStorageRegionSize,
      "bad test: storage region size is unexpected")
    require(executionRegionSize === expectedExecutionRegionSize,
      "bad test: storage region size is unexpected")
    // Acquire enough execution memory to exceed the execution region
    assert(mm.acquireExecutionMemory(800L, evictedBlocks) === 800L)
    assert(mm.executionMemoryUsed === 800L)
    assert(mm.storageMemoryUsed === 0L)
    assertEnsureFreeSpaceNotCalled(ms)
    require(mm.executionMemoryUsed > executionRegionSize,
      s"bad test: execution memory used should exceed the execution region")
    // Storage should not be able to evict execution
    assert(mm.acquireStorageMemory(dummyBlock, 100L, evictedBlocks))
    assert(mm.executionMemoryUsed === 800L)
    assert(mm.storageMemoryUsed === 100L)
    assertEnsureFreeSpaceCalled(ms, 100L)
    assert(!mm.acquireStorageMemory(dummyBlock, 250L, evictedBlocks))
    assert(mm.executionMemoryUsed === 800L)
    assert(mm.storageMemoryUsed === 100L)
    assertEnsureFreeSpaceCalled(ms, 250L)
    mm.releaseExecutionMemory(maxMemory)
    mm.releaseStorageMemory(maxMemory)
    // Acquire some execution memory again, but this time keep it within the execution region
    assert(mm.acquireExecutionMemory(200L, evictedBlocks) === 200L)
    assert(mm.executionMemoryUsed === 200L)
    assert(mm.storageMemoryUsed === 0L)
    assertEnsureFreeSpaceNotCalled(ms)
    require(mm.executionMemoryUsed < executionRegionSize,
      s"bad test: execution memory used should be within the execution region")
    // Storage should still not be able to evict execution
    assert(mm.acquireStorageMemory(dummyBlock, 750L, evictedBlocks))
    assert(mm.executionMemoryUsed === 200L)
    assert(mm.storageMemoryUsed === 750L)
    assertEnsureFreeSpaceCalled(ms, 750L)
    assert(!mm.acquireStorageMemory(dummyBlock, 850L, evictedBlocks))
    assert(mm.executionMemoryUsed === 200L)
    assert(mm.storageMemoryUsed === 750L)
    assertEnsureFreeSpaceCalled(ms, 850L)
  }

}
