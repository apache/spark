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

import org.mockito.Mockito.when

import org.apache.spark.SparkConf
import org.apache.spark.storage.{MemoryStore, TestBlockId}

class StaticMemoryManagerSuite extends MemoryManagerSuite {
  private val conf = new SparkConf().set("spark.storage.unrollFraction", "0.4")

  /**
   * Make a [[StaticMemoryManager]] and a [[MemoryStore]] with limited class dependencies.
   */
  private def makeThings(
      maxExecutionMem: Long,
      maxStorageMem: Long): (StaticMemoryManager, MemoryStore) = {
    val mm = new StaticMemoryManager(
      conf,
      maxOnHeapExecutionMemory = maxExecutionMem,
      maxStorageMemory = maxStorageMem,
      numCores = 1)
    val ms = makeMemoryStore(mm)
    (mm, ms)
  }

  override protected def createMemoryManager(
      maxOnHeapExecutionMemory: Long,
      maxOffHeapExecutionMemory: Long): StaticMemoryManager = {
    new StaticMemoryManager(
      conf.clone
        .set("spark.memory.fraction", "1")
        .set("spark.testing.memory", maxOnHeapExecutionMemory.toString)
        .set("spark.memory.offHeap.size", maxOffHeapExecutionMemory.toString),
      maxOnHeapExecutionMemory = maxOnHeapExecutionMemory,
      maxStorageMemory = 0,
      numCores = 1)
  }

  test("basic execution memory") {
    val maxExecutionMem = 1000L
    val taskAttemptId = 0L
    val (mm, _) = makeThings(maxExecutionMem, Long.MaxValue)
    assert(mm.executionMemoryUsed === 0L)
    assert(mm.acquireExecutionMemory(10L, taskAttemptId, MemoryMode.ON_HEAP) === 10L)
    assert(mm.executionMemoryUsed === 10L)
    assert(mm.acquireExecutionMemory(100L, taskAttemptId, MemoryMode.ON_HEAP) === 100L)
    // Acquire up to the max
    assert(mm.acquireExecutionMemory(1000L, taskAttemptId, MemoryMode.ON_HEAP) === 890L)
    assert(mm.executionMemoryUsed === maxExecutionMem)
    assert(mm.acquireExecutionMemory(1L, taskAttemptId, MemoryMode.ON_HEAP) === 0L)
    assert(mm.executionMemoryUsed === maxExecutionMem)
    mm.releaseExecutionMemory(800L, taskAttemptId, MemoryMode.ON_HEAP)
    assert(mm.executionMemoryUsed === 200L)
    // Acquire after release
    assert(mm.acquireExecutionMemory(1L, taskAttemptId, MemoryMode.ON_HEAP) === 1L)
    assert(mm.executionMemoryUsed === 201L)
    // Release beyond what was acquired
    mm.releaseExecutionMemory(maxExecutionMem, taskAttemptId, MemoryMode.ON_HEAP)
    assert(mm.executionMemoryUsed === 0L)
  }

  test("basic storage memory") {
    val maxStorageMem = 1000L
    val dummyBlock = TestBlockId("you can see the world you brought to live")
    val (mm, ms) = makeThings(Long.MaxValue, maxStorageMem)
    assert(mm.storageMemoryUsed === 0L)
    assert(mm.acquireStorageMemory(dummyBlock, 10L, evictedBlocks))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.storageMemoryUsed === 10L)

    assert(mm.acquireStorageMemory(dummyBlock, 100L, evictedBlocks))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.storageMemoryUsed === 110L)
    // Acquire more than the max, not granted
    assert(!mm.acquireStorageMemory(dummyBlock, maxStorageMem + 1L, evictedBlocks))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.storageMemoryUsed === 110L)
    // Acquire up to the max, requests after this are still granted due to LRU eviction
    assert(mm.acquireStorageMemory(dummyBlock, maxStorageMem, evictedBlocks))
    assertEvictBlocksToFreeSpaceCalled(ms, 110L)
    assert(mm.storageMemoryUsed === 1000L)
    assert(mm.acquireStorageMemory(dummyBlock, 1L, evictedBlocks))
    assertEvictBlocksToFreeSpaceCalled(ms, 1L)
    assert(evictedBlocks.nonEmpty)
    evictedBlocks.clear()
    // Note: We evicted 1 byte to put another 1-byte block in, so the storage memory used remains at
    // 1000 bytes. This is different from real behavior, where the 1-byte block would have evicted
    // the 1000-byte block entirely. This is set up differently so we can write finer-grained tests.
    assert(mm.storageMemoryUsed === 1000L)
    mm.releaseStorageMemory(800L)
    assert(mm.storageMemoryUsed === 200L)
    // Acquire after release
    assert(mm.acquireStorageMemory(dummyBlock, 1L, evictedBlocks))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.storageMemoryUsed === 201L)
    mm.releaseAllStorageMemory()
    assert(mm.storageMemoryUsed === 0L)
    assert(mm.acquireStorageMemory(dummyBlock, 1L, evictedBlocks))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.storageMemoryUsed === 1L)
    // Release beyond what was acquired
    mm.releaseStorageMemory(100L)
    assert(mm.storageMemoryUsed === 0L)
  }

  test("execution and storage isolation") {
    val maxExecutionMem = 200L
    val maxStorageMem = 1000L
    val taskAttemptId = 0L
    val dummyBlock = TestBlockId("ain't nobody love like you do")
    val (mm, ms) = makeThings(maxExecutionMem, maxStorageMem)
    // Only execution memory should increase
    assert(mm.acquireExecutionMemory(100L, taskAttemptId, MemoryMode.ON_HEAP) === 100L)
    assert(mm.storageMemoryUsed === 0L)
    assert(mm.executionMemoryUsed === 100L)
    assert(mm.acquireExecutionMemory(1000L, taskAttemptId, MemoryMode.ON_HEAP) === 100L)
    assert(mm.storageMemoryUsed === 0L)
    assert(mm.executionMemoryUsed === 200L)
    // Only storage memory should increase
    assert(mm.acquireStorageMemory(dummyBlock, 50L, evictedBlocks))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.storageMemoryUsed === 50L)
    assert(mm.executionMemoryUsed === 200L)
    // Only execution memory should be released
    mm.releaseExecutionMemory(133L, taskAttemptId, MemoryMode.ON_HEAP)
    assert(mm.storageMemoryUsed === 50L)
    assert(mm.executionMemoryUsed === 67L)
    // Only storage memory should be released
    mm.releaseAllStorageMemory()
    assert(mm.storageMemoryUsed === 0L)
    assert(mm.executionMemoryUsed === 67L)
  }

  test("unroll memory") {
    val maxStorageMem = 1000L
    val dummyBlock = TestBlockId("lonely water")
    val (mm, ms) = makeThings(Long.MaxValue, maxStorageMem)
    assert(mm.acquireUnrollMemory(dummyBlock, 100L, evictedBlocks))
    when(ms.currentUnrollMemory).thenReturn(100L)
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.storageMemoryUsed === 100L)
    mm.releaseUnrollMemory(40L)
    assert(mm.storageMemoryUsed === 60L)
    when(ms.currentUnrollMemory).thenReturn(60L)
    assert(mm.acquireStorageMemory(dummyBlock, 800L, evictedBlocks))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.storageMemoryUsed === 860L)
    // `spark.storage.unrollFraction` is 0.4, so the max unroll space is 400 bytes.
    // As of this point, cache memory is 800 bytes and current unroll memory is 60 bytes.
    // Requesting 240 more bytes of unroll memory will leave our total unroll memory at
    // 300 bytes, still under the 400-byte limit. Therefore, all 240 bytes are granted.
    assert(mm.acquireUnrollMemory(dummyBlock, 240L, evictedBlocks))
    assertEvictBlocksToFreeSpaceCalled(ms, 100L) // 860 + 240 - 1000
    when(ms.currentUnrollMemory).thenReturn(300L) // 60 + 240
    assert(mm.storageMemoryUsed === 1000L)
    evictedBlocks.clear()
    // We already have 300 bytes of unroll memory, so requesting 150 more will leave us
    // above the 400-byte limit. Since there is not enough free memory, this request will
    // fail even after evicting as much as we can (400 - 300 = 100 bytes).
    assert(!mm.acquireUnrollMemory(dummyBlock, 150L, evictedBlocks))
    assertEvictBlocksToFreeSpaceCalled(ms, 100L)
    assert(mm.storageMemoryUsed === 900L)
    // Release beyond what was acquired
    mm.releaseUnrollMemory(maxStorageMem)
    assert(mm.storageMemoryUsed === 0L)
  }

}
