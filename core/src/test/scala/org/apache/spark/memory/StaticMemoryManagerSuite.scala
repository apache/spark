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

import org.mockito.Mockito.when

import org.apache.spark.SparkConf
import org.apache.spark.storage.{BlockId, BlockStatus, MemoryStore, TestBlockId}


class StaticMemoryManagerSuite extends MemoryManagerSuite {
  private val conf = new SparkConf().set("spark.storage.unrollFraction", "0.4")
  private val evictedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]

  /**
   * Make a [[StaticMemoryManager]] and a [[MemoryStore]] with limited class dependencies.
   */
  private def makeThings(
      maxExecutionMem: Long,
      maxStorageMem: Long): (StaticMemoryManager, MemoryStore) = {
    val mm = new StaticMemoryManager(
      conf, maxExecutionMemory = maxExecutionMem, maxStorageMemory = maxStorageMem)
    val ms = makeMemoryStore(mm)
    (mm, ms)
  }

  test("basic execution memory") {
    val maxExecutionMem = 1000L
    val (mm, _) = makeThings(maxExecutionMem, Long.MaxValue)
    assert(mm.executionMemoryUsed === 0L)
    assert(mm.acquireExecutionMemory(10L, evictedBlocks) === 10L)
    assert(mm.executionMemoryUsed === 10L)
    assert(mm.acquireExecutionMemory(100L, evictedBlocks) === 100L)
    // Acquire up to the max
    assert(mm.acquireExecutionMemory(1000L, evictedBlocks) === 890L)
    assert(mm.executionMemoryUsed === maxExecutionMem)
    assert(mm.acquireExecutionMemory(1L, evictedBlocks) === 0L)
    assert(mm.executionMemoryUsed === maxExecutionMem)
    mm.releaseExecutionMemory(800L)
    assert(mm.executionMemoryUsed === 200L)
    // Acquire after release
    assert(mm.acquireExecutionMemory(1L, evictedBlocks) === 1L)
    assert(mm.executionMemoryUsed === 201L)
    // Release beyond what was acquired
    mm.releaseExecutionMemory(maxExecutionMem)
    assert(mm.executionMemoryUsed === 0L)
  }

  test("basic storage memory") {
    val maxStorageMem = 1000L
    val dummyBlock = TestBlockId("you can see the world you brought to live")
    val (mm, ms) = makeThings(Long.MaxValue, maxStorageMem)
    assert(mm.storageMemoryUsed === 0L)
    assert(mm.acquireStorageMemory(dummyBlock, 10L, evictedBlocks))
    // `ensureFreeSpace` should be called with the number of bytes requested
    assertEnsureFreeSpaceCalled(ms, 10L)
    assert(mm.storageMemoryUsed === 10L)
    assert(mm.acquireStorageMemory(dummyBlock, 100L, evictedBlocks))
    assertEnsureFreeSpaceCalled(ms, 100L)
    assert(mm.storageMemoryUsed === 110L)
    // Acquire more than the max, not granted
    assert(!mm.acquireStorageMemory(dummyBlock, maxStorageMem + 1L, evictedBlocks))
    assertEnsureFreeSpaceCalled(ms, maxStorageMem + 1L)
    assert(mm.storageMemoryUsed === 110L)
    // Acquire up to the max, requests after this are still granted due to LRU eviction
    assert(mm.acquireStorageMemory(dummyBlock, maxStorageMem, evictedBlocks))
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

  test("execution and storage isolation") {
    val maxExecutionMem = 200L
    val maxStorageMem = 1000L
    val dummyBlock = TestBlockId("ain't nobody love like you do")
    val (mm, ms) = makeThings(maxExecutionMem, maxStorageMem)
    // Only execution memory should increase
    assert(mm.acquireExecutionMemory(100L, evictedBlocks) === 100L)
    assert(mm.storageMemoryUsed === 0L)
    assert(mm.executionMemoryUsed === 100L)
    assert(mm.acquireExecutionMemory(1000L, evictedBlocks) === 100L)
    assert(mm.storageMemoryUsed === 0L)
    assert(mm.executionMemoryUsed === 200L)
    // Only storage memory should increase
    assert(mm.acquireStorageMemory(dummyBlock, 50L, evictedBlocks))
    assertEnsureFreeSpaceCalled(ms, 50L)
    assert(mm.storageMemoryUsed === 50L)
    assert(mm.executionMemoryUsed === 200L)
    // Only execution memory should be released
    mm.releaseExecutionMemory(133L)
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
    assertEnsureFreeSpaceCalled(ms, 100L)
    assert(mm.storageMemoryUsed === 100L)
    mm.releaseUnrollMemory(40L)
    assert(mm.storageMemoryUsed === 60L)
    when(ms.currentUnrollMemory).thenReturn(60L)
    assert(mm.acquireUnrollMemory(dummyBlock, 500L, evictedBlocks))
    // `spark.storage.unrollFraction` is 0.4, so the max unroll space is 400 bytes.
    // Since we already occupy 60 bytes, we will try to ensure only 400 - 60 = 340 bytes.
    assertEnsureFreeSpaceCalled(ms, 340L)
    assert(mm.storageMemoryUsed === 560L)
    when(ms.currentUnrollMemory).thenReturn(560L)
    assert(!mm.acquireUnrollMemory(dummyBlock, 800L, evictedBlocks))
    assert(mm.storageMemoryUsed === 560L)
    // We already have 560 bytes > the max unroll space of 400 bytes, so no bytes are freed
    assertEnsureFreeSpaceCalled(ms, 0L)
    // Release beyond what was acquired
    mm.releaseUnrollMemory(maxStorageMem)
    assert(mm.storageMemoryUsed === 0L)
  }

}
