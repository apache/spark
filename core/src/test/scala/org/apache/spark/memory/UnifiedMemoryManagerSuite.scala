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

import org.scalatest.PrivateMethodTester

import org.apache.spark.SparkConf
import org.apache.spark.storage.{MemoryStore, TestBlockId}

class UnifiedMemoryManagerSuite extends MemoryManagerSuite with PrivateMethodTester {
  private val dummyBlock = TestBlockId("--")

  private val storageFraction: Double = 0.5

  /**
   * Make a [[UnifiedMemoryManager]] and a [[MemoryStore]] with limited class dependencies.
   */
  private def makeThings(maxMemory: Long): (UnifiedMemoryManager, MemoryStore) = {
    val mm = createMemoryManager(maxMemory)
    val ms = makeMemoryStore(mm)
    (mm, ms)
  }

  override protected def createMemoryManager(
      maxOnHeapExecutionMemory: Long,
      maxOffHeapExecutionMemory: Long): UnifiedMemoryManager = {
    val conf = new SparkConf()
      .set("spark.memory.fraction", "1")
      .set("spark.testing.memory", maxOnHeapExecutionMemory.toString)
      .set("spark.memory.offHeapSize", maxOffHeapExecutionMemory.toString)
      .set("spark.memory.storageFraction", storageFraction.toString)
    UnifiedMemoryManager(conf, numCores = 1)
  }

  test("basic execution memory") {
    val maxMemory = 1000L
    val taskAttemptId = 0L
    val (mm, _) = makeThings(maxMemory)
    assert(mm.executionMemoryUsed === 0L)
    assert(mm.acquireExecutionMemory(10L, taskAttemptId, MemoryMode.ON_HEAP) === 10L)
    assert(mm.executionMemoryUsed === 10L)
    assert(mm.acquireExecutionMemory(100L, taskAttemptId, MemoryMode.ON_HEAP) === 100L)
    // Acquire up to the max
    assert(mm.acquireExecutionMemory(1000L, taskAttemptId, MemoryMode.ON_HEAP) === 890L)
    assert(mm.executionMemoryUsed === maxMemory)
    assert(mm.acquireExecutionMemory(1L, taskAttemptId, MemoryMode.ON_HEAP) === 0L)
    assert(mm.executionMemoryUsed === maxMemory)
    mm.releaseExecutionMemory(800L, taskAttemptId, MemoryMode.ON_HEAP)
    assert(mm.executionMemoryUsed === 200L)
    // Acquire after release
    assert(mm.acquireExecutionMemory(1L, taskAttemptId, MemoryMode.ON_HEAP) === 1L)
    assert(mm.executionMemoryUsed === 201L)
    // Release beyond what was acquired
    mm.releaseExecutionMemory(maxMemory, taskAttemptId, MemoryMode.ON_HEAP)
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
    assert(evictedBlocks.isEmpty)
    // Acquire more than the max, not granted
    assert(!mm.acquireStorageMemory(dummyBlock, maxMemory + 1L, evictedBlocks))
    assertEnsureFreeSpaceCalled(ms, maxMemory + 1L)
    assert(mm.storageMemoryUsed === 110L)
    assert(evictedBlocks.isEmpty)
    // Acquire up to the max, requests after this are still granted due to LRU eviction
    assert(mm.acquireStorageMemory(dummyBlock, maxMemory, evictedBlocks))
    assertEnsureFreeSpaceCalled(ms, 1000L)
    assert(mm.storageMemoryUsed === 1000L)
    assert(evictedBlocks.nonEmpty)
    evictedBlocks.clear()
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
    val taskAttemptId = 0L
    val (mm, ms) = makeThings(maxMemory)
    // Acquire enough storage memory to exceed the storage region
    assert(mm.acquireStorageMemory(dummyBlock, 750L, evictedBlocks))
    assertEnsureFreeSpaceCalled(ms, 750L)
    assert(mm.executionMemoryUsed === 0L)
    assert(mm.storageMemoryUsed === 750L)
    assert(evictedBlocks.isEmpty)
    // Execution needs to request 250 bytes to evict storage memory
    assert(mm.acquireExecutionMemory(100L, taskAttemptId, MemoryMode.ON_HEAP) === 100L)
    assert(mm.executionMemoryUsed === 100L)
    assert(mm.storageMemoryUsed === 750L)
    assertEnsureFreeSpaceNotCalled(ms)
    assert(evictedBlocks.isEmpty)
    // Execution wants 200 bytes but only 150 are free, so storage is evicted
    assert(mm.acquireExecutionMemory(200L, taskAttemptId, MemoryMode.ON_HEAP) === 200L)
    assert(mm.executionMemoryUsed === 300L)
    assertEnsureFreeSpaceCalled(ms, 50L)
    assert(mm.executionMemoryUsed === 300L)
    assert(evictedBlocks.nonEmpty)
    mm.releaseAllStorageMemory()
    evictedBlocks.clear()
    require(mm.executionMemoryUsed === 300L)
    require(mm.storageMemoryUsed === 0, "bad test: all storage memory should have been released")
    // Acquire some storage memory again, but this time keep it within the storage region
    assert(mm.acquireStorageMemory(dummyBlock, 400L, evictedBlocks))
    assertEnsureFreeSpaceCalled(ms, 400L)
    assert(mm.storageMemoryUsed === 400L)
    assert(mm.executionMemoryUsed === 300L)
    // Execution cannot evict storage because the latter is within the storage fraction,
    // so grant only what's remaining without evicting anything, i.e. 1000 - 300 - 400 = 300
    assert(mm.acquireExecutionMemory(400L, taskAttemptId, MemoryMode.ON_HEAP) === 300L)
    assert(mm.executionMemoryUsed === 600L)
    assert(mm.storageMemoryUsed === 400L)
    assertEnsureFreeSpaceNotCalled(ms)
    assert(evictedBlocks.isEmpty)
  }

  test("execution can evict storage blocks when storage memory is below max mem (SPARK-12155)") {
    val maxMemory = 1000L
    val taskAttemptId = 0L
    val (mm, ms) = makeThings(maxMemory)
    // Acquire enough storage memory to exceed the storage region size
    assert(mm.acquireStorageMemory(dummyBlock, 750L, evictedBlocks))
    assertEnsureFreeSpaceCalled(ms, 750L)
    assert(mm.executionMemoryUsed === 0L)
    assert(mm.storageMemoryUsed === 750L)
    // Should now be able to require up to 500 bytes of memory
    assert(mm.acquireExecutionMemory(500L, taskAttemptId, MemoryMode.ON_HEAP) === 500L)
    assert(mm.storageMemoryUsed === 500L)
    assert(mm.executionMemoryUsed === 500L)
    assert(evictedBlocks.nonEmpty)
  }

  test("storage does not evict execution") {
    val maxMemory = 1000L
    val taskAttemptId = 0L
    val (mm, ms) = makeThings(maxMemory)
    // Acquire enough execution memory to exceed the execution region
    assert(mm.acquireExecutionMemory(800L, taskAttemptId, MemoryMode.ON_HEAP) === 800L)
    assert(mm.executionMemoryUsed === 800L)
    assert(mm.storageMemoryUsed === 0L)
    assertEnsureFreeSpaceNotCalled(ms)
    // Storage should not be able to evict execution
    assert(mm.acquireStorageMemory(dummyBlock, 100L, evictedBlocks))
    assert(mm.executionMemoryUsed === 800L)
    assert(mm.storageMemoryUsed === 100L)
    assertEnsureFreeSpaceCalled(ms, 100L)
    assert(!mm.acquireStorageMemory(dummyBlock, 250L, evictedBlocks))
    assert(mm.executionMemoryUsed === 800L)
    assert(mm.storageMemoryUsed === 100L)
    assertEnsureFreeSpaceCalled(ms, 250L)
    mm.releaseExecutionMemory(maxMemory, taskAttemptId, MemoryMode.ON_HEAP)
    mm.releaseStorageMemory(maxMemory)
    // Acquire some execution memory again, but this time keep it within the execution region
    assert(mm.acquireExecutionMemory(200L, taskAttemptId, MemoryMode.ON_HEAP) === 200L)
    assert(mm.executionMemoryUsed === 200L)
    assert(mm.storageMemoryUsed === 0L)
    assertEnsureFreeSpaceNotCalled(ms)
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

  test("small heap") {
    val systemMemory = 1024 * 1024
    val reservedMemory = 300 * 1024
    val memoryFraction = 0.8
    val conf = new SparkConf()
      .set("spark.memory.fraction", memoryFraction.toString)
      .set("spark.testing.memory", systemMemory.toString)
      .set("spark.testing.reservedMemory", reservedMemory.toString)
    val mm = UnifiedMemoryManager(conf, numCores = 1)
    val expectedMaxMemory = ((systemMemory - reservedMemory) * memoryFraction).toLong
    assert(mm.maxMemory === expectedMaxMemory)

    // Try using a system memory that's too small
    val conf2 = conf.clone().set("spark.testing.memory", (reservedMemory / 2).toString)
    val exception = intercept[IllegalArgumentException] {
      UnifiedMemoryManager(conf2, numCores = 1)
    }
    assert(exception.getMessage.contains("larger heap size"))
  }

}
