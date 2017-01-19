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
import org.apache.spark.storage.TestBlockId
import org.apache.spark.storage.memory.MemoryStore

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
      .set("spark.memory.offHeap.size", maxOffHeapExecutionMemory.toString)
      .set("spark.memory.storageFraction", storageFraction.toString)
    UnifiedMemoryManager(conf, numCores = 1)
  }

  test("basic execution memory") {
    val maxMemory = 1000L
    val taskAttemptId = 0L
    val (mm, _) = makeThings(maxMemory)
    val memoryMode = MemoryMode.ON_HEAP
    assert(mm.executionMemoryUsed === 0L)
    assert(mm.acquireExecutionMemory(10L, taskAttemptId, memoryMode) === 10L)
    assert(mm.executionMemoryUsed === 10L)
    assert(mm.acquireExecutionMemory(100L, taskAttemptId, memoryMode) === 100L)
    // Acquire up to the max
    assert(mm.acquireExecutionMemory(1000L, taskAttemptId, memoryMode) === 890L)
    assert(mm.executionMemoryUsed === maxMemory)
    assert(mm.acquireExecutionMemory(1L, taskAttemptId, memoryMode) === 0L)
    assert(mm.executionMemoryUsed === maxMemory)
    mm.releaseExecutionMemory(800L, taskAttemptId, memoryMode)
    assert(mm.executionMemoryUsed === 200L)
    // Acquire after release
    assert(mm.acquireExecutionMemory(1L, taskAttemptId, memoryMode) === 1L)
    assert(mm.executionMemoryUsed === 201L)
    // Release beyond what was acquired
    mm.releaseExecutionMemory(maxMemory, taskAttemptId, memoryMode)
    assert(mm.executionMemoryUsed === 0L)
  }

  test("basic storage memory") {
    val maxMemory = 1000L
    val (mm, ms) = makeThings(maxMemory)
    val memoryMode = MemoryMode.ON_HEAP
    assert(mm.storageMemoryUsed === 0L)
    assert(mm.acquireStorageMemory(dummyBlock, 10L, memoryMode))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.storageMemoryUsed === 10L)

    assert(mm.acquireStorageMemory(dummyBlock, 100L, memoryMode))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.storageMemoryUsed === 110L)
    // Acquire more than the max, not granted
    assert(!mm.acquireStorageMemory(dummyBlock, maxMemory + 1L, memoryMode))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.storageMemoryUsed === 110L)
    // Acquire up to the max, requests after this are still granted due to LRU eviction
    assert(mm.acquireStorageMemory(dummyBlock, maxMemory, memoryMode))
    assertEvictBlocksToFreeSpaceCalled(ms, 110L)
    assert(mm.storageMemoryUsed === 1000L)
    assert(evictedBlocks.nonEmpty)
    evictedBlocks.clear()
    assert(mm.acquireStorageMemory(dummyBlock, 1L, memoryMode))
    assertEvictBlocksToFreeSpaceCalled(ms, 1L)
    assert(evictedBlocks.nonEmpty)
    evictedBlocks.clear()
    // Note: We evicted 1 byte to put another 1-byte block in, so the storage memory used remains at
    // 1000 bytes. This is different from real behavior, where the 1-byte block would have evicted
    // the 1000-byte block entirely. This is set up differently so we can write finer-grained tests.
    assert(mm.storageMemoryUsed === 1000L)
    mm.releaseStorageMemory(800L, memoryMode)
    assert(mm.storageMemoryUsed === 200L)
    // Acquire after release
    assert(mm.acquireStorageMemory(dummyBlock, 1L, memoryMode))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.storageMemoryUsed === 201L)
    mm.releaseAllStorageMemory()
    assert(mm.storageMemoryUsed === 0L)
    assert(mm.acquireStorageMemory(dummyBlock, 1L, memoryMode))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.storageMemoryUsed === 1L)
    // Release beyond what was acquired
    mm.releaseStorageMemory(100L, memoryMode)
    assert(mm.storageMemoryUsed === 0L)
  }

  test("execution evicts storage") {
    val maxMemory = 1000L
    val taskAttemptId = 0L
    val (mm, ms) = makeThings(maxMemory)
    val memoryMode = MemoryMode.ON_HEAP
    // Acquire enough storage memory to exceed the storage region
    assert(mm.acquireStorageMemory(dummyBlock, 750L, memoryMode))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.executionMemoryUsed === 0L)
    assert(mm.storageMemoryUsed === 750L)
    // Execution needs to request 250 bytes to evict storage memory
    assert(mm.acquireExecutionMemory(100L, taskAttemptId, memoryMode) === 100L)
    assert(mm.executionMemoryUsed === 100L)
    assert(mm.storageMemoryUsed === 750L)
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    // Execution wants 200 bytes but only 150 are free, so storage is evicted
    assert(mm.acquireExecutionMemory(200L, taskAttemptId, memoryMode) === 200L)
    assert(mm.executionMemoryUsed === 300L)
    assert(mm.storageMemoryUsed === 700L)
    assertEvictBlocksToFreeSpaceCalled(ms, 50L)
    assert(evictedBlocks.nonEmpty)
    evictedBlocks.clear()
    mm.releaseAllStorageMemory()
    require(mm.executionMemoryUsed === 300L)
    require(mm.storageMemoryUsed === 0, "bad test: all storage memory should have been released")
    // Acquire some storage memory again, but this time keep it within the storage region
    assert(mm.acquireStorageMemory(dummyBlock, 400L, memoryMode))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.storageMemoryUsed === 400L)
    assert(mm.executionMemoryUsed === 300L)
    // Execution cannot evict storage because the latter is within the storage fraction,
    // so grant only what's remaining without evicting anything, i.e. 1000 - 300 - 400 = 300
    assert(mm.acquireExecutionMemory(400L, taskAttemptId, memoryMode) === 300L)
    assert(mm.executionMemoryUsed === 600L)
    assert(mm.storageMemoryUsed === 400L)
    assertEvictBlocksToFreeSpaceNotCalled(ms)
  }

  test("execution memory requests smaller than free memory should evict storage (SPARK-12165)") {
    val maxMemory = 1000L
    val taskAttemptId = 0L
    val (mm, ms) = makeThings(maxMemory)
    val memoryMode = MemoryMode.ON_HEAP
    // Acquire enough storage memory to exceed the storage region size
    assert(mm.acquireStorageMemory(dummyBlock, 700L, memoryMode))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.executionMemoryUsed === 0L)
    assert(mm.storageMemoryUsed === 700L)
    // SPARK-12165: previously, MemoryStore would not evict anything because it would
    // mistakenly think that the 300 bytes of free space was still available even after
    // using it to expand the execution pool. Consequently, no storage memory was released
    // and the following call granted only 300 bytes to execution.
    assert(mm.acquireExecutionMemory(500L, taskAttemptId, memoryMode) === 500L)
    assertEvictBlocksToFreeSpaceCalled(ms, 200L)
    assert(mm.storageMemoryUsed === 500L)
    assert(mm.executionMemoryUsed === 500L)
    assert(evictedBlocks.nonEmpty)
  }

  test("storage does not evict execution") {
    val maxMemory = 1000L
    val taskAttemptId = 0L
    val (mm, ms) = makeThings(maxMemory)
    val memoryMode = MemoryMode.ON_HEAP
    // Acquire enough execution memory to exceed the execution region
    assert(mm.acquireExecutionMemory(800L, taskAttemptId, memoryMode) === 800L)
    assert(mm.executionMemoryUsed === 800L)
    assert(mm.storageMemoryUsed === 0L)
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    // Storage should not be able to evict execution
    assert(mm.acquireStorageMemory(dummyBlock, 100L, memoryMode))
    assert(mm.executionMemoryUsed === 800L)
    assert(mm.storageMemoryUsed === 100L)
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(!mm.acquireStorageMemory(dummyBlock, 250L, memoryMode))
    assert(mm.executionMemoryUsed === 800L)
    assert(mm.storageMemoryUsed === 100L)
    // Do not attempt to evict blocks, since evicting will not free enough memory:
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    mm.releaseExecutionMemory(maxMemory, taskAttemptId, memoryMode)
    mm.releaseStorageMemory(maxMemory, memoryMode)
    // Acquire some execution memory again, but this time keep it within the execution region
    assert(mm.acquireExecutionMemory(200L, taskAttemptId, memoryMode) === 200L)
    assert(mm.executionMemoryUsed === 200L)
    assert(mm.storageMemoryUsed === 0L)
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    // Storage should still not be able to evict execution
    assert(mm.acquireStorageMemory(dummyBlock, 750L, memoryMode))
    assert(mm.executionMemoryUsed === 200L)
    assert(mm.storageMemoryUsed === 750L)
    assertEvictBlocksToFreeSpaceNotCalled(ms) // since there were 800 bytes free
    assert(!mm.acquireStorageMemory(dummyBlock, 850L, memoryMode))
    assert(mm.executionMemoryUsed === 200L)
    assert(mm.storageMemoryUsed === 750L)
    // Do not attempt to evict blocks, since evicting will not free enough memory:
    assertEvictBlocksToFreeSpaceNotCalled(ms)
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
    assert(mm.maxHeapMemory === expectedMaxMemory)

    // Try using a system memory that's too small
    val conf2 = conf.clone().set("spark.testing.memory", (reservedMemory / 2).toString)
    val exception = intercept[IllegalArgumentException] {
      UnifiedMemoryManager(conf2, numCores = 1)
    }
    assert(exception.getMessage.contains("increase heap size"))
  }

  test("insufficient executor memory") {
    val systemMemory = 1024 * 1024
    val reservedMemory = 300 * 1024
    val memoryFraction = 0.8
    val conf = new SparkConf()
      .set("spark.memory.fraction", memoryFraction.toString)
      .set("spark.testing.memory", systemMemory.toString)
      .set("spark.testing.reservedMemory", reservedMemory.toString)
    val mm = UnifiedMemoryManager(conf, numCores = 1)

    // Try using an executor memory that's too small
    val conf2 = conf.clone().set("spark.executor.memory", (reservedMemory / 2).toString)
    val exception = intercept[IllegalArgumentException] {
      UnifiedMemoryManager(conf2, numCores = 1)
    }
    assert(exception.getMessage.contains("increase executor memory"))
  }

  test("execution can evict cached blocks when there are multiple active tasks (SPARK-12155)") {
    val conf = new SparkConf()
      .set("spark.memory.fraction", "1")
      .set("spark.memory.storageFraction", "0")
      .set("spark.testing.memory", "1000")
    val mm = UnifiedMemoryManager(conf, numCores = 2)
    val ms = makeMemoryStore(mm)
    val memoryMode = MemoryMode.ON_HEAP
    assert(mm.maxHeapMemory === 1000)
    // Have two tasks each acquire some execution memory so that the memory pool registers that
    // there are two active tasks:
    assert(mm.acquireExecutionMemory(100L, 0, memoryMode) === 100L)
    assert(mm.acquireExecutionMemory(100L, 1, memoryMode) === 100L)
    // Fill up all of the remaining memory with storage.
    assert(mm.acquireStorageMemory(dummyBlock, 800L, memoryMode))
    assertEvictBlocksToFreeSpaceNotCalled(ms)
    assert(mm.storageMemoryUsed === 800)
    assert(mm.executionMemoryUsed === 200)
    // A task should still be able to allocate 100 bytes execution memory by evicting blocks
    assert(mm.acquireExecutionMemory(100L, 0, memoryMode) === 100L)
    assertEvictBlocksToFreeSpaceCalled(ms, 100L)
    assert(mm.executionMemoryUsed === 300)
    assert(mm.storageMemoryUsed === 700)
    assert(evictedBlocks.nonEmpty)
  }

  test("SPARK-15260: atomically resize memory pools") {
    val conf = new SparkConf()
      .set("spark.memory.fraction", "1")
      .set("spark.memory.storageFraction", "0")
      .set("spark.testing.memory", "1000")
    val mm = UnifiedMemoryManager(conf, numCores = 2)
    makeBadMemoryStore(mm)
    val memoryMode = MemoryMode.ON_HEAP
    // Acquire 1000 then release 600 bytes of storage memory, leaving the
    // storage memory pool at 1000 bytes but only 400 bytes of which are used.
    assert(mm.acquireStorageMemory(dummyBlock, 1000L, memoryMode))
    mm.releaseStorageMemory(600L, memoryMode)
    // Before the fix for SPARK-15260, we would first shrink the storage pool by the amount of
    // unused storage memory (600 bytes), try to evict blocks, then enlarge the execution pool
    // by the same amount. If the eviction threw an exception, then we would shrink one pool
    // without enlarging the other, resulting in an assertion failure.
    intercept[RuntimeException] {
      mm.acquireExecutionMemory(1000L, 0, memoryMode)
    }
    val assertInvariants = PrivateMethod[Unit]('assertInvariants)
    mm.invokePrivate[Unit](assertInvariants())
  }

}
