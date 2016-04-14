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

import org.apache.spark.SparkConf
import org.apache.spark.storage.BlockId

/**
 * A [[MemoryManager]] that statically partitions the heap space into disjoint regions.
 *
 * The sizes of the execution and storage regions are determined through
 * `spark.shuffle.memoryFraction` and `spark.storage.memoryFraction` respectively. The two
 * regions are cleanly separated such that neither usage can borrow memory from the other.
 */
private[spark] class StaticMemoryManager(
    conf: SparkConf,
    numCores: Int,
    totalHeapMemory: Long,
    totalOffHeapMemory: Long)
  extends MemoryManager(conf, numCores) {

  def this(conf: SparkConf, numCores: Int) {
    this(
      conf,
      numCores = numCores,
      totalHeapMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory),
      totalOffHeapMemory = conf.getSizeAsBytes("spark.memory.offHeap.size", 0))
  }

  override val heapMemoryPool: MemoryPool = {
    val maxHeapExecutionMemory: Long = {
      val memoryFraction = conf.getDouble("spark.shuffle.memoryFraction", 0.2)
      val safetyFraction = conf.getDouble("spark.shuffle.safetyFraction", 0.8)
      (totalHeapMemory * memoryFraction * safetyFraction).toLong
    }
    val maxHeapStorageMemory: Long = {
      val memoryFraction = conf.getDouble("spark.storage.memoryFraction", 0.6)
      val safetyFraction = conf.getDouble("spark.storage.safetyFraction", 0.9)
      (totalHeapMemory * memoryFraction * safetyFraction).toLong
    }
    new MemoryPool(
      conf,
      numCores = numCores,
      memoryMode = MemoryMode.ON_HEAP,
      totalMemory = totalHeapMemory,
      maxExecutionMemory = maxHeapExecutionMemory,
      maxStorageMemory = maxHeapStorageMemory,
      unevictableStorageMemory = maxHeapStorageMemory)
  }

  override val offHeapMemoryPool: MemoryPool = {
    new MemoryPool(
      conf,
      numCores = numCores,
      memoryMode = MemoryMode.OFF_HEAP,
      totalMemory = totalOffHeapMemory,
      maxExecutionMemory = totalOffHeapMemory,
      maxStorageMemory = 0L, // StaticMemoryManager does not support off-heap storage memory
      unevictableStorageMemory = 0L)
  }

  // Max number of bytes worth of blocks to evict when unrolling
  private val maxUnrollMemory: Long = {
    (heapMemoryPool.maxStorageMemory * conf.getDouble("spark.storage.unrollFraction", 0.2)).toLong
  }

  override def acquireUnrollMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized {
    // TODO(josh): update after off-heap caching patch is merged.
    require(memoryMode != MemoryMode.OFF_HEAP,
      "StaticMemoryManager does not support off-heap unroll memory")
    val currentUnrollMemory = heapMemoryPool.memoryStore.currentUnrollMemory
    val freeMemory = math.min(
      heapMemoryPool.freeMemory, heapMemoryPool.maxStorageMemory - heapMemoryPool.storageMemoryUsed)
    // When unrolling, we will use all of the existing free memory, and, if necessary,
    // some extra space freed from evicting cached blocks. We must place a cap on the
    // amount of memory to be evicted by unrolling, however, otherwise unrolling one
    // big block can blow away the entire cache.
    val maxBytesToAttemptToFreeViaEviction =
      math.max(0, maxUnrollMemory - currentUnrollMemory - freeMemory)
    acquireStorageMemory(blockId, numBytes, memoryMode, maxBytesToAttemptToFreeViaEviction)
  }
}
