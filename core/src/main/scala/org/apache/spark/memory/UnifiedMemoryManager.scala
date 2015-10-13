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

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.storage.{BlockStatus, BlockId}


/**
 * A [[MemoryManager]] that enforces a soft boundary between execution and storage such that
 * either side can borrow memory from the other.
 *
 * The region shared between execution and storage is a fraction of the total heap space
 * configurable through `spark.memory.fraction` (default 0.75). The position of the boundary
 * within this space is further determined by `spark.memory.storageFraction` (default 0.5).
 * This means the size of the storage region is 0.75 * 0.5 = 0.375 of the heap space by default.
 *
 * Storage can borrow as much execution memory as is free until execution reclaims its space.
 * When this happens, cached blocks will be evicted from memory until sufficient borrowed
 * memory is released to satisfy the execution memory request.
 *
 * Similarly, execution can borrow as much storage memory as is free. However, execution
 * memory is *never* evicted by storage due to the complexities involved in implementing this.
 * The implication is that attempts to cache blocks may fail if execution has already eaten
 * up most of the storage space, in which case the new blocks will be evicted immediately
 * according to their respective storage levels.
 */
private[spark] class UnifiedMemoryManager(conf: SparkConf, maxMemory: Long) extends MemoryManager {

  def this(conf: SparkConf) {
    this(conf, UnifiedMemoryManager.getMaxMemory(conf))
  }

  /**
   * Size of the storage region, in bytes.
   *
   * This region is not statically reserved; execution can borrow from it if necessary.
   * Cached blocks can be evicted only if actual storage memory usage exceeds this region.
   */
  private val storageRegionSize: Long = {
    (maxMemory * conf.getDouble("spark.memory.storageFraction", 0.5)).toLong
  }

  /**
   * Total amount of memory, in bytes, not currently occupied by either execution or storage.
   */
  private def totalFreeMemory: Long = synchronized {
    assert(_executionMemoryUsed <= maxMemory)
    assert(_storageMemoryUsed <= maxMemory)
    assert(_executionMemoryUsed + _storageMemoryUsed <= maxMemory)
    maxMemory - _executionMemoryUsed - _storageMemoryUsed
  }

  /**
   * Total available memory for execution, in bytes.
   * In this model, this is equivalent to the amount of memory not occupied by storage.
   */
  override def maxExecutionMemory: Long = synchronized {
    maxMemory - _storageMemoryUsed
  }

  /**
   * Total available memory for storage, in bytes.
   * In this model, this is equivalent to the amount of memory not occupied by execution.
   */
  override def maxStorageMemory: Long = synchronized {
    maxMemory - _executionMemoryUsed
  }

  /**
   * Acquire N bytes of memory for execution, evicting cached blocks if necessary.
   *
   * This method evicts blocks only up to the amount of memory borrowed by storage.
   * Blocks evicted in the process, if any, are added to `evictedBlocks`.
   * @return number of bytes successfully granted (<= N).
   */
  override def acquireExecutionMemory(
      numBytes: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Long = synchronized {
    assert(numBytes >= 0)
    val memoryBorrowedByStorage = math.max(0, _storageMemoryUsed - storageRegionSize)
    // If there is not enough free memory AND storage has borrowed some execution memory,
    // then evict as much memory borrowed by storage as needed to grant this request
    val shouldEvictStorage = totalFreeMemory < numBytes && memoryBorrowedByStorage > 0
    if (shouldEvictStorage) {
      val spaceToEnsure = math.min(numBytes, memoryBorrowedByStorage)
      memoryStore.ensureFreeSpace(spaceToEnsure, evictedBlocks)
    }
    val bytesToGrant = math.min(numBytes, totalFreeMemory)
    _executionMemoryUsed += bytesToGrant
    bytesToGrant
  }

  /**
   * Acquire N bytes of memory to cache the given block, evicting existing ones if necessary.
   * Blocks evicted in the process, if any, are added to `evictedBlocks`.
   * @return whether all N bytes were successfully granted.
   */
  override def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean = synchronized {
    assert(numBytes >= 0)
    memoryStore.ensureFreeSpace(blockId, numBytes, evictedBlocks)
    val enoughMemory = totalFreeMemory >= numBytes
    if (enoughMemory) {
      _storageMemoryUsed += numBytes
    }
    enoughMemory
  }

}

private object UnifiedMemoryManager {

  /**
   * Return the total amount of memory shared between execution and storage, in bytes.
   */
  private def getMaxMemory(conf: SparkConf): Long = {
    val systemMaxMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)
    val memoryFraction = conf.getDouble("spark.memory.fraction", 0.75)
    (systemMaxMemory * memoryFraction).toLong
  }
}
