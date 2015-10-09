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
 * When this happens, cached blocks will be evicted from memory until all borrowed storage
 * memory is released.
 *
 * Similarly, execution can borrow as much storage memory as is free. However, execution
 * memory is *never* evicted by storage due to the complexities involved in implementing this.
 * The implication is that attempts to cache blocks may fail if execution has already eaten
 * up most of the storage space, in which case the new blocks will be evicted directly.
 */
private[spark] class UnifiedMemoryManager(
    conf: SparkConf,
    maxMemory: Long) extends MemoryManager {

  def this(conf: SparkConf) {
    this(conf, UnifiedMemoryManager.getMaxMemory(conf))
  }

  // Size of the storage region, in bytes
  // Cached blocks can be evicted only if actual storage memory used exceeds this
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
   */
  override val maxExecutionMemory: Long = maxMemory

  /**
   * Total available memory for storage, in bytes.
   */
  override val maxStorageMemory: Long = maxMemory

  /**
   * Acquire N bytes of memory for execution, evicting cached blocks if necessary.
   *
   * This method only evicts blocks up to the amount of memory borrowed by storage.
   * Blocks evicted in the process, if any, are added to `evictedBlocks`.
   * @return number of bytes successfully granted (<= N).
   */
  override def acquireExecutionMemory(
      numBytes: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Long = {
    assert(numBytes >= 0)
    var spaceToEnsure = 0L

    // Grant as much of `numBytes` as is available
    def grantFreeMemory(): Long = synchronized {
      val bytesToGrant = math.min(numBytes, totalFreeMemory)
      _executionMemoryUsed += bytesToGrant
      return bytesToGrant
    }

    synchronized {
      // If there is not enough free memory AND storage has borrowed some execution memory,
      // then evict as much memory borrowed by storage as needed by dropping existing blocks
      val memoryBorrowedByStorage = math.max(0, _storageMemoryUsed - storageRegionSize)
      val shouldEvictStorage = totalFreeMemory < numBytes && memoryBorrowedByStorage > 0
      if (shouldEvictStorage) {
        // Note: do not call `ensureFreeSpace` in this synchronized block to avoid deadlocks
        spaceToEnsure = math.min(numBytes, memoryBorrowedByStorage)
      } else {
        return grantFreeMemory()
      }
    }

    // If we reached here, then we should ensure free space and try again.
    //
    // Note: here we assume this method is externally synchronized by the caller, meaning at any
    // given time only one thread can be acquiring execution memory. However, it is still possible
    // for another thread to acquire storage memory concurrently. Because of this, there are two
    // potential race conditions:
    //
    //   (1) Before ensuring free space, someone else jumps in and puts more blocks, borrowing
    //       more memory from execution in the process. In this case we may end up freeing less
    //       space than what we could have freed otherwise.
    //
    //   (2) After ensuring free space, someone else jumps in and steals the space we just freed
    //
    // In both cases, the worst case is that we don't grant all execution memory requested, which
    // is generally safe since we expect the caller to just spill. Note that fixing this requires
    // synchronizing all memory acquisitions, execution and storage, on the same lock, which may
    // be difficult.

    assert(spaceToEnsure > 0)
    memoryStore.ensureFreeSpace(spaceToEnsure, evictedBlocks)
    grantFreeMemory()
  }

  /**
   * Acquire N bytes of memory to cache the given block, evicting existing ones if necessary.
   * Blocks evicted in the process, if any, are added to `evictedBlocks`.
   * @return whether all N bytes were successfully granted.
   */
  override def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean = {
    assert(numBytes >= 0)
    // Note: Keep this outside synchronized block to avoid potential deadlocks!
    memoryStore.ensureFreeSpace(blockId, numBytes, evictedBlocks)
    synchronized {
      val enoughMemory = totalFreeMemory >= numBytes
      if (enoughMemory) {
        _storageMemoryUsed += numBytes
      }
      enoughMemory
    }
  }

}

private object UnifiedMemoryManager {

  /**
   * Return the total amount of memory shared between execution and storage, in bytes.
   */
  private def getMaxMemory(conf: SparkConf): Long = {
    (Runtime.getRuntime.maxMemory * conf.getDouble("spark.memory.fraction", 0.75)).toLong
  }
}
