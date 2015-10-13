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

import org.apache.spark.Logging
import org.apache.spark.storage.{BlockId, BlockStatus, MemoryStore}


/**
 * An abstract memory manager that enforces how memory is shared between execution and storage.
 *
 * In this context, execution memory refers to that used for computation in shuffles, joins,
 * sorts and aggregations, while storage memory refers to that used for caching and propagating
 * internal data across the cluster. There exists one of these per JVM.
 */
private[spark] abstract class MemoryManager extends Logging {

  // The memory store used to evict cached blocks
  private var _memoryStore: MemoryStore = _
  protected def memoryStore: MemoryStore = {
    if (_memoryStore == null) {
      throw new IllegalArgumentException("memory store not initialized yet")
    }
    _memoryStore
  }

  // Amount of execution/storage memory in use, accesses must be synchronized on `this`
  protected var _executionMemoryUsed: Long = 0
  protected var _storageMemoryUsed: Long = 0

  /**
   * Set the [[MemoryStore]] used by this manager to evict cached blocks.
   * This must be set after construction due to initialization ordering constraints.
   */
  final def setMemoryStore(store: MemoryStore): Unit = {
    _memoryStore = store
  }

  /**
   * Total available memory for execution, in bytes.
   */
  def maxExecutionMemory: Long

  /**
   * Total available memory for storage, in bytes.
   */
  def maxStorageMemory: Long

  // TODO: avoid passing evicted blocks around to simplify method signatures (SPARK-10985)

  /**
   * Acquire N bytes of memory for execution, evicting cached blocks if necessary.
   * Blocks evicted in the process, if any, are added to `evictedBlocks`.
   * @return number of bytes successfully granted (<= N).
   */
  def acquireExecutionMemory(
      numBytes: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Long

  /**
   * Acquire N bytes of memory to cache the given block, evicting existing ones if necessary.
   * Blocks evicted in the process, if any, are added to `evictedBlocks`.
   * @return whether all N bytes were successfully granted.
   */
  def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean

  /**
   * Acquire N bytes of memory to unroll the given block, evicting existing ones if necessary.
   *
   * This extra method allows subclasses to differentiate behavior between acquiring storage
   * memory and acquiring unroll memory. For instance, the memory management model in Spark
   * 1.5 and before places a limit on the amount of space that can be freed from unrolling.
   * Blocks evicted in the process, if any, are added to `evictedBlocks`.
   *
   * @return whether all N bytes were successfully granted.
   */
  def acquireUnrollMemory(
      blockId: BlockId,
      numBytes: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean = synchronized {
    acquireStorageMemory(blockId, numBytes, evictedBlocks)
  }

  /**
   * Release N bytes of execution memory.
   */
  def releaseExecutionMemory(numBytes: Long): Unit = synchronized {
    if (numBytes > _executionMemoryUsed) {
      logWarning(s"Attempted to release $numBytes bytes of execution " +
        s"memory when we only have ${_executionMemoryUsed} bytes")
      _executionMemoryUsed = 0
    } else {
      _executionMemoryUsed -= numBytes
    }
  }

  /**
   * Release N bytes of storage memory.
   */
  def releaseStorageMemory(numBytes: Long): Unit = synchronized {
    if (numBytes > _storageMemoryUsed) {
      logWarning(s"Attempted to release $numBytes bytes of storage " +
        s"memory when we only have ${_storageMemoryUsed} bytes")
      _storageMemoryUsed = 0
    } else {
      _storageMemoryUsed -= numBytes
    }
  }

  /**
   * Release all storage memory acquired.
   */
  def releaseAllStorageMemory(): Unit = synchronized {
    _storageMemoryUsed = 0
  }

  /**
   * Release N bytes of unroll memory.
   */
  def releaseUnrollMemory(numBytes: Long): Unit = synchronized {
    releaseStorageMemory(numBytes)
  }

  /**
   * Execution memory currently in use, in bytes.
   */
  final def executionMemoryUsed: Long = synchronized {
    _executionMemoryUsed
  }

  /**
   * Storage memory currently in use, in bytes.
   */
  final def storageMemoryUsed: Long = synchronized {
    _storageMemoryUsed
  }

}
