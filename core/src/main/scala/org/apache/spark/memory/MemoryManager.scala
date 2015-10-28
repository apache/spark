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

import org.apache.spark.storage.{BlockId, BlockStatus, MemoryStore}


/**
 * An abstract memory manager that enforces how memory is shared between execution and storage.
 *
 * In this context, execution memory refers to that used for computation in shuffles, joins,
 * sorts and aggregations, while storage memory refers to that used for caching and propagating
 * internal data across the cluster. There exists one of these per JVM.
 */
private[spark] abstract class MemoryManager {

  // The memory store used to evict cached blocks
  private var _memoryStore: MemoryStore = _
  protected def memoryStore: MemoryStore = {
    if (_memoryStore == null) {
      throw new IllegalArgumentException("memory store not initialized yet")
    }
    _memoryStore
  }

  /**
   * Set the [[MemoryStore]] used by this manager to evict cached blocks.
   * This must be set after construction due to initialization ordering constraints.
   */
  def setMemoryStore(store: MemoryStore): Unit = {
    _memoryStore = store
  }

  /**
   * Acquire N bytes of memory for execution.
   * @return number of bytes successfully granted (<= N).
   */
  def acquireExecutionMemory(numBytes: Long): Long

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
   * Blocks evicted in the process, if any, are added to `evictedBlocks`.
   * @return whether all N bytes were successfully granted.
   */
  def acquireUnrollMemory(
      blockId: BlockId,
      numBytes: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean

  /**
   * Release N bytes of execution memory.
   */
  def releaseExecutionMemory(numBytes: Long): Unit

  /**
   * Release N bytes of storage memory.
   */
  def releaseStorageMemory(numBytes: Long): Unit

  /**
   * Release all storage memory acquired.
   */
  def releaseStorageMemory(): Unit

  /**
   * Release N bytes of unroll memory.
   */
  def releaseUnrollMemory(numBytes: Long): Unit

  /**
   * Total available memory for execution, in bytes.
   */
  def maxExecutionMemory: Long

  /**
   * Total available memory for storage, in bytes.
   */
  def maxStorageMemory: Long

  /**
   * Execution memory currently in use, in bytes.
   */
  def executionMemoryUsed: Long

  /**
   * Storage memory currently in use, in bytes.
   */
  def storageMemoryUsed: Long

}
