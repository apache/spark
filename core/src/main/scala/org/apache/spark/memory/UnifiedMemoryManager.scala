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
private[spark] class UnifiedMemoryManager(conf: SparkConf) extends MemoryManager {

  /**
   * Acquire N bytes of memory for execution.
   * @return number of bytes successfully granted (<= N).
   */
  override def acquireExecutionMemory(numBytes: Long): Long = numBytes

  /**
   * Acquire N bytes of memory to cache the given block, evicting existing ones if necessary.
   * Blocks evicted in the process, if any, are added to `evictedBlocks`.
   * @return whether all N bytes were successfully granted.
   */
  override def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean = true

  /**
   * Acquire N bytes of memory to unroll the given block, evicting existing ones if necessary.
   * Blocks evicted in the process, if any, are added to `evictedBlocks`.
   * @return whether all N bytes were successfully granted.
   */
  override def acquireUnrollMemory(
      blockId: BlockId,
      numBytes: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean = true

  /**
   * Release N bytes of execution memory.
   */
  override def releaseExecutionMemory(numBytes: Long): Unit = { }

  /**
   * Release N bytes of storage memory.
   */
  override def releaseStorageMemory(numBytes: Long): Unit = { }

  /**
   * Release all storage memory acquired.
   */
  override def releaseStorageMemory(): Unit = { }

  /**
   * Release N bytes of unroll memory.
   */
  override def releaseUnrollMemory(numBytes: Long): Unit = { }

  /**
   * Total available memory for execution, in bytes.
   */
  override def maxExecutionMemory: Long = 0

  /**
   * Total available memory for storage, in bytes.
   */
  override def maxStorageMemory: Long = 0

  /**
   * Execution memory currently in use, in bytes.
   */
  override def executionMemoryUsed: Long = 0

  /**
   * Storage memory currently in use, in bytes.
   */
  override def storageMemoryUsed: Long = 0

}
