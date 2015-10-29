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

import org.apache.spark.{TaskContext, Logging}
import org.apache.spark.storage.{MemoryStore, BlockStatus, BlockId}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class StorageMemoryPool extends MemoryPool with Logging {

  private[this] var _memoryUsed: Long = 0L

  override def memoryUsed: Long = _memoryUsed

  private var _memoryStore: MemoryStore = _
  def memoryStore: MemoryStore = {
    if (_memoryStore == null) {
      throw new IllegalArgumentException("memory store not initialized yet")
    }
    _memoryStore
  }

  /**
   * Set the [[MemoryStore]] used by this manager to evict cached blocks.
   * This must be set after construction due to initialization ordering constraints.
   */
  final def setMemoryStore(store: MemoryStore): Unit = {
    _memoryStore = store
  }

  /**
   * Acquire N bytes of memory to cache the given block, evicting existing ones if necessary.
   * Blocks evicted in the process, if any, are added to `evictedBlocks`.
   * @return whether all N bytes were successfully granted.
   */
  def acquireMemory(
      blockId: BlockId,
      numBytes: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean = synchronized {
    acquireMemory(blockId, numBytes, numBytes, evictedBlocks)
  }

  /**
   * Acquire N bytes of storage memory for the given block, evicting existing ones if necessary.
   *
   * @param blockId the ID of the block we are acquiring storage memory for
   * @param numBytesToAcquire the size of this block
   * @param numBytesToFree the size of space to be freed through evicting blocks
   * @return whether all N bytes were successfully granted.
   */
  def acquireMemory(
      blockId: BlockId,
      numBytesToAcquire: Long,
      numBytesToFree: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean = synchronized {
    assert(numBytesToAcquire >= 0)
    assert(numBytesToFree >= 0)
    // TODO(josh): check whether there is enough memory / handle eviction
    memoryStore.ensureFreeSpace(blockId, numBytesToFree, evictedBlocks)
    // Register evicted blocks, if any, with the active task metrics
    Option(TaskContext.get()).foreach { tc =>
      val metrics = tc.taskMetrics()
      val lastUpdatedBlocks = metrics.updatedBlocks.getOrElse(Seq[(BlockId, BlockStatus)]())
      metrics.updatedBlocks = Some(lastUpdatedBlocks ++ evictedBlocks.toSeq)
    }
    // TODO(josh): is this assertion in the right place?
    assert(memoryUsed <= poolSize)
    val enoughMemory = numBytesToAcquire <= memoryFree
    if (enoughMemory) {
      _memoryUsed += numBytesToAcquire
    }
    enoughMemory
  }

  def releaseMemory(size: Long): Unit = {
    if (size > _memoryUsed) {
      logWarning(s"Attempted to release $size bytes of storage " +
        s"memory when we only have ${_memoryUsed} bytes")
      _memoryUsed = 0
    } else {
      _memoryUsed -= size
    }
  }

  def releaseAllMemory(): Unit = {
    _memoryUsed = 0
  }

  def shrinkPoolByEvictingBlocks(spaceToEnsure: Long): Long = {
    val evictedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
    memoryStore.ensureFreeSpace(spaceToEnsure, evictedBlocks)
    val spaceFreed = evictedBlocks.map(_._2.memSize).sum
    _memoryUsed -= spaceFreed
    decrementPoolSize(spaceFreed)
    spaceFreed
  }
}
