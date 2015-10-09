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

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.storage.{BlockId, BlockStatus}


/**
 * A [[MemoryManager]] that statically partitions the heap space into disjoint regions.
 *
 * The sizes of the execution and storage regions are determined through
 * `spark.shuffle.memoryFraction` and `spark.storage.memoryFraction` respectively. The two
 * regions are cleanly separated such that neither usage can borrow memory from the other.
 */
private[spark] class StaticMemoryManager(
    conf: SparkConf,
    override val maxExecutionMemory: Long,
    override val maxStorageMemory: Long)
  extends MemoryManager with Logging {

  // Max number of bytes worth of blocks to evict when unrolling
  private val maxMemoryToEvictForUnroll: Long = {
    (maxStorageMemory * conf.getDouble("spark.storage.unrollFraction", 0.2)).toLong
  }

  // Amount of execution / storage memory in use
  // Accesses must be synchronized on `this`
  private var _executionMemoryUsed: Long = 0
  private var _storageMemoryUsed: Long = 0

  def this(conf: SparkConf) {
    this(
      conf,
      StaticMemoryManager.getMaxExecutionMemory(conf),
      StaticMemoryManager.getMaxStorageMemory(conf))
  }

  /**
   * Acquire N bytes of memory for execution.
   * @return number of bytes successfully granted (<= N).
   */
  override def acquireExecutionMemory(numBytes: Long): Long = synchronized {
    assert(_executionMemoryUsed <= maxExecutionMemory)
    val bytesToGrant = math.min(numBytes, maxExecutionMemory - _executionMemoryUsed)
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
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean = {
    acquireStorageMemory(blockId, numBytes, numBytes, evictedBlocks)
  }

  /**
   * Acquire N bytes of memory to unroll the given block, evicting existing ones if necessary.
   *
   * This evicts at most M bytes worth of existing blocks, where M is a fraction of the storage
   * space specified by `spark.storage.unrollFraction`. Blocks evicted in the process, if any,
   * are added to `evictedBlocks`.
   *
   * @return whether all N bytes were successfully granted.
   */
  override def acquireUnrollMemory(
      blockId: BlockId,
      numBytes: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean = {
    val currentUnrollMemory = memoryStore.currentUnrollMemory
    val maxNumBytesToFree = math.max(0, maxMemoryToEvictForUnroll - currentUnrollMemory)
    val numBytesToFree = math.min(numBytes, maxNumBytesToFree)
    acquireStorageMemory(blockId, numBytes, numBytesToFree, evictedBlocks)
  }

  /**
   * Acquire N bytes of storage memory for the given block, evicting existing ones if necessary.
   *
   * @param blockId the ID of the block we are acquiring storage memory for
   * @param numBytesToAcquire the size of this block
   * @param numBytesToFree the size of space to be freed through evicting blocks
   * @param evictedBlocks a holder for blocks evicted in the process
   * @return whether all N bytes were successfully granted.
   */
  private def acquireStorageMemory(
      blockId: BlockId,
      numBytesToAcquire: Long,
      numBytesToFree: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean = {
    // Note: Keep this outside synchronized block to avoid potential deadlocks!
    memoryStore.ensureFreeSpace(blockId, numBytesToFree, evictedBlocks)
    synchronized {
      assert(_storageMemoryUsed <= maxStorageMemory)
      val enoughMemory = _storageMemoryUsed + numBytesToAcquire <= maxStorageMemory
      if (enoughMemory) {
        _storageMemoryUsed += numBytesToAcquire
      }
      enoughMemory
    }
  }

  /**
   * Release N bytes of execution memory.
   */
  override def releaseExecutionMemory(numBytes: Long): Unit = synchronized {
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
  override def releaseStorageMemory(numBytes: Long): Unit = synchronized {
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
  override def releaseStorageMemory(): Unit = synchronized {
    _storageMemoryUsed = 0
  }

  /**
   * Release N bytes of unroll memory.
   */
  override def releaseUnrollMemory(numBytes: Long): Unit = {
    releaseStorageMemory(numBytes)
  }

  /**
   * Amount of execution memory currently in use, in bytes.
   */
  override def executionMemoryUsed: Long = synchronized {
    _executionMemoryUsed
  }

  /**
   * Amount of storage memory currently in use, in bytes.
   */
  override def storageMemoryUsed: Long = synchronized {
    _storageMemoryUsed
  }

}


private[spark] object StaticMemoryManager {

  /**
   * Return the total amount of memory available for the storage region, in bytes.
   */
  private def getMaxStorageMemory(conf: SparkConf): Long = {
    val memoryFraction = conf.getDouble("spark.storage.memoryFraction", 0.6)
    val safetyFraction = conf.getDouble("spark.storage.safetyFraction", 0.9)
    (Runtime.getRuntime.maxMemory * memoryFraction * safetyFraction).toLong
  }


  /**
   * Return the total amount of memory available for the execution region, in bytes.
   */
  private def getMaxExecutionMemory(conf: SparkConf): Long = {
    val memoryFraction = conf.getDouble("spark.shuffle.memoryFraction", 0.2)
    val safetyFraction = conf.getDouble("spark.shuffle.safetyFraction", 0.8)
    (Runtime.getRuntime.maxMemory * memoryFraction * safetyFraction).toLong
  }

}
