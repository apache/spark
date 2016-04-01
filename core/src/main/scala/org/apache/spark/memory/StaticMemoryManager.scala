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
  extends MemoryManager(
    conf,
    numCores,
    totalHeapMemory,
    totalOffHeapMemory) {

  def this(conf: SparkConf, numCores: Int) {
    this(
      conf,
      numCores = numCores,
      totalHeapMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory),
      totalOffHeapMemory = conf.getSizeAsBytes("spark.memory.offHeap.size", 0))
  }

  override protected val maxHeapExecutionMemory: Long = {
    val memoryFraction = conf.getDouble("spark.shuffle.memoryFraction", 0.2)
    val safetyFraction = conf.getDouble("spark.shuffle.safetyFraction", 0.8)
    (totalHeapMemory * memoryFraction * safetyFraction).toLong
  }
  override val maxHeapStorageMemory: Long = {
    val memoryFraction = conf.getDouble("spark.storage.memoryFraction", 0.6)
    val safetyFraction = conf.getDouble("spark.storage.safetyFraction", 0.9)
    (totalHeapMemory * memoryFraction * safetyFraction).toLong
  }
  override protected val unevictableHeapStorageMemory: Long = maxHeapStorageMemory
  override protected val maxOffHeapExecutionMemory: Long = totalOffHeapMemory
  // Max number of bytes worth of blocks to evict when unrolling
  private val maxUnrollMemory: Long = {
    (maxHeapStorageMemory * conf.getDouble("spark.storage.unrollFraction", 0.2)).toLong
  }
  // StaticMemoryManager does not support off-heap storage memory:
  override protected val maxOffHeapStorageMemory: Long = 0L
  override protected val unevictableOffHeapStorageMemory: Long = 0L

  override def acquireUnrollMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized {
    // TODO(josh): update after off-heap caching patch is merged.
    require(memoryMode != MemoryMode.OFF_HEAP,
      "StaticMemoryManager does not support off-heap unroll memory")
    val currentUnrollMemory = memoryStore.currentUnrollMemory
    val freeMemory = freeHeapMemory
    // When unrolling, we will use all of the existing free memory, and, if necessary,
    // some extra space freed from evicting cached blocks. We must place a cap on the
    // amount of memory to be evicted by unrolling, however, otherwise unrolling one
    // big block can blow away the entire cache.
    val maxNumBytesToFree = math.max(0, maxUnrollMemory - currentUnrollMemory - freeMemory)
    // Keep it within the range 0 <= X <= maxNumBytesToFree
    val maxBytesToAttemptToFreeViaEviction =
      math.max(0, math.min(maxNumBytesToFree, numBytes - freeMemory))
    acquireStorageMemory(blockId, numBytes, memoryMode, maxBytesToAttemptToFreeViaEviction)
  }

}


private[spark] object StaticMemoryManager {

  /**
   * Return the total amount of memory available for the storage region, in bytes.
   */
  private def getMaxStorageMemory(conf: SparkConf): Long = {
    val systemMaxMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)
    val memoryFraction = conf.getDouble("spark.storage.memoryFraction", 0.6)
    val safetyFraction = conf.getDouble("spark.storage.safetyFraction", 0.9)
    (systemMaxMemory * memoryFraction * safetyFraction).toLong
  }

  /**
   * Return the total amount of memory available for the execution region, in bytes.
   */
  private def getMaxExecutionMemory(conf: SparkConf): Long = {
    val systemMaxMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)
    val memoryFraction = conf.getDouble("spark.shuffle.memoryFraction", 0.2)
    val safetyFraction = conf.getDouble("spark.shuffle.safetyFraction", 0.8)
    (systemMaxMemory * memoryFraction * safetyFraction).toLong
  }

}
