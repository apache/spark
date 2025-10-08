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

package org.apache.spark.sql.execution.streaming.state

import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

import org.rocksdb._

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.memory.{MemoryMode, UnifiedMemoryManager, UnmanagedMemoryConsumer, UnmanagedMemoryConsumerId}

/**
 * Singleton responsible for managing cache and write buffer manager associated with all RocksDB
 * state store instances running on a single executor if boundedMemoryUsage is enabled for RocksDB.
 * If boundedMemoryUsage is disabled, a new cache object is returned.
 * This also implements UnmanagedMemoryConsumer to report RocksDB memory usage to Spark's
 * UnifiedMemoryManager, allowing Spark to account for RocksDB memory when making
 * memory allocation decisions.
 */
object RocksDBMemoryManager extends Logging with UnmanagedMemoryConsumer {
  private var writeBufferManager: WriteBufferManager = null
  private var cache: Cache = null

  // Tracks memory usage and bounded memory mode per unique ID
  private case class InstanceMemoryInfo(memoryUsage: Long, isBoundedMemory: Boolean)
  private val instanceMemoryMap = new ConcurrentHashMap[String, InstanceMemoryInfo]()

  override def unmanagedMemoryConsumerId: UnmanagedMemoryConsumerId = {
    UnmanagedMemoryConsumerId("RocksDB", "RocksDB-Memory-Manager")
  }

  override def memoryMode: MemoryMode = {
    // RocksDB uses native/off-heap memory for its data structures
    MemoryMode.OFF_HEAP
  }

  override def getMemBytesUsed: Long = {
    val memoryInfos = instanceMemoryMap.values().asScala.toSeq
    if (memoryInfos.isEmpty) {
      return 0L
    }

    // Separate instances by bounded vs unbounded memory mode
    val (bounded, unbounded) = memoryInfos.partition(_.isBoundedMemory)

    // For bounded memory instances, they all share the same memory pool,
    // so just take the max value (they should all be similar)
    val boundedMemory = if (bounded.nonEmpty) bounded.map(_.memoryUsage).max else 0L

    // For unbounded memory instances, sum their individual usages
    val unboundedMemory = unbounded.map(_.memoryUsage).sum

    // Total is bounded memory (shared) + sum of unbounded memory (individual)
    boundedMemory + unboundedMemory
  }

  /**
   * Register/update a RocksDB instance with its memory usage.
   * @param uniqueId The instance's unique identifier
   * @param memoryUsage The current memory usage in bytes
   * @param isBoundedMemory Whether this instance uses bounded memory mode
   */
  def updateMemoryUsage(
      uniqueId: String,
      memoryUsage: Long,
      isBoundedMemory: Boolean): Unit = {
    instanceMemoryMap.put(uniqueId, InstanceMemoryInfo(memoryUsage, isBoundedMemory))
    logDebug(s"Updated memory usage for $uniqueId: $memoryUsage bytes " +
      s"(bounded=$isBoundedMemory)")
  }

  /**
   * Unregister a RocksDB instance.
   * @param uniqueId The instance's unique identifier
   */
  def unregisterInstance(uniqueId: String): Unit = {
    instanceMemoryMap.remove(uniqueId)
    logDebug(s"Unregistered instance $uniqueId")
  }

  def getNumRocksDBInstances(boundedMemory: Boolean): Long = {
    instanceMemoryMap.values().asScala.count(_.isBoundedMemory == boundedMemory)
  }

  /**
   * Get the memory usage for a specific instance, accounting for bounded memory sharing.
   * @param uniqueId The instance's unique identifier
   * @param totalMemoryUsage The total memory usage of this instance
   * @return The adjusted memory usage accounting for sharing in bounded memory mode
   */
  def getInstanceMemoryUsage(uniqueId: String, totalMemoryUsage: Long): Long = {
    val instanceInfo = instanceMemoryMap.
      getOrDefault(uniqueId, InstanceMemoryInfo(0L, isBoundedMemory = false))
    if (instanceInfo.isBoundedMemory) {
      // In bounded memory mode, divide by the number of bounded instances
      // since they share the same memory pool
      val numBoundedInstances = getNumRocksDBInstances(true)
      totalMemoryUsage / numBoundedInstances
    } else {
      // In unbounded memory mode, each instance has its own memory
      totalMemoryUsage
    }
  }

  /**
   * Get the pinned blocks memory usage for a specific instance, accounting for bounded memory
   * sharing.
   * @param uniqueId The instance's unique identifier
   * @param totalPinnedUsage The total pinned usage from the cache
   * @return The adjusted pinned blocks memory usage accounting for sharing in bounded memory mode
   */
  def getInstancePinnedBlocksMemUsage(
      uniqueId: String,
      totalPinnedUsage: Long): Long = {
    val instanceInfo = instanceMemoryMap.
      getOrDefault(uniqueId, InstanceMemoryInfo(0L, isBoundedMemory = false))
    if (instanceInfo.isBoundedMemory) {
      // In bounded memory mode, divide by the number of bounded instances
      // since they share the same cache
      val numBoundedInstances = getNumRocksDBInstances(true /* boundedMemory */)
      totalPinnedUsage / numBoundedInstances
    } else {
      // In unbounded memory mode, each instance has its own cache
      totalPinnedUsage
    }
  }

  def getOrCreateRocksDBMemoryManagerAndCache(conf: RocksDBConf): (WriteBufferManager, Cache)
    = synchronized {
    // Register with UnifiedMemoryManager (idempotent operation)
    if (SparkEnv.get != null) {
      UnifiedMemoryManager.registerUnmanagedMemoryConsumer(this)
    }

    if (conf.boundedMemoryUsage) {
      if (writeBufferManager == null) {
        assert(cache == null)

        // Check that the provided ratios don't exceed the limit
        if (conf.writeBufferCacheRatio + conf.highPriorityPoolRatio >= 1.0) {
          throw new IllegalArgumentException("Sum of writeBufferCacheRatio and " +
            "highPriorityPoolRatio should be less than 1.0")
        }

        if (conf.totalMemoryUsageMB <= 0) {
          throw new IllegalArgumentException("Total memory usage must be a positive integer")
        }

        val totalMemoryUsageInBytes: Long = conf.totalMemoryUsageMB * 1024 * 1024
        logInfo(log"Creating RocksDB state store LRU cache with " +
          log"total_size=${MDC(NUM_BYTES, totalMemoryUsageInBytes)}")

        // SPARK-44878 - avoid using strict limit to prevent insertion exception on cache full.
        // Please refer to RocksDB issue here - https://github.com/facebook/rocksdb/issues/8670
        cache = new LRUCache(totalMemoryUsageInBytes,
          -1,
          /* strictCapacityLimit = */false,
          conf.highPriorityPoolRatio)

        writeBufferManager = new WriteBufferManager(
          (totalMemoryUsageInBytes * conf.writeBufferCacheRatio).toLong,
          cache)
      }
      (writeBufferManager, cache)
    } else {
      (null, new LRUCache(conf.blockCacheSizeMB * 1024 * 1024))
    }
  }

  /** Used only for unit testing */
  def resetWriteBufferManagerAndCache: Unit = synchronized {
    writeBufferManager = null
    cache = null
    instanceMemoryMap.clear()
  }
}
