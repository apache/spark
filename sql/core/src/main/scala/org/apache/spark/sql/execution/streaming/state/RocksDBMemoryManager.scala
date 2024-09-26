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

import org.rocksdb._

import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.MDC

/**
 * Singleton responsible for managing cache and write buffer manager associated with all RocksDB
 * state store instances running on a single executor if boundedMemoryUsage is enabled for RocksDB.
 * If boundedMemoryUsage is disabled, a new cache object is returned.
 */
object RocksDBMemoryManager extends StateStoreThreadAwareLogging {
  private var writeBufferManager: WriteBufferManager = null
  private var cache: Cache = null

  def getOrCreateRocksDBMemoryManagerAndCache(conf: RocksDBConf): (WriteBufferManager, Cache)
    = synchronized {
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
  }
}
