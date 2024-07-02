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

package org.apache.spark.storage.memory

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.memory.{MemoryManager, MemoryMode}
import org.apache.spark.storage.BlockId

/**
 * Interfaces provided in this class will be wrapped together with external cache APIs, like
 * createEntry(), evict(), remove(), etc.
 * @param conf
 * @param memoryManager
 */
private[spark] class ExternalMemoryStore(
    conf: SparkConf,
    memoryManager: MemoryManager)
  extends Logging {

  // Note: all changes to memory allocations, notably evicting entries and
  // acquiring memory, must be synchronized on `memoryManager`!

  private[spark] def initExternalCache(conf: SparkConf): Unit = {}

  /**
   * call before API createEntry()
   * @param numBytes
   * @return whether all numBytes bytes were successfully granted.
   */
  private[spark] def acquireStorageMemory(numBytes: Long): Boolean = {
    memoryManager.acquireStorageMemory(BlockId("file_cache"), numBytes, MemoryMode.OFF_HEAP)
  }

  /**
   * work together with evictEntries() in external cache
   * @param spaceToFree
   * @return actual bytes returned by evictEntries
   */
  private[spark] def evictEntriesToFreeSpace(spaceToFree: Long): Long = {
    spaceToFree
  }

  /**
   * call after API remove entry
   * free numBytes bytes memory in StorageMemoryPool
   * @param numBytes
   */
  private[spark] def releaseStorageMemory(numBytes: Long): Unit = {
    memoryManager.releaseStorageMemory(numBytes, MemoryMode.OFF_HEAP)
  }
}

