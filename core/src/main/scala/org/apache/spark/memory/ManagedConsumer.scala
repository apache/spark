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

/**
 * Storage-memory counterpart of [[org.apache.spark.memory.MemoryConsumer]]: holds bytes
 * acquired via [[MemoryManager.acquireStorageMemory]] and synchronously releases them on
 * Spark's request. Typical implementor: a native off-heap cache (e.g. Velox AsyncDataCache
 * via Gluten) sharing `spark.memory.offHeap.size` with Spark's MemoryStore.
 *
 * == Contract ==
 *   - [[name]] is the registry key (JVM-unique; ON_HEAP and OFF_HEAP share one namespace).
 *     Use the SAME instance for register / acquire / unregister so identity-based
 *     self-exclusion works during shrink rounds.
 *   - A component that also implements [[UnmanagedMemoryConsumer]] MUST NOT report the same
 *     bytes through both APIs -- they would be subtracted twice from `effectiveMaxMemory`.
 *   - `MemoryManager.shrinkExternal` owns storage-pool accounting: it deducts exactly
 *     `shrink`'s return value from the pool. Implementations MUST NOT call
 *     [[MemoryManager.releaseStorageMemory]] from inside [[shrink]].
 *   - [[shrink]] runs inside the `MemoryManager` monitor; it MUST NOT cycle back into
 *     `MemoryStore.{putBytes, remove, evictBlocksToFreeSpace}` (lock-order cycle on
 *     `MemoryStore.entries`) and SHOULD return within
 *     `spark.memory.managedConsumer.shrinkWarnThresholdMs` (default 100ms) to avoid
 *     blocking other acquisitions.
 *   - [[shrink]] MUST be synchronous (claimed bytes reclaimable on return). Over-release
 *     and partial release are fine; negative return is a contract violation. Exceptions
 *     are caught and treated as 0-byte release.
 */
trait ManagedConsumer {

  /**
   * Registry key and log identifier; MUST be non-empty and JVM-unique. Defaults to
   * `getClass.getSimpleName`; override for anonymous classes (where the default is "")
   * or when multiple instances of the same class may coexist.
   */
  def name: String = getClass.getSimpleName

  /** Memory mode this consumer manages; [[shrink]] is only invoked when modes match. */
  def memoryMode: MemoryMode = MemoryMode.OFF_HEAP

  /**
   * Cheap snapshot of bytes currently releasable via [[shrink]]; used to skip empty
   * consumers and order candidates largest-first. Non-negative; 0 means nothing to
   * release right now.
   */
  def getShrinkableMemoryBytes: Long

  /**
   * Synchronously release approximately `numBytes`. See class scaladoc for the full
   * contract.
   *
   * @return actual bytes released, >= 0. Framework deducts this value from the storage pool.
   */
  def shrink(numBytes: Long): Long
}
