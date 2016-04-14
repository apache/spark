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
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.memory.MemoryStore


/**
 * Manages bookkeeping for a fixed-size region of memory. This class is internal to
 * the [[MemoryManager]].
 */
private[memory] class MemoryPool(
    conf: SparkConf,
    numCores: Int,
    memoryMode: MemoryMode,
    val totalMemory: Long,
    val maxExecutionMemory: Long,
    val maxStorageMemory: Long,
    val unevictableStorageMemory: Long) extends Logging {

  // Access to all of these fields is guarded by synchronizing on `this`:

  def freeMemory: Long = synchronized { _freeMemory }
  protected var _freeMemory: Long = totalMemory

  def storageMemoryUsed: Long = synchronized { _storageMemoryUsed }
  protected var _storageMemoryUsed: Long = 0L

  protected var executionMemoryUsedForTask = new mutable.HashMap[Long, Long]()

  final def executionMemoryUsed: Long = synchronized {
    executionMemoryUsedForTask.values.sum
  }

  private def assertInvariants(): Unit = {
    assert(_freeMemory >= 0)
    assert(_freeMemory <= totalMemory)
    assert(_storageMemoryUsed + executionMemoryUsed == totalMemory - _freeMemory,
      "memory was not conserved")
  }

  // Initial invariants:
  assert(maxExecutionMemory <= totalMemory)
  assert(maxStorageMemory <= totalMemory)
  assert(unevictableStorageMemory <= maxStorageMemory)
  assertInvariants()

  private var _memoryStore: MemoryStore = _
  def memoryStore: MemoryStore = {
    if (_memoryStore == null) {
      throw new IllegalStateException("memory store not initialized yet")
    }
    _memoryStore
  }

  /**
   * Set the [[MemoryStore]] used by this manager to evict cached blocks.
   * This must be set after construction due to initialization ordering constraints.
   */
  def setMemoryStore(store: MemoryStore): Unit = synchronized {
    _memoryStore = store
  }

  /**
   * Acquire N bytes of memory to cache the given block, evicting existing ones if necessary.
   *
   * @return whether all N bytes were successfully granted.
   */
  def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,
      maxBytesToAttemptToFreeViaEviction: Long): Boolean = synchronized {
    assert(numBytes >= 0)
    assertInvariants()

    if (numBytes > maxStorageMemory) {
      return false
    }

    def freeStorageMemory = math.min(_freeMemory, maxStorageMemory - _storageMemoryUsed)

    // First, attempt to fulfill as much of the request as possible using free memory
    val freeMemoryAcquired = {
      val acquired = math.min(numBytes, freeStorageMemory)
      _freeMemory -= acquired
      _storageMemoryUsed += acquired
      acquired
    }
    assertInvariants()
    // Next, acquire the remaining memory by evicting blocks
    var success: Boolean = false
    try {
      val extraMemoryNeeded = numBytes - freeMemoryAcquired
      // TODO: Once we support off-heap caching, this will need to change:
      if (extraMemoryNeeded > 0
          && maxBytesToAttemptToFreeViaEviction > 0
          && memoryMode == MemoryMode.ON_HEAP) {
        memoryStore.evictBlocksToFreeSpace(
          Some(blockId),
          math.min(extraMemoryNeeded, maxBytesToAttemptToFreeViaEviction))
      }
      // NOTE: If the memory store evicts blocks, then those evictions will synchronously call
      // back into this StorageMemoryPool in order to free memory, so the free memory counters
      // should have been updated.
      success = extraMemoryNeeded == 0 || freeStorageMemory >= extraMemoryNeeded
      if (extraMemoryNeeded > 0 && freeStorageMemory >= extraMemoryNeeded) {
        _freeMemory -= extraMemoryNeeded
        _storageMemoryUsed += extraMemoryNeeded
      }
    } finally {
      if (!success) {
        releaseStorageMemory(freeMemoryAcquired)
      }
    }
    assertInvariants()
    success
  }

  /**
   * Try to acquire up to `numBytes` of execution memory for the current task and return the
   * number of bytes obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
   *
   * Because N varies dynamically, we keep track of the set of active tasks and redo the
   * calculations of 1 / 2N and 1 / N in waiting tasks whenever this set changes. This is all don
   * by synchronizing access to mutable state and using wait() and notifyAll() to signal changes to
   * callers. Prior to Spark 1.6, this arbitration of memory across tasks was performed by the
   * ShuffleMemoryManager.
   *
   * @return the number of bytes granted to the task.
   */
  private[memory]
  def acquireExecutionMemory(numBytes: Long, taskAttemptId: Long): Long = synchronized {
    require(numBytes > 0, s"invalid number of bytes requested: $numBytes")
    assertInvariants()

    def freeExecutionMem = math.min(_freeMemory, maxExecutionMemory - executionMemoryUsed)

    // Add this task to the taskMemory map just so we can keep an accurate count of the number
    // of active tasks, to let other tasks ramp down their memory in calls to `acquireMemory`
    if (!executionMemoryUsedForTask.contains(taskAttemptId)) {
      executionMemoryUsedForTask(taskAttemptId) = 0L
      // This will later cause waiting tasks to wake up and check numTasks again
      notifyAll()
    }

    // Keep looping until we're either sure that we don't want to grant this request (because this
    // task would have more than 1 / numActiveTasks of the memory) or we have enough free
    // memory to give it (we always let each task get at least 1 / (2 * numActiveTasks)).
    // TODO: simplify this to limit each task to its own slot
    while (true) {
      val numActiveTasks = executionMemoryUsedForTask.keys.size
      val curMem = executionMemoryUsedForTask(taskAttemptId)

      // If there is not enough free memory, see whether we can reclaim some by evicting cached
      // blocks:
      val evictableStorageMemory = math.max(0, _storageMemoryUsed - unevictableStorageMemory)
      if (numBytes > freeExecutionMem && numBytes - freeExecutionMem <= evictableStorageMemory) {
        val extraMemoryNeeded = numBytes - freeExecutionMem
        val spaceFreedByEviction = {
          // Once we support off-heap caching, this will need to change:
          if (memoryMode == MemoryMode.ON_HEAP) {
            memoryStore.evictBlocksToFreeSpace(None, extraMemoryNeeded)
          } else {
            0
          }
        }
        // When a block is released, BlockManager.dropFromMemory() calls releaseMemory()
        // so we do not need to update the memory bookkeeping structures here.
        if (spaceFreedByEviction >= extraMemoryNeeded) {
          assert(numBytes <= freeExecutionMem)
        }
      }

      // Maximum amount of execution memory that can be used.
      // This is used to compute the upper bound of how much memory each task can occupy.
      val maxPoolSize = math.min(
        maxExecutionMemory, totalMemory - math.min(_storageMemoryUsed, unevictableStorageMemory))
      val maxMemoryPerTask = maxPoolSize / numActiveTasks
      val minMemoryPerTask = executionMemoryUsed / (2 * numActiveTasks)

      // How much we can grant this task; keep its share within 0 <= X <= 1 / numActiveTasks
      val maxToGrant = math.min(numBytes, math.max(0, maxMemoryPerTask - curMem))
      // Only give it as much memory as is free, which might be none if it reached 1 / numTasks
      val toGrant = math.min(maxToGrant, freeExecutionMem)

      // We want to let each task get at least 1 / (2 * numActiveTasks) before blocking;
      // if we can't give it this much now, wait for other tasks to free up memory
      // (this happens if older tasks allocated lots of memory before N grew)
      if (toGrant < numBytes && curMem + toGrant < minMemoryPerTask) {
        val poolName = memoryMode match {
          case MemoryMode.ON_HEAP => "execution memory"
          case MemoryMode.OFF_HEAP => "off-heap execution memory"
        }
        logInfo(s"TID $taskAttemptId waiting for at least 1/2N of $poolName to be free")
        wait()
      } else {
        _freeMemory -= toGrant
        executionMemoryUsedForTask(taskAttemptId) += toGrant
        assertInvariants()
        return toGrant
      }
    }
    sys.error("internal error: unreachable state")
  }

  /**
   * Release numBytes of execution memory belonging to the given task.
   */
  private[memory]
  def releaseExecutionMemory(numBytes: Long, taskAttemptId: Long): Unit = synchronized {
    assertInvariants()
    val curMem = executionMemoryUsedForTask.getOrElse(taskAttemptId, 0L)
    require(numBytes <= curMem,
      s"Release called on $numBytes bytes but task only has $curMem bytes " +
        s"of execution memory")
    if (executionMemoryUsedForTask.contains(taskAttemptId)) {
      executionMemoryUsedForTask(taskAttemptId) -= numBytes
      if (executionMemoryUsedForTask(taskAttemptId) <= 0) {
        executionMemoryUsedForTask.remove(taskAttemptId)
      }
    }
    _freeMemory += numBytes
    notifyAll() // Notify waiters in acquireExecutionMemory() that memory has been freed
    assertInvariants()
  }

  /**
   * Release all memory for the given task and mark it as inactive (e.g. when a task ends).
   *
   * @return the number of bytes freed.
   */
  private[memory] def releaseAllExecutionMemoryForTask(taskAttemptId: Long): Long = synchronized {
    assertInvariants()
    val memoryUsed = executionMemoryUsedForTask.getOrElse(taskAttemptId, 0L)
    releaseExecutionMemory(memoryUsed, taskAttemptId)
    assertInvariants()
    memoryUsed
  }

  /**
   * Release N bytes of storage memory.
   */
  def releaseStorageMemory(numBytes: Long): Unit = synchronized {
    assertInvariants()
    require(numBytes <= _storageMemoryUsed,
      s"Attempted to release $numBytes bytes of storage " +
        s"memory when we only have ${_storageMemoryUsed} bytes")
    _storageMemoryUsed -= numBytes
    _freeMemory += numBytes
    assertInvariants()
  }

  /**
   * Release all storage memory acquired.
   */
  final def releaseAllStorageMemory(): Unit = synchronized {
    assertInvariants()
    _freeMemory += _storageMemoryUsed
    _storageMemoryUsed = 0
    assertInvariants()
  }
  /**
   * Returns the execution memory consumption, in bytes, for the given task.
   */
  private[memory] def getExecutionMemoryUsageForTask(taskAttemptId: Long): Long = synchronized {
    executionMemoryUsedForTask.getOrElse(taskAttemptId, 0L)
  }
}
