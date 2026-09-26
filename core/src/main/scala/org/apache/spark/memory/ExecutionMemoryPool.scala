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

import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._

/**
 * Implements policies and bookkeeping for sharing an adjustable-sized pool of memory between tasks.
 *
 * Tries to ensure that each task gets a reasonable share of memory, instead of some task ramping up
 * to a large amount first and then causing others to spill to disk repeatedly.
 *
 * If there are N tasks, it ensures that each task can acquire at least 1 / 2N of the memory
 * before it has to spill, and at most 1 / N. Because N varies dynamically, we keep track of the
 * set of active tasks and redo the calculations of 1 / 2N and 1 / N in waiting tasks whenever this
 * set changes. This is all done by synchronizing access to mutable state and using wait() and
 * notifyAll() to signal changes to callers. Prior to Spark 1.6, this arbitration of memory across
 * tasks was performed by the ShuffleMemoryManager.
 *
 * @param lock a [[MemoryManager]] instance to synchronize on
 * @param memoryMode the type of memory tracked by this pool (on- or off-heap)
 */
private[memory] class ExecutionMemoryPool(
    lock: Object,
    memoryMode: MemoryMode
  ) extends MemoryPool(lock) with Logging {

  private[this] val poolName: String = memoryMode match {
    case MemoryMode.ON_HEAP => "on-heap execution"
    case MemoryMode.OFF_HEAP => "off-heap execution"
  }

  /**
   * Ordinary execution memory by task. Optional-only owners do not enter ordinary fair sharing.
   */
  @GuardedBy("lock")
  private val memoryForTask = new mutable.HashMap[Long, Long]()

  @GuardedBy("lock")
  private var optionalMemoryForTask: mutable.LongMap[Long] = null

  @GuardedBy("lock")
  private var optionalBytes = 0L

  def optionalMemoryUsed: Long = lock.synchronized { optionalBytes }

  def ordinaryMemoryUsed: Long = lock.synchronized { memoryForTask.values.sum }

  def getOptionalMemoryUsageForTask(taskAttemptId: Long): Long = lock.synchronized {
    if (optionalMemoryForTask == null) 0L else optionalMemoryForTask.getOrElse(taskAttemptId, 0L)
  }

  override def memoryUsed: Long = lock.synchronized {
    memoryForTask.values.sum + optionalBytes
  }

  /**
   * Returns the memory consumption, in bytes, for the given task.
   */
  def getMemoryUsageForTask(taskAttemptId: Long): Long = lock.synchronized {
    memoryForTask.getOrElse(taskAttemptId, 0L) + getOptionalMemoryUsageForTask(taskAttemptId)
  }

  /** Remaining ordinary share, without changing task participation. */
  private[memory] def absoluteMemoryHeadroom(taskAttemptId: Long, maxMemory: Long): Long = {
    assert(Thread.holdsLock(lock))
    val current = memoryForTask.getOrElse(taskAttemptId, -1L)
    if (current >= 0L) {
      math.max(0L, maxMemory / memoryForTask.size - current)
    } else {
      val share = maxMemory / (memoryForTask.size.toLong + 1L)
      // Ordinary admission must still register a new task when its prospective share is zero.
      if (share == 0L) -1L else share
    }
  }

  /** Check the minimum-share wait using the prospective pool size after borrowing free storage. */
  private[memory] def canGrantWithoutWaiting(
      taskAttemptId: Long,
      requested: Long,
      granted: Long,
      poolSizeAfterGrowth: Long): Boolean = {
    assert(Thread.holdsLock(lock))
    val tasks = memoryForTask.size + (if (memoryForTask.contains(taskAttemptId)) 0 else 1)
    val current = memoryForTask.getOrElse(taskAttemptId, 0L)
    granted == requested || current + granted >= poolSizeAfterGrowth / (2L * tasks)
  }

  /**
   * Check a prospective ordinary request without registering a task or changing its charge.
   * `availableMemory` includes free storage the caller can borrow without eviction; `maxPoolSize`
   * uses the same potential fair-share ceiling as ordinary acquisition. False asks the caller to
   * drain optional owners before any grant, eviction, or capacity wait.
   */
  private[memory] def canAcquireMemory(
      numBytes: Long,
      taskAttemptId: Long,
      maxPoolSize: Long,
      availableMemory: Long): Boolean = lock.synchronized {
    val tasks = memoryForTask.size + (if (memoryForTask.contains(taskAttemptId)) 0 else 1)
    val current = memoryForTask.getOrElse(taskAttemptId, 0L)
    numBytes <= availableMemory && numBytes <= math.max(0L, maxPoolSize / tasks - current)
  }

  /**
   * Reserve all `numBytes` for optional work, or return zero without changing this pool.
   *
   * Only currently free execution memory is eligible. Optional admission retains a conservative
   * ceiling counting ordinary and optional-only tasks and both kinds of bytes for its owner.
   * Optional bytes consume physical capacity, but never reduce ordinary fair shares.
   * This method never waits for capacity, grows the pool, or evicts memory. Acquiring the existing
   * bookkeeping monitor can still block. Successful reservations require optional release.
   */
  private[memory] def tryAcquireMemory(
      numBytes: Long,
      taskAttemptId: Long,
      maxPoolSize: Long,
      availableMemory: Long = Long.MaxValue): Long = lock.synchronized {
    require(numBytes >= 0, s"invalid number of bytes requested: $numBytes")
    if (numBytes == 0) {
      return 0L
    }
    val currentOptional = getOptionalMemoryUsageForTask(taskAttemptId)
    val optionalOnlyTasks = if (optionalMemoryForTask == null) 0 else {
      optionalMemoryForTask.keysIterator.count(task => !memoryForTask.contains(task))
    }
    val isNewTask = !memoryForTask.contains(taskAttemptId) && currentOptional == 0L
    val numActiveTasks = memoryForTask.size + optionalOnlyTasks + (if (isNewTask) 1 else 0)
    val currentMemory = memoryForTask.getOrElse(taskAttemptId, 0L) + currentOptional
    val remainingShare = math.max(0L, maxPoolSize / numActiveTasks - currentMemory)
    if (numBytes > math.min(memoryFree, availableMemory) || numBytes > remainingShare) {
      0L
    } else {
      if (optionalMemoryForTask == null) {
        optionalMemoryForTask = new mutable.LongMap[Long]()
      }
      optionalMemoryForTask(taskAttemptId) = currentOptional + numBytes
      optionalBytes += numBytes
      numBytes
    }
  }

  /**
   * Try to acquire up to `numBytes` of memory for the given task and return the number of bytes
   * obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
   *
   * @param numBytes number of bytes to acquire
   * @param taskAttemptId the task attempt acquiring memory
   * @param maybeGrowPool a callback that potentially grows the size of this pool. It takes in
   *                      one parameter (Long) that represents the desired amount of memory by
   *                      which this pool should be expanded.
   * @param computeMaxPoolSize a callback that returns the maximum allowable size of this pool
   *                           at this given moment. This is not a field because the max pool
   *                           size is variable in certain cases. For instance, in unified
   *                           memory management, the execution pool can be expanded by evicting
   *                           cached blocks, thereby shrinking the storage pool.
   * @param computeMemoryFree an optional physical-capacity limit after unmanaged usage, or null
   *                          to use the pool free space directly
   *
   * @return the number of bytes granted to the task.
   */
  private[memory] def acquireMemory(
      numBytes: Long,
      taskAttemptId: Long,
      maybeGrowPool: Long => Unit = (additionalSpaceNeeded: Long) => (),
      computeMaxPoolSize: () => Long = () => poolSize,
      computeMemoryFree: () => Long = null): Long = lock.synchronized {
    assert(numBytes > 0, s"invalid number of bytes requested: $numBytes")

    // TODO: clean up this clunky method signature

    // Add this task to the taskMemory map just so we can keep an accurate count of the number
    // of active tasks, to let other tasks ramp down their memory in calls to `acquireMemory`
    if (!memoryForTask.contains(taskAttemptId)) {
      memoryForTask(taskAttemptId) = 0L
      // This will later cause waiting tasks to wake up and check numTasks again
      lock.notifyAll()
    }

    // Keep looping until we're either sure that we don't want to grant this request (because this
    // task would have more than 1 / numActiveTasks of the memory) or we have enough free
    // memory to give it (we always let each task get at least 1 / (2 * numActiveTasks)).
    // TODO: simplify this to limit each task to its own slot
    while (true) {
      val numActiveTasks = memoryForTask.keys.size
      val curMem = memoryForTask(taskAttemptId)

      // In every iteration of this loop, we should first try to reclaim any borrowed execution
      // space from storage. This is necessary because of the potential race condition where new
      // storage blocks may steal the free execution memory that this task was waiting for.
      maybeGrowPool(numBytes - memoryFree)

      // Maximum size the pool would have after potentially growing the pool.
      // This is used to compute the upper bound of how much memory each task can occupy. This
      // must take into account potential free memory as well as the amount this pool currently
      // occupies. Otherwise, we may run into SPARK-12155 where, in unified memory management,
      // we did not take into account space that could have been freed by evicting cached blocks.
      val maxPoolSize = computeMaxPoolSize()
      val maxMemoryPerTask = maxPoolSize / numActiveTasks
      val minMemoryPerTask = poolSize / (2 * numActiveTasks)

      // How much we can grant this task; keep its share within 0 <= X <= 1 / numActiveTasks
      val maxToGrant = math.min(numBytes, math.max(0, maxMemoryPerTask - curMem))
      // Only give it as much memory as is free, which might be none if it reached 1 / numTasks
      val available = if (computeMemoryFree == null) memoryFree else computeMemoryFree()
      val toGrant = math.min(maxToGrant, available)

      // We want to let each task get at least 1 / (2 * numActiveTasks) before blocking;
      // if we can't give it this much now, wait for other tasks to free up memory
      // (this happens if older tasks allocated lots of memory before N grew)
      if (toGrant < numBytes && curMem + toGrant < minMemoryPerTask) {
        logInfo(log"TID ${MDC(TASK_ATTEMPT_ID, taskAttemptId)} waiting for at least 1/2N of" +
          log" ${MDC(POOL_NAME, poolName)} pool to be free")
        lock.wait()
      } else {
        memoryForTask(taskAttemptId) += toGrant
        return toGrant
      }
    }
    0L  // Never reached
  }

  /**
   * Release `numBytes` of memory acquired by the given task.
   */
  def releaseMemory(numBytes: Long, taskAttemptId: Long): Unit = lock.synchronized {
    val curMem = memoryForTask.getOrElse(taskAttemptId, 0L)
    val memoryToFree = if (curMem < numBytes) {
      logWarning(
        log"Internal error: release called on ${MDC(NUM_BYTES, numBytes)} " +
          log"bytes but task only has ${MDC(CURRENT_MEMORY_SIZE, curMem)} bytes " +
          log"of memory from the ${MDC(MEMORY_POOL_NAME, poolName)} pool")
      curMem
    } else {
      numBytes
    }
    if (memoryForTask.contains(taskAttemptId)) {
      memoryForTask(taskAttemptId) -= memoryToFree
      if (memoryForTask(taskAttemptId) <= 0) {
        memoryForTask.remove(taskAttemptId)
      }
    }
    lock.notifyAll() // Notify waiters in acquireMemory() that memory has been freed
  }

  /**
   * Release all memory for the given task and mark it as inactive (e.g. when a task ends).
   * @return the number of bytes freed.
   */
  def releaseAllMemoryForTask(taskAttemptId: Long): Long = lock.synchronized {
    val ordinary = releaseAllOrdinaryMemoryForTask(taskAttemptId)
    val optional = getOptionalMemoryUsageForTask(taskAttemptId)
    releaseOptionalMemory(optional, taskAttemptId)
    ordinary + optional
  }

  def releaseAllOrdinaryMemoryForTask(taskAttemptId: Long): Long = lock.synchronized {
    val numBytes = memoryForTask.getOrElse(taskAttemptId, 0L)
    releaseMemory(numBytes, taskAttemptId)
    numBytes
  }

  /** Ordinary release must never consume optional credit, or vice versa. */
  def releaseOptionalMemory(numBytes: Long, taskAttemptId: Long): Unit = lock.synchronized {
    val current = getOptionalMemoryUsageForTask(taskAttemptId)
    assert(numBytes >= 0L && numBytes <= current,
      s"invalid optional release: $numBytes bytes from task $taskAttemptId holding $current")
    if (numBytes > 0L) {
      if (numBytes == current) optionalMemoryForTask.remove(taskAttemptId)
      else optionalMemoryForTask(taskAttemptId) = current - numBytes
      optionalBytes -= numBytes
      lock.notifyAll()
    }
  }

}
