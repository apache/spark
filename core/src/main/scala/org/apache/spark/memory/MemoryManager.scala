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
import scala.collection.mutable.ArrayBuffer

import com.google.common.annotations.VisibleForTesting

import org.apache.spark.util.Utils
import org.apache.spark.{SparkException, TaskContext, SparkConf, Logging}
import org.apache.spark.storage.{BlockId, BlockStatus, MemoryStore}
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.memory.MemoryAllocator

/**
 * An abstract memory manager that enforces how memory is shared between execution and storage.
 *
 * In this context, execution memory refers to that used for computation in shuffles, joins,
 * sorts and aggregations, while storage memory refers to that used for caching and propagating
 * internal data across the cluster. There exists one MemoryManager per JVM.
 *
 * The MemoryManager abstract base class itself implements policies for sharing execution memory
 * between tasks; it tries to ensure that each task gets a reasonable share of memory, instead of
 * some task ramping up to a large amount first and then causing others to spill to disk repeatedly.
 * If there are N tasks, it ensures that each task can acquire at least 1 / 2N of the memory
 * before it has to spill, and at most 1 / N. Because N varies dynamically, we keep track of the
 * set of active tasks and redo the calculations of 1 / 2N and 1 / N in waiting tasks whenever
 * this set changes. This is all done by synchronizing access to mutable state and using wait() and
 * notifyAll() to signal changes to callers. Prior to Spark 1.6, this arbitration of memory across
 * tasks was performed by the ShuffleMemoryManager.
 */
private[spark] abstract class MemoryManager(conf: SparkConf, numCores: Int) extends Logging {

  // -- Methods related to memory allocation policies and bookkeeping ------------------------------

  // The memory store used to evict cached blocks
  private var _memoryStore: MemoryStore = _
  protected def memoryStore: MemoryStore = {
    if (_memoryStore == null) {
      throw new IllegalArgumentException("memory store not initialized yet")
    }
    _memoryStore
  }

  // Amount of execution/storage memory in use, accesses must be synchronized on `this`
  @GuardedBy("this") protected var _executionMemoryUsed: Long = 0
  @GuardedBy("this") protected var _storageMemoryUsed: Long = 0
  // Map from taskAttemptId -> memory consumption in bytes
  @GuardedBy("this") private val executionMemoryForTask = new mutable.HashMap[Long, Long]()

  /**
   * Set the [[MemoryStore]] used by this manager to evict cached blocks.
   * This must be set after construction due to initialization ordering constraints.
   */
  final def setMemoryStore(store: MemoryStore): Unit = {
    _memoryStore = store
  }

  /**
   * Total available memory for execution, in bytes.
   */
  def maxExecutionMemory: Long

  /**
   * Total available memory for storage, in bytes.
   */
  def maxStorageMemory: Long

  // TODO: avoid passing evicted blocks around to simplify method signatures (SPARK-10985)

  /**
   * Acquire N bytes of memory to cache the given block, evicting existing ones if necessary.
   * Blocks evicted in the process, if any, are added to `evictedBlocks`.
   * @return whether all N bytes were successfully granted.
   */
  def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean

  /**
   * Acquire N bytes of memory to unroll the given block, evicting existing ones if necessary.
   *
   * This extra method allows subclasses to differentiate behavior between acquiring storage
   * memory and acquiring unroll memory. For instance, the memory management model in Spark
   * 1.5 and before places a limit on the amount of space that can be freed from unrolling.
   * Blocks evicted in the process, if any, are added to `evictedBlocks`.
   *
   * @return whether all N bytes were successfully granted.
   */
  def acquireUnrollMemory(
      blockId: BlockId,
      numBytes: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean = synchronized {
    acquireStorageMemory(blockId, numBytes, evictedBlocks)
  }

  /**
   * Acquire N bytes of memory for execution, evicting cached blocks if necessary.
   * Blocks evicted in the process, if any, are added to `evictedBlocks`.
   * @return number of bytes successfully granted (<= N).
   */
  @VisibleForTesting
  private[memory] def doAcquireExecutionMemory(
      numBytes: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Long

  /**
   * Try to acquire up to `numBytes` of execution memory for the current task and return the number
   * of bytes obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
   *
   * Subclasses should override `doAcquireExecutionMemory` in order to customize the policies
   * that control global sharing of memory between execution and storage.
   */
  private[memory]
  final def acquireExecutionMemory(numBytes: Long, taskAttemptId: Long): Long = synchronized {
    assert(numBytes > 0, "invalid number of bytes requested: " + numBytes)

    // Add this task to the taskMemory map just so we can keep an accurate count of the number
    // of active tasks, to let other tasks ramp down their memory in calls to tryToAcquire
    if (!executionMemoryForTask.contains(taskAttemptId)) {
      executionMemoryForTask(taskAttemptId) = 0L
      // This will later cause waiting tasks to wake up and check numTasks again
      notifyAll()
    }

    // Once the cross-task memory allocation policy has decided to grant more memory to a task,
    // this method is called in order to actually obtain that execution memory, potentially
    // triggering eviction of storage memory:
    def acquire(toGrant: Long): Long = synchronized {
      val evictedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
      val acquired = doAcquireExecutionMemory(toGrant, evictedBlocks)
      // Register evicted blocks, if any, with the active task metrics
      Option(TaskContext.get()).foreach { tc =>
        val metrics = tc.taskMetrics()
        val lastUpdatedBlocks = metrics.updatedBlocks.getOrElse(Seq[(BlockId, BlockStatus)]())
        metrics.updatedBlocks = Some(lastUpdatedBlocks ++ evictedBlocks.toSeq)
      }
      executionMemoryForTask(taskAttemptId) += acquired
      acquired
    }

    // Keep looping until we're either sure that we don't want to grant this request (because this
    // task would have more than 1 / numActiveTasks of the memory) or we have enough free
    // memory to give it (we always let each task get at least 1 / (2 * numActiveTasks)).
    // TODO: simplify this to limit each task to its own slot
    while (true) {
      val numActiveTasks = executionMemoryForTask.keys.size
      val curMem = executionMemoryForTask(taskAttemptId)
      val freeMemory = maxExecutionMemory - executionMemoryForTask.values.sum

      // How much we can grant this task; don't let it grow to more than 1 / numActiveTasks;
      // don't let it be negative
      val maxToGrant =
        math.min(numBytes, math.max(0, (maxExecutionMemory / numActiveTasks) - curMem))
      // Only give it as much memory as is free, which might be none if it reached 1 / numTasks
      val toGrant = math.min(maxToGrant, freeMemory)

      if (curMem < maxExecutionMemory / (2 * numActiveTasks)) {
        // We want to let each task get at least 1 / (2 * numActiveTasks) before blocking;
        // if we can't give it this much now, wait for other tasks to free up memory
        // (this happens if older tasks allocated lots of memory before N grew)
        if (
          freeMemory >= math.min(maxToGrant, maxExecutionMemory / (2 * numActiveTasks) - curMem)) {
          return acquire(toGrant)
        } else {
          logInfo(
            s"TID $taskAttemptId waiting for at least 1/2N of execution memory pool to be free")
          wait()
        }
      } else {
        return acquire(toGrant)
      }
    }
    0L  // Never reached
  }

  @VisibleForTesting
  private[memory] def releaseExecutionMemory(numBytes: Long): Unit = synchronized {
    if (numBytes > _executionMemoryUsed) {
      logWarning(s"Attempted to release $numBytes bytes of execution " +
        s"memory when we only have ${_executionMemoryUsed} bytes")
      _executionMemoryUsed = 0
    } else {
      _executionMemoryUsed -= numBytes
    }
  }

  /**
   * Release numBytes of execution memory belonging to the given task.
   */
  private[memory]
  final def releaseExecutionMemory(numBytes: Long, taskAttemptId: Long): Unit = synchronized {
    val curMem = executionMemoryForTask.getOrElse(taskAttemptId, 0L)
    if (curMem < numBytes) {
      if (Utils.isTesting) {
        throw new SparkException(
          s"Internal error: release called on $numBytes bytes but task only has $curMem")
      } else {
        logWarning(s"Internal error: release called on $numBytes bytes but task only has $curMem")
      }
    }
    if (executionMemoryForTask.contains(taskAttemptId)) {
      executionMemoryForTask(taskAttemptId) -= numBytes
      if (executionMemoryForTask(taskAttemptId) <= 0) {
        executionMemoryForTask.remove(taskAttemptId)
      }
      releaseExecutionMemory(numBytes)
    }
    notifyAll() // Notify waiters in acquireExecutionMemory() that memory has been freed
  }

  /**
   * Release all memory for the given task and mark it as inactive (e.g. when a task ends).
   * @return the number of bytes freed.
   */
  private[memory] def releaseAllExecutionMemoryForTask(taskAttemptId: Long): Long = synchronized {
    val numBytesToFree = getExecutionMemoryUsageForTask(taskAttemptId)
    releaseExecutionMemory(numBytesToFree, taskAttemptId)
    numBytesToFree
  }

  /**
   * Release N bytes of storage memory.
   */
  def releaseStorageMemory(numBytes: Long): Unit = synchronized {
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
  def releaseAllStorageMemory(): Unit = synchronized {
    _storageMemoryUsed = 0
  }

  /**
   * Release N bytes of unroll memory.
   */
  def releaseUnrollMemory(numBytes: Long): Unit = synchronized {
    releaseStorageMemory(numBytes)
  }

  /**
   * Execution memory currently in use, in bytes.
   */
  final def executionMemoryUsed: Long = synchronized {
    _executionMemoryUsed
  }

  /**
   * Storage memory currently in use, in bytes.
   */
  final def storageMemoryUsed: Long = synchronized {
    _storageMemoryUsed
  }

  /**
   * Returns the execution memory consumption, in bytes, for the given task.
   */
  private[memory] def getExecutionMemoryUsageForTask(taskAttemptId: Long): Long = synchronized {
    executionMemoryForTask.getOrElse(taskAttemptId, 0L)
  }

  // -- Fields related to Tungsten managed memory -------------------------------------------------

  /**
   * The default page size, in bytes.
   *
   * If user didn't explicitly set "spark.buffer.pageSize", we figure out the default value
   * by looking at the number of cores available to the process, and the total amount of memory,
   * and then divide it by a factor of safety.
   */
  val pageSizeBytes: Long = {
    val minPageSize = 1L * 1024 * 1024   // 1MB
    val maxPageSize = 64L * minPageSize  // 64MB
    val cores = if (numCores > 0) numCores else Runtime.getRuntime.availableProcessors()
    // Because of rounding to next power of 2, we may have safetyFactor as 8 in worst case
    val safetyFactor = 16
    val size = ByteArrayMethods.nextPowerOf2(maxExecutionMemory / cores / safetyFactor)
    val default = math.min(maxPageSize, math.max(minPageSize, size))
    conf.getSizeAsBytes("spark.buffer.pageSize", default)
  }

  /**
   * Tracks whether Tungsten memory will be allocated on the JVM heap or off-heap using
   * sun.misc.Unsafe.
   */
  final val tungstenMemoryIsAllocatedInHeap: Boolean =
    !conf.getBoolean("spark.unsafe.offHeap", false)

  /**
   * Allocates memory for use by Unsafe/Tungsten code.
   */
  private[memory] final val tungstenMemoryAllocator: MemoryAllocator =
    if (tungstenMemoryIsAllocatedInHeap) MemoryAllocator.HEAP else MemoryAllocator.UNSAFE
}
