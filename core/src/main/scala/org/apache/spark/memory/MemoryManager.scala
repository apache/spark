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

import java.lang.ref.WeakReference
import java.util
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkException, TaskContext, SparkConf, Logging}
import org.apache.spark.storage.{BlockId, BlockStatus, MemoryStore}
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.memory.{MemoryAllocator, MemoryBlock}

/**
 * An abstract memory manager that enforces how memory is shared between execution and storage.
 *
 * In this context, execution memory refers to that used for computation in shuffles, joins,
 * sorts and aggregations, while storage memory refers to that used for caching and propagating
 * internal data across the cluster. There exists one of these per JVM.
 */
// TODO(josh) pass in numCores
private[spark] abstract class MemoryManager(conf: SparkConf, numCores: Int = 1) extends Logging {

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
   * Acquire N bytes of memory for execution, evicting cached blocks if necessary.
   * Blocks evicted in the process, if any, are added to `evictedBlocks`.
   * @return number of bytes successfully granted (<= N).
   */
  def acquireExecutionMemory(
      numBytes: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Long

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
   * Release N bytes of execution memory.
   */
  def releaseExecutionMemory(numBytes: Long): Unit = synchronized {
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

  // -- The code formerly known as ShuffleMemoryManager --------------------------------------------

  /*
   * Allocates a pool of memory to tasks for use in shuffle operations. Each disk-spilling
   * collection (ExternalAppendOnlyMap or ExternalSorter) used by these tasks can acquire memory
   * from this pool and release it as it spills data out. When a task ends, all its memory will be
   * released by the Executor.
   *
   * This class tries to ensure that each task gets a reasonable share of memory, instead of some
   * task ramping up to a large amount first and then causing others to spill to disk repeatedly.
   * If there are N tasks, it ensures that each tasks can acquire at least 1 / 2N of the memory
   * before it has to spill, and at most 1 / N. Because N varies dynamically, we keep track of the
   * set of active tasks and redo the calculations of 1 / 2N and 1 / N in waiting tasks whenever
   * this set changes. This is all done by synchronizing access to `memoryManager` to mutate state
   * and using wait() and notifyAll() to signal changes.
   */

  /**
   * Sets the page size, in bytes.
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


  private val taskMemory = new mutable.HashMap[Long, Long]()  // taskAttemptId -> memory bytes

  private def currentTaskAttemptId(): Long = {
    // In case this is called on the driver, return an invalid task attempt id.
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(-1L)
  }

  /**
   * Try to acquire up to numBytes memory for the current task, and return the number of bytes
   * obtained, or 0 if none can be allocated. This call may block until there is enough free memory
   * in some situations, to make sure each task has a chance to ramp up to at least 1 / 2N of the
   * total memory pool (where N is the # of active tasks) before it is forced to spill. This can
   * happen if the number of tasks increases but an older task had a lot of memory already.
   */
  def tryToAcquire(numBytes: Long): Long = synchronized {
    val taskAttemptId = currentTaskAttemptId()
    assert(numBytes > 0, "invalid number of bytes requested: " + numBytes)

    // Add this task to the taskMemory map just so we can keep an accurate count of the number
    // of active tasks, to let other tasks ramp down their memory in calls to tryToAcquire
    if (!taskMemory.contains(taskAttemptId)) {
      taskMemory(taskAttemptId) = 0L
      // This will later cause waiting tasks to wake up and check numTasks again
      notifyAll()
    }

    // Keep looping until we're either sure that we don't want to grant this request (because this
    // task would have more than 1 / numActiveTasks of the memory) or we have enough free
    // memory to give it (we always let each task get at least 1 / (2 * numActiveTasks)).
    // TODO: simplify this to limit each task to its own slot
    while (true) {
      val numActiveTasks = taskMemory.keys.size
      val curMem = taskMemory(taskAttemptId)
      val freeMemory = maxExecutionMemory - taskMemory.values.sum

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
            s"TID $taskAttemptId waiting for at least 1/2N of shuffle memory pool to be free")
          wait()
        }
      } else {
        return acquire(toGrant)
      }
    }
    0L  // Never reached
  }

  /**
   * Acquire N bytes of execution memory from the memory manager for the current task.
   * @return number of bytes actually acquired (<= N).
   */
  private def acquire(numBytes: Long): Long = synchronized {
    val taskAttemptId = currentTaskAttemptId()
    val evictedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
    val acquired = acquireExecutionMemory(numBytes, evictedBlocks)
    // Register evicted blocks, if any, with the active task metrics
    // TODO: just do this in `acquireExecutionMemory` (SPARK-10985)
    Option(TaskContext.get()).foreach { tc =>
      val metrics = tc.taskMetrics()
      val lastUpdatedBlocks = metrics.updatedBlocks.getOrElse(Seq[(BlockId, BlockStatus)]())
      metrics.updatedBlocks = Some(lastUpdatedBlocks ++ evictedBlocks.toSeq)
    }
    taskMemory(taskAttemptId) += acquired
    acquired
  }

  /** Release numBytes bytes for the current task. */
  def release(numBytes: Long): Unit = synchronized {
    val taskAttemptId = currentTaskAttemptId()
    val curMem = taskMemory.getOrElse(taskAttemptId, 0L)
    if (curMem < numBytes) {
      throw new SparkException(
        s"Internal error: release called on $numBytes bytes but task only has $curMem")
    }
    if (taskMemory.contains(taskAttemptId)) {
      taskMemory(taskAttemptId) -= numBytes
      releaseExecutionMemory(numBytes)
    }
    notifyAll() // Notify waiters in tryToAcquire that memory has been freed
  }

  /** Release all memory for the current task and mark it as inactive (e.g. when a task ends). */
  private[memory] def releaseMemoryForThisTask(): Unit = synchronized {
    val taskAttemptId = currentTaskAttemptId()
    taskMemory.remove(taskAttemptId).foreach { numBytes =>
      releaseExecutionMemory(numBytes)
    }
    notifyAll() // Notify waiters in tryToAcquire that memory has been freed
  }

  /** Returns the memory consumption, in bytes, for the current task */
  private[memory] def getMemoryConsumptionForThisTask(): Long = synchronized {
    val taskAttemptId = currentTaskAttemptId()
    taskMemory.getOrElse(taskAttemptId, 0L)
  }

  // -- Methods related to Tungsten managed memory -------------------------------------------------

  /**
   * Tracks whether Tungsten memory will be allocated on the JVM heap or off-heap using
   * sun.misc.Unsafe.
   */
  final val tungstenMemoryIsAllocatedInHeap: Boolean =
    !conf.getBoolean("spark.unsafe.offHeap", false)

  /**
   * Allocates memory for use by Unsafe/Tungsten code. Exposed to enable untracked allocations of
   * temporary data structures.
   */
  final val tungstenMemoryAllocator: MemoryAllocator =
    if (tungstenMemoryIsAllocatedInHeap) MemoryAllocator.HEAP else MemoryAllocator.UNSAFE

  private val POOLING_THRESHOLD_BYTES: Int = 1024 * 1024

  /**
   * Returns true if allocations of the given size should go through the pooling mechanism and
   * false otherwise.
   */
  private def shouldPool(size: Long): Boolean = {
    // Very small allocations are less likely to benefit from pooling.
    // At some point, we should explore supporting pooling for off-heap memory, but for now we'll
    // ignore that case in the interest of simplicity.
    size >= POOLING_THRESHOLD_BYTES && tungstenMemoryIsAllocatedInHeap
  }

  @GuardedBy("this")
  private val bufferPoolsBySize: util.Map[Long, util.LinkedList[WeakReference[MemoryBlock]]] =
    new util.HashMap[Long, util.LinkedList[WeakReference[MemoryBlock]]]

  /**
   * Allocates a contiguous block of memory. Note that the allocated memory is not guaranteed
   * to be zeroed out (call `zero()` on the result if this is necessary).
   */
  @throws(classOf[OutOfMemoryError])
  final def allocateMemoryBlock(size: Long): MemoryBlock = {
    // TODO(josh): Integrate with execution memory management
    if (shouldPool(size)) {
      this synchronized {
        val pool: util.LinkedList[WeakReference[MemoryBlock]] = bufferPoolsBySize.get(size)
        if (pool != null) {
          while (!pool.isEmpty) {
            val blockReference: WeakReference[MemoryBlock] = pool.pop
            val memory: MemoryBlock = blockReference.get
            if (memory != null) {
              assert(memory.size == size)
              return memory
            }
          }
          bufferPoolsBySize.remove(size)
        }
      }
      tungstenMemoryAllocator.allocate(size)
    } else {
      tungstenMemoryAllocator.allocate(size)
    }
  }

  final def freeMemoryBlock(memory: MemoryBlock) {
    // TODO(josh): Integrate with execution memory management
    val size: Long = memory.size
    if (shouldPool(size)) {
      this synchronized {
        var pool: util.LinkedList[WeakReference[MemoryBlock]] = bufferPoolsBySize.get(size)
        if (pool == null) {
          pool = new util.LinkedList[WeakReference[MemoryBlock]]
          bufferPoolsBySize.put(size, pool)
        }
        pool.add(new WeakReference[MemoryBlock](memory))
      }
    } else {
      tungstenMemoryAllocator.free(memory)
    }
  }
}
