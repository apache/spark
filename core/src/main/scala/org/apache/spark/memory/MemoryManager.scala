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

import org.apache.spark.{Logging, SparkConf, TaskContext}
import org.apache.spark.storage.{BlockId, BlockStatus, MemoryStore}
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.memory.MemoryAllocator

/**
 * An abstract memory manager that enforces how memory is shared between execution and storage.
 *
 * In this context, execution memory refers to that used for computation in shuffles, joins,
 * sorts and aggregations, while storage memory refers to that used for caching and propagating
 * internal data across the cluster. There exists one MemoryManager per JVM.
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
  @GuardedBy("this") protected var _onHeapExecutionMemoryUsed: Long = 0
  @GuardedBy("this") protected var _offHeapExecutionMemoryUsed: Long = 0
  @GuardedBy("this") protected var _storageMemoryUsed: Long = 0
  @GuardedBy("this") private[this] val onHeapExecutionMemoryArbitrator: CrossTaskMemoryArbitrator =
    new CrossTaskMemoryArbitrator(this, maxOnHeapExecutionMemory _, "on-heap execution")
  @GuardedBy("this") private[this] val offHeapExecutionMemoryArbitrator: CrossTaskMemoryArbitrator =
    new CrossTaskMemoryArbitrator(this, maxOffHeapExecutionMemory _, "off-heap execution")

  /**
   * Set the [[MemoryStore]] used by this manager to evict cached blocks.
   * This must be set after construction due to initialization ordering constraints.
   */
  final def setMemoryStore(store: MemoryStore): Unit = {
    _memoryStore = store
  }

  /**
   * Total on-heap memory available for execution, in bytes.
   */
  def maxOnHeapExecutionMemory: Long

  /**
   * Total off-heap memory available for execution, in bytes.
   */
  final def maxOffHeapExecutionMemory: Long = conf.getSizeAsBytes("spark.memory.offHeapSize", 0)

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
   * Try to acquire up to `numBytes` of on-heap execution memory for the current task and return the
   * number of bytes obtained, or 0 if none can be allocated.
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
  final def acquireOnHeapExecutionMemory(numBytes: Long, taskAttemptId: Long): Long = synchronized {
    val memoryGranted = onHeapExecutionMemoryArbitrator.acquireMemory(numBytes, taskAttemptId)
    if (memoryGranted > 0L) {
      val evictedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
      val acquired = doAcquireExecutionMemory(memoryGranted, evictedBlocks)
      assert(acquired <= memoryGranted)
      onHeapExecutionMemoryArbitrator.releaseMemory(memoryGranted - acquired, taskAttemptId)
      // Register evicted blocks, if any, with the active task metrics
      Option(TaskContext.get()).foreach { tc =>
        val metrics = tc.taskMetrics()
        val lastUpdatedBlocks = metrics.updatedBlocks.getOrElse(Seq[(BlockId, BlockStatus)]())
        metrics.updatedBlocks = Some(lastUpdatedBlocks ++ evictedBlocks.toSeq)
      }
      acquired
    } else {
      0L
    }
  }

  /**
   * Try to acquire up to `numBytes` of off-heap execution memory for the current task and return
   * the number of bytes obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
   */
  private[memory]
  def acquireOffHeapExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long): Long = synchronized {
    val memoryGranted = offHeapExecutionMemoryArbitrator.acquireMemory(numBytes, taskAttemptId)
    _offHeapExecutionMemoryUsed += memoryGranted
    memoryGranted
  }

  @VisibleForTesting
  private[memory] def releaseOnHeapExecutionMemory(numBytes: Long): Unit = synchronized {
    if (numBytes > _onHeapExecutionMemoryUsed) {
      logWarning(s"Attempted to release $numBytes bytes of on-heap execution " +
        s"memory when we only have ${_onHeapExecutionMemoryUsed} bytes")
      _onHeapExecutionMemoryUsed = 0
    } else {
      _onHeapExecutionMemoryUsed -= numBytes
    }
  }

  @VisibleForTesting
  private[memory] def releaseOffHeapExecutionMemory(numBytes: Long): Unit = synchronized {
    if (numBytes > _offHeapExecutionMemoryUsed) {
      logWarning(s"Attempted to release $numBytes bytes of off-heap execution " +
        s"memory when we only have ${_offHeapExecutionMemoryUsed} bytes")
      _offHeapExecutionMemoryUsed = 0
    } else {
      _offHeapExecutionMemoryUsed -= numBytes
    }
  }

  /**
   * Release numBytes of on-heap execution memory belonging to the given task.
   */
  private[memory]
  final def releaseOnHeapExecutionMemory(numBytes: Long, taskAttemptId: Long): Unit = synchronized {
    onHeapExecutionMemoryArbitrator.releaseMemory(numBytes, taskAttemptId)
    releaseOnHeapExecutionMemory(numBytes)
  }

  /**
   * Release numBytes of off-heap execution memory belonging to the given task.
   */
  private[memory]
  final def releaseOffHeapExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long): Unit = synchronized {
    offHeapExecutionMemoryArbitrator.releaseMemory(numBytes, taskAttemptId)
    releaseOffHeapExecutionMemory(numBytes)
  }

  /**
   * Release all memory for the given task and mark it as inactive (e.g. when a task ends).
   * @return the number of bytes freed.
   */
  private[memory] def releaseAllExecutionMemoryForTask(taskAttemptId: Long): Long = synchronized {
    val freedOnHeap = onHeapExecutionMemoryArbitrator.releaseAllMemoryForTask(taskAttemptId)
    releaseOnHeapExecutionMemory(freedOnHeap)
    val freedOffHeap = offHeapExecutionMemoryArbitrator.releaseAllMemoryForTask(taskAttemptId)
    releaseOffHeapExecutionMemory(freedOffHeap)
    freedOnHeap + freedOffHeap
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
    _onHeapExecutionMemoryUsed
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
    onHeapExecutionMemoryArbitrator.getMemoryUsageForTask(taskAttemptId)
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
    val size = ByteArrayMethods.nextPowerOf2(maxOnHeapExecutionMemory / cores / safetyFactor)
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
