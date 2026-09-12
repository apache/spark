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

import java.util.concurrent.locks.ReentrantReadWriteLock
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.memory.MemoryStore
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.memory.MemoryAllocator
import org.apache.spark.util.Utils

/**
 * An abstract memory manager that enforces how memory is shared between execution and storage.
 *
 * In this context, execution memory refers to that used for computation in shuffles, joins,
 * sorts and aggregations, while storage memory refers to that used for caching and propagating
 * internal data across the cluster. There exists one MemoryManager per JVM.
 */
private[spark] abstract class MemoryManager(
    conf: SparkConf,
    numCores: Int,
    onHeapStorageMemory: Long,
    onHeapExecutionMemory: Long) extends Logging {

  require(onHeapExecutionMemory > 0, "onHeapExecutionMemory must be > 0")

  // Acquire the marker before this manager's monitor. Shared ownership never excludes ordinary
  // operations, including capacity waiters; optional admission only tries the exclusive side.
  protected val optionalAdmissionGate = new ReentrantReadWriteLock()

  // This lock protects only registrations. Callbacks never run while it or this manager's
  // monitor is held, and registration does not wait behind ordinary capacity waiters.
  private val optionalReclaimers = new mutable.LinkedHashMap[Runnable, (Long, MemoryMode)]()
  @volatile private var onHeapOptionalReclaimers = 0
  @volatile private var offHeapOptionalReclaimers = 0

  /**
   * Register a task-owned, release-only callback before its first optional admission.
   * Returns an idempotent unregister action; callers must drain the owner before unregistering.
   * Callbacks may run concurrently, repeatedly, or after unregistering and must release each
   * reservation exactly once. They may take a short owner-state lock, but must not acquire a
   * TaskMemoryManager monitor, allocate execution memory, or wait for I/O or task cleanup.
   *
   * Never hold a lock needed by a reclaimer while requesting ordinary memory or invoking another
   * operation that may reclaim optional memory, including storage cleanup. Otherwise two tasks
   * can hold their own owner locks while reclaiming each other. Optional admission and release
   * may use that lock: neither invokes reclamation nor acquires a TaskMemoryManager monitor.
   */
  private[memory] final def registerOptionalMemoryReclaimer(
      taskAttemptId: Long,
      memoryMode: MemoryMode,
      reclaimer: Runnable): Runnable = {
    // A distinct forwarding object gives each registration identity even if callbacks are reused.
    val registered = new Runnable {
      /** Release this owner's optional bytes without allocating or destroying a whole reader. */
      override def run(): Unit = reclaimer.run()
    }
    optionalReclaimers.synchronized {
      optionalReclaimers(registered) = (taskAttemptId, memoryMode)
      memoryMode match {
        case MemoryMode.ON_HEAP => onHeapOptionalReclaimers += 1
        case MemoryMode.OFF_HEAP => offHeapOptionalReclaimers += 1
      }
    }
    new Runnable {
      /** Remove this registration only; an already-captured callback remains safe to invoke. */
      override def run(): Unit = optionalReclaimers.synchronized {
        if (optionalReclaimers.remove(registered).isDefined) {
          memoryMode match {
            case MemoryMode.ON_HEAP => onHeapOptionalReclaimers -= 1
            case MemoryMode.OFF_HEAP => offHeapOptionalReclaimers -= 1
          }
        }
      }
    }
  }

  /** Check for eligible owners without invoking callbacks or inspecting native state. */
  protected final def hasOptionalMemoryReclaimers(memoryMode: MemoryMode): Boolean = {
    memoryMode match {
      case MemoryMode.ON_HEAP => onHeapOptionalReclaimers != 0
      case MemoryMode.OFF_HEAP => offHeapOptionalReclaimers != 0
    }
  }

  /**
   * Drain a snapshot of matching owners under ordinary admission's shared gate, outside all
   * this manager's and the registry's monitors. Owners may run under the requesting task's monitor
   * and must follow the registration's lock-order contract. They must synchronously cancel pure
   * I/O and release exact credits, without dropping readers/sessions or awaiting task cleanup.
   * Continue draining other owners after a non-fatal failure, then propagate it without inventing
   * freed credit. Registrations remain live so a failed drain may be retried safely.
   */
  protected final def reclaimOptionalMemory(memoryMode: Option[MemoryMode]): Unit = {
    require(!Thread.holdsLock(this), "optional callbacks cannot run under the memory manager")
    val callbacks = optionalReclaimers.synchronized {
      optionalReclaimers.iterator.collect {
        case (callback, (_, mode)) if memoryMode.forall(_ == mode) => callback
      }.toList
    }
    var failure: Throwable = null
    callbacks.foreach { callback =>
      try {
        callback.run()
      } catch {
        case NonFatal(error) =>
          if (failure == null) failure = error else if (failure ne error) {
            failure.addSuppressed(error)
          }
      }
    }
    if (failure != null) throw failure
  }

  /**
   * Mark an operation that can hold this monitor while evicting blocks or waiting for capacity.
   *
   * MemoryStore uses this before its atomic unroll/storage transfers take the monitor, preserving
   * marker-before-monitor ordering when they call back into ordinary allocation. An outermost
   * MemoryStore operation drains optional owners before taking the monitor: a preflight outside
   * that monitor could otherwise race ordinary allocations and require a callback inside it.
   * Nested operations reuse the outer drain. Failures propagate and the marker is always released.
   * Release-only storage cleanup logs non-fatal drain failures and continues: aborting removal
   * could leave a cached entry behind after BlockManager deletes its metadata. Failed owners keep
   * their memory charges and registrations; failures from the cleanup body still propagate.
   */
  private[spark] final def withMemoryReclamation[T](
      body: => T,
      releaseOnly: Boolean = false): T = {
    val gate = optionalAdmissionGate.readLock()
    gate.lock()
    try {
      if ((onHeapOptionalReclaimers != 0 || offHeapOptionalReclaimers != 0) &&
          optionalAdmissionGate.getReadHoldCount == 1) {
        try {
          reclaimOptionalMemory(None)
        } catch {
          case NonFatal(error) if releaseOnly =>
            logWarning("Failed to reclaim optional memory before storage cleanup", error)
        }
      }
      body
    } finally {
      gate.unlock()
    }
  }

  // -- Methods related to memory allocation policies and bookkeeping ------------------------------

  @GuardedBy("this")
  protected val onHeapStorageMemoryPool = new StorageMemoryPool(this, MemoryMode.ON_HEAP)
  @GuardedBy("this")
  protected val offHeapStorageMemoryPool = new StorageMemoryPool(this, MemoryMode.OFF_HEAP)
  @GuardedBy("this")
  protected val onHeapExecutionMemoryPool = new ExecutionMemoryPool(this, MemoryMode.ON_HEAP)
  @GuardedBy("this")
  protected val offHeapExecutionMemoryPool = new ExecutionMemoryPool(this, MemoryMode.OFF_HEAP)

  onHeapStorageMemoryPool.incrementPoolSize(onHeapStorageMemory)
  onHeapExecutionMemoryPool.incrementPoolSize(onHeapExecutionMemory)

  protected[this] val maxOffHeapMemory = conf.get(MEMORY_OFFHEAP_SIZE)
  protected[this] val offHeapStorageMemory =
    (maxOffHeapMemory * conf.get(MEMORY_STORAGE_FRACTION)).toLong

  /**
   * The maximum number of consumers listed individually in the per-consumer memory breakdown
   * attached to an UNABLE_TO_ACQUIRE_MEMORY error. Read by [[TaskMemoryManager]]. See
   * `spark.memory.oomErrorConsumerBreakdownLimit`.
   */
  private[memory] val oomErrorConsumerBreakdownLimit: Int =
    conf.get(MEMORY_OOM_ERROR_CONSUMER_BREAKDOWN_LIMIT)

  offHeapExecutionMemoryPool.incrementPoolSize(maxOffHeapMemory - offHeapStorageMemory)
  offHeapStorageMemoryPool.incrementPoolSize(offHeapStorageMemory)

  /**
   * Total available on heap memory for storage, in bytes. This amount can vary over time,
   * depending on the MemoryManager implementation.
   * In this model, this is equivalent to the amount of memory not occupied by execution.
   */
  def maxOnHeapStorageMemory: Long

  /**
   * Total available off heap memory for storage, in bytes. This amount can vary over time,
   * depending on the MemoryManager implementation.
   */
  def maxOffHeapStorageMemory: Long

  /** Whether storage admission is impossible even after reclaiming all optional memory. */
  private[spark] def isStorageMemoryRequestTooLarge(
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = false

  /**
   * Set the [[MemoryStore]] used by this manager to evict cached blocks.
   * This must be set after construction due to initialization ordering constraints.
   */
  final def setMemoryStore(store: MemoryStore): Unit = synchronized {
    onHeapStorageMemoryPool.setMemoryStore(store)
    offHeapStorageMemoryPool.setMemoryStore(store)
  }

  /**
   * Acquire N bytes of memory to cache the given block, evicting existing ones if necessary.
   *
   * @return whether all N bytes were successfully granted.
   */
  def acquireStorageMemory(blockId: BlockId, numBytes: Long, memoryMode: MemoryMode): Boolean

  /**
   * Acquire N bytes of memory to unroll the given block, evicting existing ones if necessary.
   *
   * This extra method allows subclasses to differentiate behavior between acquiring storage
   * memory and acquiring unroll memory. For instance, the memory management model in Spark
   * 1.5 and before places a limit on the amount of space that can be freed from unrolling.
   *
   * @return whether all N bytes were successfully granted.
   */
  def acquireUnrollMemory(blockId: BlockId, numBytes: Long, memoryMode: MemoryMode): Boolean

  /**
   * Try to acquire up to `numBytes` of execution memory for the current task and return the
   * number of bytes obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
   */
  private[memory]
  def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long

  /**
   * Reserve all `numBytes` for optional task work without waiting for capacity or reclaiming
   * memory.
   *
   * Managers must opt in to this policy; the default rejects the request without changing their
   * accounting. Implementations may still contend on bookkeeping locks. A successful reservation
   * must be task-attributed and released through the existing execution-memory release methods.
   *
   * @return `numBytes` on success, or zero with no reservation on denial
   */
  private[memory] def tryAcquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long = {
    require(numBytes >= 0, s"invalid number of bytes requested: $numBytes")
    0L
  }

  /**
   * Release numBytes of execution memory belonging to the given task.
   */
  private[memory]
  def releaseExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Unit = synchronized {
    memoryMode match {
      case MemoryMode.ON_HEAP => onHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId)
      case MemoryMode.OFF_HEAP => offHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId)
    }
  }

  /**
   * Release all memory for the given task and mark it as inactive (e.g. when a task ends).
   *
   * @return the number of bytes freed.
   */
  private[memory] def releaseAllExecutionMemoryForTask(taskAttemptId: Long): Long = synchronized {
    onHeapExecutionMemoryPool.releaseAllMemoryForTask(taskAttemptId) +
      offHeapExecutionMemoryPool.releaseAllMemoryForTask(taskAttemptId)
  }

  /**
   * Release N bytes of storage memory.
   */
  def releaseStorageMemory(numBytes: Long, memoryMode: MemoryMode): Unit = synchronized {
    memoryMode match {
      case MemoryMode.ON_HEAP => onHeapStorageMemoryPool.releaseMemory(numBytes)
      case MemoryMode.OFF_HEAP => offHeapStorageMemoryPool.releaseMemory(numBytes)
    }
  }

  /**
   * Release all storage memory acquired.
   */
  final def releaseAllStorageMemory(): Unit = synchronized {
    onHeapStorageMemoryPool.releaseAllMemory()
    offHeapStorageMemoryPool.releaseAllMemory()
  }

  /**
   * Release N bytes of unroll memory.
   */
  final def releaseUnrollMemory(numBytes: Long, memoryMode: MemoryMode): Unit = synchronized {
    releaseStorageMemory(numBytes, memoryMode)
  }

  /**
   * Execution memory currently in use, in bytes.
   */
  final def executionMemoryUsed: Long = synchronized {
    onHeapExecutionMemoryPool.memoryUsed + offHeapExecutionMemoryPool.memoryUsed
  }

  /**
   * Storage memory currently in use, in bytes.
   */
  final def storageMemoryUsed: Long = synchronized {
    onHeapStorageMemoryPool.memoryUsed + offHeapStorageMemoryPool.memoryUsed
  }

  /**
   *  On heap execution memory currently in use, in bytes.
   */
  final def onHeapExecutionMemoryUsed: Long = synchronized {
    onHeapExecutionMemoryPool.memoryUsed
  }

  /**
   *  Off heap execution memory currently in use, in bytes.
   */
  final def offHeapExecutionMemoryUsed: Long = synchronized {
    offHeapExecutionMemoryPool.memoryUsed
  }

  /**
   *  On heap storage memory currently in use, in bytes.
   */
  final def onHeapStorageMemoryUsed: Long = synchronized {
    onHeapStorageMemoryPool.memoryUsed
  }

  /**
   *  Off heap storage memory currently in use, in bytes.
   */
  final def offHeapStorageMemoryUsed: Long = synchronized {
    offHeapStorageMemoryPool.memoryUsed
  }

  /**
   * Returns the execution memory consumption, in bytes, for the given task.
   */
  private[memory] def getExecutionMemoryUsageForTask(taskAttemptId: Long): Long = synchronized {
    onHeapExecutionMemoryPool.getMemoryUsageForTask(taskAttemptId) +
      offHeapExecutionMemoryPool.getMemoryUsageForTask(taskAttemptId)
  }

  // -- Fields related to Tungsten managed memory -------------------------------------------------

  /**
   * Tracks whether Tungsten memory will be allocated on the JVM heap or off-heap using
   * sun.misc.Unsafe.
   */
  final val tungstenMemoryMode: MemoryMode = {
    if (conf.get(MEMORY_OFFHEAP_ENABLED)) {
      require(conf.get(MEMORY_OFFHEAP_SIZE) > 0,
        "spark.memory.offHeap.size must be > 0 when spark.memory.offHeap.enabled == true")
      require(Platform.unaligned(),
        "No support for unaligned Unsafe. Set spark.memory.offHeap.enabled to false.")
      MemoryMode.OFF_HEAP
    } else {
      MemoryMode.ON_HEAP
    }
  }

  /**
   * The default page size, in bytes.
   *
   * If user didn't explicitly set "spark.buffer.pageSize", we figure out the default value
   * by looking at the number of cores available to the process, and the total amount of memory,
   * and then divide it by a factor of safety.
   *
   * SPARK-37593 If we are using G1GC, ZGC or ShenandoahGC, it's better to take the
   * LONG_ARRAY_OFFSET into consideration so that the requested memory size is power of 2
   * and can be divided by heap region size to reduce memory waste.
   */
  private lazy val defaultPageSizeBytes = {
    val minPageSize = 1L * 1024 * 1024   // 1MB
    val maxPageSize = 64L * minPageSize  // 64MB
    val cores = if (numCores > 0) numCores else Runtime.getRuntime.availableProcessors()
    // Because of rounding to next power of 2, we may have safetyFactor as 8 in worst case
    val safetyFactor = 16
    val maxTungstenMemory: Long = tungstenMemoryMode match {
      case MemoryMode.ON_HEAP => onHeapExecutionMemoryPool.poolSize
      case MemoryMode.OFF_HEAP => offHeapExecutionMemoryPool.poolSize
    }
    val size = ByteArrayMethods.nextPowerOf2(maxTungstenMemory / cores / safetyFactor)
    val chosenPageSize = math.min(maxPageSize, math.max(minPageSize, size))
    if ((Utils.isG1GC || Utils.isZGC || Utils.isShenandoahGC) &&
        tungstenMemoryMode == MemoryMode.ON_HEAP) {
      chosenPageSize - Platform.LONG_ARRAY_OFFSET
    } else {
      chosenPageSize
    }
  }

  val pageSizeBytes: Long = conf.get(BUFFER_PAGESIZE).getOrElse(defaultPageSizeBytes)

  /**
   * Allocates memory for use by Unsafe/Tungsten code.
   */
  private[memory] final val tungstenMemoryAllocator: MemoryAllocator = {
    tungstenMemoryMode match {
      case MemoryMode.ON_HEAP => MemoryAllocator.HEAP
      case MemoryMode.OFF_HEAP => MemoryAllocator.UNSAFE
    }
  }
}
