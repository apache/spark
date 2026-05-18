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

import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkIllegalArgumentException}
import org.apache.spark.internal.{config, Logging, LogKeys}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.Tests._
import org.apache.spark.internal.config.UNMANAGED_MEMORY_POLLING_INTERVAL
import org.apache.spark.storage.BlockId
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * A [[MemoryManager]] that enforces a soft boundary between execution and storage such that
 * either side can borrow memory from the other.
 *
 * The region shared between execution and storage is a fraction of (the total heap space - 300MB)
 * configurable through `spark.memory.fraction` (default 0.6). The position of the boundary
 * within this space is further determined by `spark.memory.storageFraction` (default 0.5).
 * This means the size of the storage region is 0.6 * 0.5 = 0.3 of the heap space by default.
 *
 * Storage can borrow as much execution memory as is free until execution reclaims its space.
 * When this happens, cached blocks will be evicted from memory until sufficient borrowed
 * memory is released to satisfy the execution memory request.
 *
 * Similarly, execution can borrow as much storage memory as is free. However, execution
 * memory is *never* evicted by storage due to the complexities involved in implementing this.
 * The implication is that attempts to cache blocks may fail if execution has already eaten
 * up most of the storage space, in which case the new blocks will be evicted immediately
 * according to their respective storage levels.
 *
 * @param onHeapStorageRegionSize Size of the storage region, in bytes.
 *                          This region is not statically reserved; execution can borrow from
 *                          it if necessary. Cached blocks can be evicted only if actual
 *                          storage memory usage exceeds this region.
 */
private[spark] class UnifiedMemoryManager(
    conf: SparkConf,
    val maxHeapMemory: Long,
    onHeapStorageRegionSize: Long,
    numCores: Int)
  extends MemoryManager(
    conf,
    numCores,
    onHeapStorageRegionSize,
    maxHeapMemory - onHeapStorageRegionSize) with Logging  {

  /**
   * Unmanaged memory tracking infrastructure.
   *
   * Unmanaged memory refers to memory consumed by components that manage their own memory
   * outside of Spark's unified memory management system. Examples include:
   * - RocksDB state stores used in structured streaming
   * - Native libraries with their own memory management
   * - Off-heap caches managed by unmanaged systems
   *
   * We track this memory to:
   * 1. Provide visibility into total memory usage on executors
   * 2. Prevent OOM errors by accounting for it in memory allocation decisions
   * 3. Enable better debugging and monitoring of memory-intensive applications
   *
   * The polling mechanism periodically queries registered unmanaged memory consumers
   * to detect inactive consumers and handle cleanup.
   */
  // Configuration for polling interval (in milliseconds)
  private val unmanagedMemoryPollingIntervalMs = conf.get(UNMANAGED_MEMORY_POLLING_INTERVAL)
  // Initialize background polling if enabled
  if (unmanagedMemoryPollingIntervalMs > 0) {
    UnifiedMemoryManager.startPollingIfNeeded(unmanagedMemoryPollingIntervalMs)
  }

  /**
   * Get the current unmanaged memory usage in bytes for a specific memory mode.
   * @param memoryMode The memory mode (ON_HEAP or OFF_HEAP) to get usage for
   * @return The current unmanaged memory usage in bytes
   */
  private def getUnmanagedMemoryUsed(memoryMode: MemoryMode): Long = {
    // Only consider unmanaged memory if polling is enabled
    if (unmanagedMemoryPollingIntervalMs <= 0) {
      return 0L
    }
    memoryMode match {
      case MemoryMode.ON_HEAP => UnifiedMemoryManager.unmanagedOnHeapUsed.get()
      case MemoryMode.OFF_HEAP => UnifiedMemoryManager.unmanagedOffHeapUsed.get()
    }
  }

  private def assertInvariants(): Unit = {
    assert(onHeapExecutionMemoryPool.poolSize + onHeapStorageMemoryPool.poolSize == maxHeapMemory)
    assert(
      offHeapExecutionMemoryPool.poolSize + offHeapStorageMemoryPool.poolSize == maxOffHeapMemory)
  }

  assertInvariants()

  override def maxOnHeapStorageMemory: Long = synchronized {
    maxHeapMemory - onHeapExecutionMemoryPool.memoryUsed
  }

  override def maxOffHeapStorageMemory: Long = synchronized {
    maxOffHeapMemory - offHeapExecutionMemoryPool.memoryUsed
  }

  /**
   * Try to acquire up to `numBytes` of execution memory for the current task and return the
   * number of bytes obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
   */
  override private[memory] def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long = synchronized {
    assertInvariants()
    assert(numBytes >= 0)
    val (executionPool, storagePool, storageRegionSize, maxMemory) = memoryMode match {
      case MemoryMode.ON_HEAP => (
        onHeapExecutionMemoryPool,
        onHeapStorageMemoryPool,
        onHeapStorageRegionSize,
        maxHeapMemory)
      case MemoryMode.OFF_HEAP => (
        offHeapExecutionMemoryPool,
        offHeapStorageMemoryPool,
        offHeapStorageMemory,
        maxOffHeapMemory)
    }

    /**
     * Grow the execution pool by evicting cached blocks, thereby shrinking the storage pool.
     *
     * When acquiring memory for a task, the execution pool may need to make multiple
     * attempts. Each attempt must be able to evict storage in case another task jumps in
     * and caches a large block between the attempts. This is called once per attempt.
     */
    def maybeGrowExecutionPool(extraMemoryNeeded: Long): Unit = {
      if (extraMemoryNeeded > 0) {
        // Compute the reclaim cap BEFORE asking externals to shrink, otherwise shrunk bytes
        // would let execution claim into the protected storage region.
        val memoryReclaimableFromStorage = math.max(
          storagePool.memoryFree,
          storagePool.poolSize - storageRegionSize)
        val targetReclaim = math.min(extraMemoryNeeded, memoryReclaimableFromStorage)
        val shrinkNeeded = math.max(0L, targetReclaim - storagePool.memoryFree)
        if (shrinkNeeded > 0L) {
          shrinkExternal(shrinkNeeded, memoryMode)
        }
        if (memoryReclaimableFromStorage > 0) {
          val spaceToReclaim = storagePool.freeSpaceToShrinkPool(targetReclaim)
          storagePool.decrementPoolSize(spaceToReclaim)
          executionPool.incrementPoolSize(spaceToReclaim)
        }
      }
    }

    /**
     * The size the execution pool would have after evicting storage memory.
     *
     * The execution memory pool divides this quantity among the active tasks evenly to cap
     * the execution memory allocation for each task. It is important to keep this greater
     * than the execution pool size, which doesn't take into account potential memory that
     * could be freed by evicting storage. Otherwise we may hit SPARK-12155.
     *
     * Additionally, this quantity should be kept below `maxMemory` to arbitrate fairness
     * in execution memory allocation across tasks, Otherwise, a task may occupy more than
     * its fair share of execution memory, mistakenly thinking that other tasks can acquire
     * the portion of storage memory that cannot be evicted.
     *
     * This also factors in unmanaged memory usage to ensure we don't over-allocate memory
     * when unmanaged components are consuming significant memory.
     */
    def computeMaxExecutionPoolSize(): Long = {
      val unmanagedMemory = getUnmanagedMemoryUsed(memoryMode)
      val availableMemory = maxMemory - math.min(storagePool.memoryUsed, storageRegionSize)
      // Reduce available memory by unmanaged memory usage to prevent over-allocation
      math.max(0L, availableMemory - unmanagedMemory)
    }

    executionPool.acquireMemory(
      numBytes, taskAttemptId, maybeGrowExecutionPool, () => computeMaxExecutionPoolSize())
  }

  override def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = {
    acquireStorageMemoryUnified(
      numBytes,
      memoryMode,
      exclude = None,
      (effective, unmanaged) =>
        logInfo(log"Will not store ${MDC(BLOCK_ID, blockId)} as the required space" +
          log" (${MDC(NUM_BYTES, numBytes)} bytes) exceeds our" +
          log" memory limit (${MDC(NUM_BYTES_MAX, effective)} bytes)" +
          (if (unmanaged > 0) log" (unmanaged memory usage: ${MDC(NUM_BYTES, unmanaged)} bytes)"
           else log"")),
      (pool, n) => pool.acquireMemory(blockId, n))
  }

  override def acquireUnrollMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = {
    acquireStorageMemory(blockId, numBytes, memoryMode)
  }

  override def acquireStorageMemory(
      self: ManagedConsumer,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = {
    require(self != null, "self ManagedConsumer must not be null")
    require(self.memoryMode == memoryMode,
      s"requested memoryMode=$memoryMode does not match self.memoryMode=${self.memoryMode}; " +
        "a ManagedConsumer may only acquire memory in the mode it manages")
    acquireStorageMemoryUnified(
      numBytes,
      memoryMode,
      exclude = Some(self),
      (effective, unmanaged) =>
        logInfo(log"Will not grant external storage memory request of " +
          log"${MDC(NUM_BYTES, numBytes)} bytes as it exceeds the " +
          log"effective limit (${MDC(NUM_BYTES_MAX, effective)} bytes)" +
          (if (unmanaged > 0) log" (unmanaged memory usage: ${MDC(NUM_BYTES, unmanaged)} bytes)"
           else log"")),
      (pool, n) => pool.acquireMemoryForManagedConsumer(n))
  }

  /**
   * Shared body for the two [[acquireStorageMemory]] overloads. `logFailFast` and
   * `acquireFromPool` capture the only per-overload differences. Order is: fail-fast on
   * `effectiveMaxMemory`, borrow free execution memory, [[shrinkExternal]] for any remaining
   * deficit, then delegate to the pool. Borrow runs before shrink because it is free
   * (no eviction).
   */
  private def acquireStorageMemoryUnified(
      numBytes: Long,
      memoryMode: MemoryMode,
      exclude: Option[ManagedConsumer],
      logFailFast: (Long, Long) => Unit,
      acquireFromPool: (StorageMemoryPool, Long) => Boolean): Boolean = synchronized {
    assertInvariants()
    require(numBytes >= 0, s"numBytes must be >= 0, got $numBytes")
    val (executionPool, storagePool, maxMemory) = memoryMode match {
      case MemoryMode.ON_HEAP => (
        onHeapExecutionMemoryPool,
        onHeapStorageMemoryPool,
        maxOnHeapStorageMemory)
      case MemoryMode.OFF_HEAP => (
        offHeapExecutionMemoryPool,
        offHeapStorageMemoryPool,
        maxOffHeapStorageMemory)
    }

    val unmanagedMemory = getUnmanagedMemoryUsed(memoryMode)
    val effectiveMaxMemory = math.max(0L, maxMemory - unmanagedMemory)

    if (numBytes > effectiveMaxMemory) {
      logFailFast(effectiveMaxMemory, unmanagedMemory)
      return false
    }
    if (numBytes > storagePool.memoryFree) {
      val memoryBorrowedFromExecution = Math.min(executionPool.memoryFree,
        numBytes - storagePool.memoryFree)
      executionPool.decrementPoolSize(memoryBorrowedFromExecution)
      storagePool.incrementPoolSize(memoryBorrowedFromExecution)
    }
    val deficitAfterBorrow = math.max(0L, numBytes - storagePool.memoryFree)
    if (deficitAfterBorrow > 0L) {
      shrinkExternal(deficitAfterBorrow, memoryMode, exclude)
    }
    acquireFromPool(storagePool, numBytes)
  }

  override private[spark] def getShrinkableConsumers(
      memoryMode: MemoryMode): Iterable[ManagedConsumer] =
    UnifiedMemoryManager.getShrinkableConsumers(memoryMode)
}

object UnifiedMemoryManager extends Logging {

  // Set aside a fixed amount of memory for non-storage, non-execution purposes.
  // This serves a function similar to `spark.memory.fraction`, but guarantees that we reserve
  // sufficient memory for the system even for small heaps. E.g. if we have a 1GB JVM, then
  // the memory used for execution and storage will be (1024 - 300) * 0.6 = 434MB by default.
  private val RESERVED_SYSTEM_MEMORY_BYTES = 300 * 1024 * 1024

  private val unmanagedMemoryConsumers =
    new ConcurrentHashMap[UnmanagedMemoryConsumerId, UnmanagedMemoryConsumer]

  // Cached unmanaged memory usage values updated by polling
  private val unmanagedOnHeapUsed = new AtomicLong(0L)
  private val unmanagedOffHeapUsed = new AtomicLong(0L)

  // Atomic flag to ensure polling is only started once per JVM
  private val pollingStarted = new AtomicBoolean(false)

  /**
   * Returns the total unmanaged memory in bytes, including both
   * on-heap unmanaged memory and off-heap unmanaged memory.
   */
  private[spark] def getUnmanagedMemoryUsed: Long = {
    UnifiedMemoryManager.unmanagedOnHeapUsed.get() + UnifiedMemoryManager.unmanagedOffHeapUsed.get()
  }

  /**
   * Register an unmanaged memory consumer to track its memory usage.
   *
   * Unmanaged memory consumers are components that manage their own memory outside
   * of Spark's unified memory management system. By registering, their memory usage
   * will be periodically polled and factored into Spark's memory allocation decisions.
   *
   * @param unmanagedMemoryConsumer The consumer to register for memory tracking
   */
  def registerUnmanagedMemoryConsumer(
      unmanagedMemoryConsumer: UnmanagedMemoryConsumer): Unit = {
    val id = unmanagedMemoryConsumer.unmanagedMemoryConsumerId
    unmanagedMemoryConsumers.put(id, unmanagedMemoryConsumer)
    unmanagedMemoryConsumer match {
      case mc: ManagedConsumer if isRegisteredManaged(mc) => warnCrossRegistered(mc)
      case _ =>
    }
  }

  private def isRegisteredManaged(mc: ManagedConsumer): Boolean = {
    val n = mc.name
    n != null && n.nonEmpty && (managedConsumers.get(n) eq mc)
  }

  private def isRegisteredUnmanaged(umc: UnmanagedMemoryConsumer): Boolean = {
    val id = umc.unmanagedMemoryConsumerId
    unmanagedMemoryConsumers.get(id) eq umc
  }

  private def warnCrossRegistered(mc: ManagedConsumer): Unit = {
    logWarning(log"Object ${MDC(LogKeys.OBJECT_ID, MemoryManager.consumerLogName(mc))} " +
      log"is registered as BOTH ManagedConsumer and UnmanagedMemoryConsumer; the same " +
      log"bytes will be subtracted twice from effectiveMaxMemory. Pick exactly one SPI.")
  }

  /**
   * Unregister an unmanaged memory consumer.
   * This should be called when a component is shutting down to prevent memory leaks
   * and ensure accurate memory tracking.
   *
   * @param unmanagedMemoryConsumer The consumer to unregister. Only used in tests
   */
  private[spark] def unregisterUnmanagedMemoryConsumer(
      unmanagedMemoryConsumer: UnmanagedMemoryConsumer): Unit = {
    val id = unmanagedMemoryConsumer.unmanagedMemoryConsumerId
    unmanagedMemoryConsumers.remove(id)
  }


  /**
   * Get the current memory usage in bytes for a specific component type.
   * @param componentType The type of component to filter by (e.g., "RocksDB")
   * @return Total memory usage in bytes for the specified component type
   */
  def getMemoryByComponentType(componentType: String): Long = {
    unmanagedMemoryConsumers.asScala.values.toSeq
      .filter(_.unmanagedMemoryConsumerId.componentType == componentType)
      .map { memoryUser =>
        try {
          memoryUser.getMemBytesUsed
        } catch {
          case e: Exception =>
            0L
        }
      }
      .sum
  }

  /**
   * Clear all unmanaged memory users.
   * This is useful during executor shutdown or cleanup.
   * Since each executor runs in its own JVM, this clears all users for this executor.
   */
  def clearUnmanagedMemoryUsers(): Unit = {
    unmanagedMemoryConsumers.clear()
    // Reset cached values when clearing consumers
    unmanagedOnHeapUsed.set(0L)
    unmanagedOffHeapUsed.set(0L)
  }

  // -- Managed consumer registry --

  private val managedConsumers =
    new ConcurrentHashMap[String, ManagedConsumer]()

  /**
   * Register a [[ManagedConsumer]] as a candidate for [[MemoryManager.shrinkExternal]]
   * (requires `spark.memory.managedConsumer.enabled=true`).
   *
   * The registry is JVM-global and does NOT propagate across the cluster. Keyed by
   * [[ManagedConsumer.name]] (ON_HEAP and OFF_HEAP share one namespace); re-registering
   * the SAME instance is idempotent, a DIFFERENT instance under an already-taken name
   * fails. Callers MUST invoke [[unregisterManagedConsumer]] on shutdown -- the registry
   * holds strong references.
   */
  def registerManagedConsumer(consumer: ManagedConsumer): Unit = {
    require(consumer != null, "ManagedConsumer must not be null")
    val n = consumer.name
    require(n != null && n.nonEmpty,
      "ManagedConsumer.name must be non-empty (used as the registry key)")
    val prior = managedConsumers.putIfAbsent(n, consumer)
    if (prior != null && (prior ne consumer)) {
      throw new IllegalArgumentException(
        s"A different ManagedConsumer is already registered under name '$n'. " +
          s"Existing: ${prior.getClass.getName}, new: ${consumer.getClass.getName}. " +
          "Names must be unique within this JVM (ON_HEAP and OFF_HEAP share one namespace).")
    }
    consumer match {
      case umc: UnmanagedMemoryConsumer if isRegisteredUnmanaged(umc) =>
        warnCrossRegistered(consumer)
      case _ =>
    }
  }

  /**
   * Unregister a [[ManagedConsumer]]. Removes only when the registered instance is the
   * one passed here (a stale unregister cannot evict a later re-registration under the
   * same name). No-op for null / empty-name / not-registered.
   */
  private[spark] def unregisterManagedConsumer(consumer: ManagedConsumer): Unit = {
    if (consumer == null) return
    val n = consumer.name
    if (n != null && n.nonEmpty) {
      managedConsumers.remove(n, consumer)
    }
  }

  /**
   * Snapshot of registered managed consumers for `memoryMode`, filtered to those reporting
   * positive [[ManagedConsumer.getShrinkableMemoryBytes]], sorted DESC (tie-break unspecified).
   * Iteration is weakly-consistent; consumers that throw or return negative are coerced to 0
   * and filtered out.
   */
  private[spark] def getShrinkableConsumers(
      memoryMode: MemoryMode): Iterable[ManagedConsumer] = {
    if (managedConsumers.isEmpty) return Iterable.empty
    def safeGetShrinkableBytes(c: ManagedConsumer): Long = {
      try {
        val b = c.getShrinkableMemoryBytes
        if (b < 0L) {
          logWarning(log"ManagedConsumer ${MDC(LogKeys.OBJECT_ID,
              MemoryManager.consumerLogName(c))} returned negative " +
            log"getShrinkableMemoryBytes=${MDC(LogKeys.NUM_BYTES, b)}; treating as 0")
          0L
        } else {
          b
        }
      } catch {
        case NonFatal(t) =>
          logWarning(log"ManagedConsumer ${MDC(LogKeys.OBJECT_ID,
              MemoryManager.consumerLogName(c))} threw from getShrinkableMemoryBytes; " +
            log"treating as 0: ${MDC(LogKeys.ERROR, t.getMessage)}", t)
          0L
      }
    }
    managedConsumers.values().asScala.iterator
      .filter(_.memoryMode == memoryMode)
      .map(c => (c, safeGetShrinkableBytes(c)))
      .filter(_._2 > 0L)
      .toSeq
      .sortBy(-_._2)
      .map(_._1)
  }

  /** Test-only: clear all managed consumers. */
  private[spark] def clearManagedConsumers(): Unit = {
    managedConsumers.clear()
  }

  // Shared polling infrastructure - only one polling thread per JVM
  @volatile private var unmanagedMemoryPoller: ScheduledExecutorService = _

  /**
   * Start unmanaged memory polling if not already started.
   * This ensures only one polling thread is created per JVM, regardless of how many
   * UnifiedMemoryManager instances are created.
   */
  private[memory] def startPollingIfNeeded(pollingIntervalMs: Long): Unit = {
    if (pollingStarted.compareAndSet(false, true)) {
      unmanagedMemoryPoller = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
        "unmanaged-memory-poller")

      val pollingTask = new Runnable {
        override def run(): Unit = Utils.tryLogNonFatalError {
          pollUnmanagedMemoryUsers()
        }
      }

      unmanagedMemoryPoller.scheduleAtFixedRate(
        pollingTask,
        0L, // initial delay
        pollingIntervalMs,
        TimeUnit.MILLISECONDS)

      logInfo(log"Unmanaged memory polling started with interval " +
        log"${MDC(LogKeys.TIME, pollingIntervalMs)}ms")
    }
  }

  private def pollUnmanagedMemoryUsers(): Unit = {
    val consumers = unmanagedMemoryConsumers.asScala.toMap

    // Get memory usage for each consumer, handling failures gracefully
    val memoryUsages = consumers.map { case (userId, memoryUser) =>
      try {
        val memoryUsed = memoryUser.getMemBytesUsed
        if (memoryUsed == -1L) {
          logDebug(log"Unmanaged memory consumer ${MDC(LogKeys.OBJECT_ID, userId.toString)} " +
            log"is no longer active, marking for removal")
          (userId, memoryUser, None) // Mark for removal
        } else if (memoryUsed < 0L) {
          logWarning(log"Invalid memory usage value ${MDC(LogKeys.NUM_BYTES, memoryUsed)} " +
            log"from unmanaged memory user ${MDC(LogKeys.OBJECT_ID, userId.toString)}")
          (userId, memoryUser, Some(0L)) // Treat as 0
        } else {
          (userId, memoryUser, Some(memoryUsed))
        }
      } catch {
        case NonFatal(e) =>
          logWarning(log"Failed to get memory usage for unmanaged memory user " +
            log"${MDC(LogKeys.OBJECT_ID, userId.toString)} ${MDC(LogKeys.EXCEPTION, e)}")
          (userId, memoryUser, Some(0L)) // Treat as 0 on error
      }
    }

    // Remove inactive consumers
    memoryUsages.filter(_._3.isEmpty).foreach { case (userId, _, _) =>
      unmanagedMemoryConsumers.remove(userId)
      logInfo(log"Removed inactive unmanaged memory consumer " +
        log"${MDC(LogKeys.OBJECT_ID, userId.toString)}")
    }
    // Calculate total memory usage by mode
    val activeUsages = memoryUsages.filter(_._3.isDefined)
    val onHeapTotal = activeUsages
      .filter(_._2.memoryMode == MemoryMode.ON_HEAP)
      .map(_._3.get)
      .sum
    val offHeapTotal = activeUsages
      .filter(_._2.memoryMode == MemoryMode.OFF_HEAP)
      .map(_._3.get)
      .sum
    // Update cached values atomically
    unmanagedOnHeapUsed.set(onHeapTotal)
    unmanagedOffHeapUsed.set(offHeapTotal)
    // Log polling results for monitoring
    val totalMemoryUsed = onHeapTotal + offHeapTotal
    val numConsumers = activeUsages.size
    logDebug(s"Unmanaged memory polling completed: $numConsumers consumers, " +
      s"total memory used: ${totalMemoryUsed} bytes " +
      s"(on-heap: ${onHeapTotal}, off-heap: ${offHeapTotal})")
  }

  /**
   * Shutdown the unmanaged memory polling thread. Only used in tests
   */
  private[spark] def shutdownUnmanagedMemoryPoller(): Unit = {
    synchronized {
      if (unmanagedMemoryPoller != null) {
        unmanagedMemoryPoller.shutdown()
        try {
          if (!unmanagedMemoryPoller.awaitTermination(5, TimeUnit.SECONDS)) {
            unmanagedMemoryPoller.shutdownNow()
          }
        } catch {
          case _: InterruptedException =>
            Thread.currentThread().interrupt()
        }
        unmanagedMemoryPoller = null
        pollingStarted.set(false)
        logInfo(log"Unmanaged memory poller shutdown complete")
      }
    }
  }

  def apply(conf: SparkConf, numCores: Int): UnifiedMemoryManager = {
    val maxMemory = getMaxMemory(conf)
    new UnifiedMemoryManager(
      conf,
      maxHeapMemory = maxMemory,
      onHeapStorageRegionSize =
        (maxMemory * conf.get(config.MEMORY_STORAGE_FRACTION)).toLong,
      numCores = numCores)
  }

  /**
   * Return the total amount of memory shared between execution and storage, in bytes.
   */
  private def getMaxMemory(conf: SparkConf): Long = {
    val systemMemory = conf.get(TEST_MEMORY)
    val reservedMemory = conf.getLong(TEST_RESERVED_MEMORY.key,
      if (conf.contains(IS_TESTING)) 0 else RESERVED_SYSTEM_MEMORY_BYTES)
    val minSystemMemory = (reservedMemory * 1.5).ceil.toLong
    if (systemMemory < minSystemMemory) {
      throw new SparkIllegalArgumentException(
        errorClass = "INVALID_DRIVER_MEMORY",
        messageParameters = Map(
          "systemMemory" -> systemMemory.toString,
          "minSystemMemory" -> minSystemMemory.toString,
          "config" -> config.DRIVER_MEMORY.key))
    }
    // SPARK-12759 Check executor memory to fail fast if memory is insufficient
    if (conf.contains(config.EXECUTOR_MEMORY)) {
      val executorMemory = conf.getSizeAsBytes(config.EXECUTOR_MEMORY.key)
      if (executorMemory < minSystemMemory) {
        throw new SparkIllegalArgumentException(
          errorClass = "INVALID_EXECUTOR_MEMORY",
          messageParameters = Map(
            "executorMemory" -> executorMemory.toString,
            "minSystemMemory" -> minSystemMemory.toString,
            "config" -> config.EXECUTOR_MEMORY.key))
      }
    }
    val usableMemory = systemMemory - reservedMemory
    val memoryFraction = conf.get(config.MEMORY_FRACTION)
    (usableMemory * memoryFraction).toLong
  }
}
