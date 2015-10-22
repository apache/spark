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

package org.apache.spark.shuffle

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.google.common.annotations.VisibleForTesting

import org.apache.spark._
import org.apache.spark.memory.{StaticMemoryManager, MemoryManager}
import org.apache.spark.storage.{BlockId, BlockStatus}
import org.apache.spark.unsafe.array.ByteArrayMethods

/**
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
 *
 * Use `ShuffleMemoryManager.create()` factory method to create a new instance.
 *
 * @param memoryManager the interface through which this manager acquires execution memory
 * @param pageSizeBytes number of bytes for each page, by default.
 */
private[spark]
class ShuffleMemoryManager protected (
    memoryManager: MemoryManager,
    val pageSizeBytes: Long)
  extends Logging {

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
  def tryToAcquire(numBytes: Long): Long = memoryManager.synchronized {
    val taskAttemptId = currentTaskAttemptId()
    assert(numBytes > 0, "invalid number of bytes requested: " + numBytes)

    // Add this task to the taskMemory map just so we can keep an accurate count of the number
    // of active tasks, to let other tasks ramp down their memory in calls to tryToAcquire
    if (!taskMemory.contains(taskAttemptId)) {
      taskMemory(taskAttemptId) = 0L
      // This will later cause waiting tasks to wake up and check numTasks again
      memoryManager.notifyAll()
    }

    // Keep looping until we're either sure that we don't want to grant this request (because this
    // task would have more than 1 / numActiveTasks of the memory) or we have enough free
    // memory to give it (we always let each task get at least 1 / (2 * numActiveTasks)).
    // TODO: simplify this to limit each task to its own slot
    while (true) {
      val numActiveTasks = taskMemory.keys.size
      val curMem = taskMemory(taskAttemptId)
      val maxMemory = memoryManager.maxExecutionMemory
      val freeMemory = maxMemory - taskMemory.values.sum

      // How much we can grant this task; don't let it grow to more than 1 / numActiveTasks;
      // don't let it be negative
      val maxToGrant = math.min(numBytes, math.max(0, (maxMemory / numActiveTasks) - curMem))
      // Only give it as much memory as is free, which might be none if it reached 1 / numTasks
      val toGrant = math.min(maxToGrant, freeMemory)

      if (curMem < maxMemory / (2 * numActiveTasks)) {
        // We want to let each task get at least 1 / (2 * numActiveTasks) before blocking;
        // if we can't give it this much now, wait for other tasks to free up memory
        // (this happens if older tasks allocated lots of memory before N grew)
        if (freeMemory >= math.min(maxToGrant, maxMemory / (2 * numActiveTasks) - curMem)) {
          return acquire(toGrant)
        } else {
          logInfo(
            s"TID $taskAttemptId waiting for at least 1/2N of shuffle memory pool to be free")
          memoryManager.wait()
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
  private def acquire(numBytes: Long): Long = memoryManager.synchronized {
    val taskAttemptId = currentTaskAttemptId()
    val evictedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
    val acquired = memoryManager.acquireExecutionMemory(numBytes, evictedBlocks)
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
  def release(numBytes: Long): Unit = memoryManager.synchronized {
    val taskAttemptId = currentTaskAttemptId()
    val curMem = taskMemory.getOrElse(taskAttemptId, 0L)
    if (curMem < numBytes) {
      throw new SparkException(
        s"Internal error: release called on $numBytes bytes but task only has $curMem")
    }
    if (taskMemory.contains(taskAttemptId)) {
      taskMemory(taskAttemptId) -= numBytes
      memoryManager.releaseExecutionMemory(numBytes)
    }
    memoryManager.notifyAll() // Notify waiters in tryToAcquire that memory has been freed
  }

  /** Release all memory for the current task and mark it as inactive (e.g. when a task ends). */
  def releaseMemoryForThisTask(): Unit = memoryManager.synchronized {
    val taskAttemptId = currentTaskAttemptId()
    taskMemory.remove(taskAttemptId).foreach { numBytes =>
      memoryManager.releaseExecutionMemory(numBytes)
    }
    memoryManager.notifyAll() // Notify waiters in tryToAcquire that memory has been freed
  }

  /** Returns the memory consumption, in bytes, for the current task */
  def getMemoryConsumptionForThisTask(): Long = memoryManager.synchronized {
    val taskAttemptId = currentTaskAttemptId()
    taskMemory.getOrElse(taskAttemptId, 0L)
  }
}


private[spark] object ShuffleMemoryManager {

  def create(
      conf: SparkConf,
      memoryManager: MemoryManager,
      numCores: Int): ShuffleMemoryManager = {
    val maxMemory = memoryManager.maxExecutionMemory
    val pageSize = ShuffleMemoryManager.getPageSize(conf, maxMemory, numCores)
    new ShuffleMemoryManager(memoryManager, pageSize)
  }

  /**
   * Create a dummy [[ShuffleMemoryManager]] with the specified capacity and page size.
   */
  def create(maxMemory: Long, pageSizeBytes: Long): ShuffleMemoryManager = {
    val conf = new SparkConf
    val memoryManager = new StaticMemoryManager(
      conf, maxExecutionMemory = maxMemory, maxStorageMemory = Long.MaxValue)
    new ShuffleMemoryManager(memoryManager, pageSizeBytes)
  }

  @VisibleForTesting
  def createForTesting(maxMemory: Long): ShuffleMemoryManager = {
    create(maxMemory, 4 * 1024 * 1024)
  }

  /**
   * Sets the page size, in bytes.
   *
   * If user didn't explicitly set "spark.buffer.pageSize", we figure out the default value
   * by looking at the number of cores available to the process, and the total amount of memory,
   * and then divide it by a factor of safety.
   */
  private def getPageSize(conf: SparkConf, maxMemory: Long, numCores: Int): Long = {
    val minPageSize = 1L * 1024 * 1024   // 1MB
    val maxPageSize = 64L * minPageSize  // 64MB
    val cores = if (numCores > 0) numCores else Runtime.getRuntime.availableProcessors()
    // Because of rounding to next power of 2, we may have safetyFactor as 8 in worst case
    val safetyFactor = 16
    val size = ByteArrayMethods.nextPowerOf2(maxMemory / cores / safetyFactor)
    val default = math.min(maxPageSize, math.max(minPageSize, size))
    conf.getSizeAsBytes("spark.buffer.pageSize", default)
  }
}
