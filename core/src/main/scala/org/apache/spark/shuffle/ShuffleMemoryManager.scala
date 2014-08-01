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

import org.apache.spark.{SparkException, SparkConf}
import scala.collection.mutable

/**
 * Allocates a pool of memory to task threads for use in shuffle operations. Each disk-spilling
 * collection (ExternalAppendOnlyMap or ExternalSorter) used by these tasks can acquire memory
 * from this pool and release it as it spills data out. When a task ends, all its memory will be
 * released by the Executor.
 *
 * This class tries to ensure that each thread gets a reasonable share of memory, instead of some
 * thread ramping up to a large amount first and then causing others to spill to disk repeatedly.
 * If there are N threads, it ensures that each thread can acquire at least 1 / 2N of the memory
 * before it has to spill, and at most 1 / N. Because N varies dynamically, we keep track of the
 * set of active threads and redo the calculations of 1 / 2N and 1 / N in waiting threads whenever
 * this set changes. This is all done by synchronizing access on "this" to mutate state and using
 * wait() and notifyAll() to signal changes.
 */
private[spark] class ShuffleMemoryManager(maxMemory: Long) {
  private val threadMemory = new mutable.HashMap[Long, Long]()  // threadId -> memory bytes

  def this(conf: SparkConf) = this(ShuffleMemoryManager.getMaxMemory(conf))

  /**
   * Try to acquire numBytes memory for the current thread, or return false if the pool cannot
   * allocate this much memory to it. This call may block until there is enough free memory in
   * some situations, to make sure each thread has a chance to ramp up to a reasonable share of
   * the available memory before being forced to spill.
   */
  def tryToAcquire(numBytes: Long): Boolean = synchronized {
    val threadId = Thread.currentThread().getId

    // Add this thread to the threadMemory map just so we can keep an accurate count of the number
    // of active threads, to let other threads ramp down their memory in calls to tryToAcquire
    threadMemory.getOrElseUpdate(threadId, 0L)

    // Keep looping until we're either sure that we don't want to grant this request (because this
    // thread would have more than 1 / numActiveThreads of the memory) or we have enough free
    // memory to give it (we always let each thread get at least 1 / (2 * numActiveThreads)).
    while (true) {
      val numActiveThreads = threadMemory.keys.size
      val curMem = threadMemory(threadId)
      if (curMem + numBytes > maxMemory / numActiveThreads) {
        // We'd get more than 1 / numActiveThreads of the total memory; don't allow that
        return false
      }
      val bytesFree = maxMemory - threadMemory.values.sum
      if (bytesFree >= numBytes) {
        // Grant the request
        threadMemory(threadId) = curMem + numBytes
        // Notify other waiting threads because the # active of threads may have increased, so
        // they may cancel their current waits
        notifyAll()
        return true
      } else if (curMem + numBytes <= maxMemory / (2 * numActiveThreads)) {
        // This thread has so little memory that we want it to block and acquire a bigger
        // amount instead of cancelling the request. Wait on "this" for a thread to call notify.
        // Before doing the wait, however, also notify other current waiters in case our thread
        // becoming active just pushed them over the limit to give up their own waits.
        notifyAll()
        wait()
      } else {
        // Thread would have between 1 / (2 * numActiveThreads) and 1 / numActiveThreads memory
        return false
      }
    }
    false  // Never reached
  }

  /** Release numBytes bytes for the current thread. */
  def release(numBytes: Long): Unit = synchronized {
    val threadId = Thread.currentThread().getId
    val curMem = threadMemory.getOrElse(threadId, 0L)
    if (curMem < numBytes) {
      throw new SparkException(
        s"Internal error: release called on ${numBytes} bytes but thread only has ${curMem}")
    }
    threadMemory(threadId) -= numBytes
    notifyAll()  // Notify waiters who locked "this" in tryToAcquire that memory has freed
  }

  /** Release all memory for the current thread and mark it as inactive (e.g. when a task ends). */
  def releaseMemoryForThisThread(): Unit = synchronized {
    val threadId = Thread.currentThread().getId
    threadMemory.remove(threadId)
    notifyAll()  // Notify waiters who locked "this" in tryToAcquire that memory has freed
  }
}

private object ShuffleMemoryManager {
  def getMaxMemory(conf: SparkConf): Long = {
    val memoryFraction = conf.getDouble("spark.shuffle.memoryFraction", 0.2)
    val safetyFraction = conf.getDouble("spark.shuffle.safetyFraction", 0.8)
    (Runtime.getRuntime.maxMemory * memoryFraction * safetyFraction).toLong
  }
}
