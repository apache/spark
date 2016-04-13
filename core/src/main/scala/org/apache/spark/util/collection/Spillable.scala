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

package org.apache.spark.util.collection

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}

/**
 * Spills contents of an in-memory collection to disk when the memory threshold
 * has been exceeded.
 */
private[spark] trait Spillable[C] extends Logging {
  /**
   * Spills the current in-memory collection to disk, and releases the memory.
   */
  protected def spillCollection(): Unit

  /**
   * After a spill, reset any internal data structures so they are ready to accept more input data
   */
  protected def resetAfterSpill(): Unit

  /**
    * return an estimate of the current memory used by the collection.
    *
    * Note this is *not* the same as the memory requested from the memory manager, for two reasons:
    * (1) If we allow the collection to use some initial amount of memory that is untracked, that
    * should still be reported here. (which would lead to this amount being larger than what is
    * tracked by the memory manager.)
    * (2) If we've just requested a large increase in memory from the memory manager, but aren't
    * actually *using* that memory yet, we will not report it here (which would lead to this amount
    * being smaller than what is tracked by the memory manager.)
    */
  def estimateUsedMemory(): Long

  /**
   * Spills the in-memory collection, releases memory, and updates metrics.  This can be
   * used to force a spill, even if this collection beleives it still has extra memory, to
   * free up memory for other operators.  For example, during a stage which does a shuffle-read
   * and a shuffle-write, after the shuffle-read is finished, we can spill to free up memory
   * for the shuffle-write.
   * [[maybeSpill]] can be used when the collection
   * should only spill if it doesn't have enough memory
   */
  final def spill(): Unit = {
    spill(estimateUsedMemory())
  }

  final def spill(currentMemory: Long): Unit = {
    _spillCount += 1
    logSpillage(currentMemory)
    spillCollection()
    _elementsRead = 0
    _memoryBytesSpilled += currentMemory
    releaseMemory()
    resetAfterSpill()
  }

  // Number of elements read from input since last spill
  protected def elementsRead: Long = _elementsRead

  // Called by subclasses every time a record is read
  // It's used for checking spilling frequency
  protected def addElementsRead(): Unit = { _elementsRead += 1 }

  // Memory manager that can be used to acquire/release memory
  protected[this] def taskMemoryManager: TaskMemoryManager

  // Initial threshold for the size of a collection before we start tracking its memory usage
  // For testing only
  private[this] val initialMemoryThreshold: Long =
    SparkEnv.get.conf.getLong("spark.shuffle.spill.initialMemoryThreshold", 5 * 1024 * 1024)

  // Force this collection to spill when there are this many elements in memory
  // For testing only
  private[this] val numElementsForceSpillThreshold: Long =
    SparkEnv.get.conf.getLong("spark.shuffle.spill.numElementsForceSpillThreshold", Long.MaxValue)

  // Threshold for this collection's size in bytes before we start tracking its memory usage
  // To avoid a large number of small spills, initialize this to a value orders of magnitude > 0
  private[this] var myMemoryThreshold = initialMemoryThreshold

  /**
   * The amount of memory that has been allocated to this Spillable by the memory manager.
   *
   * Note that this is *not* the same as [[estimateUsedMemory]] -- see the doc on that method
   * for why these differ
   */
  def allocatedMemory: Long = {
    // we don't ever request initialMemoryThreshold from the memory manager
    myMemoryThreshold - initialMemoryThreshold
  }

  // Number of elements read from input since last spill
  private[this] var _elementsRead = 0L

  // Number of bytes spilled in total
  private[this] var _memoryBytesSpilled = 0L

  // Number of spills
  private[this] var _spillCount = 0

  private[this] val memoryConsumer = new SpillableMemoryConsumer(this, taskMemoryManager)

  /**
   * Spills the current in-memory collection to disk if needed. Attempts to acquire more
   * memory before spilling.  If this does spill, it will call [[resetAfterSpill()]] to
   * prepare the in-memory data structures to accept more data
   *
   * @param currentMemory estimated size of the collection in bytes
   * @return true if `collection` was spilled to disk; false otherwise
   */
  protected def maybeSpill(currentMemory: Long): Boolean = {
    var shouldSpill = false
    if (elementsRead % 32 == 0 && currentMemory >= myMemoryThreshold) {
      // Claim up to double our current memory from the shuffle memory pool
      val amountToRequest = 2 * currentMemory - myMemoryThreshold
      val granted =
        taskMemoryManager.acquireExecutionMemory(amountToRequest, MemoryMode.ON_HEAP,
          memoryConsumer)
      myMemoryThreshold += granted
      // If we were granted too little memory to grow further (either tryToAcquire returned 0,
      // or we already had more memory than myMemoryThreshold), spill the current collection
      shouldSpill = currentMemory >= myMemoryThreshold
    }
    shouldSpill = shouldSpill || _elementsRead > numElementsForceSpillThreshold
    // Actually spill
    if (shouldSpill) {
      spill(currentMemory)
    }
    shouldSpill
  }

  /**
   * @return number of bytes spilled in total
   */
  def memoryBytesSpilled: Long = _memoryBytesSpilled

  /**
   * Release our memory back to the execution pool so that other tasks can grab it.
   */
  def releaseMemory(): Unit = {
    // The amount we requested does not include the initial memory tracking threshold
    taskMemoryManager.releaseExecutionMemory(
      myMemoryThreshold - initialMemoryThreshold, MemoryMode.ON_HEAP, memoryConsumer)
    myMemoryThreshold = initialMemoryThreshold
  }

  /**
   * Prints a standard log message detailing spillage.
   *
   * @param size number of bytes spilled
   */
  @inline private def logSpillage(size: Long) {
    val threadId = Thread.currentThread().getId
    logInfo("Thread %d spilling in-memory map of %s to disk (%d time%s so far)"
      .format(threadId, org.apache.spark.util.Utils.bytesToString(size),
        _spillCount, if (_spillCount > 1) "s" else ""))
  }
}

/**
  * A light-wrapper around Spillables to implement MemoryConsumer, just so that
  * they can be tracked and logged in TaskMemoryManager.
  *
  * Note that this does *not* give cooperative memory management for Spillables, its just to
  * make debug logs clearly on memory usage.
  */
class SpillableMemoryConsumer(val sp: Spillable[_], val taskMM: TaskMemoryManager)
    extends MemoryConsumer(taskMM) with Logging {
  def spill(size: Long, trigger: MemoryConsumer): Long = {
    // If another memory consumer requests more memory, we can't easily spill here.  The
    // problem is that even if we do spill, there may be an iterator that is already
    // reading from the in-memory data structures, which would hold a reference to that
    // object even if we spilled.  So even if we spilled, we aren't *actually* freeing memory
    // unless we update any in-flight iterators to switch to the spilled data
    logDebug(s"Spill requested for ${sp} (TID ${taskMemoryManager.getTaskAttemptId}) by " +
      s"${trigger}, but ${this} can't spill")
    0L
  }

  override def toString(): String = {
    s"SpillableConsumer($sp)"
  }

  override def getUsed(): Long = {
    sp.allocatedMemory
  }

}
