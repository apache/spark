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
import scala.language.reflectiveCalls

/**
 * Spills contents of an in-memory collection to disk when the memory threshold
 * has been exceeded.
 *
 * @tparam C collection type that provides a size estimate
 */
private[spark] trait Spillable[C <: { def estimateSize(): Long }] {

  // Number of elements read from input since last spill
  protected[this] var elementsRead: Long

  // Memory manager that can be used to acquire/release memory
  private[this] val shuffleMemoryManager = SparkEnv.get.shuffleMemoryManager

  // What threshold of elementsRead we start estimating collection size at
  private[this] val trackMemoryThreshold = 1000

  // How much of the shared memory pool this collection has claimed
  private[this] var myMemoryThreshold = 0L

  // Number of bytes spilled in total
  private[this] var _memoryBytesSpilled = 0L

  // Number of spills
  private[this] var _spillCount = 0

  /**
   * Spills the current in-memory collection to disk if needed. Attempts to acquire more
   * memory before spilling.
   *
   * @tparam A type of collection to be spilled
   * @return if spilled, a new empty collection instance; otherwise, the same collection instance
   */
  protected[this] def maybeSpill[A <: C](collection: A): A = {
    if (elementsRead > trackMemoryThreshold && elementsRead % 32 == 0 &&
        collection.estimateSize() >= myMemoryThreshold) {
      // Claim up to double our current memory from the shuffle memory pool
      val currentMemory = collection.estimateSize()
      val amountToRequest = 2 * currentMemory - myMemoryThreshold
      val granted = shuffleMemoryManager.tryToAcquire(amountToRequest)
      myMemoryThreshold += granted
      if (myMemoryThreshold <= currentMemory) {
        // We were granted too little memory to grow further (either tryToAcquire returned 0,
        // or we already had more memory than myMemoryThreshold); spill the current collection
        _spillCount += 1
        val empty = spill[A](collection)
        _memoryBytesSpilled += currentMemory
        release()
        return empty
      }
    }
    collection
  }

  /**
   * Spills the current in-memory collection to disk, and releases the memory.
   *
   * @param collection collection to spill to disk
   * @return new, empty collection
   */
  protected[this] def spill[A <: C](collection: A): A

  /**
   * @return total number of times this collection was spilled
   */
  protected[this] def spillCount: Int = _spillCount

  /**
   * @return number of bytes spilled in total
   */
  def memoryBytesSpilled: Long = _memoryBytesSpilled

  /**
   * Release our memory back to the shuffle pool so that other threads can grab it.
   */
  private[this] def release(): Unit = {
    shuffleMemoryManager.release(myMemoryThreshold)
    myMemoryThreshold = 0L
  }
}
