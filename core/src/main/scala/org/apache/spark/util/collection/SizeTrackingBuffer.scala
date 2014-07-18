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

import java.util.Arrays
import java.util.Comparator

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.util.SizeEstimator

/**
 * Append-only buffer that keeps track of its estimated size in bytes.
 * We sample with a slow exponential back-off using the SizeEstimator to amortize the time,
 * as each call to SizeEstimator can take a sizable amount of time (order of a few milliseconds).
 *
 * The tracking code is copied from SizeTrackingAppendOnlyMap -- we'll factor that out soon.
 */
private[spark] class SizeTrackingBuffer[T <: AnyRef](initialCapacity: Int = 64)
  extends SizeTrackingCollection[T]
{
  // Basic growable array data structure. NOTE: We use an Array of AnyRef because Arrays.sort()
  // is not easy to call on an Array[T], and Scala doesn't provide a great way to sort a generic
  // array using a Comparator.
  private var capacity = initialCapacity
  private var curSize = 0
  private var data = new Array[AnyRef](initialCapacity)

  // Size-tracking variables: we maintain a sequence of samples since the size of the collection
  // depends on both the array and how many of its elements are filled. We reset this each time
  // we grow the array since the ratio of null vs non-null elements will change.

  private case class Sample(bytes: Long, numUpdates: Long)

  /**
   * Controls the base of the exponential which governs the rate of sampling.
   * E.g., a value of 2 would mean we sample at 1, 2, 4, 8, ... elements.
   */
  private val SAMPLE_GROWTH_RATE = 1.1

  /** All samples taken since last resetSamples(). Only the last two are used for extrapolation. */
  private val samples = new ArrayBuffer[Sample]()

  /** Number of insertions into the buffer since the last resetSamples(). */
  private var numUpdates: Long = _

  /** The value of 'numUpdates' at which we will take our next sample. */
  private var nextSampleNum: Long = _

  /** The average number of bytes per update between our last two samples. */
  private var bytesPerUpdate: Double = _

  resetSamples()

  /** Add an element into the buffer */
  def += (element: T): Unit = {
    if (curSize == capacity) {
      growArray()
    }
    data(curSize) = element
    curSize += 1
    numUpdates += 1
    if (nextSampleNum == numUpdates) {
      takeSample()
    }
  }

  /** Total number of elements in buffer */
  override def size: Int = curSize

  /** Iterate over the elements of the buffer */
  override def iterator: Iterator[T] = new Iterator[T] {
    var pos = 0

    override def hasNext: Boolean = pos < curSize

    override def next(): T = {
      val elem = data(pos)
      pos += 1
      elem.asInstanceOf[T]
    }
  }

  /** Whether the next insert will cause the underlying array to grow */
  def atGrowThreshold: Boolean = curSize == capacity

  /** Estimate the current size of the buffer in bytes. O(1) time. */
  def estimateSize(): Long = {
    assert(samples.nonEmpty)
    val extrapolatedDelta = bytesPerUpdate * (numUpdates - samples.last.numUpdates)
    (samples.last.bytes + extrapolatedDelta).toLong
  }

  /** Double the size of the array because we've reached capacity */
  private def growArray(): Unit = {
    if (capacity == (1 << 30)) {
      // Doubling the capacity would create an array bigger than Int.MaxValue, so don't
      throw new Exception("Can't grow buffer beyond 2^30 elements")
    }
    val newCapacity = capacity * 2
    val newArray = new Array[AnyRef](newCapacity)
    System.arraycopy(data, 0, newArray, 0, capacity)
    data = newArray
    capacity = newCapacity
    resetSamples()
  }

  /** Called after the buffer grows in size, as this creates many new null elements. */
  private def resetSamples() {
    numUpdates = 1
    nextSampleNum = 1
    samples.clear()
    takeSample()
  }

  /** Takes a new sample of the current buffer's size. */
  private def takeSample() {
    samples += Sample(SizeEstimator.estimate(this), numUpdates)
    // Only use the last two samples to extrapolate. If fewer than 2 samples, assume no change.
    bytesPerUpdate = 0
    if (samples.length >= 2) {
      val last = samples(samples.length - 1)
      val prev = samples(samples.length - 2)
      bytesPerUpdate = (last.bytes - prev.bytes).toDouble / (last.numUpdates - prev.numUpdates)
      bytesPerUpdate = math.max(0, bytesPerUpdate)
    }
    nextSampleNum = math.ceil(numUpdates * SAMPLE_GROWTH_RATE).toLong
  }

  /** Iterate through the data in a given order. For this class this is not really destructive. */
  override def destructiveSortedIterator(cmp: Comparator[T]): Iterator[T] = {
    Arrays.sort(data, 0, curSize, cmp.asInstanceOf[Comparator[AnyRef]])
    iterator
  }
}
