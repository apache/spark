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

import scala.collection.mutable.PriorityQueue

/**
 * PercentileHeap is designed to be used to quickly track the percentile of a group of numbers
 * that may contain duplicates. Inserting a new number has O(log n) time complexity and
 * determining the percentile has O(1) time complexity.
 * The basic idea is to maintain two heaps: a smallerHalf and a largerHalf. The smallerHalf
 * stores the smaller half of all numbers while the largerHalf stores the larger half.
 * The sizes of two heaps need to match the percentage each time when a new number is inserted so
 * that the ratio of their sizes is percentage to (1 - percentage). Therefore each time when
 * percentile() is called we check if the sizes of two heaps match the percentage. If they do,
 * we should return the average of the two top values of heaps. Otherwise we return the top of the
 * heap which exceeds its percentage.
 */
private[spark] class PercentileHeap(percentage: Double = 0.5)(implicit val ord: Ordering[Double]) {
  assert(percentage >= 0 && percentage <= 1)

  /**
   * Stores all the numbers less than the current percentile in a smallerHalf,
   * i.e percentile is the maximum, at the root.
   */
  private[this] val smallerHalf = PriorityQueue.empty[Double](ord)

  /**
   * Stores all the numbers greater than the current percentile in a largerHalf,
   * i.e percentile is the minimum, at the root.
   */
  private[this] val largerHalf = PriorityQueue.empty[Double](ord.reverse)

  def isEmpty(): Boolean = {
    smallerHalf.isEmpty && largerHalf.isEmpty
  }

  def size(): Int = {
    smallerHalf.size + largerHalf.size
  }

  // Exposed for testing.
  def smallerSize(): Int = smallerHalf.size

  def insert(x: Double): Unit = {
    // If both heaps are empty, we insert it to the heap that has larger percentage.
    if (isEmpty) {
      if (percentage < 0.5) smallerHalf.enqueue(x) else largerHalf.enqueue(x)
    } else {
      // If the number is larger than current percentile, it should be inserted into largerHalf,
      // otherwise smallerHalf.
      if (x > percentile) {
        largerHalf.enqueue(x)
      } else {
        smallerHalf.enqueue(x)
      }
    }
    rebalance()
  }

  // Calculate the deviation between the ratio of smaller heap size to larger heap size and the
  // expected ratio, which is percentage : (1 - percentage). Negative result means the smaller
  // heap has too less elements, positive result means the smaller heap has too many elements.
  private def calculateDeviation(smallerSize: Int, largerSize: Int): Double = {
    smallerSize * (1 - percentage) - largerSize * percentage
  }

  private[this] def rebalance(): Unit = {
    // If moving one value from heap to the other heap can fit the percentage better, then
    // move it.
    val currentDev = calculateDeviation(smallerHalf.size, largerHalf.size)
    if (currentDev > 0) {
      val newDev = calculateDeviation(smallerHalf.size - 1, largerHalf.size + 1)
      if (math.abs(newDev) < currentDev) {
        largerHalf.enqueue(smallerHalf.dequeue)
      }
    }
    if (currentDev < 0) {
      val newDev = calculateDeviation(smallerHalf.size + 1, largerHalf.size - 1)
      if (math.abs(newDev) < -currentDev) {
        smallerHalf.enqueue(largerHalf.dequeue())
      }
    }
  }

  def percentile: Double = {
    if (isEmpty) {
      throw new NoSuchElementException("PercentileHeap is empty.")
    }
    val dev = calculateDeviation(smallerHalf.size, largerHalf.size)
    // If the deviation is very small, we take the average of the top elements from the two heaps
    // as the percentile.
    if (smallerHalf.nonEmpty && largerHalf.nonEmpty && math.abs(dev / size) < 0.01) {
      (largerHalf.head + smallerHalf.head) / 2.0
    } else if (dev < 0) {
      largerHalf.head
    } else {
      smallerHalf.head
    }
  }
}
