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

import scala.collection.mutable

/**
 * MedianHeap inserts number by O(log n) and returns the median by O(1) time complexity.
 * The basic idea is to maintain two heaps: a maxHeap and a minHeap. The maxHeap stores
 * the smaller half of all numbers while the minHeap stores the larger half.  The sizes
 * of two heaps need to be balanced each time when a new number is inserted so that their
 * sizes will not be different by more than 1. Therefore each time when findMedian() is
 * called we check if two heaps have the same size. If they do, we should return the
 * average of the two top values of heaps. Otherwise we return the top of the heap which
 * has one more element.
 */

private[spark] class MedianHeap(implicit val ord: Ordering[Double]) {

  // Stores all the numbers less than the current median in a maxHeap,
  // i.e median is the maximum, at the root
  private[this] var maxHeap = mutable.PriorityQueue.empty[Double](ord)

  // Stores all the numbers greater than the current median in a minHeap,
  // i.e median is the minimum, at the root
  private[this] var minHeap = mutable.PriorityQueue.empty[Double](ord.reverse)

  // Returns if there is no element in MedianHeap.
  def isEmpty(): Boolean = {
    maxHeap.isEmpty && minHeap.isEmpty
  }

  // Size of MedianHeap.
  def size(): Int = {
    maxHeap.size + minHeap.size
  }

  // Insert a new number into MedianHeap.
  def insert(x: Double): Unit = {
    // If both heaps are empty, we arbitrarily insert it into a heap, let's say, the minHeap.
    if (isEmpty) {
      minHeap.enqueue(x)
    } else {
      // If the number is larger than current median, it should be inserted into minHeap,
      // otherwise maxHeap.
      if (x > findMedian) {
        minHeap.enqueue(x)
      } else {
        maxHeap.enqueue(x)
      }
    }
    rebalance()
  }

  // Re-balance the heaps.
  private[this] def rebalance(): Unit = {
    if (minHeap.size - maxHeap.size > 1) {
      maxHeap.enqueue(minHeap.dequeue())
    }
    if (maxHeap.size - minHeap.size > 1) {
      minHeap.enqueue(maxHeap.dequeue)
    }
  }

  // Returns the median of the numbers.
  def findMedian(): Double = {
    if (isEmpty) {
      throw new NoSuchElementException("MedianHeap is empty.")
    }
    if (minHeap.size == maxHeap.size) {
      (minHeap.head + maxHeap.head) / 2.0
    } else if (minHeap.size > maxHeap.size) {
      minHeap.head
    } else {
      maxHeap.head
    }
  }
}
