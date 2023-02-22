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
 * PercentileHeap tracks the percentile of a collection of numbers.
 *
 * Insertion is O(log n), Lookup is O(1).
 *
 * The implementation keeps two heaps: a bottom heap (`botHeap`) and a top heap (`topHeap`). The
 * bottom heap stores all the numbers below the percentile and the top heap stores the ones above
 * the percentile. During insertion the relative sizes of the heaps are adjusted to match the
 * target percentile.
 */
private[spark] class PercentileHeap(percentage: Double = 0.5) {
  assert(percentage > 0 && percentage < 1)

  private[this] val topHeap = PriorityQueue.empty[Double](Ordering[Double].reverse)
  private[this] val botHeap = PriorityQueue.empty[Double](Ordering[Double])

  def isEmpty(): Boolean = botHeap.isEmpty && topHeap.isEmpty

  def size(): Int = botHeap.size + topHeap.size

  /**
   * Returns percentile of the inserted elements as if the inserted elements were sorted and we
   * returned `sorted((sorted.length * percentage).toInt)`.
   */
  def percentile(): Double = {
    if (isEmpty) throw new NoSuchElementException("empty")
    topHeap.head
  }

  def insert(x: Double): Unit = {
    if (isEmpty) {
      topHeap.enqueue(x)
    } else {
      val p = topHeap.head
      val growBot = ((size + 1) * percentage).toInt > botHeap.size
      if (growBot) {
        if (x < p) {
          botHeap.enqueue(x)
        } else {
          topHeap.enqueue(x)
          botHeap.enqueue(topHeap.dequeue)
        }
      } else {
        if (x < p) {
          botHeap.enqueue(x)
          topHeap.enqueue(botHeap.dequeue())
        } else {
          topHeap.enqueue(x)
        }
      }
    }
  }
}
