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

import java.util.Comparator

/**
 * A common interface for our size-tracking collections, which are used in external operations.
 * These all support estimating the size, checking when the collection is at a grow threshold
 * (so it will require much more memory than it did previously), and obtaining a sorted iterator
 *
 */
trait SizeTrackingCollection[T] extends Iterable[T] {
  /**
   * Will the collection grow its underlying storage capacity the next time we do an insert?
   * Collections implementing this usually double in capacity so this is a big jump in memory use.
   */
  def atGrowThreshold: Boolean

  /** Estimate the collection's current memory usage in bytes. */
  def estimateSize(): Long

  /** Iterate through the data in a given order. This may destroy the underlying collection. */
  def destructiveSortedIterator(cmp: Comparator[T]): Iterator[T]
}
