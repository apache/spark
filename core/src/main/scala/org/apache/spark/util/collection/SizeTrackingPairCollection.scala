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
 * A common interface for our size-tracking collections of key-value pairs, which are used in
 * external operations. These all support estimating the size and obtaining a memory-efficient
 * sorted iterator.
 */
// TODO: should extend Iterable[Product2[K, V]] instead of (K, V)
private[spark] trait SizeTrackingPairCollection[K, V] extends Iterable[(K, V)] {
  /** Estimate the collection's current memory usage in bytes. */
  def estimateSize(): Long

  /** Iterate through the data in a given key order. This may destroy the underlying collection. */
  def destructiveSortedIterator(keyComparator: Comparator[K]): Iterator[(K, V)]
}
