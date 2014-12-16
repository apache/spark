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
 * Append-only buffer of key-value pairs that keeps track of its estimated size in bytes.
 */
private[spark] class SizeTrackingPairBuffer[K, V](initialCapacity: Int = 64)
  extends SizeTracker with SizeTrackingPairCollection[K, V]
{
  require(initialCapacity <= (1 << 29), "Can't make capacity bigger than 2^29 elements")
  require(initialCapacity >= 1, "Invalid initial capacity")

  // Basic growable array data structure. We use a single array of AnyRef to hold both the keys
  // and the values, so that we can sort them efficiently with KVArraySortDataFormat.
  private var capacity = initialCapacity
  private var curSize = 0
  private var data = new Array[AnyRef](2 * initialCapacity)

  /** Add an element into the buffer */
  def insert(key: K, value: V): Unit = {
    if (curSize == capacity) {
      growArray()
    }
    data(2 * curSize) = key.asInstanceOf[AnyRef]
    data(2 * curSize + 1) = value.asInstanceOf[AnyRef]
    curSize += 1
    afterUpdate()
  }

  /** Total number of elements in buffer */
  override def size: Int = curSize

  /** Iterate over the elements of the buffer */
  override def iterator: Iterator[(K, V)] = new Iterator[(K, V)] {
    var pos = 0

    override def hasNext: Boolean = pos < curSize

    override def next(): (K, V) = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val pair = (data(2 * pos).asInstanceOf[K], data(2 * pos + 1).asInstanceOf[V])
      pos += 1
      pair
    }
  }

  /** Double the size of the array because we've reached capacity */
  private def growArray(): Unit = {
    if (capacity == (1 << 29)) {
      // Doubling the capacity would create an array bigger than Int.MaxValue, so don't
      throw new Exception("Can't grow buffer beyond 2^29 elements")
    }
    val newCapacity = capacity * 2
    val newArray = new Array[AnyRef](2 * newCapacity)
    System.arraycopy(data, 0, newArray, 0, 2 * capacity)
    data = newArray
    capacity = newCapacity
    resetSamples()
  }

  /** Iterate through the data in a given order. For this class this is not really destructive. */
  override def destructiveSortedIterator(keyComparator: Comparator[K]): Iterator[(K, V)] = {
    new Sorter(new KVArraySortDataFormat[K, AnyRef]).sort(data, 0, curSize, keyComparator)
    iterator
  }
}
