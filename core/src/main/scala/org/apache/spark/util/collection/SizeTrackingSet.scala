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

import java.util.{Arrays, Comparator}
import scala.reflect._

/**
 * An set that keeps track of its estimated size in bytes.
 */
private[spark] class SizeTrackingSet[T : ClassTag]
  extends OpenHashSet[T] with SizeTracker
{
  
  // Triggered by destructiveSortedIterator; the underlying data array may no longer be used
  private var destroyed = false
  private val destructionMessage = "Set state is invalid from destructive sorting!"
  
  override def add(k: T) {
    assert(!destroyed, destructionMessage)
    super.add(k)
    super.afterUpdate()
  }
  
  override def rehash(k: T, allocateFunc: (Int) => Unit, moveFunc: (Int, Int) => Unit) {
    super.rehash(k, allocateFunc, moveFunc)
    resetSamples()
  }

  /**
   * Return an iterator of the set in sorted order. This provides a way to sort the map without
   * using additional memory, at the expense of destroying the validity of the map.
   */
  def destructiveSortedIterator(keyComparator: Comparator[T]): Iterator[T] = {
    destroyed = true
    // Pack keys into the front of the underlying array
    var keyIndex, newIndex = 0
    while (keyIndex < _capacity) {
      if (_data(keyIndex) != null) {
        _data(newIndex) = _data(keyIndex)
        newIndex += 1
      }
      keyIndex += 1
    }
    assert(size == newIndex) 

    new Sorter(new KeyArraySortDataFormat[T]).sort(_data, 0, newIndex, keyComparator)

    new Iterator[T] {
      var i = 0
      def hasNext: Boolean = i < newIndex
      def next(): T = {
        val item = _data(i).asInstanceOf[T]
        i += 1
        item
      }
    }
  }
}

/** Format ro sort a simple Array[K]. */
private [spark]
class KeyArraySortDataFormat[T : ClassTag] extends SortDataFormat[T, Array[T]] {
  override def getKey(data: Array[T], pos: Int): T = {
    data(pos).asInstanceOf[T]
  }
  
  override def swap(data: Array[T], pos0: Int, pos1: Int): Unit = {
    val tmp = data(pos0)
    data(pos0) = data(pos1)
    data(pos1) = tmp
  }
  
  override def copyElement(src: Array[T], srcPos: Int, dst: Array[T], dstPos: Int) {
    dst(dstPos) = src(srcPos)
  }
  
  /** Copy a range of elements starting at src(srcPos) to dest, starting at destPos. */
  override def copyRange(src: Array[T], srcPos: Int,
                                   dst: Array[T], dstPos: Int, length: Int) {
    System.arraycopy(src, srcPos, dst, dstPos, length)
  }

  /** Allocates a new structure that can hold up to 'length' elements. */
  override def allocate(length: Int): Array[T] = {
    new Array[T](length)
  }
}

