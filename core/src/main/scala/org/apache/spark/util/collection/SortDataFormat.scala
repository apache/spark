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

import scala.reflect.ClassTag

/**
 * Abstraction for sorting an arbitrary input buffer of data. This interface requires determining
 * the sort key for a given element index, as well as swapping elements and moving data from one
 * buffer to another.
 *
 * Example format: an array of numbers, where each element is also the key.
 * See [[KVArraySortDataFormat]] for a more exciting format.
 *
 * Note: Declaring and instantiating multiple subclasses of this class would prevent JIT inlining
 * overridden methods and hence decrease the shuffle performance.
 *
 * @tparam K Type of the sort key of each element
 * @tparam Buffer Internal data structure used by a particular format (e.g., Array[Int]).
 */
// TODO: Making Buffer a real trait would be a better abstraction, but adds some complexity.
private[spark]
abstract class SortDataFormat[K, Buffer] {

  /**
   * Creates a new mutable key for reuse. This should be implemented if you want to override
   * [[getKey(Buffer, Int, K)]].
   */
  def newKey(): K = null.asInstanceOf[K]

  /** Return the sort key for the element at the given index. */
  protected def getKey(data: Buffer, pos: Int): K

  /**
   * Returns the sort key for the element at the given index and reuse the input key if possible.
   * The default implementation ignores the reuse parameter and invokes [[getKey(Buffer, Int]].
   * If you want to override this method, you must implement [[newKey()]].
   */
  def getKey(data: Buffer, pos: Int, reuse: K): K = {
    getKey(data, pos)
  }

  /** Swap two elements. */
  def swap(data: Buffer, pos0: Int, pos1: Int): Unit

  /** Copy a single element from src(srcPos) to dst(dstPos). */
  def copyElement(src: Buffer, srcPos: Int, dst: Buffer, dstPos: Int): Unit

  /**
   * Copy a range of elements starting at src(srcPos) to dst, starting at dstPos.
   * Overlapping ranges are allowed.
   */
  def copyRange(src: Buffer, srcPos: Int, dst: Buffer, dstPos: Int, length: Int): Unit

  /**
   * Allocates a Buffer that can hold up to 'length' elements.
   * All elements of the buffer should be considered invalid until data is explicitly copied in.
   */
  def allocate(length: Int): Buffer
}

/**
 * Supports sorting an array of key-value pairs where the elements of the array alternate between
 * keys and values, as used in [[AppendOnlyMap]].
 *
 * @tparam K Type of the sort key of each element
 * @tparam T Type of the Array we're sorting. Typically this must extend AnyRef, to support cases
 *           when the keys and values are not the same type.
 */
private[spark]
class KVArraySortDataFormat[K, T <: AnyRef : ClassTag] extends SortDataFormat[K, Array[T]] {

  override def getKey(data: Array[T], pos: Int): K = data(2 * pos).asInstanceOf[K]

  override def swap(data: Array[T], pos0: Int, pos1: Int): Unit = {
    val tmpKey = data(2 * pos0)
    val tmpVal = data(2 * pos0 + 1)
    data(2 * pos0) = data(2 * pos1)
    data(2 * pos0 + 1) = data(2 * pos1 + 1)
    data(2 * pos1) = tmpKey
    data(2 * pos1 + 1) = tmpVal
  }

  override def copyElement(src: Array[T], srcPos: Int, dst: Array[T], dstPos: Int): Unit = {
    dst(2 * dstPos) = src(2 * srcPos)
    dst(2 * dstPos + 1) = src(2 * srcPos + 1)
  }

  override def copyRange(src: Array[T], srcPos: Int,
      dst: Array[T], dstPos: Int, length: Int): Unit = {
    System.arraycopy(src, 2 * srcPos, dst, 2 * dstPos, 2 * length)
  }

  override def allocate(length: Int): Array[T] = {
    new Array[T](2 * length)
  }
}
