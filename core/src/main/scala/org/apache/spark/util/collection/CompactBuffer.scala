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
 * An append-only buffer similar to ArrayBuffer, but more memory-efficient for small buffers.
 * ArrayBuffer always allocates an Object array to store the data, with 16 entries by default,
 * so it has about 80-100 bytes of overhead. In contrast, CompactBuffer can keep up to two
 * elements in fields of the main object, and only allocates an Array[AnyRef] if there are more
 * entries than that. This makes it more efficient for operations like groupBy where we expect
 * some keys to have very few elements.
 */
private[spark] class CompactBuffer[T: ClassTag] extends Seq[T] with Serializable {
  // Number of elements, including our two in the main object
  private var curSize = 0

  // Array for extra elements
  private var otherElements: Array[T] = new Array[T](2)

  def apply(position: Int): T = {
    if (position < 0 || position >= curSize) {
      throw new IndexOutOfBoundsException
    }
    otherElements(position)
  }

  private def update(position: Int, value: T): Unit = {
    if (position < 0 || position >= curSize) {
      throw new IndexOutOfBoundsException
    }
    otherElements(position) = value
  }

  def += (value: T): CompactBuffer[T] = {
    val newIndex = curSize
    growToSize(curSize + 1)
    otherElements(newIndex) = value
    this
  }

  def ++= (values: TraversableOnce[T]): CompactBuffer[T] = {
    values match {
      // Optimize merging of CompactBuffers, used in cogroup and groupByKey
      case compactBuf: CompactBuffer[T] =>
        val oldSize = curSize
        // Copy the other buffer's size and elements to local variables in case it is equal to us
        val itsSize = compactBuf.curSize
        val itsElements = compactBuf.otherElements
        growToSize(curSize + itsSize)
        System.arraycopy(itsElements, 0, otherElements, oldSize, itsSize)

      case _ =>
        values.foreach(e => this += e)
    }
    this
  }

  override def length: Int = curSize

  override def size: Int = curSize

  override def iterator: Iterator[T] = new Iterator[T] {
    private var pos = 0
    override def hasNext: Boolean = pos < curSize
    override def next(): T = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      pos += 1
      apply(pos - 1)
    }
  }

  /** Increase our size to newSize and grow the backing array if needed. */
  private def growToSize(newSize: Int): Unit = {
    if (newSize < 0) {
      throw new UnsupportedOperationException("Can't grow buffer past Int.MaxValue elements")
    }
    val capacity = otherElements.length
    if (newSize > capacity) {
      var newArrayLen = 8
      while (newSize > newArrayLen) {
        newArrayLen *= 2
        if (newArrayLen == Int.MinValue) {
          // Prevent overflow if we double from 2^30 to 2^31, which will become Int.MinValue.
          // Note that we set the new array length to Int.MaxValue - 2 so that our capacity
          // calculation above still gives a positive integer.
          newArrayLen = Int.MaxValue
        }
      }
      val newArray = new Array[T](newArrayLen)
      System.arraycopy(otherElements, 0, newArray, 0, otherElements.length)
      otherElements = newArray
    }
    curSize = newSize
  }
}

private[spark] object CompactBuffer {
  def apply[T: ClassTag](): CompactBuffer[T] = new CompactBuffer[T]

  def apply[T: ClassTag](value: T): CompactBuffer[T] = {
    val buf = new CompactBuffer[T]
    buf += value
  }
}
