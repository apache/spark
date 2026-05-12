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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.Platform

/**
 * Helper for MaxMinByK aggregate providing heap operations.
 * Heap operates on indices to avoid copying large values.
 *
 * Binary heap layout: [size (4 bytes), idx0 (4 bytes), idx1 (4 bytes), ..., idx(k-1) (4 bytes)]
 * Total size: (k + 1) * 4 bytes
 *
 * All integers are stored in native byte order via Platform (Unsafe) for direct access.
 */
private[catalyst] object MaxMinByKHeap {

  def getSize(heap: Array[Byte]): Int =
    Platform.getInt(heap, Platform.BYTE_ARRAY_OFFSET)

  def setSize(heap: Array[Byte], size: Int): Unit =
    Platform.putInt(heap, Platform.BYTE_ARRAY_OFFSET, size)

  def getIdx(heap: Array[Byte], pos: Int): Int =
    Platform.getInt(heap, Platform.BYTE_ARRAY_OFFSET + (pos + 1).toLong * 4)

  def setIdx(heap: Array[Byte], pos: Int, idx: Int): Unit =
    Platform.putInt(heap, Platform.BYTE_ARRAY_OFFSET + (pos + 1).toLong * 4, idx)

  def swap(heap: Array[Byte], i: Int, j: Int): Unit = {
    val tmp = getIdx(heap, i)
    setIdx(heap, i, getIdx(heap, j))
    setIdx(heap, j, tmp)
  }

  def siftUp(
      heap: Array[Byte],
      pos: Int,
      orderings: Array[Any],
      compare: (Any, Any) => Int): Unit = {
    var current = pos
    while (current > 0) {
      val parent = (current - 1) / 2
      val curOrd = orderings(getIdx(heap, current))
      val parOrd = orderings(getIdx(heap, parent))

      if (compare(curOrd, parOrd) < 0) {
        swap(heap, current, parent)
        current = parent
      } else {
        return
      }
    }
  }

  def siftDown(
      heap: Array[Byte],
      pos: Int,
      size: Int,
      orderings: Array[Any],
      compare: (Any, Any) => Int): Unit = {
    var current = pos
    while (2 * current + 1 < size) {
      val left = 2 * current + 1
      val right = left + 1
      val leftOrd = orderings(getIdx(heap, left))

      val preferred = if (right < size) {
        val rightOrd = orderings(getIdx(heap, right))
        if (compare(rightOrd, leftOrd) < 0) right else left
      } else {
        left
      }

      val curOrd = orderings(getIdx(heap, current))
      val prefOrd = orderings(getIdx(heap, preferred))
      if (compare(curOrd, prefOrd) <= 0) {
        return
      }

      swap(heap, current, preferred)
      current = preferred
    }
  }

  /**
   * Insert element into heap. If heap is full, replaces root if new element is better.
   */
  def insert(
      value: Any,
      ord: Any,
      k: Int,
      valuesArr: Array[Any],
      orderingsArr: Array[Any],
      heap: Array[Byte],
      compare: (Any, Any) => Int): Unit = {
    val size = getSize(heap)
    if (size < k) {
      valuesArr(size) = InternalRow.copyValue(value)
      orderingsArr(size) = InternalRow.copyValue(ord)

      setIdx(heap, size, size)
      siftUp(heap, size, orderingsArr, compare)
      setSize(heap, size + 1)
    } else if (compare(ord, orderingsArr(getIdx(heap, 0))) > 0) {
      val idx = getIdx(heap, 0)
      valuesArr(idx) = InternalRow.copyValue(value)
      orderingsArr(idx) = InternalRow.copyValue(ord)

      siftDown(heap, 0, size, orderingsArr, compare)
    }
  }

  /**
   * Get mutable array from buffer for in-place updates.
   * Converts UnsafeArrayData (after spill) to GenericArrayData.
   */
  def getMutableArray(buffer: InternalRow, offset: Int, elementType: DataType): Array[Any] = {
    buffer.getArray(offset) match {
      case g: GenericArrayData =>
        g.array.asInstanceOf[Array[Any]]
      case other =>
        val size = other.numElements()
        val newArr = new Array[Any](size)

        for (i <- 0 until size) {
          if (!other.isNullAt(i)) {
            newArr(i) = InternalRow.copyValue(other.get(i, elementType))
          }
        }

        val newArrayData = new GenericArrayData(newArr)
        buffer.update(offset, newArrayData)
        newArr
    }
  }

  /**
   * Get mutable heap binary buffer from buffer for in-place updates.
   */
  def getMutableHeap(buffer: InternalRow, offset: Int): Array[Byte] = {
    buffer.getBinary(offset)
  }

  /** Read-only view of array data, used during merge to read input buffer. */
  def readArray(arr: ArrayData, elementType: DataType): IndexedSeq[Any] = {
    val size = arr.numElements()
    (0 until size).map { i =>
      if (arr.isNullAt(i)) null else arr.get(i, elementType)
    }
  }
}
