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

/**
 * Helper for MaxMinByK aggregate providing heap operations.
 * Heap operates on indices to avoid copying large values.
 * heapArr layout: [size, idx0, idx1, ..., idx(k-1)]
 *
 * Note: heapArr is Array[Any] containing boxed Integers because GenericArrayData
 * uses Array[Any] internally.
 */
object MaxMinByKHeap {

  def getSize(heapArr: Array[Any]): Int = heapArr(0).asInstanceOf[Int]

  def setSize(heapArr: Array[Any], size: Int): Unit = heapArr(0) = size

  def getIdx(heapArr: Array[Any], pos: Int): Int = heapArr(pos + 1).asInstanceOf[Int]

  def setIdx(heapArr: Array[Any], pos: Int, idx: Int): Unit = heapArr(pos + 1) = idx

  def swap(heapArr: Array[Any], i: Int, j: Int): Unit = {
    val tmp = getIdx(heapArr, i)
    setIdx(heapArr, i, getIdx(heapArr, j))
    setIdx(heapArr, j, tmp)
  }

  def siftUp(
      heapArr: Array[Any],
      pos: Int,
      orderings: Array[Any],
      compare: (Any, Any) => Int): Unit = {
    var current = pos
    while (current > 0) {
      val parent = (current - 1) / 2
      val curOrd = orderings(getIdx(heapArr, current))
      val parOrd = orderings(getIdx(heapArr, parent))

      if (compare(curOrd, parOrd) < 0) {
        swap(heapArr, current, parent)
        current = parent
      } else {
        return
      }
    }
  }

  def siftDown(
      heapArr: Array[Any],
      pos: Int,
      size: Int,
      orderings: Array[Any],
      compare: (Any, Any) => Int): Unit = {
    var current = pos
    while (2 * current + 1 < size) {
      val left = 2 * current + 1
      val right = left + 1
      val leftOrd = orderings(getIdx(heapArr, left))
      val preferred = if (right < size) {
        val rightOrd = orderings(getIdx(heapArr, right))
        if (compare(rightOrd, leftOrd) < 0) right else left
      } else {
        left
      }

      val curOrd = orderings(getIdx(heapArr, current))
      val prefOrd = orderings(getIdx(heapArr, preferred))
      if (compare(curOrd, prefOrd) <= 0) {
        return
      }
  
      swap(heapArr, current, preferred)
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
      heapArr: Array[Any],
      compare: (Any, Any) => Int): Unit = {
    val size = getSize(heapArr)
    if (size < k) {
      valuesArr(size) = InternalRow.copyValue(value)
      orderingsArr(size) = InternalRow.copyValue(ord)
      setIdx(heapArr, size, size)
      siftUp(heapArr, size, orderingsArr, compare)
      setSize(heapArr, size + 1)
    } else if (compare(ord, orderingsArr(getIdx(heapArr, 0))) > 0) {
      val idx = getIdx(heapArr, 0)
      valuesArr(idx) = InternalRow.copyValue(value)
      orderingsArr(idx) = InternalRow.copyValue(ord)
      siftDown(heapArr, 0, size, orderingsArr, compare)
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

  /** Read-only view of array data, used during merge to read input buffer. */
  def readArray(arr: ArrayData, elementType: DataType): IndexedSeq[Any] = {
    val size = arr.numElements()
    (0 until size).map { i =>
      if (arr.isNullAt(i)) null else arr.get(i, elementType)
    }
  }
}
