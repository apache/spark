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

package org.apache.spark.sql.execution.window

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray
import org.apache.spark.sql.types._

/**
 * An optimized sliding window frame that calculates min and/or max aggregate functions
 * using monotonic deques. This provides O(N) time complexity instead of O(N * W) of
 * [[SlidingWindowFunctionFrame]] or O(N log W) of [[SegmentTreeWindowFunctionFrame]].
 */
private[window] final class SlidingWindowMinMaxFunctionFrame(
    target: InternalRow,
    processor: AggregateProcessor,
    lbound: BoundOrdering,
    ubound: Option[BoundOrdering],
    functions: Array[Expression],
    inputSchema: Seq[Attribute])
  extends WindowFunctionFrame {

  /** Rows of the partition currently being processed. */
  private[this] var input: ExternalAppendOnlyUnsafeRowArray = null

  /** Iterators over the [[input]] */
  private[this] var lowerIterator: Iterator[UnsafeRow] = _
  private[this] var inputIterator: Iterator[UnsafeRow] = _

  /** The row at lowerBound. */
  private[this] var lowerRow: UnsafeRow = null

  /** The next row from `input`. */
  private[this] var nextRow: InternalRow = null

  /** Index of the first input row with a value equal to or greater than the lower bound of the
   * current output row.
   */
  private[this] var lowerBound = 0

  /** Index of the first input row with a value greater than the upper bound of the current
   * output row.
   */
  private[this] var upperBound = 0

  private[this] val sourceRow = new SpecificInternalRow(functions.map(_.dataType).toIndexedSeq)

  private[this] val deques: Array[MinMaxDeque] = functions.zipWithIndex.map {
    case (func, i) =>
      val isMin = func.isInstanceOf[Min]
      val child = func match {
        case m: Min => m.child
        case m: Max => m.child
      }
      val boundChild = BindReferences.bindReference(child, inputSchema)
      val ordering = TypeUtils.getInterpretedOrdering(child.dataType)
      new MinMaxDeque(isMin, boundChild, child.dataType, ordering, i)
  }

  override def prepare(rows: ExternalAppendOnlyUnsafeRowArray): Unit = {
    input = rows
    lowerIterator = input.generateIterator()
    lowerRow = WindowFunctionFrame.getNextOrNull(lowerIterator)
    deques.foreach(_.clear())
    lowerBound = 0

    if (ubound.isEmpty) {
      val iter = input.generateIterator()
      var idx = 0
      while (iter.hasNext) {
        val row = iter.next()
        deques.foreach(_.admit(row, idx))
        idx += 1
      }
      upperBound = input.length
      nextRow = null
      inputIterator = null
    } else {
      inputIterator = input.generateIterator()
      nextRow = WindowFunctionFrame.getNextOrNull(inputIterator)
      upperBound = 0
    }
  }

  override def write(index: Int, current: InternalRow): Unit = {
    var bufferUpdated = index == 0

    // Drop all rows from the buffer for which the input row value is smaller than
    // the output row lower bound.
    while (lowerBound < upperBound && lbound.compare(lowerRow, lowerBound, current, index) < 0) {
      lowerBound += 1
      lowerRow = WindowFunctionFrame.getNextOrNull(lowerIterator)
      bufferUpdated = true
    }

    // Add all rows to the buffer for which the input row value is equal to or less than
    // the output row upper bound.
    if (ubound.isDefined) {
      val ub = ubound.get
      while (nextRow != null && ub.compare(nextRow, upperBound, current, index) <= 0) {
        if (lbound.compare(nextRow, lowerBound, current, index) < 0) {
          lowerBound += 1
          lowerRow = WindowFunctionFrame.getNextOrNull(lowerIterator)
        } else {
          deques.foreach(_.admit(nextRow, upperBound))
          bufferUpdated = true
        }
        nextRow = WindowFunctionFrame.getNextOrNull(inputIterator)
        upperBound += 1
      }
    }

    if (bufferUpdated) {
      deques.foreach(_.dropBefore(lowerBound))
    }

    // Write output values to target.
    if (processor != null && bufferUpdated) {
      var i = 0
      while (i < deques.length) {
        sourceRow.update(i, deques(i).currentValue())
        i += 1
      }
      processor.evaluate(sourceRow, target)
    }
  }

  override def currentLowerBound(): Int = lowerBound

  override def currentUpperBound(): Int = upperBound

  private class MinMaxDeque(
      val isMin: Boolean,
      val boundChild: Expression,
      val dataType: DataType,
      val ordering: Ordering[Any],
      val bufferIndex: Int) {

    private var capacity = 16
    private var values = new Array[Any](capacity)
    private var indices = new Array[Int](capacity)
    private var head = 0
    private var tail = 0
    private var size = 0

    private val tempRow = new SpecificInternalRow(Seq(dataType))
    private val isPrimitive = dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
           DateType | TimestampType | TimestampNTZType | _: YearMonthIntervalType |
           _: DayTimeIntervalType => true
      case _ => false
    }

    def clear(): Unit = {
      var i = 0
      while (i < size) {
        values((head + i) % capacity) = null
        i += 1
      }
      head = 0
      tail = 0
      size = 0
    }

    private def expand(): Unit = {
      val newCapacity = capacity * 2
      val newValues = new Array[Any](newCapacity)
      val newIndices = new Array[Int](newCapacity)
      
      var i = 0
      while (i < size) {
        val idx = (head + i) % capacity
        newValues(i) = values(idx)
        newIndices(i) = indices(idx)
        i += 1
      }
      
      values = newValues
      indices = newIndices
      head = 0
      tail = size
      capacity = newCapacity
    }

    private def isEmpty: Boolean = size == 0

    private def peekLastValue(): Any = {
      values((tail - 1 + capacity) % capacity)
    }

    private def pollLast(): Unit = {
      tail = (tail - 1 + capacity) % capacity
      values(tail) = null
      size -= 1
    }

    private def peekFirstIndex(): Int = {
      indices(head)
    }

    private def pollFirst(): Unit = {
      values(head) = null
      head = (head + 1) % capacity
      size -= 1
    }

    private def offerLast(value: Any, index: Int): Unit = {
      if (size == capacity) {
        expand()
      }
      values(tail) = value
      indices(tail) = index
      tail = (tail + 1) % capacity
      size += 1
    }

    private def evaluateAndCopy(row: InternalRow): Any = {
      val value = boundChild.eval(row)
      if (value == null) {
        null
      } else if (isPrimitive) {
        value
      } else {
        tempRow.update(0, value)
        val copiedRow = tempRow.copy()
        copiedRow.get(0, dataType)
      }
    }

    def admit(row: InternalRow, index: Int): Unit = {
      val value = evaluateAndCopy(row)
      if (value != null) {
        if (isMin) {
          while (!isEmpty && ordering.compare(peekLastValue(), value) >= 0) {
            pollLast()
          }
        } else {
          while (!isEmpty && ordering.compare(peekLastValue(), value) <= 0) {
            pollLast()
          }
        }
        offerLast(value, index)
      }
    }

    def dropBefore(boundary: Int): Unit = {
      while (!isEmpty && peekFirstIndex() < boundary) {
        pollFirst()
      }
    }

    def currentValue(): Any = {
      if (isEmpty) {
        null
      } else {
        values(head)
      }
    }
  }
}
