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
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types._

/**
 * An optimized sliding window frame that calculates min and/or max aggregate functions using
 * monotonic deques. This provides O(N) time complexity instead of O(N * W) of
 * [[SlidingWindowFunctionFrame]] or O(N log W) of [[SegmentTreeWindowFunctionFrame]].
 *
 * This frame is only instantiated when `isMinMaxOnly` is true (all window functions are Min or
 * Max and no FILTER clause is used), enforced upstream in [[WindowEvaluatorFactoryBase]].
 */
private[window] final class SlidingWindowMinMaxFunctionFrame(
    target: InternalRow,
    processor: AggregateProcessor,
    lbound: BoundOrdering,
    ubound: BoundOrdering,
    functions: Array[Expression],
    inputSchema: Seq[Attribute],
    numMonotonicDequeFrames: Option[SQLMetric] = None)
    extends WindowFunctionFrame {

  /** Rows of the partition currently being processed. */
  private[this] var input: ExternalAppendOnlyUnsafeRowArray = null

  // Spill-safety: when `input` (ExternalAppendOnlyUnsafeRowArray) spills, its
  // iterator reuses a single UnsafeRow whose pointer is rebound on each next().
  // This is safe because both cursors follow a read-before-advance pattern:
  // `lowerRow`/`nextRow` are used for comparison *before* calling getNextOrNull.
  // Values are extracted from the row via `evaluateAndCopy` before advancing.
  // DO NOT cache a historical row without an explicit .copy(); the shared
  // reusable UnsafeRow would silently mutate.
  private[this] var lowerIterator: Iterator[UnsafeRow] = _
  private[this] var inputIterator: Iterator[UnsafeRow] = _

  /** The row at lowerBound. */
  private[this] var lowerRow: UnsafeRow = null

  /** The next row from `input`. */
  private[this] var nextRow: InternalRow = null

  /**
   * Index of the first input row with a value equal to or greater than the lower bound of the
   * current output row.
   */
  private[this] var lowerBound = 0

  /**
   * Index of the first input row with a value greater than the upper bound of the current output
   * row.
   */
  private[this] var upperBound = 0

  // `sourceRow` is used as the `source` argument to `processor.evaluate(source, target)`.
  // Layout compatibility is guaranteed because Min/Max each contribute exactly one
  // `aggBufferAttributes` entry typed `child.dataType`, which equals `Min/Max.dataType`.
  // Neither is a `SizeBasedWindowFunction`, so no extra slot is prepended.
  // `isMinMaxOnly` (enforced in WindowEvaluatorFactoryBase) ensures this invariant holds.
  private[this] val sourceRow = new SpecificInternalRow(functions.map(_.dataType).toIndexedSeq)

  // Each deque is addressed by its position in this array (one entry per Min/Max function),
  // so no separate per-deque ordinal is needed.
  private[this] val deques: Array[MinMaxDeque] = functions.map { func =>
    val isMin = func.isInstanceOf[Min]
    val child = func match {
      case m: Min => m.child
      case m: Max => m.child
    }
    new MinMaxDeque(
      isMin,
      BindReferences.bindReference(child, inputSchema),
      child.dataType,
      TypeUtils.getInterpretedOrdering(child.dataType))
  }

  override def prepare(rows: ExternalAppendOnlyUnsafeRowArray): Unit = {
    numMonotonicDequeFrames.foreach(_ += 1)
    input = rows
    lowerIterator = input.generateIterator()
    lowerRow = WindowFunctionFrame.getNextOrNull(lowerIterator)
    var di = 0
    while (di < deques.length) { deques(di).clear(); di += 1 }
    lowerBound = 0

    inputIterator = input.generateIterator()
    nextRow = WindowFunctionFrame.getNextOrNull(inputIterator)
    upperBound = 0
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
    while (nextRow != null && ubound.compare(nextRow, upperBound, current, index) <= 0) {
      if (lbound.compare(nextRow, lowerBound, current, index) < 0) {
        lowerBound += 1
        lowerRow = WindowFunctionFrame.getNextOrNull(lowerIterator)
      } else {
        var di = 0
        while (di < deques.length) { deques(di).admit(nextRow, upperBound); di += 1 }
        bufferUpdated = true
      }
      nextRow = WindowFunctionFrame.getNextOrNull(inputIterator)
      upperBound += 1
    }

    if (bufferUpdated) {
      var di = 0
      while (di < deques.length) { deques(di).dropBefore(lowerBound); di += 1 }
    }

    // Write output values to target.
    // See sourceRow comment above for why evaluate(sourceRow, target) is safe here.
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

  // MinMaxDeque fields are plain constructor params (not vals) since this is a private inner
  // class and nothing outside reads them.
  private class MinMaxDeque(
      isMin: Boolean,
      boundChild: Expression,
      dataType: DataType,
      ordering: Ordering[Any]) {

    private var capacity = 16
    private var values = new Array[Any](capacity)
    private var indices = new Array[Int](capacity)
    private var head = 0
    private var tail = 0
    private var size = 0

    def clear(): Unit = {
      var i = 0
      while (i < size) {
        values((head + i) & (capacity - 1)) = null
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
        val idx = (head + i) & (capacity - 1)
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
      values((tail - 1) & (capacity - 1))
    }

    private def pollLast(): Unit = {
      tail = (tail - 1) & (capacity - 1)
      values(tail) = null
      size -= 1
    }

    private def peekFirstIndex(): Int = {
      indices(head)
    }

    private def pollFirst(): Unit = {
      values(head) = null
      head = (head + 1) & (capacity - 1)
      size -= 1
    }

    private def offerLast(value: Any, index: Int): Unit = {
      if (size == capacity) {
        expand()
      }
      values(tail) = value
      indices(tail) = index
      tail = (tail + 1) & (capacity - 1)
      size += 1
    }

    // ExtractWindowExpressions hoists window aggregate arguments into the Project below,
    // so `boundChild` is a BoundReference over an UnsafeRow. Thus getBinary/getDecimal
    // allocate a fresh object per call, and we only need InternalRow.copyValue for the
    // remaining reference types (String, Struct, Array, Map).
    private def evaluateAndCopy(row: InternalRow): Any = {
      val value = boundChild.eval(row)
      if (value == null) null else InternalRow.copyValue(value)
    }

    def admit(row: InternalRow, index: Int): Unit = {
      val value = evaluateAndCopy(row)
      if (value != null) {
        // Use strict inequality (> for min-deque, < for max-deque) so that among equal-valued
        // elements we retain the first one, matching the behavior of the naive and segment-tree
        // paths (both of which keep the earliest equal value in the window).
        if (isMin) {
          while (!isEmpty && ordering.compare(peekLastValue(), value) > 0) {
            pollLast()
          }
        } else {
          while (!isEmpty && ordering.compare(peekLastValue(), value) < 0) {
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
