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

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.NoOp


/**
 * A window function calculates the results of a number of window functions for a window frame.
 * Before use a frame must be prepared by passing it all the rows in the current partition. After
 * preparation the update method can be called to fill the output rows.
 */
private[window] abstract class WindowFunctionFrame {
  /**
   * Prepare the frame for calculating the results for a partition.
   *
   * @param rows to calculate the frame results for.
   */
  def prepare(rows: RowBuffer): Unit

  /**
   * Write the current results to the target row.
   */
  def write(index: Int, current: InternalRow): Unit
}

/**
 * The offset window frame calculates frames containing LEAD/LAG statements.
 *
 * @param target to write results to.
 * @param ordinal the ordinal is the starting offset at which the results of the window frame get
 *                written into the (shared) target row. The result of the frame expression with
 *                index 'i' will be written to the 'ordinal' + 'i' position in the target row.
 * @param expressions to shift a number of rows.
 * @param inputSchema required for creating a projection.
 * @param newMutableProjection function used to create the projection.
 * @param offset by which rows get moved within a partition.
 */
private[window] final class OffsetWindowFunctionFrame(
    target: InternalRow,
    ordinal: Int,
    expressions: Array[OffsetWindowFunction],
    inputSchema: Seq[Attribute],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection,
    offset: Int)
  extends WindowFunctionFrame {

  /** Rows of the partition currently being processed. */
  private[this] var input: RowBuffer = null

  /** Index of the input row currently used for output. */
  private[this] var inputIndex = 0

  /**
   * Create the projection used when the offset row exists.
   * Please note that this project always respect null input values (like PostgreSQL).
   */
  private[this] val projection = {
    // Collect the expressions and bind them.
    val inputAttrs = inputSchema.map(_.withNullability(true))
    val boundExpressions = Seq.fill(ordinal)(NoOp) ++ expressions.toSeq.map { e =>
      BindReferences.bindReference(e.input, inputAttrs)
    }

    // Create the projection.
    newMutableProjection(boundExpressions, Nil).target(target)
  }

  /** Create the projection used when the offset row DOES NOT exists. */
  private[this] val fillDefaultValue = {
    // Collect the expressions and bind them.
    val inputAttrs = inputSchema.map(_.withNullability(true))
    val boundExpressions = Seq.fill(ordinal)(NoOp) ++ expressions.toSeq.map { e =>
      if (e.default == null || e.default.foldable && e.default.eval() == null) {
        // The default value is null.
        Literal.create(null, e.dataType)
      } else {
        // The default value is an expression.
        BindReferences.bindReference(e.default, inputAttrs)
      }
    }

    // Create the projection.
    newMutableProjection(boundExpressions, Nil).target(target)
  }

  override def prepare(rows: RowBuffer): Unit = {
    input = rows
    // drain the first few rows if offset is larger than zero
    inputIndex = 0
    while (inputIndex < offset) {
      input.next()
      inputIndex += 1
    }
    inputIndex = offset
  }

  override def write(index: Int, current: InternalRow): Unit = {
    if (inputIndex >= 0 && inputIndex < input.size) {
      val r = input.next()
      projection(r)
    } else {
      // Use default values since the offset row does not exist.
      fillDefaultValue(current)
    }
    inputIndex += 1
  }
}

/**
 * The sliding window frame calculates frames with the following SQL form:
 * ... BETWEEN 1 PRECEDING AND 1 FOLLOWING
 *
 * @param target to write results to.
 * @param processor to calculate the row values with.
 * @param lbound comparator used to identify the lower bound of an output row.
 * @param ubound comparator used to identify the upper bound of an output row.
 */
private[window] final class SlidingWindowFunctionFrame(
    target: InternalRow,
    processor: AggregateProcessor,
    lbound: BoundOrdering,
    ubound: BoundOrdering)
  extends WindowFunctionFrame {

  /** Rows of the partition currently being processed. */
  private[this] var input: RowBuffer = null

  /** The next row from `input`. */
  private[this] var nextRow: InternalRow = null

  /** The rows within current sliding window. */
  private[this] val buffer = new util.ArrayDeque[InternalRow]()

  /**
   * Index of the first input row with a value greater than the upper bound of the current
   * output row.
   */
  private[this] var inputHighIndex = 0

  /**
   * Index of the first input row with a value equal to or greater than the lower bound of the
   * current output row.
   */
  private[this] var inputLowIndex = 0

  /** Prepare the frame for calculating a new partition. Reset all variables. */
  override def prepare(rows: RowBuffer): Unit = {
    input = rows
    nextRow = rows.next()
    inputHighIndex = 0
    inputLowIndex = 0
    buffer.clear()
  }

  /** Write the frame columns for the current row to the given target row. */
  override def write(index: Int, current: InternalRow): Unit = {
    var bufferUpdated = index == 0

    // Add all rows to the buffer for which the input row value is equal to or less than
    // the output row upper bound.
    while (nextRow != null && ubound.compare(nextRow, inputHighIndex, current, index) <= 0) {
      buffer.add(nextRow.copy())
      nextRow = input.next()
      inputHighIndex += 1
      bufferUpdated = true
    }

    // Drop all rows from the buffer for which the input row value is smaller than
    // the output row lower bound.
    while (!buffer.isEmpty && lbound.compare(buffer.peek(), inputLowIndex, current, index) < 0) {
      buffer.remove()
      inputLowIndex += 1
      bufferUpdated = true
    }

    // Only recalculate and update when the buffer changes.
    if (bufferUpdated) {
      processor.initialize(input.size)
      val iter = buffer.iterator()
      while (iter.hasNext) {
        processor.update(iter.next())
      }
      processor.evaluate(target)
    }
  }
}

/**
 * The unbounded window frame calculates frames with the following SQL forms:
 * ... (No Frame Definition)
 * ... BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
 *
 * Its results are the same for each and every row in the partition. This class can be seen as a
 * special case of a sliding window, but is optimized for the unbound case.
 *
 * @param target to write results to.
 * @param processor to calculate the row values with.
 */
private[window] final class UnboundedWindowFunctionFrame(
    target: InternalRow,
    processor: AggregateProcessor)
  extends WindowFunctionFrame {

  /** Prepare the frame for calculating a new partition. Process all rows eagerly. */
  override def prepare(rows: RowBuffer): Unit = {
    val size = rows.size
    processor.initialize(size)
    var i = 0
    while (i < size) {
      processor.update(rows.next())
      i += 1
    }
  }

  /** Write the frame columns for the current row to the given target row. */
  override def write(index: Int, current: InternalRow): Unit = {
    // Unfortunately we cannot assume that evaluation is deterministic. So we need to re-evaluate
    // for each row.
    processor.evaluate(target)
  }
}

/**
 * The UnboundPreceding window frame calculates frames with the following SQL form:
 * ... BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
 *
 * There is only an upper bound. Very common use cases are for instance running sums or counts
 * (row_number). Technically this is a special case of a sliding window. However a sliding window
 * has to maintain a buffer, and it must do a full evaluation everytime the buffer changes. This
 * is not the case when there is no lower bound, given the additive nature of most aggregates
 * streaming updates and partial evaluation suffice and no buffering is needed.
 *
 * @param target to write results to.
 * @param processor to calculate the row values with.
 * @param ubound comparator used to identify the upper bound of an output row.
 */
private[window] final class UnboundedPrecedingWindowFunctionFrame(
    target: InternalRow,
    processor: AggregateProcessor,
    ubound: BoundOrdering)
  extends WindowFunctionFrame {

  /** Rows of the partition currently being processed. */
  private[this] var input: RowBuffer = null

  /** The next row from `input`. */
  private[this] var nextRow: InternalRow = null

  /**
   * Index of the first input row with a value greater than the upper bound of the current
   * output row.
   */
  private[this] var inputIndex = 0

  /** Prepare the frame for calculating a new partition. */
  override def prepare(rows: RowBuffer): Unit = {
    input = rows
    nextRow = rows.next()
    inputIndex = 0
    processor.initialize(input.size)
  }

  /** Write the frame columns for the current row to the given target row. */
  override def write(index: Int, current: InternalRow): Unit = {
    var bufferUpdated = index == 0

    // Add all rows to the aggregates for which the input row value is equal to or less than
    // the output row upper bound.
    while (nextRow != null && ubound.compare(nextRow, inputIndex, current, index) <= 0) {
      processor.update(nextRow)
      nextRow = input.next()
      inputIndex += 1
      bufferUpdated = true
    }

    // Only recalculate and update when the buffer changes.
    if (bufferUpdated) {
      processor.evaluate(target)
    }
  }
}

/**
 * The UnboundFollowing window frame calculates frames with the following SQL form:
 * ... BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 *
 * There is only an upper bound. This is a slightly modified version of the sliding window. The
 * sliding window operator has to check if both upper and the lower bound change when a new row
 * gets processed, where as the unbounded following only has to check the lower bound.
 *
 * This is a very expensive operator to use, O(n * (n - 1) /2), because we need to maintain a
 * buffer and must do full recalculation after each row. Reverse iteration would be possible, if
 * the commutativity of the used window functions can be guaranteed.
 *
 * @param target to write results to.
 * @param processor to calculate the row values with.
 * @param lbound comparator used to identify the lower bound of an output row.
 */
private[window] final class UnboundedFollowingWindowFunctionFrame(
    target: InternalRow,
    processor: AggregateProcessor,
    lbound: BoundOrdering)
  extends WindowFunctionFrame {

  /** Rows of the partition currently being processed. */
  private[this] var input: RowBuffer = null

  /**
   * Index of the first input row with a value equal to or greater than the lower bound of the
   * current output row.
   */
  private[this] var inputIndex = 0

  /** Prepare the frame for calculating a new partition. */
  override def prepare(rows: RowBuffer): Unit = {
    input = rows
    inputIndex = 0
  }

  /** Write the frame columns for the current row to the given target row. */
  override def write(index: Int, current: InternalRow): Unit = {
    var bufferUpdated = index == 0

    // Duplicate the input to have a new iterator
    val tmp = input.copy()

    // Drop all rows from the buffer for which the input row value is smaller than
    // the output row lower bound.
    tmp.skip(inputIndex)
    var nextRow = tmp.next()
    while (nextRow != null && lbound.compare(nextRow, inputIndex, current, index) < 0) {
      nextRow = tmp.next()
      inputIndex += 1
      bufferUpdated = true
    }

    // Only recalculate and update when the buffer changes.
    if (bufferUpdated) {
      processor.initialize(input.size)
      while (nextRow != null) {
        processor.update(nextRow)
        nextRow = tmp.next()
      }
      processor.evaluate(target)
    }
  }
}
