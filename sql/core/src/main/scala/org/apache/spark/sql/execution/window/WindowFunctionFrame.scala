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
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.aggregate.NoOp
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray


/**
 * A window function calculates the results of a number of window functions for a window frame.
 * Before use a frame must be prepared by passing it all the rows in the current partition. After
 * preparation the update method can be called to fill the output rows.
 *
 * Note: `WindowFunctionFrame` instances are reused during window execution. The `prepare` method
 * will be called before processing the next partition, and must reset the states.
 */
abstract class WindowFunctionFrame {
  /**
   * Prepare the frame for calculating the results for a partition.
   *
   * @param rows to calculate the frame results for.
   */
  def prepare(rows: ExternalAppendOnlyUnsafeRowArray): Unit

  /**
   * Write the current results to the target row.
   */
  def write(index: Int, current: InternalRow): Unit

  /**
   * The current lower window bound in the row array (inclusive).
   *
   * This should be called after the current row is updated via `write`.
   */
  def currentLowerBound(): Int

  /**
   * The current row index of the upper window bound in the row array (exclusive)
   *
   * This should be called after the current row is updated via `write`.
   */
  def currentUpperBound(): Int
}

object WindowFunctionFrame {
  def getNextOrNull(iterator: Iterator[UnsafeRow]): UnsafeRow = {
    if (iterator.hasNext) iterator.next() else null
  }
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
abstract class OffsetWindowFunctionFrameBase(
    target: InternalRow,
    ordinal: Int,
    expressions: Array[OffsetWindowFunction],
    inputSchema: Seq[Attribute],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection,
    offset: Int)
  extends WindowFunctionFrame {

  /** Rows of the partition currently being processed. */
  protected var input: ExternalAppendOnlyUnsafeRowArray = null

  /**
   * An iterator over the [[input]]
   */
  protected var inputIterator: Iterator[UnsafeRow] = _

  /** Index of the input row currently used for output. */
  protected var inputIndex = 0

  /** Attributes of the input row currently used for output. */
  protected val inputAttrs = inputSchema.map(_.withNullability(true))

  /**
   * Create the projection used when the offset row exists.
   * Please note that this project always respect null input values (like PostgreSQL).
   */
  protected val projection = {
    // Collect the expressions and bind them.
    val boundExpressions = Seq.fill(ordinal)(NoOp) ++ bindReferences(
      expressions.toSeq.map(_.input), inputAttrs)

    // Create the projection.
    newMutableProjection(boundExpressions, Nil).target(target)
  }

  /** Create the projection used when the offset row DOES NOT exists. */
  protected val fillDefaultValue = {
    // Collect the expressions and bind them.
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

  /** Holder the UnsafeRow where the input operator by function is not null. */
  protected var nextSelectedRow = EmptyRow

  // The number of rows skipped to get the next UnsafeRow where the input operator by function
  // is not null.
  protected var skippedNonNullCount = 0

  // Reset the states by the data of the new partition.
  protected def resetStates(rows: ExternalAppendOnlyUnsafeRowArray): Unit = {
    input = rows
    inputIterator = input.generateIterator()
    inputIndex = 0
    skippedNonNullCount = 0
    nextSelectedRow = EmptyRow
  }

  /** Create the projection to determine whether input is null. */
  protected val project = UnsafeProjection.create(Seq(IsNull(expressions.head.input)), inputSchema)

  /** Check if the output value of the first index is null. */
  protected def nullCheck(row: InternalRow): Boolean = project(row).getBoolean(0)

  /** find the offset row whose input is not null */
  protected def findNextRowWithNonNullInput(): Unit = {
    // In order to find the offset row whose input is not-null,
    // offset < = input.length must be guaranteed.
    assert(offset <= input.length)
    while (skippedNonNullCount < offset && inputIndex < input.length) {
      val r = WindowFunctionFrame.getNextOrNull(inputIterator)
      if (!nullCheck(r)) {
        nextSelectedRow = r
        skippedNonNullCount += 1
      }
      inputIndex += 1
    }
    if (skippedNonNullCount < offset && inputIndex == input.length) {
      // The size of non-null input is less than offset, cannot find the offset row whose input
      // is not null. Therefore, reset `nextSelectedRow` with empty row.
      nextSelectedRow = EmptyRow
    }
  }

  override def currentLowerBound(): Int = throw new UnsupportedOperationException()

  override def currentUpperBound(): Int = throw new UnsupportedOperationException()
}

/**
 * The frameless offset window frame is an internal window frame just used to optimize the
 * performance for the window function that returns the value of the input column offset
 * by a number of rows according to the current row. The internal window frame is not a popular
 * window frame cannot be specified and used directly by the users. This window frame
 * calculates frames containing LEAD/LAG statements.
 */
class FrameLessOffsetWindowFunctionFrame(
    target: InternalRow,
    ordinal: Int,
    expressions: Array[OffsetWindowFunction],
    inputSchema: Seq[Attribute],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection,
    offset: Int,
    ignoreNulls: Boolean = false)
  extends OffsetWindowFunctionFrameBase(
    target, ordinal, expressions, inputSchema, newMutableProjection, offset) {

  override def prepare(rows: ExternalAppendOnlyUnsafeRowArray): Unit = {
    resetStates(rows)
    if (ignoreNulls) {
      findNextRowWithNonNullInput()
    } else {
      // drain the first few rows if offset is larger than zero
      while (inputIndex < offset) {
        if (inputIterator.hasNext) inputIterator.next()
        inputIndex += 1
      }
      inputIndex = offset
    }
  }

  private val doWrite = if (ignoreNulls && offset > 0) {
    // For illustration, here is one example: the input data contains nine rows,
    // and the input values of each row are: null, x, null, null, y, null, z, v, null.
    // We use lead(input, 2) with IGNORE NULLS and the process is as follows:
    // 1. current row -> null, next selected row -> y, output: y;
    // 2. current row -> x, next selected row -> z, output: z;
    // 3. current row -> null, next selected row -> z, output: z;
    // 4. current row -> null, next selected row -> z, output: z;
    // 5. current row -> y, next selected row -> v, output: v;
    // 6. current row -> null, next selected row -> v, output: v;
    // 7. current row -> z, next selected row -> empty, output: null;
    // ... next selected row is empty, all following return null.
    (current: InternalRow) =>
      if (nextSelectedRow == EmptyRow) {
        // Use default values since the offset row whose input value is not null does not exist.
        fillDefaultValue(current)
      } else {
        if (nullCheck(current)) {
          projection(nextSelectedRow)
        } else {
          skippedNonNullCount -= 1
          findNextRowWithNonNullInput()
          if (skippedNonNullCount == offset) {
            projection(nextSelectedRow)
          } else {
            // Use default values since the offset row whose input value is not null does not exist.
            fillDefaultValue(current)
            nextSelectedRow = EmptyRow
          }
        }
      }
  } else if (ignoreNulls && offset < 0) {
    // For illustration, here is one example: the input data contains nine rows,
    // and the input values of each row are: null, x, null, null, y, null, z, v, null.
    // We use lag(input, 1) with IGNORE NULLS and the process is as follows:
    // 1. current row -> null, next selected row -> empty, output: null;
    // 2. current row -> x, next selected row -> empty, output: null;
    // 3. current row -> null, next selected row -> x, output: x;
    // 4. current row -> null, next selected row -> x, output: x;
    // 5. current row -> y, next selected row -> x, output: x;
    // 6. current row -> null, next selected row -> y, output: y;
    // 7. current row -> z, next selected row -> y, output: y;
    // 8. current row -> v, next selected row -> z, output: z;
    // 9. current row -> null, next selected row -> v, output: v;
    val absOffset = Math.abs(offset)
    (current: InternalRow) =>
      if (skippedNonNullCount == absOffset) {
        nextSelectedRow = EmptyRow
        skippedNonNullCount -= 1
        while (nextSelectedRow == EmptyRow && inputIndex < input.length) {
          val r = WindowFunctionFrame.getNextOrNull(inputIterator)
          if (!nullCheck(r)) {
            nextSelectedRow = r
          }
          inputIndex += 1
        }
      }
      if (nextSelectedRow == EmptyRow) {
        // Use default values since the offset row whose input value is not null does not exist.
        fillDefaultValue(current)
      } else {
        projection(nextSelectedRow)
      }
      if (!nullCheck(current)) {
        skippedNonNullCount += 1
      }
  } else {
    (current: InternalRow) =>
      if (inputIndex >= 0 && inputIndex < input.length) {
        val r = WindowFunctionFrame.getNextOrNull(inputIterator)
        projection(r)
      } else {
        // Use default values since the offset row does not exist.
        fillDefaultValue(current)
      }
      inputIndex += 1
  }

  override def write(index: Int, current: InternalRow): Unit = {
    doWrite(current)
  }
}

/**
 * The unbounded offset window frame is an internal window frame just used to optimize the
 * performance for the window function that returns the value of the input column offset
 * by a number of rows within the frame and has specified ROWS BETWEEN UNBOUNDED PRECEDING
 * AND UNBOUNDED FOLLOWING. The internal window frame is not a popular window frame cannot be
 * specified and used directly by the users.
 * The unbounded offset window frame calculates frames containing NTH_VALUE statements.
 * The unbounded offset window frame return the same value for all rows in the window partition.
 */
class UnboundedOffsetWindowFunctionFrame(
    target: InternalRow,
    ordinal: Int,
    expressions: Array[OffsetWindowFunction],
    inputSchema: Seq[Attribute],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection,
    offset: Int,
    ignoreNulls: Boolean = false)
  extends OffsetWindowFunctionFrameBase(
    target, ordinal, expressions, inputSchema, newMutableProjection, offset) {
  assert(offset > 0)

  override def prepare(rows: ExternalAppendOnlyUnsafeRowArray): Unit = {
    if (offset > rows.length) {
      fillDefaultValue(EmptyRow)
    } else {
      resetStates(rows)
      if (ignoreNulls) {
        findNextRowWithNonNullInput()
        if (nextSelectedRow == EmptyRow) {
          // Use default values since the offset row whose input value is not null does not exist.
          fillDefaultValue(EmptyRow)
        } else {
          projection(nextSelectedRow)
        }
      } else {
        var selectedRow: UnsafeRow = null
        // drain the first few rows if offset is larger than one
        while (inputIndex < offset) {
          selectedRow = WindowFunctionFrame.getNextOrNull(inputIterator)
          inputIndex += 1
        }
        projection(selectedRow)
      }
    }
  }

  override def write(index: Int, current: InternalRow): Unit = {
    // The results are the same for each row in the partition, and have been evaluated in prepare.
    // Don't need to recalculate here.
  }
}

/**
 * The unbounded preceding offset window frame is an internal window frame just used to optimize
 * the performance for the window function that returns the value of the input column offset
 * by a number of rows within the frame and has specified ROWS BETWEEN UNBOUNDED PRECEDING
 * AND CURRENT ROW. The internal window frame is not a popular window frame cannot be specified
 * and used directly by the users.
 * The unbounded preceding offset window frame calculates frames containing NTH_VALUE statements.
 * The unbounded preceding offset window frame return the same value for rows which index
 * (starting from 1) equal to or greater than offset in the window partition.
 */
class UnboundedPrecedingOffsetWindowFunctionFrame(
    target: InternalRow,
    ordinal: Int,
    expressions: Array[OffsetWindowFunction],
    inputSchema: Seq[Attribute],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection,
    offset: Int,
    ignoreNulls: Boolean = false)
  extends OffsetWindowFunctionFrameBase(
    target, ordinal, expressions, inputSchema, newMutableProjection, offset) {
  assert(offset > 0)

  override def prepare(rows: ExternalAppendOnlyUnsafeRowArray): Unit = {
    if (offset > rows.length) {
      fillDefaultValue(EmptyRow)
    } else {
      resetStates(rows)
      if (ignoreNulls) {
        findNextRowWithNonNullInput()
      } else {
        // drain the first few rows if offset is larger than one
        while (inputIndex < offset) {
          nextSelectedRow = WindowFunctionFrame.getNextOrNull(inputIterator)
          inputIndex += 1
        }
      }
    }
  }

  override def write(index: Int, current: InternalRow): Unit = {
    if (index >= inputIndex - 1 && nextSelectedRow != null) {
      projection(nextSelectedRow)
    } else {
      fillDefaultValue(EmptyRow)
    }
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
final class SlidingWindowFunctionFrame(
    target: InternalRow,
    processor: AggregateProcessor,
    lbound: BoundOrdering,
    ubound: BoundOrdering)
  extends WindowFunctionFrame {

  /** Rows of the partition currently being processed. */
  private[this] var input: ExternalAppendOnlyUnsafeRowArray = null

  /**
   * An iterator over the [[input]]
   */
  private[this] var inputIterator: Iterator[UnsafeRow] = _

  /** The next row from `input`. */
  private[this] var nextRow: InternalRow = null

  /** The rows within current sliding window. */
  private[this] val buffer = new util.ArrayDeque[InternalRow]()

  /**
   * Index of the first input row with a value equal to or greater than the lower bound of the
   * current output row.
   */
  private[this] var lowerBound = 0

  /**
   * Index of the first input row with a value greater than the upper bound of the current
   * output row.
   */
  private[this] var upperBound = 0

  /** Prepare the frame for calculating a new partition. Reset all variables. */
  override def prepare(rows: ExternalAppendOnlyUnsafeRowArray): Unit = {
    input = rows
    inputIterator = input.generateIterator()
    nextRow = WindowFunctionFrame.getNextOrNull(inputIterator)
    lowerBound = 0
    upperBound = 0
    buffer.clear()
  }

  /** Write the frame columns for the current row to the given target row. */
  override def write(index: Int, current: InternalRow): Unit = {
    var bufferUpdated = index == 0

    // Drop all rows from the buffer for which the input row value is smaller than
    // the output row lower bound.
    while (!buffer.isEmpty && lbound.compare(buffer.peek(), lowerBound, current, index) < 0) {
      buffer.remove()
      lowerBound += 1
      bufferUpdated = true
    }

    // Add all rows to the buffer for which the input row value is equal to or less than
    // the output row upper bound.
    while (nextRow != null && ubound.compare(nextRow, upperBound, current, index) <= 0) {
      if (lbound.compare(nextRow, lowerBound, current, index) < 0) {
        lowerBound += 1
      } else {
        buffer.add(nextRow.copy())
        bufferUpdated = true
      }
      nextRow = WindowFunctionFrame.getNextOrNull(inputIterator)
      upperBound += 1
    }

    // Only recalculate and update when the buffer changes.
    if (processor != null && bufferUpdated) {
      processor.initialize(input.length)
      val iter = buffer.iterator()
      while (iter.hasNext) {
        processor.update(iter.next())
      }
      processor.evaluate(target)
    }
  }

  override def currentLowerBound(): Int = lowerBound

  override def currentUpperBound(): Int = upperBound
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
final class UnboundedWindowFunctionFrame(
    target: InternalRow,
    processor: AggregateProcessor)
  extends WindowFunctionFrame {

  val lowerBound: Int = 0
  var upperBound: Int = 0

  /** Prepare the frame for calculating a new partition. Process all rows eagerly. */
  override def prepare(rows: ExternalAppendOnlyUnsafeRowArray): Unit = {
    if (processor != null) {
      processor.initialize(rows.length)
      val iterator = rows.generateIterator()
      while (iterator.hasNext) {
        processor.update(iterator.next())
      }

      processor.evaluate(target)
    }

    upperBound = rows.length
  }

  /** Write the frame columns for the current row to the given target row. */
  override def write(index: Int, current: InternalRow): Unit = {
    // The results are the same for each row in the partition, and have been evaluated in prepare.
    // Don't need to recalculate here.
  }

  override def currentLowerBound(): Int = lowerBound

  override def currentUpperBound(): Int = upperBound
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
final class UnboundedPrecedingWindowFunctionFrame(
    target: InternalRow,
    processor: AggregateProcessor,
    ubound: BoundOrdering)
  extends WindowFunctionFrame {

  /** Rows of the partition currently being processed. */
  private[this] var input: ExternalAppendOnlyUnsafeRowArray = null

  /**
   * An iterator over the [[input]]
   */
  private[this] var inputIterator: Iterator[UnsafeRow] = _

  /** The next row from `input`. */
  private[this] var nextRow: InternalRow = null

  /**
   * Index of the first input row with a value greater than the upper bound of the current
   * output row.
   */
  private[this] var inputIndex = 0

  /** Prepare the frame for calculating a new partition. */
  override def prepare(rows: ExternalAppendOnlyUnsafeRowArray): Unit = {
    input = rows
    inputIndex = 0
    inputIterator = input.generateIterator()
    if (inputIterator.hasNext) {
      nextRow = inputIterator.next()
    }

    if (processor != null) {
      processor.initialize(input.length)
    }
  }

  /** Write the frame columns for the current row to the given target row. */
  override def write(index: Int, current: InternalRow): Unit = {
    var bufferUpdated = index == 0

    // Add all rows to the aggregates for which the input row value is equal to or less than
    // the output row upper bound.
    while (nextRow != null && ubound.compare(nextRow, inputIndex, current, index) <= 0) {
      if (processor != null) {
        processor.update(nextRow)
      }
      nextRow = WindowFunctionFrame.getNextOrNull(inputIterator)
      inputIndex += 1
      bufferUpdated = true
    }

    // Only recalculate and update when the buffer changes.
    if (processor != null && bufferUpdated) {
      processor.evaluate(target)
    }
  }

  override def currentLowerBound(): Int = 0

  override def currentUpperBound(): Int = inputIndex
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
final class UnboundedFollowingWindowFunctionFrame(
    target: InternalRow,
    processor: AggregateProcessor,
    lbound: BoundOrdering)
  extends WindowFunctionFrame {

  /** Rows of the partition currently being processed. */
  private[this] var input: ExternalAppendOnlyUnsafeRowArray = null

  /**
   * Index of the first input row with a value equal to or greater than the lower bound of the
   * current output row.
   */
  private[this] var inputIndex = 0

  /** Prepare the frame for calculating a new partition. */
  override def prepare(rows: ExternalAppendOnlyUnsafeRowArray): Unit = {
    input = rows
    inputIndex = 0
  }

  /** Write the frame columns for the current row to the given target row. */
  override def write(index: Int, current: InternalRow): Unit = {
    var bufferUpdated = index == 0

    // Ignore all the rows from the buffer for which the input row value is smaller than
    // the output row lower bound.
    val iterator = input.generateIterator(startIndex = inputIndex)

    var nextRow = WindowFunctionFrame.getNextOrNull(iterator)
    while (nextRow != null && lbound.compare(nextRow, inputIndex, current, index) < 0) {
      inputIndex += 1
      bufferUpdated = true
      nextRow = WindowFunctionFrame.getNextOrNull(iterator)
    }

    // Only recalculate and update when the buffer changes.
    if (processor != null && bufferUpdated) {
      processor.initialize(input.length)
      if (nextRow != null) {
        processor.update(nextRow)
      }
      while (iterator.hasNext) {
        processor.update(iterator.next())
      }
      processor.evaluate(target)
    }
  }

  override def currentLowerBound(): Int = inputIndex

  override def currentUpperBound(): Int = input.length
}
