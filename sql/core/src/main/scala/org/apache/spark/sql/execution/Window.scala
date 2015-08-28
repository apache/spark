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

package org.apache.spark.sql.execution

import java.util

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.CompactBuffer
import scala.collection.mutable

/**
 * :: DeveloperApi ::
 * This class calculates and outputs (windowed) aggregates over the rows in a single (sorted)
 * partition. The aggregates are calculated for each row in the group. Special processing
 * instructions, frames, are used to calculate these aggregates. Frames are processed in the order
 * specified in the window specification (the ORDER BY ... clause). There are four different frame
 * types:
 * - Entire partition: The frame is the entire partition, i.e.
 *   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING. For this case, window function will take all
 *   rows as inputs and be evaluated once.
 * - Growing frame: We only add new rows into the frame, i.e. UNBOUNDED PRECEDING AND ....
 *   Every time we move to a new row to process, we add some rows to the frame. We do not remove
 *   rows from this frame.
 * - Shrinking frame: We only remove rows from the frame, i.e. ... AND UNBOUNDED FOLLOWING.
 *   Every time we move to a new row to process, we remove some rows from the frame. We do not add
 *   rows to this frame.
 * - Moving frame: Every time we move to a new row to process, we remove some rows from the frame
 *   and we add some rows to the frame. Examples are:
 *     1 PRECEDING AND CURRENT ROW and 1 FOLLOWING AND 2 FOLLOWING.
 *
 * Different frame boundaries can be used in Growing, Shrinking and Moving frames. A frame
 * boundary can be either Row or Range based:
 * - Row Based: A row based boundary is based on the position of the row within the partition.
 *   An offset indicates the number of rows above or below the current row, the frame for the
 *   current row starts or ends. For instance, given a row based sliding frame with a lower bound
 *   offset of -1 and a upper bound offset of +2. The frame for row with index 5 would range from
 *   index 4 to index 6.
 * - Range based: A range based boundary is based on the actual value of the ORDER BY
 *   expression(s). An offset is used to alter the value of the ORDER BY expression, for
 *   instance if the current order by expression has a value of 10 and the lower bound offset
 *   is -3, the resulting lower bound for the current row will be 10 - 3 = 7. This however puts a
 *   number of constraints on the ORDER BY expressions: there can be only one expression and this
 *   expression must have a numerical data type. An exception can be made when the offset is 0,
 *   because no value modification is needed, in this case multiple and non-numeric ORDER BY
 *   expression are allowed.
 *
 * This is quite an expensive operator because every row for a single group must be in the same
 * partition and partitions must be sorted according to the grouping and sort order. The operator
 * requires the planner to take care of the partitioning and sorting.
 *
 * The operator is semi-blocking. The window functions and aggregates are calculated one group at
 * a time, the result will only be made available after the processing for the entire group has
 * finished. The operator is able to process different frame configurations at the same time. This
 * is done by delegating the actual frame processing (i.e. calculation of the window functions) to
 * specialized classes, see [[WindowFunctionFrame]], which take care of their own frame type:
 * Entire Partition, Sliding, Growing & Shrinking. Boundary evaluation is also delegated to a pair
 * of specialized classes: [[RowBoundOrdering]] & [[RangeBoundOrdering]].
 */
@DeveloperApi
case class Window(
    projectList: Seq[Attribute],
    windowExpression: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan)
  extends UnaryNode {

  override def output: Seq[Attribute] = projectList ++ windowExpression.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = {
    if (partitionSpec.isEmpty) {
      // Only show warning when the number of bytes is larger than 100 MB?
      logWarning("No Partition Defined for Window operation! Moving all data to a single "
        + "partition, this can cause serious performance degradation.")
      AllTuples :: Nil
    } else ClusteredDistribution(partitionSpec) :: Nil
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def canProcessUnsafeRows: Boolean = true

  /**
   * Create a bound ordering object for a given frame type and offset. A bound ordering object is
   * used to determine which input row lies within the frame boundaries of an output row.
   *
   * This method uses Code Generation. It can only be used on the executor side.
   *
   * @param frameType to evaluate. This can either be Row or Range based.
   * @param offset with respect to the row.
   * @return a bound ordering object.
   */
  private[this] def createBoundOrdering(frameType: FrameType, offset: Int): BoundOrdering = {
    frameType match {
      case RangeFrame =>
        val (exprs, current, bound) = if (offset == 0) {
          // Use the entire order expression when the offset is 0.
          val exprs = orderSpec.map(_.child)
          val projection = newMutableProjection(exprs, child.output)
          (orderSpec, projection(), projection())
        } else if (orderSpec.size == 1) {
          // Use only the first order expression when the offset is non-null.
          val sortExpr = orderSpec.head
          val expr = sortExpr.child
          // Create the projection which returns the current 'value'.
          val current = newMutableProjection(expr :: Nil, child.output)()
          // Flip the sign of the offset when processing the order is descending
          val boundOffset =
            if (sortExpr.direction == Descending) {
              -offset
            } else {
              offset
            }
          // Create the projection which returns the current 'value' modified by adding the offset.
          val boundExpr = Add(expr, Cast(Literal.create(boundOffset, IntegerType), expr.dataType))
          val bound = newMutableProjection(boundExpr :: Nil, child.output)()
          (sortExpr :: Nil, current, bound)
        } else {
          sys.error("Non-Zero range offsets are not supported for windows " +
            "with multiple order expressions.")
        }
        // Construct the ordering. This is used to compare the result of current value projection
        // to the result of bound value projection. This is done manually because we want to use
        // Code Generation (if it is enabled).
        val (sortExprs, schema) = exprs.map { case e =>
          val ref = AttributeReference("ordExpr", e.dataType, e.nullable)()
          (SortOrder(ref, e.direction), ref)
        }.unzip
        val ordering = newOrdering(sortExprs, schema)
        RangeBoundOrdering(ordering, current, bound)
      case RowFrame => RowBoundOrdering(offset)
    }
  }

  /**
   * Create a frame processor.
   *
   * This method uses Code Generation. It can only be used on the executor side.
   *
   * @param frame boundaries.
   * @param functions to process in the frame.
   * @param ordinal at which the processor starts writing to the output.
   * @return a frame processor.
   */
  private[this] def createFrameProcessor(
      frame: WindowFrame,
      functions: Array[WindowFunction],
      ordinal: Int): WindowFunctionFrame = frame match {
    // Growing Frame.
    case SpecifiedWindowFrame(frameType, UnboundedPreceding, FrameBoundaryExtractor(high)) =>
      val uBoundOrdering = createBoundOrdering(frameType, high)
      new UnboundedPrecedingWindowFunctionFrame(ordinal, functions, uBoundOrdering)

    // Shrinking Frame.
    case SpecifiedWindowFrame(frameType, FrameBoundaryExtractor(low), UnboundedFollowing) =>
      val lBoundOrdering = createBoundOrdering(frameType, low)
      new UnboundedFollowingWindowFunctionFrame(ordinal, functions, lBoundOrdering)

    // Moving Frame.
    case SpecifiedWindowFrame(frameType,
        FrameBoundaryExtractor(low), FrameBoundaryExtractor(high)) =>
      val lBoundOrdering = createBoundOrdering(frameType, low)
      val uBoundOrdering = createBoundOrdering(frameType, high)
      new SlidingWindowFunctionFrame(ordinal, functions, lBoundOrdering, uBoundOrdering)

    // Entire Partition Frame.
    case SpecifiedWindowFrame(_, UnboundedPreceding, UnboundedFollowing) =>
      new UnboundedWindowFunctionFrame(ordinal, functions)

    // Error
    case fr =>
      sys.error(s"Unsupported Frame $fr for functions: $functions")
  }

  /**
   * Create the resulting projection.
   *
   * This method uses Code Generation. It can only be used on the executor side.
   *
   * @param expressions unbound ordered function expressions.
   * @return the final resulting projection.
   */
  private[this] def createResultProjection(
      expressions: Seq[Expression]): MutableProjection = {
    val unboundToAttr = expressions.map {
      e => (e, AttributeReference("windowResult", e.dataType, e.nullable)())
    }
    val unboundToAttrMap = unboundToAttr.toMap
    val patchedWindowExpression = windowExpression.map(_.transform(unboundToAttrMap))
    newMutableProjection(
      projectList ++ patchedWindowExpression,
      child.output ++ unboundToAttr.map(_._2))()
  }

  protected override def doExecute(): RDD[InternalRow] = {
    // Prepare processing.
    // Group the window expression by their processing frame.
    val windowExprs = windowExpression.flatMap {
      _.collect {
        case e: WindowExpression => e
      }
    }

    // Create Frame processor factories and order the unbound window expressions by the frame they
    // are processed in; this is the order in which their results will be written to window
    // function result buffer.
    val framedWindowExprs = windowExprs.groupBy(_.windowSpec.frameSpecification)
    val factories = Array.ofDim[() => WindowFunctionFrame](framedWindowExprs.size)
    val unboundExpressions = mutable.Buffer.empty[Expression]
    framedWindowExprs.zipWithIndex.foreach {
      case ((frame, unboundFrameExpressions), index) =>
        // Track the ordinal.
        val ordinal = unboundExpressions.size

        // Track the unbound expressions
        unboundExpressions ++= unboundFrameExpressions

        // Bind the expressions.
        val functions = unboundFrameExpressions.map { e =>
          BindReferences.bindReference(e.windowFunction, child.output)
        }.toArray

        // Create the frame processor factory.
        factories(index) = () => createFrameProcessor(frame, functions, ordinal)
    }

    // Start processing.
    child.execute().mapPartitions { stream =>
      new Iterator[InternalRow] {

        // Get all relevant projections.
        val result = createResultProjection(unboundExpressions)
        val grouping = newProjection(partitionSpec, child.output)

        // Manage the stream and the grouping.
        var nextRow: InternalRow = EmptyRow
        var nextGroup: InternalRow = EmptyRow
        var nextRowAvailable: Boolean = false
        private[this] def fetchNextRow() {
          nextRowAvailable = stream.hasNext
          if (nextRowAvailable) {
            nextRow = stream.next()
            nextGroup = grouping(nextRow)
          } else {
            nextRow = EmptyRow
            nextGroup = EmptyRow
          }
        }
        fetchNextRow()

        // Manage the current partition.
        var rows: CompactBuffer[InternalRow] = _
        val frames: Array[WindowFunctionFrame] = factories.map(_())
        val numFrames = frames.length
        private[this] def fetchNextPartition() {
          // Collect all the rows in the current partition.
          val currentGroup = nextGroup
          rows = new CompactBuffer
          while (nextRowAvailable && nextGroup == currentGroup) {
            rows += nextRow.copy()
            fetchNextRow()
          }

          // Setup the frames.
          var i = 0
          while (i < numFrames) {
            frames(i).prepare(rows)
            i += 1
          }

          // Setup iteration
          rowIndex = 0
          rowsSize = rows.size
        }

        // Iteration
        var rowIndex = 0
        var rowsSize = 0
        override final def hasNext: Boolean = rowIndex < rowsSize || nextRowAvailable

        val join = new JoinedRow
        val windowFunctionResult = new GenericMutableRow(unboundExpressions.size)
        override final def next(): InternalRow = {
          // Load the next partition if we need to.
          if (rowIndex >= rowsSize && nextRowAvailable) {
            fetchNextPartition()
          }

          if (rowIndex < rowsSize) {
            // Get the results for the window frames.
            var i = 0
            while (i < numFrames) {
              frames(i).write(windowFunctionResult)
              i += 1
            }

            // 'Merge' the input row with the window function result
            join(rows(rowIndex), windowFunctionResult)
            rowIndex += 1

            // Return the projection.
            result(join)
          } else throw new NoSuchElementException
        }
      }
    }
  }
}

/**
 * Function for comparing boundary values.
 */
private[execution] abstract class BoundOrdering {
  def compare(input: Seq[InternalRow], inputIndex: Int, outputIndex: Int): Int
}

/**
 * Compare the input index to the bound of the output index.
 */
private[execution] final case class RowBoundOrdering(offset: Int) extends BoundOrdering {
  override def compare(input: Seq[InternalRow], inputIndex: Int, outputIndex: Int): Int =
    inputIndex - (outputIndex + offset)
}

/**
 * Compare the value of the input index to the value bound of the output index.
 */
private[execution] final case class RangeBoundOrdering(
    ordering: Ordering[InternalRow],
    current: Projection,
    bound: Projection) extends BoundOrdering {
  override def compare(input: Seq[InternalRow], inputIndex: Int, outputIndex: Int): Int =
    ordering.compare(current(input(inputIndex)), bound(input(outputIndex)))
}

/**
 * A window function calculates the results of a number of window functions for a window frame.
 * Before use a frame must be prepared by passing it all the rows in the current partition. After
 * preparation the update method can be called to fill the output rows.
 *
 * TODO How to improve performance? A few thoughts:
 * - Window functions are expensive due to its distribution and ordering requirements.
 * Unfortunately it is up to the Spark engine to solve this. Improvements in the form of project
 * Tungsten are on the way.
 * - The window frame processing bit can be improved though. But before we start doing that we
 * need to see how much of the time and resources are spent on partitioning and ordering, and
 * how much time and resources are spent processing the partitions. There are a couple ways to
 * improve on the current situation:
 * - Reduce memory footprint by performing streaming calculations. This can only be done when
 * there are no Unbound/Unbounded Following calculations present.
 * - Use Tungsten style memory usage.
 * - Use code generation in general, and use the approach to aggregation taken in the
 *   GeneratedAggregate class in specific.
 *
 * @param ordinal of the first column written by this frame.
 * @param functions to calculate the row values with.
 */
private[execution] abstract class WindowFunctionFrame(
    ordinal: Int,
    functions: Array[WindowFunction]) {

  // Make sure functions are initialized.
  functions.foreach(_.init())

  /** Number of columns the window function frame is managing */
  val numColumns = functions.length

  /**
   * Create a fresh thread safe copy of the frame.
   *
   * @return the copied frame.
   */
  def copy: WindowFunctionFrame

  /**
   * Create new instances of the functions.
   *
   * @return an array containing copies of the current window functions.
   */
  protected final def copyFunctions: Array[WindowFunction] = functions.map(_.newInstance())

  /**
   * Prepare the frame for calculating the results for a partition.
   *
   * @param rows to calculate the frame results for.
   */
  def prepare(rows: CompactBuffer[InternalRow]): Unit

  /**
   * Write the result for the current row to the given target row.
   *
   * @param target row to write the result for the current row to.
   */
  def write(target: GenericMutableRow): Unit

  /** Reset the current window functions. */
  protected final def reset(): Unit = {
    var i = 0
    while (i < numColumns) {
      functions(i).reset()
      i += 1
    }
  }

  /** Prepare an input row for processing. */
  protected final def prepare(input: InternalRow): Array[AnyRef] = {
    val prepared = new Array[AnyRef](numColumns)
    var i = 0
    while (i < numColumns) {
      prepared(i) = functions(i).prepareInputParameters(input)
      i += 1
    }
    prepared
  }

  /** Evaluate a prepared buffer (iterator). */
  protected final def evaluatePrepared(iterator: java.util.Iterator[Array[AnyRef]]): Unit = {
    reset()
    while (iterator.hasNext) {
      val prepared = iterator.next()
      var i = 0
      while (i < numColumns) {
        functions(i).update(prepared(i))
        i += 1
      }
    }
    evaluate()
  }

  /** Evaluate a prepared buffer (array). */
  protected final def evaluatePrepared(prepared: Array[Array[AnyRef]],
      fromIndex: Int, toIndex: Int): Unit = {
    var i = 0
    while (i < numColumns) {
      val function = functions(i)
      function.reset()
      var j = fromIndex
      while (j < toIndex) {
        function.update(prepared(j)(i))
        j += 1
      }
      function.evaluate()
      i += 1
    }
  }

  /** Update an array of window functions. */
  protected final def update(input: InternalRow): Unit = {
    var i = 0
    while (i < numColumns) {
      val aggregate = functions(i)
      val preparedInput = aggregate.prepareInputParameters(input)
      aggregate.update(preparedInput)
      i += 1
    }
  }

  /** Evaluate the window functions. */
  protected final def evaluate(): Unit = {
    var i = 0
    while (i < numColumns) {
      functions(i).evaluate()
      i += 1
    }
  }

  /** Fill a target row with the current window function results. */
  protected final def fill(target: GenericMutableRow, rowIndex: Int): Unit = {
    var i = 0
    while (i < numColumns) {
      target.update(ordinal + i, functions(i).get(rowIndex))
      i += 1
    }
  }
}

/**
 * The sliding window frame calculates frames with the following SQL form:
 * ... BETWEEN 1 PRECEDING AND 1 FOLLOWING
 *
 * @param ordinal of the first column written by this frame.
 * @param functions to calculate the row values with.
 * @param lbound comparator used to identify the lower bound of an output row.
 * @param ubound comparator used to identify the upper bound of an output row.
 */
private[execution] final class SlidingWindowFunctionFrame(
    ordinal: Int,
    functions: Array[WindowFunction],
    lbound: BoundOrdering,
    ubound: BoundOrdering) extends WindowFunctionFrame(ordinal, functions) {

  /** Rows of the partition currently being processed. */
  private[this] var input: CompactBuffer[InternalRow] = null

  /** Index of the first input row with a value greater than the upper bound of the current
    * output row. */
  private[this] var inputHighIndex = 0

  /** Index of the first input row with a value equal to or greater than the lower bound of the
    * current output row. */
  private[this] var inputLowIndex = 0

  /** Buffer used for storing prepared input for the window functions. */
  private[this] val buffer = new util.ArrayDeque[Array[AnyRef]]

  /** Index of the row we are currently writing. */
  private[this] var outputIndex = 0

  /** Prepare the frame for calculating a new partition. Reset all variables. */
  override def prepare(rows: CompactBuffer[InternalRow]): Unit = {
    input = rows
    inputHighIndex = 0
    inputLowIndex = 0
    outputIndex = 0
    buffer.clear()
  }

  /** Write the frame columns for the current row to the given target row. */
  override def write(target: GenericMutableRow): Unit = {
    var bufferUpdated = outputIndex == 0

    // Add all rows to the buffer for which the input row value is equal to or less than
    // the output row upper bound.
    while (inputHighIndex < input.size &&
        ubound.compare(input, inputHighIndex, outputIndex) <= 0) {
      buffer.offer(prepare(input(inputHighIndex)))
      inputHighIndex += 1
      bufferUpdated = true
    }

    // Drop all rows from the buffer for which the input row value is smaller than
    // the output row lower bound.
    while (inputLowIndex < inputHighIndex &&
        lbound.compare(input, inputLowIndex, outputIndex) < 0) {
      buffer.pop()
      inputLowIndex += 1
      bufferUpdated = true
    }

    // Only recalculate and update when the buffer changes.
    if (bufferUpdated) {
      evaluatePrepared(buffer.iterator())
      fill(target, outputIndex)
    }

    // Move to the next row.
    outputIndex += 1
  }

  /** Copy the frame. */
  override def copy: SlidingWindowFunctionFrame =
    new SlidingWindowFunctionFrame(ordinal, copyFunctions, lbound, ubound)
}

/**
 * The unbounded window frame calculates frames with the following SQL forms:
 * ... (No Frame Definition)
 * ... BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
 *
 * Its results are  the same for each and every row in the partition. This class can be seen as a
 * special case of a sliding window, but is optimized for the unbound case.
 *
 * @param ordinal of the first column written by this frame.
 * @param functions to calculate the row values with.
 */
private[execution] final class UnboundedWindowFunctionFrame(
    ordinal: Int,
    functions: Array[WindowFunction]) extends WindowFunctionFrame(ordinal, functions) {

  /** Index of the row we are currently writing. */
  private[this] var outputIndex = 0

  /** Prepare the frame for calculating a new partition. Process all rows eagerly. */
  override def prepare(rows: CompactBuffer[InternalRow]): Unit = {
    reset()
    outputIndex = 0
    val iterator = rows.iterator
    while (iterator.hasNext) {
      update(iterator.next())
    }
    evaluate()
  }

  /** Write the frame columns for the current row to the given target row. */
  override def write(target: GenericMutableRow): Unit = {
    fill(target, outputIndex)
    outputIndex += 1
  }

  /** Copy the frame. */
  override def copy: UnboundedWindowFunctionFrame =
    new UnboundedWindowFunctionFrame(ordinal, copyFunctions)
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
 * @param ordinal of the first column written by this frame.
 * @param functions to calculate the row values with.
 * @param ubound comparator used to identify the upper bound of an output row.
 */
private[execution] final class UnboundedPrecedingWindowFunctionFrame(
    ordinal: Int,
    functions: Array[WindowFunction],
    ubound: BoundOrdering) extends WindowFunctionFrame(ordinal, functions) {

  /** Rows of the partition currently being processed. */
  private[this] var input: CompactBuffer[InternalRow] = null

  /** Index of the first input row with a value greater than the upper bound of the current
    * output row. */
  private[this] var inputIndex = 0

  /** Index of the row we are currently writing. */
  private[this] var outputIndex = 0

  /** Prepare the frame for calculating a new partition. */
  override def prepare(rows: CompactBuffer[InternalRow]): Unit = {
    reset()
    input = rows
    inputIndex = 0
    outputIndex = 0
  }

  /** Write the frame columns for the current row to the given target row. */
  override def write(target: GenericMutableRow): Unit = {
    var bufferUpdated = outputIndex == 0

    // Add all rows to the aggregates for which the input row value is equal to or less than
    // the output row upper bound.
    while (inputIndex < input.size && ubound.compare(input, inputIndex, outputIndex) <= 0) {
      update(input(inputIndex))
      inputIndex += 1
      bufferUpdated = true
    }

    // Only recalculate and update when the buffer changes.
    if (bufferUpdated) {
      evaluate()
      fill(target, outputIndex)
    }

    // Move to the next row.
    outputIndex += 1
  }

  /** Copy the frame. */
  override def copy: UnboundedPrecedingWindowFunctionFrame =
    new UnboundedPrecedingWindowFunctionFrame(ordinal, copyFunctions, ubound)
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
 * the communitativity of the used window functions can be guaranteed.
 *
 * @param ordinal of the first column written by this frame.
 * @param functions to calculate the row values with.
 * @param lbound comparator used to identify the lower bound of an output row.
 */
private[execution] final class UnboundedFollowingWindowFunctionFrame(
    ordinal: Int,
    functions: Array[WindowFunction],
    lbound: BoundOrdering) extends WindowFunctionFrame(ordinal, functions) {

  /** Buffer used for storing prepared input for the window functions. */
  private[this] var buffer: Array[Array[AnyRef]] = _

  /** Rows of the partition currently being processed. */
  private[this] var input: CompactBuffer[InternalRow] = null

  /** Index of the first input row with a value equal to or greater than the lower bound of the
    * current output row. */
  private[this] var inputIndex = 0

  /** Index of the row we are currently writing. */
  private[this] var outputIndex = 0

  /** Prepare the frame for calculating a new partition. */
  override def prepare(rows: CompactBuffer[InternalRow]): Unit = {
    input = rows
    inputIndex = 0
    outputIndex = 0
    val size = input.size
    buffer = Array.ofDim(size)
    var i = 0
    while (i < size) {
      buffer(i) = prepare(input(i))
      i += 1
    }
    evaluatePrepared(buffer, 0, buffer.length)
  }

  /** Write the frame columns for the current row to the given target row. */
  override def write(target: GenericMutableRow): Unit = {
    var bufferUpdated = outputIndex == 0

    // Drop all rows from the buffer for which the input row value is smaller than
    // the output row lower bound.
    while (inputIndex < input.size && lbound.compare(input, inputIndex, outputIndex) < 0) {
      inputIndex += 1
      bufferUpdated = true
    }

    // Only recalculate and update when the buffer changes.
    if (bufferUpdated) {
      evaluatePrepared(buffer, inputIndex, buffer.length)
      fill(target, outputIndex)
    }

    // Move to the next row.
    outputIndex += 1
  }

  /** Copy the frame. */
  override def copy: UnboundedFollowingWindowFunctionFrame =
    new UnboundedFollowingWindowFunctionFrame(ordinal, copyFunctions, lbound)
}
