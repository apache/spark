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
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.types.{StructType, NullType, IntegerType}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.CompactBuffer
import scala.collection.mutable

/**
 * :: DeveloperApi ::
 * This class calculates and outputs (windowed) aggregates over the rows in a single (sorted)
 * partition. The aggregates are calculated for each row in the group. Special processing
 * instructions, frames, are used to calculate these aggregates. Frames are processed in the order
 * specified in the window specification (the ORDER BY ... clause). There are five different frame
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
 * - Offset frame: The frame consist of one row, which is an offset number of rows away from the
 *   current row. Only non-aggregate expressions can be evaluated in a offset frame.
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
      functions: Array[Expression],
      ordinal: Int,
      result: MutableRow,
      size: MutableLiteral): WindowFunctionFrame = {
    // Construct the target row.
    val target = if (ordinal == 0) result
    else new OffsetMutableRow(ordinal, result)

    // Construct an aggregate processor if we have to.
    def processor = {
      val prepared = functions.map {
        case f: SizeBasedWindowFunction => f.withSize(size)
        case f => f
      }
      AggregateProcessor(prepared, child.output, newMutableProjection)
    }

    // Create the frame processor.
    frame match {
      // Offset Frame
      case SpecifiedWindowFrame(RowFrame, FrameBoundaryExtractor(l), FrameBoundaryExtractor(h))
          if l == h =>
        new OffsetWindowFunctionFrame(target, functions, child.output, newMutableProjection, l)

      // Growing Frame.
      case SpecifiedWindowFrame(frameType, UnboundedPreceding, FrameBoundaryExtractor(high)) =>
        val uBoundOrdering = createBoundOrdering(frameType, high)
        new UnboundedPrecedingWindowFunctionFrame(target, processor, uBoundOrdering)

      // Shrinking Frame.
      case SpecifiedWindowFrame(frameType, FrameBoundaryExtractor(low), UnboundedFollowing) =>
        val lBoundOrdering = createBoundOrdering(frameType, low)
        new UnboundedFollowingWindowFunctionFrame(target, processor, lBoundOrdering)

      // Moving Frame.
      case SpecifiedWindowFrame(frameType, FrameBoundaryExtractor(l), FrameBoundaryExtractor(h)) =>
        val lBoundOrdering = createBoundOrdering(frameType, l)
        val uBoundOrdering = createBoundOrdering(frameType, h)
        new SlidingWindowFunctionFrame(target, processor, lBoundOrdering, uBoundOrdering)

      // Entire Partition Frame.
      case SpecifiedWindowFrame(_, UnboundedPreceding, UnboundedFollowing) =>
        new UnboundedWindowFunctionFrame(target, processor)

      // Error
      case fr =>
        sys.error(s"Unsupported Frame $fr for functions: $functions")
    }
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
    val factories = Array.ofDim[(MutableRow, MutableLiteral) =>
      WindowFunctionFrame](framedWindowExprs.size)
    val unboundExpressions = mutable.Buffer.empty[Expression]
    framedWindowExprs.zipWithIndex.foreach {
      case ((frame, unboundFrameExpressions), index) =>
        // Track the ordinal.
        val ordinal = unboundExpressions.size

        // Track the unbound expressions
        unboundExpressions ++= unboundFrameExpressions

        // Bind the expressions.
        val functions = unboundFrameExpressions.map { e =>
          // Perhaps move code below to analyser. The dependency used in the pattern match might
          // be to narrow (only RankLike and its subclasses).
          val function = e.windowFunction match {
            case r: RankLike => r.withOrder(windowSpec.orderSpec)
            case f => f
          }
          BindReferences.bindReference(function, child.output)
        }.toArray

        // Create the frame processor factory.
        factories(index) = (result: MutableRow, size: MutableLiteral) =>
          createFrameProcessor(frame, functions, ordinal, result, size)
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
        val windowFunctionResult = new GenericMutableRow(unboundExpressions.size)
        val partitionSize = MutableLiteral(0, IntegerType, nullable = false)
        val frames: Array[WindowFunctionFrame] = factories.map{ f =>
          f(windowFunctionResult, partitionSize)
        }
        val numFrames = frames.length
        private[this] def fetchNextPartition() {
          // Collect all the rows in the current partition.
          val currentGroup = nextGroup
          rows = new CompactBuffer
          while (nextRowAvailable && nextGroup == currentGroup) {
            rows += nextRow.copy()
            fetchNextRow()
          }

          // Propagate partition size.
          partitionSize.value = rows.size

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
        override final def next(): InternalRow = {
          // Load the next partition if we need to.
          if (rowIndex >= rowsSize && nextRowAvailable) {
            fetchNextPartition()
          }

          if (rowIndex < rowsSize) {
            // Get the results for the window frames.
            var i = 0
            while (i < numFrames) {
              frames(i).write()
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
 */
private[execution] abstract class WindowFunctionFrame {
  /**
   * Prepare the frame for calculating the results for a partition.
   *
   * @param rows to calculate the frame results for.
   */
  def prepare(rows: CompactBuffer[InternalRow]): Unit

  /**
   * Write the current results to the target row.
   */
  def write(): Unit
}

/**
 * The offset window frame calculates frames containing LEAD/LAG statements.
 *
 * @param target to write results to.
 * @param expressions to shift a number of rows.
 * @param inputSchema required for creating a projection.
 * @param newMutableProjection function used to create the projection.
 * @param offset by which rows get moved within a partition.
 */
private[execution] final class OffsetWindowFunctionFrame(
    target: MutableRow,
    expressions: Array[Expression],
    inputSchema: Seq[Attribute],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => () => MutableProjection,
    offset: Int) extends WindowFunctionFrame {

  /** Rows of the partition currently being processed. */
  private[this] var input: CompactBuffer[InternalRow] = null

  /** Index of the row we are currently using for output. */
  private[this] var inputIndex = 0

  /** Check if the output has been explicitly cleared. */
  private[this] var outputNull = false

  /** Create a */
  private[this] val projection = newMutableProjection(expressions.toSeq, inputSchema)()
  projection.target(target)

  override def prepare(rows: CompactBuffer[InternalRow]): Unit = {
    input = rows
    inputIndex = offset
  }

  override def write(): Unit = {
    val size = input.size
    if (inputIndex >= 0 && inputIndex < size) {
      projection(input(inputIndex))
      outputNull = false
    }
    else if (!outputNull) {
      var i = 0
      while (i < expressions.length) {
        target.setNullAt(i)
        i += 1
      }
      outputNull = true
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
private[execution] final class SlidingWindowFunctionFrame(
    target: MutableRow,
    processor: AggregateProcessor,
    lbound: BoundOrdering,
    ubound: BoundOrdering) extends WindowFunctionFrame {

  /** Rows of the partition currently being processed. */
  private[this] var input: CompactBuffer[InternalRow] = null

  /** Index of the first input row with a value greater than the upper bound of the current
    * output row. */
  private[this] var inputHighIndex = 0

  /** Index of the first input row with a value equal to or greater than the lower bound of the
    * current output row. */
  private[this] var inputLowIndex = 0

  /** Buffer used for storing prepared input for the window functions. */
  private[this] val buffer = new util.ArrayDeque[InternalRow]

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
  override def write(): Unit = {
    var bufferUpdated = outputIndex == 0

    // Add all rows to the buffer for which the input row value is equal to or less than
    // the output row upper bound.
    while (inputHighIndex < input.size &&
        ubound.compare(input, inputHighIndex, outputIndex) <= 0) {
      buffer.offer(input(inputHighIndex))
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
      val iterator = buffer.iterator()
      val status = processor.initialize
      while (iterator.hasNext) {
        processor.update(status, iterator.next())
      }
      processor.evaluate(target, status)
    }

    // Move to the next row.
    outputIndex += 1
  }
}

/**
 * The unbounded window frame calculates frames with the following SQL forms:
 * ... (No Frame Definition)
 * ... BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
 *
 * Its results are  the same for each and every row in the partition. This class can be seen as a
 * special case of a sliding window, but is optimized for the unbound case.
 *
 * @param target to write results to.
 * @param processor to calculate the row values with.
 */
private[execution] final class UnboundedWindowFunctionFrame(
    target: MutableRow,
    processor: AggregateProcessor) extends WindowFunctionFrame {

  /** The collected aggregate status of all rows in the input. */
  private[this] var status: MutableRow = _

  /** Prepare the frame for calculating a new partition. Process all rows eagerly. */
  override def prepare(rows: CompactBuffer[InternalRow]): Unit = {
    status = processor.initialize
    val iterator = rows.iterator
    while (iterator.hasNext) {
      processor.update(status, iterator.next())
    }
  }

  /** Write the frame columns for the current row to the given target row. */
  override def write(): Unit = {
    // Unfortunately we cannot assume that evaluation is deterministic.
    processor.evaluate(target, status)
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
private[execution] final class UnboundedPrecedingWindowFunctionFrame(
    target: MutableRow,
    processor: AggregateProcessor,
    ubound: BoundOrdering) extends WindowFunctionFrame {

  /** Rows of the partition currently being processed. */
  private[this] var input: CompactBuffer[InternalRow] = null

  /** Index of the first input row with a value greater than the upper bound of the current
    * output row. */
  private[this] var inputIndex = 0

  /** Index of the row we are currently writing. */
  private[this] var outputIndex = 0

  /** The collected aggregate status of all rows seen so far. */
  private[this] var status: MutableRow = _

  /** Prepare the frame for calculating a new partition. */
  override def prepare(rows: CompactBuffer[InternalRow]): Unit = {
    input = rows
    inputIndex = 0
    outputIndex = 0
    status = processor.initialize
  }

  /** Write the frame columns for the current row to the given target row. */
  override def write(): Unit = {
    var bufferUpdated = outputIndex == 0

    // Add all rows to the aggregates for which the input row value is equal to or less than
    // the output row upper bound.
    while (inputIndex < input.size && ubound.compare(input, inputIndex, outputIndex) <= 0) {
      processor.update(status, input(inputIndex))
      inputIndex += 1
      bufferUpdated = true
    }

    // Only recalculate and update when the buffer changes.
    if (bufferUpdated) {
      processor.evaluate(target, status)
    }

    // Move to the next row.
    outputIndex += 1
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
 * the communitativity of the used window functions can be guaranteed.
 *
 * @param target to write results to.
 * @param processor to calculate the row values with.
 * @param lbound comparator used to identify the lower bound of an output row.
 */
private[execution] final class UnboundedFollowingWindowFunctionFrame(
    target: MutableRow,
    processor: AggregateProcessor,
    lbound: BoundOrdering) extends WindowFunctionFrame {

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
  }

  /** Write the frame columns for the current row to the given target row. */
  override def write(): Unit = {
    var bufferUpdated = outputIndex == 0

    // Drop all rows from the buffer for which the input row value is smaller than
    // the output row lower bound.
    while (inputIndex < input.size && lbound.compare(input, inputIndex, outputIndex) < 0) {
      inputIndex += 1
      bufferUpdated = true
    }

    // Only recalculate and update when the buffer changes.
    if (bufferUpdated) {
      var i = inputIndex
      val size = input.size
      val status = processor.initialize
      while (i < size) {
        processor.update(status, input(i))
        i += 1
      }
      processor.evaluate(target, status)
    }

    // Move to the next row.
    outputIndex += 1
  }
}

/**
 * This class prepares and manages the processing of a number of aggregate functions.
 *
 * The following aggregates are supported:
 * [[AggregateExpression1]]
 * [[AggregateExpression2]]
 * [[AggregateFunction2]]
 * [[AlgebraicAggregate]]
 *
 * Note that the [[AggregateExpression1]] code path will probably be removed in SPARK 1.6.0.
 *
 * The current implementation only supports evaluation in [[Complete]] mode. This is enough for
 * Window processing. Adding other processing modes is dependent on the support of
 * [[AggregateExpression1]].
 *
 * Processing of any number of distinct aggregates is supported using Set operations. More
 * advanced distinct operators (e.g. Sort Based Operators) should be added before the
 * [[AggregateProcessor]] is created.
 *
 * The implementation is split into an object which takes care of construction, and a the actual
 * processor class. Construction might be expensive and could be separated into a 'driver' and a
 * 'executor' part.
 */
private[execution] object AggregateProcessor {
  def apply(functions: Array[Expression],
            inputSchema: Seq[Attribute],
            newMutableProjection: (Seq[Expression], Seq[Attribute]) => () => MutableProjection):
      AggregateProcessor = {
    val bufferSchema = mutable.Buffer.empty[AttributeReference]
    val initialValues = mutable.Buffer.empty[Expression]
    val updateExpressions = mutable.Buffer.empty[Expression]
    val evaluateExpressions = mutable.Buffer.empty[Expression]
    val aggregates1 = mutable.Buffer.empty[AggregateExpression1]
    val aggregates1BufferOffsets = mutable.Buffer.empty[Int]
    val aggregates1OutputOffsets = mutable.Buffer.empty[Int]
    val aggregates2 = mutable.Buffer.empty[AggregateFunction2]
    val aggregates2OutputOffsets = mutable.Buffer.empty[Int]

    // Flatten AggregateExpression2's
    val flattened = functions.zipWithIndex.map {
      case (AggregateExpression2(af2, _, distinct), i) => (af2, distinct, i)
      case (e, i) => (e, false, i)
    }

    // Add distinct evaluation path.
    val distinctExpressionSchemaMap = mutable.HashMap.empty[Seq[Expression], AttributeReference]
    flattened.filter(_._2).foreach {
      case (af2, _, _) =>
        // TODO cannocalize expressions?
        val children = af2.children
        if (!distinctExpressionSchemaMap.contains(af2.children)) {
          // TODO Typing?
          val ref = AttributeReference("de", new OpenHashSetUDT(NullType), nullable = false)()
          distinctExpressionSchemaMap += children -> ref
          bufferSchema += ref
          initialValues += NewSet(NullType)
          if (children.size > 1) {
            updateExpressions += CreateStruct(children)
          } else {
            updateExpressions += children.head
          }
        }
    }

    // Add functions.
    flattened.foreach {
      case (agg: AlgebraicAggregate, true, _) =>
        val ref = distinctExpressionSchemaMap(agg.children)
        evaluateExpressions += ReduceSetAlgebraic(ref, agg)
      case (agg: AlgebraicAggregate, false, _) =>
        agg.bufferOffset = bufferSchema.size
        bufferSchema ++= agg.bufferAttributes
        initialValues ++= agg.initialValues
        updateExpressions ++= agg.updateExpressions
        evaluateExpressions += agg.evaluateExpression
      case (agg: AggregateFunction2, true, _) =>
        val ref = distinctExpressionSchemaMap(agg.children)
        evaluateExpressions += ReduceSetAggregate(ref, agg)
      case (agg: AggregateFunction2, false, i) =>
        aggregates2 += agg
        aggregates2OutputOffsets += i
        agg.bufferOffset = bufferSchema.size
        bufferSchema ++= agg.bufferAttributes
        val nops = Seq.fill(agg.bufferAttributes.size)(NoOp)
        initialValues ++= nops
        updateExpressions ++= nops
        evaluateExpressions += NoOp
      case (agg: AggregateExpression1, false, i) =>
        aggregates1 += agg
        aggregates1BufferOffsets += bufferSchema.size
        aggregates1OutputOffsets += i
        // TODO typing
        bufferSchema += AttributeReference("agg", NullType, nullable = false)()
        initialValues += NoOp
        updateExpressions += NoOp
        evaluateExpressions += NoOp
    }

    // Create the projections.
    val initialProjection = newMutableProjection(initialValues, Nil)()
    val updateProjection = newMutableProjection(updateExpressions, bufferSchema ++ inputSchema)()
    val evaluateProjection = newMutableProjection(evaluateExpressions, bufferSchema)()

    // Create the processor
    new AggregateProcessor(bufferSchema.toArray, initialProjection, updateProjection,
      evaluateProjection, aggregates2.toArray, aggregates2OutputOffsets.toArray,
      aggregates1.toArray, aggregates1BufferOffsets.toArray, aggregates1OutputOffsets.toArray)
  }
}

/**
 * This class manages the processing of a number of aggregate functions. See the documentation of
 * the object for more information.
 */
private[execution] final class AggregateProcessor(
    private[this] val bufferSchema: Array[AttributeReference],
    private[this] val initialProjection: MutableProjection,
    private[this] val updateProjection: MutableProjection,
    private[this] val evaluateProjection: MutableProjection,
    private[this] val aggregates2: Array[AggregateFunction2],
    private[this] val aggregates2OutputOffsets: Array[Int],
    private[this] val aggregates1: Array[AggregateExpression1],
    private[this] val aggregates1BufferOffsets: Array[Int],
    private[this] val aggregates1OutputOffsets: Array[Int]) {

  private[this] val join = new JoinedRow
  private[this] val bufferSchemaSize = bufferSchema.length
  private[this] val aggregates2Size = aggregates2.length
  private[this] val aggregates1Size = aggregates1.length

  // Create the initial state
  def initialize: MutableRow = {
    val buffer = new GenericMutableRow(bufferSchemaSize)
    initialProjection.target(buffer)(EmptyRow)
    var i = 0
    while (i < aggregates2Size) {
      aggregates2(i).initialize(buffer)
      i += 1
    }
    i = 0
    while (i < aggregates1Size) {
      buffer(aggregates1BufferOffsets(i)) = aggregates1(i).newInstance()
      i += 1
    }
    buffer
  }

  // Update the buffer.
  def update(buffer: MutableRow, input: InternalRow): Unit = {
    updateProjection.target(buffer)(join(buffer, input))
    var i = 0
    while (i < aggregates2Size) {
      aggregates2(i).update(buffer, input)
      i += 1
    }
    i = 0
    while (i < aggregates1Size) {
      buffer.getAs[AggregateFunction1](aggregates1BufferOffsets(i)).update(input)
      i += 1
    }
  }

  // Evaluate buffer.
  def evaluate(target: MutableRow, buffer: MutableRow): Unit = {
    evaluateProjection.target(target)(buffer)
    var i = 0
    while (i < aggregates2Size) {
      val value = aggregates2(i).eval(buffer)
      target.update(aggregates2OutputOffsets(i), value)
      i += 1
    }
    i = 0
    while (i < aggregates1Size) {
      val value = buffer.getAs[AggregateFunction1](aggregates1BufferOffsets(i)).eval(EmptyRow)
      target.update(aggregates1OutputOffsets(i), value)
      i += 1
    }
  }
}

private[execution] final class OffsetMutableRow(offset: Int, delegate: MutableRow)
    extends MutableRow {
  def setNullAt(i: Int): Unit = delegate.setNullAt(i + offset)
  def update(i: Int, value: Any): Unit = delegate.update(i + offset, value)
  def get(i: Int): Any = delegate.get(i + offset)
  def numFields: Int = delegate.numFields - offset
}
