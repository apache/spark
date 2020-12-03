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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.types.{CalendarIntervalType, DateType, IntegerType, TimestampType}

trait WindowExecBase extends UnaryExecNode {
  def windowExpression: Seq[NamedExpression]
  def partitionSpec: Seq[Expression]
  def orderSpec: Seq[SortOrder]

  /**
   * Create the resulting projection.
   *
   * This method uses Code Generation. It can only be used on the executor side.
   *
   * @param expressions unbound ordered function expressions.
   * @return the final resulting projection.
   */
  protected def createResultProjection(expressions: Seq[Expression]): UnsafeProjection = {
    val references = expressions.zipWithIndex.map { case (e, i) =>
      // Results of window expressions will be on the right side of child's output
      BoundReference(child.output.size + i, e.dataType, e.nullable)
    }
    val unboundToRefMap = expressions.zip(references).toMap
    val patchedWindowExpression = windowExpression.map(_.transform(unboundToRefMap))
    UnsafeProjection.create(
      child.output ++ patchedWindowExpression,
      child.output)
  }

  /**
   * Create a bound ordering object for a given frame type and offset. A bound ordering object is
   * used to determine which input row lies within the frame boundaries of an output row.
   *
   * This method uses Code Generation. It can only be used on the executor side.
   *
   * @param frame to evaluate. This can either be a Row or Range frame.
   * @param bound with respect to the row.
   * @param timeZone the session local timezone for time related calculations.
   * @return a bound ordering object.
   */
  private def createBoundOrdering(
      frame: FrameType, bound: Expression, timeZone: String): BoundOrdering = {
    (frame, bound) match {
      case (RowFrame, CurrentRow) =>
        RowBoundOrdering(0)

      case (RowFrame, IntegerLiteral(offset)) =>
        RowBoundOrdering(offset)

      case (RowFrame, _) =>
        sys.error(s"Unhandled bound in windows expressions: $bound")

      case (RangeFrame, CurrentRow) =>
        val ordering = RowOrdering.create(orderSpec, child.output)
        RangeBoundOrdering(ordering, IdentityProjection, IdentityProjection)

      case (RangeFrame, offset: Expression) if orderSpec.size == 1 =>
        // Use only the first order expression when the offset is non-null.
        val sortExpr = orderSpec.head
        val expr = sortExpr.child

        // Create the projection which returns the current 'value'.
        val current = MutableProjection.create(expr :: Nil, child.output)

        // Flip the sign of the offset when processing the order is descending
        val boundOffset = sortExpr.direction match {
          case Descending => UnaryMinus(offset)
          case Ascending => offset
        }

        // Create the projection which returns the current 'value' modified by adding the offset.
        val boundExpr = (expr.dataType, boundOffset.dataType) match {
          case (DateType, IntegerType) => DateAdd(expr, boundOffset)
          case (TimestampType, CalendarIntervalType) =>
            TimeAdd(expr, boundOffset, Some(timeZone))
          case (a, b) if a == b => Add(expr, boundOffset)
        }
        val bound = MutableProjection.create(boundExpr :: Nil, child.output)

        // Construct the ordering. This is used to compare the result of current value projection
        // to the result of bound value projection. This is done manually because we want to use
        // Code Generation (if it is enabled).
        val boundSortExprs = sortExpr.copy(BoundReference(0, expr.dataType, expr.nullable)) :: Nil
        val ordering = RowOrdering.create(boundSortExprs, Nil)
        RangeBoundOrdering(ordering, current, bound)

      case (RangeFrame, _) =>
        sys.error("Non-Zero range offsets are not supported for windows " +
          "with multiple order expressions.")
    }
  }

  /**
   * Collection containing an entry for each window frame to process. Each entry contains a frame's
   * [[WindowExpression]]s and factory function for the [[WindowFrameFunction]].
   */
  protected lazy val windowFrameExpressionFactoryPairs = {
    type FrameKey = (String, FrameType, Expression, Expression)
    type ExpressionBuffer = mutable.Buffer[Expression]
    val framedFunctions = mutable.Map.empty[FrameKey, (ExpressionBuffer, ExpressionBuffer)]

    // Add a function and its function to the map for a given frame.
    def collect(tpe: String, fr: SpecifiedWindowFrame, e: Expression, fn: Expression): Unit = {
      val key = (tpe, fr.frameType, fr.lower, fr.upper)
      val (es, fns) = framedFunctions.getOrElseUpdate(
        key, (ArrayBuffer.empty[Expression], ArrayBuffer.empty[Expression]))
      es += e
      fns += fn
    }

    // Collect all valid window functions and group them by their frame.
    windowExpression.foreach { x =>
      x.foreach {
        case e @ WindowExpression(function, spec) =>
          val frame = spec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
          function match {
            case AggregateExpression(f, _, _, _, _) => collect("AGGREGATE", frame, e, f)
            case f: FrameLessOffsetWindowFunction =>
              collect("FRAME_LESS_OFFSET", f.fakeFrame, e, f)
            case f: OffsetWindowFunction if !f.ignoreNulls &&
              frame.frameType == RowFrame && frame.lower == UnboundedPreceding =>
              frame.upper match {
                case UnboundedFollowing => collect("UNBOUNDED_OFFSET", f.fakeFrame, e, f)
                case CurrentRow => collect("UNBOUNDED_PRECEDING_OFFSET", f.fakeFrame, e, f)
                case _ => collect("AGGREGATE", frame, e, f)
              }
            case f: AggregateWindowFunction => collect("AGGREGATE", frame, e, f)
            case f: PythonUDF => collect("AGGREGATE", frame, e, f)
            case f => sys.error(s"Unsupported window function: $f")
          }
        case _ =>
      }
    }

    // Map the groups to a (unbound) expression and frame factory pair.
    var numExpressions = 0
    val timeZone = conf.sessionLocalTimeZone
    framedFunctions.toSeq.map {
      case (key, (expressions, functionSeq)) =>
        val ordinal = numExpressions
        val functions = functionSeq.toArray

        // Construct an aggregate processor if we need one.
        // Currently we don't allow mixing of Pandas UDF and SQL aggregation functions
        // in a single Window physical node. Therefore, we can assume no SQL aggregation
        // functions if Pandas UDF exists. In the future, we might mix Pandas UDF and SQL
        // aggregation function in a single physical node.
        def processor = if (functions.exists(_.isInstanceOf[PythonUDF])) {
          null
        } else {
          AggregateProcessor(
            functions,
            ordinal,
            child.output,
            (expressions, schema) =>
              MutableProjection.create(expressions, schema))
        }

        // Create the factory to produce WindowFunctionFrame.
        val factory = key match {
          // Frameless offset Frame
          case ("FRAME_LESS_OFFSET", _, IntegerLiteral(offset), _) =>
            target: InternalRow =>
              new FrameLessOffsetWindowFunctionFrame(
                target,
                ordinal,
                // OFFSET frame functions are guaranteed be OffsetWindowFunction.
                functions.map(_.asInstanceOf[OffsetWindowFunction]),
                child.output,
                (expressions, schema) =>
                  MutableProjection.create(expressions, schema),
                offset)
          case ("UNBOUNDED_OFFSET", _, IntegerLiteral(offset), _) =>
            target: InternalRow => {
              new UnboundedOffsetWindowFunctionFrame(
                target,
                ordinal,
                // OFFSET frame functions are guaranteed be OffsetWindowFunction.
                functions.map(_.asInstanceOf[OffsetWindowFunction]),
                child.output,
                (expressions, schema) =>
                  MutableProjection.create(expressions, schema),
                offset)
            }
          case ("UNBOUNDED_PRECEDING_OFFSET", _, IntegerLiteral(offset), _) =>
            target: InternalRow => {
              new UnboundedPrecedingOffsetWindowFunctionFrame(
                target,
                ordinal,
                // OFFSET frame functions are guaranteed be OffsetWindowFunction.
                functions.map(_.asInstanceOf[OffsetWindowFunction]),
                child.output,
                (expressions, schema) =>
                  MutableProjection.create(expressions, schema),
                offset)
            }

          // Entire Partition Frame.
          case ("AGGREGATE", _, UnboundedPreceding, UnboundedFollowing) =>
            target: InternalRow => {
              new UnboundedWindowFunctionFrame(target, processor)
            }

          // Growing Frame.
          case ("AGGREGATE", frameType, UnboundedPreceding, upper) =>
            target: InternalRow => {
              new UnboundedPrecedingWindowFunctionFrame(
                target,
                processor,
                createBoundOrdering(frameType, upper, timeZone))
            }

          // Shrinking Frame.
          case ("AGGREGATE", frameType, lower, UnboundedFollowing) =>
            target: InternalRow => {
              new UnboundedFollowingWindowFunctionFrame(
                target,
                processor,
                createBoundOrdering(frameType, lower, timeZone))
            }

          // Moving Frame.
          case ("AGGREGATE", frameType, lower, upper) =>
            target: InternalRow => {
              new SlidingWindowFunctionFrame(
                target,
                processor,
                createBoundOrdering(frameType, lower, timeZone),
                createBoundOrdering(frameType, upper, timeZone))
            }

          case _ =>
            sys.error(s"Unsupported factory: $key")
        }

        // Keep track of the number of expressions. This is a side-effect in a map...
        numExpressions += expressions.size

        // Create the Window Expression - Frame Factory pair.
        (expressions, factory)
    }
  }
}
