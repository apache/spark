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

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Add, AggregateWindowFunction, Ascending, Attribute, BoundReference, CurrentRow, DateAdd, DateAddYMInterval, DecimalAddNoOverflowCheck, Descending, Expression, ExtractANSIIntervalDays, FrameLessOffsetWindowFunction, FrameType, IdentityProjection, IntegerLiteral, MutableProjection, NamedExpression, OffsetWindowFunction, PythonFuncExpression, RangeFrame, RowFrame, RowOrdering, SortOrder, SpecifiedWindowFrame, TimestampAddInterval, TimestampAddYMInterval, UnaryMinus, UnboundedFollowing, UnboundedPreceding, UnsafeProjection, WindowExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, DeclarativeAggregate}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{CalendarIntervalType, DateType, DayTimeIntervalType, DecimalType, IntegerType, TimestampNTZType, TimestampType, YearMonthIntervalType}
import org.apache.spark.sql.types.DayTimeIntervalType.DAY
import org.apache.spark.util.collection.Utils

trait WindowEvaluatorFactoryBase {
  def windowExpression: Seq[NamedExpression]
  def partitionSpec: Seq[Expression]
  def orderSpec: Seq[SortOrder]
  def childOutput: Seq[Attribute]
  def spillSize: SQLMetric
  /**
   * Counters for [[SegmentTreeWindowFunctionFrame]] observability. Default
   * `None` means the subclass does not integrate with the segment-tree frame
   * path (e.g. [[org.apache.spark.sql.execution.python.ArrowWindowPythonEvaluatorFactory]]);
   * only [[WindowEvaluatorFactory]] wires them.
   */
  def numSegmentTreeFrames: Option[SQLMetric] = None
  def numSegmentTreeFallbackFrames: Option[SQLMetric] = None

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
      BoundReference(childOutput.size + i, e.dataType, e.nullable)
    }
    val unboundToRefMap = Utils.toMap(expressions, references)
    val patchedWindowExpression = windowExpression.map(_.transform(unboundToRefMap))
    UnsafeProjection.create(
      childOutput ++ patchedWindowExpression,
      childOutput)
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
        throw SparkException.internalError(s"Unhandled bound in windows expressions: $bound")

      case (RangeFrame, CurrentRow) =>
        val ordering = RowOrdering.create(orderSpec, childOutput)
        RangeBoundOrdering(ordering, IdentityProjection, IdentityProjection)

      case (RangeFrame, offset: Expression) if orderSpec.size == 1 =>
        // Use only the first order expression when the offset is non-null.
        val sortExpr = orderSpec.head
        val expr = sortExpr.child

        // Create the projection which returns the current 'value'.
        val current = MutableProjection.create(expr :: Nil, childOutput)

        // Flip the sign of the offset when processing the order is descending
        val boundOffset = sortExpr.direction match {
          case Descending => UnaryMinus(offset)
          case Ascending => offset
        }

        // Create the projection which returns the current 'value' modified by adding the offset.
        val boundExpr = (expr.dataType, boundOffset.dataType) match {
          case (DateType, IntegerType) => DateAdd(expr, boundOffset)
          case (DateType, _: YearMonthIntervalType) => DateAddYMInterval(expr, boundOffset)
          case (DateType, DayTimeIntervalType(DAY, DAY)) =>
            DateAdd(expr, ExtractANSIIntervalDays(boundOffset))
          case (TimestampType | TimestampNTZType, CalendarIntervalType) =>
            TimestampAddInterval(expr, boundOffset, Some(timeZone))
          case (TimestampType | TimestampNTZType, _: YearMonthIntervalType) =>
            TimestampAddYMInterval(expr, boundOffset, Some(timeZone))
          case (TimestampType | TimestampNTZType, _: DayTimeIntervalType) =>
            TimestampAddInterval(expr, boundOffset, Some(timeZone))
          case (d: DecimalType, _: DecimalType) => DecimalAddNoOverflowCheck(expr, boundOffset, d)
          case (a, b) if a == b => Add(expr, boundOffset)
        }
        val bound = MutableProjection.create(boundExpr :: Nil, childOutput)

        // Construct the ordering. This is used to compare the result of current value projection
        // to the result of bound value projection. This is done manually because we want to use
        // Code Generation (if it is enabled).
        val boundSortExprs = sortExpr.copy(BoundReference(0, expr.dataType, expr.nullable)) :: Nil
        val ordering = RowOrdering.create(boundSortExprs, Nil)
        RangeBoundOrdering(ordering, current, bound)

      case (RangeFrame, _) =>
        throw SparkException.internalError("Non-Zero range offsets are not supported for windows " +
          "with multiple order expressions.")
    }
  }

  /**
   * Collection containing an entry for each window frame to process. Each entry contains a frame's
   * [[WindowExpression]]s and factory function for the [[WindowFunctionFrame]].
   */
  protected lazy val windowFrameExpressionFactoryPairs = {
    type FrameKey = (String, FrameType, Expression, Expression, Seq[Expression])
    type ExpressionBuffer = mutable.Buffer[Expression]
    val framedFunctions = mutable.Map.empty[FrameKey, (ExpressionBuffer, ExpressionBuffer)]

    // Add a function and its function to the map for a given frame.
    def collect(tpe: String, fr: SpecifiedWindowFrame, e: Expression, fn: Expression): Unit = {
      val key = fn match {
        // This branch is used for Lead/Lag to support ignoring null and optimize the performance
        // for NthValue ignoring null.
        // All window frames move in rows. If there are multiple Leads, Lags or NthValues acting on
        // a row and operating on different input expressions, they should not be moved uniformly
        // by row. Therefore, we put these functions in different window frames.
        case f: OffsetWindowFunction if f.ignoreNulls =>
          (tpe, fr.frameType, fr.lower, fr.upper, f.children.map(_.canonicalized))
        case _ => (tpe, fr.frameType, fr.lower, fr.upper, Nil)
      }
      val (es, fns) = framedFunctions.getOrElseUpdate(
        key, (ArrayBuffer.empty[Expression], ArrayBuffer.empty[Expression]))
      es += e
      fns += fn
    }

    // Collect all valid window functions and group them by their frame.
    windowExpression.foreach { x =>
      x.foreach {
        case e@WindowExpression(function, spec) =>
          val frame = spec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
          function match {
            case AggregateExpression(f, _, _, _, _) => collect("AGGREGATE", frame, e, f)
            case f: FrameLessOffsetWindowFunction =>
              collect("FRAME_LESS_OFFSET", f.fakeFrame, e, f)
            case f: OffsetWindowFunction if frame.frameType == RowFrame &&
              frame.lower == UnboundedPreceding =>
              frame.upper match {
                case UnboundedFollowing => collect("UNBOUNDED_OFFSET", f.fakeFrame, e, f)
                case CurrentRow => collect("UNBOUNDED_PRECEDING_OFFSET", f.fakeFrame, e, f)
                case _ => collect("AGGREGATE", frame, e, f)
              }
            case f: AggregateWindowFunction => collect("AGGREGATE", frame, e, f)
            case f => throw SparkException.internalError(s"Unsupported window function: $f")
          }
        case _ =>
      }
    }

    // Map the groups to a (unbound) expression and frame factory pair.
    var numExpressions = 0
    val timeZone = SQLConf.get.sessionLocalTimeZone
    framedFunctions.toSeq.map {
      case (key, (expressions, functionSeq)) =>
        val ordinal = numExpressions
        val functions = functionSeq.toArray

        // Construct an aggregate processor if we need one.
        // Currently we don't allow mixing of Pandas UDF and SQL aggregation functions
        // in a single Window physical node. Therefore, we can assume no SQL aggregation
        // functions if Pandas UDF exists. In the future, we might mix Pandas UDF and SQL
        // aggregation function in a single physical node.
        val aggFilters: Array[Option[Expression]] = expressions.map {
          case WindowExpression(ae: AggregateExpression, _) => ae.filter
          case _ => None
        }.toArray
        // Keep as `def` (lazy / per-call): the FRAME_LESS_OFFSET /
        // UNBOUNDED_OFFSET / UNBOUNDED_PRECEDING_OFFSET branches do not read
        // `processor`. Eager `val` construction would invoke
        // `AggregateProcessor.apply` on Lag / Lead / NthValue and throw
        // `INTERNAL_ERROR: Unsupported aggregate function`.
        def processor = if (functions.exists(_.isInstanceOf[PythonFuncExpression])) {
          null
        } else {
          AggregateProcessor(
            functions,
            ordinal,
            childOutput,
            (expressions, schema) =>
              MutableProjection.create(expressions, schema),
            aggFilters)
        }
        val conf = SQLConf.get
        val blockSize = conf.windowSegmentTreeBlockSize

        // Create the factory to produce WindowFunctionFrame.
        val factory = key match {
          // Frameless offset Frame
          case ("FRAME_LESS_OFFSET", _, IntegerLiteral(offset), _, expr) =>
            target: InternalRow =>
              new FrameLessOffsetWindowFunctionFrame(
                target,
                ordinal,
                // OFFSET frame functions are guaranteed be OffsetWindowFunction.
                functions.map(_.asInstanceOf[OffsetWindowFunction]),
                childOutput,
                (expressions, schema) =>
                  MutableProjection.create(expressions, schema),
                offset,
                expr.nonEmpty)
          case ("UNBOUNDED_OFFSET", _, IntegerLiteral(offset), _, expr) =>
            target: InternalRow => {
              new UnboundedOffsetWindowFunctionFrame(
                target,
                ordinal,
                // OFFSET frame functions are guaranteed be OffsetWindowFunction.
                functions.map(_.asInstanceOf[OffsetWindowFunction]),
                childOutput,
                (expressions, schema) =>
                  MutableProjection.create(expressions, schema),
                offset,
                expr.nonEmpty)
            }
          case ("UNBOUNDED_PRECEDING_OFFSET", _, IntegerLiteral(offset), _, expr) =>
            target: InternalRow => {
              new UnboundedPrecedingOffsetWindowFunctionFrame(
                target,
                ordinal,
                // OFFSET frame functions are guaranteed be OffsetWindowFunction.
                functions.map(_.asInstanceOf[OffsetWindowFunction]),
                childOutput,
                (expressions, schema) =>
                  MutableProjection.create(expressions, schema),
                offset,
                expr.nonEmpty)
            }

          // Entire Partition Frame.
          case ("AGGREGATE", _, UnboundedPreceding, UnboundedFollowing, _) =>
            target: InternalRow => {
              new UnboundedWindowFunctionFrame(target, processor)
            }

          // Growing Frame.
          case ("AGGREGATE", frameType, UnboundedPreceding, upper, _) =>
            target: InternalRow => {
              new UnboundedPrecedingWindowFunctionFrame(
                target,
                processor,
                createBoundOrdering(frameType, upper, timeZone))
            }

          // Shrinking Frame.
          case ("AGGREGATE", frameType, lower, UnboundedFollowing, _) =>
            target: InternalRow => {
              new UnboundedFollowingWindowFunctionFrame(
                target,
                processor,
                createBoundOrdering(frameType, lower, timeZone))
            }

          // Moving Frame.
          case ("AGGREGATE", frameType, lower, upper, _) =>
            if (eligibleForSegTree(functions, aggFilters, frameType, conf)) {
              val segFns = functions.map(_.asInstanceOf[DeclarativeAggregate])
              val cacheHint = estimateMaxCachedBlocks(lower, upper, frameType, blockSize)
              target: InternalRow => {
                // Task-completion listener registration lives inside the frame
                // constructor (one per frame instance) to avoid duplicates when
                // this closure fires multiple times per task. `TaskContext.get()`
                // is only called at task-execution time, never at driver planning.
                val tc = TaskContext.get()
                if (tc == null) {
                  throw SparkException.internalError(
                    "WindowEvaluatorFactoryBase.segTreeFrameFactory requires " +
                      "an active TaskContext")
                }
                val tmm = tc.taskMemoryManager()
                new SegmentTreeWindowFunctionFrame(
                  target,
                  processor,
                  segFns,
                  childOutput,
                  frameType,
                  createBoundOrdering(frameType, lower, timeZone),
                  createBoundOrdering(frameType, upper, timeZone),
                  (e, s) => MutableProjection.create(e, s),
                  conf,
                  cacheHint,
                  tmm,
                  numSegmentTreeFrames,
                  numSegmentTreeFallbackFrames)
              }
            } else {
              target: InternalRow => {
                new SlidingWindowFunctionFrame(
                  target,
                  processor,
                  createBoundOrdering(frameType, lower, timeZone),
                  createBoundOrdering(frameType, upper, timeZone))
              }
            }

          case _ =>
            throw SparkException.internalError(s"Unsupported factory: $key")
        }

        // Keep track of the number of expressions. This is a side-effect in a map...
        numExpressions += expressions.size

        // Create the Window Expression - Frame Factory pair.
        (expressions, factory)
    }
  }

  /**
   * Segment-tree path eligibility. The tree relies on
   * `DeclarativeAggregate.mergeExpressions`, which [[AggregateWindowFunction]]s
   * (NthValue, NTile, Rank, RowNumber, NullIndex) refuse via
   * `mergeUnsupportedByWindowFunctionError`: they extend DeclarativeAggregate
   * but are NOT merge-capable. Normal aggregate window expressions reach this
   * code as the inner DeclarativeAggregate unwrapped from
   * [[AggregateExpression]] (see `windowFrameExpressionFactoryPairs.collect`).
   *
   * DISTINCT aggregate window expressions are already rejected earlier in
   * analysis by `WindowResolution.checkWindowFunction`
   * (error class `DISTINCT_WINDOW_FUNCTION_UNSUPPORTED`), so no explicit
   * `isDistinct` gate is needed here.
   */
  private def eligibleForSegTree(
      functions: Array[Expression],
      filters: Array[Option[Expression]],
      frameType: FrameType,
      conf: SQLConf): Boolean = {
    // RANGE accepted only for single-column order specs. Multi-column RANGE
    // with non-zero offset is already rejected by `createBoundOrdering`, so
    // gating here on `orderSpec.size == 1` matches the Sliding-path invariant.
    val frameTypeOk = frameType match {
      case RowFrame => true
      case RangeFrame => orderSpec.size == 1
      case _ => false
    }
    conf.windowSegmentTreeEnabled &&
      frameTypeOk &&
      filters.forall(_.isEmpty) &&
      functions.forall(WindowSegmentTree.isEligible)
  }

  private def estimateMaxCachedBlocks(
      lower: Expression,
      upper: Expression,
      frameType: FrameType,
      blockSize: Int): Option[Int] = {
    // Reached via the moving-frame branch after `eligibleForSegTree`. Under
    // RANGE the frame width is data-dependent (defined by order-key distance,
    // not row count), so no static width inference is possible; fall back to
    // a default budget and rely on the runtime LRU + TMM spiller.
    assert(frameType == RowFrame || frameType == RangeFrame,
      s"estimateMaxCachedBlocks expects RowFrame or RangeFrame, got $frameType")
    if (frameType == RangeFrame) {
      return Some(8)
    }
    val w: Option[Int] = (lower, upper) match {
      case (CurrentRow, CurrentRow) => Some(1)
      case (IntegerLiteral(lo), IntegerLiteral(hi)) => Some(math.abs(hi - lo) + 1)
      case (CurrentRow, IntegerLiteral(hi)) => Some(math.abs(hi) + 1)
      case (IntegerLiteral(lo), CurrentRow) => Some(math.abs(lo) + 1)
      case _ => None
    }
    // `ceil(W / blockSize)` is the minimum number of blocks a single frame can
    // straddle; `+ 2` adds one block of slack at each end to cover the case
    // where the frame's [lower, upper) interval is offset within its leftmost
    // block (so the cursor temporarily holds the previous block as well) and
    // the symmetric case at the right edge -- without this slack the LRU
    // would thrash on the boundary blocks every time the cursor advances.
    w.map(ww => math.ceil(ww.toDouble / blockSize).toInt + 2).orElse(Some(8))
  }

}
