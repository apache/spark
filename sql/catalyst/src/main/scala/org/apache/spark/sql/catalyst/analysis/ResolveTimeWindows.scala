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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cast, CreateNamedStruct, Expression, GetStructField, IsNotNull, Literal, PreciseTimestampConversion, SessionWindow, Subtract, TimeWindow, WindowTime}
import org.apache.spark.sql.catalyst.plans.logical.{Expand, Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.TIME_WINDOW
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{CalendarIntervalType, DataType, LongType, Metadata, MetadataBuilder, StructType}
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * Maps a time column to multiple time windows using the Expand operator. Since it's non-trivial to
 * figure out how many windows a time column can map to, we over-estimate the number of windows and
 * filter out the rows where the time column is not inside the time window.
 */
object TimeWindowing extends Rule[LogicalPlan] {
  import org.apache.spark.sql.catalyst.dsl.expressions._

  private final val WINDOW_COL_NAME = "window"
  private final val WINDOW_START = "start"
  private final val WINDOW_END = "end"

  /**
   * Generates the logical plan for generating window ranges on a timestamp column. Without
   * knowing what the timestamp value is, it's non-trivial to figure out deterministically how many
   * window ranges a timestamp will map to given all possible combinations of a window duration,
   * slide duration and start time (offset). Therefore, we express and over-estimate the number of
   * windows there may be, and filter the valid windows. We use last Project operator to group
   * the window columns into a struct so they can be accessed as `window.start` and `window.end`.
   *
   * The windows are calculated as below:
   * maxNumOverlapping <- ceil(windowDuration / slideDuration)
   * for (i <- 0 until maxNumOverlapping)
   *   lastStart <- timestamp - (timestamp - startTime + slideDuration) % slideDuration
   *   windowStart <- lastStart - i * slideDuration
   *   windowEnd <- windowStart + windowDuration
   *   return windowStart, windowEnd
   *
   * This behaves as follows for the given parameters for the time: 12:05. The valid windows are
   * marked with a +, and invalid ones are marked with a x. The invalid ones are filtered using the
   * Filter operator.
   * window: 12m, slide: 5m, start: 0m :: window: 12m, slide: 5m, start: 2m
   *     11:55 - 12:07 +                      11:52 - 12:04 x
   *     12:00 - 12:12 +                      11:57 - 12:09 +
   *     12:05 - 12:17 +                      12:02 - 12:14 +
   *
   * @param plan The logical plan
   * @return the logical plan that will generate the time windows using the Expand operator, with
   *         the Filter operator for correctness and Project for usability.
   */
  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
    _.containsPattern(TIME_WINDOW), ruleId) {
    case p: LogicalPlan if p.children.size == 1 =>
      val child = p.children.head
      val windowExpressions =
        p.expressions.flatMap(_.collect { case t: TimeWindow => t }).toSet

      val numWindowExpr = p.expressions.flatMap(_.collect {
        case s: SessionWindow => s
        case t: TimeWindow => t
      }).toSet.size

      // Only support a single window expression for now
      if (numWindowExpr == 1 && windowExpressions.nonEmpty &&
        windowExpressions.head.timeColumn.resolved &&
        windowExpressions.head.checkInputDataTypes().isSuccess) {

        val window = windowExpressions.head

        if (StructType.acceptsType(window.timeColumn.dataType)) {
          return p.transformExpressions {
            case t: TimeWindow => t.copy(timeColumn = WindowTime(window.timeColumn))
          }
        }

        val metadata = window.timeColumn match {
          case a: Attribute => a.metadata
          case _ => Metadata.empty
        }

        val newMetadata = new MetadataBuilder()
          .withMetadata(metadata)
          .putBoolean(TimeWindow.marker, true)
          .build()

        def getWindow(i: Int, dataType: DataType): Expression = {
          val timestamp = PreciseTimestampConversion(window.timeColumn, dataType, LongType)
          val lastStart = timestamp - (timestamp - window.startTime
            + window.slideDuration) % window.slideDuration
          val windowStart = lastStart - i * window.slideDuration
          val windowEnd = windowStart + window.windowDuration

          // We make sure value fields are nullable since the dataType of TimeWindow defines them
          // as nullable.
          CreateNamedStruct(
            Literal(WINDOW_START) ::
              PreciseTimestampConversion(windowStart, LongType, dataType).castNullable() ::
              Literal(WINDOW_END) ::
              PreciseTimestampConversion(windowEnd, LongType, dataType).castNullable() ::
              Nil)
        }

        val windowAttr = AttributeReference(
          WINDOW_COL_NAME, window.dataType, metadata = newMetadata)()

        if (window.windowDuration == window.slideDuration) {
          val windowStruct = Alias(getWindow(0, window.timeColumn.dataType), WINDOW_COL_NAME)(
            exprId = windowAttr.exprId, explicitMetadata = Some(newMetadata))

          val replacedPlan = p transformExpressions {
            case t: TimeWindow => windowAttr
          }

          // For backwards compatibility we add a filter to filter out nulls
          val filterExpr = IsNotNull(window.timeColumn)

          replacedPlan.withNewChildren(
            Project(windowStruct +: child.output,
              Filter(filterExpr, child)) :: Nil)
        } else {
          val overlappingWindows =
            math.ceil(window.windowDuration * 1.0 / window.slideDuration).toInt
          val windows =
            Seq.tabulate(overlappingWindows)(i =>
              getWindow(i, window.timeColumn.dataType))

          val projections = windows.map(_ +: child.output)

          // When the condition windowDuration % slideDuration = 0 is fulfilled,
          // the estimation of the number of windows becomes exact one,
          // which means all produced windows are valid.
          val filterExpr =
          if (window.windowDuration % window.slideDuration == 0) {
            IsNotNull(window.timeColumn)
          } else {
            window.timeColumn >= windowAttr.getField(WINDOW_START) &&
              window.timeColumn < windowAttr.getField(WINDOW_END)
          }

          val substitutedPlan = Filter(filterExpr,
            Expand(projections, windowAttr +: child.output, child))

          val renamedPlan = p transformExpressions {
            case t: TimeWindow => windowAttr
          }

          renamedPlan.withNewChildren(substitutedPlan :: Nil)
        }
      } else if (numWindowExpr > 1) {
        throw QueryCompilationErrors.multiTimeWindowExpressionsNotSupportedError(p)
      } else {
        p // Return unchanged. Analyzer will throw exception later
      }
  }
}

/** Maps a time column to a session window. */
object SessionWindowing extends Rule[LogicalPlan] {
  import org.apache.spark.sql.catalyst.dsl.expressions._

  private final val SESSION_COL_NAME = "session_window"
  private final val SESSION_START = "start"
  private final val SESSION_END = "end"

  /**
   * Generates the logical plan for generating session window on a timestamp column.
   * Each session window is initially defined as [timestamp, timestamp + gap).
   *
   * This also adds a marker to the session column so that downstream can easily find the column
   * on session window.
   */
  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case p: LogicalPlan if p.children.size == 1 =>
      val child = p.children.head
      val sessionExpressions =
        p.expressions.flatMap(_.collect { case s: SessionWindow => s }).toSet

      val numWindowExpr = p.expressions.flatMap(_.collect {
        case s: SessionWindow => s
        case t: TimeWindow => t
      }).toSet.size

      // Only support a single session expression for now
      if (numWindowExpr == 1 && sessionExpressions.nonEmpty &&
        sessionExpressions.head.timeColumn.resolved &&
        sessionExpressions.head.checkInputDataTypes().isSuccess) {

        val session = sessionExpressions.head

        if (StructType.acceptsType(session.timeColumn.dataType)) {
          return p transformExpressions {
            case t: SessionWindow => t.copy(timeColumn = WindowTime(session.timeColumn))
          }
        }

        val metadata = session.timeColumn match {
          case a: Attribute => a.metadata
          case _ => Metadata.empty
        }

        val newMetadata = new MetadataBuilder()
          .withMetadata(metadata)
          .putBoolean(SessionWindow.marker, true)
          .build()

        val sessionAttr = AttributeReference(
          SESSION_COL_NAME, session.dataType, metadata = newMetadata)()

        val sessionStart =
          PreciseTimestampConversion(session.timeColumn, session.timeColumn.dataType, LongType)
        val gapDuration = session.gapDuration match {
          case expr if Cast.canCast(expr.dataType, CalendarIntervalType) =>
            Cast(expr, CalendarIntervalType)
          case other =>
            throw QueryCompilationErrors.sessionWindowGapDurationDataTypeError(other.dataType)
        }
        val sessionEnd = PreciseTimestampConversion(session.timeColumn + gapDuration,
          session.timeColumn.dataType, LongType)

        // We make sure value fields are nullable since the dataType of SessionWindow defines them
        // as nullable.
        val literalSessionStruct = CreateNamedStruct(
          Literal(SESSION_START) ::
            PreciseTimestampConversion(sessionStart, LongType, session.timeColumn.dataType)
              .castNullable() ::
            Literal(SESSION_END) ::
            PreciseTimestampConversion(sessionEnd, LongType, session.timeColumn.dataType)
              .castNullable() ::
            Nil)

        val sessionStruct = Alias(literalSessionStruct, SESSION_COL_NAME)(
          exprId = sessionAttr.exprId, explicitMetadata = Some(newMetadata))

        val replacedPlan = p transformExpressions {
          case s: SessionWindow => sessionAttr
        }

        val filterByTimeRange = session.gapDuration match {
          case Literal(interval: CalendarInterval, CalendarIntervalType) =>
            interval == null || interval.months + interval.days + interval.microseconds <= 0
          case _ => true
        }

        // As same as tumbling window, we add a filter to filter out nulls.
        // And we also filter out events with negative or zero or invalid gap duration.
        val filterExpr = if (filterByTimeRange) {
          IsNotNull(session.timeColumn) &&
            (sessionAttr.getField(SESSION_END) > sessionAttr.getField(SESSION_START))
        } else {
          IsNotNull(session.timeColumn)
        }

        replacedPlan.withNewChildren(
          Filter(filterExpr,
            Project(sessionStruct +: child.output, child)) :: Nil)
      } else if (numWindowExpr > 1) {
        throw QueryCompilationErrors.multiTimeWindowExpressionsNotSupportedError(p)
      } else {
        p // Return unchanged. Analyzer will throw exception later
      }
  }
}

/**
 * Resolves the window_time expression which extracts the correct window time from the
 * window column generated as the output of the window aggregating operators. The
 * window column is of type struct { start: TimestampType, end: TimestampType }.
 * The correct representative event time of a window is ``window.end - 1``.
 * */
object ResolveWindowTime extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case p: LogicalPlan if p.children.size == 1 =>
      val child = p.children.head
      val windowTimeExpressions =
        p.expressions.flatMap(_.collect { case w: WindowTime => w }).toSet

      val allWindowTimeExprsResolved = windowTimeExpressions.forall { w =>
        w.windowColumn.resolved && w.checkInputDataTypes().isSuccess
      }

      if (windowTimeExpressions.nonEmpty && allWindowTimeExprsResolved) {
        val windowTimeToAttrAndNewColumn = windowTimeExpressions.map { windowTime =>
          val metadata = windowTime.windowColumn match {
            case a: Attribute => a.metadata
            case _ => Metadata.empty
          }

          if (!metadata.contains(TimeWindow.marker) &&
            !metadata.contains(SessionWindow.marker)) {
            // FIXME: error framework?
            throw new AnalysisException(
              "The input is not a correct window column: $windowTime", plan = Some(p))
          }

          val newMetadata = new MetadataBuilder()
            .withMetadata(metadata)
            .remove(TimeWindow.marker)
            .remove(SessionWindow.marker)
            .build()

          val colName = windowTime.sql

          val attr = AttributeReference(colName, windowTime.dataType, metadata = newMetadata)()

          // NOTE: "window.end" is "exclusive" upper bound of window, so if we use this value as
          // it is, it is going to be bound to the different window even if we apply the same window
          // spec. Decrease 1 microsecond from window.end to let the window_time be bound to the
          // correct window range.
          val subtractExpr =
          PreciseTimestampConversion(
            Subtract(PreciseTimestampConversion(
              GetStructField(windowTime.windowColumn, 1),
              windowTime.dataType, LongType), Literal(1L)),
            LongType,
            windowTime.dataType)

          val newColumn = Alias(subtractExpr, colName)(
            exprId = attr.exprId, explicitMetadata = Some(newMetadata))

          windowTime -> (attr, newColumn)
        }.toMap

        val replacedPlan = p transformExpressions {
          case w: WindowTime => windowTimeToAttrAndNewColumn(w)._1
        }

        val newColumnsToAdd = windowTimeToAttrAndNewColumn.values.map(_._2)
        replacedPlan.withNewChildren(
          Project(newColumnsToAdd ++: child.output, child) :: Nil)
      } else {
        p // Return unchanged. Analyzer will throw exception later
      }
  }
}
