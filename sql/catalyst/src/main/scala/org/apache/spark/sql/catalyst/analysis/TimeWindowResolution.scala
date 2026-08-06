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
import org.apache.spark.sql.catalyst.ExtendedAnalysisException
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, CaseWhen, Cast, CreateNamedStruct, Expression, GetStructField, IsNotNull, LessThan, Literal, NamedExpression, PreciseTimestampConversion, SessionWindow, Subtract, TimeWindow, WindowTime}
import org.apache.spark.sql.catalyst.plans.logical.{Expand, Filter, LogicalPlan, Project}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{CalendarIntervalType, DataType, LongType, Metadata, MetadataBuilder}
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * Plan-rewrite helpers for resolving [[TimeWindow]], [[SessionWindow]] and [[WindowTime]]
 * expressions. The bodies live here so that the synthesis logic can be reused outside of a
 * [[org.apache.spark.sql.catalyst.rules.Rule]] traversal.
 */
object TimeWindowResolution {

  final val WINDOW_COL_NAME = "window"
  final val SESSION_COL_NAME = "session_window"

  final val WINDOW_START = "start"
  final val WINDOW_END = "end"
  final val SESSION_START = "start"
  final val SESSION_END = "end"

  /**
   * Synthesizes the [[Project]]/[[Expand]]+[[Filter]] sub-plan for a resolved [[TimeWindow]] and
   * returns the window [[AttributeReference]] together with the new sub-plan that wraps `child`.
   */
  def buildTimeWindowRewrite(
      window: TimeWindow,
      child: LogicalPlan): (AttributeReference, LogicalPlan) = {
    import org.apache.spark.sql.catalyst.dsl.expressions._

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
      val remainder = (timestamp - window.startTime) % window.slideDuration
      val lastStart = timestamp - CaseWhen(Seq((LessThan(remainder, 0),
        remainder + window.slideDuration)), Some(remainder))
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

    val newChild = if (window.windowDuration == window.slideDuration) {
      val windowStruct = Alias(getWindow(0, window.timeColumn.dataType), WINDOW_COL_NAME)(
        exprId = windowAttr.exprId, explicitMetadata = Some(newMetadata))

      // For backwards compatibility we add a filter to filter out nulls
      val filterExpr = IsNotNull(window.timeColumn)

      Project(windowStruct +: child.output,
        Filter(filterExpr, child))
    } else {
      val overlappingWindows = overlappingWindowCount(window)
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

      Filter(filterExpr,
        Expand(projections, windowAttr +: child.output, child))
    }

    (windowAttr, newChild)
  }

  /**
   * Synthesizes the [[Project]]+[[Filter]] sub-plan for a resolved [[SessionWindow]] and returns
   * the session window [[AttributeReference]] together with the new sub-plan that wraps `child`.
   */
  def buildSessionWindowRewrite(
      session: SessionWindow,
      child: LogicalPlan): (AttributeReference, LogicalPlan) = {
    import org.apache.spark.sql.catalyst.dsl.expressions._

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
      case expr if expr.dataType == CalendarIntervalType =>
        expr
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

    val filterByTimeRange = sessionFilterByTimeRange(gapDuration)

    // As same as tumbling window, we add a filter to filter out nulls.
    // And we also filter out events with negative or zero or invalid gap duration.
    val filterExpr = if (filterByTimeRange) {
      IsNotNull(session.timeColumn) &&
        (sessionAttr.getField(SESSION_END) > sessionAttr.getField(SESSION_START))
    } else {
      IsNotNull(session.timeColumn)
    }

    val newChild = Filter(filterExpr,
      Project(sessionStruct +: child.output, child))

    (sessionAttr, newChild)
  }

  /**
   * Synthesizes a [[Project]] sub-plan that exposes one extracted-time column per resolved
   * [[WindowTime]] in `windowTimeExpressions`, wrapping `child`. Returns the per-[[WindowTime]]
   * [[AttributeReference]] map and the new sub-plan. `errorContext` is the plan attached to the
   * [[ExtendedAnalysisException]] thrown when a window-column metadata marker is missing.
   */
  def buildWindowTimeRewrite(
      windowTimeExpressions: Set[WindowTime],
      child: LogicalPlan,
      errorContext: LogicalPlan): (Map[WindowTime, AttributeReference], LogicalPlan) = {
    val windowTimeToAttrAndNewColumn = windowTimeExpressions.map { windowTime =>
      val metadata = windowTime.windowColumn match {
        case a: Attribute => a.metadata
        case _ => Metadata.empty
      }

      if (!metadata.contains(TimeWindow.marker) &&
        !metadata.contains(SessionWindow.marker)) {
        throw new ExtendedAnalysisException(
          new AnalysisException(
            errorClass = "_LEGACY_ERROR_TEMP_3101",
            messageParameters = Map("windowTime" -> windowTime.toString)),
          plan = errorContext)
      }

      val newMetadata = new MetadataBuilder()
        .withMetadata(metadata)
        .remove(TimeWindow.marker)
        .remove(SessionWindow.marker)
        .build()

      val colName = windowTime.sql

      val attr = AttributeReference(colName, windowTime.dataType, metadata = newMetadata)()

      val newColumn = Alias(windowTimeExtractionExpression(windowTime), colName)(
        exprId = attr.exprId, explicitMetadata = Some(newMetadata))

      windowTime -> (attr, newColumn)
    }.toMap

    val newColumns: Seq[NamedExpression] =
      windowTimeToAttrAndNewColumn.values.map(_._2).toSeq
    val newChild = Project(newColumns ++ child.output, child)

    val windowTimeToAttr = windowTimeToAttrAndNewColumn.map {
      case (windowTime, (attr, _)) => windowTime -> attr
    }

    (windowTimeToAttr, newChild)
  }

  /**
   * Builds the expression extracting a [[WindowTime]]'s timestamp: the last microsecond of the
   * source window (`window.end - 1us`). `window.end` is the exclusive upper bound, so using it
   * as-is would bind the result to the next window under the same window spec.
   */
  def windowTimeExtractionExpression(windowTime: WindowTime): Expression =
    PreciseTimestampConversion(
      Subtract(
        PreciseTimestampConversion(
          GetStructField(windowTime.windowColumn, 1),
          windowTime.dataType,
          LongType),
        Literal(1L)),
      LongType,
      windowTime.dataType)

  /** Number of overlapping sliding windows a single row can fall into. */
  def overlappingWindowCount(window: TimeWindow): Int =
    math.ceil(window.windowDuration * 1.0 / window.slideDuration).toInt

  /**
   * Whether the session-window filter must also drop empty windows (`end <= start`): true when the
   * gap is non-foldable (unknown at plan time) or a foldable gap is null or non-positive.
   */
  def sessionFilterByTimeRange(gapDuration: Expression): Boolean =
    if (gapDuration.foldable) {
      val interval = gapDuration.eval().asInstanceOf[CalendarInterval]
      interval == null || interval.months + interval.days + interval.microseconds <= 0
    } else {
      true
    }
}
