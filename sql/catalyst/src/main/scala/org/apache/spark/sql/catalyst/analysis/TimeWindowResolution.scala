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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, CaseWhen, Cast, CreateNamedStruct, ExpectsInputTypes, Expression, GetStructField, IsNotNull, LessThan, Literal, NamedExpression, PreciseTimestampConversion, PreciseTimestampNanosConversion, SessionWindow, Subtract, TimestampAddInterval, TimeWindow, UnaryExpression, WindowTime}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.plans.logical.{Expand, Filter, LogicalPlan, Project}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{AbstractDataType, AnyTimestampNanoType, CalendarIntervalType, DataType, DayTimeIntervalType, LongType, Metadata, MetadataBuilder, TimestampLTZNanosType, TimestampNTZNanosType}
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

  /** Scale factor from microseconds to nanoseconds. */
  private final val NANOS_PER_MICRO = 1000L

  /** Returns true if the given data type is a nanosecond-precision timestamp type. */
  private def isNanosType(dt: DataType): Boolean = dt.isInstanceOf[AnyTimestampNanoType]

  /** Extracts the precision from a nanosecond timestamp type. Defaults to 9 for non-nanos. */
  private def nanosPrecision(dt: DataType): Int = dt match {
    case t: TimestampNTZNanosType => t.precision
    case t: TimestampLTZNanosType => t.precision
    case _ => 9
  }

  /** Converts a timestamp expression to Long (epoch micros or epoch nanos). */
  private def timestampToLong(expr: Expression, dataType: DataType): Expression = {
    if (isNanosType(dataType)) {
      PreciseTimestampNanosConversion(expr, dataType, LongType, nanosPrecision(dataType))
    } else {
      PreciseTimestampConversion(expr, dataType, LongType)
    }
  }

  /** Converts a Long expression (epoch micros or epoch nanos) back to timestamp type. */
  private def longToTimestamp(expr: Expression, dataType: DataType): Expression = {
    if (isNanosType(dataType)) {
      PreciseTimestampNanosConversion(expr, LongType, dataType, nanosPrecision(dataType))
    } else {
      PreciseTimestampConversion(expr, LongType, dataType)
    }
  }

  /** Scales a duration in microseconds to the appropriate unit for the given timestamp type. */
  private def scaleDuration(microsDuration: Long, dataType: DataType): Long = {
    if (isNanosType(dataType)) {
      Math.multiplyExact(microsDuration, NANOS_PER_MICRO)
    } else {
      microsDuration
    }
  }

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
      val timestamp = timestampToLong(window.timeColumn, dataType)
      val windowDuration = scaleDuration(window.windowDuration, dataType)
      val slideDuration = scaleDuration(window.slideDuration, dataType)
      val startTime = scaleDuration(window.startTime, dataType)
      val remainder = (timestamp - startTime) % slideDuration
      val lastStart = timestamp - CaseWhen(Seq((LessThan(remainder, 0),
        remainder + slideDuration)), Some(remainder))
      val windowStart = lastStart - i * slideDuration
      val windowEnd = windowStart + windowDuration

      // We make sure value fields are nullable since the dataType of TimeWindow defines them
      // as nullable.
      CreateNamedStruct(
        Literal(WINDOW_START) ::
          longToTimestamp(windowStart, dataType).castNullable() ::
          Literal(WINDOW_END) ::
          longToTimestamp(windowEnd, dataType).castNullable() ::
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
      timestampToLong(session.timeColumn, session.timeColumn.dataType)
    val gapDuration = session.gapDuration match {
      case expr if expr.dataType == CalendarIntervalType =>
        expr
      case expr if Cast.canCast(expr.dataType, CalendarIntervalType) =>
        Cast(expr, CalendarIntervalType)
      case other =>
        throw QueryCompilationErrors.sessionWindowGapDurationDataTypeError(other.dataType)
    }
    val sessionEnd = if (isNanosType(session.timeColumn.dataType)) {
      // For nanosecond-precision timestamps, convert the CalendarInterval gap to
      // DayTimeIntervalType (microseconds) and use TimestampAddInterval which natively
      // supports DayTimeIntervalType + nanos timestamps (timezone-aware).
      val gapAsDayTime = CalendarIntervalToMicros(gapDuration)
      timestampToLong(
        TimestampAddInterval(session.timeColumn, gapAsDayTime),
        session.timeColumn.dataType)
    } else {
      timestampToLong(session.timeColumn + gapDuration, session.timeColumn.dataType)
    }

    // We make sure value fields are nullable since the dataType of SessionWindow defines them
    // as nullable.
    val literalSessionStruct = CreateNamedStruct(
      Literal(SESSION_START) ::
        longToTimestamp(sessionStart, session.timeColumn.dataType)
          .castNullable() ::
        Literal(SESSION_END) ::
        longToTimestamp(sessionEnd, session.timeColumn.dataType)
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
   * Builds the expression extracting a [[WindowTime]]'s timestamp: the last unit of the
   * source window (`window.end - 1us` for microsecond types, `window.end - 1ns` for nanosecond
   * types). `window.end` is the exclusive upper bound, so using it as-is would bind the result
   * to the next window under the same window spec.
   */
  def windowTimeExtractionExpression(windowTime: WindowTime): Expression = {
    val dt = windowTime.dataType
    // For nanos types, we subtract 1 (nanosecond in epoch-nanos space).
    // For micros types, we subtract 1 (microsecond in epoch-micros space).
    // Both subtraction amounts are Literal(1L) because the conversion to Long already handles
    // the appropriate unit (epoch-nanos vs epoch-micros).
    longToTimestamp(
      Subtract(
        timestampToLong(GetStructField(windowTime.windowColumn, 1), dt),
        Literal(1L)),
      dt)
  }

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

/**
 * Internal expression that converts a [[CalendarInterval]] to total microseconds, typed as
 * [[DayTimeIntervalType]]. Used by session window rewrite for nanosecond-precision timestamp
 * types so that [[TimestampAddInterval]] can be used (it supports DayTimeIntervalType + nanos
 * timestamps natively with timezone awareness).
 *
 * Computes: `interval.days * MICROS_PER_DAY + interval.microseconds`.
 * Months are assumed to be zero (session window rejects monthly intervals).
 */
private[analysis] case class CalendarIntervalToMicros(child: Expression)
  extends UnaryExpression with ExpectsInputTypes {
  import org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_DAY

  override def nullIntolerant: Boolean = true
  override def inputTypes: Seq[AbstractDataType] = Seq(CalendarIntervalType)
  override def dataType: DataType = DayTimeIntervalType()

  override def nullSafeEval(input: Any): Any = {
    val interval = input.asInstanceOf[CalendarInterval]
    Math.addExact(
      Math.multiplyExact(interval.days.toLong, MICROS_PER_DAY),
      interval.microseconds)
  }

  override def doGenCode(
      ctx: CodegenContext,
      ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    ev.copy(code = eval.code +
      code"""boolean ${ev.isNull} = ${eval.isNull};
         |long ${ev.value} = ${eval.isNull} ? 0L :
         |  Math.addExact(
         |    Math.multiplyExact((long) ${eval.value}.days, ${MICROS_PER_DAY}L),
         |    ${eval.value}.microseconds);
       """.stripMargin)
  }

  override protected def withNewChildInternal(newChild: Expression): CalendarIntervalToMicros =
    copy(child = newChild)
}
