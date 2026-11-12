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

package org.apache.spark.sql.execution.streaming.operators.stateful.join

import java.util.Locale

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * The window of the column that the other input's V4 state is ordered by, which a row of this
 * input can match. For each row, `rangeFor` returns the window, both ends inclusive, between the
 * highest lower bound and the lowest upper bound that the join condition implies for that
 * column, or None when it implies neither. A bound is evaluated on the row itself, so it may
 * depend on any column of the row, as in `rightTime BETWEEN leftTime AND expiry`. The window is
 * empty when the bounds cross.
 *
 * A bound must never exclude a row that the condition matches; the join still checks the full
 * condition on every row in the window. A bound that cannot be computed exactly is left out:
 * - Only deterministic conjuncts give bounds. Spark checks each of them on every joined row,
 *   either before the row is stored or after the rows are joined.
 * - A comparison gives a bound when one side reaches the column through steps the bound can be
 *   inverted through exactly, and the other side reads only this input. A strict comparison
 *   gives an inclusive bound.
 * - A NULL bound, or one that fails to evaluate or overflows, leaves its end of the window open.
 *   A NULL makes the comparison unknown, so the row matches nothing, but an open end is enough.
 */
private[sql] class MatchingScanWindow private (
    lowerBounds: Seq[MatchingScanWindow.ColumnBound],
    upperBounds: Seq[MatchingScanWindow.ColumnBound],
    inputAttributes: Seq[Attribute]) {

  private[this] val lower = lowerBounds.map(_.evaluator(inputAttributes))
  private[this] val upper = upperBounds.map(_.evaluator(inputAttributes))

  def rangeFor(row: InternalRow): Option[(Long, Long)] = {
    var min = Long.MinValue
    var max = Long.MaxValue
    lower.foreach(_(row).foreach(bound => min = math.max(min, bound)))
    upper.foreach(_(row).foreach(bound => max = math.min(max, bound)))
    if (min == Long.MinValue && max == Long.MaxValue) None else Some((min, max))
  }
}

private[sql] object MatchingScanWindow extends PredicateHelper {

  /**
   * A bound on the indexed column: `value` is evaluated on this input's row, and `toColumn`
   * turns a non-NULL result into the bound in microseconds, or None if there is none.
   */
  case class ColumnBound(value: Expression, toColumn: Long => Option[Long]) {
    def evaluator(inputAttributes: Seq[Attribute]): InternalRow => Option[Long] = {
      val projection = UnsafeProjection.create(Seq(value), inputAttributes)
      row =>
        try {
          val result = projection(row)
          if (result.isNullAt(0)) None else toColumn(result.getLong(0))
        } catch {
          // The condition may still evaluate without error, for example when no state row is
          // read, so the error must not fail the query here.
          case NonFatal(_) => None
        }
    }
  }

  /**
   * The window of `indexedColumn`, a TIMESTAMP column of the other input, implied by
   * `condition` for rows of the input with `inputAttributes`. Returns None when the condition
   * bounds the column in neither direction.
   */
  def apply(
      condition: Option[Expression],
      inputAttributes: Seq[Attribute],
      indexedColumn: Attribute): Option[MatchingScanWindow] = {
    if (indexedColumn.dataType != TimestampType) return None
    val bounds = condition.toSeq.flatMap(splitConjunctivePredicates).filter(_.deterministic)
      .flatMap(boundsOf(_, AttributeSet(inputAttributes), indexedColumn))
    val (lower, upper) = bounds.partition(_._1)
    if (bounds.isEmpty) {
      None
    } else {
      Some(new MatchingScanWindow(lower.map(_._2), upper.map(_._2), inputAttributes))
    }
  }

  /** Bounds of one conjunct, each with whether it is a lower bound. */
  private def boundsOf(
      conjunct: Expression,
      input: AttributeSet,
      column: Attribute): Seq[(Boolean, ColumnBound)] = {
    // `side >= other` (or > ) bounds the column from below if it is in `side`, and from above if
    // it is in `other`.
    def atLeast(side: Expression, other: Expression): Seq[(Boolean, ColumnBound)] =
      (solve(side, other, isLower = true, input, column) ++
        solve(other, side, isLower = false, input, column)).toSeq
    conjunct match {
      case GreaterThanOrEqual(l, r) => atLeast(l, r)
      case GreaterThan(l, r) => atLeast(l, r)
      case LessThanOrEqual(l, r) => atLeast(r, l)
      case LessThan(l, r) => atLeast(r, l)
      case EqualTo(l, r) => atLeast(l, r) ++ atLeast(r, l)
      case Between(value, lower, upper, _) =>
        atLeast(value, lower) ++ atLeast(upper, value)
      case _ => Nil
    }
  }

  /**
   * The bound on the column given `side >= value` (isLower) or `side <= value`, where `side`
   * holds the column and `value` reads only this input.
   */
  private def solve(
      side: Expression,
      value: Expression,
      isLower: Boolean,
      input: AttributeSet,
      column: Attribute): Option[(Boolean, ColumnBound)] = {
    def readsOnlyInput(e: Expression): Boolean =
      e.deterministic && e.references.subsetOf(input) && !SubqueryExpression.hasSubquery(e)
    def holdsColumn(e: Expression): Boolean = e.references.contains(column)
    // An integral sum is inverted by moving the other term to the bound. ANSI mode fails and
    // TRY mode returns NULL on overflow, so a matching row always has the exact sum. The legacy
    // mode wraps around, so the inversion holds only if no overflow can happen.
    def exactSum(context: NumericEvalContext, x: Expression, y: Expression): Boolean =
      context.evalMode != EvalMode.LEGACY || (for (a <- maxMagnitude(x); b <- maxMagnitude(y))
        yield a + b <= BigInt(Long.MaxValue)).getOrElse(false)
    // The rewritten bound fails on overflow; the evaluator then leaves the end open.
    side match {
      case _ if !readsOnlyInput(value) => None
      case Add(x, y, context) if side.dataType == LongType && exactSum(context, x, y) =>
        if (holdsColumn(x) && readsOnlyInput(y)) {
          solve(x, Subtract(value, y, EvalMode.ANSI), isLower, input, column)
        } else if (holdsColumn(y) && readsOnlyInput(x)) {
          solve(y, Subtract(value, x, EvalMode.ANSI), isLower, input, column)
        } else {
          None
        }
      case Subtract(x, y, context) if side.dataType == LongType && exactSum(context, x, y) =>
        if (holdsColumn(x) && readsOnlyInput(y)) {
          solve(x, Add(value, y, EvalMode.ANSI), isLower, input, column)
        } else if (holdsColumn(y) && readsOnlyInput(x)) {
          // x - y >= v means y <= x - v.
          solve(y, Subtract(x, value, EvalMode.ANSI), !isLower, input, column)
        } else {
          None
        }
      case _ =>
        toColumnBound(side, isLower, column).map(f => (isLower, ColumnBound(value, f)))
    }
  }

  /**
   * The largest absolute value of an integral expression, if it is known: a timestamp cast to
   * BIGINT holds whole seconds of a 64-bit microsecond count.
   */
  private def maxMagnitude(e: Expression): Option[BigInt] = e match {
    case Cast(child, LongType, _, _) if child.dataType == TimestampType =>
      Some(BigInt(Long.MaxValue / MICROS_PER_SECOND + 1))
    case Literal(v: Long, LongType) => Some(BigInt(v).abs)
    case Literal(v: Int, IntegerType) => Some(BigInt(v).abs)
    case Cast(child, LongType, _, _) if child.dataType == IntegerType => maxMagnitude(child)
    case Add(x, y, _) => for (a <- maxMagnitude(x); b <- maxMagnitude(y)) yield a + b
    case Subtract(x, y, _) => for (a <- maxMagnitude(x); b <- maxMagnitude(y)) yield a + b
    case _ => None
  }

  /**
   * How a bound on `side` maps to a bound on the column itself, if `side` is the column, cast
   * to TIMESTAMP or to BIGINT, or shifted by an interval with no days or months.
   */
  private def toColumnBound(
      side: Expression,
      isLower: Boolean,
      column: Attribute): Option[Long => Option[Long]] = {
    def shifted(start: Expression, shift: Option[Long]): Option[Long => Option[Long]] =
      for (micros <- shift; inner <- toColumnBound(start, isLower, column))
        yield (bound: Long) => subtract(bound, micros).flatMap(inner)
    side match {
      case a: Attribute if a.semanticEquals(column) => Some(Some(_))
      case Cast(child, TimestampType, _, _) if child.dataType == TimestampType =>
        toColumnBound(child, isLower, column)
      // A cast to BIGINT floors to seconds: floor(t) >= s means t >= s seconds, and
      // floor(t) <= s means t is below s + 1 seconds.
      case Cast(child, LongType, _, _) if child.dataType == TimestampType =>
        toColumnBound(child, isLower, column).map { inner => (seconds: Long) =>
          multiply(seconds, MICROS_PER_SECOND)
            .flatMap(micros => if (isLower) Some(micros) else add(micros, MICROS_PER_SECOND - 1))
            .flatMap(inner)
        }
      case TimestampAddInterval(start, interval, _) => shifted(start, exactShift(interval))
      case DatetimeSub(_, _, replacement) => toColumnBound(replacement, isLower, column)
      case TimestampAdd(unit, quantity, start, _) =>
        val micros = exactUnitMicros(unit).flatMap { unitMicros =>
          foldableValue(quantity).collect { case n: Number => n.longValue() }
            .flatMap(multiply(_, unitMicros))
        }
        shifted(start, micros)
      case _ => None
    }
  }

  /**
   * The microseconds of a constant interval without days or months. Spark adds whole days and
   * months in the session time zone, where a day can be 23 or 25 hours long, so they have no
   * exact inverse.
   */
  private def exactShift(interval: Expression): Option[Long] =
    (interval.dataType, foldableValue(interval)) match {
      case (_: DayTimeIntervalType, Some(micros: Long))
          if -MICROS_PER_DAY < micros && micros < MICROS_PER_DAY => Some(micros)
      case (CalendarIntervalType, Some(i: CalendarInterval)) if i.months == 0 && i.days == 0 =>
        Some(i.microseconds)
      case _ => None
    }

  /** Microseconds per timestampadd unit that adds an exact duration. */
  private def exactUnitMicros(unit: String): Option[Long] = unit.toUpperCase(Locale.ROOT) match {
    case "MICROSECOND" => Some(1L)
    case "MILLISECOND" => Some(MICROS_PER_MILLIS)
    case "SECOND" => Some(MICROS_PER_SECOND)
    case "MINUTE" => Some(MICROS_PER_MINUTE)
    case "HOUR" => Some(MICROS_PER_HOUR)
    case _ => None
  }

  private def foldableValue(e: Expression): Option[Any] =
    if (e.foldable) {
      try Option(e.eval()) catch { case NonFatal(_) => None }
    } else {
      None
    }

  private def add(a: Long, b: Long): Option[Long] =
    try Some(Math.addExact(a, b)) catch { case _: ArithmeticException => None }

  private def subtract(a: Long, b: Long): Option[Long] =
    try Some(Math.subtractExact(a, b)) catch { case _: ArithmeticException => None }

  private def multiply(a: Long, b: Long): Option[Long] =
    try Some(Math.multiplyExact(a, b)) catch { case _: ArithmeticException => None }
}
