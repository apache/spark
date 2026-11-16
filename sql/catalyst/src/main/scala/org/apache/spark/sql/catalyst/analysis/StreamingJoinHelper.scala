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

import java.util.Locale

import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Expand, Join, LogicalPlan, Project, UnaryNode, Union}
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark._
import org.apache.spark.sql.catalyst.util.DateTimeConstants.{MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_MILLIS, MICROS_PER_MINUTE, MICROS_PER_SECOND}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * How far below a column that can filter its stream-stream join input's late rows one of the
 * input's watermarked columns can be, for an equality key or a range condition on it: at most
 * `micros` where the plan relates the two columns; by an amount that the plan does not bound when
 * `unbounded`; and by an amount that it does not show when `unrelated`, because it does not relate
 * the two. A window column is compared by its end, as eviction and late filtering compare it.
 */
case class EventTimeLag(micros: Long, unrelated: Boolean, unbounded: Boolean) {
  def max(other: EventTimeLag): EventTimeLag = EventTimeLag(math.max(micros, other.micros),
    unrelated || other.unrelated, unbounded || other.unbounded)

  /**
   * The cutoff for the other input's equality key when this input's watermark is `watermark`.
   * A key that the plan does not relate keeps the Spark 4.3 cutoff.
   */
  def keyWatermark(watermark: Long): Option[Long] =
    if (unbounded) None else Some(watermark - EventTimeLag.ceilMillis(micros))

  /**
   * The watermark to substitute for the column in a range condition. Spark 4.3 substituted the
   * watermark for every watermarked column; this does so only where the plan bounds the lag.
   */
  def rangeWatermark(watermark: Long): Option[Long] =
    if (unbounded || unrelated) None else Some(watermark - EventTimeLag.ceilMillis(micros))
}

object EventTimeLag {
  val zero: EventTimeLag = EventTimeLag(0L, unrelated = false, unbounded = false)
  val unrelated: EventTimeLag = EventTimeLag(0L, unrelated = true, unbounded = false)
  val unbounded: EventTimeLag = EventTimeLag(0L, unrelated = false, unbounded = true)

  private def ceilMillis(micros: Long): Long =
    micros / MICROS_PER_MILLIS + (if (micros % MICROS_PER_MILLIS == 0) 0 else 1)
}

/**
 * The [[EventTimeLag]] of the watermarked columns of stream-stream join inputs, by expression ID;
 * see [[StreamingJoinHelper.findEventTimeLags]]. A column missing from `lags` has lag zero, so
 * `EventTimeLags(Map.empty)` gives every watermarked column lag zero. That is right when multiple
 * stateful operators are allowed: execution then fails at start for a planned input with two
 * distinct watermarked columns, so the only one left filters late rows itself.
 */
case class EventTimeLags(lags: Map[ExprId, EventTimeLag]) {
  /**
   * The lag of `column` in a join input with output `inputAttributes`, if it carries a watermark.
   * The input's first watermarked column filters its late rows itself.
   */
  def lagOf(column: NamedExpression, inputAttributes: Seq[Attribute]): Option[EventTimeLag] =
    if (!column.metadata.contains(delayKey)) {
      None
    } else if (inputAttributes.find(_.metadata.contains(delayKey))
        .exists(_.exprId == column.exprId)) {
      Some(EventTimeLag.zero)
    } else {
      Some(lags.getOrElse(column.exprId, EventTimeLag.zero))
    }
}

object EventTimeLags {
  val none: EventTimeLags = EventTimeLags(Map.empty)
}

/**
 * Helper object for stream joins. See [[StreamingSymmetricHashJoinExec]] in SQL for more details.
 */
object StreamingJoinHelper extends PredicateHelper with Logging {

  /**
   * Find the equality key filtered by this input's watermark. Its ordinal can be used to
   * evict the opposite input's state, whose equality keys have the same ordering.
   * For leftTime = rightTime, a watermark on rightTime bounds leftTime even when leftTime
   * has no watermark. It does not bound right-side state without a watermark on leftTime.
   * A key computed from the event time by adding constant intervals, such as
   * leftTime = rightTime + INTERVAL 10 SECONDS or leftTime = timestampadd(HOUR, 1, rightTime),
   * is bounded too if the solver can count the intervals exactly or they never move a time
   * backward; see [[getStateKeyWatermark]]. The result does not depend on the session time zone,
   * so neither does the V4 state layout. Without `lags`, only the input's first watermarked
   * column counts as its event time; with them, every watermarked column that they bound does.
   */
  def getStateKeyWatermarkOrdinal(
      keys: Seq[Expression],
      inputAttributes: Seq[Attribute],
      lags: Option[EventTimeLags] = None): Option[Int] = {
    getEventTimeKeyOrdinal(keys, inputAttributes, lags).orElse {
      keys.indices.find(getStateKeyWatermark(keys, inputAttributes, _, Some(0L), lags).isDefined)
    }
  }

  /**
   * Find the equality key that is this input's first watermarked column, or else, with `lags`,
   * the watermarked key with the smallest lag that bounds it; see [[findEventTimeLags]].
   */
  def getEventTimeKeyOrdinal(
      keys: Seq[Expression],
      inputAttributes: Seq[Attribute],
      lags: Option[EventTimeLags] = None): Option[Int] = {
    // When multiple stateful operators are disabled, late filtering uses the first event-time
    // column. Otherwise, execution rejects multiple distinct event-time columns.
    inputAttributes.find(_.metadata.contains(delayKey)).flatMap { eventTime =>
      Some(keys.indexWhere(_.semanticEquals(eventTime))).filter(_ >= 0).orElse {
        lags.flatMap { l =>
          keys.indices.flatMap { i =>
            keyLag(keys(i), inputAttributes, l).filterNot(_.unbounded).map(i -> _.micros)
          }.sortBy(_._2).headOption.map(_._1)
        }
      }
    }
  }

  /** The lag of a key that is a watermarked column of the input. */
  private def keyLag(
      key: Expression,
      inputAttributes: Seq[Attribute],
      lags: EventTimeLags): Option[EventTimeLag] = key match {
    case n: NamedExpression => lags.lagOf(n, inputAttributes)
    case _ => None
  }

  /**
   * The lag of every watermarked column of each stream-stream join input in `plan`, combined
   * over the inputs by [[EventTimeLag.max]]. This is for
   * spark.sql.streaming.statefulOperator.allowMultiple=false, where an input can keep several
   * watermarked columns, such as its event time and a copy of it or a time window over it, and
   * filters late rows by the first of them. When `planned`, `plan` is the optimized plan that runs,
   * so each input filters late rows by its first watermarked column, and a column's lag is taken
   * against that one. Otherwise the optimizer can prune any watermarked column before a column, so
   * its lag is the largest against all of them. Spark 4.3 bounded state by every watermarked
   * column at the watermark. A lag lowers that cutoff; see [[EventTimeLag]].
   */
  def findEventTimeLags(plan: LogicalPlan, planned: Boolean): EventTimeLags = {
    val lags = plan.collect {
      case j: Join if j.left.isStreaming && j.right.isStreaming => Seq(j.left, j.right)
    }.flatten.flatMap { input =>
      val watermarked = input.output.filter(_.metadata.contains(delayKey)).map(_.exprId).distinct
      watermarked.indices.map { i =>
        val lateFiltered = if (planned) watermarked.take(1) else watermarked.take(i)
        watermarked(i) -> lateFiltered.filter(_ != watermarked(i)).map { eventTime =>
          lagBelow(input, watermarked(i), eventTime, Some(0L), Some(0L))
        }.foldLeft(EventTimeLag.zero)(_ max _)
      }
    }
    EventTimeLags(lags.groupMapReduce(_._1)(_._2)(_ max _))
  }

  /** How a column relates to the column of a node's child that [[lagBelow]] follows for it. */
  private case class Step(column: ExprId, offset: Option[Long])

  /**
   * How far `key` of `plan` can be below its column `eventTime`, which late filtering compares
   * with the watermark. Both are compared by their bounds: a time, or a window's end. `keyLow` is
   * the smallest that the join input's key is above `key`, and `eventTimeHigh` the largest that the
   * join input's late-filtered column is above `eventTime`; None means without bound. The walk
   * follows both columns down by expression ID: through a projection or an expansion that passes
   * them on, builds a time or session window from a single column, or, for the key, shifts a
   * timestamp by constant intervals; through any other node with one input that outputs both, as
   * such a node keeps an expression ID only for an unchanged value; into the input of a join that
   * outputs both; and into every input of a union, at the same positions. A union input that does
   * not watermark a column that the union, which takes its first input's metadata, watermarks
   * builds it some other way. Where both reach the same column, the lag is the difference of the
   * two offsets. Everything else leaves the columns unrelated.
   */
  private def lagBelow(
      plan: LogicalPlan,
      key: ExprId,
      eventTime: ExprId,
      keyLow: Option[Long],
      eventTimeHigh: Option[Long]): EventTimeLag = {
    def outputsBoth(p: LogicalPlan): Boolean =
      p.output.exists(_.exprId == key) && p.output.exists(_.exprId == eventTime)
    def follow(child: LogicalPlan, keyStep: Option[Step], eventTimeStep: Option[Step]) =
      (keyStep, eventTimeStep) match {
        case (Some(k), Some(t)) => lagBelow(child, k.column, t.column,
          addOffsets(keyLow, k.offset), addOffsets(eventTimeHigh, t.offset))
        case _ => EventTimeLag.unrelated
      }
    if (key == eventTime) {
      (keyLow, eventTimeHigh) match {
        case (Some(low), Some(high)) => Try(Math.subtractExact(high, low)).toOption match {
          case Some(lag) => EventTimeLag(math.max(0L, lag), unrelated = false, unbounded = false)
          case None => EventTimeLag.unbounded
        }
        case _ => EventTimeLag.unbounded
      }
    } else {
      plan match {
        case p: Project =>
          def entry(id: ExprId): Option[NamedExpression] = p.projectList.find(_.exprId == id)
          follow(p.child, entry(key).flatMap(keyStep), entry(eventTime).flatMap(eventTimeStep))
        case e: Expand =>
          // The rewrite of a sliding window puts one window per projection beside the child's
          // columns, which every projection passes on.
          def step(id: ExprId, window: (Seq[Expression], Attribute) => Option[Long]) = {
            val entries = e.output.indexWhere(_.exprId == id) match {
              case -1 => Nil
              case index => e.projections.map(_(index))
            }
            entries.map {
              case a: Attribute => Some(a.exprId)
              case _ => None
            }.distinct match {
              case Seq(Some(passedOn)) => Some(Step(passedOn, Some(0L)))
              case _ if e.output.exists(a => a.exprId == id &&
                  a.metadata.contains(TimeWindow.marker)) =>
                entries.map(timeWindowSource).distinct match {
                  case Seq(Some(source)) => Some(Step(source.exprId, window(entries, source)))
                  case _ => None
                }
              case _ => None
            }
          }
          // A window ends after its time column, and at most its length after it.
          follow(e.child, step(key, (_, _) => Some(0L)), step(eventTime, (entries, source) =>
            entries.map(timeWindowLength(_, source)).reduceOption((a, b) =>
              for (x <- a; y <- b) yield math.max(x, y)).flatten))
        case u: Union =>
          val keyIndex = u.output.indexWhere(_.exprId == key)
          val eventTimeIndex = u.output.indexWhere(_.exprId == eventTime)
          def unwatermarked(child: LogicalPlan, index: Int): Boolean =
            u.output(index).metadata.contains(delayKey) &&
              !child.output(index).metadata.contains(delayKey)
          if (keyIndex < 0 || eventTimeIndex < 0) {
            EventTimeLag.unrelated
          } else {
            u.children.map { child =>
              if (unwatermarked(child, keyIndex) || unwatermarked(child, eventTimeIndex)) {
                EventTimeLag.unbounded
              } else {
                lagBelow(child, child.output(keyIndex).exprId,
                  child.output(eventTimeIndex).exprId, keyLow, eventTimeHigh)
              }
            }.reduce(_ max _)
          }
        case j: Join =>
          j.children.find(outputsBoth).map(lagBelow(_, key, eventTime, keyLow, eventTimeHigh))
            .getOrElse(EventTimeLag.unrelated)
        case u: UnaryNode if outputsBoth(u.child) =>
          lagBelow(u.child, key, eventTime, keyLow, eventTimeHigh)
        case _ => EventTimeLag.unrelated
      }
    }
  }

  // An offset too large to count in microseconds has no bound.
  private def addOffsets(a: Option[Long], b: Option[Long]): Option[Long] =
    for (x <- a; y <- b; sum <- Try(Math.addExact(x, y)).toOption) yield sum

  /** The only attribute that `e` references, if there is one. */
  private def singleReference(e: Expression): Option[Attribute] = e.references.toSeq match {
    case Seq(a) => Some(a)
    case _ => None
  }

  /**
   * The step of [[lagBelow]] for the key's projection entry `entry`: the column it follows and the
   * smallest that the entry is above it, None without bound.
   */
  private def keyStep(entry: NamedExpression): Option[Step] = entry match {
    case a: Attribute => Some(Step(a.exprId, Some(0L)))
    case Alias(a: Attribute, _) => Some(Step(a.exprId, Some(0L)))
    // A window ends after its time column.
    case Alias(child, _) if entry.metadata.contains(TimeWindow.marker) =>
      timeWindowSource(child).map(source => Step(source.exprId, Some(0L)))
    case Alias(child, _) if entry.metadata.contains(SessionWindow.marker) =>
      sessionWindowSource(child).map(source => Step(source.exprId, Some(0L)))
    case Alias(child, _) if child.dataType == TimestampType =>
      singleReference(child).filter(_.dataType == TimestampType).flatMap { source =>
        // A shift whose whole days, months or years can move a time backward has no bound
        // here, as for equality keys.
        intervalShift(child, source).map { shift =>
          Step(source.exprId, Some(shift.min).filter(_ => shift.exact || shift.forward))
        }
      }
    case _ => None
  }

  /**
   * The step of [[lagBelow]] for the late-filtered column's projection entry `entry`: the column
   * it follows and the largest that the entry is above it, None without bound.
   */
  private def eventTimeStep(entry: NamedExpression): Option[Step] = entry match {
    case a: Attribute => Some(Step(a.exprId, Some(0L)))
    case Alias(a: Attribute, _) => Some(Step(a.exprId, Some(0L)))
    case Alias(child, _) if entry.metadata.contains(TimeWindow.marker) =>
      timeWindowSource(child).map(source => Step(source.exprId, timeWindowLength(child, source)))
    // A session ends its gap after the column, or without bound for a gap that is not constant.
    case Alias(child, _) if entry.metadata.contains(SessionWindow.marker) =>
      sessionWindowSource(child).map { source =>
        val end = child match {
          case CreateNamedStruct(Seq(_, _, _, end)) => Some(withoutConversions(end))
          case _ => None
        }
        Step(source.exprId, end.flatMap(intervalShift(_, source)).map(_.max))
      }
    case _ => None
  }

  /**
   * The length of the time window that `window` builds from `source`, evaluated with `source` at
   * the epoch. Spark counts window durations in fixed microseconds, so it is the same for every
   * time.
   */
  private def timeWindowLength(window: Expression, source: Attribute): Option[Long] = {
    try {
      window.transform {
        case a: Attribute if a.exprId == source.exprId => Literal(0L, source.dataType)
      }.eval() match {
        case row: InternalRow if !row.isNullAt(0) && !row.isNullAt(1) =>
          Some(row.getLong(1) - row.getLong(0))
        case _ => None
      }
    } catch {
      case NonFatal(_) => None
    }
  }

  /**
   * The time column of a time window, when the window is built from that column itself: the
   * rewrite of window() reads its time column only through a conversion to microseconds.
   */
  private def timeWindowSource(window: Expression): Option[Attribute] =
    window.collect { case PreciseTimestampConversion(time, _, LongType) => time }.distinct match {
      case Seq(a: Attribute) if window.references.toSeq == Seq(a) => Some(a)
      case _ => None
    }

  /** The time column of a session window, which is its start. */
  private def sessionWindowSource(window: Expression): Option[Attribute] = window match {
    case CreateNamedStruct(Seq(_, start, _, _)) => withoutConversions(start) match {
      case a: Attribute => Some(a)
      case _ => None
    }
    case _ => None
  }

  /** The expression without the conversions that the session window rewrite adds around it. */
  private def withoutConversions(e: Expression): Expression = e match {
    case PreciseTimestampConversion(child, _, _) => withoutConversions(child)
    case KnownNullable(child) => withoutConversions(child)
    case other => other
  }

  /**
   * Get the cutoff for the opposite input's equality key at `ordinal`, given this input's
   * watermark. `keys` and `inputAttributes` belong to this input, whose future rows have an event
   * time above the watermark. When the key is that event time, the equal key of any state row
   * they can still match is above the watermark too; with `lags`, so is a key on another
   * watermarked column, less its lag. When the key is the event time shifted by exact intervals,
   * solving `stateKey >= rightTime + INTERVAL 10 SECONDS` shifts the cutoff by that interval, less
   * 1 ms as for range conditions. When the shift has whole days, months or years but never moves a
   * time backward, the cutoff is the watermark itself. Returns None for any other key, including a
   * shift with whole days, months or years that can move a time backward.
   */
  def getStateKeyWatermark(
      keys: Seq[Expression],
      inputAttributes: Seq[Attribute],
      ordinal: Int,
      eventWatermark: Option[Long],
      lags: Option[EventTimeLags] = None): Option[Long] = {
    // The watermark that bounds a watermarked column of this input.
    def watermarkOf(column: NamedExpression): Option[Long] = lags match {
      case Some(l) => l.lagOf(column, inputAttributes).flatMap { lag =>
        eventWatermark.flatMap(lag.keyWatermark)
      }
      case None => eventWatermark.filter { _ =>
        inputAttributes.find(_.metadata.contains(delayKey)).exists(_.semanticEquals(column))
      }
    }
    keys(ordinal) match {
      case column: NamedExpression if watermarkOf(column).isDefined => watermarkOf(column)
      case key => shiftOfKey(key, inputAttributes, lags.isDefined).flatMap {
        case KeyShift(eventTime, true, _) =>
          val stateKey = AttributeReference("stateKey", TimestampType)()
          getStateValueWatermark(AttributeSet(Seq(stateKey)), AttributeSet(Seq(eventTime)),
            Some(GreaterThanOrEqual(stateKey, withIntervalLiterals(key))),
            watermarkOf(eventTime))
        // The solver cannot count this shift, but it keeps the key of every future row at or
        // above that row's event time, which is above the watermark.
        case KeyShift(eventTime, false, true) => watermarkOf(eventTime)
        case _ => None
      }
    }
  }

  /**
   * Get the cutoff for this input's range-bounded columns `stateAttributes` from the opposite
   * input's watermark, as [[getStateValueWatermark]] does with the opposite input's
   * `otherInputAttributes`. Without `lags`, only the opposite input's first watermarked column
   * counts as its event time; with them, each of its watermarked columns does at its own
   * watermark, the watermark less its lag, where they bound it; see [[EventTimeLag]].
   */
  def getStateValueWatermarkForLags(
      stateAttributes: AttributeSet,
      otherInputAttributes: Seq[Attribute],
      joinCondition: Option[Expression],
      eventWatermark: Option[Long],
      lags: Option[EventTimeLags]): Option[Long] = {
    val watermarked = otherInputAttributes.filter(_.metadata.contains(delayKey))
    val eventTimes = lags match {
      case Some(l) => watermarked.distinctBy(_.exprId).flatMap { column =>
        l.lagOf(column, otherInputAttributes).flatMap { lag =>
          eventWatermark.flatMap(lag.rangeWatermark)
        }.map(column -> _)
      }
      case None => watermarked.headOption.toSeq.flatMap(c => eventWatermark.map(c -> _))
    }
    // A conjunct on two of these columns gets no cutoff either way, so solving for each column
    // alone and taking the lowest gives the cutoff of all of them at their own watermarks.
    eventTimes.flatMap { case (column, watermark) =>
      getStateValueWatermark(stateAttributes, AttributeSet(Seq(column)), joinCondition,
        Some(watermark))
    }.minOption
  }

  /**
   * Get a bound on this input's event time for state rows whose equality key at `ordinal` is
   * above `keyWatermark`. When the key is the event time, the bound is the key watermark. When
   * the key is the event time shifted by exact intervals, solving
   * `leftTime + INTERVAL 10 SECONDS >= keyWatermark` subtracts that constant, less 1 ms.
   * Returns None for any other key, including any shift with whole days, months or years.
   */
  def getEventTimeWatermarkFromKey(
      keys: Seq[Expression],
      inputAttributes: Seq[Attribute],
      ordinal: Int,
      keyWatermark: Long): Option[Long] = {
    if (getEventTimeKeyOrdinal(keys, inputAttributes).contains(ordinal)) {
      Some(keyWatermark)
    } else {
      shiftOfKey(keys(ordinal), inputAttributes).filter(_.exact).flatMap { shift =>
        val keyWithWatermark = AttributeReference("stateKey", TimestampType,
          metadata = new MetadataBuilder().putLong(delayKey, 0L).build())()
        getStateValueWatermark(AttributeSet(Seq(shift.eventTime)),
          AttributeSet(Seq(keyWithWatermark)),
          Some(GreaterThanOrEqual(withIntervalLiterals(keys(ordinal)), keyWithWatermark)),
          Some(keyWatermark))
      }
    }
  }

  /**
   * Whether the key is this input's event time shifted by constant intervals that never move a
   * time backward. [[getStateKeyWatermark]] bounds the opposite state by such a key at the
   * watermark plus the shift for an exact interval, and at the watermark itself otherwise, so it
   * is never below the watermark.
   */
  def isForwardShiftOfEventTime(key: Expression, inputAttributes: Seq[Attribute]): Boolean =
    shiftOfKey(key, inputAttributes).exists(_.forward)

  /**
   * A timestamp key computed from the input's event time by constant intervals. `exact` means
   * the solver counts the shift exactly; `forward` means the shift never moves a time backward.
   */
  private case class KeyShift(eventTime: Attribute, exact: Boolean, forward: Boolean)

  /**
   * How a timestamp key shifts the input's event time, if the solver or its sign bounds it. With
   * `anyWatermarkedColumn`, the shifted column can be any watermarked column of the input.
   */
  private def shiftOfKey(
      key: Expression,
      inputAttributes: Seq[Attribute],
      anyWatermarkedColumn: Boolean = false): Option[KeyShift] = {
    val eventTime = if (anyWatermarkedColumn) {
      singleReference(key).filter { column =>
        column.metadata.contains(delayKey) && inputAttributes.exists(_.exprId == column.exprId)
      }
    } else {
      inputAttributes.find(_.metadata.contains(delayKey))
    }
    eventTime.filter(_ => key.dataType == TimestampType).flatMap { eventTime =>
      intervalShift(key, eventTime).collect {
        case shift if shift.exact || shift.forward =>
          KeyShift(eventTime, shift.exact, shift.forward)
      }
    }
  }

  /**
   * A shift of a time by constant intervals. `exact` means that the solver counts it exactly, and
   * `min` and `max` bound how far it moves a time, in microseconds, in every session time zone.
   */
  private case class Shift(exact: Boolean, min: Long, max: Long) {
    /** Whether the shift never moves a time backward. */
    def forward: Boolean = min >= 0
    def plus(other: Shift): Shift = Shift(exact && other.exact,
      Math.addExact(min, other.min), Math.addExact(max, other.max))
    def negate: Shift = Shift(exact, Math.negateExact(max), Math.negateExact(min))
  }

  private object Shift {
    def micros(value: Long): Shift = Shift(exact = true, value, value)
    // Spark adds whole days in the session time zone, where a day can be 23 or 25 hours across a
    // daylight saving change, and months vary in length, but the solver counts a day as 24 hours.
    // So only shifts without whole days or months are exact. To bound a shift without reading the
    // time zone, a day counts as 12 to 36 hours, wider than any daylight saving change, and a
    // month as 28 to 31 such days.
    def days(n: Long): Shift = if (n >= 0) {
      Shift(n == 0, Math.multiplyExact(n, 12 * MICROS_PER_HOUR),
        Math.multiplyExact(n, 36 * MICROS_PER_HOUR))
    } else {
      days(Math.negateExact(n)).negate
    }
    def months(n: Long): Shift = if (n >= 0) {
      Shift(n == 0, Math.multiplyExact(n, 28 * 12 * MICROS_PER_HOUR),
        Math.multiplyExact(n, 31 * 36 * MICROS_PER_HOUR))
    } else {
      months(Math.negateExact(n)).negate
    }
  }

  /**
   * If the expression is the event time plus or minus constant intervals, the shift. Each term
   * counts on its own, so a net forward compound shift such as
   * rightTime + INTERVAL 1 DAY - INTERVAL 1 HOUR never moves a time backward. Other functions and
   * casts from other types are not accepted: a truncation such as
   * CAST(CAST(rightTime AS BIGINT) AS TIMESTAMP) can map a later event time below the watermark.
   */
  private def intervalShift(expr: Expression, eventTime: Attribute): Option[Shift] = {
    def interval(e: Expression): Option[Shift] = {
      // An interval that fails to evaluate gives no bound; the query reports the error at run time.
      def value: Option[Any] = try Option(e.eval()) catch { case NonFatal(_) => None }
      if (!e.foldable) {
        None
      } else {
        e.dataType match {
          case _: DayTimeIntervalType => value.map { v =>
            val micros = v.asInstanceOf[Long]
            Shift.days(micros / MICROS_PER_DAY).plus(Shift.micros(micros % MICROS_PER_DAY))
          }
          case _: YearMonthIntervalType => value.map(v => Shift.months(v.asInstanceOf[Int]))
          case CalendarIntervalType => value.map { v =>
            val i = v.asInstanceOf[CalendarInterval]
            Shift.months(i.months).plus(Shift.days(i.days)).plus(Shift.micros(i.microseconds))
          }
          case _ => None
        }
      }
    }
    def combine(a: Option[Shift], b: Option[Shift]): Option[Shift] =
      for (x <- a; y <- b) yield x.plus(y)
    def shift(expr: Expression): Option[Shift] = expr match {
      case a: Attribute if a.semanticEquals(eventTime) => Some(Shift.micros(0L))
      // The analyzer wraps timestamp + interval in a cast to the same timestamp type.
      case Cast(child, TimestampType, _, _) if child.dataType == TimestampType => shift(child)
      case TimestampAddInterval(start, i, _) => combine(interval(i), shift(start))
      case TimestampAddYMInterval(start, i, _) => combine(interval(i), shift(start))
      // timestampadd adds sub-day units exactly, and days, weeks, months, quarters and years in
      // the session time zone.
      case TimestampAdd(unit, quantity, start, _) if quantity.foldable =>
        val amount = try Option(quantity.eval()) catch { case NonFatal(_) => None }
        val unitShift = amount.collect { case n: Number => n.longValue() }.flatMap { n =>
          timestampAddUnitMicros(unit).map(u => Shift.micros(Math.multiplyExact(n, u))).orElse {
            unit.toUpperCase(Locale.ROOT) match {
              case "DAY" | "DAYOFYEAR" => Some(Shift.days(n))
              case "WEEK" => Some(Shift.days(Math.multiplyExact(n, 7L)))
              case "MONTH" => Some(Shift.months(n))
              case "QUARTER" => Some(Shift.months(Math.multiplyExact(n, 3L)))
              case "YEAR" => Some(Shift.months(Math.multiplyExact(n, 12L)))
              case _ => None
            }
          }
        }
        combine(unitShift, shift(start))
      case DatetimeSub(_, _, replacement) => shift(replacement)
      case Add(left, right, _) =>
        combine(interval(right), shift(left)).orElse(combine(interval(left), shift(right)))
      case Subtract(left, right, _) => combine(interval(right).map(_.negate), shift(left))
      case _ => None
    }
    // A shift too large to count in microseconds gives no bound.
    try shift(expr) catch { case _: ArithmeticException => None }
  }

  /**
   * The key with each foldable interval, such as INTERVAL 1 HOUR * 2, replaced by a literal of
   * its value, as [[intervalShift]] evaluates it, and each timestampadd of a sub-day unit replaced
   * by the same interval added to its timestamp. The solver reads only interval literals, and the
   * admission check runs on the analyzed plan, before constant folding, so without this it would
   * find no bound where execution finds one.
   */
  private def withIntervalLiterals(key: Expression): Expression = key.transformDown {
    case e if e.foldable && !e.isInstanceOf[Literal] && isIntervalType(e.dataType) =>
      try Literal.create(e.eval(), e.dataType) catch { case NonFatal(_) => e }
    // A sub-day unit adds an exact number of microseconds, which the solver reads as an interval.
    case e @ TimestampAdd(unit, quantity, start, timeZoneId) if quantity.foldable &&
        timestampAddUnitMicros(unit).isDefined =>
      try {
        val micros = Math.multiplyExact(
          quantity.eval().asInstanceOf[Number].longValue(), timestampAddUnitMicros(unit).get)
        TimestampAddInterval(start, Literal(micros, DayTimeIntervalType()), timeZoneId)
      } catch {
        case NonFatal(_) => e
      }
  }

  /** Microseconds per unit for the timestampadd units that add an exact duration. */
  private def timestampAddUnitMicros(unit: String): Option[Long] =
    unit.toUpperCase(Locale.ROOT) match {
      case "MICROSECOND" => Some(1L)
      case "MILLISECOND" => Some(MICROS_PER_MILLIS)
      case "SECOND" => Some(MICROS_PER_SECOND)
      case "MINUTE" => Some(MICROS_PER_MINUTE)
      case "HOUR" => Some(MICROS_PER_HOUR)
      case _ => None
    }

  private def isIntervalType(dataType: DataType): Boolean = dataType match {
    case _: DayTimeIntervalType | _: YearMonthIntervalType | CalendarIntervalType => true
    case _ => false
  }

  /**
   * Whether the expression is the event time, optionally cast to TIMESTAMP or a numeric type,
   * plus or minus constants: intervals without months, or numbers. The solver reads such a cast
   * as the column itself and counts such an interval as a fixed duration, a day as 24 hours, in
   * every version and every time zone, so a range condition built from it derives the same
   * cutoff before and after the stream-stream join corrections. Months have no fixed length.
   */
  private[sql] def isConstantOffsetOf(expr: Expression, eventTime: Attribute): Boolean = {
    def isConstant(e: Expression): Boolean = {
      // A constant that fails to evaluate gives no bound; the query reports the error at run time.
      def value: Option[Any] = try Option(e.eval()) catch { case NonFatal(_) => None }
      e.foldable && (e.dataType match {
        case _: DayTimeIntervalType | _: NumericType => value.isDefined
        case CalendarIntervalType => value.exists(_.asInstanceOf[CalendarInterval].months == 0)
        case _ => false
      })
    }
    expr match {
      case a: Attribute => a.semanticEquals(eventTime)
      case Cast(child, TimestampType | _: NumericType, _, _) => isConstantOffsetOf(child, eventTime)
      case TimestampAddInterval(start, interval, _) =>
        isConstant(interval) && isConstantOffsetOf(start, eventTime)
      case DatetimeSub(_, _, replacement) => isConstantOffsetOf(replacement, eventTime)
      case Add(left, right, _) =>
        isConstant(right) && isConstantOffsetOf(left, eventTime) ||
          isConstant(left) && isConstantOffsetOf(right, eventTime)
      case Subtract(left, right, _) => isConstant(right) && isConstantOffsetOf(left, eventTime)
      case _ => false
    }
  }

  /**
   * Get state value watermark (see [[StreamingSymmetricHashJoinExec]] for context about it)
   * given the join condition and the event time watermark. This is how it works.
   * - The condition is split into conjunctive predicates, and we find the predicates of the
   *   form `leftTime + c1 < rightTime + c2`   (or <=, >, >=).
   * - We canonicalize the predicate and solve it with the event time watermark value to find the
   *  value of the state watermark.
   * This function is supposed to make best-effort attempt to get the state watermark. If there is
   * any error, it will return None.
   *
   * @param attributesToFindStateWatermarkFor attributes of the side whose state watermark
   *                                         is to be calculated
   * @param attributesWithEventWatermark  attributes of the other side which has a watermark column
   * @param joinCondition                 join condition
   * @param eventWatermark                watermark defined on the input event data
   * @return state value watermark in milliseconds, is possible.
   */
  def getStateValueWatermark(
      attributesToFindStateWatermarkFor: AttributeSet,
      attributesWithEventWatermark: AttributeSet,
      joinCondition: Option[Expression],
      eventWatermark: Option[Long]): Option[Long] = {

    // If condition or event time watermark is not provided, then cannot calculate state watermark
    if (joinCondition.isEmpty || eventWatermark.isEmpty) return None

    // If there is not watermark attribute, then cannot define state watermark
    if (!attributesWithEventWatermark.exists(_.metadata.contains(delayKey))) return None

    def getStateWatermarkSafely(l: Expression, r: Expression): Option[Long] = {
      try {
        getStateWatermarkFromLessThenPredicate(
          l, r, attributesToFindStateWatermarkFor, attributesWithEventWatermark, eventWatermark)
      } catch {
        case NonFatal(e) =>
          logWarning(log"Error trying to extract state constraint from condition " +
            log"${MDC(JOIN_CONDITION, joinCondition)}", e)
          None
      }
    }

    val allStateWatermarks = splitConjunctivePredicates(joinCondition.get).flatMap { predicate =>

      // The generated the state watermark cleanup expression is inclusive of the state watermark.
      // If state watermark is W, all state where timestamp <= W will be cleaned up.
      // Now when the canonicalized join condition solves to leftTime >= W, we don't want to clean
      // up leftTime <= W. Rather we should clean up leftTime <= W - 1. Hence the -1 below.
      val stateWatermark = predicate match {
        case LessThan(l, r) => getStateWatermarkSafely(l, r)
        case LessThanOrEqual(l, r) => getStateWatermarkSafely(l, r).map(_ - 1)
        case GreaterThan(l, r) => getStateWatermarkSafely(r, l)
        case GreaterThanOrEqual(l, r) => getStateWatermarkSafely(r, l).map(_ - 1)
        case Between(input, lower, upper, _) =>
          getStateWatermarkSafely(lower, input).map(_ - 1)
            .orElse(getStateWatermarkSafely(input, upper).map(_ - 1))
        case _ => None
      }
      if (stateWatermark.nonEmpty) {
        logInfo(log"Condition ${MDC(JOIN_CONDITION, joinCondition)} generated " +
          log"watermark constraint = ${MDC(WATERMARK_CONSTRAINT, stateWatermark.get)}")
      }
      stateWatermark
    }
    allStateWatermarks.reduceOption((x, y) => Math.min(x, y))
  }

  /**
   * Extract the state value watermark (milliseconds) from the condition
   * `LessThan(leftExpr, rightExpr)` where . For example: if we want to find the constraint for
   * leftTime using the watermark on the rightTime. Example:
   *
   * Input:                 rightTime-with-watermark + c1 < leftTime + c2
   * Canonical form:        rightTime-with-watermark + c1 + (-c2) + (-leftTime) < 0
   * Solving for rightTime: rightTime-with-watermark + c1 + (-c2) < leftTime
   * With watermark value:  watermark-value + c1 + (-c2) < leftTime
   */
  private def getStateWatermarkFromLessThenPredicate(
      leftExpr: Expression,
      rightExpr: Expression,
      attributesToFindStateWatermarkFor: AttributeSet,
      attributesWithEventWatermark: AttributeSet,
      eventWatermark: Option[Long]): Option[Long] = {

    val attributesInCondition =
      leftExpr.collect { case a: AttributeReference => a } ++
      rightExpr.collect { case a: AttributeReference => a }
    if (attributesInCondition.count(attributesToFindStateWatermarkFor.contains) > 1 ||
        attributesInCondition.count(attributesWithEventWatermark.contains) > 1) {
      // Multiple attribute occurrences on either side require coefficient handling.
      return None
    }

    def containsAttributeToFindStateConstraintFor(e: Expression): Boolean = {
      e.collectLeaves().collectFirst {
        case a @ AttributeReference(_, _, _, _)
          if attributesToFindStateWatermarkFor.contains(a) => a
      }.nonEmpty
    }

    // Canonicalization step 1: convert to (rightTime-with-watermark + c1) - (leftTime + c2) < 0
    val allOnLeftExpr = Subtract(leftExpr, rightExpr)
    logDebug(s"All on Left:\n${allOnLeftExpr.treeString(true)}\n${allOnLeftExpr.asCode}")

    // Canonicalization step 2: extract commutative terms
    //    rightTime-with-watermark, c1, -leftTime, -c2
    // Equal terms still contribute separately to the arithmetic sum.
    val terms = collectTerms(allOnLeftExpr)
    logDebug("Terms extracted from join condition:\n\t" + terms.mkString("\n\t"))

    // Find the term that has leftTime (i.e. the one present in attributesToFindConstraintFor
    val constraintTerms = terms.filter(containsAttributeToFindStateConstraintFor)

    // Verify there is only one correct constraint term and of the correct type
    if (constraintTerms.size > 1) {
      logWarning(
        log"Failed to extract state constraint terms: multiple time terms in condition\n\t" +
          log"${MDC(EXPR_TERMS, terms.mkString("\n\t"))}")
      return None
    }
    if (constraintTerms.isEmpty) {
      logDebug("Failed to extract state constraint terms: no time terms in condition\n\t" +
        terms.mkString("\n\t"))
      return None
    }
    val constraintTerm = constraintTerms.head
    if (!constraintTerm.exists(_.isInstanceOf[UnaryMinus])) {
      // Incorrect condition. We want the constraint term in canonical form to be `-leftTime`
      // so that resolve for it as `-leftTime + watermark + c < 0` ==> `watermark + c < leftTime`.
      // Now, if the original conditions is `rightTime-with-watermark > leftTime` and watermark
      // condition is `rightTime-with-watermark > watermarkValue`, then no constraint about
      // `leftTime` can be inferred. In this case, after canonicalization and collection of terms,
      // the constraintTerm would be `leftTime` and not `-leftTime`. Hence, we return None.
      return None
    }

    // Replace watermark attribute with watermark value, and generate the resolved expression
    // from the other terms. That is,
    // rightTime-with-watermark, c1, -c2  =>  watermark, c1, -c2  =>  watermark + c1 + (-c2)
    logDebug(s"Constraint term from join condition:\t$constraintTerm")
    val exprWithWatermarkSubstituted = terms.filterNot(_ eq constraintTerm).map { term =>
      term.transform {
        case a @ AttributeReference(_, _, _, metadata)
          if attributesWithEventWatermark.contains(a) && metadata.contains(delayKey) =>
          Multiply(Literal(eventWatermark.get.toDouble), Literal(1000.0))
      }
    }.reduceLeft(Add(_, _))

    // Callers can pass only the constrained column and the opposite event time, so a term on any
    // other column passes the occurrence check above. Its value is unknown here.
    if (exprWithWatermarkSubstituted.references.nonEmpty) {
      logDebug(s"Failed to extract state constraint: unresolved $exprWithWatermarkSubstituted")
      return None
    }

    // Calculate the constraint value
    logInfo(log"Final expression to evaluate " +
      log"constraint:\t${MDC(WATERMARK_CONSTRAINT, exprWithWatermarkSubstituted)}")
    val constraintValue = exprWithWatermarkSubstituted.eval().asInstanceOf[java.lang.Double]
    Some((Double2double(constraintValue) / 1000.0).toLong)
  }

  /**
   * Collect all the terms present in an expression after converting it into the form
   * a + b + c + d where each term be either an attribute or a literal casted to long,
   * optionally wrapped in a unary minus.
   */
  private def collectTerms(exprToCollectFrom: Expression): Seq[Expression] = {
    var invalid = false

    /** Wrap a term with UnaryMinus if its needs to be negated. */
    def negateIfNeeded(expr: Expression, minus: Boolean): Expression = {
      if (minus) UnaryMinus(expr) else expr
    }

    /**
     * Recursively split the expression into its leaf terms contains attributes or literals.
     * Returns terms only of the forms:
     *    Cast(AttributeReference), UnaryMinus(Cast(AttributeReference)),
     *    Cast(AttributeReference, Double), UnaryMinus(Cast(AttributeReference, Double))
     *    Multiply(Literal), UnaryMinus(Multiply(Literal))
     *    Multiply(Cast(Literal)), UnaryMinus(Multiple(Cast(Literal)))
     *
     * Note:
     * - If term needs to be negated for making it a commutative term,
     *   then it will be wrapped in UnaryMinus(...)
     * - Each terms will be representing timestamp value or time interval in microseconds,
     *   typed as doubles.
     */
    def collect(expr: Expression, negate: Boolean): Seq[Expression] = {
      expr match {
        case Add(left, right, _) =>
          collect(left, negate) ++ collect(right, negate)
        case Subtract(left, right, _) =>
          collect(left, negate) ++ collect(right, !negate)
        case TimestampAddInterval(left, right, _) =>
          collect(left, negate) ++ collect(right, negate)
        case DatetimeSub(_, _, child) => collect(child, negate)
        case UnaryMinus(child, _) =>
          collect(child, !negate)
        case CheckOverflow(child, _, _) =>
          collect(child, negate)
        case Cast(child, dataType, _, _) =>
          dataType match {
            case _: NumericType | _: TimestampType => collect(child, negate)
            case _ =>
              invalid = true
              Seq.empty
          }
        case a: AttributeReference =>
          val castedRef = if (a.dataType != DoubleType) Cast(a, DoubleType) else a
          Seq(negateIfNeeded(castedRef, negate))
        case lit: Literal =>
          // If literal of type calendar interval, then explicitly convert to millis
          // Convert other number like literal to doubles representing millis (by x1000)
          val castedLit = lit.dataType match {
            case CalendarIntervalType =>
              val calendarInterval = lit.value.asInstanceOf[CalendarInterval]
              if (calendarInterval.months != 0) {
                invalid = true
                logWarning(
                  log"Failed to extract state value watermark from condition " +
                    log"${MDC(JOIN_CONDITION, exprToCollectFrom)} " +
                    log"as imprecise intervals like months and years cannot be used for" +
                    log"watermark calculation. Use interval in terms of day instead.")
                Literal(0.0)
              } else {
                Literal(calendarInterval.days * MICROS_PER_DAY.toDouble +
                  calendarInterval.microseconds.toDouble)
              }
            case _: DayTimeIntervalType =>
              // Unbox and then cast
              Literal(lit.value.asInstanceOf[Long].toDouble)
            case DoubleType =>
              Multiply(lit, Literal(1000000.0))
            case _: NumericType =>
              Multiply(Cast(lit, DoubleType), Literal(1000000.0))
            case _: TimestampType =>
              Multiply(PreciseTimestampConversion(lit, TimestampType, LongType), Literal(1000000.0))
          }
          Seq(negateIfNeeded(castedLit, negate))
        case a @ _ =>
          logWarning(
            log"Failed to extract state value watermark from condition " +
              log"${MDC(JOIN_CONDITION, exprToCollectFrom)} due to " +
              log"${MDC(JOIN_CONDITION_SUB_EXPR, a)}")
          invalid = true
          Seq.empty
      }
    }

    val terms = collect(exprToCollectFrom, negate = false)
    if (!invalid) terms else Seq.empty
  }
}
