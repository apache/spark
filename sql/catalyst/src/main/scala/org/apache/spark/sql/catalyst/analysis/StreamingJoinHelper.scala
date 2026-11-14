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

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark._
import org.apache.spark.sql.catalyst.util.DateTimeConstants.{MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_MILLIS, MICROS_PER_MINUTE, MICROS_PER_SECOND}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

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
   * so neither does the V4 state layout.
   */
  def getStateKeyWatermarkOrdinal(
      keys: Seq[Expression],
      inputAttributes: Seq[Attribute]): Option[Int] = {
    getEventTimeKeyOrdinal(keys, inputAttributes).orElse {
      keys.indices.find(getStateKeyWatermark(keys, inputAttributes, _, Some(0L)).isDefined)
    }
  }

  /** Find the equality key that is this input's event-time column itself. */
  def getEventTimeKeyOrdinal(
      keys: Seq[Expression],
      inputAttributes: Seq[Attribute]): Option[Int] = {
    // When multiple stateful operators are disabled, late filtering uses the first event-time
    // column. Otherwise, execution rejects multiple distinct event-time columns.
    inputAttributes.find(_.metadata.contains(delayKey)).flatMap { eventTime =>
      keys.indexWhere(_.semanticEquals(eventTime)) match {
        case ordinal if ordinal >= 0 => Some(ordinal)
        case _ => None
      }
    }
  }

  /**
   * Get the cutoff for the opposite input's equality key at `ordinal`, given this input's
   * watermark. `keys` and `inputAttributes` belong to this input, whose future rows have an event
   * time above the watermark. When the key is that event time, the equal key of any state row
   * they can still match is above the watermark too. When the key is the event time shifted by
   * exact intervals, solving `stateKey >= rightTime + INTERVAL 10 SECONDS` shifts the cutoff by
   * that interval, less 1 ms as for range conditions. When the shift has whole days, months or
   * years but never moves a time backward, the cutoff is the watermark itself. Returns None for
   * any other key, including a shift with whole days, months or years that can move a time
   * backward.
   */
  def getStateKeyWatermark(
      keys: Seq[Expression],
      inputAttributes: Seq[Attribute],
      ordinal: Int,
      eventWatermark: Option[Long]): Option[Long] = {
    if (getEventTimeKeyOrdinal(keys, inputAttributes).contains(ordinal)) {
      eventWatermark
    } else {
      shiftOfKey(keys(ordinal), inputAttributes).flatMap {
        case KeyShift(eventTime, true, _) =>
          val stateKey = AttributeReference("stateKey", TimestampType)()
          getStateValueWatermark(AttributeSet(Seq(stateKey)), AttributeSet(Seq(eventTime)),
            Some(GreaterThanOrEqual(stateKey, withIntervalLiterals(keys(ordinal)))),
            eventWatermark)
        // The solver cannot count this shift, but it keeps the key of every future row at or
        // above that row's event time, which is above the watermark.
        case KeyShift(_, false, true) => eventWatermark
        case _ => None
      }
    }
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

  /** How a timestamp key shifts the input's event time, if the solver or its sign bounds it. */
  private def shiftOfKey(key: Expression, inputAttributes: Seq[Attribute]): Option[KeyShift] =
    inputAttributes.find(_.metadata.contains(delayKey)).filter(_ => key.dataType == TimestampType)
      .flatMap { eventTime =>
        intervalShift(key, eventTime).collect {
          case (exact, forward) if exact || forward => KeyShift(eventTime, exact, forward)
        }
      }

  /**
   * If the expression is the event time plus or minus constant intervals, whether the shift is
   * exact and whether it never moves a time backward. Other functions and casts from other types
   * are not accepted: a truncation such as CAST(CAST(rightTime AS BIGINT) AS TIMESTAMP) can map a
   * later event time below the watermark.
   */
  private def intervalShift(expr: Expression, eventTime: Attribute): Option[(Boolean, Boolean)] = {
    // Spark adds whole days in the session time zone, where a day can be 23 or 25 hours across a
    // daylight saving change, and months vary in length, but the solver counts a day as 24 hours.
    // So only intervals without whole days or months are exact, in every time zone.
    def interval(e: Expression, negate: Boolean): Option[(Boolean, Boolean)] = {
      def forward(parts: Long*): Boolean =
        if (negate) parts.forall(_ <= 0) else parts.forall(_ >= 0)
      // An interval that fails to evaluate gives no bound; the query reports the error at run time.
      def value: Option[Any] = try Option(e.eval()) catch { case NonFatal(_) => None }
      if (!e.foldable) {
        None
      } else {
        e.dataType match {
          case _: DayTimeIntervalType =>
            value.map { v =>
              val micros = v.asInstanceOf[Long]
              (-MICROS_PER_DAY < micros && micros < MICROS_PER_DAY, forward(micros))
            }
          case _: YearMonthIntervalType =>
            value.map { v =>
              val months = v.asInstanceOf[Int]
              (months == 0, forward(months))
            }
          case CalendarIntervalType =>
            value.map { v =>
              val i = v.asInstanceOf[CalendarInterval]
              (i.months == 0 && i.days == 0, forward(i.months, i.days, i.microseconds))
            }
          case _ => None
        }
      }
    }
    def combine(
        a: Option[(Boolean, Boolean)],
        b: Option[(Boolean, Boolean)]): Option[(Boolean, Boolean)] =
      for ((exactA, forwardA) <- a; (exactB, forwardB) <- b)
        yield (exactA && exactB, forwardA && forwardB)
    expr match {
      case a: Attribute if a.semanticEquals(eventTime) => Some((true, true))
      // The analyzer wraps timestamp + interval in a cast to the same timestamp type.
      case Cast(child, TimestampType, _, _) if child.dataType == TimestampType =>
        intervalShift(child, eventTime)
      case TimestampAddInterval(start, i, _) =>
        combine(interval(i, negate = false), intervalShift(start, eventTime))
      case TimestampAddYMInterval(start, i, _) =>
        combine(interval(i, negate = false), intervalShift(start, eventTime))
      // timestampadd adds sub-day units exactly, and days, weeks, months, quarters and years in
      // the session time zone.
      case TimestampAdd(unit, quantity, start, _) if quantity.foldable =>
        val exactUnit = timestampAddUnitMicros(unit).isDefined
        val known = exactUnit || calendarTimestampAddUnits.contains(unit.toUpperCase(Locale.ROOT))
        val amount = try Option(quantity.eval()) catch { case NonFatal(_) => None }
        val shift = amount.filter(_ => known).collect {
          case n: Number => (exactUnit, n.longValue() >= 0)
        }
        combine(shift, intervalShift(start, eventTime))
      case DatetimeSub(_, _, replacement) => intervalShift(replacement, eventTime)
      case Add(left, right, _) =>
        combine(interval(right, negate = false), intervalShift(left, eventTime)).orElse(
          combine(interval(left, negate = false), intervalShift(right, eventTime)))
      case Subtract(left, right, _) =>
        combine(interval(right, negate = true), intervalShift(left, eventTime))
      case _ => None
    }
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

  /** The timestampadd units that Spark adds as local days or months. */
  private val calendarTimestampAddUnits =
    Set("DAY", "DAYOFYEAR", "WEEK", "MONTH", "QUARTER", "YEAR")

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
   * @param exactForUnwatermarkedColumns  whether a column without its own watermark gets no
   *                                      cutoff from a condition that the solver counts
   *                                      inexactly. State eviction and the admission check set
   *                                      it, so they agree.
   * @param baseAdmissionRules            whether to skip the check that the bounded column is a
   *                                      TIMESTAMP column or a time window, which Spark 4.4
   *                                      added to the solver. It does not skip the inexact
   *                                      arithmetic check for columns without their own
   *                                      watermark, which exactForUnwatermarkedColumns alone
   *                                      controls; leave that false too for the result of
   *                                      earlier versions. Only the admission rule that stays
   *                                      in force with the finalization check disabled sets it.
   * @return state value watermark in milliseconds, is possible.
   */
  def getStateValueWatermark(
      attributesToFindStateWatermarkFor: AttributeSet,
      attributesWithEventWatermark: AttributeSet,
      joinCondition: Option[Expression],
      eventWatermark: Option[Long],
      exactForUnwatermarkedColumns: Boolean = false,
      baseAdmissionRules: Boolean = false): Option[Long] = {

    // If condition or event time watermark is not provided, then cannot calculate state watermark
    if (joinCondition.isEmpty || eventWatermark.isEmpty) return None

    // If there is not watermark attribute, then cannot define state watermark
    if (!attributesWithEventWatermark.exists(_.metadata.contains(delayKey))) return None

    def getStateWatermarkSafely(
        l: Expression,
        r: Expression,
        keepConstantOnly: Boolean): Option[Long] = {
      try {
        getStateWatermarkFromLessThenPredicate(l, r, attributesToFindStateWatermarkFor,
          attributesWithEventWatermark, eventWatermark, exactForUnwatermarkedColumns,
          keepConstantOnly, baseAdmissionRules)
      } catch {
        case NonFatal(e) =>
          logWarning(log"Error trying to extract state constraint from condition " +
            log"${MDC(JOIN_CONDITION, joinCondition)}", e)
          None
      }
    }

    def stateWatermarkOf(predicate: Expression, keepConstantOnly: Boolean): Option[Long] = {
      def solve(l: Expression, r: Expression): Option[Long] =
        getStateWatermarkSafely(l, r, keepConstantOnly)

      // The generated the state watermark cleanup expression is inclusive of the state watermark.
      // If state watermark is W, all state where timestamp <= W will be cleaned up.
      // Now when the canonicalized join condition solves to leftTime >= W, we don't want to clean
      // up leftTime <= W. Rather we should clean up leftTime <= W - 1. Hence the -1 below.
      val stateWatermark = predicate match {
        case LessThan(l, r) => solve(l, r)
        case LessThanOrEqual(l, r) => solve(l, r).map(_ - 1)
        case GreaterThan(l, r) => solve(r, l)
        case GreaterThanOrEqual(l, r) => solve(r, l).map(_ - 1)
        case Between(input, lower, upper, _) =>
          solve(lower, input).map(_ - 1).orElse(solve(input, upper).map(_ - 1))
        case _ => None
      }
      if (stateWatermark.nonEmpty) {
        logInfo(log"Condition ${MDC(JOIN_CONDITION, joinCondition)} generated " +
          log"watermark constraint = ${MDC(WATERMARK_CONSTRAINT, stateWatermark.get)}")
      }
      stateWatermark
    }

    val conjuncts = splitConjunctivePredicates(joinCondition.get)
    val boundingConjuncts = conjuncts.flatMap { predicate =>
      stateWatermarkOf(predicate, keepConstantOnly = false).map(predicate -> _)
    }
    // A constant-only conjunct is left out when the other bounds follow the watermark exactly.
    // The solver can over-estimate an inexact bound, such as a truncating cast or whole days,
    // above a row that can still match. A constant's cutoff keeps that row, as in earlier
    // versions, so keep the constants in the minimum then.
    val constantBounds =
      if (boundingConjuncts.exists { case (predicate, _) => hasInexactArithmetic(predicate) }) {
        conjuncts.filterNot(c => boundingConjuncts.exists(_._1 eq c))
          .flatMap(stateWatermarkOf(_, keepConstantOnly = true))
      } else {
        Nil
      }
    (boundingConjuncts.map(_._2) ++ constantBounds).reduceOption((x, y) => Math.min(x, y))
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
      eventWatermark: Option[Long],
      exactForUnwatermarkedColumns: Boolean,
      keepConstantOnly: Boolean,
      baseAdmissionRules: Boolean): Option[Long] = {

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

    // The terms count a whole day as 24 hours, although Spark adds days in the session time
    // zone, where a day can be 23 or 25 hours, and read CAST(ts AS BIGINT) as ts, although the
    // cast drops the fraction of a second. Either can put the cutoff above a row that can still
    // match. A column with its own watermark keeps such a cutoff, as existing queries rely on
    // it. With exactForUnwatermarkedColumns, any other column, such as the bounded column of an
    // input without a watermark, gets no cutoff from such a condition.
    val stateColumnHasWatermark = constraintTerm.exists {
      case a: AttributeReference =>
        attributesToFindStateWatermarkFor.contains(a) && a.metadata.contains(delayKey)
      case _ => false
    }
    if (exactForUnwatermarkedColumns && !stateColumnHasWatermark &&
        hasInexactArithmetic(allOnLeftExpr)) {
      logDebug(s"Inexact arithmetic gives an unwatermarked column no cutoff: $allOnLeftExpr")
      return None
    }

    // State eviction compares this cutoff in timestamp units against the constrained column, or
    // against its end for a time window; see [[isWindowEvictionSafe]].
    val stateColumn = constraintTerm.collectFirst {
      case a: AttributeReference if attributesToFindStateWatermarkFor.contains(a) => a
    }
    val watermarkColumn = terms.flatMap(_.collect {
      case a: AttributeReference
          if attributesWithEventWatermark.contains(a) && a.metadata.contains(delayKey) => a
    }).headOption
    val canEvictStateColumn = stateColumn.exists { column =>
      column.dataType == TimestampType || watermarkColumn.exists(isWindowEvictionSafe(column, _))
    }
    if (!baseAdmissionRules && !canEvictStateColumn) return None

    // A bound needs a term on the opposite input's watermarked event time; a conjunct that uses
    // only constants does not follow the watermark, and gives a cutoff only with
    // keepConstantOnly. The occurrence check above limits each input to one attribute, so there
    // can be at most one such term.
    val watermarkTerms = terms.filter(_.references.exists(attributesWithEventWatermark.contains))
    if (!baseAdmissionRules && (watermarkTerms.isEmpty && !keepConstantOnly ||
        !watermarkTerms.headOption.forall(_.references.forall(_.metadata.contains(delayKey))))) {
      return None
    }

    // A negative opposite-time term lets later input match old state instead of bounding it.
    if (!baseAdmissionRules && watermarkTerms.headOption.exists(_.isInstanceOf[UnaryMinus])) {
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

  /** Whether a column of a join input can take a range cutoff: TIMESTAMP, or a time window. */
  def canTakeRangeCutoff(column: Attribute): Boolean =
    column.dataType == TimestampType || windowDuration(column).isDefined

  /** The length of the windows in a column built by window(), when the column records it. */
  private def windowDuration(column: Attribute): Option[Long] = {
    val metadata = column.metadata
    if (metadata.contains(TimeWindow.marker) && metadata.contains(TimeWindow.durationKey)) {
      Some(metadata.getLong(TimeWindow.durationKey))
    } else {
      None
    }
  }

  /**
   * Whether evicting `stateWindow` by its end is safe when a range condition compares it, as a
   * whole, with `watermarkWindow`, the other input's watermarked time window. Ordering windows
   * compares their starts first. A window evicted at a cutoff no higher than the watermark
   * starts at least its own duration before the watermark, while a later window of the other
   * input ends above the watermark, so it starts less than its own duration before it. When the
   * stored windows last at least as long as the other input's, every later window therefore
   * starts after every evicted window and orders above it. Shorter stored windows get no cutoff.
   */
  private def isWindowEvictionSafe(stateWindow: Attribute, watermarkWindow: Attribute): Boolean =
    (windowDuration(stateWindow), windowDuration(watermarkWindow)) match {
      case (Some(stored), Some(other)) => stored >= other
      case _ => false
    }

  /**
   * Whether the solver counts part of the expression inexactly: an interval of a day or more,
   * whose whole days Spark adds in the session time zone, where a day can be 23 or 25 hours, or a
   * cast that drops part of a time, such as CAST(ts AS BIGINT).
   */
  private def hasInexactArithmetic(expr: Expression): Boolean = {
    def hasWholeDays(interval: Expression): Boolean = {
      // An interval that cannot be evaluated here counts as inexact.
      def value = try Option(interval.eval()) catch { case NonFatal(_) => None }
      !interval.foldable || value.forall {
        case micros: Long => micros <= -MICROS_PER_DAY || micros >= MICROS_PER_DAY
        case calendarInterval: CalendarInterval => calendarInterval.days != 0
        case _ => true
      }
    }
    def isTruncatingCast(from: DataType, to: DataType): Boolean =
      !from.isInstanceOf[IntegralType] && (to match {
        case _: IntegralType | FloatType => true
        case decimal: DecimalType => decimal.scale < 6
        case _ => false
      })
    // Whole days count as inexact in every session time zone, as they do for equality keys.
    // Whether a column gets a cutoff decides which column V4 stores and finds rows by, and the
    // offset log does not keep the session time zone, so that must not depend on the zone.
    expr.exists {
      case TimestampAddInterval(_, interval, _) => hasWholeDays(interval)
      case Cast(child, dataType, _, _) => isTruncatingCast(child.dataType, dataType)
      case _ => false
    }
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
