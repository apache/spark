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

import scala.reflect.ClassTag

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{RDD, ZippedPartitionsBaseRDD, ZippedPartitionsPartition}
import org.apache.spark.sql.catalyst.analysis.StreamingJoinHelper
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeSet, Between, BinaryComparison, BoundReference, Cast, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, NamedExpression, PredicateHelper, TimestampAddInterval}
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark._
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_DAY
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.streaming.operators.stateful.{StatefulOperatorStateInfo, WatermarkSupport}
import org.apache.spark.sql.execution.streaming.operators.stateful.WatermarkSupport.watermarkExpression
import org.apache.spark.sql.execution.streaming.state.{StateStoreCheckpointInfo, StateStoreCoordinatorRef, StateStoreProviderId}
import org.apache.spark.sql.types.{CalendarIntervalType, DayTimeIntervalType, TimestampType}
import org.apache.spark.unsafe.types.CalendarInterval


/**
 * Helper object for [[StreamingSymmetricHashJoinExec]]. See that object for more details.
 */
object StreamingSymmetricHashJoinHelper extends Logging {

  sealed trait JoinSide
  case object LeftSide extends JoinSide { override def toString(): String = "left" }
  case object RightSide extends JoinSide { override def toString(): String = "right" }

  sealed trait JoinStateWatermarkPredicate {
    def expr: Expression
    def desc: String
    override def toString: String = s"$desc: $expr"
  }
  /**
   * Predicate for watermark on state keys.
   *
   * @param stateWatermark Current batch's eviction watermark. Entries with timestamp
   *                       at or below this value are eligible for eviction in this batch.
   * @param prevStateWatermark Safe exclusive lower bound in the same timestamp domain as
   *                           `stateWatermark`. It is derived from a prior eviction cutoff
   *                           only when prior eviction and late-input filtering prove that
   *                           the skipped range is empty: entries at or below this value
   *                           were already evicted, and newly admitted rows cannot land
   *                           there. The effective range of entries to evict in this batch
   *                           is `(prevStateWatermark, stateWatermark]`. State manager
   *                           implementations can leverage this lower bound to optimize
   *                           eviction (e.g. narrowing the scan range to skip already-evicted
   *                           entries). `None` means we do not have a known lower bound, or
   *                           cannot prove that late-input filtering excludes newly admitted
   *                           rows at or below it. In that case eviction must consider all
   *                           entries up to `stateWatermark`.
   */
  case class JoinStateKeyWatermarkPredicate(
      expr: Expression,
      stateWatermark: Long,
      prevStateWatermark: Option[Long] = None)
    extends JoinStateWatermarkPredicate {
    def desc: String = "key predicate"
  }
  /**
   * Predicate for watermark on state values. See [[JoinStateKeyWatermarkPredicate]] for
   * the semantics of `stateWatermark` and `prevStateWatermark`.
   */
  case class JoinStateValueWatermarkPredicate(
      expr: Expression,
      stateWatermark: Long,
      prevStateWatermark: Option[Long] = None)
    extends JoinStateWatermarkPredicate {
    def desc: String = "value predicate"
  }

  case class JoinStateWatermarkPredicates(
    left: Option[JoinStateWatermarkPredicate] = None,
    right: Option[JoinStateWatermarkPredicate] = None) {
    override def toString(): String = {
      s"state cleanup [ left ${left.map(_.toString).getOrElse("= null")}, " +
        s"right ${right.map(_.toString).getOrElse("= null")} ]"
    }
  }

  /**
   * Wrapper around various useful splits of the join condition.
   * left AND right AND joined is equivalent to full.
   *
   * Note that left and right do not necessarily contain *all* conjuncts which satisfy
   * their condition.
   *
   * @param leftSideOnly Deterministic conjuncts which reference only the left side of the join.
   * @param rightSideOnly Deterministic conjuncts which reference only the right side of the join.
   * @param bothSides Conjuncts which are nondeterministic, occur after a nondeterministic conjunct,
   *                  or reference both left and right sides of the join.
   * @param full The full join condition.
   */
  case class JoinConditionSplitPredicates(
      leftSideOnly: Option[Expression],
      rightSideOnly: Option[Expression],
      bothSides: Option[Expression],
      full: Option[Expression]) {
    override def toString(): String = {
      s"condition = [ leftOnly = ${leftSideOnly.map(_.toString).getOrElse("null")}, " +
        s"rightOnly = ${rightSideOnly.map(_.toString).getOrElse("null")}, " +
        s"both = ${bothSides.map(_.toString).getOrElse("null")}, " +
        s"full = ${full.map(_.toString).getOrElse("null")} ]"
    }
  }

  object JoinConditionSplitPredicates extends PredicateHelper {
    def apply(condition: Option[Expression], left: SparkPlan, right: SparkPlan):
        JoinConditionSplitPredicates = {
      // Split the condition into 3 parts:
      // * Conjuncts that can be evaluated on only the left input.
      // * Conjuncts that can be evaluated on only the right input.
      // * Conjuncts that require both left and right input.
      //
      // Note that we treat nondeterministic conjuncts as though they require both left and right
      // input. To maintain their semantics, they need to be evaluated exactly once per joined row.
      val (leftCondition, rightCondition, joinedCondition) = {
        if (condition.isEmpty) {
          (None, None, None)
        } else {
          // Span rather than partition, because nondeterministic expressions don't commute
          // across AND.
          val (deterministicConjuncts, nonDeterministicConjuncts) =
            splitConjunctivePredicates(condition.get).partition(_.deterministic)

          val (leftConjuncts, nonLeftConjuncts) = deterministicConjuncts.partition { cond =>
            cond.references.subsetOf(left.outputSet)
          }

          val (rightConjuncts, nonRightConjuncts) = deterministicConjuncts.partition { cond =>
            cond.references.subsetOf(right.outputSet)
          }

          (
            leftConjuncts.reduceOption(And),
            rightConjuncts.reduceOption(And),
            (nonLeftConjuncts.intersect(nonRightConjuncts) ++ nonDeterministicConjuncts)
              .reduceOption(And)
          )
        }
      }

      JoinConditionSplitPredicates(leftCondition, rightCondition, joinedCondition, condition)
    }
  }

  /**
   * Get the state watermarks used to derive the join's output watermark. A value-eviction cutoff
   * may constrain a different timestamp column and therefore not bound buffered event times.
   */
  def getStateWatermark(
      leftAttributes: Seq[Attribute],
      rightAttributes: Seq[Attribute],
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      condition: Option[Expression],
      eventTimeWatermarkForEviction: Option[Long],
      allowMultipleEventTimeColumns: Boolean): (Option[Long], Option[Long]) = {

    val predicates = getStateWatermarkPredicates(leftAttributes, rightAttributes,
      leftKeys, rightKeys, condition, eventTimeWatermarkForEviction, None,
      allowMultipleEventTimeColumns)

    def getOneSideStateWatermark(
        oneSideInputAttributes: Seq[Attribute],
        oneSideKeys: Seq[Expression],
        predicate: Option[JoinStateWatermarkPredicate]): Option[Long] = {
      WatermarkSupport.findEventTimeColumn(oneSideInputAttributes,
        allowMultipleEventTimeColumns).map { eventTime =>
        predicate match {
          case Some(JoinStateKeyWatermarkPredicate(expr, watermark, _)) =>
            // Retained keys are above the cutoff. When the key is this side's event time,
            // possibly plus a constant, that bounds the event times of the retained rows.
            // Otherwise return 0 for the reason below.
            expr.collectFirst { case b: BoundReference => b.ordinal }.flatMap { ordinal =>
              StreamingJoinHelper.getEventTimeWatermarkFromKey(
                oneSideKeys, oneSideInputAttributes, ordinal, watermark)
            }.getOrElse(0L)
          case Some(JoinStateValueWatermarkPredicate(expr, watermark, _))
              if expr.references.contains(eventTime) => watermark
          // Buffered rows can still emit old event times. Do not advance the output watermark
          // using a bound on a different column, or on only the other side's state. Return 0
          // rather than None: produceOutputWatermark takes the minimum of the defined bounds, so
          // None would drop this side and let the output watermark advance past those rows.
          case _ => 0L
        }
      }
    }

    val leftStateWatermark = getOneSideStateWatermark(leftAttributes, leftKeys, predicates.left)
    val rightStateWatermark = getOneSideStateWatermark(rightAttributes, rightKeys, predicates.right)

    (leftStateWatermark, rightStateWatermark)
  }

  /** Get the predicates defining the state watermarks for both sides of the join */
  def getStateWatermarkPredicates(
      leftAttributes: Seq[Attribute],
      rightAttributes: Seq[Attribute],
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      condition: Option[Expression],
      eventTimeWatermarkForEviction: Option[Long],
      eventTimeWatermarkForLateEvents: Option[Long],
      useFirstEventTimeColumn: Boolean): JoinStateWatermarkPredicates = {

    // Perform assertions against multiple event time columns in the same DataFrame. This method
    // assumes there is only one event time column per each side (left / right) and it is not very
    // clear to reason about the correctness if there are multiple event time columns. Disallow to
    // be conservative.
    WatermarkSupport.findEventTimeColumn(leftAttributes,
      allowMultipleEventTimeColumns = useFirstEventTimeColumn)
    WatermarkSupport.findEventTimeColumn(rightAttributes,
      allowMultipleEventTimeColumns = useFirstEventTimeColumn)

    def getOneSideStateWatermarkPredicate(
        oneSideInputAttributes: Seq[Attribute],
        oneSideJoinKeys: Seq[Expression],
        otherSideInputAttributes: Seq[Attribute],
        otherSideJoinKeys: Seq[Expression]): Option[JoinStateWatermarkPredicate] = {
      // State is bounded by future input from the opposite side, not by its own watermark.
      val joinKeyOrdinalForWatermark = StreamingJoinHelper.getStateKeyWatermarkOrdinal(
        otherSideJoinKeys, otherSideInputAttributes)
      val isWatermarkDefinedOnInput = oneSideInputAttributes.exists(_.metadata.contains(delayKey))
      val isWatermarkDefinedOnJoinKey = joinKeyOrdinalForWatermark.isDefined

      if (isWatermarkDefinedOnJoinKey) { // case 1 and 3 in the StreamingSymmetricHashJoinExec docs
        val ordinal = joinKeyOrdinalForWatermark.get
        val keyExprWithWatermark = BoundReference(
          ordinal, oneSideJoinKeys(ordinal).dataType, oneSideJoinKeys(ordinal).nullable)
        // The opposite watermark bounds this key when the opposite key is that input's event
        // time, possibly shifted by constant intervals: by the shift for an exact interval, and
        // unshifted for whole days, months or years that never move a time backward. See
        // StreamingJoinHelper.getStateKeyWatermark for which shifts qualify.
        def keyWatermark(watermark: Option[Long]): Option[Long] =
          StreamingJoinHelper.getStateKeyWatermark(
            otherSideJoinKeys, otherSideInputAttributes, ordinal, watermark)
        keyWatermark(eventTimeWatermarkForEviction).flatMap { stateWatermark =>
          watermarkExpression(Some(keyExprWithWatermark), Some(stateWatermark)).map { e =>
            // V4 can skip keys at or below the previous watermark only if late-input filtering
            // rejects new rows in that range on this same key. A watermark on the other input's
            // equality key can drive eviction even when this input has no watermark, or has one
            // on a different column. New rows can then pass late-input filtering with keys
            // already eligible for eviction. V4 must scan from the beginning to find them;
            // otherwise, outer joins can fail to emit null rows for these unmatched inputs.
            val prevStateKeyWatermark =
              if (oneSideInputAttributes.find(_.metadata.contains(delayKey))
                .exists(oneSideJoinKeys(ordinal).semanticEquals)) {
                // Late filtering drops new keys at or below the late-input watermark, and the
                // previous batch evicted keys up to its cutoff, which is the cutoff at that
                // watermark. A shifted cutoff can lie on either side of the watermark, so the
                // scan starts after the lower of the two.
                eventTimeWatermarkForLateEvents.flatMap { lateWatermark =>
                  keyWatermark(Some(lateWatermark)).map(math.min(_, lateWatermark))
                }
              } else {
                None
              }
            JoinStateKeyWatermarkPredicate(e, stateWatermark, prevStateKeyWatermark)
          }
        }
      } else if (isWatermarkDefinedOnInput) { // case 2 in the StreamingSymmetricHashJoinExec docs
        // TODO(SPARK-61347): this needs the input's own watermark, although the analysis check
        //   admits a range bound without one.
        // TODO(SPARK-61348): the analysis check also admits a range bound on a column that is
        //   not TIMESTAMP, which this branch never evicts.
        getStateValueWatermarkForInput(oneSideInputAttributes, otherSideInputAttributes,
          condition, eventTimeWatermarkForEviction).flatMap { case (attribute, watermark) =>
          // Late filtering only constrains the input's event-time column. A different indexed
          // column can admit values below the previous cutoff and must be scanned from the start.
          val prevStateValueWatermark = eventTimeWatermarkForLateEvents.filter { _ =>
            oneSideInputAttributes.find(_.metadata.contains(delayKey))
              .exists(_.semanticEquals(attribute))
          }.flatMap { lateWatermark =>
            // Also scan from the start if the derived cutoff exceeds the late-input watermark.
            StreamingJoinHelper.getStateValueWatermark(AttributeSet(Seq(attribute)),
              AttributeSet(otherSideInputAttributes), condition, Some(lateWatermark))
              .filter(_ <= lateWatermark)
          }
          watermarkExpression(Some(attribute), Some(watermark)).map { e =>
            JoinStateValueWatermarkPredicate(e, watermark, prevStateValueWatermark)
          }
        }
      } else {
        None
      }
    }

    val leftStateWatermarkPredicate =
      getOneSideStateWatermarkPredicate(leftAttributes, leftKeys, rightAttributes, rightKeys)
    val rightStateWatermarkPredicate =
      getOneSideStateWatermarkPredicate(rightAttributes, rightKeys, leftAttributes, leftKeys)
    JoinStateWatermarkPredicates(leftStateWatermarkPredicate, rightStateWatermarkPredicate)
  }

  /** Keep each cutoff attached to the column constrained by the join condition. */
  private def getStateValueWatermarkForInput(
      inputAttributes: Seq[Attribute],
      otherInputAttributes: Seq[Attribute],
      condition: Option[Expression],
      eventWatermark: Option[Long]): Option[(Attribute, Long)] = {
    // TODO(SPARK-61348): the analysis check admits a range bound on a column that is not
    //   TIMESTAMP, which no candidate below takes. A left, right or full outer join whose
    //   preserved side is bounded only through such a column cast to TIMESTAMP then keeps its
    //   unmatched rows in state and never emits them. SPARK-61348 rejects that shape at analysis.
    // TODO(SPARK-61349): a conjunct that uses only constants still caps the cutoff, so it can
    //   stop eviction.
    val eventTime = inputAttributes.find(_.metadata.contains(delayKey))
    // First try the watermarked column. If it has no inferable bound, try the other timestamp
    // columns in input schema order and use the first with a bound. This makes the indexed column
    // deterministic for a fixed input schema and join condition.
    val candidates = eventTime.toSeq ++ inputAttributes.filter { attribute =>
      attribute.dataType == TimestampType && !eventTime.exists(_.semanticEquals(attribute))
    }
    candidates.iterator.flatMap { attribute =>
      StreamingJoinHelper.getStateValueWatermark(AttributeSet(Seq(attribute)),
        AttributeSet(otherInputAttributes), condition, eventWatermark).map(attribute -> _)
    }.take(1).toSeq.headOption
  }

  /**
   * Get the equality-key ordinal at which a V4 state manager stores and looks up a side's rows.
   * A side evicted by key uses the key that the opposite input's watermark bounds. Any other side
   * stores each row at its own event time, unless it is evicted on another column. When that
   * event time is an equality key, its ordinal gives the same stored timestamps and lets a lookup
   * read one timestamp instead of scanning every timestamp of the key.
   */
  def getStateKeyOrdinalForLookup(
      oneSideAttributes: Seq[Attribute],
      oneSideKeys: Seq[Expression],
      otherSideAttributes: Seq[Attribute],
      otherSideKeys: Seq[Expression],
      predicate: Option[JoinStateWatermarkPredicate]): Option[Int] = {
    def storesRowsAtKey(ordinal: Int): Boolean = predicate match {
      case Some(p: JoinStateValueWatermarkPredicate) =>
        p.expr.references.forall(_.semanticEquals(oneSideKeys(ordinal)))
      case _ => true
    }
    StreamingJoinHelper.getStateKeyWatermarkOrdinal(otherSideKeys, otherSideAttributes).orElse(
      StreamingJoinHelper.getEventTimeKeyOrdinal(oneSideKeys, oneSideAttributes)
        .filter(storesRowsAtKey))
  }

  /**
   * Conservatively prove that older state can use the current watermark semantics. Without
   * watermarks there is no affected state. An input that neither version evicts keeps the same
   * rows, but its state watermark in the join's output watermark changed, so such an input is
   * accepted only when no stateful operator after the join reads its event time:
   * `consumedSides` names the inputs whose event time such an operator reads, both when the
   * operator cannot be traced and none when no stateful operator follows. A key-only join, with
   * no other condition and no equality key that uses an event time, is evicted by neither
   * version, and V4 reads its rows by key prefix. Otherwise each side must have exactly one
   * event-time column, and either its own event time must be the sole watermarked equality key
   * at the same ordinal, or the join must be a time-interval join on the event times (see
   * [[isOwnEventTimeIntervalJoin]]). This excludes other range bounds. It also excludes a key
   * computed from the event time by a constant, which older versions evicted at the unshifted
   * watermark. A NULL selected key does not change the semantics: such a row can never match.
   * Old V4 state indexed it at timestamp 0 under its own key, and the first batch after the
   * restart scans V4 state from the start, so it evicts the row and a preserved side emits it
   * unless an older version already joined it. V1 to V3 keep such a row, as older versions did,
   * because its eviction predicate is unknown.
   * TODO: evict the NULL-key rows that older versions stored in V1 to V3 state.
   */
  private[sql] def isWatermarkCheckpointCompatible(
      leftAttributes: Seq[Attribute],
      rightAttributes: Seq[Attribute],
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      condition: Option[Expression],
      consumedSides: Set[JoinSide]): Boolean = {
    val leftEventTime = leftAttributes.filter(_.metadata.contains(delayKey))
    val rightEventTime = rightAttributes.filter(_.metadata.contains(delayKey))
    val keyOnly = condition.isEmpty && !(leftKeys ++ rightKeys).exists { key =>
      key.references.exists(_.metadata.contains(delayKey)) || (key match {
        case named: NamedExpression => named.metadata.contains(delayKey)
        case _ => false
      })
    }

    def watermarkKeyOrdinals(keys: Seq[Expression]): Seq[Int] = {
      keys.zipWithIndex.collect {
        case (key: NamedExpression, index) if key.metadata.contains(delayKey) => index
      }
    }

    val leftOrdinals = watermarkKeyOrdinals(leftKeys)
    val rightOrdinals = watermarkKeyOrdinals(rightKeys)
    if (leftEventTime.isEmpty && rightEventTime.isEmpty) {
      leftOrdinals.isEmpty && rightOrdinals.isEmpty
    } else if (keyOnly && consumedSides.isEmpty) {
      true
    } else if (leftEventTime.size == 1 && rightEventTime.size == 1) {
      val leftOrdinal = StreamingJoinHelper.getEventTimeKeyOrdinal(leftKeys, leftAttributes)
      val rightOrdinal = StreamingJoinHelper.getEventTimeKeyOrdinal(rightKeys, rightAttributes)
      val alignedOwnEventTimeKeys = leftOrdinal.isDefined && leftOrdinal == rightOrdinal &&
        leftOrdinals == leftOrdinal.toSeq && rightOrdinals == rightOrdinal.toSeq
      alignedOwnEventTimeKeys || isOwnEventTimeIntervalJoin(leftAttributes, rightAttributes,
        leftKeys, rightKeys, condition, leftEventTime.head, rightEventTime.head, consumedSides)
    } else {
      false
    }
  }

  /**
   * Whether every ordering conjunct compares the two inputs' TIMESTAMP event times, each only cast
   * or offset by constants (see [[StreamingJoinHelper.isConstantOffsetOf]]), or gives no bound to
   * either input in either version, and each event time either gets a range bound or gets none and
   * is not in `consumedSides`. Each half of a BETWEEN counts as one conjunct, as the solver reads
   * it. Older versions and this one solve such conjuncts the same way for columns with their own
   * watermark, and older versions solved every column of an input but then found bounds only in the
   * same conjuncts, so they derived the same value predicates, scan starts and V4 index columns,
   * and neither evicts an input that gets no bound. Old V4 stored a null event time at timestamp 0
   * under the row's own key; both versions scan that entry the same way. No equality key may use an
   * event time: older versions evicted such keys at the unshifted watermark. Time windows are
   * excluded: this version still derives the older ordering bound for them, but SPARK-61348 later
   * removes it for windows shorter than the other input's.
   */
  private def isOwnEventTimeIntervalJoin(
      leftAttributes: Seq[Attribute],
      rightAttributes: Seq[Attribute],
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      condition: Option[Expression],
      leftEventTime: Attribute,
      rightEventTime: Attribute,
      consumedSides: Set[JoinSide]): Boolean = {
    def conjuncts(e: Expression): Seq[Expression] = e match {
      case And(left, right) => conjuncts(left) ++ conjuncts(right)
      case other => Seq(other)
    }
    def eventTimeOf(e: Expression): Option[Attribute] =
      Seq(leftEventTime, rightEventTime).find(StreamingJoinHelper.isConstantOffsetOf(e, _))
    def comparesInputs(a: Expression, b: Expression): Boolean =
      (eventTimeOf(a), eventTimeOf(b)) match {
        case (Some(x), Some(y)) => !x.semanticEquals(y)
        case _ => false
      }
    // Whether the comparison references both inputs and neither version derives a bound for
    // either input from it, as for one between columns of both inputs that have no watermark,
    // or one with a month interval, which the solver cannot read. Older versions solved every
    // column of an input against the other input; this version solves one candidate column at a
    // time. A comparison within one input stays excluded: older versions read some of them, such
    // as one with a numeric constant, as a bound.
    def givesNoBound(comparison: Expression): Boolean = {
      val inputs = Seq(leftAttributes -> rightAttributes, rightAttributes -> leftAttributes)
      inputs.forall { case (input, _) =>
        comparison.references.exists(a => input.exists(_.semanticEquals(a)))
      } && inputs.forall { case (input, other) =>
        StreamingJoinHelper.getStateValueWatermark(AttributeSet(input), AttributeSet(other),
          Some(comparison), Some(1000000L)).isEmpty &&
          getStateValueWatermarkForInput(input, other, Some(comparison), Some(1000000L)).isEmpty
      }
    }
    // The solver reads bounds only from these conjunct shapes; other conjuncts give no bound in
    // either version.
    val conditionIsInterval = condition.toSeq.flatMap(conjuncts).forall {
      case c @ (_: LessThan | _: LessThanOrEqual | _: GreaterThan | _: GreaterThanOrEqual) =>
        val comparison = c.asInstanceOf[BinaryComparison]
        comparesInputs(comparison.left, comparison.right) || givesNoBound(comparison)
      case Between(input, lower, upper, _) =>
        (comparesInputs(input, lower) || givesNoBound(GreaterThanOrEqual(input, lower))) &&
          (comparesInputs(input, upper) || givesNoBound(LessThanOrEqual(input, upper)))
      case _ => true
    }
    val keysUseEventTime = (leftKeys ++ rightKeys).exists(
      _.references.exists(_.metadata.contains(delayKey)))
    def boundsEventTime(
        predicate: Option[JoinStateWatermarkPredicate], eventTime: Attribute): Boolean = {
      predicate.exists {
        case JoinStateValueWatermarkPredicate(e, _, _) => e.references.contains(eventTime)
        case _ => false
      }
    }
    val timestampEventTimes =
      leftEventTime.dataType == TimestampType && rightEventTime.dataType == TimestampType
    !keysUseEventTime && timestampEventTimes && conditionIsInterval && {
      // The cutoffs are the watermark plus constants, so any watermark shows whether they exist.
      val predicates = getStateWatermarkPredicates(leftAttributes, rightAttributes, leftKeys,
        rightKeys, condition, Some(1000000L), None, useFirstEventTimeColumn = false)
      def sameEviction(
          predicate: Option[JoinStateWatermarkPredicate],
          eventTime: Attribute,
          side: JoinSide): Boolean = {
        boundsEventTime(predicate, eventTime) || predicate.isEmpty && !consumedSides(side)
      }
      sameEviction(predicates.left, leftEventTime, LeftSide) &&
        sameEviction(predicates.right, rightEventTime, RightSide)
    }
  }

  /**
   * For the right input of a left semi join, cutoffs at the late-events watermark that are exact
   * today: no left row that passes late filtering can match a right row at or below them. Returns
   * a predicate on the right join key and a predicate on the right input row, either of which
   * can be absent.
   *
   * The key cutoff applies when the left input's late-filtered column is the left join key at an
   * ordinal: the right key at that ordinal must then be above the late-events watermark. The
   * value cutoff is on the right event time, solved only from conjuncts that compare it with the
   * left input's late-filtered column, each shifted only by literal intervals without a month
   * part, and with no cast other than one from timestamp to timestamp. A day part counts only in
   * a time zone with a fixed offset, such as UTC, where Spark adds a day as 24 hours, as the
   * solver counts it. Any other conjunct can give a cutoff that is too high, so it gives none
   * here: for example an interval with whole days in a time zone with daylight saving time, or a
   * truncating cast.
   * TODO(SPARK-61381): derive exact cutoffs for those conditions too.
   */
  def getLeftSemiExactRightCutoffs(
      leftAttributes: Seq[Attribute],
      rightAttributes: Seq[Attribute],
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      condition: Option[Expression],
      eventTimeWatermarkForLateEvents: Option[Long],
      useFirstEventTimeColumn: Boolean): (Option[Expression], Option[Expression]) = {
    // The column the left input's late filter compares with the late-events watermark.
    val leftLateFiltered = WatermarkSupport.findEventTimeColumn(leftAttributes,
      allowMultipleEventTimeColumns = useFirstEventTimeColumn)
    val rightEventTime = WatermarkSupport.findEventTimeColumn(rightAttributes,
      allowMultipleEventTimeColumns = useFirstEventTimeColumn)

    val keyCutoff = leftLateFiltered.flatMap { column =>
      leftKeys.indexWhere(_.semanticEquals(column)) match {
        case ordinal if ordinal >= 0 =>
          val rightKey = BoundReference(
            ordinal, rightKeys(ordinal).dataType, rightKeys(ordinal).nullable)
          watermarkExpression(Some(rightKey), eventTimeWatermarkForLateEvents)
        case _ => None
      }
    }

    // Whether the expression is the column shifted only by literal intervals that the solver
    // counts exactly: no months, and whole days only where the zone that the shift adds them in
    // has a fixed offset. Elsewhere a day can be 23 or 25 hours, but the solver counts 24.
    def addsDaysExactly(shift: TimestampAddInterval): Boolean =
      shift.timeZoneId.exists(zone => DateTimeUtils.getZoneId(zone).getRules.isFixedOffset)
    def isExactShiftOf(e: Expression, column: Attribute): Boolean = e match {
      case a: Attribute => a.semanticEquals(column)
      case Cast(child, TimestampType, _, _) if child.dataType == TimestampType =>
        isExactShiftOf(child, column)
      case s @ TimestampAddInterval(start, Literal(micros: Long, _: DayTimeIntervalType), _) =>
        (-MICROS_PER_DAY < micros && micros < MICROS_PER_DAY || addsDaysExactly(s)) &&
          isExactShiftOf(start, column)
      case s @ TimestampAddInterval(start, Literal(i: CalendarInterval, CalendarIntervalType), _) =>
        i.months == 0 && (i.days == 0 || addsDaysExactly(s)) && isExactShiftOf(start, column)
      case _ => false
    }
    def conjuncts(e: Expression): Seq[Expression] = e match {
      case And(left, right) => conjuncts(left) ++ conjuncts(right)
      case Between(input, lower, upper, _) =>
        Seq(GreaterThanOrEqual(input, lower), LessThanOrEqual(input, upper))
      case other => Seq(other)
    }
    val valueCutoff = for {
      left <- leftLateFiltered
      right <- rightEventTime
      watermark <- eventTimeWatermarkForLateEvents
      cutoff <- condition.toSeq.flatMap(conjuncts).filter {
        case c: BinaryComparison =>
          isExactShiftOf(c.left, right) && isExactShiftOf(c.right, left) ||
            isExactShiftOf(c.left, left) && isExactShiftOf(c.right, right)
        case _ => false
      }.flatMap { conjunct =>
        StreamingJoinHelper.getStateValueWatermark(AttributeSet(Seq(right)),
          AttributeSet(Seq(left)), Some(conjunct), Some(watermark))
      }.maxOption
      expr <- watermarkExpression(Some(right), Some(cutoff))
    } yield expr

    (keyCutoff, valueCutoff)
  }

  /**
   * A custom RDD that allows partitions to be "zipped" together, while ensuring the tasks'
   * preferred location is based on which executors have the required join state stores already
   * loaded. This class is a variant of [[org.apache.spark.rdd.ZippedPartitionsRDD2]] which only
   * changes signature of `f`.
   */
  class StateStoreAwareZipPartitionsRDD[A: ClassTag, B: ClassTag, V: ClassTag](
      sc: SparkContext,
      var f: (Int, Iterator[A], Iterator[B]) => Iterator[V],
      var rdd1: RDD[A],
      var rdd2: RDD[B],
      stateInfo: StatefulOperatorStateInfo,
      stateStoreNames: Seq[String],
      @transient private val storeCoordinator: Option[StateStoreCoordinatorRef])
      extends ZippedPartitionsBaseRDD[V](sc, List(rdd1, rdd2)) {

    /**
     * Set the preferred location of each partition using the executor that has the related
     * [[StateStoreProvider]] already loaded.
     */
    override def getPreferredLocations(partition: Partition): Seq[String] = {
      stateStoreNames.flatMap { storeName =>
        val stateStoreProviderId = StateStoreProviderId(stateInfo, partition.index, storeName)
        storeCoordinator.flatMap(_.getLocation(stateStoreProviderId))
      }.distinct
    }

    override def compute(s: Partition, context: TaskContext): Iterator[V] = {
      val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions
      if (partitions(0).index != partitions(1).index) {
        throw new IllegalStateException(s"Partition ID should be same in both side: " +
          s"left ${partitions(0).index} , right ${partitions(1).index}")
      }

      val partitionId = partitions(0).index
      f(partitionId, rdd1.iterator(partitions(0), context), rdd2.iterator(partitions(1), context))
    }

    override def clearDependencies(): Unit = {
      super.clearDependencies()
      rdd1 = null
      rdd2 = null
      f = null
    }
  }

  implicit class StateStoreAwareZipPartitionsHelper[T: ClassTag](dataRDD: RDD[T]) {
    /**
     * Function used by `StreamingSymmetricHashJoinExec` to zip together the partitions of two
     * child RDDs for joining the data in corresponding partitions, while ensuring the tasks'
     * preferred location is based on which executors have the required join state stores already
     * loaded.
     */
    def stateStoreAwareZipPartitions[U: ClassTag, V: ClassTag](
        dataRDD2: RDD[U],
        stateInfo: StatefulOperatorStateInfo,
        storeNames: Seq[String],
        storeCoordinator: StateStoreCoordinatorRef
      )(f: (Int, Iterator[T], Iterator[U]) => Iterator[V]): RDD[V] = {
      new StateStoreAwareZipPartitionsRDD(
        dataRDD.sparkContext, f, dataRDD, dataRDD2, stateInfo, storeNames, Some(storeCoordinator))
    }
  }

  case class JoinerStateStoreCkptInfo(
      keyToNumValues: StateStoreCheckpointInfo,
      keyWithIndexToValue: StateStoreCheckpointInfo)

  case class JoinStateStoreCkptInfo(
      left: JoinerStateStoreCkptInfo,
      right: JoinerStateStoreCkptInfo)

  case class JoinerStateStoreCheckpointId(
      keyToNumValues: Option[String],
      keyWithIndexToValue: Option[String])

  case class JoinStateStoreCheckpointId(
       left: JoinerStateStoreCheckpointId,
       right: JoinerStateStoreCheckpointId)

}
