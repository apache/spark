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

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeWatermark, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark._
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_DAY
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * Helper object for stream joins. See [[StreamingSymmetricHashJoinExec]] in SQL for more details.
 */
object StreamingJoinHelper extends PredicateHelper with Logging {

  /**
   * Check the provided logical plan to see if its join keys contain a watermark attribute.
   *
   * Will return false if the plan is not an equijoin.
   * @param plan the logical plan to check
   */
  def isWatermarkInJoinKeys(plan: LogicalPlan): Boolean = {
    plan match {
      case ExtractEquiJoinKeys(_, leftKeys, rightKeys, _, _, _, _, _) =>
        (leftKeys ++ rightKeys).exists {
          case a: AttributeReference => a.metadata.contains(EventTimeWatermark.delayKey)
          case _ => false
        }
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
          logWarning(s"Error trying to extract state constraint from condition $joinCondition", e)
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
        case _ => None
      }
      if (stateWatermark.nonEmpty) {
        logInfo(s"Condition $joinCondition generated watermark constraint = ${stateWatermark.get}")
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

    val attributesInCondition = AttributeSet(
      leftExpr.collect { case a: AttributeReference => a } ++
      rightExpr.collect { case a: AttributeReference => a }
    )
    if (attributesInCondition.filter { attributesToFindStateWatermarkFor.contains(_) }.size > 1 ||
        attributesInCondition.filter { attributesWithEventWatermark.contains(_) }.size > 1) {
      // If more than attributes present in condition from one side, then it cannot be solved
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
    val terms = ExpressionSet(collectTerms(allOnLeftExpr))
    logDebug("Terms extracted from join condition:\n\t" + terms.mkString("\n\t"))

    // Find the term that has leftTime (i.e. the one present in attributesToFindConstraintFor
    val constraintTerms = terms.filter(containsAttributeToFindStateConstraintFor)

    // Verify there is only one correct constraint term and of the correct type
    if (constraintTerms.size > 1) {
      logWarning("Failed to extract state constraint terms: multiple time terms in condition\n\t" +
        terms.mkString("\n\t"))
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
    val exprWithWatermarkSubstituted = (terms - constraintTerm).map { term =>
      term.transform {
        case a @ AttributeReference(_, _, _, metadata)
          if attributesWithEventWatermark.contains(a) && metadata.contains(delayKey) =>
          Multiply(Literal(eventWatermark.get.toDouble), Literal(1000.0))
      }
    }.reduceLeft(Add(_, _))

    // Calculate the constraint value
    logInfo(s"Final expression to evaluate constraint:\t$exprWithWatermarkSubstituted")
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
        case TimeAdd(left, right, _) =>
          collect(left, negate) ++ collect(right, negate)
        case DatetimeSub(_, _, child) => collect(child, negate)
        case UnaryMinus(child, _) =>
          collect(child, !negate)
        case CheckOverflow(child, _, _) =>
          collect(child, negate)
        case PromotePrecision(child) =>
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
                  s"Failed to extract state value watermark from condition $exprToCollectFrom " +
                    s"as imprecise intervals like months and years cannot be used for" +
                    s"watermark calculation. Use interval in terms of day instead.")
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
            s"Failed to extract state value watermark from condition $exprToCollectFrom due to $a")
          invalid = true
          Seq.empty
      }
    }

    val terms = collect(exprToCollectFrom, negate = false)
    if (!invalid) terms else Seq.empty
  }
}
