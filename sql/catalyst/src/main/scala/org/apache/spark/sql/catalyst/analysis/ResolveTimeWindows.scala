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

import org.apache.spark.sql.catalyst.expressions.{SessionWindow, TimeWindow, WindowTime}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{SESSION_WINDOW, TIME_WINDOW, WINDOW_TIME}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.StructType

/**
 * Maps a time column to multiple time windows using the Expand operator. Since it's non-trivial to
 * figure out how many windows a time column can map to, we over-estimate the number of windows and
 * filter out the rows where the time column is not inside the time window.
 */
object TimeWindowing extends Rule[LogicalPlan] {

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
   *   remainder <- (timestamp - startTime) % slideDuration
   *   lastStart <- timestamp - ((remainder < 0) ? remainder + slideDuration : remainder)
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

        // time window is provided as time column of window function, replace it with WindowTime
        if (StructType.acceptsType(window.timeColumn.dataType)) {
          p.transformExpressions {
            case t: TimeWindow => t.copy(timeColumn = WindowTime(window.timeColumn))
          }
        } else {
          val (windowAttr, newChild) =
            TimeWindowResolution.buildTimeWindowRewrite(window, child)

          val replacedPlan = p.transformExpressions {
            case _: TimeWindow => windowAttr
          }
          replacedPlan.withNewChildren(newChild :: Nil)
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

  /**
   * Generates the logical plan for generating session window on a timestamp column.
   * Each session window is initially defined as [timestamp, timestamp + gap).
   *
   * This also adds a marker to the session column so that downstream can easily find the column
   * on session window.
   */
  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
    _.containsPattern(SESSION_WINDOW), ruleId) {
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
          p.transformExpressions {
            case t: SessionWindow => t.copy(timeColumn = WindowTime(session.timeColumn))
          }
        } else {
          val (sessionAttr, newChild) =
            TimeWindowResolution.buildSessionWindowRewrite(session, child)

          val replacedPlan = p.transformExpressions {
            case _: SessionWindow => sessionAttr
          }
          replacedPlan.withNewChildren(newChild :: Nil)
        }
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
 */
object ResolveWindowTime extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
    _.containsPattern(WINDOW_TIME), ruleId) {
    case p: LogicalPlan if p.children.size == 1 =>
      val child = p.children.head
      val windowTimeExpressions =
        p.expressions.flatMap(_.collect { case w: WindowTime => w }).toSet

      val allWindowTimeExprsResolved = windowTimeExpressions.forall { w =>
        w.windowColumn.resolved && w.checkInputDataTypes().isSuccess
      }

      if (windowTimeExpressions.nonEmpty && allWindowTimeExprsResolved) {
        val (windowTimeToAttr, newChild) =
          TimeWindowResolution.buildWindowTimeRewrite(windowTimeExpressions, child, p)

        val replacedPlan = p.transformExpressions {
          case w: WindowTime => windowTimeToAttr(w)
        }
        replacedPlan.withNewChildren(newChild :: Nil)
      } else {
        p // Return unchanged. Analyzer will throw exception later
      }
  }
}
