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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.AGGREGATE
import org.apache.spark.sql.internal.SQLConf

/**
 * Rewrites COUNT(DISTINCT IF(cond, base, NULL)) and
 * COUNT(DISTINCT CASE WHEN cond THEN base END) into
 * COUNT(DISTINCT base) FILTER (WHERE cond).
 *
 * This canonicalization reduces the number of distinct groups seen by
 * RewriteDistinctAggregates from N (one per unique conditional expression) down to 1
 * (all share the same base column), collapsing the Expand factor from Nx to 1x.
 *
 * Correctness: COUNT DISTINCT ignores NULLs, so nulling out rows where !cond
 * is semantically identical to filtering those rows out entirely.
 */
object RewriteCountDistinctConditional extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!SQLConf.get.rewriteCountDistinctConditionalEnabled) {
      return plan
    }
    plan.transformUpWithPruning(_.containsPattern(AGGREGATE)) {
      case agg: Aggregate => agg.transformExpressionsUp {
        case ae @ AggregateExpression(
            count: Count,
            _,
            true,   // isDistinct
            None,   // no existing FILTER
            _)
            if count.children.size == 1 =>
          extractCondAndBase(count.children.head) match {
            case Some((cond, base)) =>
              ae.copy(
                aggregateFunction = count.withNewChildren(Seq(base)).asInstanceOf[Count],
                filter = Some(cond))
            case None => ae
          }
      }
    }
  }

  /**
   * Matches:
   *   IF(cond, base, null)
   *   CASE WHEN cond THEN base END
   *   CASE WHEN cond THEN base ELSE NULL END
   *
   * The analyzer may wrap the null branch in a Cast for type alignment, so the
   * null check is done after unwrapping any surrounding Casts.
   *
   * Returns None for anything else, including IF(cond, base, fallback) where
   * fallback is not null -- those change semantics and must not be rewritten.
   */
  private def extractCondAndBase(expr: Expression): Option[(Expression, Expression)] =
    expr match {
      case If(cond, base, e) if isNullExpr(e) => Some((cond, base))
      case CaseWhen(Seq((cond, base)), None) => Some((cond, base))
      case CaseWhen(Seq((cond, base)), Some(e)) if isNullExpr(e) => Some((cond, base))
      case _ => None
    }

  private def isNullExpr(e: Expression): Boolean = e match {
    case Literal(null, _) => true
    case Cast(child, _, _, _) => isNullExpr(child)
    case _ => false
  }
}
