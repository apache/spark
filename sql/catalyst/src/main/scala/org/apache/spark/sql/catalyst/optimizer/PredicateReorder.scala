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
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.FilterEstimation
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * A rule that reorder predicate expressions to get better performance.
 */
object PredicateReorder extends Rule[LogicalPlan] with PredicateHelper {

  // Return 1.0D - 1.0D / Int.MaxValue if the expression is not supported calculate
  // filter selectivity to avoid rankingAnd always return 0.0D.
  private def selectivity(exp: Expression, filterEstimation: FilterEstimation): Double = {
    filterEstimation.calculateFilterSelectivity(exp, false)
      .getOrElse(1.0D - 1.0D / Int.MaxValue)
  }

  // Formula: (selectivity - 1.0D) / expression cost
  private def rankingAnd(exp: Expression, filterEstimation: FilterEstimation): Double = {
    (selectivity(exp, filterEstimation) - 1.0D) / expressionCost(exp)
  }

  // Formula: (-selectivity) / expression cost
  private def rankingOr(exp: Expression, filterEstimation: FilterEstimation): Double = {
    -selectivity(exp, filterEstimation) / expressionCost(exp)
  }

  // The cost of a call expression e is computed as:
  //   cost(exp) = typeSize + functionCost + cost(children).
  private def expressionCost(exp: Expression): Double = exp match {
      case e: Expression if e.children.isEmpty =>
        e.dataType.defaultSize
      case e: IsNull =>
        e.dataType.defaultSize + 1.0D + e.children.map(expressionCost).sum
      case e: IsNotNull =>
        e.dataType.defaultSize + 1.0D + e.children.map(expressionCost).sum
      case e: IsNaN =>
        e.dataType.defaultSize + 1.0D + e.children.map(expressionCost).sum
      case e: Not =>
        e.dataType.defaultSize + 1.0D + e.children.map(expressionCost).sum
      case e: BinaryOperator =>
        e.dataType.defaultSize + 1.0D + e.children.map(expressionCost).sum
      case e: StringRegexExpression =>
        e.dataType.defaultSize + 2.0D + e.children.map(expressionCost).sum
      case e @ In(_, list) =>
        e.dataType.defaultSize + 2.0D * (list.size - 1)
      case e @ InSet(_, set) =>
        e.dataType.defaultSize + 2.0D * (set.size - 1)
      case e: MultiLikeBase =>
        e.dataType.defaultSize + 2.0D * e.patterns.size
      case e: Cast =>
        8.0D + e.dataType.defaultSize + e.children.map(expressionCost).sum
      case e =>
        32.0D + e.children.map(expressionCost).sum
    }

  // We do not recursively sort all expressions for performance.
  private def reorderPredicates(exp: Expression, filterEstimation: FilterEstimation): Expression = {
    exp match {
      case _: Or =>
        splitDisjunctivePredicates(exp)
          .map(e => (e, rankingOr(e, filterEstimation))).sortWith(_._2 < _._2).map(_._1)
          .reduceLeft(Or)
      case _: And =>
        splitConjunctivePredicates(exp)
          .map(e => (e, rankingAnd(e, filterEstimation))).sortWith(_._2 < _._2).map(_._1)
          .reduceLeft(And)
      case _ => exp
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.predicateReorder) {
      plan transform {
        case f @ Filter(cond, _) => f.copy(condition = reorderPredicates(cond, FilterEstimation(f)))
      }
    } else {
      plan
    }
  }
}
