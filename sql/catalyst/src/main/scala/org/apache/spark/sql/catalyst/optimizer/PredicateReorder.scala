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
 * A rule that reorder conditions in filters base on estimated selectivity and compute cost.
 *
 * {{{
 *   SELECT * FROM lineitem WHERE l_comment LIKE '%a%' AND l_orderkey = 1024 ==>
 *   SELECT * FROM lineitem WHERE l_orderkey = 1024 AND l_comment LIKE '%a%'
 * }}}
 */
object PredicateReorder extends Rule[LogicalPlan] with PredicateHelper {

  private val DEFAULT_SELECTIVITY = 0.1D

  private val IS_OP_COST = 1.0D
  private val CAST_COST = 4.0D
  private val LIKE_COST = 5.0D
  private val BINARY_OP_COST = 1.0D
  private val UNARY_OP_COST = 1.0D
  private val TERNARY_OP_COST = 3.0D
  private val QUATERNARY_OP_COST = 4.0D
  private val SEPTENARY_OP_COST = 7.0D
  private val COMPLEX_TYPE_MERGING_OP_COST = 10.0D
  private val USER_DEFINED_OP_COST = 20.0D
  private val UNKNOWN_COST = 30.0D

  private def filterSelectivity(
      exp: Expression, filterEstimation: Option[FilterEstimation]): Double = {
    filterEstimation
      .flatMap(_.calculateFilterSelectivity(exp, update = false))
      .getOrElse(DEFAULT_SELECTIVITY)
  }

  // Formula: (selectivity - 1.0D) / expression cost
  private def rankingAnd(exp: Expression, filterEstimation: Option[FilterEstimation]): Double = {
    (filterSelectivity(exp, filterEstimation) - 1.0D) / expressionCost(exp)
  }

  // Formula: (-selectivity) / expression cost
  private def rankingOr(exp: Expression, filterEstimation: Option[FilterEstimation]): Double = {
    -filterSelectivity(exp, filterEstimation) / expressionCost(exp)
  }

  // The cost of a call expression exp is computed as:
  //   cost(exp) = typeSize + functionCost + cost(children).
  private def expressionCost(exp: Expression): Double = exp match {
      case e: Expression if e.children.isEmpty =>
        e.dataType.defaultSize
      case e: IsNull =>
        e.dataType.defaultSize + IS_OP_COST + e.children.map(expressionCost).sum
      case e: IsNotNull =>
        e.dataType.defaultSize + IS_OP_COST + e.children.map(expressionCost).sum
      case e: IsNaN =>
        e.dataType.defaultSize + IS_OP_COST + e.children.map(expressionCost).sum
      case e: Not =>
        e.dataType.defaultSize + IS_OP_COST + e.children.map(expressionCost).sum
      case e @ In(_, list) =>
        e.dataType.defaultSize + BINARY_OP_COST * list.size
      case e @ InSet(_, set) =>
        e.dataType.defaultSize + BINARY_OP_COST * set.size
      case e: MultiLikeBase =>
        e.dataType.defaultSize + LIKE_COST * e.patterns.size
      case e: Cast =>
        e.dataType.defaultSize + CAST_COST + e.children.map(expressionCost).sum
      case e: If =>
        e.dataType.defaultSize + e.children.map(expressionCost).sum
      case e: CaseWhen =>
        e.dataType.defaultSize + e.branches.map(c => expressionCost(c._2)).sum
      case e: UnaryExpression =>
        e.dataType.defaultSize + UNARY_OP_COST + e.children.map(expressionCost).sum
      case e: BinaryOperator =>
        e.dataType.defaultSize + BINARY_OP_COST + e.children.map(expressionCost).sum
      case e: StringRegexExpression =>
        e.dataType.defaultSize + LIKE_COST + e.children.map(expressionCost).sum
      case e: BinaryExpression =>
        e.dataType.defaultSize + BINARY_OP_COST + e.children.map(expressionCost).sum
      case e: TernaryExpression =>
        e.dataType.defaultSize + TERNARY_OP_COST + e.children.map(expressionCost).sum
      case e: QuaternaryExpression =>
        e.dataType.defaultSize + QUATERNARY_OP_COST + e.children.map(expressionCost).sum
      case e: SeptenaryExpression =>
        e.dataType.defaultSize + SEPTENARY_OP_COST + e.children.map(expressionCost).sum
      case e: ComplexTypeMergingExpression =>
        e.dataType.defaultSize + COMPLEX_TYPE_MERGING_OP_COST + e.children.map(expressionCost).sum
      case e: UserDefinedExpression =>
        e.dataType.defaultSize + USER_DEFINED_OP_COST + e.children.map(expressionCost).sum
      case e =>
        e.dataType.defaultSize + UNKNOWN_COST + e.children.map(expressionCost).sum
    }

  // We do not recursively sort all expressions for performance.
  private def reorderPredicates(
      exp: Expression,
      filterEstimation: Option[FilterEstimation]): Expression = {
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
        case f @ Filter(cond, _) =>
          val filterEstimation = if (conf.cboEnabled || conf.planStatsEnabled) {
            Some(FilterEstimation(f))
          } else {
            None
          }
          f.copy(condition = reorderPredicates(cond, filterEstimation))
      }
    } else {
      plan
    }
  }
}
