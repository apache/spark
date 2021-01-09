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

import org.apache.spark.sql.catalyst.expressions.{And, CaseWhen, Expression, MultiLikeBase, Or, PredicateHelper, SubqueryExpression, UserDefinedExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.FilterEstimation
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * A rule that reorder predicate expressions to get better performance.
 */
object PredicateReorder extends Rule[LogicalPlan] with PredicateHelper {
  // Get priority from a Expression. Expressions with higher priority are executed in preference
  // to expressions with lower priority.
  private def getPriority(exp: Expression, filterEstimation: Option[FilterEstimation]) = exp match {
    case e: Expression if e.find(_.isInstanceOf[SubqueryExpression]).isDefined => 1.0
    case e: Expression if e.find(_.isInstanceOf[UserDefinedExpression]).isDefined => 2.0
    case e: Expression if e.find(_.isInstanceOf[MultiLikeBase]).isDefined ||
      e.find(_.isInstanceOf[CaseWhen]).isDefined =>
      val maxSize = e.collect {
        case m: MultiLikeBase => m.patterns.length
        case c: CaseWhen => c.branches.size
      }.max
      3.0 + (1.0 / maxSize)
    case e =>
      filterEstimation.flatMap(_.calculateFilterSelectivity(e, false).map(5.0 - _)).getOrElse(4.0)
  }

  private def reorderPredicates(e: Expression, estimation: Option[FilterEstimation]): Expression = {
    e match {
      case _: Or =>
        splitDisjunctivePredicates(e)
          .map(reorderPredicates(_, estimation))
          .reduceLeft(Or)
      case _: And =>
        splitConjunctivePredicates(e)
          .map(e => (e, getPriority(e, estimation))).sortWith(_._2 > _._2).map(_._1)
          .reduceLeft(And)
      case _ => e
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.predicateReorder) {
      plan
    } else {
      plan transform {
        case f @ Filter(cond, _) =>
          val filterEstimation = if (conf.cboEnabled) Some(FilterEstimation(f)) else None
          f.copy(condition = reorderPredicates(cond, filterEstimation))
      }
    }
  }
}
