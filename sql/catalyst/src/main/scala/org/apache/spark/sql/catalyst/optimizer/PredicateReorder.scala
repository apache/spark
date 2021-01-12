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

import org.apache.spark.sql.catalyst.expressions.{And, Expression, Or, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.FilterEstimation
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * A rule that reorder predicate expressions to get better performance.
 */
object PredicateReorder extends Rule[LogicalPlan] with PredicateHelper {
  // Get priority from a Expression. Expressions with higher priority are executed in preference
  // to expressions with lower priority.
  private def getPriority(exp: Expression, filterEstimation: FilterEstimation) = {
    filterEstimation.calculateFilterSelectivity(exp, false).map(1.0 - _).getOrElse(0.0)
  }

  private def reorderPredicates(exp: Expression, filterEstimation: FilterEstimation): Expression = {
    exp match {
      case _: Or =>
        splitDisjunctivePredicates(exp)
          .map(reorderPredicates(_, filterEstimation))
          .reduceLeft(Or)
      case _: And =>
        splitConjunctivePredicates(exp)
          .map(e => (e, getPriority(e, filterEstimation))).sortWith(_._2 > _._2).map(_._1)
          .reduceLeft(And)
      case _ => exp
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.cboEnabled && conf.predicateReorder) {
      plan transform {
        case f @ Filter(cond, _) => f.copy(condition = reorderPredicates(cond, FilterEstimation(f)))
      }
    } else {
      plan
    }
  }
}
