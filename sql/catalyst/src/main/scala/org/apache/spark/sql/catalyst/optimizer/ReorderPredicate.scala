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

import org.apache.spark.sql.catalyst.expressions.{And, CaseWhen, DynamicPruningSubquery, Expression, InSet, LikeAnyBase, PredicateHelper, UserDefinedExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * A rule that reorder predicate expressions to get better performance.
 */
object ReorderPredicate extends Rule[LogicalPlan] with PredicateHelper {
  // Get priority from a Expression. Expressions with higher priority are executed in preference
  // to expressions with lower priority.
  private def getPriority(exp: Expression): Int = exp match {
    case e: Expression if !e.deterministic => 1
    case e: Expression if e.find(_.isInstanceOf[DynamicPruningSubquery]).isDefined => 2
    case e: Expression if e.find(_.isInstanceOf[LikeAnyBase]).isDefined => 3
    case e: Expression if e.find(_.isInstanceOf[CaseWhen]).isDefined => 4
    case e: Expression if e.find(_.isInstanceOf[InSet]).isDefined => 5
    case e: Expression if e.find(_.isInstanceOf[UserDefinedExpression]).isDefined => 6
    case _ => 7
  }

  private def reorderConditional(e: Expression): Expression = splitConjunctivePredicates(e) match {
    case Seq(cond) =>
      cond
    case other =>
      other.sortWith(getPriority(_) > getPriority(_)).map(reorderConditional).reduceLeft(And)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case f @ Filter(cond, _) => f.copy(condition = reorderConditional(cond))
  }
}
