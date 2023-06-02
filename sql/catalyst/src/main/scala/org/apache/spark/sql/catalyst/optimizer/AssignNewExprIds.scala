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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, ExprId, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Reassigns expression IDs in every expression of the given LogicalPlan (including subqueries
 * contained in the plan).
 */
object AssignNewExprIds extends Rule[LogicalPlan] {

  private def assignNewExprIdsinExpr(input: Expression,
                                     reassignedExprs: mutable.HashMap[ExprId, Attribute]):
  Expression = input match {
    case a: Attribute =>
      val newAttribute = reassignedExprs.getOrElse(a.exprId,
        a.withExprId(NamedExpression.newExprId))
      reassignedExprs.put(a.exprId, newAttribute)
      reassignedExprs.put(newAttribute.exprId, newAttribute)
      newAttribute
    case a: Alias =>
      val newAlias = Alias(a.child, a.name)(NamedExpression.newExprId,
        a.qualifier, a.explicitMetadata, a.nonInheritableMetadataKeys)
      reassignedExprs.put(a.exprId, newAlias.toAttribute)
      reassignedExprs.put(newAlias.exprId, newAlias.toAttribute)
      newAlias
    case a: AggregateExpression =>
      if (a.mode == PartialMerge || a.mode == Partial) {
        // Partial aggregation's attributes are going to be reused in the final aggregations.
        // In order to avoid renaming attributes of final aggregations, keep the intermediate
        // attributes as is.
        reassignedExprs.put(a.resultAttribute.exprId, a.resultAttribute)
        return a
      }
      val newResultId = NamedExpression.newExprId
      val updatedExpression = a.copy(resultId = newResultId)
      reassignedExprs.put(newResultId, updatedExpression.resultAttribute)
      reassignedExprs.put(a.resultId, updatedExpression.resultAttribute)
      updatedExpression
    case p: Expression => p
  }

  private def transformUpAllExpressions(plan: LogicalPlan,
                                        rule: PartialFunction[Expression, Expression]):
  LogicalPlan = {
    plan.transformUpWithSubqueries {
      case q => q.transformExpressionsUp(rule)
    }
  }

  private def assignNewExprIds(plan: LogicalPlan,
                               reassignedExprs: mutable.HashMap[ExprId, Attribute]):
  LogicalPlan = {
    transformUpAllExpressions(plan, {
      case e: Expression => assignNewExprIdsinExpr(e, reassignedExprs)
    })
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    val reassignedExprs = mutable.HashMap.empty[ExprId, Attribute]
    assignNewExprIds(plan, reassignedExprs)
  }
}
