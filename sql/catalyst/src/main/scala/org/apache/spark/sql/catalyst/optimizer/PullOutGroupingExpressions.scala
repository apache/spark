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

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.AGGREGATE

/**
 * This rule ensures that [[Aggregate]] nodes doesn't contain complex grouping expressions in the
 * optimization phase.
 *
 * Complex grouping expressions are pulled out to a [[Project]] node under [[Aggregate]] and are
 * referenced in both grouping expressions and aggregate expressions without aggregate functions.
 * These references ensure that optimization rules don't change the aggregate expressions to invalid
 * ones that no longer refer to any grouping expressions and also simplify the expression
 * transformations on the node (need to transform the expression only once).
 *
 * For example, in the following query Spark shouldn't optimize the aggregate expression
 * `Not(IsNull(c))` to `IsNotNull(c)` as the grouping expression is `IsNull(c)`:
 * SELECT not(c IS NULL)
 * FROM t
 * GROUP BY c IS NULL
 * Instead, the aggregate expression references a `_groupingexpression` attribute:
 * Aggregate [_groupingexpression#233], [NOT _groupingexpression#233 AS (NOT (c IS NULL))#230]
 * +- Project [isnull(c#219) AS _groupingexpression#233]
 *    +- LocalRelation [c#219]
 */
object PullOutGroupingExpressions extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformWithPruning(_.containsPattern(AGGREGATE)) {
      case a: Aggregate if a.resolved =>
        val complexGroupingExpressionMap = mutable.LinkedHashMap.empty[Expression, NamedExpression]
        val newGroupingExpressions = a.groupingExpressions.toIndexedSeq.map {
          case e if !e.foldable && e.children.nonEmpty =>
            complexGroupingExpressionMap
              .getOrElseUpdate(e.canonicalized, Alias(e, "_groupingexpression")())
              .toAttribute
          case o => o
        }
        if (complexGroupingExpressionMap.nonEmpty) {
          def replaceComplexGroupingExpressions(e: Expression): Expression = {
            e match {
              case _: AggregateExpression => e
              case _ if e.foldable => e
              case _ if complexGroupingExpressionMap.contains(e.canonicalized) =>
                complexGroupingExpressionMap.get(e.canonicalized).map(_.toAttribute).getOrElse(e)
              case _ => e.mapChildren(replaceComplexGroupingExpressions)
            }
          }

          val newAggregateExpressions = a.aggregateExpressions
            .map(replaceComplexGroupingExpressions(_).asInstanceOf[NamedExpression])
          val newChild = Project(a.child.output ++ complexGroupingExpressionMap.values, a.child)
          Aggregate(newGroupingExpressions, newAggregateExpressions, newChild)
        } else {
          a
        }
    }
  }
}
