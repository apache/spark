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

import org.apache.spark.sql.catalyst.expressions.{Cube, Expression, GroupingSet, GroupingSets, Literal, Rollup, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin
import org.apache.spark.sql.types.IntegerType

/**
 * Replaces ordinal in 'order by' or 'group by' with UnresolvedOrdinal expression.
 */
object SubstituteUnresolvedOrdinals extends Rule[LogicalPlan] {
  private def containIntLiteral(e: Expression): Boolean = e match {
    case Literal(_, IntegerType) => true
    case gs: GroupingSet => gs.children.exists(containIntLiteral)
    case _ => false
  }

  private def substituteUnresolvedOrdinal(expression: Expression): Expression = expression match {
    case ordinal @ Literal(index: Int, IntegerType) =>
      withOrigin(ordinal.origin)(UnresolvedOrdinal(index))
    case e => e
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case s: Sort if conf.orderByOrdinal && s.order.exists(o => containIntLiteral(o.child)) =>
      val newOrders = s.order.map {
        case order @ SortOrder(ordinal @ Literal(index: Int, IntegerType), _, _, _) =>
          val newOrdinal = withOrigin(ordinal.origin)(UnresolvedOrdinal(index))
          withOrigin(order.origin)(order.copy(child = newOrdinal))
        case other => other
      }
      withOrigin(s.origin)(s.copy(order = newOrders))

    case a: Aggregate if conf.groupByOrdinal && a.groupingExpressions.exists(containIntLiteral) =>
      val newGroups = a.groupingExpressions.map {
        case ordinal @ Literal(index: Int, IntegerType) =>
          withOrigin(ordinal.origin)(UnresolvedOrdinal(index))
        case cube @ Cube(_, children) =>
          withOrigin(cube.origin)(cube.withNewChildren(children.map(substituteUnresolvedOrdinal)))
        case rollup @ Rollup(_, children) =>
          withOrigin(rollup.origin)(
            rollup.withNewChildren(children.map(substituteUnresolvedOrdinal)))
        case groupingSets @ GroupingSets(_, flatGroupingSets, userGivenGroupByExprs) =>
          withOrigin(groupingSets.origin)(groupingSets.withNewChildren(
            flatGroupingSets.map(substituteUnresolvedOrdinal)
              ++ userGivenGroupByExprs.map(substituteUnresolvedOrdinal)))
        case other => other
      }
      withOrigin(a.origin)(a.copy(groupingExpressions = newGroups))
  }
}
