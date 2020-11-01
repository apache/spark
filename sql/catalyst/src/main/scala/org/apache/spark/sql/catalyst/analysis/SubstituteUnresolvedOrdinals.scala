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

import org.apache.spark.sql.catalyst.expressions.{Cube, Expression, Literal, Rollup, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, GroupingSets, LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin
import org.apache.spark.sql.types.IntegerType

/**
 * Replaces ordinal in 'order by' or 'group by' with UnresolvedOrdinal expression.
 */
object SubstituteUnresolvedOrdinals extends Rule[LogicalPlan] {
  private def containIntLiteral(e: Expression): Boolean = e match {
    case Literal(_, IntegerType) => true
    case Cube(groupByExprs) => groupByExprs.exists(containIntLiteral)
    case Rollup(groupByExprs) => groupByExprs.exists(containIntLiteral)
    case _ => false
  }

  private def resolveOrdinal(expression: Expression): Expression = expression match {
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
        case cube @ Cube(groupByExprs) =>
          withOrigin(cube.origin)(cube.copy(groupByExprs = groupByExprs.map(resolveOrdinal)))
        case rollup @ Rollup(groupByExprs) =>
          withOrigin(rollup.origin)(rollup.copy(groupByExprs = groupByExprs.map(resolveOrdinal)))
        case other => other
      }
      withOrigin(a.origin)(a.copy(groupingExpressions = newGroups))

    case gs: GroupingSets if conf.orderByOrdinal &&
      (gs.selectedGroupByExprs.exists(_.exists(containIntLiteral)) ||
        gs.groupByExprs.exists(containIntLiteral)) =>
      withOrigin(gs.origin)(gs.copy(
        selectedGroupByExprs = gs.selectedGroupByExprs.map(_.map(resolveOrdinal)),
        groupByExprs = gs.groupByExprs.map(resolveOrdinal)))
  }
}
