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

import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.expressions.{Expression, SortOrder}
import org.apache.spark.sql.catalyst.planning.IntegerIndex
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin

/**
 * Replaces ordinal in 'order by' or 'group by' with UnresolvedOrdinal expression.
 */
class UnresolvedOrdinalSubstitution(conf: CatalystConf) extends Rule[LogicalPlan] {
  private def isIntegerLiteral(sorter: Expression) = IntegerIndex.unapply(sorter).nonEmpty

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case s @ Sort(orders, global, child) if conf.orderByOrdinal &&
      orders.exists(o => isIntegerLiteral(o.child)) =>
      val newOrders = orders.map {
        case order @ SortOrder(ordinal @ IntegerIndex(index: Int), _) =>
          val newOrdinal = withOrigin(ordinal.origin)(UnresolvedOrdinal(index))
          withOrigin(order.origin)(order.copy(child = newOrdinal))
        case other => other
      }
      withOrigin(s.origin)(s.copy(order = newOrders))
    case a @ Aggregate(groups, aggs, child) if conf.groupByOrdinal &&
      groups.exists(isIntegerLiteral(_)) =>
      val newGroups = groups.map {
        case ordinal @ IntegerIndex(index) =>
          withOrigin(ordinal.origin)(UnresolvedOrdinal(index))
        case other => other
      }
      withOrigin(a.origin)(a.copy(groupingExpressions = newGroups))
  }
}
