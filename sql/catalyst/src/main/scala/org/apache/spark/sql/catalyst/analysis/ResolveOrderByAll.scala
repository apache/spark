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

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{SORT, UNRESOLVED_ATTRIBUTE}

/**
 * Resolve "order by all" in the following SQL pattern:
 *  `select col1, col2 from table order by all`.
 *
 * It orders the query result by all columns, from left to right. The query above becomes:
 *
 *  `select col1, col2 from table order by col1, col2`
 *
 * This should also support specifying asc/desc, and nulls first/last.
 */
object ResolveOrderByAll extends Rule[LogicalPlan] {

  val ALL = "ALL"

  /**
   * An extractor to pull out the SortOrder field in the ORDER BY ALL clause. We pull out that
   * SortOrder object so we can pass its direction and null ordering.
   */
  object OrderByAll {
    def unapply(s: Sort): Option[SortOrder] = {
      // This only applies to global ordering.
      if (!s.global) {
        return None
      }
      // Don't do this if we have more than one order field. That means it's not order by all.
      if (s.order.size != 1) {
        return None
      }
      // Don't do this if there's a child field called ALL. That should take precedence.
      if (s.child.output.exists(_.name.toUpperCase() == ALL)) {
        return None
      }

      s.order.find { so =>
        so.child match {
          case a: UnresolvedAttribute => a.name.toUpperCase() == ALL
          case _ => false
        }
      }
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
    _.containsAllPatterns(UNRESOLVED_ATTRIBUTE, SORT), ruleId) {
    // This only makes sense if the child is resolved.
    case s: Sort if s.child.resolved =>
      s match {
        case OrderByAll(sortOrder) =>
          // Replace a single order by all with N fields, where N = child's output, while
          // retaining the same asc/desc and nulls ordering.
          val order = s.child.output.map(a => sortOrder.copy(child = a))
          s.copy(order = order)
        case _ =>
          s
      }
  }
}
