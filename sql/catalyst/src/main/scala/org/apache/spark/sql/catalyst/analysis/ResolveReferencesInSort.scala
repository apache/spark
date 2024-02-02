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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan, Project, Sort}
import org.apache.spark.sql.connector.catalog.CatalogManager

/**
 * A virtual rule to resolve [[UnresolvedAttribute]] in [[Sort]]. It's only used by the real
 * rule `ResolveReferences`. The column resolution order for [[Sort]] is:
 * 1. Resolves the column to [[AttributeReference]] with the output of the child plan. This
 *    includes metadata columns as well.
 * 2. Resolves the column to a literal function which is allowed to be invoked without braces, e.g.
 *    `SELECT col, current_date FROM t`.
 * 3. If the child plan is Aggregate or Filter(_, Aggregate), resolves the column to
 *    [[TempResolvedColumn]] with the output of Aggregate's child plan.
 *    This is to allow Sort to host grouping expressions and aggregate functions, which can
 *    be pushed down to the Aggregate later. For example,
 *    `SELECT max(a) FROM t GROUP BY b HAVING max(a) > 1 ORDER BY min(a)`.
 * 4. Resolves the column to [[AttributeReference]] with the output of a descendant plan node.
 *    Spark will propagate the missing attributes from the descendant plan node to the Sort node.
 *    This is to allow users to ORDER BY columns that are not in the SELECT clause, which is
 *    widely supported in other SQL dialects. For example, `SELECT a FROM t ORDER BY b`.
 * 5. If the order by expressions only have one single unresolved column named ALL, expanded it to
 *    include all columns in the SELECT list. This is to support SQL pattern like
 *    `SELECT col1, col2 FROM t ORDER BY ALL`. This should also support specifying asc/desc, and
 *    nulls first/last.
 * 6. Resolves the column to outer references with the outer plan if we are resolving subquery
 *    expressions.
 *
 * Note, 3 and 4 are actually orthogonal. If the child plan is Aggregate, 4 can only resolve columns
 * as the grouping columns, which is completely covered by 3.
 */
class ResolveReferencesInSort(val catalogManager: CatalogManager)
  extends SQLConfHelper with ColumnResolutionHelper {

  def apply(s: Sort): LogicalPlan = {
    val resolvedBasic = s.order.map(resolveExpressionByPlanOutput(_, s.child))
    val resolvedWithAgg = s.child match {
      case Filter(_, agg: Aggregate) => resolvedBasic.map(resolveColWithAgg(_, agg))
      case _ => resolvedBasic.map(resolveColWithAgg(_, s.child))
    }
    val (missingAttrResolved, newChild) = resolveExprsAndAddMissingAttrs(resolvedWithAgg, s.child)
    val orderByAllResolved = resolveOrderByAll(
      s.global, newChild, missingAttrResolved.map(_.asInstanceOf[SortOrder]))
    val resolvedFinal = orderByAllResolved
      .map(e => resolveColsLastResort(e).asInstanceOf[SortOrder])
    if (s.child.output == newChild.output) {
      s.copy(order = resolvedFinal)
    } else {
      // Add missing attributes and then project them away.
      val newSort = s.copy(order = resolvedFinal, child = newChild)
      Project(s.child.output, newSort)
    }
  }

  private def resolveOrderByAll(
      globalSort: Boolean,
      child: LogicalPlan,
      orders: Seq[SortOrder]): Seq[SortOrder] = {
    // This only applies to global ordering.
    if (!globalSort) return orders
    // Don't do this if we have more than one order field. That means it's not order by all.
    if (orders.length != 1) return orders

    val order = orders.head
    order.child match {
      case a: UnresolvedAttribute if a.equalsIgnoreCase("ALL") =>
        // Replace a single order by all with N fields, where N = child's output, while
        // retaining the same asc/desc and nulls ordering.
        child.output.map(a => order.copy(child = a))
      case _ => orders
    }
  }
}
