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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, GetStructField, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, AppendColumns, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_ATTRIBUTE

/**
 * A virtual rule to resolve [[UnresolvedAttribute]] in [[Aggregate]]. It's only used by the real
 * rule `ResolveReferences`. The column resolution order for [[Aggregate]] is:
 * 1. Resolves the column to [[AttributeReference]] with the output of the child plan. This
 *    includes metadata columns as well.
 * 2. Resolves the column to a literal function which is allowed to be invoked without braces, e.g.
 *    `SELECT col, current_date FROM t`.
 * 3. If `Aggregate.aggregateExpressions` are all resolved, resolve GROUP BY alias and GROUP BY ALL
 *    for `Aggregate.groupingExpressions`:
 * 3.1. If the grouping expressions contain an unresolved column whose name matches an alias in the
 *      SELECT list, resolves that unresolved column to the alias. This is to support SQL pattern
 *      like `SELECT a + b AS c, max(col) FROM t GROUP BY c`.
 * 3.2. If the grouping expressions only have one single unresolved column named 'ALL', expanded it
 *      to include all non-aggregate columns in the SELECT list. This is to support SQL pattern like
 *      `SELECT col1, col2, agg_expr(...) FROM t GROUP BY ALL`.
 * 4. Resolves the column in `Aggregate.aggregateExpressions` to [[LateralColumnAliasReference]] if
 *    it references the alias defined previously in the SELECT list. The rule
 *    `ResolveLateralColumnAliasReference` will further resolve [[LateralColumnAliasReference]] and
 *    rewrite the plan. This is to support SQL pattern like
 *    `SELECT col1 + 1 AS x, x + 1 AS y, y + 1 AS z FROM t`.
 * 5. Resolves the column to outer references with the outer plan if we are resolving subquery
 *    expressions.
 */
object ResolveReferencesInAggregate extends SQLConfHelper with ColumnResolutionHelper {
  def apply(a: Aggregate): Aggregate = {
    val planForResolve = a.child match {
      // SPARK-25942: Resolves aggregate expressions with `AppendColumns`'s children, instead of
      // `AppendColumns`, because `AppendColumns`'s serializer might produce conflict attribute
      // names leading to ambiguous references exception.
      case appendColumns: AppendColumns => appendColumns
      case _ => a
    }

    val resolvedAggExprsNoOuter = a.aggregateExpressions
      .map(resolveExpressionByPlanChildren(_, planForResolve, allowOuter = false)
        .asInstanceOf[NamedExpression])

    val resolvedGroupingExprs = a.groupingExpressions
      .map(resolveExpressionByPlanChildren(_, planForResolve, allowOuter = false))
      // SPARK-31670: Resolve Struct field in groupByExpressions and aggregateExpressions
      // with CUBE/ROLLUP will be wrapped with alias like Alias(GetStructField, name) with
      // different ExprId. This cause aggregateExpressions can't be replaced by expanded
      // groupByExpressions in `ResolveGroupingAnalytics.constructAggregateExprs()`, we trim
      // unnecessary alias of GetStructField here.
      .map(trimTopLevelGetStructFieldAlias)

    // Only makes sense to do the rewrite once all the aggregate expressions have been resolved.
    // Otherwise, we might incorrectly pull an actual aggregate expression over to the grouping
    // expression list (because we don't know they would be aggregate expressions until resolved).
    if (resolvedAggExprsNoOuter.forall(_.resolved)) {
      val finalGroupExprs = resolveGroupByAll(
        resolvedAggExprsNoOuter,
        resolveGroupByAlias(resolvedAggExprsNoOuter, resolvedGroupingExprs)
      ).map(resolveOuterRef)
      a.copy(finalGroupExprs, resolvedAggExprsNoOuter, a.child)
    } else {
      // If the SELECT list is not full resolved at this point, we need to apply lateral column
      // alias and outer reference resolution, which are not supported in GROUP BY. We can't
      // resolve group by alias and group by all here.
      // Aggregate supports Lateral column alias, which has higher priority than outer reference.
      val resolvedAggExprsWithLCA = resolveLateralColumnAlias(resolvedAggExprsNoOuter)
      val resolvedAggExprsWithOuter = resolvedAggExprsWithLCA.map(resolveOuterRef)
        .map(_.asInstanceOf[NamedExpression])
      a.copy(resolvedGroupingExprs.map(resolveOuterRef), resolvedAggExprsWithOuter, a.child)
    }
  }

  private def resolveGroupByAlias(
      selectList: Seq[NamedExpression],
      groupExprs: Seq[Expression]): Seq[Expression] = {
    assert(selectList.forall(_.resolved))
    if (conf.groupByAliases) {
      groupExprs.map { g =>
        g.transformWithPruning(_.containsPattern(UNRESOLVED_ATTRIBUTE)) {
          case u: UnresolvedAttribute =>
            selectList.find(ne => conf.resolver(ne.name, u.name)).getOrElse(u)
        }
      }
    } else {
      groupExprs
    }
  }

  private def resolveGroupByAll(
      selectList: Seq[NamedExpression],
      groupExprs: Seq[Expression]): Seq[Expression] = {
    assert(selectList.forall(_.resolved))
    if (isGroupByAll(groupExprs)) {
      val expandedGroupExprs = expandGroupByAll(selectList)
      if (expandedGroupExprs.isEmpty) {
        // Don't replace the ALL when we fail to infer the grouping columns. We will eventually
        // tell the user in checkAnalysis that we cannot resolve the all in group by.
        groupExprs
      } else {
        // This is a valid GROUP BY ALL aggregate.
        expandedGroupExprs.get
      }
    } else {
      groupExprs
    }
  }

  /**
   * Returns all the grouping expressions inferred from a GROUP BY ALL aggregate.
   * The result is optional. If Spark fails to infer the grouping columns, it is None.
   * Otherwise, it contains all the non-aggregate expressions from the project list of the input
   * Aggregate.
   */
  private def expandGroupByAll(selectList: Seq[NamedExpression]): Option[Seq[Expression]] = {
    val groupingExprs = selectList.filter(!_.exists(AggregateExpression.isAggregate))
    // If the grouping exprs are empty, this could either be (1) a valid global aggregate, or
    // (2) we simply fail to infer the grouping columns. As an example, in "i + sum(j)", we will
    // not automatically infer the grouping column to be "i".
    if (groupingExprs.isEmpty && selectList.exists(containsAttribute)) {
      None
    } else {
      Some(groupingExprs)
    }
  }

  /**
   * Trim groupByExpression's top-level GetStructField Alias. Since these expressions are not
   * NamedExpression originally, we are safe to trim top-level GetStructField Alias.
   */
  private def trimTopLevelGetStructFieldAlias(e: Expression): Expression = {
    e match {
      case Alias(s: GetStructField, _) => s
      case other => other
    }
  }

  /**
   * Returns true iff this is a GROUP BY ALL: the grouping expressions only have a single column,
   * which is an unresolved column named ALL.
   */
  private def isGroupByAll(exprs: Seq[Expression]): Boolean = {
    if (exprs.length != 1) return false
    exprs.head match {
      case a: UnresolvedAttribute => a.equalsIgnoreCase("ALL")
      case _ => false
    }
  }

  /**
   * Returns true if the expression includes an Attribute outside the aggregate expression part.
   * For example:
   *  "i" -> true
   *  "i + 2" -> true
   *  "i + sum(j)" -> true
   *  "sum(j)" -> false
   *  "sum(j) / 2" -> false
   */
  private def containsAttribute(expr: Expression): Boolean = expr match {
    case _ if AggregateExpression.isAggregate(expr) =>
      // Don't recurse into AggregateExpressions
      false
    case _: Attribute =>
      true
    case e =>
      e.children.exists(containsAttribute)
  }

  /**
   * A check to be used in [[CheckAnalysis]] to see if we have any unresolved group by at the
   * end of analysis, so we can tell users that we fail to infer the grouping columns.
   */
  def checkUnresolvedGroupByAll(operator: LogicalPlan): Unit = operator match {
    case a: Aggregate if a.aggregateExpressions.forall(_.resolved) &&
        isGroupByAll(a.groupingExpressions) =>
      if (expandGroupByAll(a.aggregateExpressions).isEmpty) {
        operator.failAnalysis(
          errorClass = "UNRESOLVED_ALL_IN_GROUP_BY",
          messageParameters = Map.empty)
      }
    case _ =>
  }
}
