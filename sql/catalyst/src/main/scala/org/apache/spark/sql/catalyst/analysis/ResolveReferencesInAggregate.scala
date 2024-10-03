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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{AliasHelper, Attribute, Expression, IntegerLiteral, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, AppendColumns, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.TreePattern.{LATERAL_COLUMN_ALIAS_REFERENCE, UNRESOLVED_ATTRIBUTE}
import org.apache.spark.sql.connector.catalog.CatalogManager

/**
 * A virtual rule to resolve [[UnresolvedAttribute]] in [[Aggregate]]. It's only used by the real
 * rule `ResolveReferences`. The column resolution order for [[Aggregate]] is:
 * 1. Resolves the columns to [[AttributeReference]] with the output of the child plan. This
 *    includes metadata columns as well.
 * 2. Resolves the columns to a literal function which is allowed to be invoked without braces, e.g.
 *    `SELECT col, current_date FROM t`.
 * 3. If aggregate expressions are all resolved, resolve GROUP BY alias and GROUP BY ALL.
 * 3.1. If the grouping expressions contain an unresolved column whose name matches an alias in the
 *      SELECT list, resolves that unresolved column to the alias. This is to support SQL pattern
 *      like `SELECT a + b AS c, max(col) FROM t GROUP BY c`.
 * 3.2. If the grouping expressions only have one single unresolved column named 'ALL', expanded it
 *      to include all non-aggregate columns in the SELECT list. This is to support SQL pattern like
 *      `SELECT col1, col2, agg_expr(...) FROM t GROUP BY ALL`.
 * 4. Resolves the columns in aggregate expressions to [[LateralColumnAliasReference]] if
 *    it references the alias defined previously in the SELECT list. The rule
 *    `ResolveLateralColumnAliasReference` will further resolve [[LateralColumnAliasReference]] and
 *    rewrite the plan. This is to support SQL pattern like
 *    `SELECT col1 + 1 AS x, x + 1 AS y, y + 1 AS z FROM t`.
 * 5. Resolves the columns to outer references with the outer plan if we are resolving subquery
 *    expressions.
 */
class ResolveReferencesInAggregate(val catalogManager: CatalogManager) extends SQLConfHelper
  with ColumnResolutionHelper with AliasHelper {

  def apply(a: Aggregate): Aggregate = {
    val planForResolve = a.child match {
      // SPARK-25942: Resolves aggregate expressions with `AppendColumns`'s children, instead of
      // `AppendColumns`, because `AppendColumns`'s serializer might produce conflict attribute
      // names leading to ambiguous references exception.
      case appendColumns: AppendColumns => appendColumns
      case _ => a
    }

    val resolvedGroupExprsBasic = a.groupingExpressions
      .map(resolveExpressionByPlanChildren(_, planForResolve))
    val resolvedAggExprsBasic = a.aggregateExpressions.map(
      resolveExpressionByPlanChildren(_, planForResolve))
    val resolvedAggExprsWithLCA = resolveLateralColumnAlias(resolvedAggExprsBasic)
    val resolvedAggExprsFinal = resolvedAggExprsWithLCA.map(resolveColsLastResort)
      .map(_.asInstanceOf[NamedExpression])
    // `groupingExpressions` may rely on `aggregateExpressions`, due to features like GROUP BY alias
    // and GROUP BY ALL. We only do basic resolution for `groupingExpressions`, and will further
    // resolve it after `aggregateExpressions` are all resolved. Note: the basic resolution is
    // needed as `aggregateExpressions` may rely on `groupingExpressions` as well, for the session
    // window feature. See the rule `SessionWindowing` for more details.
    val resolvedGroupExprs = if (resolvedAggExprsFinal.forall(_.resolved)) {
      val resolved = resolveGroupByAll(
        resolvedAggExprsFinal,
        resolveGroupByAlias(resolvedAggExprsFinal, resolvedGroupExprsBasic)
      ).map(resolveColsLastResort)
      // TODO: currently we don't support LCA in `groupingExpressions` yet.
      if (resolved.exists(_.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE))) {
        throw new AnalysisException(
          errorClass = "UNSUPPORTED_FEATURE.LATERAL_COLUMN_ALIAS_IN_GROUP_BY",
          messageParameters = Map.empty)
      }
      resolved
    } else {
      // Do not resolve columns in grouping expressions to outer references here, as the aggregate
      // expressions are not fully resolved yet and we still have chances to resolve GROUP BY
      // alias/ALL in the next iteration. If aggregate expressions end up as unresolved, we don't
      // need to resolve grouping expressions at all, as `CheckAnalysis` will report error for
      // aggregate expressions first.
      resolvedGroupExprsBasic
    }
    a.copy(
      // The aliases in grouping expressions are useless and will be removed at the end of analysis
      // by the rule `CleanupAliases`. However, some rules need to find the grouping expressions
      // from aggregate expressions during analysis. If we don't remove alias here, then these rules
      // can't find the grouping expressions via `semanticEquals` and the analysis will fail.
      // Example rules: ResolveGroupingAnalytics (See SPARK-31670 for more details) and
      // ResolveLateralColumnAliasReference.
      groupingExpressions = resolvedGroupExprs.map { e =>
        // Only trim the alias if the expression is resolved, as the alias may be needed to resolve
        // the expression, such as `NamePlaceHolder` in `CreateNamedStruct`.
        // Note: this rule will be invoked even if the Aggregate is fully resolved. So alias in
        //       GROUP BY will be removed eventually, by following iterations.
        if (e.resolved) trimAliases(e) else e
      },
      aggregateExpressions = resolvedAggExprsFinal)
  }

  private def resolveGroupByAlias(
      selectList: Seq[NamedExpression],
      groupExprs: Seq[Expression]): Seq[Expression] = {
    assert(selectList.forall(_.resolved))
    if (conf.groupByAliases) {
      val resolvedGroupExprs = groupExprs.map { g =>
        g.transformWithPruning(_.containsPattern(UNRESOLVED_ATTRIBUTE)) {
          case u: UnresolvedAttribute =>
            selectList.find(ne => conf.resolver(ne.name, u.name)).getOrElse(u)
        }
      }
      checkIntegerLiteral(selectList, resolvedGroupExprs)
    } else {
      groupExprs
    }
  }

  private def checkIntegerLiteral(
      selectList: Seq[NamedExpression],
      groupExprs: Seq[Expression]): Seq[Expression] = {
    val containsIntegerLiteral = groupExprs.exists(expr =>
      trimAliases(expr) match {
        case IntegerLiteral(_) => true
        case _ => false
      })
    if (containsIntegerLiteral) {
      val expandedGroupExprs = expandGroupByAll(selectList)
      if (expandedGroupExprs.isEmpty) {
        groupExprs
      } else {
        val map = expandedGroupExprs.get.zipWithIndex.map { case (expr, index) =>
          (expr, index + 1)}.toMap
        groupExprs.map { expr =>
          trimAliases(expr) match {
            // When expression is an integer literal, use an integer literal of the index instead.
            case IntegerLiteral(_) =>
              if (map.contains(expr)) {
                Literal(map.getOrElse(expr, 0))
              } else {
                expr
              }
            case _ => expr
          }
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
        expandedGroupExprs.get.zipWithIndex.map { case (expr, index) =>
          trimAliases(expr) match {
            // HACK ALERT: If the expanded grouping expression is an integer literal, don't use it
            //             but use an integer literal of the index. The reason is we may repeatedly
            //             analyze the plan, and the original integer literal may cause failures
            //             with a later GROUP BY ordinal resolution. GROUP BY constant is
            //             meaningless so whatever value does not matter here.
            case IntegerLiteral(_) =>
              // GROUP BY ordinal uses 1-based index.
              Literal(index + 1)
            case _ => expr
          }
        }
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
    val groupingExprs = selectList.filter(e => !AggregateExpression.containsAggregate(e))
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
    case _: AggregateExpression =>
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
