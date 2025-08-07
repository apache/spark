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

package org.apache.spark.sql.catalyst.analysis.resolver

import java.util.HashSet

import org.apache.spark.sql.catalyst.analysis.{AnalysisErrorAt, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  AliasHelper,
  AttributeReference,
  Expression,
  ExprId,
  ExprUtils
}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}

/**
 * Resolves an [[Aggregate]] by resolving its child, aggregate expressions and grouping
 * expressions. Updates the [[NameScopeStack]] with its output and performs validation
 * related to [[Aggregate]] resolution.
 */
class AggregateResolver(operatorResolver: Resolver, expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[Aggregate, LogicalPlan]
    with AliasHelper {
  private val scopes = operatorResolver.getNameScopes
  private val lcaResolver = expressionResolver.getLcaResolver

  /**
   * Resolve [[Aggregate]] operator.
   *
   * 1. Resolve the child (inline table).
   * 2. Clear [[NameScope.availableAliases]]. Those are only relevant for the immediate aggregate
   *    expressions for output prioritization to work correctly in
   *    [[NameScope.tryResolveMultipartNameByOutput]].
   * 3. Resolve aggregate expressions using [[ExpressionResolver.resolveAggregateExpressions]] and
   *    set [[NameScope.ordinalReplacementExpressions]] for grouping expressions resolution.
   * 4. If there's just one [[UnresolvedAttribute]] with a single-part name "ALL", expand it using
   *    aggregate expressions which don't contain aggregate functions. There should not exist a
   *    column with that name in the lower operator's output, otherwise it takes precedence.
   * 5. Resolve grouping expressions using [[ExpressionResolver.resolveGroupingExpressions]]. This
   *    includes alias references to aggregate expressions, which is done in
   *    [[NameScope.resolveMultipartName]] and replacing [[UnresolvedOrdinals]] with corresponding
   *    expressions from aggregate list, done in [[OrdinalResolver]].
   * 6. Remove all the unnecessary [[Alias]]es from the grouping (all the aliases) and aggregate
   *    (keep the outermost one) expressions. This is needed to stay compatible with the
   *    fixed-point implementation. For example:
   *
   *    {{{ SELECT timestamp(col1:str) FROM VALUES('a') GROUP BY timestamp(col1:str); }}}
   *
   *    Here we end up having inner [[Alias]]es in both the grouping and aggregate expressions
   *    lists which are uncomparable because they have different expression IDs (thus we have to
   *    strip them).
   *
   * If the resulting [[Aggregate]] contains lateral columns references, delegate the resolution of
   * these columns to [[LateralColumnAliasResolver.handleLcaInAggregate]]. Otherwise, validate the
   * [[Aggregate]] using the [[ExprUtils.assertValidAggregation]], update the `scopes` with the
   * output of [[Aggregate]] and return the result.
   */
  def resolve(unresolvedAggregate: Aggregate): LogicalPlan = {
    scopes.pushScope()

    val resolvedAggregate = try {
      val resolvedChild = operatorResolver.resolve(unresolvedAggregate.child)

      scopes.current.availableAliases.clear()

      val resolvedAggregateExpressions = expressionResolver.resolveAggregateExpressions(
        unresolvedAggregate.aggregateExpressions,
        unresolvedAggregate
      )

      scopes.current.setOrdinalReplacementExpressions(
        OrdinalReplacementGroupingExpressions(
          expressions = resolvedAggregateExpressions.expressions.toIndexedSeq,
          hasStar = resolvedAggregateExpressions.hasStar,
          expressionIndexesWithAggregateFunctions =
            resolvedAggregateExpressions.expressionIndexesWithAggregateFunctions
        )
      )

      val resolvedGroupingExpressions =
        if (canGroupByAll(unresolvedAggregate.groupingExpressions)) {
          tryResolveGroupByAll(
            resolvedAggregateExpressions,
            unresolvedAggregate
          )
        } else {
          expressionResolver.resolveGroupingExpressions(
            unresolvedAggregate.groupingExpressions,
            unresolvedAggregate
          )
        }

      val resolvedGroupingExpressionsWithoutAliases = resolvedGroupingExpressions.map(trimAliases)
      val resolvedAggregateExpressionsWithoutAliases =
        resolvedAggregateExpressions.expressions.map(trimNonTopLevelAliases)

      val resolvedAggregate = unresolvedAggregate.copy(
        groupingExpressions = resolvedGroupingExpressionsWithoutAliases,
        aggregateExpressions = resolvedAggregateExpressionsWithoutAliases,
        child = resolvedChild
      )

      if (resolvedAggregateExpressions.hasLateralColumnAlias) {
        val aggregateWithLcaResolutionResult = lcaResolver.handleLcaInAggregate(resolvedAggregate)
        AggregateResolutionResult(
          operator = aggregateWithLcaResolutionResult.resolvedOperator,
          outputList = aggregateWithLcaResolutionResult.outputList,
          groupingAttributeIds =
            getGroupingAttributeIds(aggregateWithLcaResolutionResult.baseAggregate),
          aggregateListAliases = aggregateWithLcaResolutionResult.aggregateListAliases,
          baseAggregate = aggregateWithLcaResolutionResult.baseAggregate
        )
      } else {
        // TODO: This validation function does a post-traversal. This is discouraged in single-pass
        //       Analyzer.
        ExprUtils.assertValidAggregation(resolvedAggregate)

        AggregateResolutionResult(
          operator = resolvedAggregate,
          outputList = resolvedAggregate.aggregateExpressions,
          groupingAttributeIds = getGroupingAttributeIds(resolvedAggregate),
          aggregateListAliases = scopes.current.getTopAggregateExpressionAliases,
          baseAggregate = resolvedAggregate
        )
      }
    } finally {
      scopes.popScope()
    }

    scopes.overwriteOutputAndExtendHiddenOutput(
      output = resolvedAggregate.outputList.map(_.toAttribute),
      groupingAttributeIds = Some(resolvedAggregate.groupingAttributeIds),
      aggregateListAliases = resolvedAggregate.aggregateListAliases,
      baseAggregate = Some(resolvedAggregate.baseAggregate)
    )

    resolvedAggregate.operator
  }

  /**
   * Resolve `GROUP BY ALL`.
   *
   * Examples below show which queries should be resolved with `tryResolveGroupByAll` and which
   * should be resolved generically (using the [[ExpressionResolver.resolveGroupingExpressions]]):
   *
   * Example 1:
   *
   * {{{
   * -- Table `table_1` has a column `all`.
   * SELECT * from table_1 GROUP BY all;
   * }}}
   * this one should be grouped by the column `all`.
   *
   * Example 2:
   *
   * {{{
   * -- Table `table_2` doesn't have a column `all`.
   * SELECT * from table_2 GROUP BY all;
   * }}}
   * this one should be grouped by all the columns from `table_1`.
   *
   * Example 3:
   *
   * {{{
   * -- Table `table_3` doesn't have a column `all` and there other grouping expressions.
   * SELECT * from table_3 GROUP BY all, column;
   * }}}
   * this one should be grouped by column `all` which doesn't exist so `UNRESOLVED_COLUMN`
   * exception is thrown.
   *
   * Example 4:
   *
   * {{{ SELECT col1, col2 + 1, COUNT(col1 + 1) FROM VALUES(1, 2) GROUP BY ALL; }}}
   * this one should be grouped by keyword `ALL`. It means that the grouping expressions list is
   * going to contain all the aggregate expressions that don't have aggregate expressions in their
   * subtrees. The grouping expressions list will be [col1, col2 + 1], and COUNT(col1 + 1) won't be
   * included, being an [[AggregateExpression]].
   *
   * Example 5:
   *
   * {{{ SELECT col1 AS b, sum(col2) + col1 FROM VALUES (1, 2) GROUP BY ALL; }}}
   * this one should be grouped by keyword `ALL`. It means that the grouping expressions list is
   * going to contain all the aggregate expressions that don't have aggregate expressions in their
   * subtrees. The grouping expressions list will be [col1 AS `col1`].
   * All the [[Alias]]es should be stripped in order to pass logical plan comparison and to prevent
   * unintentional exceptions from being thrown by [[ExprUtils.assertValidAggregation]], so the
   * final grouping expressions list will be [col1].
   */
  private def tryResolveGroupByAll(
      aggregateExpressions: ResolvedAggregateExpressions,
      aggregate: Aggregate): Seq[Expression] = {
    if (aggregateExpressions.resolvedExpressionsWithoutAggregates.isEmpty &&
      aggregateExpressions.hasAttributeOutsideOfAggregateExpressions) {
      aggregate.failAnalysis(
        errorClass = "UNRESOLVED_ALL_IN_GROUP_BY",
        messageParameters = Map.empty
      )
    }

    aggregateExpressions.resolvedExpressionsWithoutAggregates.map {
      case alias: Alias =>
        alias.child
      case other => other
    }
  }

  private def canGroupByAll(expressions: Seq[Expression]): Boolean = {
    val isOrderByAll = expressions match {
      case Seq(unresolvedAttribute: UnresolvedAttribute) =>
        unresolvedAttribute.equalsIgnoreCase("ALL")
      case _ => false
    }
    isOrderByAll && scopes.current
      .resolveMultipartName(
        Seq("ALL"),
        canReferenceAggregateExpressionAliases = true
      )
      .candidates
      .isEmpty
  }

  private def getGroupingAttributeIds(aggregate: Aggregate): HashSet[ExprId] = {
    val groupingAttributeIds = new HashSet[ExprId](aggregate.groupingExpressions.size)
    aggregate.groupingExpressions.foreach { rootExpression =>
      rootExpression.foreach {
        case attribute: AttributeReference =>
          groupingAttributeIds.add(attribute.exprId)
        case _ =>
      }
    }

    groupingAttributeIds
  }
}
