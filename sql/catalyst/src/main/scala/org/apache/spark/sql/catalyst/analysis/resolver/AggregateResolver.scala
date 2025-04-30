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

import java.util.{HashSet, LinkedHashMap}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.analysis.{
  withPosition,
  AnalysisErrorAt,
  NondeterministicExpressionCollection,
  UnresolvedAttribute
}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  AttributeReference,
  Expression,
  ExprId,
  ExprUtils,
  IntegerLiteral,
  Literal,
  NamedExpression
}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Project}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.IntegerType

/**
 * Resolves an [[Aggregate]] by resolving its child, aggregate expressions and grouping
 * expressions. Updates the [[NameScopeStack]] with its output and performs validation
 * related to [[Aggregate]] resolution.
 */
class AggregateResolver(operatorResolver: Resolver, expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[Aggregate, Aggregate] {
  private val scopes = operatorResolver.getNameScopes

  /**
   * Resolve [[Aggregate]] operator.
   *
   * 1. Resolve the child (inline table).
   * 2. Resolve aggregate expressions using [[ExpressionResolver.resolveAggregateExpressions]].
   * 3. If there's just one [[UnresolvedAttribute]] with a single-part name "ALL", expand it using
   *    aggregate expressions which don't contain aggregate functions. There should not exist a
   *    column with that name in the lower operator's output, otherwise it takes precedence.
   * 4. Resolve grouping expressions using [[ExpressionResolver.resolveGroupingExpressions]]. This
   *    includes alias references to aggregate expressions, which is done in
   *    [[NameScope.resolveMultipartName]].
   * 5. Substitute ordinals with aggregate expressions in appropriate places.
   * 6. Substitute non-deterministic expressions with derived attribute references to an
   *    artificial [[Project]] list.
   *
   * At the end of resolution we validate the [[Aggregate]] using the
   * [[ExprUtils.assertValidAggregation]], update the `scopes` with the output of [[Aggregate]] and
   * return the result.
   */
  def resolve(unresolvedAggregate: Aggregate): Aggregate = {
    val resolvedAggregate = scopes.withNewScope() {
      val resolvedChild = operatorResolver.resolve(unresolvedAggregate.child)

      val resolvedAggregateExpressions = expressionResolver.resolveAggregateExpressions(
        unresolvedAggregate.aggregateExpressions,
        unresolvedAggregate
      )

      val resolvedGroupingExpressions =
        if (canGroupByAll(unresolvedAggregate.groupingExpressions)) {
          tryResolveGroupByAll(
            resolvedAggregateExpressions,
            unresolvedAggregate
          )
        } else {
          val partiallyResolvedGroupingExpressions = expressionResolver.resolveGroupingExpressions(
            unresolvedAggregate.groupingExpressions,
            unresolvedAggregate
          )
          withPosition(unresolvedAggregate) {
            tryReplaceOrdinalsInGroupingExpressions(
              partiallyResolvedGroupingExpressions,
              resolvedAggregateExpressions
            )
          }
        }

      val resolvedAggregate = unresolvedAggregate.copy(
        groupingExpressions = resolvedGroupingExpressions,
        aggregateExpressions = resolvedAggregateExpressions.expressions,
        child = resolvedChild
      )

      tryPullOutNondeterministic(resolvedAggregate)
    }

    // TODO: This validation function does a post-traversal. This is discouraged in single-pass
    //       Analyzer.
    ExprUtils.assertValidAggregation(resolvedAggregate)

    scopes.overwriteOutputAndExtendHiddenOutput(
      output = resolvedAggregate.aggregateExpressions.map(_.toAttribute),
      groupingAttributeIds = Some(getGroupingAttributeIds(resolvedAggregate))
    )

    resolvedAggregate
  }

  /**
   * Replaces the ordinals with the actual expressions from the resolved aggregate expression list
   * or throws if any of aggregate expression are irregular.
   * There are three cases:
   *  - If the aggregate expression referenced by the ordinal is a [[Literal]] with the Integer
   *    data type - preserve the ordinal literal in order to pass logical plan comparison.
   *  - If [[SQLConf.groupByOrdinal]] flag is set to false, treat the grouping expression as
   *    a [[Literal]] instead of ordinal.
   *  - If aggregate expression is an [[Alias]] return [[Alias.child]].
   *  - Otherwise, replace the ordinal with the aggregate expression.
   * Remove all the leftover [[Alias]]es at the end of resolution.
   * For the query:
   *
   * {{{ SELECT col1 + 1, col1, col2 FROM VALUES(1, 2) GROUP BY 2, col2, 3; }}}
   *
   * It would replace `2` with the `col1` and `3` with `col2` so the final grouping expression list
   * would be: [col1, col2, col2].
   *
   * In case of having an integer [[Literal]] in the aggregate expressions which is referenced by an
   * ordinal, example and final grouping expression list are the following:
   *  - Example:
   *    {{{ SELECT col1 + 1, col1, 10 FROM VALUES(1, 2) GROUP BY 2, col2, 3; }}}
   *  - Grouping expressions:
   *    [col1, col2, 3] // we preserve the ordinal instead of replacing it.
   */
  private def tryReplaceOrdinalsInGroupingExpressions(
      groupingExpressions: Seq[Expression],
      aggregateExpressions: ResolvedAggregateExpressions): Seq[Expression] = {
    val aggregateExpressionsArray = aggregateExpressions.expressions.toArray
    val groupByOrdinal = conf.groupByOrdinal
    groupingExpressions.map { expression =>
      val maybeGroupByOrdinal = if (groupByOrdinal) {
        tryReplaceOrdinalInGroupingExpression(
          expression,
          aggregateExpressionsArray,
          aggregateExpressions
        )
      } else {
        expression
      }
      maybeGroupByOrdinal match {
        case alias: Alias =>
          alias.child
        case other => other
      }
    }
  }

  private def tryReplaceOrdinalInGroupingExpression(
      groupingExpression: Expression,
      aggregateExpressionsArray: Array[NamedExpression],
      resolvedAggregateExpressions: ResolvedAggregateExpressions): Expression = {
    TryExtractOrdinal(groupingExpression) match {
      case Some(ordinal) =>
        if (ordinal > aggregateExpressionsArray.length) {
          throw QueryCompilationErrors.groupByPositionRangeError(
            ordinal,
            aggregateExpressionsArray.length
          )
        }

        if (resolvedAggregateExpressions.hasStar) {
          throw QueryCompilationErrors.starNotAllowedWhenGroupByOrdinalPositionUsedError()
        }

        if (resolvedAggregateExpressions.expressionIndexesWithAggregateFunctions
            .contains(ordinal - 1)) {
          throw QueryCompilationErrors.groupByPositionRefersToAggregateFunctionError(
            ordinal,
            aggregateExpressionsArray(ordinal - 1)
          )
        }

        val resolvedOrdinalAggregateExpression =
          aggregateExpressionsArray(ordinal - 1) match {
            case alias: Alias =>
              alias.child
            case other => other
          }

        resolvedOrdinalAggregateExpression match {
          case Literal(_: Int, IntegerType) =>
            Literal(ordinal)
          case other => other
        }
      case None => groupingExpression
    }
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
   * {{{ SELECT col1, 5 FROM VALUES(1) GROUP BY ALL; }}}
   * this one should be grouped by keyword `ALL`. If there is an aggregate expression which is a
   * [[Literal]] with the Integer data type - preserve the ordinal literal in order to pass logical
   * plan comparison. The grouping expressions list will be [col1, 2].
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

    aggregateExpressions.resolvedExpressionsWithoutAggregates.zipWithIndex.map {
      case (expression, index) =>
        expression match {
          case IntegerLiteral(_) =>
            Literal(index + 1)
          case _ => expression
        }
    }
  }

  /**
   * In case there are non-deterministic expressions in either `groupingExpressions` or
   * `aggregateExpressions` replace them with attributes created out of corresponding
   * non-deterministic expression. Example:
   *
   * {{{ SELECT RAND() GROUP BY 1; }}}
   *
   * This query would have the following analyzed plan:
   *   Aggregate(
   *     groupingExpressions = [AttributeReference(_nonDeterministic)]
   *     aggregateExpressions = [Alias(AttributeReference(_nonDeterministic), `rand()`)]
   *     child = Project(
   *               projectList = [Alias(Rand(...), `_nondeterministic`)]
   *               child = OneRowRelation
   *             )
   *   )
   */
  private def tryPullOutNondeterministic(aggregate: Aggregate): Aggregate = {
    val nondeterministicToAttributes: LinkedHashMap[Expression, NamedExpression] =
      NondeterministicExpressionCollection.getNondeterministicToAttributes(
        aggregate.groupingExpressions
      )

    if (!nondeterministicToAttributes.isEmpty) {
      val newChild = Project(
        scopes.current.output ++ nondeterministicToAttributes.values.asScala.toSeq,
        aggregate.child
      )
      val resolvedAggregateExpressions = aggregate.aggregateExpressions.map { expression =>
        PullOutNondeterministicExpressionInExpressionTree(expression, nondeterministicToAttributes)
      }
      val resolvedGroupingExpressions = aggregate.groupingExpressions.map { expression =>
        PullOutNondeterministicExpressionInExpressionTree(
          expression,
          nondeterministicToAttributes
        )
      }
      aggregate.copy(
        groupingExpressions = resolvedGroupingExpressions,
        aggregateExpressions = resolvedAggregateExpressions,
        child = newChild
      )
    } else {
      aggregate
    }
  }

  private def canGroupByAll(expressions: Seq[Expression]): Boolean = {
    val isOrderByAll = expressions match {
      case Seq(unresolvedAttribute: UnresolvedAttribute) =>
        unresolvedAttribute.equalsIgnoreCase("ALL")
      case _ => false
    }
    isOrderByAll && scopes.current
      .resolveMultipartName(Seq("ALL"))
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
