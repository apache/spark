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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.UnresolvedOrdinal
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Resolves [[UnresolvedOrdinal]] to an expression from aggregate/project list, if possible.
 */
class OrdinalResolver(expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[UnresolvedOrdinal, Expression] {
  private val scopes = expressionResolver.getNameScopes

  /**
   * Resolves [[UnresolvedOrdinal]] to an expression from aggregate/project list, if enabled by a
   * conf. Candidates expressions are retrieved from current [[NameScope]], where they should have
   * previously been set by either [[AggregateResolver]] or [[SortResolver]].
   */
  override def resolve(unresolvedOrdinal: UnresolvedOrdinal): Expression =
    scopes.current.getOrdinalReplacementExpressions match {
      case Some(resolvedAggregateExpressions: OrdinalReplacementGroupingExpressions)
          if conf.groupByOrdinal =>
        replaceOrdinalInGroupingExpressions(unresolvedOrdinal.ordinal, resolvedAggregateExpressions)
      case Some(sortOrderCandidates: OrdinalReplacementSortOrderExpressions)
          if conf.orderByOrdinal =>
        replaceOrdinalInSortOrderExpressions(unresolvedOrdinal.ordinal, sortOrderCandidates)
      case _ =>
        throw SparkException.internalError("Cannot resolve UnresolvedOrdinal in single-pass.")
    }

  /**
   * Replaces the ordinals with the actual expressions from the resolved aggregate expression list
   * or throws if any of aggregate expression are irregular. Handles following invalid cases:
   *  - ordinal value is greater than output list size.
   *  - ordinal value refers to aggregate function.
   *  - star in aggregate expressions when grouping by ordinal.
   *
   * If the resulting expression is an [[Alias]], return its child.
   *
   * For example, for the query:
   *
   * {{{ SELECT col1 + 1, col1, col2 AS a FROM VALUES(1, 2) GROUP BY 2, col2, 3; }}}
   *
   * It would replace `2` with the `col1` and `3` with `col2` so the final grouping expression list
   * would be: [col1, col2, col2].
   */
  private def replaceOrdinalInGroupingExpressions(
      ordinal: Int,
      ordinalReplacementGroupingExpressions: OrdinalReplacementGroupingExpressions) = {
    if (ordinal > ordinalReplacementGroupingExpressions.expressions.length || ordinal < 1) {
      throw QueryCompilationErrors.groupByPositionRangeError(
        ordinal,
        ordinalReplacementGroupingExpressions.expressions.size
      )
    }

    if (ordinalReplacementGroupingExpressions.expressionIndexesWithAggregateFunctions.contains(
        ordinal - 1
      )) {
      throw QueryCompilationErrors.groupByPositionRefersToAggregateFunctionError(
        ordinal,
        ordinalReplacementGroupingExpressions.expressions(ordinal - 1)
      )
    }

    if (ordinalReplacementGroupingExpressions.hasStar) {
      throw QueryCompilationErrors.starNotAllowedWhenGroupByOrdinalPositionUsedError()
    }

    ordinalReplacementGroupingExpressions.expressions(ordinal - 1) match {
      case alias: Alias =>
        alias.child
      case other => other
    }
  }

  /**
   * Replaces the ordinals with the actual expressions from the resolved project list or throws if
   * ordinal value is greater than project list size. Do this only in case `conf.orderByOrdinal`
   * value is `true`.
   *
   * Example 1:
   * {{{
   * -- This one would order by `col1` and `col2 + 1`
   * SELECT col1, col2 + 1 FROM VALUES(1, 2) ORDER BY col1, 2;
   * }}}
   *
   * Example 2:
   * {{{
   * -- This one would throw `ORDER_BY_POS_OUT_OF_RANGE` error
   * SELECT col1 FROM VALUES(1, 2) ORDER BY 2;
   * }}}
   */
  private def replaceOrdinalInSortOrderExpressions(
      ordinal: Int,
      ordinalReplacementSortOrderExpressions: OrdinalReplacementSortOrderExpressions) = {
    if (ordinal > ordinalReplacementSortOrderExpressions.expressions.size || ordinal < 1) {
      throw QueryCompilationErrors.orderByPositionRangeError(
        ordinal,
        ordinalReplacementSortOrderExpressions.expressions.size,
        ordinalReplacementSortOrderExpressions.unresolvedSort
      )
    }
    ordinalReplacementSortOrderExpressions.expressions(ordinal - 1).toAttribute
  }
}
