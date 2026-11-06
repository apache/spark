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

import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  AttributeSet,
  ExpressionSet,
  PipeOperator
}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan}

/**
 * Resolves [[Filter]] node and its condition.
 */
class FilterResolver(resolver: Resolver, expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[Filter, LogicalPlan]
    with ResolvesNameByHiddenOutput
    with ValidatesFilter
    with RetainsOriginalJoinOutput {
  private val scopes: NameScopeStack = resolver.getNameScopes
  private val operatorResolutionContextStack = resolver.getOperatorResolutionContextStack

  /**
   * Resolve [[Filter]] by resolving its child and its condition. If an attribute that is used in
   * the condition is not present in child's output, we need to try and find the attribute in
   * hidden output. If found, update child's output and place a [[Project]] node on top of original
   * [[Filter]] with the original output of a [[Filter]]'s child.
   *
   * See [[ResolvesNameByHiddenOutput]] doc for more context.
   */
  override def resolve(unresolvedFilter: Filter): LogicalPlan = {
    val resolvedChild = resolver.resolve(unresolvedFilter.child)

    // A pipe operator is a name resolution boundary, so names in a pipe `WHERE` are resolved
    // using only the output of the previous pipe step, like in the fixed-point Analyzer. For
    // example, `x` is ambiguous here:
    // {{{ SELECT 1 x |> AGGREGATE SUM(x) AS x GROUP BY x |> WHERE x = 1; }}}
    if (!resolvedChild.isInstanceOf[PipeOperator]) {
      scopes.current.markScopeForHiddenOutputResolution()
    }

    val childReferencedAttributes = expressionResolver.getLastReferencedAttributes

    val partiallyResolvedFilter = unresolvedFilter.copy(child = resolvedChild)
    val resolvedCondition = expressionResolver.resolveExpressionTreeInOperator(
      partiallyResolvedFilter.condition,
      partiallyResolvedFilter
    )

    val referencedAttributes = expressionResolver.getLastReferencedAttributes

    val resolvedFilter = Filter(resolvedCondition, resolvedChild)

    validateFilter(
      invalidExpressions = operatorResolutionContextStack.current.invalidExpressions,
      unresolvedOperator = unresolvedFilter,
      resolvedFilter = resolvedFilter
    )

    val missingAttributes: Seq[Attribute] =
      scopes.current.resolveMissingAttributesByHiddenOutput(referencedAttributes)

    failIfMissingAttributesInsideAndOutsideLambdaFunctions(
      expressions = Seq(resolvedCondition),
      missingAttributes = missingAttributes
    )

    validateMissingAttributesForAggregateChild(
      resolvedFilter = resolvedFilter,
      resolvedChild = resolvedChild,
      missingAttributes = missingAttributes
    )

    val resolvedChildWithMissingAttributes =
      insertMissingExpressions(resolvedChild, missingAttributes)

    val resolvedChildWithMetadataColumns = retainOriginalJoinOutput(
      plan = resolvedChildWithMissingAttributes,
      outputExpressions = Seq.empty,
      scopes = scopes,
      childReferencedAttributes = childReferencedAttributes
    )

    val finalFilter = resolvedFilter.copy(child = resolvedChildWithMetadataColumns)

    retainOriginalOutput(
      operator = finalFilter,
      missingExpressions = missingAttributes,
      scopes = scopes,
      operatorResolutionContextStack = operatorResolutionContextStack
    )
  }

  /**
   * Rejects hidden attributes that a [[Filter]] on top of an [[Aggregate]] references without
   * grouping by them. Such attributes cannot be appended to the aggregate expressions, so they
   * fail with the same missing attribute error that the fixed-point analyzer reports. As in
   * fixed-point, an attribute counts as grouped only if a grouping expression is semantically
   * equal to it. The check is a no-op when the [[Filter]] references no hidden attributes, and
   * otherwise canonicalizes each grouping expression once via an [[ExpressionSet]].
   */
  private def validateMissingAttributesForAggregateChild(
      resolvedFilter: Filter,
      resolvedChild: LogicalPlan,
      missingAttributes: Seq[Attribute]): Unit = resolvedChild match {
    case aggregate: Aggregate if missingAttributes.nonEmpty =>
      val groupingExpressions = ExpressionSet(aggregate.groupingExpressions)
      val nonGroupingAttributes = missingAttributes.filterNot(groupingExpressions.contains)
      if (nonGroupingAttributes.nonEmpty) {
        Resolver.throwMissingAttributesError(
          operator = resolvedFilter,
          missingInput = AttributeSet(nonGroupingAttributes)
        )
      }
    case _ =>
  }
}
