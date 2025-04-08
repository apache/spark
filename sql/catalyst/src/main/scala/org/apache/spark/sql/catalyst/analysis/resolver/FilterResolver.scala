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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.withPosition
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.types.BooleanType

/**
 * Resolves [[Filter]] node and its condition.
 */
class FilterResolver(resolver: Resolver, expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[Filter, LogicalPlan]
    with ResolvesNameByHiddenOutput {
  override protected val scopes: NameScopeStack = resolver.getNameScopes

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

    val partiallyResolvedFilter = unresolvedFilter.copy(child = resolvedChild)
    val resolvedCondition = expressionResolver.resolveExpressionTreeInOperator(
      partiallyResolvedFilter.condition,
      partiallyResolvedFilter
    )

    val referencedAttributes = expressionResolver.getLastReferencedAttributes

    val resolvedFilter = Filter(resolvedCondition, resolvedChild)

    checkValidFilter(unresolvedFilter, resolvedFilter)

    val missingAttributes: Seq[Attribute] =
      scopes.current.resolveMissingAttributesByHiddenOutput(referencedAttributes)
    val resolvedChildWithMissingAttributes =
      insertMissingExpressions(resolvedChild, missingAttributes)
    val finalFilter = resolvedFilter.copy(child = resolvedChildWithMissingAttributes)

    retainOriginalOutput(finalFilter, missingAttributes)
  }

  private def checkValidFilter(unresolvedFilter: Filter, resolvedFilter: Filter): Unit = {
    withPosition(unresolvedFilter) {
      val invalidExpressions = expressionResolver.getLastInvalidExpressionsInTheContextOfOperator
      if (invalidExpressions.nonEmpty) {
        throwInvalidWhereCondition(resolvedFilter, invalidExpressions)
      }

      if (resolvedFilter.condition.dataType != BooleanType) {
        throwDataTypeMismatchFilterNotBoolean(resolvedFilter)
      }
    }
  }

  private def throwInvalidWhereCondition(
      filter: Filter,
      invalidExpressions: Seq[Expression]): Nothing = {
    throw new AnalysisException(
      errorClass = "INVALID_WHERE_CONDITION",
      messageParameters = Map(
        "condition" -> toSQLExpr(filter.condition),
        "expressionList" -> invalidExpressions.map(_.sql).mkString(", ")
      )
    )
  }

  private def throwDataTypeMismatchFilterNotBoolean(filter: Filter): Nothing =
    throw new AnalysisException(
      errorClass = "DATATYPE_MISMATCH.FILTER_NOT_BOOLEAN",
      messageParameters = Map(
        "sqlExpr" -> makeCommaSeparatedExpressionString(filter.expressions),
        "filter" -> toSQLExpr(filter.condition),
        "type" -> toSQLType(filter.condition.dataType)
      )
    )

  private def makeCommaSeparatedExpressionString(expressions: Seq[Expression]): String = {
    expressions.map(toSQLExpr).mkString(", ")
  }
}
