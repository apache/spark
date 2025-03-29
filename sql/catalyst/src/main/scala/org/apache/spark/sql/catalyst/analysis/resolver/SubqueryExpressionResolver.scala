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

import org.apache.spark.sql.catalyst.analysis.{UnresolvedInlineTable, ValidateSubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference,
  Exists,
  Expression,
  InSubquery,
  ListQuery,
  ScalarSubquery,
  SubExprUtils,
  SubqueryExpression
}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * [[SubqueryExpressionResolver]] resolves specific [[SubqueryExpression]]s, such as
 * [[ScalarSubquery]], [[ListQuery]], and [[Exists]].
 */
class SubqueryExpressionResolver(expressionResolver: ExpressionResolver, resolver: Resolver) {
  private val scopes = resolver.getNameScopes
  private val traversals = expressionResolver.getExpressionTreeTraversals
  private val cteRegistry = resolver.getCteRegistry
  private val subqueryRegistry = resolver.getSubqueryRegistry
  private val expressionIdAssigner = expressionResolver.getExpressionIdAssigner
  private val typeCoercionResolver = expressionResolver.getGenericTypeCoercionResolver

  /**
   * Resolve [[ScalarSubquery]]:
   *  - Resolve the subquery plan;
   *  - Get outer references;
   *  - Type coerce it;
   *  - Validate it.
   */
  def resolveScalarSubquery(unresolvedScalarSubquery: ScalarSubquery): Expression = {
    traversals.current.parentOperator match {
      case unresolvedInlineTable: UnresolvedInlineTable =>
        throw QueryCompilationErrors.inlineTableContainsScalarSubquery(unresolvedInlineTable)
      case _ =>
    }

    val resolvedSubqueryExpressionPlan = resolveSubqueryExpressionPlan(
      unresolvedScalarSubquery.plan
    )

    val resolvedScalarSubquery = unresolvedScalarSubquery.copy(
      plan = resolvedSubqueryExpressionPlan.plan,
      outerAttrs = resolvedSubqueryExpressionPlan.outerExpressions
    )

    val coercedScalarSubquery =
      typeCoercionResolver.resolve(resolvedScalarSubquery).asInstanceOf[ScalarSubquery]

    validateSubqueryExpression(coercedScalarSubquery)

    coercedScalarSubquery
  }

  /**
   * Resolve [[InSubquery]]:
   *  - Resolve the underlying [[ListQuery]];
   *  - Resolve the values;
   *  - Type coerce it;
   *  - Validate it.
   */
  def resolveInSubquery(unresolvedInSubquery: InSubquery): Expression = {
    val resolvedQuery =
      expressionResolver.resolve(unresolvedInSubquery.query).asInstanceOf[ListQuery]

    val resolvedValues = unresolvedInSubquery.values.map { value =>
      expressionResolver.resolve(value)
    }

    val resolvedInSubquery =
      unresolvedInSubquery.copy(values = resolvedValues, query = resolvedQuery)

    val coercedInSubquery =
      typeCoercionResolver.resolve(resolvedInSubquery).asInstanceOf[InSubquery]

    validateSubqueryExpression(coercedInSubquery.query)

    coercedInSubquery
  }

  /**
   * Resolve [[ListSubquery]], which is always a child of the [[InSubquery]].
   */
  def resolveListQuery(unresolvedListQuery: ListQuery): Expression = {
    val resolvedSubqueryExpressionPlan = resolveSubqueryExpressionPlan(unresolvedListQuery.plan)

    unresolvedListQuery.copy(
      plan = resolvedSubqueryExpressionPlan.plan,
      outerAttrs = resolvedSubqueryExpressionPlan.outerExpressions,
      numCols = resolvedSubqueryExpressionPlan.output.size
    )
  }

  /**
   * Resolve [[Exists]] subquery:
   *  - Resolve the subquery plan;
   *  - Get outer references;
   *  - Type coerce it;
   *  - Validate it.
   */
  def resolveExists(unresolvedExists: Exists): Expression = {
    val resolvedSubqueryExpressionPlan = resolveSubqueryExpressionPlan(unresolvedExists.plan)

    val resolvedExists = unresolvedExists.copy(
      plan = resolvedSubqueryExpressionPlan.plan,
      outerAttrs = resolvedSubqueryExpressionPlan.outerExpressions
    )

    val coercedExists = typeCoercionResolver.resolve(resolvedExists).asInstanceOf[Exists]

    validateSubqueryExpression(coercedExists)

    coercedExists
  }

  /**
   * Resolve [[SubqueryExpression]] plan. Subquery expressions require:
   *  - Fresh [[NameScope]] to isolate the name resolution;
   *  - Fresh [[ExpressionIdAssigner]] mapping, because it's a separate plan branch;
   *  - Fresh [[CteScope]] with the root flag set so that [[CTERelationDefs]] are merged
   *    under the subquery root.
   *  - Fresh [[SubqueryRegistry]] scope to isolate the subquery expression plan resolution.
   */
  private def resolveSubqueryExpressionPlan(
      unresolvedSubqueryPlan: LogicalPlan): ResolvedSubqueryExpressionPlan = {
    val resolvedSubqueryExpressionPlan = scopes.withNewScope(isSubqueryRoot = true) {
      expressionIdAssigner.withNewMapping(isSubqueryRoot = true) {
        cteRegistry.withNewScope(isRoot = true) {
          subqueryRegistry.withNewScope() {
            val resolvedPlan = resolver.resolve(unresolvedSubqueryPlan)

            ResolvedSubqueryExpressionPlan(
              plan = resolvedPlan,
              output = scopes.current.output,
              outerExpressions = getOuterExpressions(resolvedPlan)
            )
          }
        }
      }
    }

    for (expression <- resolvedSubqueryExpressionPlan.outerExpressions) {
      expressionResolver.validateExpressionUnderSupportedOperator(expression)
    }

    resolvedSubqueryExpressionPlan
  }

  /**
   * Get outer expressions from the subquery plan. These are the expressions that are actual
   * [[AttributeReference]]s or [[AggregateExpression]]s with outer references in the subtree.
   * [[AggregateExpressionResolver]] strips out the outer aggregate expression subtrees, but
   * for [[SubqueryExpression.outerAttrs]] to be well-formed, we need to put those back. After
   * that we validate the outer expressions.
   *
   * We reuse [[SubExprUtils.getOuterReferences]] for the top-down traversal to stay compatible
   * with the fixed-point Analyzer, because the order of [[SubqueryExpression.outerAttrs]] is a
   * part of an implicit alias name for that expression.
   */
  private def getOuterExpressions(subQuery: LogicalPlan): Seq[Expression] = {
    val subqueryScope = subqueryRegistry.currentScope

    SubExprUtils.getOuterReferences(subQuery).map {
      case attribute: AttributeReference =>
        subqueryScope.getOuterAggregateExpression(attribute.exprId).getOrElse(attribute)
      case other =>
        other
    }
  }

  /**
   * Generically validates [[SubqueryExpression]]. Should not be done in the main pass, because the
   * logic is too sophisticated.
   */
  private def validateSubqueryExpression(subqueryExpression: SubqueryExpression): Unit = {
    ValidateSubqueryExpression(traversals.current.parentOperator, subqueryExpression)
  }
}
