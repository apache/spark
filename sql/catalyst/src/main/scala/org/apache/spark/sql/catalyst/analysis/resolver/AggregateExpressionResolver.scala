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

import org.apache.spark.sql.catalyst.analysis.{
  AnalysisErrorAt,
  AnsiTypeCoercion,
  CollationTypeCoercion,
  TypeCoercion
}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Project}

/**
 * A resolver for [[AggregateExpression]]s which are introduced while resolving an
 * [[UnresolvedFunction]]. It is responsible for the following:
 *  - Handling of the exceptions related to [[AggregateExpressions]].
 *  - Updating the [[ExpressionResolver.expressionResolutionContextStack]].
 *  - Applying type coercion rules to the [[AggregateExpressions]]s children. This is the only
 *    resolution that we apply here as we already resolved the children of [[AggregateExpression]]
 *    in the [[FunctionResolver]].
 */
class AggregateExpressionResolver(
    expressionResolver: ExpressionResolver,
    timezoneAwareExpressionResolver: TimezoneAwareExpressionResolver)
    extends TreeNodeResolver[AggregateExpression, Expression]
    with ResolvesExpressionChildren {
  private val typeCoercionTransformations: Seq[Expression => Expression] =
    if (conf.ansiEnabled) {
      AggregateExpressionResolver.ANSI_TYPE_COERCION_TRANSFORMATIONS
    } else {
      AggregateExpressionResolver.TYPE_COERCION_TRANSFORMATIONS
    }

  private val typeCoercionResolver: TypeCoercionResolver =
    new TypeCoercionResolver(timezoneAwareExpressionResolver, typeCoercionTransformations)

  private val expressionResolutionContextStack =
    expressionResolver.getExpressionResolutionContextStack

  /**
   * Resolves the given [[AggregateExpression]] by applying:
   *   - Type coercion rules
   *   - Validity checks. Those include:
   *     - Whether the [[AggregateExpression]] is under a valid operator.
   *     - Whether there is a nested [[AggregateExpression]].
   *     - Whether there is a nondeterministic child.
   *   - Updates to the [[ExpressionResolver.expressionResolutionContextStack]]
   */
  override def resolve(aggregateExpression: AggregateExpression): Expression = {
    val aggregateExpressionWithTypeCoercion =
      withResolvedChildren(aggregateExpression, typeCoercionResolver.resolve)

    throwIfNotUnderValidOperator(aggregateExpression)
    throwIfNestedAggregateExists(aggregateExpressionWithTypeCoercion)
    throwIfHasNondeterministicChildren(aggregateExpressionWithTypeCoercion)

    expressionResolutionContextStack
      .peek()
      .hasAggregateExpressionsInASubtree = true

    // There are two different cases that we handle regarding the value of the flag:
    //
    //   - We have an attribute under an `AggregateExpression`:
    //       {{{ SELECT COUNT(col1) FROM VALUES (1); }}}
    //     In this case, value of the `hasAttributeInASubtree` flag should be `false` as it
    //     indicates whether there is an attribute in the subtree that's not `AggregateExpression`
    //     so we can throw the `MISSING_GROUP_BY` exception appropriately.
    //
    //   - In the following example:
    //       {{{ SELECT COUNT(*), col1 + 1 FROM VALUES (1); }}}
    //     It would be `true` as described above.
    expressionResolutionContextStack.peek().hasAttributeInASubtree = false

    aggregateExpressionWithTypeCoercion
  }

  private def throwIfNotUnderValidOperator(aggregateExpression: AggregateExpression): Unit = {
    expressionResolver.getParentOperator.get match {
      case _: Aggregate | _: Project =>
      case filter: Filter =>
        filter.failAnalysis(
          errorClass = "INVALID_WHERE_CONDITION",
          messageParameters = Map(
            "condition" -> toSQLExpr(filter.condition),
            "expressionList" -> Seq(aggregateExpression).mkString(", ")
          )
        )
      case other =>
        other.failAnalysis(
          errorClass = "UNSUPPORTED_EXPR_FOR_OPERATOR",
          messageParameters = Map(
            "invalidExprSqls" -> Seq(aggregateExpression).mkString(", ")
          )
        )
    }
  }

  private def throwIfNestedAggregateExists(aggregateExpression: AggregateExpression): Unit = {
    if (expressionResolutionContextStack
        .peek()
        .hasAggregateExpressionsInASubtree) {
      aggregateExpression.failAnalysis(
        errorClass = "NESTED_AGGREGATE_FUNCTION",
        messageParameters = Map.empty
      )
    }
  }

  private def throwIfHasNondeterministicChildren(aggregateExpression: AggregateExpression): Unit = {
    aggregateExpression.aggregateFunction.children.foreach(child => {
      if (!child.deterministic) {
        child.failAnalysis(
          errorClass = "AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION",
          messageParameters = Map("sqlExpr" -> toSQLExpr(aggregateExpression))
        )
      }
    })
  }
}

object AggregateExpressionResolver {
  // Ordering in the list of type coercions should be in sync with the list in [[TypeCoercion]].
  private val TYPE_COERCION_TRANSFORMATIONS: Seq[Expression => Expression] = Seq(
    CollationTypeCoercion.apply,
    TypeCoercion.InTypeCoercion.apply,
    TypeCoercion.FunctionArgumentTypeCoercion.apply,
    TypeCoercion.IfTypeCoercion.apply,
    TypeCoercion.ImplicitTypeCoercion.apply
  )

  // Ordering in the list of type coercions should be in sync with the list in [[AnsiTypeCoercion]].
  private val ANSI_TYPE_COERCION_TRANSFORMATIONS: Seq[Expression => Expression] = Seq(
    CollationTypeCoercion.apply,
    AnsiTypeCoercion.InTypeCoercion.apply,
    AnsiTypeCoercion.FunctionArgumentTypeCoercion.apply,
    AnsiTypeCoercion.IfTypeCoercion.apply,
    AnsiTypeCoercion.ImplicitTypeCoercion.apply
  )
}
