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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.{AnsiTypeCoercion, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.{ConditionalExpression, Expression}

/**
 * Resolver for [[If]], [[CaseWhen]] and [[Coalesce]] expressions.
 */
class ConditionalExpressionResolver(
    expressionResolver: ExpressionResolver,
    timezoneAwareExpressionResolver: TimezoneAwareExpressionResolver)
  extends TreeNodeResolver[ConditionalExpression, Expression]
  with ResolvesExpressionChildren
  with SQLConfHelper {

  private val typeCoercionRules: Seq[Expression => Expression] =
    if (conf.ansiEnabled) {
      ConditionalExpressionResolver.ANSI_TYPE_COERCION_RULES
    } else {
      ConditionalExpressionResolver.TYPE_COERCION_RULES
    }
  private val typeCoercionResolver: TypeCoercionResolver =
    new TypeCoercionResolver(timezoneAwareExpressionResolver, typeCoercionRules)

  override def resolve(unresolvedConditionalExpression: ConditionalExpression): Expression = {
    val conditionalExpressionWithResolvedChildren =
      withResolvedChildren(unresolvedConditionalExpression, expressionResolver.resolve)

    typeCoercionResolver.resolve(conditionalExpressionWithResolvedChildren)
  }
}

object ConditionalExpressionResolver {
  // Ordering in the list of type coercions should be in sync with the list in [[TypeCoercion]].
  private val TYPE_COERCION_RULES: Seq[Expression => Expression] = Seq(
    TypeCoercion.CaseWhenTypeCoercion.apply,
    TypeCoercion.FunctionArgumentTypeCoercion.apply,
    TypeCoercion.IfTypeCoercion.apply
  )

  // Ordering in the list of type coercions should be in sync with the list in [[AnsiTypeCoercion]].
  private val ANSI_TYPE_COERCION_RULES: Seq[Expression => Expression] = Seq(
    AnsiTypeCoercion.CaseWhenTypeCoercion.apply,
    AnsiTypeCoercion.FunctionArgumentTypeCoercion.apply,
    AnsiTypeCoercion.IfTypeCoercion.apply
  )
}
