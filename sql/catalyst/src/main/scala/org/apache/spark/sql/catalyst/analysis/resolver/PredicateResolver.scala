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
  AnsiStringPromotionTypeCoercion,
  AnsiTypeCoercion,
  BooleanEqualityTypeCoercion,
  CollationTypeCoercion,
  DecimalPrecisionTypeCoercion,
  DivisionTypeCoercion,
  IntegralDivisionTypeCoercion,
  StringPromotionTypeCoercion,
  TypeCoercion
}
import org.apache.spark.sql.catalyst.expressions.{Expression, Predicate}

/**
 * Resolver class for resolving all [[Predicate]] expressions. Recursively resolves all children
 * and applies selected type coercions to the expression.
 */
class PredicateResolver(
    expressionResolver: ExpressionResolver,
    timezoneAwareExpressionResolver: TimezoneAwareExpressionResolver)
  extends TreeNodeResolver[Predicate, Expression]
  with ResolvesExpressionChildren {

  private val typeCoercionRules = if (conf.ansiEnabled) {
    PredicateResolver.ANSI_TYPE_COERCION_RULES
  } else {
    PredicateResolver.TYPE_COERCION_RULES
  }
  private val typeCoercionResolver =
    new TypeCoercionResolver(timezoneAwareExpressionResolver, typeCoercionRules)

  override def resolve(unresolvedPredicate: Predicate): Expression = {
    val predicateWithResolvedChildren =
      withResolvedChildren(unresolvedPredicate, expressionResolver.resolve)
    typeCoercionResolver.resolve(predicateWithResolvedChildren)
  }
}

object PredicateResolver {
  // Ordering in the list of type coercions should be in sync with the list in [[TypeCoercion]].
  private val TYPE_COERCION_RULES: Seq[Expression => Expression] = Seq(
    CollationTypeCoercion.apply,
    TypeCoercion.InTypeCoercion.apply,
    StringPromotionTypeCoercion.apply,
    DecimalPrecisionTypeCoercion.apply,
    BooleanEqualityTypeCoercion.apply,
    DivisionTypeCoercion.apply,
    IntegralDivisionTypeCoercion.apply,
    TypeCoercion.ImplicitTypeCoercion.apply,
    TypeCoercion.DateTimeOperationsTypeCoercion.apply
  )

  // Ordering in the list of type coercions should be in sync with the list in [[AnsiTypeCoercion]].
  private val ANSI_TYPE_COERCION_RULES: Seq[Expression => Expression] = Seq(
    CollationTypeCoercion.apply,
    AnsiTypeCoercion.InTypeCoercion.apply,
    AnsiStringPromotionTypeCoercion.apply,
    DecimalPrecisionTypeCoercion.apply,
    DivisionTypeCoercion.apply,
    IntegralDivisionTypeCoercion.apply,
    AnsiTypeCoercion.ImplicitTypeCoercion.apply,
    AnsiTypeCoercion.AnsiDateTimeOperationsTypeCoercion.apply
  )
}
