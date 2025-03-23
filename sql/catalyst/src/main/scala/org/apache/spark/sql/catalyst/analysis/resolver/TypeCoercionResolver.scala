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
  AnsiGetDateFieldOperationsTypeCoercion,
  AnsiStringPromotionTypeCoercion,
  AnsiTypeCoercion,
  BooleanEqualityTypeCoercion,
  CollationTypeCoercion,
  DecimalPrecisionTypeCoercion,
  DivisionTypeCoercion,
  IntegralDivisionTypeCoercion,
  StackTypeCoercion,
  StringLiteralTypeCoercion,
  StringPromotionTypeCoercion,
  TypeCoercion
}
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}

/**
 * [[TypeCoercionResolver]] is used by other resolvers to uniformly apply type coercions to all
 * expressions.
 */
class TypeCoercionResolver(
    timezoneAwareExpressionResolver: TimezoneAwareExpressionResolver,
    typeCoercionTransformations: Seq[Expression => Expression] = Seq.empty)
  extends TreeNodeResolver[Expression, Expression] {

  private val typeCoercionTransformationsOrDefault = if (typeCoercionTransformations.nonEmpty) {
    typeCoercionTransformations
  } else {
    if (conf.ansiEnabled) {
      TypeCoercionResolver.ANSI_TYPE_COERCION_TRANSFORMATIONS
    } else {
      TypeCoercionResolver.TYPE_COERCION_TRANSFORMATIONS
    }
  }

  /**
   * Resolves type coercion for expression by applying necessary transformations on the expression
   * and its children. Because fixed-point sometimes resolves type coercion in multiple passes, we
   * apply each provided transformation twice, cyclically, to ensure that types are resolved. For
   * example in a query like:
   *
   * {{{ SELECT '1' + '1' }}}
   *
   * fixed-point analyzer requires two passes to resolve types.
   */
  override def resolve(expression: Expression): Expression = {
    val withTypeCoercedOnce = applyTypeCoercion(expression)
    // This is a hack necessary because fixed-point analyzer sometimes requires multiple passes to
    // resolve type coercion. Instead, in single pass, we apply type coercion twice on the same
    // node in order to ensure that types are resolved.
    applyTypeCoercion(withTypeCoercedOnce)
  }

  /**
   * Takes in a sequence of type coercion transformations that should be applied to an expression
   * and applies them in order. Finally, [[TypeCoercionResolver]] applies timezone to expression's
   * children, as a child could be replaced with Cast(child, type), therefore [[Cast]] resolution
   * is needed. Timezone is applied only on children that have been re-instantiated, because
   * otherwise children are already resolved.
   */
  private def applyTypeCoercion(expression: Expression) = {
    val oldChildren = expression.children

    val withTypeCoercion = typeCoercionTransformationsOrDefault.foldLeft(expression) {
      case (expr, rule) => rule.apply(expr)
    }

    val newChildren = withTypeCoercion.children.zip(oldChildren).map {
      case (newChild: Cast, oldChild) if !newChild.eq(oldChild) =>
        timezoneAwareExpressionResolver.withResolvedTimezone(newChild, conf.sessionLocalTimeZone)
      case (newChild, _) => newChild
    }
    withTypeCoercion.withNewChildren(newChildren)
  }
}

object TypeCoercionResolver {

  // Ordering in the list of type coercions should be in sync with the list in [[TypeCoercion]].
  private val TYPE_COERCION_TRANSFORMATIONS: Seq[Expression => Expression] = Seq(
    CollationTypeCoercion.apply,
    TypeCoercion.InTypeCoercion.apply,
    StringPromotionTypeCoercion.apply,
    DecimalPrecisionTypeCoercion.apply,
    BooleanEqualityTypeCoercion.apply,
    TypeCoercion.FunctionArgumentTypeCoercion.apply,
    TypeCoercion.ConcatTypeCoercion.apply,
    TypeCoercion.MapZipWithTypeCoercion.apply,
    TypeCoercion.EltTypeCoercion.apply,
    TypeCoercion.CaseWhenTypeCoercion.apply,
    TypeCoercion.IfTypeCoercion.apply,
    StackTypeCoercion.apply,
    DivisionTypeCoercion.apply,
    IntegralDivisionTypeCoercion.apply,
    TypeCoercion.ImplicitTypeCoercion.apply,
    TypeCoercion.DateTimeOperationsTypeCoercion.apply,
    TypeCoercion.WindowFrameTypeCoercion.apply,
    StringLiteralTypeCoercion.apply
  )

  // Ordering in the list of type coercions should be in sync with the list in [[AnsiTypeCoercion]].
  private val ANSI_TYPE_COERCION_TRANSFORMATIONS: Seq[Expression => Expression] = Seq(
    CollationTypeCoercion.apply,
    AnsiTypeCoercion.InTypeCoercion.apply,
    AnsiStringPromotionTypeCoercion.apply,
    DecimalPrecisionTypeCoercion.apply,
    AnsiTypeCoercion.FunctionArgumentTypeCoercion.apply,
    AnsiTypeCoercion.ConcatTypeCoercion.apply,
    AnsiTypeCoercion.MapZipWithTypeCoercion.apply,
    AnsiTypeCoercion.EltTypeCoercion.apply,
    AnsiTypeCoercion.CaseWhenTypeCoercion.apply,
    AnsiTypeCoercion.IfTypeCoercion.apply,
    StackTypeCoercion.apply,
    DivisionTypeCoercion.apply,
    IntegralDivisionTypeCoercion.apply,
    AnsiTypeCoercion.ImplicitTypeCoercion.apply,
    AnsiTypeCoercion.AnsiDateTimeOperationsTypeCoercion.apply,
    AnsiTypeCoercion.WindowFrameTypeCoercion.apply,
    AnsiGetDateFieldOperationsTypeCoercion.apply
  )
}
