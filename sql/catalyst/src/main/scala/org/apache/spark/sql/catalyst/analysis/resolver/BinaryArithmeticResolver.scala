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
  BinaryArithmeticWithDatetimeResolver,
  DecimalPrecisionTypeCoercion,
  DivisionTypeCoercion,
  IntegralDivisionTypeCoercion,
  StringPromotionTypeCoercion,
  TypeCoercion
}
import org.apache.spark.sql.catalyst.expressions.{
  Add,
  BinaryArithmetic,
  DateAdd,
  Divide,
  Expression,
  Multiply,
  Subtract,
  SubtractDates
}
import org.apache.spark.sql.types.{DateType, StringType}

/**
 * [[BinaryArithmeticResolver]] is invoked by [[ExpressionResolver]] in order to resolve
 * [[BinaryArithmetic]] nodes. During resolution, calling [[BinaryArithmeticWithDatetimeResolver]]
 * and applying type coercion can result in [[BinaryArithmetic]] producing some other type of node
 * or a subtree of nodes. In such cases a downwards traversal is necessary, but not going deeper
 * than the original expression's children, since all nodes below that point are guaranteed to be
 * already resolved.
 *
 * For example, given a query:
 *
 *  SELECT '4 11:11' - INTERVAL '4 22:12' DAY TO MINUTE
 *
 * [[BinaryArithmeticResolver]] is called for the following expression:
 *
 *     Subtract(
 *         Literal('4 11:11', StringType),
 *         Literal(Interval('4 22:12' DAY TO MINUTE), DayTimeIntervalType(0,2))
 *     )
 *
 * After calling [[BinaryArithmeticWithDatetimeResolver]] and applying type coercion,
 * the expression is transformed into:
 *
 *     Cast(
 *         DatetimeSub(
 *             TimeAdd(
 *                 Literal('4 11:11', StringType),
 *                 UnaryMinus(
 *                     Literal(Interval('4 22:12' DAY TO MINUTE), DayTimeIntervalType(0,2))
 *                 )
 *             )
 *         )
 *     )
 *
 * A single [[Subtract]] node is replaced with a subtree of nodes. In order to resolve this subtree
 * we need to invoke [[ExpressionResolver]] recursively on the top-most node's children. The
 * top-most node itself is not resolved recursively in order to avoid recursive calls to
 * [[BinaryArithmeticResolver]] and other sub-resolvers. To prevent a case where we resolve the
 * same node twice, we need to mark nodes that will act as a limit for the downwards traversal by
 * applying a [[ExpressionResolver.SINGLE_PASS_SUBTREE_BOUNDARY]] tag to them. These children
 * along with all the nodes below them are guaranteed to be resolved at this point. When
 * [[ExpressionResolver]] reaches one of the tagged nodes, it returns identity rather than
 * resolving it. Finally, after resolving the subtree, we need to resolve the top-most node itself,
 * which in this case means applying a timezone, if necessary.
 */
class BinaryArithmeticResolver(
    expressionResolver: ExpressionResolver,
    timezoneAwareExpressionResolver: TimezoneAwareExpressionResolver)
    extends TreeNodeResolver[BinaryArithmetic, Expression]
    with ProducesUnresolvedSubtree {

  private val typeCoercionTransformations: Seq[Expression => Expression] =
    if (conf.ansiEnabled) {
      BinaryArithmeticResolver.ANSI_TYPE_COERCION_TRANSFORMATIONS
    } else {
      BinaryArithmeticResolver.TYPE_COERCION_TRANSFORMATIONS
    }
  private val typeCoercionResolver: TypeCoercionResolver =
    new TypeCoercionResolver(timezoneAwareExpressionResolver, typeCoercionTransformations)

  override def resolve(unresolvedBinaryArithmetic: BinaryArithmetic): Expression = {
    val binaryArithmeticWithResolvedChildren: BinaryArithmetic =
      withResolvedChildren(unresolvedBinaryArithmetic, expressionResolver.resolve _)
        .asInstanceOf[BinaryArithmetic]

    val binaryArithmeticWithResolvedSubtree: Expression =
      withResolvedSubtree(binaryArithmeticWithResolvedChildren, expressionResolver.resolve) {
        transformBinaryArithmeticNode(binaryArithmeticWithResolvedChildren)
      }

    timezoneAwareExpressionResolver.withResolvedTimezone(
      binaryArithmeticWithResolvedSubtree,
      conf.sessionLocalTimeZone
    )
  }

  /**
   * Transform [[BinaryArithmetic]] node by calling [[BinaryArithmeticWithDatetimeResolver]] and
   * applying type coercion. Initial node can be replaced with some other type of node or a subtree
   * of nodes.
   */
  private def transformBinaryArithmeticNode(binaryArithmetic: BinaryArithmetic): Expression = {
    val binaryArithmeticWithDateTypeReplaced: Expression =
      replaceDateType(binaryArithmetic)
    val binaryArithmeticWithTypeCoercion: Expression =
      typeCoercionResolver.resolve(binaryArithmeticWithDateTypeReplaced)
    // In case that original expression's children types are DateType and StringType, fixed-point
    // fails to resolve the expression with a single application of
    // [[BinaryArithmeticWithDatetimeResolver]]. Therefore, single-pass resolver needs to invoke
    // [[BinaryArithmeticWithDatetimeResolver.resolve]], type coerce and only after that fix the
    // date/string case. Instead of invoking [[BinaryArithmeticWithDatetimeResolver]] again, we
    // handle the case directly.
    (
      binaryArithmetic.left.dataType,
      binaryArithmetic.right.dataType
    ) match {
      case (_: DateType, _: StringType) =>
        binaryArithmeticWithTypeCoercion match {
          case add: Add => DateAdd(add.left, add.right)
          case subtract: Subtract => SubtractDates(subtract.left, subtract.right)
          case other => other
        }
      case _ => binaryArithmeticWithTypeCoercion
    }
  }

  /**
   * When DateType like operand is given to [[BinaryArithmetic]], apply
   * [[BinaryArithmeticWithDatetimeResolver]] in order to replace the [[BinaryArithmetic]] with
   * the appropriate equivalent for DateTime types.
   */
  private def replaceDateType(expression: Expression) = expression match {
    case arithmetic @ (_: Add | _: Subtract | _: Multiply | _: Divide) =>
      BinaryArithmeticWithDatetimeResolver.resolve(arithmetic)
    case other => other
  }
}

object BinaryArithmeticResolver {
  // Ordering in the list of type coercions should be in sync with the list in [[TypeCoercion]].
  private val TYPE_COERCION_TRANSFORMATIONS: Seq[Expression => Expression] = Seq(
    StringPromotionTypeCoercion.apply,
    DecimalPrecisionTypeCoercion.apply,
    DivisionTypeCoercion.apply,
    IntegralDivisionTypeCoercion.apply,
    TypeCoercion.ImplicitTypeCoercion.apply,
    TypeCoercion.DateTimeOperationsTypeCoercion.apply
  )

  // Ordering in the list of type coercions should be in sync with the list in [[AnsiTypeCoercion]].
  private val ANSI_TYPE_COERCION_TRANSFORMATIONS: Seq[Expression => Expression] = Seq(
    AnsiStringPromotionTypeCoercion.apply,
    DecimalPrecisionTypeCoercion.apply,
    DivisionTypeCoercion.apply,
    IntegralDivisionTypeCoercion.apply,
    AnsiTypeCoercion.ImplicitTypeCoercion.apply,
    AnsiTypeCoercion.AnsiDateTimeOperationsTypeCoercion.apply
  )
}
