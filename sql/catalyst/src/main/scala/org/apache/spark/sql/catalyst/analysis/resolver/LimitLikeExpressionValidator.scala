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
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.types.IntegerType

/**
 * The [[LimitLikeExpressionValidator]] validates [[LocalLimit]], [[GlobalLimit]], [[Offset]] or
 * [[Tail]] integer expressions.
 */
class LimitLikeExpressionValidator extends QueryErrorsBase {
  def validateLimitLikeExpr(
      limitLikeExpression: Expression,
      partiallyResolvedLimitLike: LogicalPlan
  ): Expression = {
    val evaluatedExpression =
      evaluateLimitLikeExpression(limitLikeExpression, partiallyResolvedLimitLike)

    checkValidLimitWithOffset(evaluatedExpression, partiallyResolvedLimitLike)

    limitLikeExpression
  }

  private def checkValidLimitWithOffset(evaluatedExpression: Int, plan: LogicalPlan): Unit = {
    plan match {
      case LocalLimit(_, Offset(offsetExpr, _)) =>
        val offset = offsetExpr.eval().asInstanceOf[Int]
        if (Int.MaxValue - evaluatedExpression < offset) {
          throw throwInvalidLimitWithOffsetSumExceedsMaxInt(evaluatedExpression, offset, plan)
        }
      case _ =>
    }
  }

  /**
   * Evaluate a resolved limit expression of [[GlobalLimit]], [[LocalLimit]], [[Offset]] or
   * [[Tail]], while performing required checks:
   *   - The expression has to be foldable
   *   - The result data type has to be [[IntegerType]]
   *   - The evaluated expression has to be non-null
   *   - The evaluated expression has to be positive
   *
   * The `foldable` check is implemented in some expressions
   * as a recursive expression tree traversal.
   * It is not an ideal approach for the single-pass [[ExpressionResolver]],
   * but __is__ practical, since:
   *  - We have to call `eval` here anyway, and it's recursive
   *  - In practice `LIMIT`, `OFFSET` and `TAIL` expression trees are very small
   *
   * The return type of evaluation is Int, as we perform check that the expression has
   * IntegerType.
   */
  private def evaluateLimitLikeExpression(expression: Expression, plan: LogicalPlan): Int = {
    val operatorName = plan match {
      case _: Offset => "offset"
      case _: Tail => "tail"
      case _: LocalLimit | _: GlobalLimit => "limit"
      case other =>
        throw SparkException.internalError(
          s"Unexpected limit like operator type: ${other.getClass.getName}"
        )
    }
    if (!expression.foldable) {
      throwInvalidLimitLikeExpressionIsUnfoldable(operatorName, expression)
    }
    if (expression.dataType != IntegerType) {
      throwInvalidLimitLikeExpressionDataType(operatorName, expression)
    }
    expression.eval() match {
      case null =>
        throwInvalidLimitLikeExpressionIsNull(operatorName, expression)
      case value: Int if value < 0 =>
        throwInvalidLimitLikeExpressionIsNegative(operatorName, expression, value)
      case result =>
        result.asInstanceOf[Int]
    }
  }

  private def throwInvalidLimitWithOffsetSumExceedsMaxInt(
      limit: Int,
      offset: Int,
      plan: LogicalPlan): Nothing =
    throw new AnalysisException(
      errorClass = "SUM_OF_LIMIT_AND_OFFSET_EXCEEDS_MAX_INT",
      messageParameters = Map("limit" -> limit.toString, "offset" -> offset.toString),
      origin = plan.origin
    )

  private def throwInvalidLimitLikeExpressionIsUnfoldable(
      name: String,
      expression: Expression): Nothing =
    throw new AnalysisException(
      errorClass = "INVALID_LIMIT_LIKE_EXPRESSION.IS_UNFOLDABLE",
      messageParameters = Map("name" -> name, "expr" -> toSQLExpr(expression)),
      origin = expression.origin
    )

  private def throwInvalidLimitLikeExpressionDataType(
      name: String,
      expression: Expression): Nothing =
    throw new AnalysisException(
      errorClass = "INVALID_LIMIT_LIKE_EXPRESSION.DATA_TYPE",
      messageParameters = Map(
        "name" -> name,
        "expr" -> toSQLExpr(expression),
        "dataType" -> toSQLType(expression.dataType)
      ),
      origin = expression.origin
    )

  private def throwInvalidLimitLikeExpressionIsNull(name: String, expression: Expression): Nothing =
    throw new AnalysisException(
      errorClass = "INVALID_LIMIT_LIKE_EXPRESSION.IS_NULL",
      messageParameters = Map(
        "name" -> name,
        "expr" -> toSQLExpr(expression)
      ),
      origin = expression.origin
    )

  private def throwInvalidLimitLikeExpressionIsNegative(
      name: String,
      expression: Expression,
      value: Int): Nothing =
    throw new AnalysisException(
      errorClass = "INVALID_LIMIT_LIKE_EXPRESSION.IS_NEGATIVE",
      messageParameters =
        Map("name" -> name, "expr" -> toSQLExpr(expression), "v" -> toSQLValue(value, IntegerType)),
      origin = expression.origin
    )
}
