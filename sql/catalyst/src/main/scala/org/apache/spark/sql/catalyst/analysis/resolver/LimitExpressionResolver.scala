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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.types.IntegerType

/**
 * The [[LimitExpressionResolver]] is a resolver that resolves a [[LocalLimit]] or [[GlobalLimit]]
 * expression and performs all the necessary validation.
 */
class LimitExpressionResolver(expressionResolver: TreeNodeResolver[Expression, Expression])
    extends TreeNodeResolver[Expression, Expression]
    with QueryErrorsBase {

  /**
   * Resolve a limit expression of [[GlobalLimit]] or [[LocalLimit]] and perform validation.
   */
  override def resolve(unresolvedLimitExpression: Expression): Expression = {
    val resolvedLimitExpression = expressionResolver.resolve(unresolvedLimitExpression)
    validateLimitExpression(resolvedLimitExpression, expressionName = "limit")
    resolvedLimitExpression
  }

  /**
   * Validate a resolved limit expression of [[GlobalLimit]] or [[LocalLimit]]:
   *  - The expression has to be foldable
   *  - The result data type has to be [[IntegerType]]
   *  - The evaluated expression has to be non-null
   *  - The evaluated expression has to be positive
   *
   * The `foldable` check is implemented in some expressions
   * as a recursive expression tree traversal.
   * It is not an ideal approach for the single-pass [[ExpressionResolver]],
   * but __is__ practical, since:
   *  - We have to call `eval` here anyway, and it's recursive
   *  - In practice `LIMIT` expression trees are very small
   */
  private def validateLimitExpression(expression: Expression, expressionName: String): Unit = {
    if (!expression.foldable) {
      throwInvalidLimitLikeExpressionIsUnfoldable(expressionName, expression)
    }
    if (expression.dataType != IntegerType) {
      throwInvalidLimitLikeExpressionDataType(expressionName, expression)
    }
    expression.eval() match {
      case null =>
        throwInvalidLimitLikeExpressionIsNull(expressionName, expression)
      case value: Int if value < 0 =>
        throwInvalidLimitLikeExpressionIsNegative(expressionName, expression, value)
      case _ =>
    }
  }

  private def throwInvalidLimitLikeExpressionIsUnfoldable(
      name: String,
      expression: Expression): Nothing =
    throw new AnalysisException(
      errorClass = "INVALID_LIMIT_LIKE_EXPRESSION.IS_UNFOLDABLE",
      messageParameters = Map(
        "name" -> name,
        "expr" -> toSQLExpr(expression)
      ),
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
      messageParameters = Map("name" -> name, "expr" -> toSQLExpr(expression)),
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
