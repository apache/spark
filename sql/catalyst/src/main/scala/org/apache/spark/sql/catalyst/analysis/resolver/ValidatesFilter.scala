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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.types.BooleanType

/**
 * [[ValidatesFilter]] is used by resolvers to validate resolved [[Filter]] operator.
 */
trait ValidatesFilter extends QueryErrorsBase {

  /**
   * Method used to validate [[Filter]] operator. Throws in the following cases:
   *  - If condition is invalid (based on
   *    `UnsupportedExpressionInOperatorValidation.isExpressionInUnsupportedOperator` check)
   *  - If the data type of the condition is not boolean.
   */
  def validateFilter(
      invalidExpressions: Seq[Expression],
      unresolvedOperator: LogicalPlan,
      resolvedFilter: Filter): Unit = {
    withPosition(unresolvedOperator) {
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
